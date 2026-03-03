"""
utils/scraper_utils.py

Supports two scraping strategies:
  - "pagination" : fetches multiple URLs (?page=1, ?page=2, ...)
  - "scroll"     : loads one page, then clicks "Load More" or auto-scrolls
                   N times before extracting — all events on one page
"""

import asyncio
import json
import os
import sys
from datetime import datetime
from typing import List, Set, Tuple, Optional
import logging

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CacheMode,
    CrawlerRunConfig,
    LLMExtractionStrategy,
    LLMConfig,
)
from crawl4ai.async_configs import BrowserConfig

from config import ScraperSiteConfig
from utils.data_utils import clean_venue_data, is_duplicate_venue
from utils.schema_utils import get_venue_schema_for_llm

logger = logging.getLogger(__name__)

# Disable LiteLLM's internal retry loop so our model rotation logic fires immediately.
# Without this, LiteLLM retries 2-3x internally before raising, bypassing our handler.
os.environ["LITELLM_NUM_RETRIES"] = "0"
os.environ["LITELLM_REQUEST_TIMEOUT"] = "30"

MAX_CONSECUTIVE_EMPTY = 2
MAX_RETRIES = 3

INVALID_KEYWORDS = {
    "support", "faq", "help", "privacy", "terms",
    "contact", "about", "login", "register", "instagram", "logo",
}
FREE_KEYWORDS = ("free", "no cost", "complimentary")
TIME_JUNK = ("am", "pm", "from", "to", "hrs", "hours")


# ==================================================
# Browser Config
# ==================================================
def get_browser_config() -> BrowserConfig:
    extra_args = []
    if sys.platform == "win32":
        extra_args = [
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
        ]
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,
        extra_args=extra_args,
    )


# ==================================================
# LLM Strategy
# ==================================================
# Model rotation list — ordered by preference.
# Primary is now llama-3.1-8b-instant (500k TPD vs 100k for 70b).
# 70b is used as fallback only when 8b fails on quality.
# Groq free tier limits (approximate):
#   llama-3.1-8b-instant  : 500k tokens/day, 20k tokens/min
#   llama-3.3-70b-versatile: 100k tokens/day, 6k tokens/min
#   mixtral-8x7b-32768     : 500k tokens/day, 5k tokens/min
# Model rotation — ordered by token efficiency.
# Context window limits (free tier):
#   llama-3.1-8b-instant   : 6k TPM,  500k TPD  — small context, fast, cheap
#   llama-3.3-70b-versatile: 12k TPM, 100k TPD  — large context, best quality
#   mixtral-8x7b-32768     : 5k TPM,  500k TPD  — medium context
# When a page is too large for 8b (>6k tokens), it auto-rotates to 70b.
GROQ_MODELS = [
    "groq/llama-3.1-8b-instant",        # PRIMARY: cheapest, 500k TPD
    "groq/llama-3.3-70b-versatile",     # FALLBACK 1: larger context (handles big pages)
    "groq/mixtral-8x7b-32768",          # FALLBACK 2: last resort
]

# Track which model index to use (rotates on rate limit)
_current_model_index = 0


def _get_next_model() -> str:
    """Rotate to next model in the list."""
    global _current_model_index
    _current_model_index = (_current_model_index + 1) % len(GROQ_MODELS)
    model = GROQ_MODELS[_current_model_index]
    logger.warning(f"🔄 Rotating LLM model → {model}")
    return model


def _is_rate_limit_error(e: Exception) -> bool:
    """Detect TPM/TPD rate limits AND request-too-large errors from LiteLLM/Groq."""
    err_str = str(e).lower()
    err_type = type(e).__name__.lower()
    return (
        "ratelimit" in err_type
        or "rate_limit" in err_str
        or "rate limit" in err_str
        or "tokens per day" in err_str
        or "tokens per minute" in err_str
        or "tpd" in err_str
        or "tpm" in err_str
        or "request too large" in err_str      # context window exceeded
        or "please reduce your message" in err_str
    )


def _is_too_large_error(e: Exception) -> bool:
    """Detect request-too-large specifically (needs model rotation, not waiting)."""
    err_str = str(e).lower()
    return (
        "request too large" in err_str
        or "please reduce your message" in err_str
    )


def _parse_retry_wait(e: Exception) -> float:
    """Parse wait seconds from Groq rate limit error. Caps at 65s (above = daily limit, rotate instead)."""
    import re as _re
    s = str(e)
    h = _re.search("(\\d+)h", s)
    m = _re.search("(\\d+)m", s)
    sec = _re.search("([\\d.]+)s", s)
    total = (int(h.group(1)) * 3600 if h else 0)
    total += (int(m.group(1)) * 60 if m else 0)
    total += (float(sec.group(1)) if sec else 0)
    return min(total + 2, 65.0) if total > 0 else 10.0


# Strict category taxonomy — LLM must pick exactly one of these
EVENT_CATEGORIES = [
    "Music",          # concerts, gigs, DJ nights, bands, classical, choir
    "Comedy",         # stand-up, open mic, improv, sketch comedy
    "Nightlife",      # parties, club nights, bar events, pub quizzes
    "Food & Drinks",  # food festivals, wine tasting, cooking classes, restaurant events
    "Art",            # exhibitions, gallery openings, installations, craft fairs
    "Theatre",        # plays, musicals, drama, opera, dance performances
    "Sports",         # matches, tournaments, races, fitness events, yoga
    "Tech",           # hackathons, tech talks, product launches, coding events
    "Business",       # conferences, networking, seminars, workshops, trade shows
    "Education",      # classes, courses, lectures, training, certifications
    "Kids",           # children's events, family activities, school events
    "Wellness",       # meditation, therapy, health talks, mental wellness
    "Film",           # movie screenings, film festivals, documentary screenings
    "Literature",     # book launches, author talks, poetry, literary festivals
    "Travel",         # tours, travel meetups, adventure trips, city walks
    "Festival",       # cultural festivals, fairs, celebrations, holiday events
    "Meetup",         # social gatherings, community meetups, hobby groups
    "Other",          # anything that clearly doesn't fit above categories
]

DEFAULT_INSTRUCTION = (
    "Extract ALL future/upcoming events from this page. "
    "Return ONLY a raw JSON array — no markdown, no explanation, no code fences. "
    "\n\n"
    "RULES:\n"
    "1. SKIP any event missing ALL THREE of: title, venue/location, AND date. "
    "   An event must have at minimum a title + location OR title + date to be included.\n"
    "2. Use exactly 'Not Available' (not null, not empty) for any missing string field.\n"
    "3. For category, you MUST pick exactly one from this list:\n"
    "   Music, Comedy, Nightlife, Food & Drinks, Art, Theatre, Sports, Tech, Business, "
    "   Education, Kids, Wellness, Film, Literature, Travel, Festival, Meetup, Other\n"
    "   HOW TO CATEGORIZE — use the event title and description:\n"
    "   - Contains words like concert/gig/band/DJ/singer → Music\n"
    "   - Contains stand-up/comedian/comedy/open mic → Comedy\n"
    "   - Contains party/nightclub/pub/bar/rave → Nightlife\n"
    "   - Contains food/drink/wine/chef/restaurant/cuisine/tasting → Food & Drinks\n"
    "   - Contains art/gallery/exhibition/craft/paint/photography → Art\n"
    "   - Contains play/theatre/drama/musical/dance/ballet/opera → Theatre\n"
    "   - Contains match/sport/tournament/yoga/fitness/run/marathon → Sports\n"
    "   - Contains tech/startup/hackathon/coding/AI/software → Tech\n"
    "   - Contains conference/seminar/summit/networking/business → Business\n"
    "   - Contains workshop/course/class/training/lecture → Education\n"
    "   - Contains kids/children/family/school → Kids\n"
    "   - Contains wellness/meditation/mental health/therapy → Wellness\n"
    "   - Contains film/movie/screening/cinema/documentary → Film\n"
    "   - Contains book/author/poetry/literary/reading → Literature\n"
    "   - Contains tour/travel/trip/walk/adventure → Travel\n"
    "   - Contains festival/fair/celebration/carnival → Festival\n"
    "   - Contains meetup/chai/tea/community/social/gathering/club → Meetup\n"
    "   - If unsure → Other\n"
    "4. Do NOT include navigation links, ads, banners, or non-event page elements."
)


async def call_groq_direct(
    markdown_text: str,
    custom_instruction: Optional[str] = None,
) -> list:
    """
    Call Groq API directly using httpx — bypasses Crawl4AI/LiteLLM retry wrappers.
    We control model rotation on every error type ourselves.
    Returns parsed list of venue dicts, or [] on failure.
    """
    import httpx

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set in environment")

    schema = get_venue_schema_for_llm()
    instruction = custom_instruction or DEFAULT_INSTRUCTION
    system_prompt = (
        f"{instruction}\n\n"
        f"Schema to follow (return a JSON array of objects matching this):\n"
        f"{json.dumps(schema, indent=2)}"
    )

    global _current_model_index

    for attempt in range(MAX_RETRIES):
        model_name = GROQ_MODELS[_current_model_index].replace("groq/", "")
        logger.info(f"🤖 Calling Groq: {model_name} (attempt {attempt+1})")

        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Extract events from this page content:\n\n{markdown_text}"},
            ],
            "temperature": 0,
            "max_tokens": 4096,    # Increased — 2048 truncated large JSON responses
        }

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                data = resp.json()

                if resp.status_code == 200:
                    text = data["choices"][0]["message"]["content"].strip()
                    if text.startswith("```"):
                        text = text.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
                    return json.loads(text)

                error = data.get("error", {})
                err_msg = error.get("message", str(data))
                err_code = error.get("code", "")

                if resp.status_code == 429 or err_code == "rate_limit_exceeded":
                    if "request too large" in err_msg.lower() or "please reduce" in err_msg.lower():
                        # Context window exceeded — rotate model immediately
                        _current_model_index = (_current_model_index + 1) % len(GROQ_MODELS)
                        next_model = GROQ_MODELS[_current_model_index]
                        logger.warning(f"🔄 Request too large — rotating to {next_model}")
                        await asyncio.sleep(1)
                    else:
                        wait_s = _parse_retry_wait_str(err_msg)
                        # wait_s >= 65 = daily (TPD) limit → rotate
                        # wait_s < 65  = per-minute (TPM) limit → just wait
                        # wait_s == 10 = fallback (no time parsed) → likely TPM, wait 15s
                        if wait_s >= 65:
                            _current_model_index = (_current_model_index + 1) % len(GROQ_MODELS)
                            next_model = GROQ_MODELS[_current_model_index]
                            logger.warning(f"🔄 Daily limit hit — rotating to {next_model}")
                            await asyncio.sleep(3)
                        else:
                            actual_wait = max(wait_s, 15.0)  # min 15s to avoid hammering
                            logger.warning(f"⏳ TPM rate limit — waiting {actual_wait:.0f}s")
                            await asyncio.sleep(actual_wait)
                else:
                    logger.error(f"Groq API error {resp.status_code}: {err_msg}")
                    await asyncio.sleep(2 ** attempt)

        except json.JSONDecodeError as e:
            logger.warning(f"JSON truncated from Groq (attempt {attempt+1}) — response cut off mid-JSON")
            # Don't retry immediately — the model hit max_tokens. Try next model with more capacity.
            _current_model_index = (_current_model_index + 1) % len(GROQ_MODELS)
            next_model = GROQ_MODELS[_current_model_index]
            logger.warning(f"🔄 JSON truncated — rotating to {next_model}")
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"Groq call error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2 ** attempt)

    logger.error("All Groq attempts exhausted")
    return []


def _parse_retry_wait_str(msg: str) -> float:
    """Parse retry wait seconds from Groq error message string."""
    import re as _re
    h = _re.search(r"(\d+)h", msg)
    m = _re.search(r"(\d+)m", msg)
    s = _re.search(r"([\d.]+)s", msg)
    total = (int(h.group(1)) * 3600 if h else 0)
    total += (int(m.group(1)) * 60 if m else 0)
    total += (float(s.group(1)) if s else 0)
    return min(total + 2, 65.0) if total > 0 else 10.0


def get_llm_strategy(
    custom_instruction: Optional[str] = None,
    model: Optional[str] = None,
) -> LLMExtractionStrategy:
    """Legacy — only used to satisfy type hints. Actual extraction uses call_groq_direct."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set in environment")
    selected_model = model or GROQ_MODELS[_current_model_index]
    llm_config = LLMConfig(provider=selected_model, api_token=api_key, max_tokens=2048, temperature=0)
    return LLMExtractionStrategy(
        llm_config=llm_config,
        schema=get_venue_schema_for_llm(),
        extraction_type="schema",
        instruction=custom_instruction or DEFAULT_INSTRUCTION,
        verbose=False,
    )


# ==================================================
# URL builder (pagination only)
# ==================================================
def build_page_url(site_config: ScraperSiteConfig, page_number: int) -> str:
    if not site_config.url_pattern or page_number == 1:
        return site_config.base_url
    return site_config.base_url + site_config.url_pattern.format(page=page_number)


# ==================================================
# Venue validation helpers
# ==================================================
def _normalize_rate(rate: str) -> str:
    rate = (rate or "").strip().lower()
    if not rate or rate == "not available":
        return "Check booking page"
    if any(k in rate for k in FREE_KEYWORDS):
        return "Free"
    if any(k in rate for k in TIME_JUNK):
        return "Check booking page"
    return rate


def _missing(val) -> bool:
    """True if value is empty, None, or sentinel."""
    return not val or str(val).strip() in ("", "Not Available", "N/A", "null", "None")


def _is_junk_item(venue: dict) -> bool:
    name     = (venue.get("name") or "").strip()
    location = venue.get("location") or ""
    date     = venue.get("date") or ""

    # Must have a non-empty name
    if not name or name.lower() in INVALID_KEYWORDS:
        return True

    # Drop known nav/support URLs
    event_url = venue.get("event_url") or ""
    if any(x in event_url for x in ("support.", "help.", "faq.", "/contact")):
        return True

    # ---- CORE RULE: must have title + location + date ----
    # Allow if at least title + one of (location OR date) is present
    has_location = not _missing(location)
    has_date     = not _missing(date)
    if not has_location and not has_date:
        return True   # has title only — not enough

    # Category validation — normalize to known list or set to Other
    # (done here so it's applied consistently even if LLM returns garbage)
    return False


def _normalize_category(raw: str) -> str:
    """Map LLM category output to the canonical taxonomy. Fallback to 'Other'."""
    if not raw or raw.strip() in ("Not Available", ""):
        return "Other"
    r = raw.strip().lower()
    mapping = {
        "music": "Music",
        "concert": "Music", "gig": "Music", "band": "Music", "dj": "Music",
        "comedy": "Comedy", "stand-up": "Comedy", "standup": "Comedy", "open mic": "Comedy",
        "nightlife": "Nightlife", "party": "Nightlife", "club": "Nightlife", "pub": "Nightlife", "bar": "Nightlife",
        "food": "Food & Drinks", "food & drinks": "Food & Drinks", "drink": "Food & Drinks",
        "wine": "Food & Drinks", "culinary": "Food & Drinks", "dining": "Food & Drinks",
        "art": "Art", "gallery": "Art", "exhibition": "Art", "craft": "Art", "photography": "Art",
        "theatre": "Theatre", "theater": "Theatre", "drama": "Theatre", "musical": "Theatre",
        "dance": "Theatre", "ballet": "Theatre", "opera": "Theatre", "performance": "Theatre",
        "sports": "Sports", "sport": "Sports", "yoga": "Sports", "fitness": "Sports",
        "marathon": "Sports", "run": "Sports", "match": "Sports", "tournament": "Sports",
        "tech": "Tech", "technology": "Tech", "startup": "Tech", "hackathon": "Tech",
        "coding": "Tech", "software": "Tech", "ai": "Tech",
        "business": "Business", "conference": "Business", "seminar": "Business",
        "networking": "Business", "summit": "Business", "b2b": "Business",
        "education": "Education", "workshop": "Education", "class": "Education",
        "course": "Education", "training": "Education", "lecture": "Education",
        "kids": "Kids", "children": "Kids", "family": "Kids",
        "wellness": "Wellness", "meditation": "Wellness", "health": "Wellness", "mental": "Wellness",
        "film": "Film", "movie": "Film", "cinema": "Film", "screening": "Film", "documentary": "Film",
        "literature": "Literature", "book": "Literature", "author": "Literature", "poetry": "Literature",
        "travel": "Travel", "tour": "Travel", "trip": "Travel", "adventure": "Travel",
        "festival": "Festival", "fair": "Festival", "carnival": "Festival", "celebration": "Festival",
        "meetup": "Meetup", "meet-up": "Meetup", "community": "Meetup", "social": "Meetup", "gathering": "Meetup",
        "other": "Other",
    }
    # Exact match first
    if r in mapping:
        return mapping[r]
    # Substring match
    for key, val in mapping.items():
        if key in r:
            return val
    return "Other"


# ==================================================
# Parse raw LLM output into clean venue list
# ==================================================
def _parse_venues(
    extracted: list,
    site_config: ScraperSiteConfig,
    source_url: str,
    seen_names: Set[str],
) -> List[dict]:
    now = datetime.utcnow().isoformat()
    venues = []

    for item in extracted:
        try:
            venue = clean_venue_data(item)

            if _is_junk_item(venue):
                logger.debug(f"[{site_config.site_id}] Dropped junk: {venue.get('name')}")
                continue

            name = venue["name"].strip()
            if is_duplicate_venue(name, seen_names):
                continue

            venue["rate"] = _normalize_rate(venue.get("rate", ""))
            venue["category"] = _normalize_category(venue.get("category", ""))
            venue.update({
                "source_site_id": site_config.site_id,
                "source_site_name": site_config.name,
                "source_url": source_url,
                "created_at": now,
                "updated_at": now,
                "last_scraped": now,
            })

            seen_names.add(name)
            venues.append(venue)

        except Exception as e:
            logger.warning(f"[{site_config.site_id}] Skipped item: {e}")

    return venues


# ==================================================
# STRATEGY 1: Fetch a single paginated URL
# ==================================================
async def fetch_paginated_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    site_config: ScraperSiteConfig,
    llm_strategy: LLMExtractionStrategy,  # kept for signature compat, unused
    session_id: str,
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:
    """Fetch one paginated page, extract HTML via Crawl4AI, call Groq directly."""

    url = build_page_url(site_config, page_number)
    logger.info(f"[{site_config.site_id}] Pagination page {page_number}: {url}")

    # Crawl only — no LLM extraction here (we call Groq directly below)
    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        session_id=session_id,
        word_count_threshold=15,
        excluded_tags=["nav", "header", "footer", "script", "style",
                       "aside", "form", "noscript", "iframe", "svg"],
        remove_overlay_elements=True,
    )
    if site_config.css_selector:
        crawler_config.css_selector = site_config.css_selector

    try:
        result = await crawler.arun(url=url, config=crawler_config)
    except Exception as e:
        logger.error(f"[{site_config.site_id}] Crawl error: {e}")
        return [], True

    if not result.success or not result.markdown:
        logger.warning(f"[{site_config.site_id}] No markdown from page {page_number}")
        return [], True

    # Trim markdown to stay safely under model context limits
    markdown = result.markdown[:12000]

    extracted = await call_groq_direct(markdown, site_config.custom_instruction)

    if not isinstance(extracted, list):
        return [], True

    venues = _parse_venues(extracted, site_config, url, seen_names)
    logger.info(f"[{site_config.site_id}] Page {page_number}: {len(venues)} venues")
    return venues, False


# ==================================================
# STRATEGY 2: Scroll / Load More on a single page
# ==================================================
async def fetch_scroll_page(
    crawler: AsyncWebCrawler,
    site_config: ScraperSiteConfig,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    seen_names: Set[str],
) -> List[dict]:
    """
    Load one page, then either:
      A) Click a "Load More" button N times, OR
      B) Scroll to bottom N times (infinite scroll)
    Then extract all events from the fully-loaded page.
    """
    url = site_config.base_url
    logger.info(f"[{site_config.site_id}] Scroll strategy: {url}")
    logger.info(
        f"[{site_config.site_id}] "
        f"{'Clicking: ' + site_config.load_more_selector if site_config.load_more_selector else 'Auto-scrolling'} "
        f"× {site_config.scroll_count}"
    )

    # Build JS to run inside the browser after initial load
    selector = site_config.load_more_selector
    wait_ms = site_config.scroll_wait_ms
    clicks = site_config.scroll_count
    css = site_config.css_selector or "body"

    if selector:
        # Wait for page to fully render first (initial_wait_ms),
        # then repeatedly click Load More with a gap between each click.
        # Uses MutationObserver to detect when new cards actually appear
        # before doing the next click — much more reliable than fixed delays.
        js_script = f"""
        async () => {{
            const waitMs = {wait_ms};
            const clicks = {clicks};
            const btnSel = "{selector}";
            const cardSel = "{css}";

            // Wait for initial page render
            await new Promise(r => setTimeout(r, 2000));

            for (let i = 0; i < clicks; i++) {{
                const btn = document.querySelector(btnSel);
                if (!btn || btn.disabled || btn.style.display === "none") {{
                    console.log("[scraper] Load More not available at click " + i);
                    break;
                }}

                // Count current cards before click
                const before = document.querySelectorAll(cardSel).length;

                btn.scrollIntoView({{behavior: "smooth", block: "center"}});
                await new Promise(r => setTimeout(r, 300));
                btn.click();

                // Wait for new cards to appear (up to waitMs)
                const deadline = Date.now() + waitMs;
                while (Date.now() < deadline) {{
                    await new Promise(r => setTimeout(r, 300));
                    const after = document.querySelectorAll(cardSel).length;
                    if (after > before) break;  // New content loaded
                }}

                console.log("[scraper] Click " + (i+1) + " done, cards: " +
                    document.querySelectorAll(cardSel).length);
            }}
        }}
        """
    else:
        # Pure infinite scroll — scroll to bottom and wait for new content each time
        js_script = f"""
        async () => {{
            const waitMs = {wait_ms};
            const scrolls = {clicks};
            const cardSel = "{css}";

            await new Promise(r => setTimeout(r, 2000));  // initial render wait

            for (let i = 0; i < scrolls; i++) {{
                const before = document.querySelectorAll(cardSel).length;
                window.scrollTo(0, document.body.scrollHeight);

                const deadline = Date.now() + waitMs;
                while (Date.now() < deadline) {{
                    await new Promise(r => setTimeout(r, 300));
                    if (document.querySelectorAll(cardSel).length > before) break;
                }}

                console.log("[scraper] Scroll " + (i+1) + " done, cards: " +
                    document.querySelectorAll(cardSel).length);
            }}
        }}
        """

    # Crawl only — JS runs, page scrolls/clicks, then we get markdown
    wait_condition = f"css:{site_config.css_selector}" if site_config.css_selector else None

    crawler_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        session_id=session_id,
        js_code=js_script,
        wait_for=wait_condition,
        page_timeout=60000,
        wait_for_timeout=15000,
        word_count_threshold=15,
        excluded_tags=["nav", "header", "footer", "script", "style",
                       "aside", "form", "noscript", "iframe", "svg"],
        remove_overlay_elements=True,
    )
    if site_config.css_selector:
        crawler_config.css_selector = site_config.css_selector

    try:
        result = await crawler.arun(url=url, config=crawler_config)
    except Exception as e:
        logger.error(f"[{site_config.site_id}] Scroll crawl error: {e}")
        return []

    if not result.success or not result.markdown:
        logger.warning(f"[{site_config.site_id}] No markdown from scroll page")
        return []

    markdown = result.markdown[:12000]
    extracted = await call_groq_direct(markdown, site_config.custom_instruction)

    if not isinstance(extracted, list):
        logger.error(f"[{site_config.site_id}] Scroll extraction failed")
        return []

    venues = _parse_venues(extracted, site_config, url, seen_names)
    logger.info(f"[{site_config.site_id}] Scroll extracted: {len(venues)} venues")
    return venues


# ==================================================
# Scrape ONE SITE (routes to correct strategy)
# ==================================================
async def scrape_single_site(site_config: ScraperSiteConfig) -> dict:
    logger.info(
        f"🚀 [{site_config.site_id}] Starting scrape "
        f"(strategy={site_config.strategy}): {site_config.base_url}"
    )

    browser_config = get_browser_config()
    session_id = f"session-{site_config.site_id}"
    llm_strategy = get_llm_strategy(site_config.custom_instruction)

    seen_names: Set[str] = set()
    total_saved = 0
    pages_scraped = 0

    from utils.mongodb_utils import mongodb_manager

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:

            # ------------------------------------------
            # SCROLL strategy — single fetch with JS
            # ------------------------------------------
            if site_config.strategy == "scroll":
                venues = await fetch_scroll_page(
                    crawler=crawler,
                    site_config=site_config,
                    llm_strategy=llm_strategy,
                    session_id=session_id,
                    seen_names=seen_names,
                )
                pages_scraped = 1

                if venues:
                    logger.info(
                        f"[{site_config.site_id}] 💾 Saving {len(venues)} venues"
                    )
                    await mongodb_manager.upsert_venues_batch(
                        venues=venues,
                        site_id=site_config.site_id,
                        site_name=site_config.name,
                    )
                    total_saved = len(venues)
                else:
                    logger.warning(f"[{site_config.site_id}] No venues from scroll")

            # ------------------------------------------
            # PAGINATION strategy — loop through pages
            # ------------------------------------------
            else:
                consecutive_empty = 0

                for page in range(1, site_config.max_pages + 1):
                    venues, had_error = await fetch_paginated_page(
                        crawler=crawler,
                        page_number=page,
                        site_config=site_config,
                        llm_strategy=llm_strategy,
                        session_id=session_id,
                        seen_names=seen_names,
                    )
                    pages_scraped += 1

                    if venues:
                        consecutive_empty = 0
                        logger.info(
                            f"[{site_config.site_id}] 💾 Saving {len(venues)} venues (page {page})"
                        )
                        await mongodb_manager.upsert_venues_batch(
                            venues=venues,
                            site_id=site_config.site_id,
                            site_name=site_config.name,
                        )
                        total_saved += len(venues)
                    else:
                        if not had_error:
                            consecutive_empty += 1
                            logger.info(
                                f"[{site_config.site_id}] Empty page {page} "
                                f"({consecutive_empty}/{MAX_CONSECUTIVE_EMPTY})"
                            )

                    if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                        logger.info(
                            f"[{site_config.site_id}] Stopping: "
                            f"{MAX_CONSECUTIVE_EMPTY} consecutive empty pages"
                        )
                        break

                    if page < site_config.max_pages:
                        await asyncio.sleep(site_config.llm_delay_seconds or 5)

    except asyncio.CancelledError:
        logger.warning(f"[{site_config.site_id}] Scrape cancelled")
    except Exception as e:
        logger.exception(f"[{site_config.site_id}] Fatal scrape error: {e}")

    logger.info(
        f"✅ [{site_config.site_id}] Done — "
        f"{total_saved} venues saved, {pages_scraped} pages scraped"
    )

    return {
        "site_id": site_config.site_id,
        "site_name": site_config.name,
        "status": "success",
        "total_venues": total_saved,
        "pages_scraped": pages_scraped,
    }


# ==================================================
# Scrape MULTIPLE SITES (sequential)
# ==================================================
async def scrape_multiple_sites(site_configs: List[ScraperSiteConfig]) -> List[dict]:
    results = []
    for site in site_configs:
        results.append(await scrape_single_site(site))
    return results