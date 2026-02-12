# utils/scraper_utils.py

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

from config import ScraperSiteConfig
from utils.data_utils import clean_venue_data, is_duplicate_venue
from utils.schema_utils import get_venue_schema_for_llm

logger = logging.getLogger(__name__)


# ==================================================
# Browser Config (Windows Safe)
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
# LLM Strategy (TOKEN SAFE)
# ==================================================
def get_llm_strategy(custom_instruction: Optional[str] = None) -> LLMExtractionStrategy:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("❌ GROQ_API_KEY not set")

    instruction = custom_instruction or (
    "Extract future events only. "
    "Return a valid JSON array strictly matching the schema. "
    "Use not found for missing values. "
    "For category, classify the event using its name and description. "
    )


    llm_config = LLMConfig(
        provider="groq/llama-3.1-8b-instant",
        api_token=api_key,

        # ✅ OUTPUT TOKEN LIMIT (SAFE)
        max_tokens=512,

        # Deterministic output
        temperature=1,
        
    )

    return LLMExtractionStrategy(
        llm_config=llm_config,
        schema=get_venue_schema_for_llm(),
        extraction_type="schema",
        instruction=instruction,
        verbose=False,
    )


# ==================================================
# Fetch + Extract ONE PAGE (TOKEN SAFE)
# ==================================================
async def fetch_and_process_page(
    crawler: AsyncWebCrawler,
    page_number: int,
    site_config: ScraperSiteConfig,
    llm_strategy: LLMExtractionStrategy,
    session_id: str,
    seen_names: Set[str],
) -> Tuple[List[dict], bool]:

    url = f"{site_config.base_url}?page={page_number}"
    logger.info(f"[{site_config.site_id}] Page {page_number}")

    try:
        result = await crawler.arun(
            url=url,
            config=CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                extraction_strategy=llm_strategy,
                session_id=session_id,
                # ⚠️ Do NOT add unsupported args like text_mode
            ),
        )
    except Exception as e:
        logger.error(f"[{site_config.site_id}] Crawl error: {e}")
        return [], False

    if not result.success or not result.extracted_content:
        logger.info(f"[{site_config.site_id}] No LLM content")
        return [], False

    try:
        extracted = json.loads(result.extracted_content)
    except Exception as e:
        logger.error(f"[{site_config.site_id}] JSON parse error: {e}")
        return [], False

    if not isinstance(extracted, list) or not extracted:
        return [], False

    venues = []
    now = datetime.utcnow().isoformat()

    # ❌ GLOBAL BLOCKLIST (kills Support / FAQ / Footer)
    INVALID_KEYWORDS = {
        "support", "faq", "help", "privacy", "terms",
        "contact", "about", "login", "register", "instagram", "logo", 
    }

    for item in extracted:
        try:
            venue = clean_venue_data(item)

            name = venue.get("name", "").strip()
            if not name:
                continue

            name_lower = name.lower()

            # ❌ Drop footer / nav junk
            if any(k in name_lower for k in INVALID_KEYWORDS):
                logger.debug(f"[{site_config.site_id}] Dropped junk: {name}")
                continue

            # ❌ Drop obvious non-event URLs
            event_url = venue.get("event_url", "")
            if any(x in event_url for x in ("support.", "help.", "faq.", "/contact")):
                continue

            # ❌ Require at least ONE real event signal
            if (
                venue.get("date") in ("Not Available", None)
                and venue.get("location") in ("Not Available", None)
            ):
                continue

            # ❌ De-duplication
            if is_duplicate_venue(name, seen_names):
                continue

            rate = venue.get("rate", "").strip().lower()

            FREE_KEYWORDS = ("free", "no cost", "complimentary")
            TIME_JUNK = ("am", "pm", "from", "to", "hrs", "hours")

            if not rate:
                venue["rate"] = "Check booking page"

            elif any(k in rate for k in FREE_KEYWORDS):
                venue["rate"] = "Free"

            elif any(k in rate for k in TIME_JUNK):
                venue["rate"] = "Check booking page"

            else:
                venue["rate"] = rate



            venue.update({
                "source_site_id": site_config.site_id,
                "source_site_name": site_config.name,
                "source_url": url,
                "created_at": now,
                "updated_at": now,
                "last_scraped": now,
            })

            seen_names.add(name)
            venues.append(venue)

        except Exception as e:
            logger.warning(f"[{site_config.site_id}] Skipped item: {e}")

    logger.info(f"[{site_config.site_id}] Extracted {len(venues)} venues")
    return venues, False



# ==================================================
# Scrape ONE SITE (PAGE → SAVE → DELAY)
# ==================================================
async def scrape_single_site(site_config: ScraperSiteConfig) -> dict:
    logger.info(f"🚀 [{site_config.site_id}] Starting scrape")

    browser_config = get_browser_config()
    session_id = f"session-{site_config.site_id}"
    llm_strategy = get_llm_strategy(site_config.custom_instruction)

    seen_names: Set[str] = set()
    total_saved = 0
    pages_scraped = 0

    from utils.mongodb_utils import mongodb_manager

    try:
        async with AsyncWebCrawler(config=browser_config) as crawler:
            for page in range(1, site_config.max_pages + 1):

                venues, _ = await fetch_and_process_page(
                    crawler=crawler,
                    page_number=page,
                    site_config=site_config,
                    llm_strategy=llm_strategy,
                    session_id=session_id,
                    seen_names=seen_names,
                )

                pages_scraped += 1

                if venues:
                    logger.info(
                        f"[{site_config.site_id}] 💾 Saving {len(venues)} venues (page {page})"
                    )
                    await mongodb_manager.upsert_venues_batch(
                        venues=venues,
                        site_id=site_config.site_id,
                        site_name=site_config.name,
                    )
                    total_saved += len(venues)

                # ❗ DO NOT EXIT EARLY ON EMPTY PAGE
                if not venues and page == 1:
                    logger.info(f"[{site_config.site_id}] No venues on first page, stopping")
                    break

                # ⏳ RATE LIMIT CONTROL
                await asyncio.sleep(site_config.llm_delay_seconds or 61)

    except asyncio.CancelledError:
        logger.warning(f"[{site_config.site_id}] Scrape cancelled safely")
    except Exception as e:
        logger.exception(f"[{site_config.site_id}] Fatal scrape error: {e}")

    logger.info(
        f"✅ [{site_config.site_id}] Done: {total_saved} venues, {pages_scraped} pages"
    )

    return {
        "site_id": site_config.site_id,
        "site_name": site_config.name,
        "status": "success",
        "total_venues": total_saved,
        "pages_scraped": pages_scraped,
    }


# ==================================================
# Scrape MULTIPLE SITES (SEQUENTIAL, SAFE)
# ==================================================
async def scrape_multiple_sites(site_configs: List[ScraperSiteConfig]) -> List[dict]:
    results = []

    for site in site_configs:
        results.append(await scrape_single_site(site))

    return results
