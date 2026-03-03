"""
config.py

Supports two scraping strategies per site:
  - "pagination"  : traditional page-by-page URLs (?page=2, /page/2, etc.)
  - "scroll"      : infinite scroll or "Load More" button click

HOW TO IDENTIFY WHICH STRATEGY A SITE USES:
  1. Open the site, scroll to the bottom
  2. If the URL changes when you go to next page → use "pagination"
  3. If events load without URL change (scroll or button) → use "scroll"

HOW TO FIND css_selector (both strategies):
  1. Chrome → right-click an event card → Inspect
  2. Find the repeating parent container (e.g. <ul class="event-list">)
  3. Copy its CSS selector

HOW TO FIND load_more_selector (scroll strategy only):
  1. Right-click the "Load More" / "Show More" button → Inspect
  2. Copy its CSS selector (e.g. "button.load-more", "a.view-more")
  3. If there's no button and it auto-scrolls, leave load_more_selector=None
     and set scroll_count to how many times to scroll down

HOW TO FIND url_pattern (pagination strategy only):
  1. Click page 2 on the site, look at the URL
  2. Replace the page number with {page} (e.g. "?page={page}", "/page/{page}")
"""

from typing import List, Optional, Literal
from pydantic import BaseModel


class ScraperSiteConfig(BaseModel):
    """Configuration for a single scraping site."""
    site_id: str
    name: str
    base_url: str

    # -------------------------------------------------------
    # STRATEGY: "pagination" or "scroll"
    # -------------------------------------------------------
    strategy: Literal["pagination", "scroll"] = "pagination"

    # --- PAGINATION settings (used when strategy="pagination") ---
    url_pattern: Optional[str] = "?page={page}"
    max_pages: int = 5

    # --- SCROLL settings (used when strategy="scroll") ---
    # CSS selector of "Load More" button. None = pure infinite scroll.
    load_more_selector: Optional[str] = None
    # How many times to click Load More or scroll down before extracting.
    scroll_count: int = 5
    # Milliseconds to wait after each scroll/click for content to load.
    scroll_wait_ms: int = 1500

    # -------------------------------------------------------
    # SHARED settings
    # -------------------------------------------------------
    css_selector: Optional[str] = None
    enabled: bool = True
    scrape_interval_hours: int = 12
    llm_delay_seconds: int = 5
    custom_instruction: Optional[str] = None
    category: Optional[str] = None
    location: Optional[str] = None


class AppConfig(BaseModel):
    cleanup_old_venues_days: int = 30
    max_concurrent_scrapes: int = 3
    backup_enabled: bool = True


SCRAPING_SITES: List[ScraperSiteConfig] = [

    # PAGINATION example — URL changes per page
    ScraperSiteConfig(
        site_id="allevents_kochi",
        name="AllEvents Kochi",
        base_url="https://allevents.in/kochi/all",
        strategy="pagination",
        url_pattern="?page={page}",
        max_pages=2,
        css_selector=None,
        enabled=True,
        scrape_interval_hours=12,
        llm_delay_seconds=61,
        category="events",
        location="kochi",
    ),

    ScraperSiteConfig(
        site_id="allevents_online",
        name="AllEvents online",
        base_url="https://allevents.in/online/all",
        strategy="pagination",
        url_pattern="?page={page}",
        max_pages=2,
        css_selector=None,
        enabled=True,
        scrape_interval_hours=12,
        llm_delay_seconds=61,
        category="events",
        location="online",
    ),

    ScraperSiteConfig(
    site_id="district",
    name="District",
    base_url="https://www.district.in/events/",
    strategy="scroll",
    load_more_selector=None,       # infinite scroll
    scroll_count=3,
    scroll_wait_ms=2500,
    css_selector="h5.dds-tracking-tight",   # targets event title elements — stable, unique
    enabled=True,
    scrape_interval_hours=12,
    location="bengaluru",          # or whichever city
    custom_instruction=(
        "Extract ALL events from the page. Each event has: "
        "a date in a span above the title, "
        "a title in an h5 tag, "
        "a location in the first span below the title, "
        "a price in the second span below the title (e.g. '₹49 onwards' or 'Free'). "
        "Return ONLY a JSON array. "
        "SKIP any event missing title AND location AND date. "
        "For category pick from: Music, Comedy, Nightlife, Food & Drinks, Art, Theatre, Sports, Tech, Business, Education, Kids, Wellness, Film, Festival, Meetup, Other."
    ),
    ),


    # SCROLL + LOAD MORE BUTTON example — URL stays same, button loads more

    # district.in is a JS SPA — content loads via scroll, URL never changes
    # [EXTRACT] 0.00s in logs = LLM never ran = page had no content = needs scroll
    # ScraperSiteConfig(
    #     site_id="district",
    #     name="district",
    #     base_url="https://www.district.in/events/",
    #     strategy="scroll",
    #     load_more_selector=None,   # pure infinite scroll, no button
    #     scroll_count=6,
    #     scroll_wait_ms=2500,       # SPA needs more time per scroll
    #     css_selector=".event-card",  # inspect & update
    #     enabled=True,
    # ),

    # INFINITE SCROLL example (no button — uncomment and configure to use)
    # ScraperSiteConfig(
    #     site_id="example_infinite",
    #     name="Example Infinite Scroll Site",
    #     base_url="https://example.com/events",
    #     strategy="scroll",
    #     load_more_selector=None,
    #     scroll_count=8,
    #     scroll_wait_ms=1500,
    #     css_selector=".event-card",
    #     enabled=False,
    # ),
]

APP_CONFIG = AppConfig()


def get_enabled_sites() -> List[ScraperSiteConfig]:
    return [s for s in SCRAPING_SITES if s.enabled]


def get_site_by_id(site_id: str) -> Optional[ScraperSiteConfig]:
    return next((s for s in SCRAPING_SITES if s.site_id == site_id), None)


def get_sites_by_category(category: str) -> List[ScraperSiteConfig]:
    return [s for s in SCRAPING_SITES if s.category == category and s.enabled]


def get_sites_by_location(location: str) -> List[ScraperSiteConfig]:
    return [s for s in SCRAPING_SITES if s.location == location and s.enabled]