from typing import List, Dict, Optional
from pydantic import BaseModel, HttpUrl


class ScraperSiteConfig(BaseModel):
    """Configuration for a single scraping site."""
    site_id: str  # Unique identifier
    name: str  # Display name
    base_url: str
    #css_selector: Optional[str] = None
    enabled: bool = True
    scrape_interval_hours: int = 12
    max_pages: int = 10
    llm_delay_seconds: int = 61
    custom_instruction: Optional[str] = None

    
    # Site-specific LLM instructions (optional)
    custom_instruction: Optional[str] = None
    
    # Metadata
    category: Optional[str] = None  # e.g., "events", "venues", "concerts"
    location: Optional[str] = None  # e.g., "kochi", "mumbai"


class AppConfig(BaseModel):
    """Global application configuration."""
    required_keys: List[str] = [
        "name",
        "location",
        "date",
        "description",
    ]
    
    default_scrape_interval_hours: int = 12
    cleanup_old_venues_days: int = 30
    max_concurrent_scrapes: int = 3
    backup_enabled: bool = True


# Define your scraping sites
SCRAPING_SITES: List[ScraperSiteConfig] = [
    ScraperSiteConfig(
        site_id="all-events",
        name="all-events",
        base_url="https://allevents.in/kochi",
        css_selector=None,
        enabled=True,
        scrape_interval_hours=12,
        category="",
        location="pune"
    ),
    ScraperSiteConfig(
        site_id="allevents_mumbai",
        name="AllEvents Mumbai",
        base_url="https://allevents.in/mumbai",
        css_selector=None,
        enabled=True,
        scrape_interval_hours=12,
        category="events",
        location="mumbai"
    ),
    ScraperSiteConfig(
        site_id="dist-sports",
        name="sports",
        base_url="https://allevents.in/pune-in",
        css_selector=None,
        enabled=True,
        scrape_interval_hours=12,
        category="sports",
        location="delhi"
    ),
    ScraperSiteConfig(
        site_id="district",
        name="food",
        base_url="https://www.townscript.com/in/online",
        css_selector=None,
        enabled=True,
        scrape_interval_hours=24,  # Different interval
        category="food",
        location="bangalore"
    ),
    
]

# Global app configuration
APP_CONFIG = AppConfig()


def get_enabled_sites() -> List[ScraperSiteConfig]:
    """Get all enabled scraping sites."""
    return [site for site in SCRAPING_SITES if site.enabled]


def get_site_by_id(site_id: str) -> Optional[ScraperSiteConfig]:
    """Get a specific site configuration by ID."""
    return next((site for site in SCRAPING_SITES if site.site_id == site_id), None)


def get_sites_by_category(category: str) -> List[ScraperSiteConfig]:
    """Get all sites in a specific category."""
    return [site for site in SCRAPING_SITES if site.category == category and site.enabled]


def get_sites_by_location(location: str) -> List[ScraperSiteConfig]:
    """Get all sites for a specific location."""
    return [site for site in SCRAPING_SITES if site.location == location and site.enabled]