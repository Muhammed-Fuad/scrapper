"""
models/venue.py

Key fixes:
  - description field un-commented and consistent across Venue and VenueDB.
    Leaving it commented in the model but active in the schema causes silent field drops.
  - event_url is Optional[str] in BOTH Venue and VenueDB (was str/required in VenueDB,
    which crashed on null URLs from sites that don't show individual event links).
  - source_url field added to match what scraper_utils.py actually writes
    (was source_page_url in model vs source_url in scraper — silent data loss).
  - Sentinel value standardized to "Not Available" to match scraper_utils filters.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


class Venue(BaseModel):
    """Data structure for a scraped event/venue."""

    # Core event fields (populated by LLM extraction)
    name: str = Field(..., description="Event name/title")
    location: str = Field(..., description="Event location or venue name")
    date: str = Field(..., description="Event date or date range")
    rate: str = Field(..., description="Ticket price, 'Free', or 'Check booking page'")
    description: Optional[str] = Field(None, description="Short event summary")
    event_url: Optional[str] = Field(None, description="Direct URL to the event page")
    category: Optional[str] = Field(None, description="Event category e.g. Music, Sports, Tech")

    # Source tracking (added by scraper after extraction)
    source_site_id: Optional[str] = None
    source_site_name: Optional[str] = None
    source_url: Optional[str] = None  # FIXED: was source_page_url, scraper writes source_url

    # Extra metadata
    tags: Optional[List[str]] = None

    class Config:
        arbitrary_types_allowed = True
        json_schema_extra = {
            "example": {
                "name": "Summer Music Festival",
                "location": "Central Park, Kochi",
                "date": "15 Jun 2025",
                "rate": "₹500",
                "description": "Annual summer music festival featuring local artists.",
                "event_url": "https://allevents.in/kochi/summer-music-festival/123456",
                "category": "Music",
            }
        }


class VenueDB(BaseModel):
    """
    Extended model for MongoDB storage.
    Includes all Venue fields plus DB timestamp fields.
    """

    # Core fields
    name: str
    location: str
    date: str
    rate: str
    description: Optional[str] = None
    event_url: Optional[str] = None      # FIXED: Optional in both models now
    category: Optional[str] = None
    tags: Optional[List[str]] = None

    # Source tracking — field names match what scraper_utils.py writes
    source_site_id: Optional[str] = None
    source_site_name: Optional[str] = None
    source_url: Optional[str] = None     # FIXED: was source_page_url

    # DB-managed timestamps (ISO strings to avoid datetime serialization issues)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_scraped: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class ScrapingLog(BaseModel):
    """Audit log for a single scraping run."""

    site_id: str
    site_name: str
    started_at: str          # ISO format
    completed_at: Optional[str] = None
    status: str              # "success" | "failed" | "partial" | "no_data"
    venues_found: int = 0
    venues_created: int = 0
    venues_updated: int = 0
    pages_scraped: int = 0
    errors: int = 0
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None