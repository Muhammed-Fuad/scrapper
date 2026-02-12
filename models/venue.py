# models/venue.py
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class Venue(BaseModel):
    """Represents the data structure of a Venue."""
    
    # Required fields for LLM extraction
    name: str = Field(..., description="Event name/title")
    location: str = Field(..., description="Event location/venue")
    date: str = Field(..., description="Event date")
    
    # description: str = Field(..., description="Brief description of the event")
    event_url: Optional[str] = Field(None, description="Direct URL to event page")
    
    # Optional fields
    rate: str = Field(..., description="Ticket price or entry fee")
    
    # Source tracking (added after extraction)
    source_site_id: Optional[str] = None
    source_site_name: Optional[str] = None
    source_page_url: Optional[str] = None
    
    # Metadata (don't include in LLM schema)
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    
    class Config:
        # Disable validation for datetime to avoid ForwardRef issues
        arbitrary_types_allowed = True
        json_schema_extra = {
            "example": {
                "name": "Summer Music Festival",
                "location": "Central Park, Kochi",
                "date": "2024-06-15",
                "rate": "₹500-₹1000",
                # "description": "Annual summer music festival featuring local artists.",
                "event_url": "https://allevents.in/kochi/summer-music-festival/123456"
            }
        }


class VenueDB(BaseModel):
    """Extended model for database storage (includes datetime fields)."""
    
    # All Venue fields
    name: str
    location: str
    date: str
    rate: str
    # description: str
    event_url: Optional[str] = None
    source_site_id: Optional[str] = None
    source_site_name: Optional[str] = None
    source_page_url: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    
    # Database-only fields (stored as ISO strings to avoid ForwardRef)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    last_scraped: Optional[str] = None
    
    class Config:
        arbitrary_types_allowed = True


class ScrapingLog(BaseModel):
    """Log of scraping operations."""
    
    site_id: str
    site_name: str
    started_at: str  # ISO format string
    completed_at: Optional[str] = None
    status: str  # "success", "failed", "partial"
    venues_found: int = 0
    venues_created: int = 0
    venues_updated: int = 0
    errors: int = 0
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None