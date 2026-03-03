"""
utils/data_utils.py

Key fixes:
  - Sentinel value changed from "Not Found" → "Not Available" to match the
    scraper_utils.py filter checks (mismatched sentinels let junk data slip through).
  - save_venues_to_json no longer filters by Venue.model_fields — that was stripping
    source_site_id, created_at, updated_at, last_scraped, etc. added after extraction.
    Now saves the full dict as-is, which is what you actually want for backups.
  - is_complete_venue updated to check "Not Available" sentinel consistently.
  - clean_venue_data handles the "error" boolean field Crawl4AI sometimes injects.
"""

import json
import os
from typing import List, Set, Any


# Single source of truth for the "missing value" sentinel
# Must match the LLM instruction ("Use 'Not Available' for missing values")
# and the filter checks in scraper_utils.py
MISSING_SENTINEL = "Not Available"


def is_duplicate_venue(venue_name: str, seen_names: Set[str]) -> bool:
    """Return True if this name has already been seen (case-sensitive)."""
    if not venue_name:
        return True
    return venue_name in seen_names


def is_complete_venue(venue: dict, required_keys: List[str]) -> bool:
    """
    Return True if all required_keys have a non-empty, non-sentinel value.
    """
    return all(
        key in venue
        and venue[key]
        and venue[key] not in (None, "", MISSING_SENTINEL, "Not Found")
        for key in required_keys
    )


def clean_venue_data(venue: dict) -> dict:
    """
    Normalize a raw LLM-extracted venue dict:
      - Removes the 'error' boolean Crawl4AI sometimes injects
      - Coerces None / empty / non-string values to MISSING_SENTINEL
      - Strips whitespace from all string fields
      - Keeps event_url as None (not sentinel) when absent — it's optional
    """
    # Crawl4AI sometimes adds {"error": false} — drop it
    venue.pop("error", None)

    # Fields that must be strings (use sentinel if missing)
    string_fields = ["name", "location", "date", "rate", "category", "description"]

    for key in string_fields:
        value = venue.get(key)
        if value is None or value == "" or not isinstance(value, str):
            venue[key] = MISSING_SENTINEL
        else:
            cleaned = value.strip()
            venue[key] = cleaned if cleaned else MISSING_SENTINEL

    # event_url: keep None rather than sentinel — it's truly optional
    raw_url = venue.get("event_url")
    if raw_url and isinstance(raw_url, str):
        stripped = raw_url.strip()
        venue["event_url"] = stripped if stripped else None
    else:
        venue["event_url"] = None

    return venue


def save_venues_to_json(venues: List[dict], filename: str = "venues_backup.json") -> str:
    """
    Save the full venue dicts to a JSON backup file.

    FIXED: No longer filters by Venue.model_fields — doing so stripped all the
    metadata fields added after extraction (source_site_id, created_at, etc.).
    The backup should contain the complete record as stored in MongoDB.
    """
    if not venues:
        print("⚠️ No venues to save.")
        return filename

    # Ensure parent directory exists
    parent_dir = os.path.dirname(filename)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(venues, f, ensure_ascii=False, indent=4, default=str)

    print(f"✅ Saved {len(venues)} venues → '{filename}'")
    return filename