# utils/data_utils.py
import json
from typing import List, Set, Dict, Any
from models.venue import Venue


def is_duplicate_venue(venue_name: str, seen_names: Set[str]) -> bool:
    """Check if venue name already exists in the set."""
    if not venue_name:
        return True
    return venue_name in seen_names


def is_complete_venue(venue: dict, required_keys: List[str]) -> bool:
    """Check if venue has all required fields with values."""
    return all(
        key in venue 
        and venue[key] 
        and venue[key] not in [None, "", "Not Found"]
        for key in required_keys
    )


def clean_venue_data(venue: dict) -> dict:
    """Clean and standardize venue data with proper None handling."""
    
    # Remove 'error' field if False
    if venue.get("error") is False:
        venue.pop("error", None)
    
    # Ensure all required fields exist and are strings
    required_keys = ["name", "location", "date", "rate", "event_url"]
    
    for key in required_keys:
        value = venue.get(key)
        
        # Convert None, empty, or invalid values to "Not Found"
        if value is None or value == "" or not isinstance(value, str):
            venue[key] = "Not Found"
        else:
            # Clean the string value
            venue[key] = str(value).strip()
            
            # Replace empty strings after stripping
            if not venue[key]:
                venue[key] = "Not Found"
    
    # Clean event_url if present
    if 'event_url' in venue:
        if venue['event_url'] is None or venue['event_url'] == "":
            venue['event_url'] = None
        elif isinstance(venue['event_url'], str):
            venue['event_url'] = venue['event_url'].strip()
    
    return venue


def save_venues_to_json(venues: List[dict], filename: str = "venues_backup.json"):
    """
    Save venue data as a JSON array (backup).
    """
    if not venues:
        print("⚠️ No venues to save.")
        return

    # Extract all field names from Venue model
    fieldnames = list(Venue.model_fields.keys())

    # Clean each venue dict to include only valid fields
    cleaned_venues = []
    for v in venues:
        record = {}
        for f in fieldnames:
            value = v.get(f, "")
            # Ensure None values are handled
            record[f] = value if value is not None else ""
        cleaned_venues.append(record)

    # Save as a pretty JSON array
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cleaned_venues, f, ensure_ascii=False, indent=4)

    print(f"✅ Saved {len(cleaned_venues)} venues to JSON file → '{filename}'")
    return filename