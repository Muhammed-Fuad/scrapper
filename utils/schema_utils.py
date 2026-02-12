# utils/schema_utils.py

def get_venue_schema_for_llm():
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "location": {"type": "string"},
                "date": {"type": "string"},
                "rate": {"type": "string"},
                # "description": {"type": "string"},
                "event_url": {"type": ["string", "null"]},
                "category": {"type": "string"},
            },
            "required": ["name", "location", "date", "event_url", "rate" ],
        },
    }
