def get_venue_schema_for_llm():
    """
    JSON schema for LLM event extraction.

    Notes:
    - description is re-enabled; it gives the LLM context to classify category correctly
    - event_url nullable since not all listing pages show individual URLs
    - All string fields default gracefully to "Not Available" via instruction
    """
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "name":        {"type": "string", "description": "Full name of the event"},
                "location":    {"type": "string", "description": "Venue or city where event takes place"},
                "date":        {"type": "string", "description": "Event date or date range, e.g. '15 Mar 2025' or 'Mar 15 - Mar 17 2025'"},
                "rate":        {"type": "string", "description": "Ticket price or 'Free'. Use 'Not Available' if unknown."},
                "description": {"type": "string", "description": "Short summary of the event (1-2 sentences max)"},
                "event_url":   {"type": ["string", "null"], "description": "Direct URL to the event page if available"},
                "category":    {"type": "string", "description": "Event category e.g. Music, Sports, Food, Art, Tech, Comedy, Conference, Workshop"},
            },
            "required": ["name", "location", "date", "rate", "category"],
        },
    }