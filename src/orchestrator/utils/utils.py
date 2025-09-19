import json
from typing import Optional, Dict, Any


def _json_or_none(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Safely parse a JSON string. Returns a dict if parsing succeeds,
    otherwise returns None.

    Args:
        raw: JSON string (or None).

    Returns:
        Parsed dict if valid JSON, else None.
    """
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None
