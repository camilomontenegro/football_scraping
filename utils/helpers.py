from datetime import date
from typing import Optional, Tuple


_CONVERSIONS = {
    "understat":     (105.0,        68.0),         # 0–1 normalised
    "statsbomb":     (105.0 / 120,  68.0 / 80),    # 0–120 / 0–80
    "sofascore":     (105.0 / 100,  68.0 / 100),   # 0–100 percentage
    "whoscored":     (105.0 / 100,  68.0 / 100),   # 0–100 percentage
}


def normalize_coords(x: float, y: float, source: str) -> Tuple[float, float]:
    """Convert raw coordinates from *source* to metres on a 105×68 pitch.

    Args:
        x: Raw x coordinate in the source's native system.
        y: Raw y coordinate in the source's native system.
        source: One of 'understat', 'statsbomb', 'sofascore', 'whoscored'.

    Returns:
        (x_m, y_m) as floats, each rounded to 4 decimal places.

    Raises:
        ValueError: If *source* is not a recognised key.
    """
    if source not in _CONVERSIONS:
        raise ValueError(
            f"Unknown source '{source}'. Expected one of: {list(_CONVERSIONS)}"
        )
    mx, my = _CONVERSIONS[source]
    return round(x * mx, 4), round(y * my, 4)


def parse_date(value: Optional[str]) -> Optional[date]:
    """Parse an ISO-8601 date string ('YYYY-MM-DD') into a :class:`datetime.date`.

    Returns ``None`` if *value* is ``None`` or empty.
    """
    if not value:
        return None
    return date.fromisoformat(str(value)[:10])
