from datetime import datetime
from typing import Optional

import polars as pl


def parse_ais_datetime(col: pl.Expr) -> pl.Expr:
    """Safely parse AIS timestamp strings to datetime"""
    return col.str.to_datetime(strict=False)


def time_difference_hours(start_col: pl.Expr, end_col: pl.Expr) -> pl.Expr:
    """Calculate time difference in hours between two datetime columns."""
    return (end_col - start_col).dt.total_seconds() / 3600 * (-1)


def normalize_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse arbitrary timestamp format safely."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
