from typing import Tuple

import numpy as np
import polars as pl


def to_radians(
    df: pl.DataFrame, lat_col: str = "latitude", lon_col: str = "longitude"
) -> np.ndarray:
    """Convert lat/lon DataFrame columns to radian coordinates for KDTree."""
    return np.deg2rad(df.select([lat_col, lon_col]).to_numpy())


def to_degrees(radians: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """Convert radian coordinates back to lat/lon."""
    coords = np.rad2deg(radians)
    return coords[:, 0], coords[:, 1]  # lat, lon


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km using Haversine formula."""
    from math import asin, cos, radians, sin, sqrt

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * 6371 * asin(sqrt(a))
