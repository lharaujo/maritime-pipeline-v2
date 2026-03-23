import json
from typing import Any, Dict, Optional

import searoute

from src.geospacial import haversine_distance


def generate_cache_key(voyage: Dict[str, Any]) -> str:
    """
    Creates a unique, deterministic key for the route.
    Using LOCODEs is better than coordinates to ensure cache hits.
    """
    dep = voyage.get("dep_locode", "UNK")
    arr = voyage.get("arr_locode", "UNK")
    return f"route_{dep}_{arr}"


def call_searoute(voyage: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Calculates the shortest sea path using the searoute library.
    Falls back to a straight line (Great Circle) if sea routing fails or returns invalid geometry.
    """
    origin = [voyage["dep_lon"], voyage["dep_lat"]]
    dest = [voyage["arr_lon"], voyage["arr_lat"]]

    try:
        # Returns a GeoJSON Feature
        route = searoute.searoute(origin, dest, units="nm")

        # Validation: Ensure we have a valid line (at least 2 points)
        coords = route.get("geometry", {}).get("coordinates", [])
        if not coords or len(coords) < 2:
            raise ValueError("Invalid route geometry returned by searoute")

        return route
    except Exception:
        # Fallback: Straight Line (Great Circle)
        dist_km = haversine_distance(
            voyage["dep_lat"], voyage["dep_lon"], voyage["arr_lat"], voyage["arr_lon"]
        )
        dist_nm = dist_km / 1.852

        return {
            "type": "Feature",
            "properties": {"length": dist_nm, "units": "nm"},
            "geometry": {"type": "LineString", "coordinates": [origin, dest]},
        }


def get_sea_path(voyage: Dict[str, Any], route_data: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extracts the GeoJSON geometry as a string for storage."""
    if not route_data:
        return None
    return json.dumps(route_data.get("geometry"))


def get_path_distance(
    voyage: Dict[str, Any], route_data: Optional[Dict[str, Any]]
) -> Optional[float]:
    """Extracts distance in Nautical Miles."""
    if not route_data:
        return None
    # searoute puts distance in properties
    return route_data.get("properties", {}).get("length", 0.0)


def get_path_duration(
    voyage: Dict[str, Any], route_data: Optional[Dict[str, Any]], avg_speed: float = 12.5
) -> Optional[float]:
    """
    Calculates expected duration in hours based on distance and average speed.
    $Time = \frac{Distance}{Speed}$
    """
    dist = get_path_distance(voyage, route_data)
    if not dist or dist == 0:
        return None
    return round(dist / avg_speed, 2)
