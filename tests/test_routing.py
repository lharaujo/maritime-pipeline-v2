import json
from unittest.mock import patch

import pytest

from src.voyage_enrichment import (
    call_searoute,
    generate_cache_key,
    get_path_distance,
    get_path_duration,
    get_sea_path,
)


@pytest.fixture
def sample_row():
    return {
        "dep_lat": 29.88,
        "dep_lon": -89.96,
        "dep_locode": "USCHL",
        "arr_lat": 29.86,
        "arr_lon": -89.92,
        "arr_locode": "USMXP",
    }


# --- Tests for Cache Key ---


def test_generate_cache_key(sample_row):
    key = generate_cache_key(sample_row)
    assert key == "route_USCHL_USMXP"


def test_generate_cache_key_missing(sample_row):
    row = sample_row.copy()
    del row["dep_locode"]
    key = generate_cache_key(row)
    assert key == "route_UNK_USMXP"


# --- Tests for call_searoute ---


@patch("src.voyage_enrichment.searoute.searoute")
def test_call_searoute_success(mock_sr, sample_row):
    """Test that call_searoute calls the library correctly and returns data."""
    mock_sr.return_value = {
        "type": "Feature",
        "geometry": {"coordinates": [[-89.96, 29.88], [-89.92, 29.86]]},
        "properties": {"length": 5.0},
    }
    result = call_searoute(sample_row)

    assert result["properties"]["length"] == 5.0
    # Verify it was called with nautical miles
    args, kwargs = mock_sr.call_args
    assert kwargs.get("units") == "nm"


@patch("src.voyage_enrichment.searoute.searoute")
def test_call_searoute_fallback(mock_sr, sample_row):
    """Test that we fallback to a straight line when searoute fails."""
    mock_sr.side_effect = Exception("Point on land")

    result = call_searoute(sample_row)

    assert result is not None
    assert result["geometry"]["type"] == "LineString"
    # Should contain exactly 2 points (Start -> End)
    assert len(result["geometry"]["coordinates"]) == 2
    assert result["geometry"]["coordinates"][0] == [sample_row["dep_lon"], sample_row["dep_lat"]]
    # Should have calculated distance in 'properties' -> 'length'
    assert result["properties"]["length"] > 0
    assert result["properties"]["units"] == "nm"


# --- Tests for Data Extractors ---


def test_get_path_distance(sample_row):
    route = {"properties": {"length": 123.45}}
    dist = get_path_distance(sample_row, route)
    assert dist == 123.45


def test_get_path_distance_none(sample_row):
    assert get_path_distance(sample_row, None) is None


def test_get_path_duration(sample_row):
    # Distance 100 nm, Speed 10 kts -> 10 hours
    route = {"properties": {"length": 100.0}}
    duration = get_path_duration(sample_row, route, avg_speed=10.0)
    assert duration == 10.0


def test_get_sea_path_serialization(sample_row):
    route = {"geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}}
    path_str = get_sea_path(sample_row, route)

    assert isinstance(path_str, str)
    data = json.loads(path_str)
    assert data["type"] == "LineString"
    assert len(data["coordinates"]) == 2
