import numpy as np
import polars as pl

from src.geospacial import haversine_distance, to_degrees, to_radians


def test_to_radians():
    """Test conversion of lat/lon dataframe to radians."""
    df = pl.DataFrame({"latitude": [0.0, 90.0, 180.0], "longitude": [0.0, 180.0, 360.0]})
    rads = to_radians(df)

    expected = np.deg2rad(df.select(["latitude", "longitude"]).to_numpy())
    np.testing.assert_array_equal(rads, expected)


def test_to_degrees():
    """Test conversion of radians array back to lat/lon degrees."""
    # 0, pi/2, pi
    rads = np.array([[0.0, 0.0], [np.pi / 2, np.pi], [np.pi, 2 * np.pi]])
    lat, lon = to_degrees(rads)

    expected_lat = [0.0, 90.0, 180.0]
    expected_lon = [0.0, 180.0, 360.0]

    np.testing.assert_allclose(lat, expected_lat)
    np.testing.assert_allclose(lon, expected_lon)


def test_haversine_distance():
    """Test Haversine distance calculation."""
    # NYC (40.7128, -74.0060) to London (51.5074, -0.1278)
    # Expected distance is approximately 5570 km
    dist = haversine_distance(40.7128, -74.0060, 51.5074, -0.1278)
    assert 5500 < dist < 5650


def test_haversine_zero_distance():
    """Test distance between identical points is zero."""
    dist = haversine_distance(0, 0, 0, 0)
    assert dist == 0.0
