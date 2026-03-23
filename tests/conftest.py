import polars as pl
import pytest


@pytest.fixture
def sample_ais_data():
    """Sample AIS pings for testing."""
    return pl.DataFrame(
        {
            "mmsi": [123456789, 123456789, 987654321],
            "latitude": [40.7128, 40.7200, 51.5074],
            "longitude": [-74.0060, -74.0100, -0.1278],
            "base_date_time": ["2025-01-01 10:00:00", "2025-01-01 12:00:00", "2025-01-02 14:00:00"],
            "vessel_name": ["Ship A", "Ship A", "Ship B"],
            "imo": ["9123456", "9123456", "9654321"],
        }
    )


@pytest.fixture
def sample_port_data():
    """Sample port reference data."""
    return pl.DataFrame(
        {
            "LOCODE": ["USNYC", "GBLON", "ZASSB"],
            "Name": ["New York", "London", "Durban"],
            "lat": [40.7128, 51.5074, -29.8587],
            "lon": [-74.0060, -0.1278, 31.0218],
        }
    )
