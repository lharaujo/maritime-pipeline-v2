import polars as pl
import pytest

from src.extract import transform_coords_polars


def test_coordinate_transformation():
    """Test that UN/LOCODE strings are correctly converted to decimal degrees."""
    # 1. Create a dummy DataFrame mimicking the scraped Wikipedia/UNECE data
    # Format: DDMM[NS] DDDMM[EW]
    data = {
        "Coordinates": [
            "5130N 00007W",  # London
            "4043N 07400W",  # New York
            "3355S 01825E",  # Cape Town
            "3541N 13946E",  # Tokyo
        ]
    }
    df = pl.DataFrame(data)

    # 2. Run the transformation
    result_df = transform_coords_polars(df)

    # 3. Assertions (Expected Decimal Degrees)
    # London: 51 + 30/60 = 51.5 | -0.1166 (approx)
    london = result_df.row(0, named=True)
    assert pytest.approx(london["lat"], 0.1) == 51.5
    assert london["lon"] < 0  # Should be West (negative)

    # Cape Town: Should be negative Latitude (South)
    cape_town = result_df.row(2, named=True)
    assert cape_town["lat"] < 0
    assert cape_town["lon"] > 0


def test_coordinate_transformation_edge_cases():
    """Test coordinate transformation with edge cases and invalid data."""
    # Test with invalid coordinates
    data = {
        "Coordinates": [
            "",  # Empty string
            "9999N 99999W",  # Invalid values
            "5130N",  # Missing longitude
            "5130N 00007",  # Missing hemisphere
        ]
    }
    df = pl.DataFrame(data)

    # Should handle errors gracefully and return nulls
    result_df = transform_coords_polars(df)

    # Check that we have the expected columns
    assert "lat" in result_df.columns
    assert "lon" in result_df.columns

    # Check that invalid coordinates result in null values
    # (The function should filter out nulls, so result should be empty or have nulls)
    assert (
        result_df.is_empty()
        or result_df.select(["lat", "lon"]).null_count().sum_horizontal().sum() > 0
    )


def test_coordinate_transformation_precision():
    """Test that coordinate transformation maintains proper precision."""
    data = {
        "Coordinates": [
            "5130N 00007W",  # London with minutes
            "4043N 07400W",  # New York exact degrees
        ]
    }
    df = pl.DataFrame(data)
    result_df = transform_coords_polars(df)

    # Check precision - should be float64
    assert result_df["lat"].dtype == pl.Float64
    assert result_df["lon"].dtype == pl.Float64

    # Check specific values
    london = result_df.row(0, named=True)
    nyc = result_df.row(1, named=True)

    # London: 51°30'N = 51.5, 0°07'W = -0.1167
    assert abs(london["lat"] - 51.5) < 0.01
    assert abs(london["lon"] - (-0.1167)) < 0.01

    # New York: 40°43'N = 40.7167, 74°00'W = -74.0
    assert abs(nyc["lat"] - 40.7167) < 0.01
    assert abs(nyc["lon"] - (-74.0)) < 0.01
