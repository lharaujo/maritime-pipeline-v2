import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from src.settings import AppSettings, PathConfig
from src.transform import stitch_voyages


class TestVoyageStitching:
    """Test voyage stitching functionality."""

    @patch("src.transform.duckdb.connect")
    @patch("src.config.GOLD_DIR", Path("/tmp/test_gold"))
    @patch("src.config.SILVER_DIR", Path("/tmp/test_silver"))
    @patch("src.transform.modal.Dict")
    @patch("src.transform.voyage_work_queue")
    @patch("src.transform.VoyageEnricher")
    def test_stitch_voyages_success(
        self, mock_enricher, mock_queue, mock_dict, mock_duckdb_connect, tmp_path
    ):
        """Test successful voyage stitching."""
        # Mock the database connection and result
        mock_con = MagicMock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_con

        # Create mock DataFrame result
        mock_df = pl.DataFrame(
            {
                "mmsi": [123456789, 123456789],
                "imo": ["9123456", "9123456"],
                "vessel_name": ["Ship A", "Ship A"],
                "dep_locode": ["USNYC", "USNYC"],
                "arr_locode": ["GBLON", "ZASSB"],
                "dep_lat": [40.7128, 40.7128],
                "dep_lon": [-74.0060, -74.0060],
                "arr_lat": [51.5074, -29.8587],
                "arr_lon": [-0.1278, 31.0218],
                "arr_time": [datetime(2025, 1, 1, 12, 0), datetime(2025, 1, 2, 14, 0)],
                "dep_time": [datetime(2025, 1, 1, 10, 0), datetime(2025, 1, 2, 12, 0)],
            }
        )
        mock_con.execute.return_value.pl.return_value = mock_df

        # Create temporary directories
        gold_dir = tmp_path / "gold"
        gold_dir.mkdir()
        silver_dir = tmp_path / "silver"
        silver_dir.mkdir()

        # Mock Modal Dict async methods
        mock_dict_instance = mock_dict.from_name.return_value
        mock_dict_instance.contains.aio = AsyncMock(return_value=False)
        mock_dict_instance.get.aio = AsyncMock(return_value=None)

        # Mock Queue async methods
        mock_queue.clear.aio = AsyncMock()
        mock_queue.put_many.aio = AsyncMock()

        # Mock Enricher async generator
        async def mock_async_gen(*args, **kwargs):
            yield 1

        mock_enricher.return_value.consume_queue.map.aio.side_effect = mock_async_gen

        # Create custom settings with temp paths
        custom_paths = PathConfig(
            data_root=tmp_path,
            bronze=tmp_path / "bronze",
            silver=silver_dir,
            gold=gold_dir,
            reference=tmp_path / "reference",
        )
        settings = AppSettings(paths=custom_paths)

        with (
            patch("src.transform.GOLD_DIR", gold_dir),
            patch("src.transform.SILVER_DIR", silver_dir),
        ):
            asyncio.run(stitch_voyages(2025, 1, settings=settings))

            # Verify database was called
            mock_con.execute.assert_called_once()

            # Check that output file was created
            output_file = gold_dir / "voyages.parquet"
            assert output_file.exists()

    @patch("src.transform.duckdb.connect")
    @patch("src.config.GOLD_DIR", Path("/tmp/test_gold"))
    @patch("src.config.SILVER_DIR", Path("/tmp/test_silver"))
    def test_stitch_voyages_no_data(self, mock_duckdb_connect, tmp_path, *args):
        """Test handling when no voyages are found."""
        mock_con = MagicMock()
        mock_duckdb_connect.return_value.__enter__.return_value = mock_con

        # Return empty DataFrame
        mock_con.execute.return_value.pl.return_value = pl.DataFrame()

        # Create custom settings with temp paths
        custom_paths = PathConfig(
            data_root=tmp_path,
            bronze=tmp_path / "bronze",
            silver=tmp_path / "silver",
            gold=tmp_path / "gold",
            reference=tmp_path / "reference",
        )
        settings = AppSettings(paths=custom_paths)

        asyncio.run(stitch_voyages(2025, 1, settings=settings))

        # Should not create output file when no data
        # (This test mainly checks that no exception is raised)

    @patch("src.config.GOLD_DIR", Path("/tmp/test_gold"))
    @patch("src.config.SILVER_DIR", Path("/tmp/test_silver"))
    @patch("src.transform.duckdb.connect")
    def test_stitch_voyages_database_error(self, mock_duckdb_connect, tmp_path, *args):
        """Test handling of database errors."""
        mock_duckdb_connect.return_value.__enter__.side_effect = Exception("DB error")

        # Create custom settings with temp paths
        custom_paths = PathConfig(
            data_root=tmp_path,
            bronze=tmp_path / "bronze",
            silver=tmp_path / "silver",
            gold=tmp_path / "gold",
            reference=tmp_path / "reference",
        )
        settings = AppSettings(paths=custom_paths)

        with pytest.raises(Exception):
            asyncio.run(stitch_voyages(2025, 1, settings=settings))
