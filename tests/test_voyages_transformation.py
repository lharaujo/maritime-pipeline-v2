import json
from unittest.mock import patch

import duckdb
import pandas as pd
import searoute


def test_voyage_stitching_sql():
    """
    Verifies that the SQL logic correctly stitches voyages:
    1. Identifies consecutive port visits (removes noise/duplicates).
    2. Links Departure (Row N) to Arrival (Row N+1).
    3. Filters out unfinished voyages (NULL arrival).
    """
    # Sample Input: Vessel moves Port A -> Port A (duplicate) -> Port B -> Port C
    mock_ais_data = pd.DataFrame(
        {
            "mmsi": [123, 123, 123, 123],
            "imo": ["999", "999", "999", "999"],
            "vessel_name": ["Vessel A", "Vessel A", "Vessel A", "Vessel A"],
            "port_locode": ["PORTA", "PORTA", "PORTB", "PORTC"],
            "lat": [10.0, 10.0, 20.0, 30.0],
            "lon": [10.0, 10.0, 20.0, 30.0],
            "dep_time": pd.to_datetime(
                [
                    "2025-01-01 10:00:00",
                    "2025-01-01 11:00:00",  # Duplicate visit to PORTA
                    "2025-01-02 10:00:00",  # Move to PORTB
                    "2025-01-03 10:00:00",  # Move to PORTC
                ]
            ),
        }
    )

    con = duckdb.connect()
    con.register("source_view", mock_ais_data)

    # SQL Logic extracted from models/gold/voyages.py
    query = """
    WITH unique_port_visits AS (
            SELECT
                *,
                LAG(port_locode) OVER (PARTITION BY mmsi ORDER BY dep_time) as prev_port
            FROM source_view
    ),
    legs AS (
        SELECT
            mmsi, imo, vessel_name,
            port_locode as dep_locode, lat as dep_lat, lon as dep_lon, dep_time,
            LEAD(port_locode) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_locode,
            LEAD(lat) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_lat,
            LEAD(lon) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_lon,
            LEAD(dep_time) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_time
        FROM unique_port_visits
        WHERE prev_port IS NULL OR port_locode != prev_port
    )
    SELECT * FROM legs
    WHERE arr_locode IS NOT NULL AND arr_time > dep_time
    """

    result = con.execute(query).df()

    # Expectations:
    # 1. PORTA -> PORTB
    # 2. PORTB -> PORTC
    # Duplicate PORTA should be ignored/merged.
    # Final PORTC has no destination, so it is filtered out.

    assert len(result) == 2

    # Check Voyage 1
    v1 = result.iloc[0]
    assert v1["dep_locode"] == "PORTA"
    assert v1["arr_locode"] == "PORTB"

    # Check Voyage 2
    v2 = result.iloc[1]
    assert v2["dep_locode"] == "PORTB"
    assert v2["arr_locode"] == "PORTC"


@patch("searoute.searoute")
def test_enrichment_logic(mock_searoute):
    """
    Verifies the Python logic for route calculation and duration.
    Mimics the post-SQL processing in models/gold/voyages.py.
    """
    # Mock Input (output of SQL step)
    voyages = pd.DataFrame(
        {
            "dep_lon": [0.0],
            "dep_lat": [0.0],
            "arr_lon": [10.0],
            "arr_lat": [0.0],
            "dep_time": pd.to_datetime(["2025-01-01 10:00:00"]),
            "arr_time": pd.to_datetime(["2025-01-01 20:00:00"]),  # 10 hours duration
        }
    )

    # Mock searoute return
    mock_searoute.return_value = {
        "geometry": {"coordinates": [[0, 0], [5, 0], [10, 0]]},
        "properties": {"length": 100.0},
    }

    # --- LOGIC UNDER TEST (from voyages.py) ---
    geometries = []
    distances = []

    for d_lon, d_lat, a_lon, a_lat in zip(
        voyages["dep_lon"], voyages["dep_lat"], voyages["arr_lon"], voyages["arr_lat"]
    ):
        try:
            origin = [d_lon, d_lat]
            dest = [a_lon, a_lat]
            route = searoute.searoute(origin, dest, units="nm")
            geometries.append(json.dumps(route.get("geometry")))
            distances.append(route.get("properties", {}).get("length", 0))
        except Exception:
            geometries.append(None)
            distances.append(0)

    voyages["route_geometry"] = geometries
    voyages["distance_nm"] = distances

    voyages["duration_hrs"] = (
        voyages["arr_time"] - voyages["dep_time"]
    ).dt.total_seconds() / 3600

    # Final safety filter
    voyages = voyages[voyages["duration_hrs"] > 0]
    # ------------------------------------------

    assert len(voyages) == 1
    row = voyages.iloc[0]

    assert row["duration_hrs"] == 10.0
    assert row["distance_nm"] == 100.0
    assert "coordinates" in row["route_geometry"]

    # Verify searoute was called with correct coords
    mock_searoute.assert_called_once()
    args, _ = mock_searoute.call_args
    assert args[0] == [0.0, 0.0]  # Origin
    assert args[1] == [10.0, 0.0]  # Dest
