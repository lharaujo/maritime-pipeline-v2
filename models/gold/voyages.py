import pandas as pd
import searoute
import json
import warnings
import logging


def model(dbt, con):
    dbt.config(
        materialized="incremental",
        unique_key=["mmsi", "dep_time", "arr_time"],
    )

    # Suppress noisy multiprocessing warnings common in CI/CD environments
    warnings.filterwarnings("ignore", module="multiprocessing.resource_tracker")

    # 1. Incremental Logic
    # If the table exists, find the latest arrival time to only process new voyages.
    # We look back 14 days to ensure we catch the start of voyages that just completed.
    cutoff_filter = None
    if dbt.is_incremental:
        try:
            # 'dbt.this' refers to the current table (main_gold.voyages)
            # We use the existing connection to query it.
            target_name = str(dbt.this)
            res = con.execute(f"SELECT MAX(arr_time) FROM {target_name}").fetchone()
            if res and res[0]:
                cutoff_date = res[0] - pd.Timedelta(days=14)
                cutoff_filter = f"dep_time >= '{cutoff_date}'"
        except Exception:
            # Table likely doesn't exist yet, run full load
            pass

    # 2. Load Staging Data (Filtered)
    # We execute SQL on the connection to filter *before* loading data into memory/Polars
    source_rel = dbt.ref("stg_ais")
    if cutoff_filter:
        source_rel = source_rel.filter(cutoff_filter)

    # We avoid pulling data into Polars/Python memory here.
    # Instead, we pass the relation directly to the next SQL step.

    # 3. Window Functions (Stitching)
    # We identify port changes using SQL on the Polars frame via DuckDB
    q = """
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

    # Execute SQL directly on the source relation (Pushdown to DuckDB)
    voyages = source_rel.query("source_view", q).df()

    if voyages.empty:
        return voyages

    # 4. Enrichment (SeaRoute)
    # Optimized: Use list comprehension + zip instead of .apply(axis=1) to save memory
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
        except Exception as e:
            logging.error(f"Route calculation failed for {origin} -> {dest}: {e}")
            geometries.append(None)
            distances.append(0)

    voyages["route_geometry"] = geometries
    voyages["distance_nm"] = distances

    voyages["duration_hrs"] = (
        voyages["arr_time"] - voyages["dep_time"]
    ).dt.total_seconds() / 3600

    # Final safety filter to ensure no invalid durations persist
    voyages = voyages[voyages["duration_hrs"] > 0]

    return voyages
