import pandas as pd
import searoute
import json


def model(dbt, con):
    dbt.config(materialized="incremental", unique_key=["mmsi", "dep_time", "arr_time"])

    # 1. Load Staging Data
    # We reference the silver view we just created
    df = dbt.ref("stg_ais").pl()

    # 2. Window Functions (Stitching)
    # We identify port changes using SQL on the Polars frame via DuckDB
    q = """
    WITH unique_port_visits AS (
            SELECT
                *,
                LAG(port_locode) OVER (PARTITION BY mmsi ORDER BY dep_time) as prev_port
            FROM df_view
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
    WHERE arr_locode IS NOT NULL
    """

    # Register Polars DF as view for DuckDB
    con.register("df_view", df)
    voyages = con.execute(q).fetch_df()  # Pandas for searoute compatibility

    if voyages.empty:
        return voyages

    # 3. Enrichment (SeaRoute)
    def get_route(row):
        try:
            origin = [row["dep_lon"], row["dep_lat"]]
            dest = [row["arr_lon"], row["arr_lat"]]
            route = searoute.searoute(origin, dest, units="nm")
            geometry = json.dumps(route.get("geometry"))
            dist = route.get("properties", {}).get("length", 0)
            return pd.Series([geometry, dist])
        except Exception:
            return pd.Series([None, 0])

    enrichment = voyages.apply(get_route, axis=1)
    enrichment.columns = ["route_geometry", "distance_nm"]

    final_df = pd.concat([voyages, enrichment], axis=1)
    final_df["duration_hrs"] = (
        final_df["arr_time"] - final_df["dep_time"]
    ).dt.total_seconds() / 3600

    return final_df
