# src/ingest_motherduck.py
import os
import argparse
import io
import zstandard as zstd
import polars as pl
import numpy as np
import duckdb
import requests
from datetime import datetime, timedelta
from scipy.spatial import KDTree

# We import simple utils, ignoring the Modal decorators in the original file
# effectively
from src.geospacial import to_radians

# --- CONFIG ---
MD_CONN_STR = "md:"
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
PORTS_TABLE = "reference.ports"
RAW_AIS_TABLE = "silver.raw_ais_pings"


def get_db_connection():
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not found in environment variables")
    print(f"🔌 Connecting to MotherDuck with DuckDB {duckdb.__version__}...")
    con = duckdb.connect(MD_CONN_STR)

    con.execute("CREATE DATABASE IF NOT EXISTS my_voyage_db")
    con.execute("USE my_voyage_db")
    return con


def ensure_ports_exist(con):
    """Checks if ports exist in MotherDuck; if not, bootstraps them."""
    con.execute("CREATE SCHEMA IF NOT EXISTS reference")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    try:
        count = con.sql(f"SELECT COUNT(*) FROM {PORTS_TABLE}").fetchone()[0]
        print(f"✅ Found {count} ports in MotherDuck.")
    except duckdb.CatalogException:
        print("⚠️ Ports table not found. Bootstrapping...")
        # We need to adapt the scraping logic to return a DataFrame instead of saving
        # parquet. For simplicity in this refactor, let's assume we scrape and load
        # to MD. In a real scenario, you'd call your scrape function here.
        # Placeholder for the scrape logic:
        print("⏳ Scraping ports (Simulated call to existing logic)...")
        # NOTE: You would port the logic from src/extract.py here,
        # but modify it to return a Polars DF instead of writing parquet.
        # For now, let's assume the ports table is populated or we fail gracefully.
        raise RuntimeError(
            "Ports table missing. Please run initial bootstrap locally or port " "scraping logic."
        )


def load_ports_for_kdtree(con):
    """Loads ports into memory for KDTree filtering."""
    df = con.sql(f"SELECT LOCODE, Name, lat, lon FROM {PORTS_TABLE}").pl()

    port_locodes = df["LOCODE"].to_numpy()
    port_names = df["Name"].to_numpy()
    # Convert to radians for distance calc
    coords = np.deg2rad(df[["lat", "lon"]].to_numpy())
    tree = KDTree(coords)
    return tree, port_locodes, port_names


def fetch_and_filter_ais(year, month, day, tree, port_locodes, port_names):
    date_str = f"{year}-{month:02d}-{day:02d}"
    url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/" f"ais-{date_str}.csv.zst"
    print(f"⬇️ Streaming AIS data from {url}...")

    with requests.Session() as s:
        resp = s.get(url, stream=True)
        if resp.status_code != 200:
            print(f"⚠️ Failed to fetch {url}: {resp.status_code}")
            return None

        # Decompress stream in memory
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(resp.raw) as reader:
            # We read in chunks to avoid blowing up 7GB RAM
            # However, Polars read_csv is efficient. We'll read into a buffer.
            # Warning: A full day uncompressed can be 5GB+.
            # Ideally we process line by line, but for speed we use Polars.
            csv_buffer = io.BytesIO(reader.read())

    print("📦 Data downloaded. Parsing CSV...")

    # Read with Polars (Zero local disk write)
    try:
        ais = pl.read_csv(
            csv_buffer,
            columns=[
                "mmsi",
                "latitude",
                "longitude",
                "base_date_time",
                "vessel_name",
                "imo",
            ],
            ignore_errors=True,
        )
    except Exception as e:
        print(f"❌ Failed to parse CSV: {e}")
        return None

    # Filter Nulls
    ais = ais.filter(pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())

    if ais.height == 0:
        return None

    print(f"🔎 Filtering {ais.height} pings against {len(port_locodes)} ports...")

    # Coordinate Transform & KDTree Query
    ship_coords = to_radians(ais)  # Uses your existing util
    dist, idx = tree.query(ship_coords, k=1)

    # 5km radius filter (approx 0.00078 radians, but let's use the explicit dist logic)
    # Earth radius ~6371km. 5km / 6371 = 0.000784
    mask = (dist * 6371.0) <= 5.0

    if not mask.any():
        print("No pings near ports found.")
        return None

    valid_idx = idx[mask]
    filtered_ais = ais.filter(mask)

    # Enrich with Port Info
    filtered_ais = filtered_ais.with_columns(
        [
            pl.Series("port_locode", port_locodes[valid_idx]),
            pl.Series(
                "dep_time",
                filtered_ais["base_date_time"].str.to_datetime(strict=False),
            ),  # Renaming base_date_time
        ]
    ).select(["mmsi", "imo", "vessel_name", "latitude", "longitude", "dep_time", "port_locode"])

    print(f"✅ Retained {filtered_ais.height} relevant pings.")
    return filtered_ais


def process_date(con, target_date, tree, locodes, names):
    """Ingests a single day of data."""
    print(f"🚀 Starting Ingest for {target_date.date()}")
    try:
        df_filtered = fetch_and_filter_ais(
            target_date.year, target_date.month, target_date.day, tree, locodes, names
        )

        if df_filtered is not None and not df_filtered.is_empty():
            print("📤 Uploading to MotherDuck...")
            # Create table if not exists
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RAW_AIS_TABLE} (
                    mmsi BIGINT,
                    imo VARCHAR,
                    vessel_name VARCHAR,
                    latitude DOUBLE,
                    longitude DOUBLE,
                    dep_time TIMESTAMP,
                    port_locode VARCHAR,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert Data
            con.sql(f"INSERT INTO {RAW_AIS_TABLE} SELECT *, CURRENT_TIMESTAMP FROM df_filtered")
            print(f"🎉 Ingestion Complete for {target_date.date()}.")
        else:
            print(f"⚠️ No data to ingest for {target_date.date()}.")
    except Exception as e:
        print(f"❌ Failed to process {target_date.date()}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ingest AIS Data to MotherDuck")
    parser.add_argument("--year", type=int, help="Year (YYYY)")
    parser.add_argument("--month", type=int, help="Month (1-12)")
    parser.add_argument("--day", type=int, help="Day (1-31)")
    args = parser.parse_args()

    con = get_db_connection()
    ensure_ports_exist(con)
    tree, locodes, names = load_ports_for_kdtree(con)

    # Date Logic
    if args.year and args.month and args.day:
        start_date = datetime(args.year, args.month, args.day)
        end_date = start_date
    elif args.year and args.month:
        start_date = datetime(args.year, args.month, 1)
        # Logic to find last day of month
        if args.month == 12:
            end_date = datetime(args.year, 12, 31)
        else:
            end_date = datetime(args.year, args.month + 1, 1) - timedelta(days=1)
    elif args.year:
        start_date = datetime(args.year, 1, 1)
        end_date = datetime(args.year, 12, 31)
    else:
        # Default: Yesterday
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = start_date

    # Prevent future dates
    if end_date > datetime.utcnow():
        end_date = datetime.utcnow() - timedelta(days=1)

    print(f"📅 Running pipeline from {start_date.date()} to {end_date.date()}")

    current = start_date
    while current <= end_date:
        process_date(con, current, tree, locodes, names)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
