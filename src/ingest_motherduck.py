# src/ingest_motherduck.py
import os
import argparse
import logging
import time
import io
import pickle
from pathlib import Path
import zstandard as zstd
import polars as pl
import numpy as np
import duckdb
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.spatial import KDTree

# We import simple utils, ignoring the Modal decorators in the original file
# effectively
from src.geospacial import to_radians
from src.constants import PORT_STATUS_CODES, PORT_FUNCTION_FILTER

# --- CONFIG ---
MD_CONN_STR = "md:"
MOTHERDUCK_TOKEN = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
COUNTRIES_TABLE = "reference.countries"
PORTS_TABLE = "reference.ports"
RAW_AIS_TABLE = "silver.raw_ais_pings"

# --- LOGGING SETUP ---
logger = logging.getLogger("ais_ingest")


def get_db_connection():
    if not MOTHERDUCK_TOKEN:
        raise ValueError("MOTHERDUCK_TOKEN not found in environment variables")
    logger.debug(f"Connecting to MotherDuck with DuckDB {duckdb.__version__}...")
    con = duckdb.connect(MD_CONN_STR)

    con.execute("CREATE DATABASE IF NOT EXISTS my_voyage_db")
    con.execute("USE my_voyage_db")
    return con


def retry_request(url, headers=None, retries=3, stream=False):
    """Helper to fetch URL with retries."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, stream=stream, timeout=20)
            if resp.status_code == 200:
                return resp
            logger.warning(
                f"Attempt {attempt+1}/{retries} failed for {url}: Status {resp.status_code}"
            )
        except Exception as e:
            logger.warning(f"Attempt {attempt+1}/{retries} error for {url}: {e}")

        if attempt < retries - 1:
            time.sleep(2**attempt)  # Exponential backoff

    logger.error(f"Failed to fetch {url} after {retries} attempts.")
    return None


def parse_unece_coord(coord_str):
    """Parses UNECE coordinate string (e.g., '5157N', '00408E') into float."""
    if not coord_str or len(coord_str) < 5:
        return None
    try:
        factor = -1 if coord_str[-1] in ["S", "W"] else 1

        # Format is D...M...X
        # Lat: 2 digits deg, 2 digits min. Lon: 3 digits deg, 2 digits min.
        minutes = float(coord_str[-3:-1])
        degrees = float(coord_str[:-3])

        return (degrees + (minutes / 60.0)) * factor
    except Exception:
        return None


def scrape_iso_countries(con):
    """Scrapes ISO 3166-2 country codes from Wikipedia."""
    from bs4 import BeautifulSoup

    logger.info("📖 Scraping ISO country codes from Wikipedia...")
    url = "https://en.wikipedia.org/wiki/ISO_3166-2"
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = retry_request(url, headers=headers)
    if not resp:
        raise RuntimeError("Could not fetch ISO codes.")

    soup = BeautifulSoup(resp.text, "html.parser")
    h2 = soup.find("h2", {"id": "Current_codes"})
    if not h2:
        # Fallback to hardcoded major countries if scraping fails structure
        logger.error("Could not find 'Current_codes' section. Wiki format may have changed.")
        return []

    table = h2.find_next("table")
    iso_codes = set()

    for tr in table.find_all("tr")[1:]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
        if cols:
            code = cols[0]
            if len(code) == 2 and code.isupper():
                iso_codes.add(code)

    sorted_codes = sorted(list(iso_codes))
    logger.info(f"✅ Found {len(sorted_codes)} ISO country codes.")

    # Save to DB
    con.execute(f"DROP TABLE IF EXISTS {COUNTRIES_TABLE}")
    con.execute(f"CREATE TABLE {COUNTRIES_TABLE} (code VARCHAR, updated_at TIMESTAMP)")

    # Insert in batches or simple loop
    now = datetime.now(timezone.utc)
    data = [(code, now) for code in sorted_codes]
    con.executemany(f"INSERT INTO {COUNTRIES_TABLE} VALUES (?, ?)", data)

    return sorted_codes


def scrape_ports(con, iso_codes):
    """Scrapes UN/LOCODE ports for the given country codes."""
    from bs4 import BeautifulSoup

    logger.info(f"⚓ Scraping ports for {len(iso_codes)} countries...")
    headers = {"User-Agent": "Mozilla/5.0"}
    all_ports = []

    def fetch_country_ports(iso):
        url = f"https://service.unece.org/trade/locode/{iso.lower()}.htm"
        resp = retry_request(url, headers=headers, retries=3)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.find_all("tr")
        country_ports = []

        for tr in rows[10:]:  # Skip header rows
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 10:
                continue

            # Format: ' AR ', ' BLA ' -> 'ARBLA'
            locode_raw = cells[1].replace("\xa0", "").replace(" ", "").upper()

            if not locode_raw or len(locode_raw) < 3:
                continue

            full_locode = f"{iso}{locode_raw}" if len(locode_raw) == 3 else locode_raw
            name = cells[2]
            function = cells[5]
            status = cells[6].strip()
            coords_str = cells[9].strip()

            # Filters
            if PORT_FUNCTION_FILTER not in function:
                continue
            if status[:2] not in PORT_STATUS_CODES:
                continue
            if not coords_str:
                continue

            lat = parse_unece_coord(coords_str.split(" ")[0])
            lon = parse_unece_coord(coords_str.split(" ")[1]) if " " in coords_str else None

            if lat is not None and lon is not None:
                country_ports.append({"LOCODE": full_locode, "Name": name, "lat": lat, "lon": lon})
        return country_ports

    # Parallel scraping
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_iso = {executor.submit(fetch_country_ports, iso): iso for iso in iso_codes}
        for i, future in enumerate(as_completed(future_to_iso)):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(iso_codes)} countries processed...")
            res = future.result()
            if res:
                all_ports.extend(res)

    if not all_ports:
        logger.error("No ports scraped!")
        return False

    logger.info(f"✅ Extracted {len(all_ports)} total ports. Saving to DB...")

    # Save to DuckDB
    con.execute(f"DROP TABLE IF EXISTS {PORTS_TABLE}")
    con.execute(
        f"""
        CREATE TABLE {PORTS_TABLE} (
            LOCODE VARCHAR, Name VARCHAR,
            lat DOUBLE, lon DOUBLE,
            updated_at TIMESTAMP
        )
        """
    )

    # Create Polars DF for bulk insert
    df = pl.DataFrame(all_ports).with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("updated_at")
    )
    con.register("df_ports_new", df)
    con.execute(f"INSERT INTO {PORTS_TABLE} SELECT * FROM df_ports_new")
    con.unregister("df_ports_new")

    return True


def ensure_reference_data(con):
    """Checks if ports exist in MotherDuck; if not, bootstraps them."""
    con.execute("CREATE SCHEMA IF NOT EXISTS reference")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    # 1. Check Countries (Update yearly)
    try:
        res = con.sql(f"SELECT MAX(updated_at) FROM {COUNTRIES_TABLE}").fetchone()
        last_country_update = res[0] if res else None
    except Exception:
        last_country_update = None

    iso_codes = []
    if not last_country_update or (datetime.now() - last_country_update).days > 365:
        logger.info("Countries table missing or stale (>1 year). Updating...")
        iso_codes = scrape_iso_countries(con)
    else:
        logger.info("✅ Countries table is up to date.")
        iso_codes = [r[0] for r in con.sql(f"SELECT code FROM {COUNTRIES_TABLE}").fetchall()]

    # 2. Check Ports (Update every 6 months)
    try:
        res = con.sql(f"SELECT MAX(updated_at) FROM {PORTS_TABLE}").fetchone()
        last_port_update = res[0] if res else None
    except Exception:
        last_port_update = None

    if not last_port_update or (datetime.now() - last_port_update).days > 180:
        logger.info("Ports table missing or stale (>6 months). Updating...")
        success = scrape_ports(con, iso_codes)
        if not success:
            logger.error("Scraping failed. Checking for existing data...")
            # If scraping failed entirely and we have no table, we must fallback
            try:
                count = con.sql(f"SELECT COUNT(*) FROM {PORTS_TABLE}").fetchone()[0]
                if count > 0:
                    logger.warning("Using existing stale data.")
                    return
            except Exception:
                pass

            logger.warning(
                "⚠️ No ports available. Bootstrapping with HARDCODED major ports as fallback."
            )
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PORTS_TABLE} (
                LOCODE VARCHAR,
                Name VARCHAR,
                lat DOUBLE,
                lon DOUBLE,
                updated_at TIMESTAMP
            )
        """
        )

        # Insert major ports to ensure the pipeline functions on first run
        # (Rotterdam, Singapore, Shanghai, Los Angeles, New York, etc.)
        con.execute(
            f"""
            INSERT INTO {PORTS_TABLE} VALUES
            ('NLRTM', 'Rotterdam', 51.95, 4.13, CURRENT_TIMESTAMP),
            ('SGSIN', 'Singapore', 1.28, 103.85, CURRENT_TIMESTAMP),
            ('CNSHA', 'Shanghai', 31.23, 121.47, CURRENT_TIMESTAMP),
            ('CNNGB', 'Ningbo', 29.86, 121.52, CURRENT_TIMESTAMP),
            ('KRPUS', 'Busan', 35.10, 129.04, CURRENT_TIMESTAMP),
            ('USLAX', 'Los Angeles', 33.74, -118.26, CURRENT_TIMESTAMP),
            ('USLGB', 'Long Beach', 33.77, -118.19, CURRENT_TIMESTAMP),
            ('USNYC', 'New York', 40.71, -74.00, CURRENT_TIMESTAMP),
            ('DEHAM', 'Hamburg', 53.55, 9.99, CURRENT_TIMESTAMP),
            ('BEANR', 'Antwerp', 51.22, 4.40, CURRENT_TIMESTAMP),
            ('JPTYO', 'Tokyo', 35.68, 139.76, CURRENT_TIMESTAMP),
            ('AEJEA', 'Jebel Ali', 25.00, 55.06, CURRENT_TIMESTAMP)
        """
        )
        logger.info("✅ Bootstrapped ports table with major global hubs.")
    else:
        logger.info("✅ Ports table is up to date.")


def load_ports_for_kdtree(con):
    """Loads ports into memory for KDTree filtering."""
    cache_file = Path("ports_kdtree.pkl")

    # 1. Try loading from local disk cache first
    if cache_file.exists():
        # Cache valid for 24 hours
        if (time.time() - cache_file.stat().st_mtime) < 86400:
            try:
                with open(cache_file, "rb") as f:
                    data = pickle.load(f)
                logger.info("✅ Loaded KDTree from local disk cache.")
                return data["tree"], data["locodes"], data["names"]
            except Exception:
                logger.warning("⚠️ Cache file corrupted, rebuilding...")

    # 2. Build from MotherDuck if cache missing or stale
    logger.info("🏗️ Building KDTree from MotherDuck ports table...")
    df = con.sql(f"SELECT LOCODE, Name, lat, lon FROM {PORTS_TABLE}").pl()

    port_locodes = df["LOCODE"].to_numpy()
    port_names = df["Name"].to_numpy()
    # Convert to radians for distance calc
    coords = np.deg2rad(df[["lat", "lon"]].to_numpy())
    tree = KDTree(coords)

    # 3. Save to cache
    try:
        with open(cache_file, "wb") as f:
            pickle.dump({"tree": tree, "locodes": port_locodes, "names": port_names}, f)
        logger.info("💾 Saved KDTree to local disk cache.")
    except Exception as e:
        logger.warning(f"⚠️ Could not save KDTree cache: {e}")

    return tree, port_locodes, port_names


def fetch_and_filter_ais(year, month, day, tree, port_locodes, port_names):
    date_str = f"{year}-{month:02d}-{day:02d}"
    url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/" f"ais-{date_str}.csv.zst"
    logger.info(f"⬇️ Streaming AIS data from {url}...")

    csv_buffer = None
    retries = 3
    for attempt in range(retries):
        try:
            # Use a new session for each attempt to avoid state issues
            with requests.Session() as s:
                # Use a longer timeout to accommodate large file downloads
                resp = s.get(url, stream=True, timeout=60)
                resp.raise_for_status()  # Check for 4xx/5xx errors

                # Decompress stream in memory. The `reader.read()` call is where
                # an IncompleteRead error can occur if the connection breaks.
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(resp.raw) as reader:
                    csv_buffer = io.BytesIO(reader.read())

                # If we successfully read, break the loop
                logger.info("📦 Data downloaded and decompressed successfully.")
                break

        except (requests.exceptions.RequestException, zstd.ZstdError) as e:
            # Catches connection errors, timeouts, incomplete reads, and decompression errors
            logger.warning(
                f"Attempt {attempt + 1}/{retries} failed to download/decompress {url}: {e}"
            )
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))  # Exponential backoff (2, 4 seconds)
            else:
                logger.error(
                    f"❌ Aborting {date_str} due to persistent download/decompression failure."
                )
                return None

    logger.info("Parsing CSV...")

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
        logger.error(f"❌ Failed to parse CSV: {e}")
        return None

    # Filter Nulls
    ais = ais.filter(pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null())

    if ais.height == 0:
        return None

    logger.info(f"🔎 Filtering {ais.height} pings against {len(port_locodes)} ports...")

    # Coordinate Transform & KDTree Query
    ship_coords = to_radians(ais)  # Uses your existing util
    dist, idx = tree.query(ship_coords, k=1)

    # 5km radius filter (approx 0.00078 radians, but let's use the explicit dist logic)
    # Earth radius ~6371km. 5km / 6371 = 0.000784
    mask = (dist * 6371.0) <= 5.0

    if not mask.any():
        logger.info("No pings near ports found.")
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

    logger.info(f"✅ Retained {filtered_ais.height} relevant pings.")
    return filtered_ais


def process_date(target_date, tree, locodes, names):
    """Ingests a single day of data."""
    logger.info(f"🚀 Starting Ingest for {target_date.date()}")

    # Create a fresh connection for this thread to ensure thread safety
    con = get_db_connection()
    try:
        df_filtered = fetch_and_filter_ais(
            target_date.year, target_date.month, target_date.day, tree, locodes, names
        )

        if df_filtered is not None and not df_filtered.is_empty():
            logger.info(
                f"📤 Uploading {df_filtered.height} rows for {target_date.date()} to MotherDuck..."
            )

            # Ensure table exists (idempotent)
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
            logger.info(f"🎉 Ingestion Complete for {target_date.date()}.")
        else:
            logger.info(f"⚠️ No data to ingest for {target_date.date()}.")
    except Exception as e:
        logger.error(f"❌ Failed to process {target_date.date()}: {e}")
    finally:
        con.close()


def main():
    # Set logging config
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Ingest AIS Data to MotherDuck")
    parser.add_argument("--year", type=int, help="Year (YYYY)")
    parser.add_argument("--month", type=int, help="Month (1-12)")
    parser.add_argument("--day", type=int, help="Day (1-31)")
    args = parser.parse_args()

    con = get_db_connection()
    ensure_reference_data(con)
    tree, locodes, names = load_ports_for_kdtree(con)

    # Ensure the destination table exists at least once before threading
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_AIS_TABLE} (
            mmsi BIGINT, imo VARCHAR, vessel_name VARCHAR,
            latitude DOUBLE, longitude DOUBLE,
            dep_time TIMESTAMP, port_locode VARCHAR,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    con.close()  # Close main connection, threads will open their own

    # Date Logic
    if args.year and args.month and args.day:
        start_date = datetime(args.year, args.month, args.day, tzinfo=timezone.utc)
        end_date = start_date
    elif args.year and args.month:
        start_date = datetime(args.year, args.month, 1, tzinfo=timezone.utc)
        # Logic to find last day of month
        if args.month == 12:
            end_date = datetime(args.year, 12, 31, tzinfo=timezone.utc)
        else:
            end_date = datetime(args.year, args.month + 1, 1, tzinfo=timezone.utc) - timedelta(
                days=1
            )
    elif args.year:
        start_date = datetime(args.year, 1, 1, tzinfo=timezone.utc)
        end_date = datetime(args.year, 12, 31, tzinfo=timezone.utc)
    else:
        # Default: Yesterday
        start_date = datetime.now(timezone.utc) - timedelta(days=1)
        end_date = start_date

    # Prevent future dates
    if end_date > datetime.now(timezone.utc):
        end_date = datetime.now(timezone.utc) - timedelta(days=1)

    logger.info(f"📅 Running pipeline from {start_date.date()} to {end_date.date()}")

    # Generate list of dates to process
    dates_to_process = []
    curr = start_date
    while curr <= end_date:
        dates_to_process.append(curr)
        curr += timedelta(days=1)

    # Process in parallel (Max 2 workers to prevent OOM on 7GB RAM runners)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process_date, d, tree, locodes, names) for d in dates_to_process]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.error(f"Thread failed: {e}")


if __name__ == "__main__":
    main()
