import asyncio
import io
import json
import os
import queue
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import modal
import numpy as np
import polars as pl
import requests
import zstandard as zstd
from bs4 import BeautifulSoup
from scipy.spatial import KDTree

from src.config import PORTS_PATH, SILVER_DIR, app, get_logger, image, volume
from src.datetime_utils import parse_ais_datetime
from src.geospacial import to_radians
from src.voyage_enrichment import generate_cache_key

logger = get_logger(__name__)

# --- UTILS ---


def transform_coords_polars(df: pl.DataFrame) -> pl.DataFrame:
    """
    Converts UN/LOCODE DMS format coordinates to Decimal Degrees.

    Args:
        df: DataFrame with 'Coordinates' column in DMS format (e.g., "5130N 00007W")

    Returns:
        DataFrame with added 'lat' and 'lon' columns in decimal degrees
    """

    def parse_lat(c: str):
        if not c or len(c.strip()) < 9:
            return None  # Need at least "DDMMH DDDMMH"
        try:
            parts = c.strip().split()
            if len(parts) != 2:
                return None
            lat_part = parts[0]
            if len(lat_part) < 5 or lat_part[-1] not in "NS":
                return None
            deg = float(lat_part[0:2])
            mnt = float(lat_part[2:4]) / 60
            if deg < 0 or deg > 90 or mnt < 0 or mnt >= 1:
                return None
            val = deg + mnt
            return -val if lat_part[-1] == "S" else val
        except Exception:
            return None

    def parse_lon(c: str):
        if not c or len(c.strip()) < 9:
            return None  # Need at least "DDMMH DDDMMH"
        try:
            parts = c.strip().split()
            if len(parts) != 2:
                return None
            lon_part = parts[1]
            if len(lon_part) < 6 or lon_part[-1] not in "EW":
                return None
            deg = float(lon_part[0:3])
            mnt = float(lon_part[3:5]) / 60
            if deg < 0 or deg > 180 or mnt < 0 or mnt >= 1:
                return None
            val = deg + mnt
            return -val if lon_part[-1] == "W" else val
        except Exception:
            return None

    return df.with_columns(
        [
            pl.col("Coordinates").map_elements(parse_lat, return_dtype=pl.Float64).alias("lat"),
            pl.col("Coordinates").map_elements(parse_lon, return_dtype=pl.Float64).alias("lon"),
        ]
    ).drop_nulls(["lat", "lon"])


# --- BOOTSTRAP ---


@app.function(volumes={"/data": volume}, timeout=1200)
def run_unlocode_bootstrap():
    os.makedirs(os.path.dirname(PORTS_PATH), exist_ok=True)
    if os.path.exists(PORTS_PATH):
        df = pl.read_parquet(PORTS_PATH)
        logger.info(f"✅ Reference data exists at {PORTS_PATH} ({len(df)} ports)")
        return len(df)

    logger.info("📖 Fetching ISO country codes (ISO 3166-2 Current codes)...")
    headers = {"User-Agent": "Mozilla/5.0"}

    # Scrape Wikipedia ISO 3166-2 "Current codes" table for ISO 3166-1 alpha-2 codes.
    iso_codes = []
    try:
        wiki_res = requests.get(
            "https://en.wikipedia.org/wiki/ISO_3166-2", headers=headers, timeout=15
        )
        wiki_res.raise_for_status()
        soup = BeautifulSoup(wiki_res.text, "html.parser")

        # Find the "Current codes" section and take the first table after it.
        h2 = soup.find("h2", {"id": "Current_codes"})
        if not h2:
            raise RuntimeError("Could not locate 'Current codes' section on ISO 3166-2 page")

        table = h2.find_next("table")
        if not table:
            raise RuntimeError("Could not locate table under 'Current codes' section")

        for tr in table.find_all("tr")[1:]:  # skip header
            cols = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if not cols:
                continue
            code = cols[0].strip()
            if len(code) == 2 and code.isupper():
                iso_codes.append(code)

        iso_codes = sorted(set(iso_codes))
        logger.info(
            f"✅ Wikipedia ISO 3166-2 current codes returned {len(iso_codes)} country codes"
        )

    except Exception as e:
        logger.error(f"Failed to obtain ISO country codes: {e}")
        # Use hardcoded fallback for major maritime countries
        iso_codes = [
            "AU",
            "BR",
            "CA",
            "CH",
            "CN",
            "DE",
            "DK",
            "ES",
            "FR",
            "GB",
            "GR",
            "HK",
            "IE",
            "IN",
            "IT",
            "JP",
            "KR",
            "MX",
            "MY",
            "NL",
            "NO",
            "NZ",
            "PH",
            "PL",
            "RU",
            "SE",
            "SG",
            "TH",
            "TR",
            "US",
            "VN",
            "ZA",
            "AE",
            "IL",
            "SA",
            "AT",
            "BE",
            "CZ",
            "FI",
            "HU",
            "PT",
            "CL",
            "AR",
            "PE",
            "CO",
            "EG",
            "ZA",
        ]
        logger.warning(f"Using hardcoded fallback of {len(iso_codes)} major maritime countries")

    if not iso_codes:
        raise RuntimeError("No ISO country codes found for ports bootstrap.")

    logger.info(f"🌍 Obtained {len(iso_codes)} ISO country codes")

    # Track failed countries (no data / errors) for later processing
    failed_countries = []
    all_chunks = []

    # Save failed list to a file in the reference folder
    failed_path = Path(PORTS_PATH).with_name("failed_countries.json")

    # Decide how many countries to process (set to 0 for all)
    max_countries = 0
    countries_to_process = iso_codes if max_countries <= 0 else iso_codes[:max_countries]
    logger.info(f"Processing {len(countries_to_process)} countries using workers...")

    # Multi-threaded scraping to speed up country processing
    def scrape_iso(iso: str) -> dict:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                url = f"https://service.unece.org/trade/locode/{iso.lower()}.htm"
                res = requests.get(url, headers=headers, timeout=15)
                if res.status_code != 200:
                    return {"iso": iso, "status": "http", "code": res.status_code}

                soup = BeautifulSoup(res.text, "html.parser")
                all_trs = soup.find_all("tr")
                extracted = []
                for tr in all_trs[10:]:
                    cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                    if len(cells) < 10:
                        continue
                    locode = cells[1].replace("\xa0", "").replace(" ", "").upper()
                    if not locode or " " in locode:
                        if attempt < max_retries - 1:
                            time.sleep(2**attempt)  # Exponential backoff
                            continue
                        else:
                            logger.warning(f"Skipping malformed LOCODE '{locode}' for {iso}")
                            continue

                    extracted.append(
                        {
                            "CountryCode": iso.upper(),
                            "LOCODE": locode,
                            "Name": cells[2],
                            "Function": cells[5],
                            "Status": cells[6].strip()[:2] if len(cells) > 6 else "",
                            "Coordinates": cells[9].strip() if len(cells) > 9 else "",
                        }
                    )

                if not extracted:
                    return {"iso": iso, "status": "no_rows"}

                df = pl.DataFrame(extracted).filter(
                    (pl.col("Function").str.contains("1"))
                    & (pl.col("Coordinates").str.contains(r"\d{4}[NS]"))
                    & (
                        pl.col("Status").is_in(
                            ["AA", "AC", "AF", "AI", "AM", "AS", "AQ", "RL", "RQ", "RN", "QQ"]
                        )
                    )
                )
                if df.is_empty():
                    return {"iso": iso, "status": "no_ports"}

                df = transform_coords_polars(df).select(
                    ["CountryCode", "LOCODE", "Name", "lat", "lon"]
                )
                return {"iso": iso, "status": "success", "rows": df.to_dicts()}

            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(2**attempt)  # Exponential backoff
                    continue
                return {"iso": iso, "status": "error", "reason": str(e)[:200]}

    batch_size = 20
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        future_to_iso = {executor.submit(scrape_iso, iso): iso for iso in countries_to_process}
        for fut in as_completed(future_to_iso):
            res = fut.result()
            iso = res.get("iso")
            if res.get("status") == "success":
                all_chunks.append(pl.DataFrame(res["rows"]))
            else:
                failed_countries.append(
                    {
                        "iso": iso,
                        "reason": res.get("reason") or res.get("status") or "unknown",
                        "http_code": res.get("code"),
                    }
                )

    # Persist list of countries with no data for later processing
    try:
        failed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(failed_path, "w", encoding="utf-8") as f:
            json.dump(failed_countries, f, ensure_ascii=False, indent=2)
        logger.info(f"📝 Saved failed countries list to {failed_path}")

        # Also persist in a modal Dict for fast lookup in other functions
        failed_cache = modal.Dict.from_name("unlocode_failed_countries", create_if_missing=True)
        failed_cache.clear()
        for entry in failed_countries:
            failed_cache[entry["iso"]] = entry
    except Exception as e:
        logger.warning(f"Failed to write failed countries list: {e}")

    logger.info(
        f"📊 Scraping summary: {len(all_chunks)} succeeded, {len(failed_countries)} failed/empty"
    )

    if not all_chunks:
        logger.error("No ports collected from any country")
        raise RuntimeError(
            "Bootstrap completed but no ports were collected. Check network access or scrape logic."
        )

    final_df = pl.concat(all_chunks).unique(subset=["LOCODE"])
    final_df.write_parquet(PORTS_PATH)
    volume.commit()
    logger.info(f"💾 Saved {len(final_df)} ports.")
    return len(final_df)


@app.function(volumes={"/data": volume})
def test_ports_file():
    """Test function to check if ports file can be read."""
    try:
        df = pl.read_parquet(PORTS_PATH)
        logger.info(f"✅ Successfully read {len(df)} ports")
        return f"Success: {len(df)} rows"
    except Exception as e:
        logger.error(f"❌ Failed to read ports file: {e}")
        return f"Error: {e}"


@app.cls(image=image, volumes={"/data": volume}, memory=4096, timeout=600)
class AISProcessor:
    model_name: str = modal.parameter()
    batch_size: int = modal.parameter(default=100)
    save_path: str = modal.parameter(default=str(SILVER_DIR))

    # Class-level defaults - properly typed
    tree: Optional[KDTree] = None
    port_locodes: Optional[np.ndarray] = None
    port_names: Optional[np.ndarray] = None

    def __enter__(self):
        """Initialize the KDTree for port proximity calculations."""
        try:
            # Read reference data
            df = pl.read_parquet(PORTS_PATH)

            # Initialize shared resources
            self.port_locodes = df["LOCODE"].to_numpy()
            self.port_names = df["Name"].to_numpy()
            coords = np.deg2rad(df[["lat", "lon"]].to_numpy())
            self.tree = KDTree(coords)

            logger.info(f"✅ Worker {os.getpid()} initialized successfully.")
            return
        except Exception as e:
            logger.error(f"❌ Worker failed to initialize: {e}")
            self.tree = None

    @modal.method()
    def process_day(self, year: int, month: int, day: int) -> str:
        # Try to initialize KDTree if not already done
        if self.tree is None:
            try:
                df = pl.read_parquet(PORTS_PATH)
                self.port_locodes = df["LOCODE"].to_numpy()
                self.port_names = df["Name"].to_numpy()
                coords = np.deg2rad(df[["lat", "lon"]].to_numpy())
                self.tree = KDTree(coords)
                logger.info(f"✅ Worker {os.getpid()} initialized KDTree for day {day}")
            except Exception as e:
                return (
                    f"❌ FAILED: Could not initialize KDTree for {year}-{month:02d}-{day:02d}: {e}"
                )

        date_str = f"{year}-{month:02d}-{day:02d}"
        url = f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/ais-{date_str}.csv.zst"

        # Check if file already exists in silver layer
        out_path = Path(self.save_path) / f"ais_{year}_{month:02d}_{day:02d}.parquet"
        if out_path.exists():
            logger.info(f"⏭️  Skipping {date_str}: file already exists at {out_path}")
            return f"⏭️  Skipped {date_str} (file already exists)"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                with requests.Session() as session:
                    resp = session.get(url, stream=True, timeout=300)  # Increased timeout
                    resp.raise_for_status()

                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp_path = tmp.name
                        for chunk in resp.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                tmp.write(chunk)

                # Decompress from the temp file
                dctx = zstd.ZstdDecompressor()
                with open(tmp_path, "rb") as f:
                    with dctx.stream_reader(f) as reader:
                        csv_bytes = reader.read()
                        csv_buffer = io.BytesIO(csv_bytes)

                        # Read once with schema inspection
                        ais = pl.read_csv(csv_buffer, ignore_errors=True)

                        # Select only available columns
                        relevant_cols = [
                            col
                            for col in [
                                "mmsi",
                                "latitude",
                                "longitude",
                                "base_date_time",
                                "vessel_name",
                                "imo",
                            ]
                            if col in ais.columns
                        ]
                        ais = ais.select(relevant_cols).filter(pl.col("mmsi").is_not_null())
                        ais = ais.with_columns(
                            [parse_ais_datetime(pl.col("base_date_time")).alias("dt")]
                        ).drop("base_date_time")

                # Clean up temp file
                os.unlink(tmp_path)

                # Ensure latitude and longitude columns exist
                if "latitude" in ais.columns and "longitude" in ais.columns:
                    ais_valid = ais.filter(
                        pl.col("latitude").is_not_null() & pl.col("longitude").is_not_null()
                    )
                    if ais_valid.is_empty():
                        return f"No valid lat/lon data for {date_str}"

                    # Convert coordinates to radians for KDTree
                    ship_coords = to_radians(ais_valid)
                    dist, idx = self.tree.query(ship_coords, k=1)
                    mask = (dist * 6371.0) <= 5.0  # 5km radius

                    if mask.any():
                        valid_idx = idx[mask]
                        # Ensure valid_idx is within bounds
                        max_idx = min(len(self.port_locodes), len(self.port_names))
                        valid_idx = valid_idx[(valid_idx >= 0) & (valid_idx < max_idx)]
                        if len(valid_idx) == 0:
                            return f"No valid port indices for {date_str}"
                        filtered_ais = ais_valid.filter(mask)
                        filtered_ais = filtered_ais.head(len(valid_idx))  # Ensure same length
                        res = filtered_ais.with_columns(
                            [
                                pl.Series("port_locode", self.port_locodes[valid_idx]),
                                pl.Series("port_name", self.port_names[valid_idx]),
                            ]
                        ).rename({"latitude": "lat", "longitude": "lon"})

                        # out_path already defined at the beginning of the method
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        res.write_parquet(out_path)
                        volume.commit()
                        return f"✅ Saved {len(res)} rows to {out_path}"

                    return f"No port pings for {date_str}"
                else:
                    logger.warning(
                        f"Invalid data for {date_str}: Missing latitude/longitude columns"
                    )
                    return f"No valid lat/lon data for {date_str}"

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {date_str}: "
                        "{e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Error processing {date_str} after {max_retries} attempts: {e}")
                    return f"Error on {date_str}: {e}"


# --- MARITIME DISTANCE CACHE ---
maritime_distance_cache = modal.Dict.from_name("maritime-distance-cache", create_if_missing=True)
voyage_work_queue = modal.Queue.from_name("voyage-work-queue", create_if_missing=True)


@app.cls(image=image)
class VoyageEnricher:
    def __enter__(self):
        """Initialize remote resources once per container startup."""
        self.cache = modal.Dict.from_name("maritime-distance-cache", create_if_missing=True)
        self.processing_registry = modal.Dict.from_name(
            "voyage-processing-registry", create_if_missing=True
        )

    @modal.method()
    async def consume_queue(self, worker_id: int):
        """Asynchronous worker that pulls from Modal Queue and processes voyages."""
        # Moving imports here ensures they are available in the Modal runtime
        from src.voyage_enrichment import generate_cache_key  # Ensure this is imported here
        from src.voyage_enrichment import (
            call_searoute,
            get_path_distance,
            get_path_duration,
            get_sea_path,
        )

        # Lazy initialization for robustness against __enter__ race conditions
        if not hasattr(self, "cache") or self.cache is None:
            logger.debug(f"Worker {worker_id}: Re-initializing cache connections...")
            self.cache = modal.Dict.from_name("maritime-distance-cache", create_if_missing=True)
            self.processing_registry = modal.Dict.from_name(
                "voyage-processing-registry", create_if_missing=True
            )

        fetched_count = 0
        success_count = 0
        skipped_count = 0
        failed_count = 0
        error_logs = []

        while True:
            try:
                # 1. Pull from queue
                try:
                    voyage = await voyage_work_queue.get.aio(timeout=5)
                except (TimeoutError, asyncio.TimeoutError, queue.Empty):
                    break

                if not voyage:
                    break

                fetched_count += 1
                v_key = generate_cache_key(voyage)

                # 2. ATOMIC CHECK
                if await self.cache.contains.aio(v_key):
                    skipped_count += 1
                    continue

                if await self.processing_registry.contains.aio(v_key):
                    skipped_count += 1
                    continue

                # 3. SET LOCK
                await self.processing_registry.put.aio(v_key, "PROCESSING")

                try:
                    # 4. Perform calculation
                    # Ensure call_searoute matches the keys in your Polars DataFrame
                    route_data = call_searoute(voyage)

                    if route_data:
                        # Fixed: Removed the 'cache' argument from these calls as they
                        # should just be pure data transformations.
                        result = {
                            "path": get_sea_path(voyage, route_data),
                            "distance_nm": get_path_distance(voyage, route_data),
                            "expected_duration_hrs": get_path_duration(voyage, route_data),
                        }

                        # 5. COMMIT TO CACHE
                        await self.cache.put.aio(v_key, result)
                        success_count += 1
                    else:
                        failed_count += 1
                        error_logs.append(f"{v_key}: No route returned")
                        logger.warning(f"Worker {worker_id}: No route found for {v_key}")

                except Exception as e:
                    failed_count += 1
                    error_logs.append(f"{v_key}: {e}")
                    # Use print for immediate visibility in Modal logs during debugging
                    print(f"❌ Worker {worker_id} logic error on {v_key}: {e}")
                finally:
                    await self.processing_registry.pop.aio(v_key, None)

            except asyncio.CancelledError:
                # Normal exit: Worker was cancelled by orchestrator
                break
            except Exception as e:
                # Genuine unexpected error
                logger.error(f"⚠️ Worker {worker_id} unexpected error: {type(e).__name__}: {e}")
                break

        logger.info(
            f"✅ Worker {worker_id} exiting. Fetched: {fetched_count} | "
            "Enriched: {success_count} | Skipped: {skipped_count} | Failed: {failed_count}"
        )
        if error_logs:
            logger.warning(f"⚠️ Worker {worker_id} Failures: {error_logs}")


# --- UPDATED BATCH ENRICHER (Optional Fallback) ---


@app.function(image=image, memory=2048, timeout=3600)
def enrich_voyage_batch(voyage_batch: list) -> list:
    """Process a batch of voyages in parallel with caching."""
    from src.voyage_enrichment import (
        call_searoute,
        get_path_distance,
        get_path_duration,
        get_sea_path,
    )

    cache = modal.Dict.from_name("maritime-distance-cache", create_if_missing=True)

    def enrich_single_voyage(voyage):
        cache_key = generate_cache_key(voyage)

        # Check cache first to avoid redundant API calls
        if cache_key in cache:
            cached = cache[cache_key]
            return {
                "path": cached.get("path"),
                "distance_nm": cached.get("distance_nm"),
                "expected_duration_hrs": cached.get("expected_duration_hrs"),
            }

        try:
            route = call_searoute(voyage)
            result = {
                "path": get_sea_path(voyage, route),
                "distance_nm": get_path_distance(voyage, route),
                "expected_duration_hrs": get_path_duration(voyage, route),
            }
            # Cache it immediately for other threads in this worker
            cache[cache_key] = result
            return result
        except Exception as e:
            logger.warning(f"Failed to enrich voyage {voyage.get('mmsi', 'unknown')}: {e}")
            return {"path": None, "distance_nm": None, "expected_duration_hrs": None}

    with ThreadPoolExecutor(max_workers=10) as executor:
        return list(executor.map(enrich_single_voyage, voyage_batch))
