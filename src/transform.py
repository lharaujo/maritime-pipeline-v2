import asyncio
from typing import Optional

import duckdb
import modal
import polars as pl

from src.config import GOLD_DIR, SILVER_DIR, get_logger
from src.datetime_utils import time_difference_hours

# Ensure these are imported correctly from your modules
from src.extract import VoyageEnricher, voyage_work_queue
from src.settings import AppSettings
from src.voyage_enrichment import generate_cache_key

logger = get_logger(__name__)


async def stitch_voyages(year: int, month: int, settings: Optional[AppSettings] = None) -> None:
    """
    Stitch AIS pings into voyage legs and enrich with maritime distances/paths.
    """
    settings = settings or AppSettings()
    input_pattern = SILVER_DIR / f"ais_{year}_{month:02d}*.parquet"
    output_path = GOLD_DIR / "voyages.parquet"

    logger.info(f"🧵 Stitching Silver layer for {year}-{month:02d}...")

    # 1. DuckDB Window Functions to identify port-to-port transitions
    sql = f"""
        WITH unique_port_visits AS (
            SELECT
                mmsi,
                dt AS dep_time,
                REGEXP_REPLACE(imo, '^IMO', '') as imo,
                vessel_name,
                port_locode,
                lat,
                lon,
                LAG(port_locode) OVER (PARTITION BY mmsi ORDER BY dt) as prev_port
            FROM read_parquet('{input_pattern}')
        ),
        filtered_events AS (
            SELECT * FROM unique_port_visits
            WHERE prev_port IS NULL OR port_locode != prev_port
        ),
        voyage_legs AS (
            SELECT
                mmsi,
                imo,
                vessel_name,
                port_locode as dep_locode,
                lat as dep_lat,
                lon as dep_lon,
                dep_time,
                LEAD(port_locode) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_locode,
                LEAD(lat) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_lat,
                LEAD(lon) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_lon,
                LEAD(dep_time) OVER (PARTITION BY mmsi ORDER BY dep_time) as arr_time
            FROM filtered_events
        )
        SELECT
            mmsi, imo, vessel_name, dep_locode, arr_locode,
            dep_lat, dep_lon, arr_lat, arr_lon, dep_time, arr_time
        FROM voyage_legs
        WHERE arr_locode IS NOT NULL AND arr_time IS NOT NULL
    """

    try:
        with duckdb.connect() as con:
            df = con.execute(sql).pl()

        if df.is_empty():
            logger.warning("No voyages found to process.")
            return

        # 2. Data Cleaning and Type Casting
        df = df.with_columns(
            [
                pl.col("dep_lat").cast(pl.Float64),
                pl.col("dep_lon").cast(pl.Float64),
                pl.col("arr_lat").cast(pl.Float64),
                pl.col("arr_lon").cast(pl.Float64),
                pl.col("dep_time").cast(pl.Datetime),
                pl.col("arr_time").cast(pl.Datetime),
            ]
        ).with_columns(
            [time_difference_hours(pl.col("arr_time"), pl.col("dep_time")).alias("duration_hrs")]
        )

        # 3. PRODUCER: Identify Unique New Routes
        cache = modal.Dict.from_name("maritime-distance-cache", create_if_missing=True)

        # Create a temp key for uniqueness check
        unique_voyages_df = df.unique(subset=["dep_locode", "arr_locode"])

        to_process = []
        for v in unique_voyages_df.to_dicts():
            v_key = generate_cache_key(v)
            # Modal Dicts use .contains.aio() for async membership checks
            if not await cache.contains.aio(v_key):
                to_process.append(v)

        # 4. CONSUMER: Distributed Processing
        if to_process:
            logger.info(f"🛰️ Sending {len(to_process)} unique routes to Modal Queue...")
            await voyage_work_queue.clear.aio()
            await voyage_work_queue.put_many.aio(to_process)

            enricher = VoyageEnricher()
            # Exhaust the async generator to wait for all workers
            async for _ in enricher.consume_queue.map.aio(range(20), order_outputs=False):
                pass

            logger.info("✅ Enrichment workers finished.")
        else:
            logger.info("⏭️ All routes already cached.")

        # 5. DATA PATCHING: Join Cache results to Main DataFrame
        logger.info("🔗 Patching enriched data from cache...")

        # Pre-calculate keys for the main dataframe to avoid row-by-row mapping later
        df = df.with_columns(
            pl.struct(["dep_locode", "arr_locode"])
            .map_elements(lambda x: generate_cache_key(x), return_dtype=pl.String)
            .alias("cache_key")
        )

        # Get unique keys we actually need
        needed_keys = df["cache_key"].unique().to_list()

        # Batch fetch from Modal Dict (much faster than a loop)
        # Note: modal.Dict doesn't have a native 'get_many',
        # so we use asyncio.gather for parallelism
        async def _fetch(k):
            val = await cache.get.aio(k)
            return (k, val) if val else (k, None)

        # Fetch all keys in parallel
        results = await asyncio.gather(*[_fetch(k) for k in needed_keys])
        cache_results = []
        for key, entry in results:
            if entry:
                cache_results.append(
                    {
                        "cache_key": key,
                        "path": entry.get("path"),
                        "distance_nm": entry.get("distance_nm"),
                        "expected_duration_hrs": entry.get("expected_duration_hrs"),
                    }
                )

        if cache_results:
            cache_df = pl.DataFrame(cache_results)
            df_enriched = df.join(cache_df, on="cache_key", how="left").drop("cache_key")
            logger.info(f"✅ Enriched {len(cache_results)} routes from cache.")
        else:
            df_enriched = df.drop("cache_key")
            logger.warning("⚠️ No enrichment data found in cache for these routes.")

        # 6. Final adjustments and Save
        df_enriched = df_enriched.with_columns(pl.lit(12.5).alias("avg_speed_kts"))

        GOLD_DIR.mkdir(parents=True, exist_ok=True)

        # Append to existing Gold data if it exists
        if output_path.exists():
            logger.info(f"📚 Appending to existing Gold file: {output_path}")
            try:
                existing_df = pl.read_parquet(output_path)
                # Concatenate and remove duplicates based on unique voyage identifiers
                df_enriched = pl.concat([existing_df, df_enriched], how="vertical_relaxed")
                df_enriched = df_enriched.unique(subset=["mmsi", "dep_time", "arr_time"])
            except Exception as e:
                logger.error(f"⚠️ Failed to read existing Gold file, creating new one: {e}")

        df_enriched.write_parquet(output_path, compression="snappy")
        logger.info(f"🏆 Gold layer updated: {output_path} (Total: {len(df_enriched)} rows)")

        # 7. Cleanup Silver Files
        logger.info("🧹 Cleaning up processed Silver files...")
        for f in SILVER_DIR.glob(f"ais_{year}_{month:02d}*.parquet"):
            f.unlink(missing_ok=True)

    except Exception as e:
        logger.error(f"❌ Transformation failed: {e}")
        raise
