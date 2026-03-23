import asyncio
import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import modal

from src.config import (
    BRONZE_DIR,
    GOLD_DIR,
    PORTS_PATH,
    REFERENCE_DIR,
    SILVER_DIR,
    app,
    get_logger,
    image,
    volume,
)
from src.extract import AISProcessor, run_unlocode_bootstrap
from src.transform import stitch_voyages

logger = get_logger("orchestrator")

# --- WORKERS & UTILS ---


@app.function(
    image=image,
    volumes={"/data": volume},
    secrets=[modal.Secret.from_name("github-auth"), modal.Secret.from_dotenv()],
)
def sync_to_github():
    token = os.environ.get("GH_TOKEN")
    if not token:
        logger.warning("⚠️ GH_TOKEN secret not found. Skipping GitHub sync.")
        return

    repo_name = os.environ.get("REPO_NAME")
    user = os.environ.get("REPO_OWNER")

    if not repo_name or not user:
        logger.error("❌ REPO_NAME or REPO_OWNER not set in .env file.")
        return

    repo_url = f"https://{token}@github.com/{user}/{repo_name}.git"
    local_repo = Path("/tmp/ais_repo")
    gold_dir = Path("/data/gold")
    gold_file = gold_dir / "voyages.parquet"

    if not gold_file.exists():
        logger.error(f"No gold file found at {gold_file} to sync!")
        return

    target_file = local_repo / "data" / "gold" / "voyages.parquet"

    try:
        if local_repo.exists():
            shutil.rmtree(local_repo)

        logger.info(f"Cloning {repo_name}...")
        subprocess.run(["git", "clone", "--depth", "1", repo_url, str(local_repo)], check=True)

        target_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(gold_file, target_file)

        os.chdir(local_repo)
        subprocess.run(["git", "config", "user.email", "bot@modal.com"], check=True)
        subprocess.run(["git", "config", "user.name", "Modal Data Bot"], check=True)

        status = subprocess.run(
            ["git", "status", "--porcelain"], capture_output=True, text=True
        ).stdout

        if not status:
            logger.info("No changes detected. Skipping push.")
            return

        subprocess.run(["git", "add", "-f", "data/gold/voyages.parquet"], check=True)
        subprocess.run(["git", "commit", "-m", f"Auto-update: {datetime.now().date()}"], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)

        logger.info("Successfully pushed latest gold data to GitHub!")
    except Exception as e:
        logger.error(f"Sync failed: {e}")


# --- MAIN PIPELINE ---


@app.function(image=image, volumes={"/data": volume}, memory=8192, timeout=3600)
async def full_pipeline(year: int, month: int, day: int = None):
    logger.info(
        f"🚀 Starting Pipeline: {year}-{month:02d}-{day:02d}"
        if day is not None
        else f"🚀 Starting Pipeline: {year}-{month:02d}"
    )

    # Create the directories directly on the mounted Volume
    for folder in [SILVER_DIR, GOLD_DIR, REFERENCE_DIR, BRONZE_DIR]:
        os.makedirs(folder, exist_ok=True)

    # Commit the folder structure so subsequent workers see it
    await volume.commit.aio()

    # Force bootstrap for testing
    logger.info("Forcing bootstrap for testing...")
    _ = await run_unlocode_bootstrap.remote.aio()

    # Wait for the ports file to become visible (with timeout)
    max_wait = 60
    for attempt in range(max_wait):
        if os.path.exists(PORTS_PATH):
            break
        logger.info("Waiting for PORTS_PATH to become visible...")
        await asyncio.sleep(5)
        await volume.reload.aio()
    else:
        raise RuntimeError("PORTS_PATH did not appear after bootstrap")

    # 1. Parallel Extraction
    processor = AISProcessor(model_name="voyage-v1")
    if not day:
        # Calculate last day of month
        next_month = datetime(year, month, 1) + timedelta(days=32)
        days_in_month = (next_month.replace(day=1) - timedelta(days=1)).day
        day_args = [(year, month, d) for d in range(1, days_in_month + 1)]
    else:
        day_args = [(year, month, day)]

    logger.info(f"Extracting {len(day_args)} days of AIS data...")

    # We use list comprehension to wait for all parallel extraction workers
    _ = [r async for r in processor.process_day.starmap.aio(day_args, order_outputs=False)]

    # 2. Commit files so they are visible to the next step
    await volume.commit.aio()
    await volume.reload.aio()

    # 3. Stitch & Enrich (This function now handles its own internal queueing)
    logger.info("🧵 Stitching AIS pings and Enriching via Groq...")
    await stitch_voyages(year, month)

    await volume.commit.aio()
    logger.info("🏁 Pipeline complete.")


# --- AUTOMATION & ENTRYPOINTS ---


@app.function(schedule=modal.Cron("0 2 * * *"))
def daily_update():
    now = datetime.now()
    # Run for previous month if today is early in the month, or current month
    # Here we default to current month for simplicity
    full_pipeline.remote(now.year, now.month, now.day)
    sync_to_github.remote()


@app.local_entrypoint()
def main(year: int = 2025, month: int = None, day: int = None):
    if month is None:
        print(f"Year {year} selected.")
        choice = input(
            "Which month should be downloaded? (Enter 1-12, or press Enter for whole year): "
        ).strip()

        if not choice:
            print(f"Downloading data for the whole year {year}...")
            for m in range(1, 13):
                full_pipeline.remote(year, m, day)
        else:
            try:
                full_pipeline.remote(year, int(choice), day)
            except ValueError:
                print("Invalid month entered.")
                return
    else:
        full_pipeline.remote(year, month, day)
    sync_to_github.remote()
