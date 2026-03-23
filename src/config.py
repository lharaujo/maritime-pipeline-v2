import logging
import sys
from pathlib import Path

import modal

# --- 1. CLOUD INFRASTRUCTURE ---

image = (
    modal.Image.debian_slim(python_version="3.12")
    .apt_install("git")
    .pip_install_from_pyproject("pyproject.toml")
    .add_local_python_source("src")
)

app = modal.App("ais-voyage-engine", image=image)
volume = modal.Volume.from_name("ais-data-store", create_if_missing=True)

# --- 2. PATHS & CONSTANTS ---

DATA_PATH = Path("/data")

BRONZE_DIR = DATA_PATH / "bronze"
SILVER_DIR = DATA_PATH / "silver"
GOLD_DIR = DATA_PATH / "gold"
REFERENCE_DIR = DATA_PATH / "reference"
PORTS_PATH = REFERENCE_DIR / "ports.parquet"

# --- 4. LOGGING CONFIGURATION ---


def get_logger(name: str) -> logging.Logger:
    """Returns a standardized logger instance."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
