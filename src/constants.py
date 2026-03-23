"""
Global constants for AIS Voyage Engine.
Centralizes magic numbers and configuration values.
"""

# Geography & Physics
EARTH_RADIUS_KM = 6371.0
PORT_PROXIMITY_RADIUS_KM = 5.0
KM_TO_NM_CONVERSION = 1.852

# Coordinate Precision
COORDINATE_PRECISION_DECIMALS = 10  # ~11 meter precision

# Path Optimization
PATH_DOWNSAMPLING_CONFIG = {
    "long": {"threshold": 100, "factor": 3},  # Keep every 3rd point if >100 points
    "medium": {"threshold": 30, "factor": 2},  # Keep every 2nd point if >30 points
}

# Logging
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
LOG_LEVEL = "INFO"

# Workers & Retries
WORKER_MAX_RETRIES = 6
WORKER_RETRY_DELAY_SECONDS = 3
WORKER_MAX_WAIT_SECONDS = 30

# Scraping
DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; AIS-Voyage-Engine/1.0)"
SCRAPE_TIMEOUT_SECONDS = 15

# Modal Configuration
MODAL_MEMORY_MB = {
    "extract": 4096,
    "transform": 2048,
    "metrics": 1024,
}
MODAL_TIMEOUT_SECONDS = {
    "extract": 600,
    "bootstrap": 1200,
    "pipeline": 3600,
}

# Port Filtering
PORT_STATUS_CODES = ["AA", "AC", "AF", "AI", "AM", "AS", "AQ", "RL", "RQ", "RN", "QQ"]
PORT_FUNCTION_FILTER = "1"  # Maritime function code
