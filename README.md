# ⚓ AIS Voyage Engine

A professional-grade, cloud-native data pipeline that transforms raw NOAA AIS pings into enriched maritime voyage insights.

## 🏗 Architecture (Medallion)
- **Bronze Layer**: Raw AIS pings filtered by port proximity (5km) using Polars & SciPy cKDTree.
- **Silver Layer**: Stitched port events (Arrivals/Departures) using dbt & SQL.
- **Gold Layer**: Enriched voyages with maritime distances calculated via dbt Python models.

## 🛠 Tech Stack
- **Orchestration**: GitHub Actions
- **Data Warehouse**: MotherDuck (Cloud DuckDB)
- **Transformation**: dbt (Data Build Tool)
- **Visualization**: Evidence.dev

## 🚀 Getting Started
1. **Environment Setup**:
   ```bash
   export MOTHERDUCK_TOKEN="your_token_here"
   pip install -e ".[dev]"
   ```

2. **Run Pipeline (Manual Backfill)**:
   ```bash
   # Ingest entire year of 2025
   python src/ingest_motherduck.py --year 2025

   # Ingest specific month
   python src/ingest_motherduck.py --year 2025 --month 1

   # Transform data (Silver -> Gold)
   dbt build --target prod
   ```

3. **Run Pipeline (Daily Automation)**:
   ```bash
   # Defaults to yesterday's data
   python src/ingest_motherduck.py
   dbt build --target prod
   ```

## 🧪 Testing
Run the test suite locally:
```bash
pytest tests/
