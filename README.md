# ⚓ AIS Voyage Engine

A professional-grade, cloud-native data pipeline that transforms raw NOAA AIS pings into enriched maritime voyage insights.

## 🏗 Architecture (Medallion)
- **Bronze Layer**: Raw AIS pings filtered by port proximity (5km) using Polars & SciPy cKDTree.
- **Silver Layer**: Stitched port events (Arrivals/Departures) using DuckDB window functions.
- **Gold Layer**: Enriched voyages with maritime distances calculated via parallelized cloud workers.

## 🛠 Tech Stack
- **Compute**: [Modal](https://modal.com) (Serverless Python)
- **Storage**: Modal Volumes (Persistent Data Lake)
- **Engines**: Polars (Memory-efficient processing) & DuckDB (OLAP Analytics)
- **UI**: Streamlit + Pydeck (3D Geospatial Dashboard)

## 🚀 Getting Started
1. **Setup**: `pip install -e ".[dev]"`
2. **Modal Login**: `modal setup`
3. **Deploy Pipeline**: `modal deploy app.py`
4. **Run Dashboard**: `modal run app.py::ui`

## 🧪 Testing
Run the test suite locally:
```bash
pytest tests/
