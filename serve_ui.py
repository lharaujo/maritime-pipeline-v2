import os
import subprocess
import sys

import modal

# 1. Initialize the App
app = modal.App("ais-voyage-engine")
volume = modal.Volume.from_name("ais-data-store")

# 2. Define the Image and "Bake" the local code into it
ui_image = (
    modal.Image.debian_slim(python_version="3.12")
    .pip_install(
        "streamlit",
        "duckdb",
        "pydeck",
        "pandas",
        "polars",
        "plotly",
        "searoute",
        "folium",
        "streamlit-folium",
    )
    .add_local_dir(".", remote_path="/root")
)


@app.function(
    image=ui_image,
    volumes={"/data": volume},
    timeout=3600,
)
@modal.web_server(8000)
def serve():
    """
    Launches Streamlit with security flags for Modal's proxy.
    """
    print("🚀 Modal 1.0 Container Started.")

    # Verify files are present
    if os.path.exists("/root/dashboard.py"):
        print("✅ Found dashboard.py in /root")
    else:
        print(f"❌ CRITICAL: dashboard.py not found. Files: {os.listdir('/root')}")

    # Start Streamlit with CORS and XSRF disabled for the proxy
    subprocess.Popen(
        [
            "streamlit",
            "run",
            "/root/dashboard.py",
            "--server.port",
            "8000",
            "--server.address",
            "0.0.0.0",
            "--server.headless",
            "true",
            "--server.enableCORS",
            "false",  # Critical for Modal Proxy
            "--server.enableXsrfProtection",
            "false",  # Critical for Modal Proxy
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
    )
