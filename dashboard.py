import json
import os

import folium
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium


@st.cache_data(ttl=3600)
def load_data(url: str) -> pd.DataFrame:
    """Load voyage data directly from GitHub."""
    storage_options = None
    token = os.environ.get("GH_TOKEN")
    if token:
        storage_options = {"Authorization": f"token {token}"}

    try:
        return pd.read_parquet(url, storage_options=storage_options)
    except Exception as e:
        st.error(f"❌ Error loading data: {e}")
        return pd.DataFrame()


def render_vessel_view(data: pd.DataFrame, search_imo_name: str, search_locode: str) -> None:
    """Render the vessel tracking visualization."""
    # Filter by minimum distance
    if search_locode:
        filtered_df = data[data["dep_locode"].astype(str).str.upper() == search_locode].copy()
    else:
        filtered_df = data.copy()

    # Filter by search query if provided
    if search_imo_name:
        search_upper = search_imo_name.upper()
        is_ship = filtered_df["imo"].astype(str).str.upper().str.contains(
            search_upper
        ) | filtered_df["vessel_name"].astype(str).str.upper().str.contains(search_upper)
        filtered_df = filtered_df[is_ship]

    # LIMIT: If no filters are active, limit to 100 rows to prevent browser crash on map render
    if not search_imo_name and not search_locode:
        st.info("ℹ️ Showing latest 100 voyages (Global View). Use filters to see specific data.")
        filtered_df = filtered_df.head(100)

    # Display metrics and info per ship
    if search_imo_name and not filtered_df.empty:
        ship_name = filtered_df["vessel_name"].iloc[0]
        imo_num = filtered_df["imo"].iloc[0]
        if not search_locode:
            st.success(f"📍 Tracking: **{ship_name}** (IMO: {imo_num})")
        else:
            st.success(
                f"""📍 Tracking: **{ship_name}** (IMO: {imo_num})
                        &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
                        Departing from **{search_locode}**"""
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Voyages this Month", len(filtered_df))
        with col2:
            total_dist = filtered_df["distance_nm"].sum()
            st.metric("Total Distance (nm)", f"{total_dist:,.0f}")
        with col3:
            avg_speed = filtered_df["avg_speed_kts"].mean()
            st.metric("Avg Speed (kts)", f"{avg_speed:.1f}")
    elif search_imo_name:
        st.error(f"🚫 No voyages found for '{search_imo_name}' with current filters.")

    # Display metrics for location-based search
    if search_locode and not filtered_df.empty:
        ship_count = filtered_df["mmsi"].nunique()
        ships_with_imo = filtered_df["imo"].nunique()
        ship_visits = len(filtered_df)

        if not search_imo_name:
            st.success(f"📍 Voyages departing from **{search_locode}**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Unique Ships", ship_count)
            with col2:
                st.metric("Ships with IMO", ships_with_imo)
            with col3:
                st.metric("Total # of Voyages", ship_visits)
    elif search_locode:
        st.error(f"🚫 No voyages found departing from **{search_locode}**.")

    st.dataframe(filtered_df)
    # Create visualization
    if not filtered_df.empty:
        # 1. Initialize the Folium Map
        # Navigation-night style equivalent in Folium is usually CartoDB dark_matter
        m = folium.Map(location=[20, 0], zoom_start=2, control_scale=True, tiles="cartodb positron")
        folium.TileLayer(
            tiles="https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png",
            attr=(
                "Map data: &copy; <a href='http://www.openseamap.org'>OpenSeaMap</a>"
                " contributors"
            ),
            name="OpenSeaMap",
            overlay=True,
        ).add_to(m)
        all_bounds = []

        for _, row in filtered_df.iterrows():
            try:
                path_coords = []

                # 2. Use Pre-calculated Path from Pipeline
                # The 'path' column contains a GeoJSON string of the LineString
                if pd.notnull(row.get("path")):
                    geo = json.loads(row["path"])
                    # GeoJSON is [lon, lat], Folium needs [lat, lon]
                    if geo.get("type") == "LineString" and "coordinates" in geo:
                        path_coords = [(p[1], p[0]) for p in geo["coordinates"]]
                    elif geo.get("type") == "MultiLineString" and "coordinates" in geo:
                        path_coords = [
                            [(p[1], p[0]) for p in segment] for segment in geo["coordinates"]
                        ]

                # Fallback if path is missing (e.g. enrichment failed)
                if not path_coords:
                    # Draw simple straight line
                    path_coords = [
                        [row["dep_lat"], row["dep_lon"]],
                        [row["arr_lat"], row["arr_lon"]],
                    ]

                # 4. Create the PolyLine
                # Collect points for auto-centering
                if path_coords:
                    if isinstance(path_coords[0][0], (float, int)):
                        all_bounds.extend(path_coords)
                    else:
                        for segment in path_coords:
                            all_bounds.extend(segment)

                color = "#FFFF00" if search_imo_name else "#00FF80"
                weight = 4 if search_imo_name else 2

                folium.PolyLine(
                    locations=path_coords,
                    color=color,
                    weight=weight,
                    opacity=0.8,
                    tooltip=(
                        f"Vessel: {row['vessel_name']}<br>"
                        f"From: {row['dep_locode']} to {row['arr_locode']}"
                    ),
                ).add_to(m)

            except Exception as e:
                st.error(f"Error routing {row['vessel_name']}: {e}")

        if all_bounds:
            m.fit_bounds(all_bounds)
        folium.LayerControl().add_to(m)

        # 5. Render the Map
        st_folium(m, width=None, height=500, returned_objects=[])

    else:
        st.info("ℹ️ No data to display with current filters.")


def main() -> None:
    """Main dashboard application."""
    st.set_page_config(page_title="AIS Fleet Tracker", layout="wide", page_icon="⚓")

    # GitHub Configuration in Sidebar
    with st.sidebar.expander("⚙️ Data Source Config", expanded=False):
        repo_owner = st.text_input(
            "Repo Owner", value=os.environ.get("REPO_OWNER", "your-github-username")
        )
        repo_name = st.text_input(
            "Repo Name", value=os.environ.get("REPO_NAME", "ais-voyage-engine")
        )
        branch = st.text_input("Branch", value="main")
        path = st.text_input("Path", value="data/gold/voyages.parquet")

    data_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/{branch}/{path}"

    df = load_data(data_url)
    if df.empty:
        st.warning(f"⚠️ No data found at: {data_url}")
        return

    # Add Date Filter
    if "dep_time" in df.columns:
        df["dep_time"] = pd.to_datetime(df["dep_time"])
        min_date = df["dep_time"].min().date()
        max_date = df["dep_time"].max().date()

        st.sidebar.header("🗓️ Date Filter")
        date_range = st.sidebar.date_input(
            "Filter by Departure Date",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if len(date_range) == 2:
            start_date, end_date = date_range
            df = df[(df["dep_time"].dt.date >= start_date) & (df["dep_time"].dt.date <= end_date)]

    st.sidebar.header("🚢 Fleet Search")

    search_imo_name = (
        st.sidebar.text_input(
            "Enter IMO or Vessel Name",
            placeholder="e.g. 9444728",
            help="Search for a specific ship to see its unique voyage history.",
        )
        .strip()
        .upper()
    )

    unique_locodes = (
        sorted(df["dep_locode"].dropna().astype(str).str.upper().unique().tolist())
        if "dep_locode" in df.columns
        else []
    )

    search_dep_locode = st.sidebar.selectbox(
        "Select Departure Location Code",
        options=[""] + unique_locodes,
        format_func=lambda x: "All" if x == "" else x,
        help="Search for voyages departing from a specific location.",
    )

    render_vessel_view(df, search_imo_name, search_dep_locode)


if __name__ == "__main__":
    main()
