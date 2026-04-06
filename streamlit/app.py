from src.data.live_bus import LiveBusData
from src.db.snowflake_conn import SnowflakeDB
from src.plots.map_plotter import MapPlotter
from streamlit_autorefresh import st_autorefresh

import streamlit as st

# init DB
db = SnowflakeDB(
    user=st.secrets["user"],
    password=st.secrets["password"],
    account=st.secrets["account"],
    database=st.secrets["database"],
    schema=st.secrets["schema"],
    warehouse=st.secrets["warehouse"],
)

# init map and live bus
map_plotter = MapPlotter()
live_bus = LiveBusData()

st.set_page_config(layout="wide")
st.title("Bus Delay Analytics + AI Assistant")
tab1, tab2, tab3, tab4 = st.tabs(["Home", "Analytics", "Stops", "Settings"])

with tab4:
    st.header("Ustawienia")
    st.write("Tutaj konfigurujesz aplikację")
    st_autorefresh(interval=5000, key="map_refresh")

    df_bus = live_bus.get_data()
    st.dataframe(df_bus)

    r = map_plotter.plot_map(
        df=df_bus,
        tooltip={
            "html": """Route: {ROUTESHORTNAME} <br>
                       Headsign: {HEADSIGN} <br>
                       Delay (s): {DELAY} <br>
                       Speed (km/h): {SPEED} <br>
                       Direction: {DIRECTION} <br>
                       Vehicle: {VEHICLECODE}"""
        },
    )
    st.pydeck_chart(r)
