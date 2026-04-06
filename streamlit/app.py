from src.data.live_bus import LiveBusData
from src.db.queries import (
    get_avg_delay_by_day_hour,
    get_delay_per_hour,
    get_route_variant_stop_hourly_delay,
    get_stop_delays,
)
from src.db.snowflake_conn import SnowflakeDB
from src.plots.bar_plotter import BarPlotter
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
plt = BarPlotter()
live_bus = LiveBusData()

st.set_page_config(layout="wide")
st.title("Bus Delay Analytics + AI Assistant")
tab1, tab2, tab3, tab4 = st.tabs(["Home", "Analytics", "Stops", "Settings"])

with tab1:
    # ========== DASHBOARD ==========
    st.header("Mean delay per hour")
    df = get_delay_per_hour()

    fig = plt.plot_bar(df=df, x="hour", y="avg_delay", title="Average delay per hour")

    st.plotly_chart(fig, use_container_width=True)

    # ========== DASHBOARD ==========
    st.header("Mean delay by day and hour")

    df_2 = get_avg_delay_by_day_hour()

    weekday = st.selectbox("Day of week", df_2["day_of_week"].unique())

    df_weekday = df_2[df_2["day_of_week"] == weekday]

    fig = plt.plot_bar(
        df=df_weekday,
        x="day_time",
        y="mean_delay",
        title=f"Average delay per hour for {weekday}",
    )

    st.plotly_chart(fig, use_container_width=True)

with tab2:
    # ========== DASHBOARD ==========
    st.header("Stops locations per route")

    df_1 = get_route_variant_stop_hourly_delay()

    df_1.columns = df_1.columns.str.upper()

    route = st.selectbox("Route", df_1["ROUTE_SHORT_NAME"].unique())

    df_route = df_1[df_1["ROUTE_SHORT_NAME"] == route]

    headsign = st.selectbox("Headsign", sorted(df_route["HEADSIGN"].unique()))

    df_headsign = df_route[df_route["HEADSIGN"] == headsign]

    trip_starttime = st.selectbox(
        "Select trip start time",
        sorted(df_headsign["TRIP_STARTTIME"].unique()),
        key="trip_starttime",
    )

    st.write(f"Selected trip start time: {trip_starttime}")

    filtered_df = df_headsign[df_headsign["TRIP_STARTTIME"] == trip_starttime]

    r = map_plotter.plot_map(
        df=filtered_df,
        tooltip={"html": "Stop name: {STOP_NAME} <br> Mean delay (s): {MEAN_DELAY}"},
    )

    st.pydeck_chart(r, use_container_width=True, width="stretch")

with tab3:
    # ========== DASHBOARD ==========
    st.header("Stops locations and delays")

    df_stops_delays = get_stop_delays()

    df_stops_delays.columns = df_stops_delays.columns.str.upper()

    r = map_plotter.plot_map(
        df=df_stops_delays,
        tooltip={"html": "Stop name: {STOP_NAME} <br> Mean delay (s): {MEAN_DELAY}"},
        radius="MEAN_DELAY * 0.1",
    )

    st.pydeck_chart(r, use_container_width=True, width="stretch")

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
