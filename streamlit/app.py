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
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["System Overview", "Route Analysis", "Stops Analysis", "Realtime", "Chatbot"]
)

with tab1:
    # ========== DASHBOARD ==========
    st.header("System Overview")

    col1, col2 = st.columns(2)

    with col1:
        df_delays = get_delay_per_hour()

        fig = plt.plot_bar(
            df=df_delays, x="hour", y="avg_delay", title="Average delay per hour"
        )

        st.plotly_chart(fig, use_container_width=True)

    # ========== DASHBOARD ==========
    with col2:
        df_delays_day = get_avg_delay_by_day_hour()

        weekday = st.selectbox("Day of week", df_delays_day["day_of_week"].unique())

        df_weekday = df_delays_day[df_delays_day["day_of_week"] == weekday]

        fig = plt.plot_bar(
            df=df_weekday,
            x="day_time",
            y="mean_delay",
            title=f"Average delay per hour for {weekday}",
        )

        st.plotly_chart(fig, use_container_width=True)

with tab2:
    # ========== DASHBOARD ==========
    st.header("Route Analysis")

    df_routes = get_route_variant_stop_hourly_delay()

    df_routes.columns = df_routes.columns.str.upper()

    route = st.selectbox("Route", df_routes["ROUTE_SHORT_NAME"].unique())

    df_route = df_routes[df_routes["ROUTE_SHORT_NAME"] == route]

    headsign = st.selectbox("Direction", sorted(df_route["HEADSIGN"].unique()))

    df_headsign = df_route[df_route["HEADSIGN"] == headsign]

    trip = st.selectbox(
        "Trip",
        sorted(df_headsign["TRIP_STARTTIME"].unique()),
        key="trip_starttime",
    )

    filtered = df_headsign[df_headsign["TRIP_STARTTIME"] == trip]

    st.subheader("Route map")

    r = map_plotter.plot_map(
        df=filtered,
        tooltip={"html": "Stop name: {STOP_NAME} <br> Mean delay (s): {MEAN_DELAY}"},
    )

    st.pydeck_chart(r, use_container_width=True, width="stretch")

with tab3:
    # ========== DASHBOARD ==========
    st.header("Stops Analysis")

    df_stops_delays = get_stop_delays()

    df_stops_delays.columns = df_stops_delays.columns.str.upper()

    col1, col2 = st.columns([2, 1])

    with col1:
        r = map_plotter.plot_map(
            df=df_stops_delays,
            tooltip={
                "html": "Stop name: {STOP_NAME} <br> Mean delay (s): {MEAN_DELAY}"
            },
            radius="MEAN_DELAY * 0.1",
        )

        st.pydeck_chart(r, use_container_width=True, width="stretch")

    with col2:
        st.subheader("Worst stops")

        st.dataframe(
            df_stops_delays.sort_values("MEAN_DELAY", ascending=False).head(10)
        )

with tab4:
    st.header("Realtime vehicles")

    st_autorefresh(interval=5000, key="refresh")

    df_bus = live_bus.get_data()

    df_realtime = df_bus[
        ["ROUTESHORTNAME", "HEADSIGN", "SPEED", "DIRECTION", "DELAY", "VEHICLECODE"]
    ]

    col1, col2 = st.columns([2, 1])

    with col1:
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

    with col2:
        st.subheader("Live data")

        st.dataframe(df_realtime.sort_values("DELAY", ascending=False).head(10))

with tab5:
    st.title("Echo Bot")

    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat messages from history on app rerun
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # React to user input
    if prompt := st.chat_input("What is up?"):
        # Display user message in chat message container
        st.chat_message("user").markdown(prompt)
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})

        response = f"Echo: {prompt}"
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            st.markdown(response)
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})
