import streamlit as st
from src.db.snowflakedb import SnowflakeDB
from src.plots.plots_repository import PlotsRepository
from src.db.queries import get_delay_per_hour, get_route_variant_stop_hourly_delay, get_avg_delay_by_day_hour, get_stop_delays

db = SnowflakeDB()
plt = PlotsRepository()

st.set_page_config(layout="wide")
st.title("Bus Delay Analytics + AI Assistant")

tab1, tab2, tab3, tab4 = st.tabs(["Home", "Analytics", "Stops", "Settings"])

with tab1:
    # ========== DASHBOARD ==========
    st.header("Mean delay per hour")

    df = get_delay_per_hour(db)

    fig = plt.plot_bar(df = df, x="hour", y="avg_delay", title="Average delay per hour")

    st.plotly_chart(fig, use_container_width=True)

    # ========== DASHBOARD ==========
    st.header("Mean delay by day and hour")

    df_2 = get_avg_delay_by_day_hour(db)

    weekday = st.selectbox(
        "Day of week", 
        df_2["day_of_week"].unique()
        )
    
    df_weekday = df_2[df_2["day_of_week"] == weekday]

    fig = plt.plot_bar(df = df_weekday, x="day_time", y="mean_delay", title=f"Average delay per hour for {weekday}")

    st.plotly_chart(fig, use_container_width=True)


with tab2:
    # ========== DASHBOARD ==========
    st.header("Stops locations per route")

    df_1 = get_route_variant_stop_hourly_delay(db)

    route = st.selectbox(
        "Route", 
        df_1["route_short_name"].unique()
        )

    df_route = df_1[df_1["route_short_name"] == route]

    headsign = st.selectbox(
        "Headsign", 
        sorted(df_route["headsign"].unique())
        )

    df_headsign = df_route[df_route["headsign"] == headsign]

    trip_starttime = st.selectbox(
        "Select trip start time",
        sorted(df_headsign["trip_starttime"].unique()),
        key = 'trip_starttime'
    )

    st.write(f"Selected trip start time: {trip_starttime}")

    filtered_df = df_headsign[
        df_headsign["trip_starttime"] == trip_starttime
    ]

    r = plt.plot_map(
            df=filtered_df, 
            tooltip={"html": "Stop name: {stop_name} <br> Mean delay (s): {mean_delay}"}
            )

    st.pydeck_chart(r, use_container_width=True, width='stretch')


with tab3:
    # ========== DASHBOARD ==========
    st.header("Stops locations and delays")

    df_stops_delays = get_stop_delays(db)

    r = plt.plot_map(
        df=df_stops_delays, 
        tooltip={"html": "Stop name: {stop_name} <br> Mean delay (s): {mean_delay}"},
        radius= 'mean_delay * 0.1',
        lat=54.45,
        long=18.55
        )

    st.pydeck_chart(r, use_container_width=True, width='stretch')

with tab4:
    st.header("Ustawienia")
    st.write("Tutaj konfigurujesz aplikację")