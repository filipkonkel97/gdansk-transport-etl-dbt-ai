import streamlit as st

@st.cache_data(ttl=3600)
def get_delay_per_hour(_db):
    return _db.query("""
        SELECT 
            *
        FROM buses.buses_mart.mart_hourly_avg_delay
        """)

@st.cache_data(ttl=3600)
def get_route_variant_stop_hourly_delay(_db):
        return _db.query("""
        SELECT 
            ROUTE_SHORT_NAME AS route_short_name,
            HEADSIGN AS headsign,
            CAST(TRIP_STARTTIME AS STRING) AS trip_starttime,
            STOP_NAME AS stop_name,
            STOP_LAT AS lat,
            STOP_LON AS lon,
            MEAN_DELAY AS mean_delay
        FROM BUSES.BUSES_MART.mart_route_variant_stop_hourly_delay
        """)

@st.cache_data(ttl=3600)
def get_avg_delay_by_day_hour(_db):
        return _db.query("""
        SELECT 
            day_of_week,
            day_time,
            mean_delay
        FROM BUSES.BUSES_MART.mart_avg_delay_by_day_hour
        """)

@st.cache_data(ttl=3600)
def get_stop_delays(_db):
        return _db.query("""
        SELECT
            s.STOP_DESC AS STOP_NAME,
            s.STOP_LAT AS LAT,
            s.STOP_LON AS LON,
            AVG(d.delay_in_seconds) AS mean_delay
        FROM BUSES.BUSES_GOLD.FCT_DELAYS d
        LEFT JOIN BUSES.BUSES_GOLD.DIM_STOPS s
            ON d.STOP_ID = s.STOP_ID
        GROUP BY s.STOP_DESC,
            s.STOP_LAT,
            s.STOP_LON
        """)