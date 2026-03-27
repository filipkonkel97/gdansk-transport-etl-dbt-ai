{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT 
    r.ROUTE_SHORT_NAME,
    d.HEADSIGN,
    TO_TIME(d.TRIP_STARTTIME) AS trip_starttime,
    s.STOP_NAME,
    s.stop_lat,
    s.stop_lon,
    AVG(d.DELAY_IN_SECONDS) AS MEAN_DELAY
FROM {{ ref('fct_delays') }} d
LEFT JOIN {{ ref('dim_routes') }} r
    ON d.ROUTE_ID = r.ROUTE_ID
LEFT JOIN {{ ref('dim_stops') }} s
    ON s.STOP_ID = d.STOP_ID
GROUP BY
    r.route_short_name,
    d.headsign,
    TO_TIME(d.TRIP_STARTTIME),
    s.STOP_NAME,
    s.stop_lat,
    s.stop_lon
ORDER BY
    r.route_short_name,
    d.headsign,
    TO_TIME(d.TRIP_STARTTIME)