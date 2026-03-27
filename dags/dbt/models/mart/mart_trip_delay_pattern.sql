{{
    config(
        schema='mart',
        materialized='view'
    )
}}

WITH fct_delays AS (
    SELECT
        r.route_short_name AS route_number,
        t.trip_headsign,
        DAYNAME(d.event_datetime) AS weekday,
        TO_TIME(d.trip_starttime) AS trip_starttime,
        d.delay_in_seconds

    FROM {{ ref('fct_delays') }} d

    LEFT JOIN {{ ref('dim_stops') }} s
        ON d.stop_id = s.stop_id

    LEFT JOIN {{ ref('dim_routes') }} r
        ON d.route_id = r.route_id

    LEFT JOIN {{ ref('dim_trips') }} t
        ON d.trip_id = t.trip_id
        AND d.route_id = t.route_id
)

SELECT
    route_number,
    trip_headsign,
    weekday,
    trip_starttime,
    AVG(delay_in_seconds) AS mean_delay,
    COUNT(*) AS observations
FROM fct_delays
GROUP BY
    route_number,
    weekday,
    trip_headsign,
    trip_starttime

ORDER BY
    route_number,
    weekday,
    trip_headsign,
    trip_starttime