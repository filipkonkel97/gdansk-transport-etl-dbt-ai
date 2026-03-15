{{
    config(
        schema='mart',
        materialized='view'
    )
}}

WITH fct_delays AS (

SELECT
    d.stop_id,
    COALESCE(s.stop_name, s.stop_desc) AS stop_name,
    d.route_id,
    r.route_short_name AS route_number,
    d.trip_id,
    t.trip_headsign,
    d.vehicle_code,

    TO_TIME(d.trip_starttime) AS trip_starttime,
    TO_TIME(d.scheduled_departure_time) AS scheduled_departure_time,

    d.event_datetime,
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
    route_id,
    route_number,
    trip_id,
    trip_headsign,
    stop_id,
    stop_name,
    trip_starttime,
    scheduled_departure_time,

    AVG(delay_in_seconds) AS mean_delay,
    MAX(delay_in_seconds) AS max_delay,
    MIN(delay_in_seconds) AS min_delay,
    COUNT(*) AS observations

FROM fct_delays

GROUP BY
    route_id,
    route_number,
    trip_id,
    trip_headsign,
    stop_id,
    stop_name,
    trip_starttime,
    scheduled_departure_time

ORDER BY
    route_number,
    route_id,
    trip_starttime,
    scheduled_departure_time