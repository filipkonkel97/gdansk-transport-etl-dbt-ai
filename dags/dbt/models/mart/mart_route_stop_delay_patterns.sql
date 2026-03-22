{{
    config(
        schema='mart',
        materialized='view'
    )
}}

WITH delays AS (
    SELECT
        headsign,
        route_id,
        stop_id,
        DATE_TRUNC('hour', TO_TIME(scheduled_departure_time)) AS departure_hour,
        DATE_PART(dow, event_datetime) AS weekday,
        delay_in_seconds
    FROM buses.buses_gold.fct_delays
)
SELECT
    r.route_short_name,
    d.headsign,
    d.route_id,
    d.stop_id,
    d.departure_hour,
    d.weekday,
    AVG(d.delay_in_seconds) AS avg_delay
FROM delays d
LEFT JOIN buses.buses_gold.dim_routes r
    ON r.route_id = d.route_id
GROUP BY
    r.route_short_name,
    d.headsign,
    d.route_id,
    d.stop_id,
    d.departure_hour,
    d.weekday
ORDER BY HEADSIGN, WEEKDAY, DEPARTURE_HOUR, STOP_ID