{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT
    DAYNAME(est_departure_time) AS weekday,
    DATE_TRUNC('hour', TO_TIME(est_departure_time)) AS departure_time,
    AVG(DELAY_IN_SECONDS) AS avg_delay_in_seconds
FROM {{ ref('fct_delays') }}
GROUP BY DATE_PART(dow, est_departure_time), DAYNAME(est_departure_time), DATE_TRUNC('hour', TO_TIME(est_departure_time))
ORDER BY DATE_PART(dow, est_departure_time), DATE_TRUNC('hour', TO_TIME(est_departure_time))