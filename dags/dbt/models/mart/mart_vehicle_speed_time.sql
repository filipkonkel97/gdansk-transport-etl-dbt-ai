{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT
    DATE_TRUNC('hour', TO_TIME(event_datetime)) AS event_datetime,
    AVG(speed) AS avg_speed,
    MIN(speed) AS min_speed,
    MAX(speed) AS max_speed
FROM {{ ref('fct_positions') }}
GROUP BY DATE_TRUNC('hour', TO_TIME(event_datetime))
ORDER BY DATE_TRUNC('hour', TO_TIME(event_datetime))