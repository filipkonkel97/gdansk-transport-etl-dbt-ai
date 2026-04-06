{{
    config(
        schema='mart',
        materialized='view'
    )
}}
SELECT
    DAYOFWEEKISO(event_datetime) as day_of_week_num,
    DAYNAME(event_datetime) AS day_of_week,
    EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE')) AS day_time,
    AVG(DELAY_IN_SECONDS) AS mean_delay
FROM {{ ref('fct_delays') }}
GROUP BY DAYOFWEEKISO(event_datetime),
        DAYNAME(event_datetime),
        EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE'))
ORDER BY DAYOFWEEKISO(event_datetime),
        DAYNAME(event_datetime),
        EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE'))