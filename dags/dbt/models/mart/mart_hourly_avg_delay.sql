{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT
    EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE')) AS hour,
    AVG(DELAY_IN_SECONDS) AS avg_delay
FROM {{ ref('fct_delays') }}
GROUP BY EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE'))
ORDER BY EXTRACT(HOUR FROM DATE_TRUNC('HOUR', event_datetime + INTERVAL '30 MINUTE'))