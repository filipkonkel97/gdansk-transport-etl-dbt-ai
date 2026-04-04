{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT
    r.route_short_name,
    AVG(delay_in_seconds) AS avg_delay
FROM {{ ref('fct_delays') }} d
LEFT JOIN {{ ref('dim_routes') }} r
    ON d.route_id = r.route_id
GROUP BY r.route_short_name
ORDER BY AVG_DELAY DESC
LIMIT 10