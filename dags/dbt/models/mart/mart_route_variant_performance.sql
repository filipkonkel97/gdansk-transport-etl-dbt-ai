{{
    config(
        schema='mart',
        materialized='view'
    )
}}

SELECT
    ROUTE_SHORT_NAME,
    headsign,
    AVG(delay_in_seconds) AS avg_delay,
    MAX(delay_in_seconds) AS max_delay,
    COUNT(*) AS events_count
FROM BUSES.BUSES_GOLD.FCT_DELAYS d
LEFT JOIN BUSES.BUSES_GOLD.DIM_ROUTES r
    ON d.ROUTE_ID = r.ROUTE_ID
GROUP BY r.ROUTE_SHORT_NAME, d.headsign