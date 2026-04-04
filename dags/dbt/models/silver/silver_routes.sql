{{
    config(
        schema='silver',
        materialized='table',
    )
}}
WITH bronze_routes AS (
    SELECT
        "ROUTEID"::INTEGER AS route_id,
        "AGENCYID"::INTEGER AS agency_id,
        "ROUTESHORTNAME"::VARCHAR(10) AS route_short_name,

        CASE
            WHEN LEFT("ROUTELONGNAME", 3) = '...'
            THEN SUBSTR("ROUTELONGNAME", 7, LENGTH("ROUTELONGNAME"))
            ELSE "ROUTELONGNAME"
        END::VARCHAR(60) AS route_long_name,

        "ACTIVATIONDATE"::DATE AS activation_date,
        "ROUTETYPE"::VARCHAR(20) AS route_type
    FROM
        {{ source('BUSES', 'BRONZE_ROUTES') }}
),
     deduplicated_data AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY route_id, route_short_name, route_long_name
                ORDER BY activation_date DESC
            ) AS rn
        FROM bronze_routes
     )

SELECT
    route_id,
    agency_id,
    route_short_name,
    route_long_name,
    activation_date,
    route_type
FROM
    deduplicated_data
WHERE rn = 1
ORDER BY route_id