{{
    config(
        schema='silver',
        materialized='table',
    )
}}
WITH bronze_trips AS (
    SELECT
        "ID"::VARCHAR(10) AS id,
        "ROUTEID"::SMALLINT AS route_id,
        "TRIPID"::SMALLINT AS trip_id,
        
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE("TRIPHEADSIGN", '\\s*\\([^)]*\\)', ''),
                        '^[^A-Za-z0-9]+', ''
                    ),
                    '\\s*>\\s*', ' - '
                ),
                '\\s+', ' '
            )
        )::VARCHAR(100) AS trip_headsign,

        "TRIPSHORTNAME"::VARCHAR(3) AS trip_short_name,
        "DIRECTIONID"::SMALLINT AS direction_id,
        "ACTIVATIONDATE"::DATE AS activation_date,
        "TYPE"::VARCHAR(15) AS type
    FROM
        {{ source('BUSES', 'BRONZE_TRIPS') }}
),
     deduplicated_data AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id, trip_headsign
                ORDER BY activation_date DESC
            ) AS rn
        FROM bronze_trips
     )
SELECT
    id,
    route_id,
    trip_id,
    trip_headsign,
    trip_short_name,
    direction_id,
    activation_date,
    type
FROM
    deduplicated_data
WHERE rn = 1
ORDER BY route_id
