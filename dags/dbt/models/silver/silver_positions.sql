{{
    config(
        schema='silver',
        materialized='incremental',
    )
}}

WITH bronze_vehicle_positions AS (
    SELECT
        CONVERT_TIMEZONE('UTC', 'Europe/Warsaw', "GENERATED") AS generated_at,
        "ROUTESHORTNAME"::VARCHAR(20) AS route_code,
        CAST("TRIPID" AS SMALLINT) AS trip_id,
        CAST("ROUTEID" AS SMALLINT) AS route_id,
        "HEADSIGN"::VARCHAR(50) AS headsign,
        CAST("VEHICLECODE" AS SMALLINT) AS vehicle_code,
        "VEHICLESERVICE"::VARCHAR(20) AS vehicle_service,
        CAST("VEHICLEID" AS INTEGER) AS vehicle_id,
        CAST("SPEED" AS SMALLINT) AS speed,
        CAST("DIRECTION" AS SMALLINT) AS direction,
        CAST("DELAY" AS INTEGER) AS delay_in_seconds,
        CONVERT_TIMEZONE('UTC', 'Europe/Warsaw', "SCHEDULEDTRIPSTARTTIME") AS trip_starttime,
        "LAT"::NUMBER(10,7) AS latitude,
        "LON"::NUMBER(10,7) AS longitude,
        CAST("GPSQUALITY" AS SMALLINT) AS gps_quality
    FROM
        {{ source('BUSES', 'BRONZE_POSITIONS') }}
)

SELECT *
FROM bronze_vehicle_positions