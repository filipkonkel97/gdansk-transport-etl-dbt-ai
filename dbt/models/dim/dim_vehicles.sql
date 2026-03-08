{{
    config(
        materialized='incremental',
        unique_key=['vehicle_code'],
        incremental_strategy='merge',
    )
}}
SELECT
    vehicle_code,
    carrier,
    transportation_type,
    vehicle_characteristics,
    production_year,
    seats,
    standing_places,
    air_conditioning,
    floor_height,
    kneeling_mechanism,
    wheelchairs_ramp,
    voice_announcements,
    drive_type
FROM
    {{ ref('scd_silver_vehicles') }}
WHERE
    dbt_valid_to IS NULL
