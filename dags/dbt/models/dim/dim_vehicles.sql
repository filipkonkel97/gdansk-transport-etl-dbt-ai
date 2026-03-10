{{
    config(
        materialized='incremental',
        unique_key='vehicle_code',
        incremental_strategy='merge',
    )
}}

<<<<<<< HEAD
WITH source_data AS (
=======
WITH source_date AS (
>>>>>>> 23fecee (feat: add new fact models (fct))
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
        drive_type,
        dbt_updated_at
    FROM
        {{ ref('scd_silver_vehicles') }}
    WHERE
        dbt_valid_to IS NULL
),

max_loaded AS (

    SELECT COALESCE(MAX(dbt_updated_at), '1900-01-01') AS max_updated
<<<<<<< HEAD
    FROM source_data
=======
    FROM {{ this }}
>>>>>>> 23fecee (feat: add new fact models (fct))

)

SELECT *
FROM source_data

{% if is_incremental() %}

WHERE dbt_updated_at > (SELECT max_updated FROM max_loaded)

{% endif %}