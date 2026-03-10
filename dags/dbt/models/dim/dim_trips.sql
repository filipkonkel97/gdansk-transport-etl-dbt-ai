{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (

    SELECT
        trip_id,
        route_id,
        trip_headsign,
        trip_short_name,
        direction_id,
        type,
        activation_date,
        dbt_updated_at
    FROM {{ ref('scd_silver_trips') }}
    WHERE dbt_valid_to IS NULL

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