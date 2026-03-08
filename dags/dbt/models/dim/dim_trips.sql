{{
    config(
        materialized='incremental',
        unique_key=['trip_id'],
        incremental_strategy='merge',
        partition_by={'field':'activation_date','data_type':'date'}
    )
}}
         
SELECT
    trip_id,
    route_id,
    trip_headsign,
    trip_short_name,
    direction_id,
    type,
    activation_date
FROM
    {{ ref('scd_silver_trips') }}
WHERE DBT_VALID_TO IS NULL

{% if is_incremental() %}
    AND activation_date > (SELECT MAX(activation_date) FROM {{ this }})
{% endif %}
