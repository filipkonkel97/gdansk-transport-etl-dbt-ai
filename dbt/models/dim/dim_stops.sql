{{
    config(
        materialized='incremental',
        unique_key=['stop_id'],
        incremental_strategy='merge',
        partition_by={'field':'activation_date','data_type':'date'}
    )
}}

SELECT
    stop_id,
    stop_code,
    stop_name,
    short_name,
    stop_desc,
    sub_name,
    zone_id,
    zone_name,
    depot,
    on_demand,
    stop_lat,
    stop_lon,
    activation_date
FROM {{ ref('scd_silver_stops') }}
WHERE DBT_VALID_TO IS NULL

{% if is_incremental() %}
    AND activation_date > (SELECT MAX(activation_date) FROM {{ this }})
{% endif %}
