{{
    config(
        materialized='incremental',
        unique_key=['route_id'],
        incremental_strategy='merge',
        partition_by={'field':'activation_date','data_type':'date'}
    )
}}
 
SELECT
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    activation_date
FROM {{ ref('scd_silver_routes') }}
WHERE DBT_VALID_TO IS NULL

{% if is_incremental() %}
    AND activation_date > (SELECT MAX(activation_date) FROM {{ this }})
{% endif %}
