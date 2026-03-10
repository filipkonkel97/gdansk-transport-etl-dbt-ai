{{ config(
    schema='gold',
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT

    {{ dbt_utils.generate_surrogate_key([
    'vehicle_code',
    'generated_at',
    'latitude',
    'longitude'
    ]) }} AS position_event_id,

    vehicle_code,
    route_id,
    trip_id,

    DATE(generated_at) AS event_date,
    generated_at AS event_datetime,

    latitude,
    longitude,

    speed,
    direction,

    delay_in_seconds,
    gps_quality,

    vehicle_id,
    vehicle_service,
    trip_starttime

FROM {{ ref('silver_positions') }}

{% if is_incremental() %}

WHERE generated_at >
(
SELECT MAX(event_datetime)
FROM {{ this }}
)

{% endif %}