-- depends_on: {{ ref('dim_vehicles') }}

{{ config(
    schema='gold',
    materialized='incremental',
    incremental_strategy='append'
) }}

SELECT

    {{ dbt_utils.generate_surrogate_key([
    'p.vehicle_code',
    'p.generated_at',
    'p.latitude',
    'p.longitude'
    ]) }} AS position_event_id,

    p.vehicle_code,
    p.route_id,
    p.trip_id,

    DATE(p.generated_at) AS event_date,
    p.generated_at AS event_datetime,

    p.latitude,
    p.longitude,

    p.speed,
    p.direction,

    p.delay_in_seconds,
    p.gps_quality,

    p.vehicle_id,
    p.vehicle_service,
    p.trip_starttime

FROM {{ ref('silver_positions') }} p 
JOIN {{ ref('dim_vehicles')}} v
ON p.vehicle_code = v.vehicle_code

{% if is_incremental() %}

WHERE generated_at >
(
SELECT MAX(event_datetime)
FROM {{ this }}
)

{% endif %}