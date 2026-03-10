{{
    config(
        schema='gold',
        materialized='incremental',
        incremental_strategy='append'
    )
}}

SELECT

    {{ dbt_utils.generate_surrogate_key([
    'trip_id',
    'stop_id',
    'vehicle_code',
    'current_datetime'
    ]) }} AS event_id,

    stop_id,
    route_id,
    trip_id,
    vehicle_code,
    headsign,

    current_datetime AS event_datetime,

    scheduled_departure_time,
    est_departure_time,
    trip_starttime,

    delay_in_seconds,
    delay_in_seconds / 60 AS delay_minutes,

    status,
    vehicle_service,
    vehicle_id

FROM {{ ref('silver_delays') }}

{% if is_incremental() %}

WHERE current_datetime >
(
    SELECT MAX(event_datetime)
    FROM {{ this }}
)

{% endif %}