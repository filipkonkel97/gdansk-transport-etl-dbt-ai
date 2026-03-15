-- depends_on: {{ ref('dim_stops') }}

{{
    config(
        schema='gold',
        materialized='incremental',
        incremental_strategy='append'
    )
}}

SELECT

    {{ dbt_utils.generate_surrogate_key([
        'd.trip_id',
        'd.stop_id',
        'd.vehicle_code',
        'd.current_datetime'
    ]) }} AS event_id,

    d.stop_id,
    d.route_id,
    d.trip_id,
    d.vehicle_code,
    d.headsign,

    d.current_datetime AS event_datetime,

    d.scheduled_departure_time,
    d.est_departure_time,
    d.trip_starttime,

    d.delay_in_seconds,
    d.delay_in_seconds / 60 AS delay_minutes,

    d.status,
    d.vehicle_service,
    d.vehicle_id

FROM {{ ref('silver_delays') }} d
JOIN {{ ref('dim_stops') }} s
ON d.stop_id = s.stop_id

{% if is_incremental() %}

AND current_datetime >
(
    SELECT MAX(event_datetime)
    FROM {{ this }}
)

{% endif %}