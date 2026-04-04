{{
    config(
        schema='gold',
        materialized='incremental',
        incremental_strategy='append'
    )
}}
WITH delays AS (
    SELECT
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
        vehicle_id,
        ROW_NUMBER() OVER(PARTITION BY STOP_ID, ROUTE_ID, TRIP_ID, trip_starttime ORDER BY current_datetime DESC) AS rn 
    
    FROM {{ ref('silver_delays') }}
)

SELECT

    {{ dbt_utils.generate_surrogate_key([
        'd.trip_id',
        'd.stop_id',
        'd.vehicle_code',
        'd.event_datetime'
    ]) }} AS event_id,

    d.stop_id,
    d.route_id,
    d.trip_id,
    d.vehicle_code,
    t.trip_headsign AS headsign,

    d.event_datetime,

    d.scheduled_departure_time,
    d.est_departure_time,
    d.trip_starttime,

    d.delay_in_seconds,
    d.delay_in_seconds / 60 AS delay_minutes,

    d.status,
    d.vehicle_service,
    d.vehicle_id

FROM delays d
JOIN {{ ref('dim_stops') }} s
    ON d.stop_id = s.stop_id
LEFT JOIN {{ ref('dim_trips') }} t
    ON d.trip_id = t.trip_id
    AND d.route_id = t.route_id
WHERE d.rn = 1

{% if is_incremental() %}

AND d.event_datetime >
(
    SELECT MAX(event_datetime)
    FROM {{ this }}
)

{% endif %}