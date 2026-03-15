{{
    config(
        schema='silver',
        materialized='incremental',
        incremental_strategy='append'
    )
}}
WITH bronze_delays AS (
    SELECT
        "STOPID"::INTEGER AS stop_id,
        "ID"::TEXT AS id,
        "DELAYINSECONDS"::INTEGER AS delay_in_seconds,
        "ESTIMATEDTIME"::TIMESTAMP AS est_departure_time,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE("HEADSIGN", '\\s*\\([^)]*\\)', ''),
                        '^[^A-Za-z0-9ĄĆĘŁŃÓŚŹŻąćęłńóśźż]+', ''
                    ),
                    '\\s*>\\s*', ' - '
                ),
                '\\s+', ' '
            )
        )::VARCHAR(50) AS headsign,
        "ROUTEID"::SMALLINT AS route_id,
        "ROUTESHORTNAME"::TEXT AS route_code,
        "SCHEDULEDTRIPSTARTTIME"::TIMESTAMP AS trip_starttime,
        "TRIPID"::SMALLINT AS trip_id,
        "STATUS"::VARCHAR(20) AS status,
        "THEORETICALTIME"::TIMESTAMP AS scheduled_departure_time,
        "TIMESTAMP"::TIMESTAMP AS current_datetime,
        "TRIP"::INTEGER AS trip,
        "VEHICLECODE"::SMALLINT AS vehicle_code,
        "VEHICLEID"::INTEGER AS vehicle_id,
        "VEHICLESERVICE"::VARCHAR(20) AS vehicle_service
    FROM
        {{ source('BUSES', 'BRONZE_DELAYS') }}
)
SELECT
    stop_id,
    id,
    delay_in_seconds,
    est_departure_time,
    headsign,
    route_id,
    route_code,
    trip_starttime,
    trip_id,
    status,
    scheduled_departure_time,
    current_datetime,
    trip,
    vehicle_code,
    vehicle_id,
    vehicle_service
FROM
    bronze_delays
WHERE
    1=1
    {% if is_incremental() %}
        AND current_datetime > (SELECT MAX(current_datetime) FROM {{ this }})
    {% endif %}
