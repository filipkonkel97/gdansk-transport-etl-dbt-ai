{{
    config(
        schema='silver',
        materialized='table',
    )
}}
WITH bronze_vehicles AS (
    SELECT
        "VEHICLECODE"::SMALLINT AS vehicle_code,
        "CARRIRER"::VARCHAR(50) AS carrier,
        "TRANSPORTATIONTYPE"::VARCHAR(10) AS transportation_type,
        "VEHICLECHARACTERISTICS"::VARCHAR(20) AS vehicle_characteristics,
        "BIDIRECTIONAL"::BOOLEAN AS bidirectional,
        "HISTORICVEHICLE"::BOOLEAN AS historic_vehicle,
        "LENGTH"::SMALLINT AS length,
        "BRAND"::VARCHAR(50) AS brand,
        "MODEL"::VARCHAR(20) AS model,
        "PRODUCTIONYEAR"::SMALLINT AS production_year,
        "SEATS"::SMALLINT AS seats,
        "STANDINGPLACES"::SMALLINT AS standing_places,
        "AIRCONDITIONING"::BOOLEAN AS air_conditioning,
        "MONITORING"::BOOLEAN AS monitoring,
        "INTERNALMONITOR"::BOOLEAN AS internal_monitor,
        "FLOORHEIGHT"::VARCHAR(50) AS floor_height,
        "KNEELINGMECHANISM"::BOOLEAN AS kneeling_mechanism,
        "WHEELCHAIRSRAMP"::BOOLEAN AS wheelchairs_ramp,
        "USB"::BOOLEAN AS usb,
        "VOICEANNOUNCEMENTS"::BOOLEAN AS voice_announcements,
        "AED"::BOOLEAN AS aed,
        "BIKEHOLDERS"::SMALLINT AS bike_holders,
        "TICKETMACHINE"::BOOLEAN AS ticket_machine,
        "PASSENGERSDOORS"::SMALLINT AS passengers_doors,
        "DRIVETYPE"::VARCHAR(20) AS drive_type
    FROM
        {{ source('BUSES', 'BRONZE_VEHICLES') }}
),
    deduplicated_data AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_code ORDER BY vehicle_code
            ) AS rn
        FROM bronze_vehicles
    )

SELECT
    vehicle_code,
    carrier,
    transportation_type,
    vehicle_characteristics,
    bidirectional,
    historic_vehicle,
    length,
    brand,
    model,
    production_year,
    seats,
    standing_places,
    air_conditioning,
    monitoring,
    internal_monitor,
    floor_height,
    kneeling_mechanism,
    wheelchairs_ramp,
    usb,
    voice_announcements,
    aed,
    bike_holders,
    ticket_machine,
    passengers_doors,
    drive_type
FROM
    deduplicated_data
WHERE rn = 1