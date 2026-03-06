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
        "BIDIRECTIONAL"::BOOLEAN,
        "HISTORICVEHICLE"::BOOLEAN AS historic_vehicle,
        "LENGTH"::SMALLINT,
        "BRAND"::VARCHAR(50),
        "MODEL"::VARCHAR(20),
        "PRODUCTIONYEAR"::SMALLINT AS production_year,
        "SEATS"::SMALLINT,
        "STANDINGPLACES"::SMALLINT AS standing_places,
        "AIRCONDITIONING"::BOOLEAN AS air_conditioning,
        "MONITORING"::BOOLEAN,
        "INTERNALMONITOR"::BOOLEAN AS internal_monitor,
        "FLOORHEIGHT"::VARCHAR(50) AS floor_height,
        "KNEELINGMECHANISM"::BOOLEAN AS kneeling_mechanism,
        "WHEELCHAIRSRAMP"::BOOLEAN AS wheelchairs_ramp,
        "USB"::BOOLEAN,
        "VOICEANNOUNCEMENTS"::BOOLEAN AS voice_announcements,
        "AED"::BOOLEAN,
        "BIKEHOLDERS"::SMALLINT AS bike_holders,
        "TICKETMACHINE"::BOOLEAN AS ticket_machine,
        "PASSENGERSDOORS"::SMALLINT AS passengers_doors,
        "DRIVETYPE"::VARCHAR(20) AS drive_type
    FROM
        {{ source('BUSES', 'BRONZE_VEHICLES') }}
)
SELECT
    *
FROM
    bronze_vehicles
