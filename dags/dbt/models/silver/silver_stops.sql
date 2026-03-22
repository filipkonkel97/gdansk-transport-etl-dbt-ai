{{
    config(
        schema='silver',
        materialized='table',
    )
}}
WITH bronze_stops AS (
    SELECT
        "STOPID"::INTEGER AS stop_id,
        CASE
            WHEN "STOPCODE" = '' THEN NULL
            ELSE "STOPCODE"
        END::TEXT AS stop_code,

        CASE 
            WHEN "STOPNAME" = '' THEN NULL
            ELSE "STOPNAME"
        END::VARCHAR(50) AS stop_name,

        "STOPSHORTNAME"::VARCHAR(50) AS short_name,

        CASE
            WHEN "STOPDESC" = '' THEN NULL
            ELSE "STOPDESC"
        END::TEXT AS stop_desc,

        CASE
            WHEN "SUBNAME" = '' THEN NULL
            ELSE "SUBNAME"
        END::VARCHAR(50) AS sub_name,

        "DATE"::DATE AS date,
        "ZONEID"::SMALLINT AS zone_id,
        "ZONENAME"::VARCHAR(100) AS zone_name,
        CASE
            WHEN "WHEELCHAIRBOARDING" = 1.0 THEN TRUE
            WHEN "WHEELCHAIRBOARDING" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS wheelchair_boarding,
        CASE
            WHEN "VIRTUAL" = 1.0 THEN TRUE
            WHEN "VIRTUAL" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS virtual,
        CASE
            WHEN "NONPASSENGER" = 1.0 THEN TRUE
            WHEN "NONPASSENGER" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS nonpassenger,
        CASE
            WHEN "DEPOT" = 1.0 THEN TRUE
            WHEN "DEPOT" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS depot,
        CASE
            WHEN "TICKETZONEBORDER" = 1.0 THEN TRUE
            WHEN "TICKETZONEBORDER" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS ticket_zone_border,
        CASE
            WHEN "ONDEMAND" = 1.0 THEN TRUE
            WHEN "ONDEMAND" = 0.0 THEN FALSE
            ELSE NULL
        END::BOOLEAN AS on_demand,
        "ACTIVATIONDATE"::DATE AS activation_date,
        "STOPLAT"::NUMERIC(10, 7) AS stop_lat,
        "STOPLON"::NUMERIC(10, 7) AS stop_lon,
        "TYPE"::VARCHAR(50) AS type
    FROM
        {{ source('BUSES', 'BRONZE_STOPS') }}
),
     deduplicated_data AS (
        SELECT
            stop_id,
            COALESCE(stop_code, sub_name) AS stop_code,
            COALESCE(stop_name, stop_desc) AS stop_name,
            short_name,
            stop_desc,
            sub_name,
            date,
            zone_id,
            zone_name,
            virtual,
            nonpassenger,
            depot,
            ticket_zone_border,
            on_demand,
            activation_date,
            stop_lat,
            stop_lon,
            type,
            ROW_NUMBER() OVER (
                PARTITION BY stop_id,stop_code
                ORDER BY activation_date DESC
            ) AS rn
        FROM bronze_stops
     )

SELECT
    stop_id,
    stop_code,
    stop_name,
    short_name,
    stop_desc,
    sub_name,
    date,
    zone_id,
    zone_name,
    virtual,
    nonpassenger,
    depot,
    ticket_zone_border,
    on_demand,
    activation_date,
    stop_lat,
    stop_lon,
    type
FROM
    deduplicated_data
WHERE rn = 1
ORDER BY stop_id
