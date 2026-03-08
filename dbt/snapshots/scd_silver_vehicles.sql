{% snapshot scd_silver_vehicles %}

{{
   config(
       target_schema='snapshot',
       unique_key='VEHICLE_CODE',
       strategy='check',
       check_cols=[
                'CARRIER',
                'TRANSPORTATION_TYPE',
                'VEHICLE_CHARACTERISTICS',
                'BIDIRECTIONAL',
                'HISTORIC_VEHICLE',
                'LENGTH',
                'BRAND',
                'MODEL',
                'PRODUCTION_YEAR',
                'SEATS',
                'STANDING_PLACES',
                'AIR_CONDITIONING',
                'MONITORING',
                'INTERNAL_MONITOR',
                'FLOOR_HEIGHT',
                'KNEELING_MECHANISM',
                'WHEELCHAIRS_RAMP',
                'USB',
                'VOICE_ANNOUNCEMENTS',
                'AED',
                'BIKE_HOLDERS',
                'TICKET_MACHINE',
                'PASSENGERS_DOORS',
                'DRIVE_TYPE'
                    ]
            )
}}

SELECT * FROM {{ ref('silver_vehicles') }}

{% endsnapshot %}
