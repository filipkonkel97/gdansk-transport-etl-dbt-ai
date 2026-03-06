{% snapshot scd_silver_vehicles %}

{{
   config(
       target_schema='snapshot',
       unique_key='vehicle_code',
       strategy='check',
       check_cols=[
                'carrierer',
                'transportation_type',
                'vehicle_characteristics',
                'bidirectional',
                'historic_vehicle',
                'length',
                'brand',
                'model',
                'production_year',
                'seats',
                'standing_places',
                'air_conditioning',
                'monitoring',
                'internal_monitor',
                'floor_height',
                'kneeling_mechanism',
                'wheelchairs_ramp',
                'usb',
                'voice_announcements',
                'aed',
                'bike_holders',
                'ticket_machine',
                'passengers_doors',
                'drive_type'
                    ]
            )
}}

select * FROM {{ ref('silver_vehicles') }}

{% endsnapshot %}
