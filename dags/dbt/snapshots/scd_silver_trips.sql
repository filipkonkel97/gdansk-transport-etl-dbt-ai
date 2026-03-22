-- depends_on: {{ ref('silver_trips') }}

{% snapshot scd_silver_trips %}

{{
   config(
       target_schema='snapshot',
       unique_key='id',
       strategy='check',
       check_cols=[ 'route_id',
                    'trip_id',
                    'trip_headsign',
                    'trip_short_name',
                    'direction_id',
                    'type'
                    ]
    )
}}

SELECT * FROM {{ ref('silver_trips') }}

{% endsnapshot %}
