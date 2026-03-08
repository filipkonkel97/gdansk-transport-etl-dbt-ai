{% snapshot scd_silver_stops %}

{{
   config(
       target_schema='snapshot',
       unique_key='stop_id',
       strategy='check',
       check_cols=['stop_code',
                   'stop_name',
                   'short_name',
                   'stop_desc',
                   'sub_name',
                   'zone_id',
                   'zone_name',
                   'virtual',
                   'nonpassenger',
                   'depot',
                   'ticket_zone_border',
                   'on_demand',
                   'activation_date',
                   'stop_lat',
                   'stop_lon',
                   'type'
                  ]
   )
}}

SELECT * FROM {{ ref('silver_stops') }}

{% endsnapshot %}
