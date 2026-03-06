{% snapshot scd_silver_routes %}

{{
   config(
       target_schema='snapshot',
       unique_key='route_id',
       strategy='check',
       check_cols=['agency_id',
                   'route_short_name',
                   'route_long_name',
                   'route_type',
                   'activation_date',
                   'route_type'
                  ]
   )
}}

SELECT * FROM {{ ref('silver_routes') }}

{% endsnapshot %}
