{% test is_datetime(model, column_name) %}

    select *
    from {{ model }}
    where SYSTEM$TYPEOF({{ column_name }}) not ilike '%TIMESTAMP%'

{% endtest %}