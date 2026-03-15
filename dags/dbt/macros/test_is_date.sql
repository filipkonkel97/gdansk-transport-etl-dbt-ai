{% test is_date(model, column_name) %}

    select *
    from {{ model }}
    where SYSTEM$TYPEOF({{ column_name }}) not ilike '%DATE%'

{% endtest %}