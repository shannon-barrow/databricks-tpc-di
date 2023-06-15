{% test dateval(model, column_name) %}

with validation as (

    select
        {{ column_name }},
        enddate

    from {{ model }}

),

validation_errors as (

    select
        effectivedate

    from validation
    -- if this is true, then even_field is actually odd!
    where effectivedate > enddate

)

select *
from validation_errors

{% endtest %}
