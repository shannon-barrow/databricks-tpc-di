{% test tradefee(model, column_name) %}

with validation as (

    select
        *

    from {{ model }}

),

validation_errors as (

    select
        fee

    from validation
    -- if this is true, then even_field is actually odd!
    where fee > tradeprice * quantity

)

select *
from validation_errors

{% endtest %}
