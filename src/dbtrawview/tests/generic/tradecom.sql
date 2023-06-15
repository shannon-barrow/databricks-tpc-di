{% test tradecom(model, column_name) %}

with validation as (

    select
        *

    from {{ model }}

),

validation_errors as (

    select
        commission

    from validation
    -- if this is true, then even_field is actually odd!
    where commission > tradeprice * quantity

)

select *
from validation_errors

{% endtest %}
