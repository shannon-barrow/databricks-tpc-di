{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    {{var('catalog')}}.{{var('stagingschema')}}.customermgmt

