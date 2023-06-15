{{
    config(
        materialized = 'view'
    )
}}
select
    *
from
    hive_metastore.roberto_salcido_tpcdi_stage.customermgmt{{var("benchmark")}}

