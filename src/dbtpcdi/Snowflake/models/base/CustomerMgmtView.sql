{{
    config(
        materialized = 'view'
    )
}}

    -- SELECT
    --     get(xmlget($1, 'Customer'), '@C_ID')::BIGINT as customerid,
    --     get(xmlget(xmlget($1, 'Customer'), 'Account'), '@CA_ID')::BIGINT as accountid,
    --     get(xmlget(xmlget(xmlget($1, 'Customer'), 'Account'), 'CA_B_ID'), '$')::BIGINT as brokerid,
    --     nullif(get(xmlget($1, 'Customer'), '@C_TAX_ID'), '')::STRING as taxid,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Account'), 'CA_NAME'), '$'), '')::STRING as accountdesc,
    --     get(xmlget(xmlget($1, 'Customer'), 'Account'), '@CA_TAX_ST')::TINYINT as taxstatus,
    --     decode(get($1, '@ActionType'), 'NEW','Active', 'ADDACCT','Active', 'UPDACCT','Active', 'UPDCUST','Active', 'CLOSEACCT','Inactive', 'INACT','Inactive') as status,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Name'), 'C_L_NAME'), '$'), '')::STRING as lastname,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Name'), 'C_F_NAME'), '$'), '')::STRING as firstname,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Name'), 'C_M_NAME'), '$'), '')::STRING as middleinitial,
    --     nullif(upper(get(xmlget($1, 'Customer'), '@C_GNDR')), '') as gender,
    --     nullif(get(xmlget($1, 'Customer'), '@C_TIER'), '')::STRING as tier,
    --     get(xmlget($1, 'Customer'), '@C_DOB')::DATE as dob,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_ADLINE1'), '$'), '')::STRING as addressline1,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_ADLINE2'), '$'), '')::STRING as addressline2,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_ZIPCODE'), '$'), '')::STRING as postalcode,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_CITY'), '$'), '')::STRING as city,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_STATE_PROV'), '$'), '')::STRING as stateprov,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'Address'), 'C_CTRY'), '$'), '')::STRING as country,
    --     nvl2(
    --         nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_1'), 'C_LOCAL'), '$'), ''),
    --         concat(
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_1'), 'C_CTRY_CODE'), '$'), ''), '+' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_1'), 'CTRY_CODE'), '$') || ' ', ''),
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_1'), 'C_AREA_CODE'), '$'), ''), '(' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_1'), 'C_AREA_CODE'), '$') || ') ', ''),
    --             get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_1'), 'C_LOCAL'), '$'),
    --             nvl(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_1'), 'C_EXT'), '$'), '')),
    --         cast(null as string)) phone1,
    --     nvl2(
    --         nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_2'), 'C_LOCAL'), '$'), ''),
    --         concat(
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_2'), 'C_CTRY_CODE'), '$'), ''), '+' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_2'), 'CTRY_CODE'), '$') || ' ', ''),
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_2'), 'C_AREA_CODE'), '$'), ''), '(' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_2'), 'C_AREA_CODE'), '$') || ') ', ''),
    --             get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_2'), 'C_LOCAL'), '$'),
    --             nvl(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_2'), 'C_EXT'), '$'), '')),
    --         cast(null as string)) phone2,
    --     nvl2(
    --         nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_3'), 'C_LOCAL'), '$'), ''),
    --         concat(
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_3'), 'C_CTRY_CODE'), '$'), ''), '+' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_3'), 'CTRY_CODE'), '$') || ' ', ''),
    --             nvl2(nullif(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_3'), 'C_AREA_CODE'), '$'), ''), '(' || get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'),             'ContactInfo'), 'C_PHONE_3'), 'C_AREA_CODE'), '$') || ') ', ''),
    --             get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_3'), 'C_LOCAL'), '$'),
    --             nvl(get(xmlget(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PHONE_3'), 'C_EXT'), '$'), '')),
    --         cast(null as string)) phone3,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_PRIM_EMAIL'), '$'), '')::STRING as email1,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'ContactInfo'), 'C_ALT_EMAIL'), '$'), '')::STRING as email2,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'TaxInfo'), 'C_LCL_TX_ID'), '$'), '')::STRING as lcl_tx_id,
    --     nullif(get(xmlget(xmlget(xmlget($1, 'Customer'), 'TaxInfo'), 'C_NAT_TX_ID'), '$'), '')::STRING as nat_tx_id,
    --     to_timestamp(get($1, '@ActionTS')) as update_ts,
    --     get($1, '@ActionType')::STRING as ActionType
    --   FROM @{{var('stage')}}/Batch1 (FILE_FORMAT => 'XML', PATTERN => '.*CustomerMgmt[.]xml.*')

select * from {{var('catalog')}}.{{var('stagingschema')}}.customermgmt_clean