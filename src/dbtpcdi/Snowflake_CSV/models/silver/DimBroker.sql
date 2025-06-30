{{
    config(
        materialized = 'table'
    )
}}
SELECT
  $1::BIGINT sk_brokerid,
  $1::BIGINT brokerid,
  $2::BIGINT managerid,
  $3::string firstname,
  $4::string lastname,
  $5::string as middleinitial,
  $7::string as branch,
  $8::string as office,
  $9::string as phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {{ ref('DimDate') }}) effectivedate,
  date('9999-12-31') enddate
FROM
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_CSV',
    PATTERN     => '.*HR[.]csv'
  ) t
WHERE $6::INT = 314