{{
    config(
        materialized = 'table'
    )
}}
SELECT
  $1:employeeid::BIGINT         AS sk_brokerid,
  $1:employeeid::BIGINT         AS brokerid,
  $1:managerid::BIGINT          AS managerid,
  $1:employeefirstname::STRING  AS firstname,
  $1:employeelastname::STRING   AS lastname,
  $1:employeemi::STRING         AS middleinitial,
  $1:employeejobcode::STRING    AS employeejobcode,
  $1:employeebranch::STRING     AS branch,
  $1:employeeoffice::STRING     AS office,
  $1:employeephone::STRING      AS phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {{ ref('DimDate') }}) effectivedate,
  date('9999-12-31') enddate
FROM
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*HR_.*[.]parquet'
  ) t
WHERE $1:employeejobcode::INT = 314