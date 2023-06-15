{{ config(
  materialized='table'
) }}

SELECT

  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {{ source('tpcdi', 'DimDate') }}) effectivedate,
  date('9999-12-31') enddate,
  bigint(concat(date_format(enddate, 'yyyyMMdd'), cast(brokerid as string))) as sk_brokerid
FROM  {{ source('tpcdi', 'HR') }}
WHERE employeejobcode = 314
