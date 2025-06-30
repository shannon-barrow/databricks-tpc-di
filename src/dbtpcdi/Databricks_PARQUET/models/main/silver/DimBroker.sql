{{
    config(
        materialized = 'table'
    )
}}
SELECT
  employeeid sk_brokerid,
  employeeid brokerid,
  managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {{ ref('DimDate') }}) effectivedate,
  date('9999-12-31') enddate
FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1/HR*.parquet`
WHERE employeejobcode = 314;