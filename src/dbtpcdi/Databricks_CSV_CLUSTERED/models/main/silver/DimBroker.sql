{{
    config(
        materialized = 'table',
        liquid_clustered_by = "sk_brokerid"
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
FROM read_files(
  '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1',
  format          => "csv",
  header          => "false",
  inferSchema     => false,
  sep             => ",",
  schemaEvolutionMode => 'none',
  fileNamePattern => "HR\\.csv",
  schema          => """
    employeeid BIGINT,
    managerid BIGINT,
    employeefirstname STRING,
    employeelastname STRING,
    employeemi STRING,
    employeejobcode STRING,
    employeebranch STRING,
    employeeoffice STRING,
    employeephone STRING
  """
)
WHERE employeejobcode = 314;