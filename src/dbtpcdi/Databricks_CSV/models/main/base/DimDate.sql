{{
    config(
        materialized = 'table'
    )
}}
SELECT *
FROM read_files(
  '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1',
  format          => "csv",
  header          => "false",
  inferSchema     => false,
  sep             => "|",
  schemaEvolutionMode => 'none',
  fileNamePattern => "Date\\.txt",
  schema          => """
    sk_dateid          BIGINT,
    datevalue          DATE,
    datedesc           STRING,
    calendaryearid     INT,
    calendaryeardesc   STRING,
    calendarqtrid      INT,
    calendarqtrdesc    STRING,
    calendarmonthid    INT,
    calendarmonthdesc  STRING,
    calendarweekid     INT,
    calendarweekdesc   STRING,
    dayofweeknum       INT,
    dayofweekdesc      STRING,
    fiscalyearid       INT,
    fiscalyeardesc     STRING,
    fiscalqtrid        INT,
    fiscalqtrdesc      STRING,
    holidayflag        BOOLEAN
  """
) AS raw;