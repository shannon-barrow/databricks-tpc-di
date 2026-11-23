{{
    config(
        materialized = 'table'
    )
}}
-- One row per <TPCDI:Action> in Batch1's CustomerMgmt XML: DIGen's single
-- CustomerMgmt.xml or the Spark generator's CustomerMgmt_N.xml files.
-- Mirrors the Databricks SQL/CustomerMgmtRaw.sql projection.
WITH actions AS (
  SELECT
    $1 AS action,
    XMLGET($1, 'Customer') AS c
  FROM
    @{{ var('stage') }}/Batch1
    (
      FILE_FORMAT => 'TXT_XML',
      PATTERN     => '.*CustomerMgmt(_[0-9]+)?[.]xml'
    )
),
parts AS (
  SELECT
    action,
    c,
    XMLGET(c, 'Name') AS nm,
    XMLGET(c, 'Address') AS addr,
    XMLGET(c, 'ContactInfo') AS ci,
    XMLGET(c, 'TaxInfo') AS tax,
    XMLGET(c, 'Account') AS acct
  FROM actions
),
phones AS (
  SELECT
    p.*,
    XMLGET(ci, 'C_PHONE_1') AS ph1,
    XMLGET(ci, 'C_PHONE_2') AS ph2,
    XMLGET(ci, 'C_PHONE_3') AS ph3
  FROM parts p
)
SELECT
  TRY_TO_NUMBER(c:"@C_ID"::STRING) AS customerid,
  TRY_TO_NUMBER(acct:"@CA_ID"::STRING) AS accountid,
  TRY_TO_NUMBER(XMLGET(acct, 'CA_B_ID'):"$"::STRING) AS brokerid,
  NULLIF(c:"@C_TAX_ID"::STRING, '') AS taxid,
  NULLIF(XMLGET(acct, 'CA_NAME'):"$"::STRING, '') AS accountdesc,
  TRY_TO_NUMBER(acct:"@CA_TAX_ST"::STRING) AS taxstatus,
  DECODE(action:"@ActionType"::STRING,
    'NEW', 'Active',
    'ADDACCT', 'Active',
    'UPDACCT', 'Active',
    'UPDCUST', 'Active',
    'CLOSEACCT', 'Inactive',
    'INACT', 'Inactive') AS status,
  NULLIF(XMLGET(nm, 'C_L_NAME'):"$"::STRING, '') AS lastname,
  NULLIF(XMLGET(nm, 'C_F_NAME'):"$"::STRING, '') AS firstname,
  NULLIF(XMLGET(nm, 'C_M_NAME'):"$"::STRING, '') AS middleinitial,
  NULLIF(UPPER(c:"@C_GNDR"::STRING), '') AS gender,
  TRY_TO_NUMBER(c:"@C_TIER"::STRING) AS tier,
  TRY_TO_DATE(c:"@C_DOB"::STRING) AS dob,
  NULLIF(XMLGET(addr, 'C_ADLINE1'):"$"::STRING, '') AS addressline1,
  NULLIF(XMLGET(addr, 'C_ADLINE2'):"$"::STRING, '') AS addressline2,
  NULLIF(XMLGET(addr, 'C_ZIPCODE'):"$"::STRING, '') AS postalcode,
  NULLIF(XMLGET(addr, 'C_CITY'):"$"::STRING, '') AS city,
  NULLIF(XMLGET(addr, 'C_STATE_PROV'):"$"::STRING, '') AS stateprov,
  NULLIF(XMLGET(addr, 'C_CTRY'):"$"::STRING, '') AS country,
  IFF(
    NULLIF(XMLGET(ph1, 'C_LOCAL'):"$"::STRING, '') IS NOT NULL,
    CONCAT(
      IFF(NULLIF(XMLGET(ph1, 'C_CTRY_CODE'):"$"::STRING, '') IS NOT NULL, '+' || XMLGET(ph1, 'C_CTRY_CODE'):"$"::STRING || ' ', ''),
      IFF(NULLIF(XMLGET(ph1, 'C_AREA_CODE'):"$"::STRING, '') IS NOT NULL, '(' || XMLGET(ph1, 'C_AREA_CODE'):"$"::STRING || ') ', ''),
      XMLGET(ph1, 'C_LOCAL'):"$"::STRING,
      COALESCE(XMLGET(ph1, 'C_EXT'):"$"::STRING, '')),
    NULL) AS phone1,
  IFF(
    NULLIF(XMLGET(ph2, 'C_LOCAL'):"$"::STRING, '') IS NOT NULL,
    CONCAT(
      IFF(NULLIF(XMLGET(ph2, 'C_CTRY_CODE'):"$"::STRING, '') IS NOT NULL, '+' || XMLGET(ph2, 'C_CTRY_CODE'):"$"::STRING || ' ', ''),
      IFF(NULLIF(XMLGET(ph2, 'C_AREA_CODE'):"$"::STRING, '') IS NOT NULL, '(' || XMLGET(ph2, 'C_AREA_CODE'):"$"::STRING || ') ', ''),
      XMLGET(ph2, 'C_LOCAL'):"$"::STRING,
      COALESCE(XMLGET(ph2, 'C_EXT'):"$"::STRING, '')),
    NULL) AS phone2,
  IFF(
    NULLIF(XMLGET(ph3, 'C_LOCAL'):"$"::STRING, '') IS NOT NULL,
    CONCAT(
      IFF(NULLIF(XMLGET(ph3, 'C_CTRY_CODE'):"$"::STRING, '') IS NOT NULL, '+' || XMLGET(ph3, 'C_CTRY_CODE'):"$"::STRING || ' ', ''),
      IFF(NULLIF(XMLGET(ph3, 'C_AREA_CODE'):"$"::STRING, '') IS NOT NULL, '(' || XMLGET(ph3, 'C_AREA_CODE'):"$"::STRING || ') ', ''),
      XMLGET(ph3, 'C_LOCAL'):"$"::STRING,
      COALESCE(XMLGET(ph3, 'C_EXT'):"$"::STRING, '')),
    NULL) AS phone3,
  NULLIF(XMLGET(ci, 'C_PRIM_EMAIL'):"$"::STRING, '') AS email1,
  NULLIF(XMLGET(ci, 'C_ALT_EMAIL'):"$"::STRING, '') AS email2,
  NULLIF(XMLGET(tax, 'C_LCL_TX_ID'):"$"::STRING, '') AS lcl_tx_id,
  NULLIF(XMLGET(tax, 'C_NAT_TX_ID'):"$"::STRING, '') AS nat_tx_id,
  TO_TIMESTAMP(action:"@ActionTS"::STRING) AS update_ts,
  action:"@ActionType"::STRING AS actiontype
FROM phones
