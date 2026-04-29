# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Files — Customer
# MAGIC
# MAGIC Reads `tpcdi_raw_data.customermgmt{sf}` filtered to:
# MAGIC `stg_target='files'` AND `ActionType IN ('NEW','INACT','UPDCUST')`
# MAGIC (the customer-touching action types — Account-only events are emitted
# MAGIC by the Account stage_files notebook).
# MAGIC
# MAGIC Mirrors the DIGen `incremental_file_splitting/Customer.sql` output
# MAGIC schema, including BOTH the concatenated `phone1/2/3` AND the split
# MAGIC `c_ctry_N / c_area_N / c_local_N / c_ext_N` components — Customer.txt's
# MAGIC incremental file format keeps both. Status is encoded as `'ACTV'/'INAC'`
# MAGIC (the DIGen splitter convention) so the augmented incremental DimCustomer
# MAGIC pipeline can re-decode it.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory",
                     "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "main")

scale_factor    = dbutils.widgets.get("scale_factor").strip()
tpcdi_directory = dbutils.widgets.get("tpcdi_directory").strip()
catalog         = dbutils.widgets.get("catalog").strip()

target_dir = f"{tpcdi_directory.rstrip('/')}/sf={scale_factor}"  # tpcdi_directory base_param already ends with augmented_incremental/_staging/

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tools_dir = f"/Workspace{_nb_path.split('/src')[0]}/src/tools"
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)
from augmented_staging._stage_ingestion import stage_to_files

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _stage_customer AS
SELECT
  CASE WHEN ActionType = 'NEW' THEN 'I' ELSE 'U' END AS cdc_flag,
  row_number() OVER (ORDER BY update_ts, customerid) - 1 AS cdc_dsn,
  customerid,
  taxid,
  CASE WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'INAC' ELSE 'ACTV' END AS status,
  lastname,
  firstname,
  middleinitial,
  gender,
  tier,
  dob,
  addressline1,
  addressline2,
  postalcode,
  city,
  stateprov,
  country,
  -- Concatenated phone variants (consistent with DIGen Customer.sql output)
  nvl2(c_local_1,
    concat(
      nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
      nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
      c_local_1,
      nvl(c_ext_1, '')),
    try_cast(null as string)) AS phone1,
  nvl2(c_local_2,
    concat(
      nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
      nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
      c_local_2,
      nvl(c_ext_2, '')),
    try_cast(null as string)) AS phone2,
  nvl2(c_local_3,
    concat(
      nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
      nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
      c_local_3,
      nvl(c_ext_3, '')),
    try_cast(null as string)) AS phone3,
  -- Split phone components
  c_ctry_1, c_area_1, c_local_1, c_ext_1,
  c_ctry_2, c_area_2, c_local_2, c_ext_2,
  c_ctry_3, c_area_3, c_local_3, c_ext_3,
  email1,
  email2,
  lcl_tx_id,
  nat_tx_id,
  update_ts,
  to_date(update_ts) AS update_dt,
  -- _pdate is a duplicate of update_dt used ONLY as the partition column. Spark strips partition cols from the written data file, so without this duplicate the per-day file would lose update_dt entirely. The DIGen splitter's rawcustomer table has update_dt in the actual file content (because reading a Delta partition col includes it in SELECT *), and we need to match that shape.
  to_date(update_ts) AS _pdate
FROM {catalog}.tpcdi_raw_data.customermgmt{scale_factor}
WHERE stg_target = 'files'
  AND ActionType IN ('NEW', 'INACT', 'UPDCUST')
""")

# COMMAND ----------

stage_to_files(
    spark, dbutils,
    source_view="_stage_customer",
    date_col="_pdate",
    filename="Customer.txt",
    target_dir=target_dir,
)
