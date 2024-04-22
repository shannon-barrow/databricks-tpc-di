# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC # DimTrade

# COMMAND ----------

@dlt.view
def TradeHistory():
  return spark.sql("""
    WITH trade as (
      SELECT
        *,
        1 batchid
      FROM 
        STREAM(read_files(
          "${files_directory}sf=${scale_factor}/Batch1",
          format => "csv",
          inferSchema => False,
          header => False,
          sep => "|",
          fileNamePattern => "Trade.txt",
          schema => "${TradeHistory.schema}"
        ))
    ),
    TradeHistoryRaw AS (
      SELECT
        th_t_id tradeid,
        min(th_dts) create_ts,
        max_by(struct(th_dts, th_st_id), th_dts) current_status
      FROM 
        read_files(
          "${files_directory}sf=${scale_factor}/Batch1",
          format => "csv",
          inferSchema => False,
          header => False,
          sep => "|",
          fileNamePattern => "TradeHistory.txt",
          schema => "${TradeHistoryRaw.schema}"
      )
      group by th_t_id
    )
    SELECT
      tradeid,
      t_dts,
      create_ts,
      CASE
        WHEN current_status.th_st_id IN ("CMPT", "CNCL") THEN current_status.th_dts 
        END close_ts,
      current_status.th_st_id status,
      if(t_is_cash = 1, TRUE, FALSE) cashflag,
      t_tt_id,
      t_s_symb,
      t_qty quantity,
      t_bid_price bidprice,
      t_ca_id,
      t_exec_name executedby,
      t_trade_price tradeprice,
      t_chrg fee,
      t_comm commission,
      t_tax tax,
      1 batchid
    FROM trade t
    JOIN TradeHistoryRaw ct
      ON t.t_id = ct.tradeid
  """)

# COMMAND ----------

@dlt.table
def TradeIncremental():
  return spark.sql("""
    WITH TradeIncremental AS (
      SELECT
        t_id tradeid,
        t_dts,
        t_st_id,
        t_tt_id,
        t_is_cash,
        t_s_symb,
        t_qty AS quantity,
        t_bid_price AS bidprice,
        t_ca_id,
        t_exec_name AS executedby,
        t_trade_price AS tradeprice,
        t_chrg AS fee,
        t_comm AS commission,
        t_tax AS tax,
        cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid,
        CASE 
          WHEN cdc_flag = 'I' THEN TRUE 
          WHEN t_st_id IN ("CMPT", "CNCL") THEN FALSE 
          ELSE cast(null as boolean) END AS create_flg
      FROM STREAM(read_files(
        "${files_directory}sf=${scale_factor}/Batch[23]",
        format => "csv",
        inferSchema => False,
        header => False,
        sep => "|",
        fileNamePattern => "Trade.txt",
        schema => "${TradeIncremental.schema}"
      ))
    )
    SELECT
      tradeid,
      t_dts,
      if(create_flg, t_dts, cast(NULL AS TIMESTAMP)) create_ts,
      if(!create_flg, t_dts, cast(NULL AS TIMESTAMP)) close_ts,
      t_st_id status,
      if(t_is_cash = 1, TRUE, FALSE) cashflag,
      t_tt_id,
      t_s_symb,
      quantity,
      bidprice,
      t_ca_id,
      executedby,
      tradeprice,
      fee,
      commission,
      tax,
      batchid
    FROM TradeIncremental t
""")

# COMMAND ----------

# APPLY CHANGES with initial hydration
dlt.create_streaming_table("TradeStg")

dlt.apply_changes(
  flow_name = "TradeStg_historical",
  # only run this code once. New files added to this location will not be ingested
  once = True,   
  target = "TradeStg",
  source = "TradeHistory",
  keys = ["tradeid"],
  ignore_null_updates = True,
  stored_as_scd_type = "1",
  sequence_by = "t_dts",
  except_column_list = ["t_dts"]
)

dlt.apply_changes(
  flow_name = "TradeStg_incremental",
  target = "TradeStg",
  source = "TradeIncremental",
  keys = ["tradeid"],
  ignore_null_updates = True,
  stored_as_scd_type = "1",
  sequence_by = "t_dts",
  except_column_list = ["t_dts"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC # DimCustomer

# COMMAND ----------

@dlt.view
def CustomerHistory():
  return spark.sql("""
    SELECT
      bigint(concat(date_format(update_ts, 'yyyyMMdd'), customerid)) sk_customerid,
      customerid,
      taxid,
      status,
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
      phone1,
      phone2,
      phone3,
      email1,
      email2,
      lcl_tx_id,
      nat_tx_id,
      1 batchid,
      update_ts
    FROM
      STREAM(${cust_mgmt_schema}.CustomerMgmt) c
    WHERE
      ActionType in ('NEW', 'INACT', 'UPDCUST')
""")

# COMMAND ----------

@dlt.table
def CustomerIncremental():
  return spark.sql("""
    WITH CustomerIncremental AS (
      SELECT
        * except(cdc_flag, cdc_dsn),
        cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
      FROM STREAM(read_files(
        "${files_directory}sf=${scale_factor}/Batch[23]",
        format => "csv",
        inferSchema => False,
        header => False,
        sep => "|",
        fileNamePattern => "Customer.txt",
        schema => "${CustomerIncremental.schema}"
      ))
    )
    SELECT
      bigint(concat(date_format(bd.batchdate, 'yyyyMMdd'), customerid)) sk_customerid,
      c.customerid,
      nullif(c.taxid, '') taxid,
      nullif(s.st_name, '') as status,
      nullif(c.lastname, '') lastname,
      nullif(c.firstname, '') firstname,
      nullif(c.middleinitial, '') middleinitial,
      gender,
      c.tier,
      c.dob,
      nullif(c.addressline1, '') addressline1,
      nullif(c.addressline2, '') addressline2,
      nullif(c.postalcode, '') postalcode,
      nullif(c.city, '') city,
      nullif(c.stateprov, '') stateprov,
      nullif(c.country, '') country,
      CASE
        WHEN isnull(c_local_1) then c_local_1
        ELSE concat(
          nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
          nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
          c_local_1,
          nvl(c_ext_1, '')
        )
      END as phone1,
      CASE
        WHEN isnull(c_local_2) then c_local_2
        ELSE concat(
          nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
          nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
          c_local_2,
          nvl(c_ext_2, '')
        )
      END as phone2,
      CASE
        WHEN isnull(c_local_3) then c_local_3
        ELSE concat(
          nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
          nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
          c_local_3,
          nvl(c_ext_3, '')
        )
      END as phone3,
      nullif(c.email1, '') email1,
      nullif(c.email2, '') email2,
      c.lcl_tx_id,
      c.nat_tx_id,
      c.batchid,
      timestamp(bd.batchdate) update_ts
    FROM
      CustomerIncremental c
      JOIN LIVE.BatchDate bd ON c.batchid = bd.batchid
      JOIN LIVE.StatusType s ON c.status = s.st_id
""")

# COMMAND ----------

# APPLY CHANGES with initial hydration
dlt.create_streaming_table(
  name="DimCustomerStg",
  schema=spark.conf.get('DimCustomerStg.schema'),
  partition_cols=["iscurrent"]
)

dlt.apply_changes(
  flow_name = "DimCustomerStg_historical",
  # only run this code once. New files added to this location will not be ingested
  once = True,   
  target = "DimCustomerStg",
  source = "CustomerHistory",
  keys = ["customerid"],
  ignore_null_updates = True,
  stored_as_scd_type = "2",
  sequence_by = "update_ts",
  except_column_list = ["update_ts"]
)

dlt.apply_changes(
  flow_name = "DimCustomerStg_incremental",
  target = "DimCustomerStg",
  source = "CustomerIncremental",
  keys = ["customerid"],
  ignore_null_updates = True,
  stored_as_scd_type = "2",
  sequence_by = "update_ts",
  except_column_list = ["update_ts"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC # DimAccount

# COMMAND ----------

@dlt.view
def AccountHistory():
  return spark.sql("""
    SELECT
      c.accountid,
      c.customerid,
      c.accountdesc,
      c.taxstatus,
      c.brokerid,
      c.status,
      c.update_ts,
      1 batchid
    FROM
      STREAM(${cust_mgmt_schema}.CustomerMgmt) c
    WHERE
      ActionType NOT IN ('UPDCUST', 'INACT')
""")

# COMMAND ----------

@dlt.table
def AccountIncremental():
  return spark.sql("""
    WITH Account_Incremental AS (
      SELECT
        * except(cdc_flag, cdc_dsn),
        cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
      FROM STREAM(read_files(
        "${files_directory}sf=${scale_factor}/Batch[23]",
        format => "csv",
        inferSchema => False,
        header => False,
        sep => "|",
        fileNamePattern => "Account.txt",
        schema => "${AccountIncremental.schema}"
      ))
    )
    SELECT
      a.accountid,
      a.ca_c_id customerid,
      a.accountdesc,
      a.taxstatus,
      a.ca_b_id brokerid,
      st.st_name as status,
      TIMESTAMP(bd.batchdate) update_ts,
      a.batchid
    FROM
      Account_Incremental a
      JOIN LIVE.BatchDate bd ON a.batchid = bd.batchid
      JOIN LIVE.StatusType st ON a.CA_ST_ID = st.st_id
""")

# COMMAND ----------

# APPLY CHANGES with initial hydration
dlt.create_streaming_table(
  name="DimAccountStg",
  schema=spark.conf.get('DimAccountStg.schema')
)

dlt.apply_changes(
  flow_name = "DimAccountStg_historical",
  # only run this code once. New files added to this location will not be ingested
  once = True,   
  target = "DimAccountStg",
  source = "AccountHistory",
  keys = ["accountid"],
  ignore_null_updates = True,
  stored_as_scd_type = "2",
  sequence_by = "update_ts",
  except_column_list = ["update_ts"]
)

dlt.apply_changes(
  flow_name = "DimAccountStg_incremental",
  target = "DimAccountStg",
  source = "AccountIncremental",
  keys = ["accountid"],
  ignore_null_updates = True,
  stored_as_scd_type = "2",
  sequence_by = "update_ts",
  except_column_list = ["update_ts"]
)


# COMMAND ----------

# MAGIC %md
# MAGIC # FactWatches

# COMMAND ----------

@dlt.view
def WatchHistory():
  return spark.sql("""
    SELECT
      w_c_id customerid,
      w_s_symb symbol,
      if(w_action = 'ACTV', to_date(w_dts), null) dateplaced,
      if(w_action = 'CNCL', to_date(w_dts), null) dateremoved,
      w_dts,
      1 batchid
    FROM 
      STREAM(read_files(
        "${files_directory}sf=${scale_factor}/Batch1",
        format => "csv",
        inferSchema => False,
        header => False,
        sep => "|",
        fileNamePattern => "WatchHistory.txt",
        schema => "${WatchHistory.schema}"
      ))
""")

# COMMAND ----------

@dlt.table
def WatchIncremental():
  return spark.sql("""
    SELECT
      w_c_id customerid,
      w_s_symb symbol,
      if(w_action = 'ACTV', to_date(w_dts), null) dateplaced,
      if(w_action = 'CNCL', to_date(w_dts), null) dateremoved,
      w_dts,
      cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
    FROM
      STREAM(read_files(
        "${files_directory}sf=${scale_factor}/Batch[23]",
        format => "csv",
        inferSchema => False,
        header => False,
        sep => "|",
        fileNamePattern => "WatchHistory.txt",
        schema => "${WatchIncremental.schema}"
      ))
""")

# COMMAND ----------

# APPLY CHANGES with initial hydration
dlt.create_streaming_table(
  name="FactWatchesStg"
)

dlt.apply_changes(
  flow_name = "FactWatchesStg_historical",
  # only run this code once. New files added to this location will not be ingested
  once = True,   
  target = "FactWatchesStg",
  source = "WatchHistory",
  keys = ["customerid", "symbol"],
  ignore_null_updates = True,
  stored_as_scd_type = "1",
  sequence_by = "w_dts",
  except_column_list = ["w_dts"]
)

dlt.apply_changes(
  flow_name = "FactWatchesStg_incremental",
  target = "FactWatchesStg",
  source = "WatchIncremental",
  keys = ["customerid", "symbol"],
  ignore_null_updates = True,
  stored_as_scd_type = "1",
  sequence_by = "w_dts",
  except_column_list = ["w_dts"]
)

