-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
CREATE OR REPLACE TABLE CompanyYearEPS (
  sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID',
  qtr_start_date DATE COMMENT 'Start date of quarter.',
  prev_year_basic_eps DOUBLE COMMENT 'Basic earnings per share for the year.',
  CONSTRAINT company_eps_pk PRIMARY KEY(sk_companyid, qtr_start_date),
  CONSTRAINT company_eps_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid)
) PARTITIONED BY (qtr_start_date);

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.companyyeareps')
SELECT
  c.sk_companyid,
  f.fi_qtr_start_date qtr_start_date,
  sum(f.fi_basic_eps) OVER (PARTITION BY c.companyid ORDER BY f.fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) prev_year_basic_eps
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.financial') f
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.dimcompany') c
  ON f.sk_companyid = c.sk_companyid