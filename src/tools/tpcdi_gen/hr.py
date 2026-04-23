"""Generate HR table.

Produces the HR.csv file containing employee records for the brokerage firm.
Also persists a _brokers temp view for downstream modules (customer, trade)
that need to assign brokers to accounts.

Broker Allocation
------------------
Approximately 30% of employees (BROKER_PCT) are brokers (jobcode 314). The
remaining 70% are assigned one of 9 other job codes from OTHER_JOBCODES.
5% of all employees have a NULL jobcode (DIGen's PERCENT_NULL pattern).

The _brokers temp view contains all employees with jobcode=314, indexed by a
sequential _idx column (0-based). Downstream modules use this index to assign
brokers to customer accounts via hash-based lookup:
    broker_id = _brokers[hash(account_id) % broker_count]

Name Dictionaries
------------------
HR uses LARGER name dictionaries than CustomerMgmt:
  - Family-Names.dict (13,483 entries) for employeefirstname
  - Given-Names.dict (8,608 entries) for employeelastname
Note: DIGen's schema counter-intuitively maps employeefirstname from
Family-Names.dict and employeelastname from Given-Names.dict. We preserve
this mapping for compatibility with the reference DIGen output.

These are loaded as "hr_family_names" and "hr_given_names" in the dictionaries
module, separate from the smaller "first_names"/"last_names" dicts used by
CustomerMgmt and Prospect.
"""

from pyspark.sql import SparkSession, functions as F, Window
from .config import *
from .utils import write_file, seed_for, dict_join, hash_key, log
from .audit import static_audits_available


def generate(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Generate HR.csv and persist the _brokers temp view for downstream joins.

    The _brokers view is consumed by the customer module to populate ca_b_id
    (broker ID) on Account records, ensuring every account references a valid
    broker from the HR table.

    Args:
        spark: Active SparkSession.
        cfg: ScaleConfig with hr_rows and batch_path().
        dicts: Dictionary data (unused directly; dict_join uses registered views).
        dbutils: Databricks dbutils for file I/O.

    Returns:
        dict with key "counts" mapping (table_name, batch_id) to row counts,
        including the broker count under ("HR_BROKERS", 1).
    """
    log("[HR] Starting generation")
    hr_df = spark.range(0, cfg.hr_rows).withColumnRenamed("id", "employeeid")

    # ManagerID: range 1 to 0.1*tableSize (DIGen starts manager IDs at 1, not 0)
    # This means the top ~10% of employees are potential managers.
    hr_df = hr_df.withColumn("managerid",
        (hash_key(F.col("employeeid"), seed_for("HR", "mgr")) % int(0.1 * cfg.hr_rows) + 1).cast("string"))

    # --- Name assignment via dictionary broadcast joins ---
    # HR uses larger name dictionaries than CustomerMgmt:
    # "hr_family_names" (Family-Names.dict, 13483 entries) -> employeefirstname
    # "hr_given_names" (Given-Names.dict, 8608 entries) -> employeelastname
    # (Note: DIGen's schema maps employeefirstname from Family-Names and
    #  employeelastname from Given-Names — the names are swapped vs intuition)
    hr_df = dict_join(hr_df, "hr_family_names", hash_key(F.col("employeeid"), seed_for("HR", "fn")), "employeefirstname")
    hr_df = dict_join(hr_df, "hr_given_names", hash_key(F.col("employeeid"), seed_for("HR", "ln")), "employeelastname")

    # Middle initial: DIGen shows ~63% empty, 37% single uppercase A-Z
    hr_df = hr_df.withColumn("employeemi",
        F.when(hash_key(F.col("employeeid"), seed_for("HR", "mi_null")) % 100 < 63, F.lit(None))
         .otherwise(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            (hash_key(F.col("employeeid"), seed_for("HR", "mi_v")) % 26 + 1).cast("int"), 1)))

    # --- Broker allocation ---
    # 30% of employees become brokers (jobcode 314).
    # Remaining employees get a random jobcode from OTHER_JOBCODES (9 values).
    # DIGen never has null jobcodes — every employee has one.
    jc_arr = F.array([F.lit(str(j)) for j in OTHER_JOBCODES])
    hr_df = (hr_df
        .withColumn("_is_broker", hash_key(F.col("employeeid"), seed_for("HR", "broker")) % 1000 < int(BROKER_PCT * 1000))
        .withColumn("employeejobcode",
            F.when(F.col("_is_broker"), F.lit(str(BROKER_JOBCODE)))
             .otherwise(jc_arr[(hash_key(F.col("employeeid"), seed_for("HR", "jc")) % len(OTHER_JOBCODES)).cast("int")])))

    # Branch: 20-30 char random string using A-Za-z (DIGen pattern).
    # Built by concatenating two MD5 hashes (64 hex chars total), translating
    # hex digits to mixed-case letters, then taking a variable-length substring.
    hr_df = hr_df.withColumn("_br_len", (hash_key(F.col("employeeid"), seed_for("HR", "br_len")) % 11 + 20).cast("int"))
    hr_df = hr_df.withColumn("_br_raw", F.concat(
        F.md5(F.concat(F.col("employeeid").cast("string"), F.lit("br1"))),
        F.md5(F.concat(F.col("employeeid").cast("string"), F.lit("br2")))))
    # Convert hex to mixed-case letters by mapping each char
    hr_df = hr_df.withColumn("_br_mapped", F.translate(F.col("_br_raw"),
        "0123456789abcdef",
        "ABCDEFGHIJabcdef"))
    hr_df = hr_df.withColumn("employeebranch",
        F.when(hash_key(F.col("employeeid"), seed_for("HR", "br_null")) % 100 < 5, F.lit(None))
         .otherwise(F.substring(F.col("_br_mapped"), 1, F.col("_br_len"))))

    # Office: "OFFICEXXXX" format (4-digit random number 1000-9999), 5% NULL
    hr_df = hr_df.withColumn("employeeoffice",
        F.when(hash_key(F.col("employeeid"), seed_for("HR", "of_null")) % 100 < 5, F.lit(None))
         .otherwise(F.concat(F.lit("OFFICE"),
            (hash_key(F.col("employeeid"), seed_for("HR", "office")) % 9000 + 1000).cast("string"))))

    # Phone: "(NNN) NNN-NNNN" format, 5% NULL
    hr_df = (hr_df
        .withColumn("_ph", hash_key(F.col("employeeid"), seed_for("HR", "phone")))
        .withColumn("employeephone",
            F.when(hash_key(F.col("employeeid"), seed_for("HR", "ph_null")) % 100 < 5, F.lit(None))
             .otherwise(F.concat(
                F.lit("("),
                F.lpad((F.col("_ph") % 999 + 1).cast("string"), 3, "0"),
                F.lit(") "),
                F.lpad(((F.col("_ph") / 1000).cast("int") % 999 + 1).cast("string"), 3, "0"),
                F.lit("-"),
                F.lpad(((F.col("_ph") / 1000000).cast("int") % 9000 + 1000).cast("string"), 4, "0")))))

    final = hr_df.select(
        F.col("employeeid").cast("string"),
        "managerid", "employeefirstname", "employeelastname", "employeemi",
        "employeejobcode", "employeebranch", "employeeoffice", "employeephone")

    write_file(final, f"{cfg.batch_path(1)}/HR.csv", ",", dbutils,
               scale_factor=cfg.sf)

    # --- Persist _brokers temp view ---
    # Extracts all employees with jobcode=314 (brokers) and assigns a sequential
    # _idx (0-based) for downstream modules to reference via hash-based lookup.
    # The customer module uses: broker_id = _brokers[hash(account) % broker_count]
    # to assign each account to a valid broker.
    brokers = (final
        .filter(F.col("employeejobcode") == str(BROKER_JOBCODE))
        .select(F.col("employeeid").alias("broker_id"))
        .withColumn("_idx", F.row_number().over(Window.orderBy(F.monotonically_increasing_id())) - 1))
    brokers.createOrReplaceTempView("_brokers")
    # Analytical estimate: 30% of employees become brokers (hash-based selection
    # with ±0.01% variance). Exact count only computed when we need it for
    # dynamic audit regeneration.
    broker_count_est = int(cfg.hr_rows * BROKER_PCT)
    broker_count = brokers.count() if not static_audits_available(cfg) else broker_count_est
    log(f"[HR] {cfg.hr_rows:,} rows (~{broker_count_est:,} brokers -> _brokers view)")
    log("[HR] Generation complete")
    return {"counts": {("HR", 1): cfg.hr_rows, ("HR_BROKERS", 1): broker_count}}
