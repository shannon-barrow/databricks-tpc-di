# Databricks notebook source
# Shared per-bronze COPY INTO templates used by the per-branch refresh
# notebooks (branch_factwatches, branch_factcashbalances, branch_factholdings,
# branch_factmarkethistory). Each branch sources this via `%run ../_seed_helpers`
# and calls `copy_bronze(cur, catalog, target_schema, stage_root, table)`.
#
# All bronze tables are CDC-flagged regular Snowflake tables created in
# `setup_sf_dt.py`. COPY INTO uses Snowflake's default LOAD HISTORY tracking
# for idempotency.

BRONZE_COL_PROJECTIONS = {
    "bronzeaccount": """
        $1::string  AS cdc_flag,
        $2::bigint  AS cdc_dsn,
        $3::bigint  AS accountid,
        $4::bigint  AS brokerid,
        $5::bigint  AS customerid,
        $6::string  AS accountdesc,
        $7::tinyint AS taxstatus,
        $8::string  AS status,
        $9::date    AS update_dt
    """,
    "bronzecashtransaction": """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS accountid,
        $4::timestamp AS ct_dts,
        $5::float     AS ct_amt,
        $6::string    AS ct_name,
        $7::date      AS event_dt
    """,
    "bronzecustomer": """
        $1::string  AS cdc_flag,
        $2::bigint  AS cdc_dsn,
        $3::bigint  AS customerid,
        $4::string  AS taxid,
        $5::string  AS status,
        $6::string  AS lastname,
        $7::string  AS firstname,
        $8::string  AS middleinitial,
        $9::string  AS gender,
        $10::tinyint AS tier,
        $11::date   AS dob,
        $12::string AS addressline1,
        $13::string AS addressline2,
        $14::string AS postalcode,
        $15::string AS city,
        $16::string AS stateprov,
        $17::string AS country,
        $18::string AS c_ctry_1,
        $19::string AS c_area_1,
        $20::string AS c_local_1,
        $21::string AS c_ext_1,
        $22::string AS c_ctry_2,
        $23::string AS c_area_2,
        $24::string AS c_local_2,
        $25::string AS c_ext_2,
        $26::string AS c_ctry_3,
        $27::string AS c_area_3,
        $28::string AS c_local_3,
        $29::string AS c_ext_3,
        $30::string AS email1,
        $31::string AS email2,
        $32::string AS lcl_tx_id,
        $33::string AS nat_tx_id,
        $34::date   AS update_dt
    """,
    "bronzedailymarket": """
        $1::string AS cdc_flag,
        $2::bigint AS cdc_dsn,
        $3::date   AS dm_date,
        $4::string AS dm_s_symb,
        $5::float  AS dm_close,
        $6::float  AS dm_high,
        $7::float  AS dm_low,
        $8::int    AS dm_vol
    """,
    "bronzeholdings": """
        $1::string AS cdc_flag,
        $2::bigint AS cdc_dsn,
        $3::bigint AS hh_h_t_id,
        $4::bigint AS hh_t_id,
        $5::int    AS hh_before_qty,
        $6::int    AS hh_after_qty,
        $7::date   AS event_dt
    """,
    "bronzetrade": """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS tradeid,
        $4::timestamp AS t_dts,
        $5::string    AS status,
        $6::string    AS t_tt_id,
        $7::tinyint   AS cashflag,
        $8::string    AS t_s_symb,
        $9::int       AS quantity,
        $10::float    AS bidprice,
        $11::bigint   AS t_ca_id,
        $12::string   AS executedby,
        $13::float    AS tradeprice,
        $14::float    AS fee,
        $15::float    AS commission,
        $16::float    AS tax,
        $17::date     AS event_dt
    """,
    "bronzewatches": """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS w_c_id,
        $4::string    AS w_s_symb,
        $5::timestamp AS w_dts,
        $6::string    AS w_action,
        $7::date      AS event_dt
    """,
}

# Map bronze table → source filename in the Snowflake stage.
BRONZE_SOURCE_FILE = {
    "bronzeaccount":         "Account.txt",
    "bronzecashtransaction": "CashTransaction.txt",
    "bronzecustomer":        "Customer.txt",
    "bronzedailymarket":     "DailyMarket.txt",
    "bronzeholdings":        "HoldingHistory.txt",
    "bronzetrade":           "Trade.txt",
    "bronzewatches":         "WatchHistory.txt",
}

def copy_bronze_sql(*, catalog: str, target_schema: str, stage_root: str, table: str) -> str:
    """Build the COPY INTO statement for one bronze table."""
    cols = BRONZE_COL_PROJECTIONS[table].strip().rstrip(",")
    basename = BRONZE_SOURCE_FILE[table]
    return (
        f"COPY INTO {catalog}.{target_schema}.{table}\n"
        f"FROM (\n"
        f"  SELECT {cols}\n"
        f"  FROM {stage_root}/{basename}\n"
        f")\n"
        f"FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0)\n"
        f"ON_ERROR = 'ABORT_STATEMENT'"
    )

def copy_bronze(cur, *, catalog: str, target_schema: str, stage_root: str, table: str) -> int:
    """Run COPY INTO for one bronze table on `cur`. Returns rows loaded (0 if
    the file was already loaded / had no new rows)."""
    cur.execute(copy_bronze_sql(catalog=catalog, target_schema=target_schema, stage_root=stage_root, table=table))
    res = cur.fetchall()
    return sum(int(r[2]) for r in res if r and len(r) > 2
               and isinstance(r[2], (int, str)) and str(r[2]).isdigit())


def run_branch(
    *,
    sf_connect_fn,
    sf_connect_kwargs: dict,
    catalog: str,
    target_schema: str,
    stage_root: str,
    query_tag_base: dict,
    independent_bronze: str | None,
    chain_bronzes: list,
    chain_dim_refresh: str | None,
    leaf_dt: str,
) -> dict:
    """Per-branch orchestrator.

      Thread A: COPY INTO `independent_bronze` (one table, one connection).
      Thread B: COPY INTO each table in `chain_bronzes` sequentially, then
        ALTER DYNAMIC TABLE `chain_dim_refresh` REFRESH (still in the same
        thread, same Snowflake session, sequential statements).

    A and B run in parallel. After both finish, ALTER DYNAMIC TABLE `leaf_dt`
    REFRESH fires on a fresh connection. Returns a dict with timings + row
    counts for downstream observability.

    `sf_connect_fn` is the `sf_connect` function — passed in because this
    helper module doesn't `%run` anything; the caller has it in scope from
    `%run ../_sf_conn` and hands it to us.
    """
    import time as _time
    import concurrent.futures as _cf

    def _open(step: str):
        return sf_connect_fn(**sf_connect_kwargs, query_tag={**query_tag_base, "step": step})

    def _do_independent(table: str):
        t0 = _time.time()
        c = _open(f"copy_{table}")
        try:
            n = copy_bronze(c.cursor(), catalog=catalog, target_schema=target_schema,
                            stage_root=stage_root, table=table)
            return {"table": table, "rows": n, "wall_s": _time.time() - t0}
        finally:
            c.close()

    def _do_chain():
        t0 = _time.time()
        c = _open("chain")
        try:
            cc = c.cursor()
            items = []
            for t in chain_bronzes:
                tt = _time.time()
                n = copy_bronze(cc, catalog=catalog, target_schema=target_schema,
                                stage_root=stage_root, table=t)
                items.append({"table": t, "rows": n, "wall_s": _time.time() - tt})
            if chain_dim_refresh:
                tr = _time.time()
                cc.execute(f"ALTER DYNAMIC TABLE {catalog}.{target_schema}.{chain_dim_refresh} REFRESH")
                refresh_rows = cc.fetchall()
                items.append({
                    "table": chain_dim_refresh,
                    "rows": None,
                    "wall_s": _time.time() - tr,
                    "alter_refresh_result": str(refresh_rows)[:500],
                })
            return {"items": items, "wall_s": _time.time() - t0}
        finally:
            c.close()

    t_start = _time.time()
    out: dict = {"independent": None, "chain": None, "leaf": None}

    futures = {}
    with _cf.ThreadPoolExecutor(max_workers=2) as ex:
        if independent_bronze:
            futures[ex.submit(_do_independent, independent_bronze)] = "independent"
        if chain_bronzes or chain_dim_refresh:
            futures[ex.submit(_do_chain)] = "chain"
        for f in _cf.as_completed(futures):
            kind = futures[f]
            out[kind] = f.result()
            print(f"[{kind}] {out[kind]}")

    # Leaf refresh — fresh connection.
    t_leaf = _time.time()
    c = _open(f"leaf_{leaf_dt}")
    try:
        cc = c.cursor()
        cc.execute(f"ALTER DYNAMIC TABLE {catalog}.{target_schema}.{leaf_dt} REFRESH")
        leaf_rows = cc.fetchall()
    finally:
        c.close()
    out["leaf"] = {
        "table": leaf_dt,
        "wall_s": _time.time() - t_leaf,
        "alter_refresh_result": str(leaf_rows)[:500],
    }
    out["branch_wall_s"] = _time.time() - t_start
    print(f"[leaf] {leaf_dt} {out['leaf']}")
    print(f"[branch_wall_s] {out['branch_wall_s']:.1f}")
    return out
