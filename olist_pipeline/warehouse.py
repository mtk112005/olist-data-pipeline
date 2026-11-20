"""Silver -> Postgres `raw` schema, using COPY (not row-by-row INSERTs).

Two load modes:
* full                - truncate and reload every table
* incremental(month)  - reference tables are refreshed; orders purchased in
                        `month` and their items/payments/reviews are upserted
                        (delete-by-order_id then insert), so re-running a month
                        is idempotent. This lets Airflow replay history one
                        month at a time, as if data arrived in real time.
Every load writes a row per table to raw._load_audit.
"""
import io
from contextlib import contextmanager

import pandas as pd
import psycopg2

from .config import SILVER, WAREHOUSE
from .contracts import CONTRACTS, raw_ddl


@contextmanager
def connect(**overrides):
    conn = psycopg2.connect(**{**WAREHOUSE, **overrides})
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema(cur):
    cur.execute("create schema if not exists raw")
    for t in CONTRACTS:
        cur.execute(raw_ddl(t))
    cur.execute("""create table if not exists raw._load_audit (
        batch_id text, table_name text, mode text, rows_loaded integer,
        loaded_at timestamptz default now())""")


def _copy(cur, df: pd.DataFrame, target: str, batch_id: str):
    cols = list(df.columns) + ["_batch_id"]
    buf = io.StringIO()
    df.assign(_batch_id=batch_id).to_csv(buf, index=False, header=False, na_rep="\\N",
                                         date_format="%Y-%m-%d %H:%M:%S")
    buf.seek(0)
    cur.copy_expert(f"copy {target} ({', '.join(cols)}) from stdin with (format csv, null '\\N')", buf)


def _audit(cur, batch_id, table, mode, n):
    cur.execute("insert into raw._load_audit (batch_id, table_name, mode, rows_loaded) values (%s,%s,%s,%s)",
                (batch_id, table, mode, n))


def read_silver(table: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER / f"{table}.parquet")


def load_full(conn, batch_id: str = "full") -> dict:
    counts = {}
    with conn.cursor() as cur:
        ensure_schema(cur)
        for table in CONTRACTS:
            df = read_silver(table)
            cur.execute(f"truncate raw.{table}")
            _copy(cur, df, f"raw.{table}", batch_id)
            _audit(cur, batch_id, table, "full", len(df))
            counts[table] = len(df)
    return counts


def _upsert_by_order(cur, df: pd.DataFrame, table: str, batch_id: str):
    cur.execute(f"create temp table _stage (like raw.{table} including defaults) on commit drop")
    _copy(cur, df, "_stage", batch_id)
    cur.execute(f"delete from raw.{table} t using (select distinct order_id from _stage) s "
                f"where t.order_id = s.order_id")
    cur.execute(f"insert into raw.{table} select * from _stage")
    cur.execute("drop table _stage")


def load_month(conn, month: str) -> dict:
    """month = 'YYYY-MM'. Loads orders purchased in that month plus children."""
    batch_id = f"month={month}"
    counts = {}
    orders = read_silver("orders")
    period = orders.order_purchase_timestamp.dt.strftime("%Y-%m")
    month_orders = orders[period == month]
    ids = set(month_orders.order_id)
    with conn.cursor() as cur:
        ensure_schema(cur)
        for table, c in CONTRACTS.items():
            if c.kind == "reference":
                df = read_silver(table)
                cur.execute(f"truncate raw.{table}")
                _copy(cur, df, f"raw.{table}", batch_id)
            else:
                df = month_orders if table == "orders" else read_silver(table).query("order_id in @ids")
                _upsert_by_order(cur, df, table, batch_id)
            _audit(cur, batch_id, table, "incremental", len(df))
            counts[table] = len(df)
    return counts


def available_months() -> list[str]:
    o = read_silver("orders")
    return sorted(o.order_purchase_timestamp.dt.strftime("%Y-%m").dropna().unique())
