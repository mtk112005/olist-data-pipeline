"""Olist ELT orchestration.

olist_monthly_replay
    Replays Olist history one month at a time (Sep 2016 -> Oct 2018) as if the
    marketplace sent us a monthly extract. Each run: land + validate files,
    load that month's orders incrementally, then `dbt build` (incremental
    models only process new rows), then record warehouse row counts.
    Trigger it once and Airflow backfills all 26 months in order.

olist_full_refresh
    Manual rebuild: full load + `dbt build --full-refresh` + `dbt docs generate`.
"""
import os
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT = os.getenv("DBT_BIN", "dbt")
DBT_DIR = "/opt/project/dbt"
DBT_CMD = f"cd {DBT_DIR} && {DBT} {{cmd}} --profiles-dir {DBT_DIR}"
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1}


def dbt_task(task_id: str, cmd: str) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=DBT_CMD.format(cmd=cmd))


@task
def land_and_validate() -> dict:
    """Copy new file versions to bronze; rebuild silver only when something changed."""
    from olist_pipeline import config, lake

    landed = lake.land()
    if any(v != "unchanged" for v in landed.values()) or not any(config.SILVER.glob("*.parquet")):
        report = lake.to_silver()
        quarantined = {t: r["quarantined"] for t, r in report.items() if r["quarantined"]}
        print("silver rebuilt; quarantined rows:", quarantined or "none")
    return landed


@task
def warehouse_counts() -> dict:
    from olist_pipeline import warehouse

    with warehouse.connect() as conn, conn.cursor() as cur:
        cur.execute("""select 'fct_orders', count(*) from core.fct_orders
                       union all select 'fct_order_items', count(*) from core.fct_order_items
                       union all select 'dim_customers', count(*) from core.dim_customers""")
        counts = dict(cur.fetchall())
    print(counts)
    return counts


@dag(
    dag_id="olist_monthly_replay",
    start_date=datetime(2016, 9, 1),
    end_date=datetime(2018, 10, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,          # months must load in order for incremental models
    default_args=DEFAULT_ARGS,
    tags=["olist", "incremental", "dbt"],
    doc_md=__doc__,
)
def olist_monthly_replay():
    @task
    def load_month(data_interval_start=None) -> dict:
        from olist_pipeline import warehouse

        month = data_interval_start.strftime("%Y-%m")
        with warehouse.connect() as conn:
            counts = warehouse.load_month(conn, month)
        print(f"loaded {month}: {counts}")
        return counts

    # Install packages only if missing: re-running `dbt deps` every month wipes
    # dbt_packages/ and races with anything else reading the project.
    deps = BashOperator(task_id="dbt_deps",
                        bash_command=f"cd {DBT_DIR} && [ -d dbt_packages/dbt_utils ] || "
                                     f"{DBT} deps --profiles-dir {DBT_DIR}")
    build = dbt_task("dbt_build", "build")
    land_and_validate() >> load_month() >> deps >> build >> warehouse_counts()


@dag(
    dag_id="olist_full_refresh",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["olist", "full-refresh", "dbt"],
)
def olist_full_refresh():
    @task
    def load_full() -> dict:
        from olist_pipeline import warehouse

        with warehouse.connect() as conn:
            return warehouse.load_full(conn)

    (land_and_validate() >> load_full() >> dbt_task("dbt_deps", "deps")
     >> dbt_task("source_freshness", "source freshness")
     >> dbt_task("dbt_build", "build --full-refresh")
     >> dbt_task("dbt_docs", "docs generate") >> warehouse_counts())


olist_monthly_replay()
olist_full_refresh()
