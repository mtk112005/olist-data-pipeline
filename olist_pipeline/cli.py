"""Command line entry point (used locally, in CI and by the Airflow DAGs).

    python -m olist_pipeline.cli land
    python -m olist_pipeline.cli silver
    python -m olist_pipeline.cli load --mode full
    python -m olist_pipeline.cli load --mode month --month 2017-11
"""
import argparse
import json

from . import lake, warehouse


def main(argv=None):
    p = argparse.ArgumentParser(prog="olist_pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("land")
    sub.add_parser("silver")
    ld = sub.add_parser("load")
    ld.add_argument("--mode", choices=["full", "month"], default="full")
    ld.add_argument("--month", help="YYYY-MM, required with --mode month")
    sub.add_parser("months")
    a = p.parse_args(argv)

    if a.cmd == "land":
        out = lake.land()
    elif a.cmd == "silver":
        out = lake.to_silver()
    elif a.cmd == "months":
        out = warehouse.available_months()
    else:
        with warehouse.connect() as conn:
            if a.mode == "full":
                out = warehouse.load_full(conn)
            else:
                if not a.month:
                    p.error("--month is required with --mode month")
                out = warehouse.load_month(conn, a.month)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
