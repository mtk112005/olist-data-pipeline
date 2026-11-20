import os
from pathlib import Path

ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
LANDING = Path(os.getenv("LANDING_DIR", ROOT / "data" / "landing"))
LAKE = ROOT / "lake"
BRONZE = LAKE / "bronze"
SILVER = LAKE / "silver"
QUARANTINE = LAKE / "quarantine"
REPORTS = LAKE / "_reports"

WAREHOUSE = {
    "host": os.getenv("WAREHOUSE_HOST", "localhost"),
    "port": int(os.getenv("WAREHOUSE_PORT", "5434")),
    "dbname": os.getenv("WAREHOUSE_DB", "warehouse"),
    "user": os.getenv("WAREHOUSE_USER", "olist"),
    "password": os.getenv("WAREHOUSE_PASSWORD", "olist"),
}
