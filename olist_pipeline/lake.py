"""Landing -> bronze -> silver.

bronze: an immutable, checksummed copy of every file version we have received
        (lake/bronze/<table>/ingest_date=YYYY-MM-DD/<file>), plus a manifest.
silver: typed, cleaned, de-duplicated parquet per table, contract-checked.
        Rows that break the contract are written to lake/quarantine/ and
        counted in a data-quality report instead of being silently dropped.
"""
import hashlib
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from .config import BRONZE, LANDING, QUARANTINE, REPORTS, SILVER
from .contracts import CONTRACTS, Contract


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def find_landing_file(contract: Contract, landing: Path = LANDING) -> Path:
    hits = list(landing.rglob(contract.file))
    if not hits:
        raise FileNotFoundError(f"{contract.file} not found under {landing}")
    return hits[0]


def land(landing: Path = LANDING, bronze: Path = BRONZE) -> dict:
    """Copy new/changed source files into bronze. Unchanged files are skipped,
    so re-running is safe (idempotent)."""
    summary = {}
    for table, c in CONTRACTS.items():
        src = find_landing_file(c, landing)
        digest = sha256(src)
        manifest_path = bronze / table / "_manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else []
        if manifest and manifest[-1]["sha256"] == digest:
            summary[table] = "unchanged"
            continue
        dest = bronze / table / f"ingest_date={date.today().isoformat()}" / c.file
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        manifest.append({"sha256": digest, "path": str(dest.relative_to(bronze)),
                         "bytes": src.stat().st_size,
                         "landed_at": datetime.now(timezone.utc).isoformat()})
        manifest_path.write_text(json.dumps(manifest, indent=2))
        summary[table] = "new version"
    return summary


def latest_bronze(table: str, bronze: Path = BRONZE) -> Path:
    manifest = json.loads((bronze / table / "_manifest.json").read_text())
    return bronze / manifest[-1]["path"]


def cast(df: pd.DataFrame, c: Contract) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for col, kind in c.columns.items():
        s = df[col] if col in df else pd.Series(pd.NA, index=df.index)
        if kind == "str":
            s = s.astype("string").str.strip().replace("", pd.NA)
        elif kind == "zip":
            s = pd.to_numeric(s, errors="coerce").astype("Int64").astype("string").str.zfill(5)
        elif kind == "int":
            s = pd.to_numeric(s, errors="coerce").round().astype("Int64")
        elif kind == "float":
            s = pd.to_numeric(s, errors="coerce")
        elif kind == "ts":
            s = pd.to_datetime(s, errors="coerce")
        out[col] = s
    for col in [k for k, v in c.columns.items() if k.endswith("_city")]:
        out[col] = out[col].str.lower()
    return out


def validate(df: pd.DataFrame, c: Contract) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Split rows into (good, quarantined) and return per-rule failure counts."""
    bad = pd.Series(False, index=df.index)
    reasons = {}
    for col in (*c.primary_key, *c.required):
        m = df[col].isna()
        if m.any():
            reasons[f"null_{col}"] = int(m.sum())
        bad |= m
    for col, values in c.allowed.items():
        m = df[col].notna() & ~df[col].isin(values)
        if m.any():
            reasons[f"not_allowed_{col}"] = int(m.sum())
        bad |= m
    good = df[~bad]
    dup = good.duplicated(subset=list(c.primary_key), keep="first")
    reasons["duplicate_key_dropped"] = int(dup.sum())
    return good[~dup], df[bad], reasons


def to_silver(bronze: Path = BRONZE, silver: Path = SILVER) -> dict:
    report = {}
    silver.mkdir(parents=True, exist_ok=True)
    QUARANTINE.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    for table, c in CONTRACTS.items():
        src = latest_bronze(table, bronze)
        raw = pd.read_csv(src, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        typed = cast(raw, c)
        good, bad, reasons = validate(typed, c)
        good.to_parquet(silver / f"{table}.parquet", index=False)
        if len(bad):
            bad.to_parquet(QUARANTINE / f"{table}.parquet", index=False)
        report[table] = {"source": str(src.name), "rows_in": len(raw), "rows_out": len(good),
                         "quarantined": len(bad), "rules": reasons}
    (REPORTS / "dq_silver.json").write_text(json.dumps(report, indent=2))
    return report
