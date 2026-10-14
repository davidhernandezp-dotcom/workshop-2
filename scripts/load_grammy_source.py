"""SOURCE PREPARATION: load the provided Grammy CSV into the relational source DB.

This is *not* the ETL Load stage. It only creates the operational source that the
pipeline will later extract from (``public.grammy_awards`` in ``grammy_source``).

The script is idempotent: it truncates and reloads the table, so running it twice
produces the same row count. It finishes with a reconciliation between the CSV and
the table (row count + null counts per column + two content checksums) and writes
the result to ``docs/evidence/source_reconciliation.json``.

Run (inside the Airflow container, see README):
    python /opt/airflow/scripts/load_grammy_source.py
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import config  # noqa: E402

CSV_PATH = config.RAW_DIR / config.GRAMMY_CSV_FILE
TABLE = f"public.{config.GRAMMY_DEFAULT_TABLE}"
CSV_COLUMNS = ["year", "title", "published_at", "updated_at", "category",
               "nominee", "artist", "workers", "img", "winner"]


def main() -> int:
    if not CSV_PATH.exists():
        print(f"ERROR: input file not found: {CSV_PATH}")
        return 1

    csv_df = pd.read_csv(CSV_PATH)
    if list(csv_df.columns) != CSV_COLUMNS:
        print(f"ERROR: unexpected CSV columns: {list(csv_df.columns)}")
        return 1

    conn = psycopg2.connect(
        host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER,
        password=config.DB_PASSWORD, dbname=config.SOURCE_DB_NAME,
    )
    try:
        with conn, conn.cursor() as cur:               # one transaction
            cur.execute((config.SQL_DIR / "source_setup.sql").read_text(encoding="utf-8"))
            cur.execute(f"TRUNCATE TABLE {TABLE} RESTART IDENTITY")
            with open(CSV_PATH, "r", encoding="utf-8") as fh:
                cur.copy_expert(
                    f"COPY {TABLE} ({', '.join(CSV_COLUMNS)}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                    fh,
                )
            cur.execute(f"SELECT count(*) FROM {TABLE}")
            db_rows = cur.fetchone()[0]
            null_counts = {}
            for col in CSV_COLUMNS:
                cur.execute(f"SELECT count(*) FROM {TABLE} WHERE {col} IS NULL")
                null_counts[col] = cur.fetchone()[0]
            cur.execute(f"SELECT sum(year)::bigint, count(*) FILTER (WHERE winner) FROM {TABLE}")
            year_sum, winners_true = cur.fetchone()
    finally:
        conn.close()

    csv_nulls = {c: int(csv_df[c].isna().sum()) for c in CSV_COLUMNS}
    checks = {
        "row_count": (len(csv_df), db_rows),
        "year_sum": (int(csv_df["year"].sum()), int(year_sum)),
        "winner_true_count": (int(csv_df["winner"].sum()), int(winners_true)),
    }
    for col in CSV_COLUMNS:
        checks[f"nulls::{col}"] = (csv_nulls[col], null_counts[col])

    report = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "csv_file": str(CSV_PATH.name),
        "target_table": TABLE,
        "checks": {k: {"csv": v[0], "table": v[1], "match": v[0] == v[1]} for k, v in checks.items()},
    }
    report["reconciled"] = all(c["match"] for c in report["checks"].values())

    print("Source preparation reconciliation (CSV vs table)")
    print("-" * 52)
    for name, c in report["checks"].items():
        flag = "OK " if c["match"] else "ERR"
        print(f"[{flag}] {name:<24} csv={c['csv']:<10} table={c['table']}")
    print("RECONCILED:", report["reconciled"])

    out = config.PROJECT_ROOT / "docs" / "evidence" / "source_reconciliation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("Report written to", out)
    return 0 if report["reconciled"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
