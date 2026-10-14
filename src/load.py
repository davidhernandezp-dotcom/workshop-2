"""LOAD stage: persist the *validated* prepared tables into the PostgreSQL warehouse.

Idempotent / rerun-safe strategy (documented in docs/failure_and_rerun_policy.md):

* The whole load is ONE database transaction: schema check -> TRUNCATE of the six
  analytical tables -> COPY of the six prepared files -> audit rows -> commit.
  If anything fails, PostgreSQL rolls everything back and the warehouse keeps the
  previous, consistent state (no half-loaded fact table).
* TRUNCATE + reload ("full refresh") is the right strategy because every run carries the
  complete batch of both sources; therefore running the same batch twice yields exactly
  the same rows, never duplicates.
* Surrogate keys are assigned deterministically in the transform step (sorted business
  keys), so the same input always produces the same keys.
* ``dw.audit_load`` is keyed by ``batch_key`` (hash of both input batches): reruns of the
  same input update ``load_count`` instead of inserting a second audit row.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2 import errors as pg_errors

from . import config
from . import schemas as S
from .errors import LoadContractError

log = logging.getLogger("airflow.task")

_ANALYTICAL_TABLES = ", ".join(f"{config.DW_SCHEMA}.{t}" for t in S.LOAD_ORDER)


def _connect():
    return psycopg2.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER,
                            password=config.DB_PASSWORD, dbname=config.DW_DB_NAME,
                            connect_timeout=10)


def _count(cur, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM {config.DW_SCHEMA}.{table}")
    return int(cur.fetchone()[0])


def load_dw(transform_meta: dict, run_id: str, validation_files: list[str]) -> dict:
    prepared_dir = Path(transform_meta["prepared_dir"])
    batch_key = transform_meta["batch_key"]
    expected = transform_meta["tables"]
    schema_sql = (config.SQL_DIR / "dw_schema.sql").read_text(encoding="utf-8")

    conn = _connect()
    try:
        with conn:                                   # BEGIN ... COMMIT / ROLLBACK
            with conn.cursor() as cur:
                cur.execute(schema_sql)                                 # idempotent DDL + views
                cur.execute(f"TRUNCATE TABLE {_ANALYTICAL_TABLES}")      # full refresh inside the transaction
                for table in S.LOAD_ORDER:
                    columns = ", ".join(S.PREPARED_COLUMNS[table])
                    with open(prepared_dir / f"{table}.csv", "r", encoding="utf-8") as fh:
                        cur.copy_expert(
                            f"COPY {config.DW_SCHEMA}.{table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)", fh)
                    loaded = _count(cur, table)
                    if loaded != expected[table]:
                        raise LoadContractError(
                            f"Row-count mismatch for {table}: prepared={expected[table]} loaded={loaded}")
                    log.info("[load] %-30s rows loaded = %d", table, loaded)

                # post-load reconciliation of a business invariant
                cur.execute(f"SELECT COALESCE(sum(spotify_matched),0), count(*) FROM {config.DW_SCHEMA}.fact_grammy_nomination")
                matched, total = cur.fetchone()
                if total != transform_meta["expected_fact_rows"]:
                    raise LoadContractError(f"Fact rows {total} != expected {transform_meta['expected_fact_rows']}")
                log.info("[load] post-load check: fact rows=%d matched=%d", total, matched)

                # audit: one row per batch (rerun-safe)
                now = datetime.now(timezone.utc)
                cur.execute(f"""
                    INSERT INTO {config.DW_SCHEMA}.audit_load
                        (batch_key, first_run_id, last_run_id, first_loaded_at, last_loaded_at, load_count, rows_loaded)
                    VALUES (%s, %s, %s, %s, %s, 1, %s)
                    ON CONFLICT (batch_key) DO UPDATE SET
                        last_run_id = EXCLUDED.last_run_id,
                        last_loaded_at = EXCLUDED.last_loaded_at,
                        load_count = {config.DW_SCHEMA}.audit_load.load_count + 1,
                        rows_loaded = EXCLUDED.rows_loaded
                    """, (batch_key, run_id, run_id, now, now, json.dumps(expected)))

                cur.execute(f"DELETE FROM {config.DW_SCHEMA}.audit_validation_result WHERE batch_key = %s", (batch_key,))
                n_audit = 0
                for path in validation_files:
                    payload = json.loads(Path(path).read_text(encoding="utf-8"))
                    for r in payload["results"]:
                        cur.execute(f"""
                            INSERT INTO {config.DW_SCHEMA}.audit_validation_result
                              (batch_key, gate, dataset, rule_id, expectation_type, column_name, severity,
                               success, unexpected_count, unexpected_percent, observed_value)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (batch_key, payload["gate"], r["dataset"], r["rule_id"], r["expectation_type"],
                             str(r["parameters"].get("column") or r["parameters"].get("column_list") or "table"),
                             r["severity"], r["success"], r["unexpected_count"], r["unexpected_percent"],
                             None if r["observed_value"] is None else str(r["observed_value"])[:200]))
                        n_audit += 1
                cur.execute(f"SELECT load_count FROM {config.DW_SCHEMA}.audit_load WHERE batch_key = %s", (batch_key,))
                load_count = int(cur.fetchone()[0])
        log.info("[load] COMMIT ok | batch_key=%s | load_count=%d | audit validation rows=%d",
                 batch_key, load_count, n_audit)
    except (pg_errors.IntegrityError, pg_errors.DataError, pg_errors.UndefinedTable,
            pg_errors.UndefinedColumn, pg_errors.SyntaxError) as exc:
        # deterministic: the same input would be rejected again -> fail without retries
        raise LoadContractError(f"Warehouse rejected the data (transaction rolled back): {exc}") from exc
    finally:
        conn.close()                                 # OperationalError (DB down) propagates -> Airflow retries

    return {"batch_key": batch_key, "load_count": load_count, "rows_loaded": expected,
            "matched_nominations": int(matched), "audit_validation_rows": n_audit}
