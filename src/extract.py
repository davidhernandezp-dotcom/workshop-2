"""EXTRACT stage: acquire both sources *without* cleaning them.

* Spotify  -> CSV file under ``data/raw`` (path configurable per DAG Run).
* Grammys  -> relational source database (PostgreSQL table ``public.grammy_awards``).
             The operational pipeline never reads the Grammy CSV directly.

Each function persists the raw batch to the run directory and returns *compact
metadata* (paths, counts, hashes) - never a DataFrame - so Airflow's metadata
database is not used as a bulk-data transport.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

from . import config
from .errors import SourceContractError

log = logging.getLogger("airflow.task")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_metadata(meta: dict, run_dir: Path, name: str) -> None:
    out = run_dir / "raw" / f"{name}_extract_metadata.json"
    out.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def extract_spotify(source_filename: str, run_dir: Path) -> dict:
    """Read the Spotify CSV as-is and persist it as the raw working file."""
    if Path(source_filename).name != source_filename:
        raise SourceContractError(f"spotify_filename must be a plain file name: {source_filename!r}")
    source_path = config.RAW_DIR / source_filename
    if not source_path.exists():
        # Deterministic failure: retrying cannot create the file.
        raise SourceContractError(f"Spotify source file not found: {source_path}")

    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(source_path, low_memory=False)
    raw_path = run_dir / "raw" / "spotify_raw.csv"
    df.to_csv(raw_path, index=False)

    meta = {
        "dataset": "spotify",
        "source": str(source_path),
        "source_sha256": _sha256_file(source_path),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "raw_path": str(raw_path),
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _write_metadata(meta, run_dir, "spotify")
    log.info("[extract_spotify] source=%s rows=%d columns=%d -> %s",
             source_path.name, meta["rows"], len(df.columns), raw_path)
    return meta


def extract_grammys(table: str, run_dir: Path) -> dict:
    """Read the Grammy table from the relational source database.

    Failure classes:
      * database unreachable / connection reset -> ``OperationalError`` is *re-raised*
        (transient: Airflow retries with back-off);
      * table does not exist -> deterministic ``SourceContractError`` (no retry).
    """
    table = config.safe_identifier(table)
    engine = create_engine(config.db_url(config.SOURCE_DB_NAME), pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM public.{table}"), conn)
            db_count = conn.execute(text(f"SELECT count(*) FROM public.{table}")).scalar_one()
    except ProgrammingError as exc:
        raise SourceContractError(
            f"Source table public.{table} could not be queried in database "
            f"{config.SOURCE_DB_NAME}: {exc.orig}"
        ) from exc
    finally:
        engine.dispose()

    if "row_id" in df.columns:
        df = df.sort_values("row_id").reset_index(drop=True)

    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    raw_path = run_dir / "raw" / "grammys_raw.csv"
    df.to_csv(raw_path, index=False)
    content_sha = hashlib.sha256(df.to_csv(index=False).encode("utf-8")).hexdigest()

    meta = {
        "dataset": "grammys",
        "source": f"postgresql://{config.DB_HOST}/{config.SOURCE_DB_NAME}.public.{table}",
        "source_sha256": content_sha,
        "rows": int(len(df)),
        "db_count": int(db_count),
        "columns": list(df.columns),
        "raw_path": str(raw_path),
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _write_metadata(meta, run_dir, "grammys")
    log.info("[extract_grammys] table=public.%s db_count=%d rows_read=%d -> %s",
             table, db_count, meta["rows"], raw_path)
    return meta
