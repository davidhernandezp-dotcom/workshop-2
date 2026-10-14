"""Central configuration for the Workshop-2 pipeline.

Everything that can change between machines (paths, database hosts, credentials)
comes from environment variables so that no secret is committed to Git.
Inside Docker the values are injected through ``.env`` (see ``.env.example``).
Outside Docker (profiling notebook, tests, helper scripts) sensible local
defaults are used.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"          # original, untouched input files
WORK_DIR = DATA_DIR / "work"        # one sub-folder per DAG Run
SQL_DIR = PROJECT_ROOT / "sql"

SPOTIFY_DEFAULT_FILE = "spotify_dataset.csv"
GRAMMY_DEFAULT_TABLE = "grammy_awards"
GRAMMY_CSV_FILE = "the_grammy_awards.csv"

# --------------------------------------------------------------------------
# Databases (one PostgreSQL server, two databases)
# --------------------------------------------------------------------------
DB_HOST = os.getenv("ETL_DB_HOST", "localhost")
DB_PORT = os.getenv("ETL_DB_PORT", "5433")
DB_USER = os.getenv("ETL_DB_USER", "etl_user")
DB_PASSWORD = os.getenv("ETL_DB_PASSWORD", "etl_password")
SOURCE_DB_NAME = os.getenv("SOURCE_DB_NAME", "grammy_source")
DW_DB_NAME = os.getenv("DW_DB_NAME", "music_dw")

DW_SCHEMA = "dw"

_TABLE_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def db_url(db_name: str) -> str:
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{db_name}"


def safe_identifier(name: str) -> str:
    """Return ``name`` only if it is a plain lower-case SQL identifier.

    Table names arrive as DAG parameters, so they are validated before being
    placed in a SQL statement (identifiers cannot be bound parameters).
    """
    if not _TABLE_NAME_RE.match(name or ""):
        raise ValueError(f"Unsafe or invalid SQL identifier: {name!r}")
    return name


def safe_run_dir_name(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which Windows bind mounts reject."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)
