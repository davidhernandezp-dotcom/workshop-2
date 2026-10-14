"""
Workshop-2 - Reliable ETL: Spotify popularity & audio profile of Grammy-recognised tracks.

Airflow 3.1.8 (Task SDK / TaskFlow) | Great Expectations 1.23.1 | PostgreSQL

                      +--> extract_spotify --> validate_spotify_raw --+
        (two sources) |                                                +--> transform_and_integrate
                      +--> extract_grammys --> validate_grammys_raw --+            |
                          (relational DB)                                           v
                                                      load_dw  <--  validate_prepared
                                                         |
                                                         v
                                                 publish_run_summary

Airflow ORCHESTRATES; the processing lives in ``src/`` and the quality contract in
``src/quality_rules.py``.  Tasks exchange compact metadata (paths, counts, hashes),
never DataFrames: bulk data are persisted under ``data/work/<run_id>/``.

Trigger with configuration (Airflow UI -> Trigger DAG w/ config) to change the sources
without editing this file, e.g. the controlled critical failure:
    {"spotify_filename": "spotify_dataset_bad.csv"}
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.sdk import Param, dag, get_current_context, task

from src import config
from src import schemas as S

# NOTE: heavy libraries (Great Expectations, SQLAlchemy, numpy-based transforms) are imported INSIDE the tasks.
# The DAG processor re-parses this file continuously; importing them here would make every parse slow.

log = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------------
# Failure policy (see docs/failure_and_rerun_policy.md)
#   * deterministic problems (missing file, critical data-quality failure, constraint
#     violation)  -> retries = 0: repeating the task on the same input cannot fix it.
#   * transient infrastructure problems (source / warehouse database unreachable)
#     -> bounded retries with exponential back-off.
# ---------------------------------------------------------------------------
NO_RETRY = dict(retries=0, execution_timeout=timedelta(minutes=20))
DB_RETRY = dict(retries=3, retry_delay=timedelta(seconds=20), retry_exponential_backoff=True,
                max_retry_delay=timedelta(minutes=2), execution_timeout=timedelta(minutes=20))
LOAD_RETRY = dict(retries=2, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
                  max_retry_delay=timedelta(minutes=3), execution_timeout=timedelta(minutes=20))


def _run_context() -> tuple[str, dict, "config.Path"]:
    ctx = get_current_context()
    run_id = ctx["run_id"]
    run_dir = config.WORK_DIR / config.safe_run_dir_name(run_id)
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_id, dict(ctx["params"]), run_dir


@dag(
    dag_id="grammy_spotify_reliable_pipeline",
    schedule=None,                      # manual runs so that every execution can be inspected
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "workshop-2", "gx", "spotify", "grammy"],
    params={
        "spotify_filename": Param("spotify_dataset.csv", type="string", pattern=r"^[A-Za-z0-9_.-]+\.csv$",
                                  description="CSV inside data/raw used as the Spotify source."),
        "grammy_table": Param("grammy_awards", type="string", pattern=r"^[a-z_][a-z0-9_]*$",
                              description="Table of the relational source DB used as the Grammy source."),
    },
    doc_md=__doc__,
)
def grammy_spotify_reliable_pipeline():

    # ------------------------------------------------------------------ EXTRACT (parallel branches)
    @task(**NO_RETRY)
    def extract_spotify() -> dict:
        from src.extract import extract_spotify as _extract_spotify
        _, params, run_dir = _run_context()
        return _extract_spotify(params["spotify_filename"], run_dir)

    @task(**DB_RETRY)
    def extract_grammys() -> dict:
        from src.extract import extract_grammys as _extract_grammys
        _, params, run_dir = _run_context()
        return _extract_grammys(params["grammy_table"], run_dir)

    # ------------------------------------------------------------------ RAW VALIDATION GATES
    @task(**NO_RETRY)
    def validate_spotify_raw(meta: dict) -> dict:
        from src.validation import run_gate
        run_id, _, run_dir = _run_context()
        df = pd.read_csv(meta["raw_path"], low_memory=False)
        result = run_gate(gate="spotify_raw", layer="raw", tables={"spotify_raw": df},
                          run_dir=run_dir, run_id=run_id)        # raises on a critical failure
        return {**meta, "validation_path": result["result_path"], "decision": result["decision"]}

    @task(**NO_RETRY)
    def validate_grammys_raw(meta: dict) -> dict:
        from src.validation import run_gate
        run_id, _, run_dir = _run_context()
        df = pd.read_csv(meta["raw_path"])
        result = run_gate(gate="grammys_raw", layer="raw", tables={"grammys_raw": df},
                          run_dir=run_dir, run_id=run_id)
        return {**meta, "validation_path": result["result_path"], "decision": result["decision"]}

    # ------------------------------------------------------------------ TRANSFORM + INTEGRATE
    @task(**NO_RETRY)
    def transform_and_integrate(spotify: dict, grammys: dict) -> dict:
        from src.transform import transform_and_integrate as _transform
        _, _, run_dir = _run_context()
        meta = _transform(spotify, grammys, run_dir)
        meta["validation_files"] = [spotify["validation_path"], grammys["validation_path"]]
        return meta

    # ------------------------------------------------------------------ PREPARED VALIDATION GATE
    @task(**NO_RETRY)
    def validate_prepared(meta: dict) -> dict:
        from src.validation import run_gate
        run_id, _, run_dir = _run_context()
        tables = {name: pd.read_csv(f"{meta['prepared_dir']}/{name}.csv") for name in S.PREPARED_COLUMNS}
        result = run_gate(gate="prepared", layer="prepared", tables=tables, run_dir=run_dir, run_id=run_id,
                          batch_key=meta["batch_key"],
                          extra_ctx={"expected_fact_rows": meta["expected_fact_rows"]})
        return {**meta, "validation_files": meta["validation_files"] + [result["result_path"]],
                "prepared_decision": result["decision"]}

    # ------------------------------------------------------------------ LOAD (transactional, rerun-safe)
    @task(**LOAD_RETRY)
    def load_dw(meta: dict) -> dict:
        from src.load import load_dw as _load_dw
        run_id, _, _ = _run_context()
        result = _load_dw(meta, run_id, meta["validation_files"])
        return {**result, "validation_files": meta["validation_files"]}

    # ------------------------------------------------------------------ PUBLISH
    @task(**NO_RETRY)
    def publish_run_summary(load_result: dict) -> str:
        """Query the warehouse (not the files) for the headline KPIs and store a run summary."""
        from sqlalchemy import create_engine, text
        run_id, params, run_dir = _run_context()
        engine = create_engine(config.db_url(config.DW_DB_NAME))
        with engine.connect() as conn:
            headline = pd.read_sql(text("SELECT * FROM dw.v_kpi_headline"), conn).iloc[0].to_dict()
        engine.dispose()
        summary = {"run_id": run_id, "params": params, "batch_key": load_result["batch_key"],
                   "load_count_for_this_batch": load_result["load_count"],
                   "rows_loaded": load_result["rows_loaded"], "headline_kpis": headline,
                   "validation_files": load_result["validation_files"]}
        path = run_dir / "run_summary.json"
        path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        log.info("[publish] headline KPIs from the DW: %s", headline)
        log.info("[publish] run summary: %s", path)
        return str(path)

    # ------------------------------------------------------------------ WORKFLOW
    sp_raw = extract_spotify()
    gr_raw = extract_grammys()
    sp_ok = validate_spotify_raw(sp_raw)          # data flow = dependency: gate must succeed first
    gr_ok = validate_grammys_raw(gr_raw)
    prepared = transform_and_integrate(sp_ok, gr_ok)   # joins both branches (waits for both gates)
    prepared_ok = validate_prepared(prepared)
    loaded = load_dw(prepared_ok)
    publish_run_summary(loaded)


grammy_spotify_reliable_pipeline()
