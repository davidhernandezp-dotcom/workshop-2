# Test scenarios

Three required scenarios, each with procedure, expected result and where the evidence lives.
`dag_id` = `grammy_spotify_reliable_pipeline`.

## Test A - successful end-to-end run
1. Trigger the DAG with the default configuration.
2. **Expected**: 8 tasks `success`; `validate_spotify_raw` -> `CONTINUE` (17/17 expectations); `validate_grammys_raw` -> `CONTINUE_WITH_WARNING`
   (11/12; **GR-08** fails on purpose: `winner` is always True); `validate_prepared` -> `CONTINUE` (44/44).
3. DW: `fact_grammy_nomination` 4,804 | `dim_year` 62 | `dim_category` 626 | `dim_genre` 114 | `dim_track` 223 | `fact_catalog_genre_baseline` 113.
4. Evidence: `docs/evidence/logs/run1_success.log`, `docs/evidence/runs/run1_success/`.

## Test B - controlled critical failure
1. `docker compose exec airflow-scheduler python /opt/airflow/scripts/make_bad_batches.py spotify` (creates `spotify_dataset_bad.csv` with 100 rows `popularity = 150`).
2. Trigger **with config** `{"spotify_filename": "spotify_dataset_bad.csv"}`.
3. **Expected**: `validate_spotify_raw` **failed** (rule **SP-05**, critical, `unexpected = 100`, 0.0877%); `validate_grammys_raw` still succeeds;
   `transform_and_integrate`, `validate_prepared`, `load_dw`, `publish_run_summary` = `upstream_failed`; the DW keeps its previous content.
4. Variant on the other branch: `make_bad_batches.py grammy`, trigger with `{"grammy_table": "grammy_awards_bad"}` -> **GR-03** (year = 2099, 30 rows).
5. Evidence: `docs/evidence/logs/run2_critical_failure.log`, `run5_grammy_critical_failure.log`, and the stored `validation/*.json` per run.
6. Recovery: trigger again with the default configuration -> success (run 3).

## Test C - rerun safety and retry
**Rerun (executed):** trigger the DAG again on the same input. Expected: same row counts, `duplicated_nomination_bk = 0`, `dw.audit_load.load_count` increases
by 1 (same `batch_key`). Evidence: `docs/evidence/sql_outputs/idempotency_after_3_successful_loads.txt`, `integrity_checks.txt`, `run4_rerun_same_input.log`.

**Transient retry (to run on your machine):**
1. `docker compose stop etl-db`, then trigger the DAG. `extract_grammys` fails and goes to **`up_for_retry`** (up to 3 retries; exponential back-off with jitter: ~20-40 s, 40-80 s, 80-120 s).
2. Within ~1 minute (after you see the first retry) `docker compose start etl-db` -> the next attempt succeeds. Capture the task's *Attempts* tab / log showing attempt 1 failed and attempt 2+ succeeded.
3. Contrast: `Trigger w/ config {"grammy_table": "no_such_table"}` -> `extract_grammys` fails **immediately without retries** (`SourceContractError`).

## What was executed where
| Item | Status |
|---|---|
| Pipeline code on **Airflow 3.1.8** (`dag.test()`), **Great Expectations 1.23.1**, **PostgreSQL 16**, full runs 1-5 | executed by the author, evidence in `docs/evidence/` |
| 30 unit tests (`pytest`) | executed, all pass |
| Profiling notebook | executed, outputs stored |
| `docker build` / `docker compose up`, Airflow UI with Celery executor, Power BI | **not executed** in the authoring environment - YAML validated only; follow README and capture your own screenshots |
| Transient-retry behaviour (Test C, second part) | designed and unit-classified (`OperationalError` re-raised vs `SourceContractError`), but **not observed live** - run it as described |
