# Failure, retry and rerun policy

## Validation severity -> pipeline action
| Failed rule severity | Decision | Airflow effect |
|---|---|---|
| any **critical** | `STOP` | the validation task raises `DataQualityGateError` (an `AirflowFailException`): task `failed`, **no retry**, downstream tasks `upstream_failed` |
| only **warning** | `CONTINUE_WITH_WARNING` | task succeeds; warning logged; result stored in JSON and `dw.audit_validation_result` |
| only **info** | `CONTINUE` | task succeeds; recorded |

Great Expectations only *detects and reports*; the policy lives in `src/validation.py::apply_policy`. Evidence JSON is written **before**
the exception is raised, so failed runs stay diagnosable.

## Retries per task
| Task | Retries | Reason |
|---|---|---|
| `extract_spotify` | 0 | local file: a missing file is deterministic |
| `extract_grammys` | 3, base 20 s, exponential back-off with jitter (~20-40 s, 40-80 s, 80-120 s; max 2 min) | **transient** DB/network problems. A missing table is deterministic -> `SourceContractError`, no retry |
| `validate_spotify_raw`, `validate_grammys_raw`, `validate_prepared` | 0 | same data -> same result |
| `transform_and_integrate` | 0 | deterministic code over files |
| `load_dw` | 2, base 30 s, exponential back-off with jitter (max 3 min) | safe to retry because the load is one atomic, idempotent transaction. Constraint / type errors -> `LoadContractError`, no retry |
| `publish_run_summary` | 0 | read-only |

## Isolation of branches
The Spotify and Grammy branches run **in parallel**. If one gate fails, the other branch still finishes its own validation (useful
diagnostics), but `transform_and_integrate` needs both and is never started: the DW is untouched.

## Rerun safety
* `load_dw` = **one transaction**: DDL check -> `TRUNCATE` of the six analytical tables -> `COPY` -> audit -> `COMMIT`. Any error rolls back and
  the previous consistent state remains (no half-loaded fact).
* **Full refresh** is appropriate: each run carries the complete batch of both sources, so reloading cannot duplicate rows.
* Deterministic surrogate keys + `UNIQUE(nomination_bk)` make duplicates impossible even if the strategy were changed.
* `batch_key` = SHA-256 of both input batches. `dw.audit_load` has **one row per batch**; reruns of the same input increase `load_count`.
* Run folders `data/work/<run_id>/` keep the raw extract, validation JSONs, prepared tables and reconciliation of every run (failed or not).

## Observed behaviour (author's test environment)
Run sequence from an empty DW: success -> critical failure (Spotify) -> success -> rerun -> critical failure (Grammy).
Result: fact rows stayed at 4,804, `load_count` = 3 (only successful loads count), 0 duplicated business keys. See `docs/evidence/`.
