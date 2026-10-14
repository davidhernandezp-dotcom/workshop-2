# Evidence index

Produced by the author in a test environment: Airflow 3.1.8 `dag.test()`, Great Expectations 1.23.1, PostgreSQL 16 (Linux, **outside Docker**;
paths in logs therefore start with `/tmp/wdata`). **Replace/complement with your own screenshots from the Airflow UI and Power BI.**

| Item | File |
|---|---|
| Source preparation reconciliation (CSV vs table) | `source_reconciliation.json` (written by `scripts/load_grammy_source.py`) |
| Profiling tables and figures | `profiling/` |
| Run 1 – success | `logs/run1_success.log`, `runs/run1_success/` |
| Run 2 – critical failure, Spotify (SP-05) | `logs/run2_critical_failure.log`, `runs/run2_critical_failure/` |
| Run 3 – restored success | `logs/run3_restored_success.log` |
| Run 4 – rerun, same input | `logs/run4_rerun_same_input.log`, `sql_outputs/idempotency_after_3_successful_loads.txt`, `sql_outputs/integrity_checks.txt` |
| Run 5 – critical failure, Grammy (GR-03) | `logs/run5_grammy_critical_failure.log` |
| KPI query outputs | `sql_outputs/kpi_views_output.txt` |
| Dashboard preview (not the BI deliverable) | `dashboard_preview.png` |

Note: the idempotency output shows `load_count = 3` because only runs 1, 3 and 4 reached the load; runs 2 and 5 were stopped by the gates.
