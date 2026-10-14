"""Automated validation with Great Expectations 1.23.x.

GX object model used (one set per validated dataset, built in memory for every run
from the declarative registry in ``quality_rules.py``)::

    Data Context (ephemeral)
      +-- Data Source  (pandas)  -> Data Asset (dataframe) -> Batch Definition (whole dataframe)
      +-- Expectation Suite      (rules of ONE dataset / layer)
      +-- Validation Definition  = Batch Definition + Expectation Suite
      +-- Checkpoint             = runs the Validation Definition, returns Validation Results

GX *detects and reports*. The pipeline policy below decides what happens:

    any failed Critical        -> STOP      (task fails; downstream does not run)
    only failed Warning        -> CONTINUE_WITH_WARNING
    only failed Informational  -> CONTINUE  (recorded)
    nothing failed             -> CONTINUE

Every gate persists a machine-readable JSON with per-rule evidence under
``<run_dir>/validation/`` before the policy is applied, so failed runs remain
diagnosable.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import great_expectations as gx
from great_expectations.data_context.types.base import ProgressBarsConfig

from .errors import DataQualityGateError
from .quality_rules import ALL_RULES, Rule

log = logging.getLogger("airflow.task")

SEVERITY_ORDER = {"info": 1, "warning": 2, "critical": 3}


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def _jsonable(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        return None if (math.isnan(obj) or math.isinf(obj)) else float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_jsonable(v) for v in obj]
    if obj is pd.NA or obj is pd.NaT:
        return None
    return obj


def _resolve(value: Any, ctx: dict) -> Any:
    return value(ctx) if callable(value) else value


def _severity_name(sev: Any) -> str:
    return getattr(sev, "name", str(sev)).split(".")[-1].lower()


def collect_specs(layer: str, datasets: list[str] | None = None) -> dict[str, list[tuple[Rule, str, dict]]]:
    """Group the (rule, expectation class, kwargs) triples by validated dataset."""
    grouped: dict[str, list[tuple[Rule, str, dict]]] = {}
    for rule in ALL_RULES:
        if rule.layer != layer:
            continue
        for cls_name, kwargs in rule.specs:
            kw = dict(kwargs)
            ds = kw.pop("__dataset__", rule.dataset)
            if datasets is not None and ds not in datasets:
                continue
            grouped.setdefault(ds, []).append((rule, cls_name, kw))
    return grouped


def _new_context() -> Any:
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    return context


def build_checkpoint(context: Any, layer: str, dataset: str,
                     specs: list[tuple[Rule, str, dict]], ctx: dict) -> tuple[Any, Any]:
    """Create Data Source -> Asset -> Batch Definition -> Suite -> Validation Definition -> Checkpoint."""
    source = context.data_sources.add_pandas(name=f"{dataset}_pandas_source")
    asset = source.add_dataframe_asset(name=f"{dataset}_asset")
    batch_definition = asset.add_batch_definition_whole_dataframe(name=f"{dataset}_whole_dataframe")

    suite = context.suites.add(gx.ExpectationSuite(name=f"{dataset}_{layer}_suite"))
    for rule, cls_name, kwargs in specs:
        expectation_class = getattr(gx.expectations, cls_name)
        resolved = {k: _resolve(v, ctx) for k, v in kwargs.items()}
        suite.add_expectation(expectation_class(
            **resolved,
            severity=rule.severity,
            meta={"rule_id": rule.rule_id, "layer": layer, "dataset": dataset},
        ))

    validation_definition = context.validation_definitions.add(gx.ValidationDefinition(
        name=f"{dataset}_{layer}_validation", data=batch_definition, suite=suite))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name=f"{dataset}_{layer}_checkpoint",
        validation_definitions=[validation_definition],
        actions=[],                                    # policy is applied by the pipeline, see apply_policy()
        result_format={"result_format": "SUMMARY"},
    ))
    return checkpoint, suite


def _describe_expectation(expectation: Any) -> dict:
    kw = expectation.configuration.kwargs
    interesting = {}
    for key in ("column", "column_list", "min_value", "max_value", "strict_min", "mostly",
                "value", "value_set", "column_set", "exact_match", "row_condition"):
        if key in kw and kw[key] is not None:
            val = kw[key]
            if isinstance(val, (list, tuple, set)) and len(val) > 12:
                val = f"<{len(val)} values>"
            interesting[key] = _jsonable(val)
    return interesting


def _records_from_result(validation_result: Any, dataset: str, layer: str) -> list[dict]:
    records = []
    for res in validation_result.results:
        exp = res.expectation
        result = res.result or {}
        exc = res.exception_info or {}
        error = None
        for info in exc.values() if isinstance(exc, dict) else []:
            if isinstance(info, dict) and info.get("raised_exception"):
                error = str(info.get("exception_message", ""))[:300]
        records.append({
            "rule_id": (exp.meta or {}).get("rule_id"),
            "layer": layer,
            "dataset": dataset,
            "expectation_type": type(exp).__name__,
            "parameters": _describe_expectation(exp),
            "severity": _severity_name(exp.severity),
            "success": bool(res.success),
            "element_count": _jsonable(result.get("element_count")),
            "unexpected_count": _jsonable(result.get("unexpected_count")),
            "unexpected_percent": (None if result.get("unexpected_percent") is None
                                   else round(float(result["unexpected_percent"]), 4)),
            "observed_value": _jsonable(result.get("observed_value")),
            "partial_unexpected_list": _jsonable((result.get("partial_unexpected_list") or [])[:10]),
            "error": error,
        })
    return records


# ----------------------------------------------------------------------------
# policy
# ----------------------------------------------------------------------------
def apply_policy(records: list[dict]) -> dict:
    failed = [r for r in records if not r["success"]]
    max_sev = max((r["severity"] for r in failed), key=lambda s: SEVERITY_ORDER[s], default="none")
    decision = {"critical": "STOP", "warning": "CONTINUE_WITH_WARNING"}.get(max_sev, "CONTINUE")
    return {
        "max_failed_severity": max_sev,
        "decision": decision,
        "failed_rules": sorted({r["rule_id"] for r in failed}),
        "failed_critical_rules": sorted({r["rule_id"] for r in failed if r["severity"] == "critical"}),
        "failed_warning_rules": sorted({r["rule_id"] for r in failed if r["severity"] == "warning"}),
        "failed_info_rules": sorted({r["rule_id"] for r in failed if r["severity"] == "info"}),
    }


def _log_records(gate: str, records: list[dict], summary: dict) -> None:
    log.info("=== GX validation gate: %s ===", gate)
    for r in sorted(records, key=lambda x: (x["dataset"], x["rule_id"] or "")):
        state = "PASS" if r["success"] else "FAIL"
        unexpected = ("" if r["unexpected_count"] is None
                      else f" unexpected={r['unexpected_count']} ({r['unexpected_percent']}%)")
        observed = "" if r["observed_value"] is None else f" observed={str(r['observed_value'])[:80]}"
        err = f" ERROR={r['error']}" if r["error"] else ""
        log.info("[%s] %-6s %-8s %-38s %s%s%s%s", state, r["rule_id"], r["severity"],
                 r["expectation_type"], r["parameters"].get("column", r["dataset"]), unexpected, observed, err)
    log.info("Gate %s -> max failed severity=%s | decision=%s | failed rules=%s",
             gate, summary["max_failed_severity"], summary["decision"], summary["failed_rules"] or "none")


# ----------------------------------------------------------------------------
# public API
# ----------------------------------------------------------------------------
def run_gate(*, gate: str, layer: str, tables: dict[str, pd.DataFrame], run_dir: Path,
             run_id: str, batch_key: str | None = None, extra_ctx: dict | None = None) -> dict:
    """Validate one or more datasets, persist the evidence and apply the policy.

    Raises ``DataQualityGateError`` when the decision is STOP, which Airflow turns into a
    failed task *without retries* (a deterministic data problem is not transient).
    """
    ctx = {"current_year": datetime.now(timezone.utc).year, "tables": tables}
    ctx.update(extra_ctx or {})
    grouped = collect_specs(layer, list(tables))

    context = _new_context()
    records: list[dict] = []
    suites, checkpoints = [], []
    for dataset, df in tables.items():
        specs = grouped.get(dataset, [])
        if not specs:
            continue
        checkpoint, suite = build_checkpoint(context, layer, dataset, specs, ctx)
        suites.append(suite.name)
        checkpoints.append(checkpoint.name)
        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": df})
        for validation_result in checkpoint_result.run_results.values():
            records.extend(_records_from_result(validation_result, dataset, layer))

    summary = apply_policy(records)
    payload = {
        "gate": gate, "layer": layer, "run_id": run_id, "batch_key": batch_key,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows_evaluated": {name: int(len(df)) for name, df in tables.items()},
        "expectation_suites": suites, "checkpoints": checkpoints,
        "statistics": {
            "evaluated_expectations": len(records),
            "successful_expectations": sum(r["success"] for r in records),
            "unsuccessful_expectations": sum(not r["success"] for r in records),
        },
        "overall_success": all(r["success"] for r in records),
        **summary,
        "results": sorted(records, key=lambda x: (x["dataset"], x["rule_id"] or "")),
    }
    out_dir = run_dir / "validation"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{gate}.json"
    out_path.write_text(json.dumps(_jsonable(payload), indent=2), encoding="utf-8")
    payload["result_path"] = str(out_path)

    _log_records(gate, records, summary)
    log.info("Validation evidence stored in %s", out_path)

    if summary["decision"] == "STOP":
        raise DataQualityGateError(
            f"Validation gate '{gate}' failed with CRITICAL severity "
            f"(rules {summary['failed_critical_rules']}). Downstream tasks are blocked. "
            f"Evidence: {out_path}"
        )
    if summary["decision"] == "CONTINUE_WITH_WARNING":
        log.warning("Gate '%s' has WARNING-level failures %s; documented policy allows continuing.",
                    gate, summary["failed_warning_rules"])
    return payload


def export_suites(target_dir: Path) -> list[Path]:
    """Dump every Expectation Suite as JSON (documentation / GitHub evidence)."""
    target_dir.mkdir(parents=True, exist_ok=True)
    context = _new_context()
    written = []
    for layer in ("raw", "prepared"):
        for dataset, specs in collect_specs(layer).items():
            ctx = {"current_year": datetime.now(timezone.utc).year,
                   "tables": {}, "expected_fact_rows": 0}
            # value sets that depend on runtime tables are replaced by a placeholder
            safe_specs = []
            for rule, cls_name, kw in specs:
                placeholders = {"value_set": ["<resolved at run time>"], "value": 0}
                kw2 = {k: (placeholders[k] if callable(v) and k in placeholders else v) for k, v in kw.items()}
                safe_specs.append((rule, cls_name, kw2))
            _, suite = build_checkpoint(context, layer, dataset, safe_specs, ctx)
            path = target_dir / f"{suite.name}.json"
            path.write_text(json.dumps(_jsonable(suite.to_json_dict()), indent=2), encoding="utf-8")
            written.append(path)
    return written
