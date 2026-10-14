"""Failure classes used by the pipeline.

Deterministic data / contract failures must NOT be retried: repeating the same
task on the same input reproduces the same result. In Airflow, raising
``AirflowFailException`` marks the task as failed immediately, even when the
task is configured with ``retries > 0``.

Outside Airflow (unit tests, notebooks) a plain exception with the same name is
used so the modules remain importable without the Airflow package.
"""
try:  # Airflow 3.x runtime
    from airflow.exceptions import AirflowFailException  # type: ignore
except ImportError:  # pragma: no cover - local tooling without Airflow
    class AirflowFailException(Exception):  # type: ignore
        """Stand-in used when Airflow is not installed."""


class DataQualityGateError(AirflowFailException):
    """A Critical validation rule failed: the downstream path must stop."""


class SourceContractError(AirflowFailException):
    """The source does not satisfy the minimum technical contract (e.g. file missing)."""


class LoadContractError(AirflowFailException):
    """The warehouse rejected the data (constraint / type violation): deterministic, do not retry."""
