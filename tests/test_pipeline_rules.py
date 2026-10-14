"""Fast unit tests for the pipeline rules (no Docker, no database needed).

Run:  python -m pytest -q
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import transform as T                       # noqa: E402
from src.errors import DataQualityGateError, SourceContractError   # noqa: E402
from src.extract import extract_spotify              # noqa: E402
from src.validation import apply_policy, run_gate    # noqa: E402
from src import config                               # noqa: E402


# ------------------------------------------------------------------ harmonisation
@pytest.mark.parametrize("raw,expected", [
    ("Smooth - Radio Edit", "smooth"),
    ("Hello (Remastered 2011)", "hello"),
    ("Crazy In Love - Remastered 2011", "crazy in love"),
    ("Don't Stop Believin'", "dont stop believin"),
    ("Rock & Roll", "rock and roll"),
])
def test_norm_title(raw, expected):
    assert T.norm_title(raw) == expected


def test_norm_text_handles_accents_and_missing():
    assert T.norm_text("Beyoncé") == "beyonce"
    assert T.norm_text(None) is None


def test_clean_label_collapses_whitespace_without_changing_content():
    assert T.clean_label("  Best   Pop  Album ") == "Best Pop Album"
    assert T.clean_label("   ") is None


def test_canonical_categories_merges_case_variants():
    s = pd.Series(["Best Rap Album", "Best Rap Album", "Best rap album"])
    assert set(T._canonical_categories(s)) == {"Best Rap Album"}


@pytest.mark.parametrize("category,family", [
    ("Record Of The Year", "General Field"),
    ("Best Rap/Sung Collaboration", "Rap"),
    ("Best Country Song", "Country/Bluegrass"),
    ("Best Latin Pop Album", "Latin"),
    ("Best Pop Solo Performance", "Pop"),
    ("Best Opera Recording", "Classical"),
    ("Something Unclassifiable", "Other"),
])
def test_music_family(category, family):
    assert T.classify_music_family(category) == family


@pytest.mark.parametrize("category,kind", [
    ("Best New Artist", "person"),
    ("Producer Of The Year, Non-Classical", "person"),
    ("Album Of The Year", "album"),
    ("Best Rock Album", "album"),
    ("Record Of The Year", "recording_or_work"),
])
def test_nominee_type(category, kind):
    assert T.classify_nominee_type(category) == kind


# ------------------------------------------------------------------ validation policy
def _rec(sev, ok):
    return {"rule_id": "X", "severity": sev, "success": ok}


def test_policy_critical_wins_over_warning():
    assert apply_policy([_rec("warning", False), _rec("critical", False)])["decision"] == "STOP"


def test_policy_warning_only_continues():
    assert apply_policy([_rec("warning", False), _rec("critical", True)])["decision"] == "CONTINUE_WITH_WARNING"


def test_policy_info_only_continues_silently():
    assert apply_policy([_rec("info", False)])["decision"] == "CONTINUE"


def test_policy_all_pass():
    assert apply_policy([_rec("critical", True)])["max_failed_severity"] == "none"


# ------------------------------------------------------------------ GX gate on tiny frames
def _spotify_frame(n=60000):
    return pd.DataFrame({
        "track_id": [f"t{i}" for i in range(n)], "artists": "A", "album_name": "B", "track_name": "C",
        "popularity": 50, "duration_ms": 200000, "explicit": False, "danceability": 0.5, "energy": 0.5,
        "loudness": -8.0, "acousticness": 0.3, "valence": 0.5, "tempo": 120.0,
        "track_genre": [f"g{i % 114}" for i in range(n)],
    })


def test_gate_passes_clean_batch(tmp_path):
    res = run_gate(gate="spotify_raw", layer="raw", tables={"spotify_raw": _spotify_frame()},
                   run_dir=tmp_path, run_id="unit")
    assert res["decision"] == "CONTINUE"
    assert (tmp_path / "validation" / "spotify_raw.json").exists()


def test_gate_stops_on_critical_range_violation(tmp_path):
    df = _spotify_frame()
    df.loc[:4, "popularity"] = 150
    with pytest.raises(DataQualityGateError):
        run_gate(gate="spotify_raw", layer="raw", tables={"spotify_raw": df}, run_dir=tmp_path, run_id="unit")
    # the evidence is stored BEFORE the exception is raised
    assert (tmp_path / "validation" / "spotify_raw.json").exists()


def test_gate_stops_when_required_column_missing(tmp_path):
    df = _spotify_frame().drop(columns=["popularity"])
    with pytest.raises(DataQualityGateError):
        run_gate(gate="spotify_raw", layer="raw", tables={"spotify_raw": df}, run_dir=tmp_path, run_id="unit")


# ------------------------------------------------------------------ extraction failure classes
def test_missing_source_file_is_deterministic_failure(tmp_path):
    with pytest.raises(SourceContractError):
        extract_spotify("does_not_exist.csv", tmp_path)


def test_path_traversal_rejected(tmp_path):
    with pytest.raises(SourceContractError):
        extract_spotify("../secret.csv", tmp_path)


def test_unsafe_table_name_rejected():
    with pytest.raises(ValueError):
        config.safe_identifier("grammy_awards; DROP TABLE x")


# ------------------------------------------------------------------ retry classification (needs no running DB for the first test)
def test_database_down_is_transient_not_a_fail_exception(tmp_path, monkeypatch):
    """Unreachable DB -> OperationalError must propagate unchanged so Airflow RETRIES the task."""
    from sqlalchemy.exc import OperationalError
    from src.extract import extract_grammys
    monkeypatch.setattr(config, "DB_PORT", "59999")          # nothing listens here
    with pytest.raises(OperationalError):
        extract_grammys("grammy_awards", tmp_path)


def test_missing_table_is_deterministic_fail(tmp_path):
    """Existing server but missing table -> SourceContractError (AirflowFailException): NO retry."""
    import psycopg2
    from src.extract import extract_grammys
    try:
        psycopg2.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER,
                         password=config.DB_PASSWORD, dbname=config.SOURCE_DB_NAME, connect_timeout=3).close()
    except psycopg2.OperationalError:
        pytest.skip("source database not reachable from this environment")
    with pytest.raises(SourceContractError):
        extract_grammys("no_such_table", tmp_path)
