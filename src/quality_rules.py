"""Quality-rule registry: the single source of truth for validation.

Every ``Rule`` documents the full reasoning chain required by the workshop

    requirement -> profiling evidence -> quality dimension -> rule ->
    metric / threshold / severity -> GX Expectation(s) -> pipeline action

``src/validation.py`` turns the registry into Great Expectations Suites and
``scripts/export_quality_rules.py`` renders it as ``docs/quality_rules.md``, so the
documentation can never drift away from the executed checks.

Thresholds are engineering decisions justified in ``justification``; none of them
was chosen merely because the current batch passes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from . import schemas as S

# ``kwargs`` values may be callables ``f(ctx)`` resolved at run time (e.g. foreign-key
# value sets taken from the prepared dimensions, or the current year).
Spec = tuple[str, dict[str, Any]]


@dataclass(frozen=True)
class Rule:
    rule_id: str
    layer: str            # raw | prepared
    dataset: str          # name of the validated data object
    attribute: str
    dimension: str        # completeness | validity | uniqueness | consistency | volume | ...
    statement: str        # what must be true
    metric: str
    threshold: str
    severity: str         # critical | warning | info
    requirements: str     # analytical requirements protected (AR1, AR2, AR3)
    justification: str    # profiling evidence / contract / invariant
    action: str           # pipeline response when the rule fails
    specs: tuple[Spec, ...]


STOP = "STOP the affected path (task fails, no retry)"
CONTINUE_WARN = "CONTINUE with warning (logged + stored in the validation result)"
RECORD = "RECORD only (informational, never blocks)"

_current_year: Callable[[dict], int] = lambda ctx: ctx["current_year"]

# ======================================================================
# RAW - SPOTIFY  (dataset: spotify_raw)
# ======================================================================
SPOTIFY_RAW_RULES: list[Rule] = [
    Rule("SP-01", "raw", "spotify_raw", "table schema", "Validity (schema)",
         "All attributes needed by AR1-AR3 must exist in the incoming file.",
         "required columns present", "all 14 required columns", "critical", "AR1, AR2, AR3",
         "Source contract: without popularity, audio features, genre or the track keys no requirement can be answered.",
         STOP,
         (("ExpectTableColumnsToMatchSet",
           {"column_set": S.SPOTIFY_REQUIRED_COLUMNS, "exact_match": False}),)),
    Rule("SP-02", "raw", "spotify_raw", "table volume", "Completeness (volume)",
         "The batch must contain a plausible number of rows.",
         "row count", "50,000 <= rows <= 500,000", "critical", "AR1, AR2, AR3",
         "Profiling: 114,000 rows = 114 genres x ~1,000 tracks. Fewer than half of the expected rows indicates a truncated "
         "extraction and would bias every genre baseline; more than 4x indicates a duplicated export.",
         STOP,
         (("ExpectTableRowCountToBeBetween", {"min_value": 50_000, "max_value": 500_000}),)),
    Rule("SP-03", "raw", "spotify_raw", "track_id", "Completeness",
         "track_id (business key) must never be missing.",
         "% non-null track_id", "100%", "critical", "AR1, AR2, AR3",
         "Profiling: 0 missing. track_id is the key used to collapse the track x genre grain and to build dim_track.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "track_id"}),)),
    Rule("SP-04", "raw", "spotify_raw", "track_id + track_genre", "Uniqueness",
         "The pair (track_id, track_genre) identifies one source row.",
         "% rows in a unique pair", ">= 99%", "warning", "AR3",
         "Profiling: track_id repeats across genres by design (24,259 repeated ids), but 450 exact (track_id, genre) pairs "
         "are repeated (894 rows involved, 0.78%). They do not distort results because transformation de-duplicates them; "
         ">1% would indicate a faulty export.",
         CONTINUE_WARN,
         (("ExpectCompoundColumnsToBeUnique",
           {"column_list": ["track_id", "track_genre"], "mostly": 0.99, "ignore_row_if": "never"}),)),
    Rule("SP-05", "raw", "spotify_raw", "popularity", "Validity",
         "popularity must be an integer score between 0 and 100.",
         "% values in [0, 100]", "100%", "critical", "AR1",
         "Documented Spotify scale (0-100). AR1 averages popularity; one out-of-scale value corrupts the KPI and cannot be repaired automatically.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "popularity"}),
          ("ExpectColumnValuesToBeBetween", {"column": "popularity", "min_value": 0, "max_value": 100}))),
    Rule("SP-06", "raw", "spotify_raw", "danceability, energy, valence", "Validity",
         "Audio features that are normalised scores must lie between 0 and 1.",
         "% values in [0, 1]", "100%", "critical", "AR2",
         "Documented Spotify scale (0-1); profiling: min 0, max 1.0. AR2 compares their averages across decades.",
         STOP,
         (("ExpectColumnValuesToBeBetween", {"column": "danceability", "min_value": 0, "max_value": 1}),
          ("ExpectColumnValuesToBeBetween", {"column": "energy", "min_value": 0, "max_value": 1}),
          ("ExpectColumnValuesToBeBetween", {"column": "valence", "min_value": 0, "max_value": 1}))),
    Rule("SP-07", "raw", "spotify_raw", "loudness", "Validity",
         "loudness (dB) must be within the physically plausible range.",
         "% values in [-60, 5]", ">= 99.9%", "warning", "AR2",
         "Domain knowledge: loudness is reported in dB, typically -60..0, slightly positive for heavily mastered tracks. "
         "Profiling: min -49.5, max +4.5. A tiny tolerance avoids stopping for isolated sensor artefacts.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToBeBetween",
           {"column": "loudness", "min_value": -60, "max_value": 5, "mostly": 0.999}),)),
    Rule("SP-08", "raw", "spotify_raw", "track_name, artists", "Completeness",
         "Integration attributes (title and artists) should be present.",
         "% non-null", ">= 99.9%", "warning", "AR1, AR2, AR3",
         "Profiling: exactly 1 of 114,000 rows has no track_name/artists/album_name. Such a row cannot be matched to a Grammy "
         "nomination; transformation drops it and logs the count.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToNotBeNull", {"column": "track_name", "mostly": 0.999}),
          ("ExpectColumnValuesToNotBeNull", {"column": "artists", "mostly": 0.999}))),
    Rule("SP-09", "raw", "spotify_raw", "track_genre", "Completeness",
         "Every track must carry a genre.",
         "% non-null track_genre", "100%", "critical", "AR1, AR3",
         "Profiling: 0 missing, 114 distinct genres. Genre is the grouping attribute of the AR3 KPIs and of the AR1 baseline.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "track_genre"}),)),
    Rule("SP-10", "raw", "spotify_raw", "tempo, duration_ms", "Validity",
         "Tempo and duration must be strictly positive.",
         "% values > 0", "tempo >= 99.5%; duration >= 99.9%", "warning", "AR2",
         "Profiling: 157 rows (0.14%) have tempo = 0 AND danceability = speechiness = time_signature = 0, i.e. one failure pattern of "
         "Spotify's audio analysis; 1 row has duration = 0 (the same record that has no name). A 0 BPM / 0 ms track is impossible, so "
         "transformation sets the affected measures to NULL (not imputed) instead of stopping.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToBeBetween",
           {"column": "tempo", "min_value": 0, "strict_min": True, "mostly": 0.995}),
          ("ExpectColumnValuesToBeBetween",
           {"column": "duration_ms", "min_value": 0, "strict_min": True, "mostly": 0.999}))),
    Rule("SP-11", "raw", "spotify_raw", "explicit", "Validity",
         "explicit must be a boolean flag.",
         "% values in {True, False}", "100%", "warning", "AR3",
         "Profiling: only True/False observed (9,747 True). Used as a dim_track attribute only.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToBeInSet", {"column": "explicit", "value_set": [True, False]}),)),
    Rule("SP-12", "raw", "spotify_raw", "track_genre", "Consistency (drift)",
         "The number of distinct genres should stay close to the known catalogue (114).",
         "distinct genres", "100 - 130", "info", "AR3",
         "Monitoring signal: a change in the number of genres suggests a source redesign that would change the genre baseline.",
         RECORD,
         (("ExpectColumnUniqueValueCountToBeBetween",
           {"column": "track_genre", "min_value": 100, "max_value": 130}),)),
]

# ======================================================================
# RAW - GRAMMYS  (dataset: grammys_raw)
# ======================================================================
GRAMMY_RAW_RULES: list[Rule] = [
    Rule("GR-01", "raw", "grammys_raw", "table schema", "Validity (schema)",
         "The Grammy source table must expose the attributes used by AR1-AR3.",
         "required columns present", "all 6 required columns", "critical", "AR1, AR2, AR3",
         "Source contract of public.grammy_awards: year, category, nominee, artist, winner, title.",
         STOP,
         (("ExpectTableColumnsToMatchSet",
           {"column_set": S.GRAMMY_REQUIRED_COLUMNS, "exact_match": False}),)),
    Rule("GR-02", "raw", "grammys_raw", "table volume", "Completeness (volume)",
         "The extracted table must contain a plausible number of rows.",
         "row count", "1,000 <= rows <= 100,000", "critical", "AR1, AR2, AR3",
         "Profiling: 4,810 rows over 62 ceremonies (~80 categories each). Below 1,000 rows the extraction is truncated.",
         STOP,
         (("ExpectTableRowCountToBeBetween", {"min_value": 1_000, "max_value": 100_000}),)),
    Rule("GR-03", "raw", "grammys_raw", "year", "Validity (temporal)",
         "year must exist and be between the first Grammy edition (1958) and the current year.",
         "% non-null and in range", "100%", "critical", "AR1, AR2",
         "Profiling: 0 missing, range 1958-2019. Year drives the decade dimension; a future or missing year breaks the temporal analysis.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "year"}),
          ("ExpectColumnValuesToBeBetween",
           {"column": "year", "min_value": 1958, "max_value": _current_year}))),
    Rule("GR-04", "raw", "grammys_raw", "category", "Completeness",
         "category must never be missing.",
         "% non-null category", "100%", "critical", "AR1, AR2, AR3",
         "Profiling: 0 missing, 638 raw spellings. Category defines dim_category and the music family used by AR3.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "category"}),)),
    Rule("GR-05", "raw", "grammys_raw", "nominee", "Completeness",
         "nominee (work title) should be present.",
         "% non-null nominee", ">= 99%", "warning", "AR1, AR2, AR3",
         "Profiling: 6 missing (0.12%): 4 'Remixer of the Year' and 2 'Best New Country & Western Artist' rows. A record without "
         "nominee cannot be identified or matched; transformation drops and logs them.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToNotBeNull", {"column": "nominee", "mostly": 0.99}),)),
    Rule("GR-06", "raw", "grammys_raw", "artist", "Completeness",
         "artist must be present in enough rows to make title+artist matching meaningful.",
         "% non-null artist", ">= 50%", "warning", "AR1, AR2, AR3",
         "Profiling: 38.3% missing, concentrated in categories that honour songwriters, composers, producers or technical credits "
         "(Song Of The Year 70/70, Opera Recording, Album Notes, Instrumental Composition, Country Song...). Missingness is therefore "
         "structural, but below 50% the integration pool would be unrepresentative. Transformation recovers part of it from other rows "
         "of the same year and title (matching only).",
         CONTINUE_WARN,
         (("ExpectColumnValuesToNotBeNull", {"column": "artist", "mostly": 0.50}),)),
    Rule("GR-07", "raw", "grammys_raw", "winner", "Validity",
         "winner must be a non-null boolean.",
         "% values in {True, False}", "100%", "critical", "AR1, AR2, AR3",
         "Source contract: boolean flag. Profiling: 0 missing.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "winner"}),
          ("ExpectColumnValuesToBeInSet", {"column": "winner", "value_set": [True, False]}))),
    Rule("GR-08", "raw", "grammys_raw", "winner", "Consistency",
         "The winner flag should discriminate winners from nominees (both values present).",
         "distinct values contain {True, False}", "both values", "warning", "AR1, AR2, AR3",
         "Profiling: winner = True in 4,810 / 4,810 rows, yet 2019 lists ~5 rows per category (all nominees) while earlier years list "
         "~1 (winners only). The flag is not reliable, therefore NO requirement uses it; rows are treated as 'Grammy recognitions'. "
         "This rule is EXPECTED to fail on the current batch and documents the limitation.",
         CONTINUE_WARN,
         (("ExpectColumnDistinctValuesToContainSet", {"column": "winner", "value_set": [True, False]}),)),
    Rule("GR-09", "raw", "grammys_raw", "year + category + nominee + artist", "Uniqueness",
         "One source row per (year, category, nominee, artist).",
         "% rows in a unique combination", "100%", "critical", "AR1, AR2, AR3",
         "Profiling: 0 duplicated combinations. A duplicated record would create two fact rows for one nomination (grain violation).",
         STOP,
         (("ExpectCompoundColumnsToBeUnique",
           {"column_list": ["year", "category", "nominee", "artist"], "ignore_row_if": "never"}),)),
    Rule("GR-10", "raw", "grammys_raw", "year", "Completeness (temporal coverage)",
         "The batch must cover (almost) every Grammy edition.",
         "distinct years", ">= 60", "warning", "AR1, AR2",
         "Profiling: 62 distinct years (1958-2019). Gaps would bias decade averages. The dataset is a static historical snapshot, so "
         "freshness does not apply; coverage is the temporal check.",
         CONTINUE_WARN,
         (("ExpectColumnUniqueValueCountToBeBetween", {"column": "year", "min_value": 60}),)),
]

# ======================================================================
# PREPARED  (datasets: one per prepared table)
# ======================================================================
def _keys(table: str, col: str) -> Callable[[dict], list]:
    return lambda ctx: sorted(ctx["tables"][table][col].dropna().unique().tolist())


_MATCHED = {"row_condition": "spotify_matched==1", "condition_parser": "pandas"}
_UNMATCHED = {"row_condition": "spotify_matched==0", "condition_parser": "pandas"}

PREPARED_RULES: list[Rule] = [
    Rule("PR-01", "prepared", "all prepared tables", "table schema", "Validity (schema)",
         "Every prepared table must have exactly the columns of the dimensional model.",
         "column set equals model", "exact", "critical", "AR1, AR2, AR3",
         "Invariant: the load writes these columns into dw.* tables; a missing/extra column would break or silently shift the load.",
         STOP,
         tuple(("ExpectTableColumnsToMatchSet", {"column_set": cols, "exact_match": True, "__dataset__": t})
               for t, cols in S.PREPARED_COLUMNS.items())),
    Rule("PR-02", "prepared", "dimensions", "surrogate / business keys", "Uniqueness",
         "Dimension surrogate keys and business keys are non-null and unique.",
         "% unique non-null keys", "100%", "critical", "AR1, AR2, AR3",
         "Invariant of the star schema: a duplicated key would multiply fact rows in every join.",
         STOP,
         (("ExpectColumnValuesToBeUnique", {"column": "year_key", "__dataset__": "dim_year"}),
          ("ExpectColumnValuesToNotBeNull", {"column": "year_key", "__dataset__": "dim_year"}),
          ("ExpectColumnValuesToBeUnique", {"column": "category_key", "__dataset__": "dim_category"}),
          ("ExpectColumnValuesToBeUnique", {"column": "category_name", "__dataset__": "dim_category"}),
          ("ExpectColumnValuesToBeUnique", {"column": "genre_key", "__dataset__": "dim_genre"}),
          ("ExpectColumnValuesToBeUnique", {"column": "genre_name", "__dataset__": "dim_genre"}),
          ("ExpectColumnValuesToBeUnique", {"column": "track_key", "__dataset__": "dim_track"}),
          ("ExpectColumnValuesToBeUnique", {"column": "spotify_track_id", "__dataset__": "dim_track"}),
          ("ExpectColumnValuesToNotBeNull", {"column": "spotify_track_id", "__dataset__": "dim_track"}))),
    Rule("PR-03", "prepared", "fact_grammy_nomination", "nomination_bk, nomination_key", "Uniqueness (grain)",
         "One fact row per Grammy nomination record: business key and surrogate key are unique.",
         "% unique keys", "100%", "critical", "AR1, AR2, AR3",
         "Declared grain of the fact table. Integration must not fan-out or duplicate nominations.",
         STOP,
         (("ExpectColumnValuesToNotBeNull", {"column": "nomination_bk"}),
          ("ExpectColumnValuesToBeUnique", {"column": "nomination_bk"}),
          ("ExpectColumnValuesToBeUnique", {"column": "nomination_key"}))),
    Rule("PR-04", "prepared", "facts and dimensions", "foreign keys", "Consistency (referential integrity)",
         "Every foreign key must exist in its dimension.",
         "% FK values found in the dimension", "100%", "critical", "AR1, AR2, AR3",
         "Invariant: the DW enforces these FKs; an orphan key would make the load fail half-way or hide rows in joins.",
         STOP,
         (("ExpectColumnValuesToBeInSet",
           {"column": "year_key", "value_set": _keys("dim_year", "year_key"), "__dataset__": "fact_grammy_nomination"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "category_key", "value_set": _keys("dim_category", "category_key"), "__dataset__": "fact_grammy_nomination"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "track_key", "value_set": _keys("dim_track", "track_key"), "__dataset__": "fact_grammy_nomination"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "genre_key", "value_set": _keys("dim_genre", "genre_key"), "__dataset__": "dim_track"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "genre_key", "value_set": _keys("dim_genre", "genre_key"), "__dataset__": "fact_catalog_genre_baseline"}))),
    Rule("PR-05", "prepared", "fact_grammy_nomination", "spotify_matched, track_key", "Consistency (integration)",
         "A nomination is flagged matched iff it points to a real dim_track member; unmatched ones point to the unknown member (-1).",
         "% rows consistent", "100%", "critical", "AR1, AR2, AR3",
         "Integration contract: matched rows must carry Spotify measures; unmatched rows must not.",
         STOP,
         (("ExpectColumnValuesToBeInSet", {"column": "spotify_matched", "value_set": [0, 1]}),
          ("ExpectColumnValuesToBeBetween", {"column": "track_key", "min_value": 1, **_MATCHED}),
          ("ExpectColumnValuesToBeInSet", {"column": "track_key", "value_set": [S.UNKNOWN_KEY], **_UNMATCHED}),
          ("ExpectColumnValuesToNotBeNull", {"column": "popularity", **_MATCHED}))),
    Rule("PR-06", "prepared", "fact_grammy_nomination", "popularity, audio features", "Validity",
         "Measures of matched nominations remain inside their documented scales after transformation.",
         "% matched rows in range", "100%", "critical", "AR1, AR2",
         "Post-transformation invariant: transformation never rescales values, so any violation means a defect was propagated.",
         STOP,
         (("ExpectColumnValuesToBeBetween", {"column": "popularity", "min_value": 0, "max_value": 100, **_MATCHED}),
          ("ExpectColumnValuesToBeBetween", {"column": "danceability", "min_value": 0, "max_value": 1, **_MATCHED}),
          ("ExpectColumnValuesToBeBetween", {"column": "energy", "min_value": 0, "max_value": 1, **_MATCHED}),
          ("ExpectColumnValuesToBeBetween", {"column": "valence", "min_value": 0, "max_value": 1, **_MATCHED}),
          ("ExpectColumnValuesToBeBetween", {"column": "acousticness", "min_value": 0, "max_value": 1, **_MATCHED}))),
    Rule("PR-07", "prepared", "fact_grammy_nomination", "loudness", "Validity",
         "Loudness of matched nominations stays in the plausible dB range.",
         "% matched rows in [-60, 5]", "100%", "warning", "AR2",
         "Same domain range as SP-07; warning because SP-07 itself tolerates 0.1% outliers upstream.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToBeBetween", {"column": "loudness", "min_value": -60, "max_value": 5, **_MATCHED}),)),
    Rule("PR-08", "prepared", "fact_grammy_nomination", "row count", "Consistency (reconciliation)",
         "The fact table has exactly one row per Grammy record that survived transformation (no fan-out, no loss).",
         "rows == expected rows", "exact", "critical", "AR1, AR2, AR3",
         "Integration invariant: rows_in(Grammy) - rows_dropped = rows(fact). Protects the one-to-at-most-one cardinality of the join.",
         STOP,
         (("ExpectTableRowCountToEqual",
           {"value": lambda ctx: ctx["expected_fact_rows"], "__dataset__": "fact_grammy_nomination"}),)),
    Rule("PR-09", "prepared", "fact_grammy_nomination", "match_candidates", "Consistency (duplicate matches)",
         "Most matched nominations should be unambiguous (one candidate Spotify track).",
         "% matched rows with 1 candidate", ">= 50%", "warning", "AR1, AR2",
         "Several Spotify tracks can share title+artist (remasters, compilations). The deterministic tie-break (highest popularity) is "
         "documented; if more than half of the matches were ambiguous, that rule would dominate the KPIs.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToBeBetween",
           {"column": "match_candidates", "min_value": 1, "max_value": 1, "mostly": 0.50, **_MATCHED}),)),
    Rule("PR-10", "prepared", "fact_grammy_nomination", "spotify_matched", "Completeness (analytical readiness)",
         "Enough nominations must be matched to compute decade-level KPIs.",
         "sum(spotify_matched)", ">= 100", "critical", "AR1, AR2, AR3",
         "Seven decades x at least ~15 observations per decade. Fewer than 100 matches means the integration (normalisation / artist "
         "matching) is broken and the dashboards would be empty or meaningless.",
         STOP,
         (("ExpectColumnSumToBeBetween", {"column": "spotify_matched", "min_value": 100}),)),
    Rule("PR-11", "prepared", "dim_category / dim_year / fact", "derived domains", "Validity (domain)",
         "Derived classifications use only their controlled vocabularies.",
         "% values in domain", "100%", "critical", "AR3",
         "Transformation derives music_family, nominee_type, coverage_type and match_status with fixed rules; any other value is a bug.",
         STOP,
         (("ExpectColumnValuesToBeInSet",
           {"column": "music_family", "value_set": S.MUSIC_FAMILIES, "__dataset__": "dim_category"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "nominee_type", "value_set": S.NOMINEE_TYPES, "__dataset__": "dim_category"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "coverage_type", "value_set": S.COVERAGE_TYPES, "__dataset__": "dim_year"}),
          ("ExpectColumnValuesToBeInSet",
           {"column": "match_status", "value_set": S.MATCH_STATUSES, "__dataset__": "fact_grammy_nomination"}))),
    Rule("PR-12", "prepared", "dim_category", "music_family", "Consistency (classification quality)",
         "The residual 'Other' family should stay a minority of categories.",
         "% categories not 'Other'", ">= 75%", "warning", "AR3",
         "The keyword classifier of categories is heuristic. A high share of 'Other' would make the AR3 family breakdown uninformative.",
         CONTINUE_WARN,
         (("ExpectColumnValuesToNotBeInSet",
           {"column": "music_family", "value_set": ["Other"], "mostly": 0.75, "__dataset__": "dim_category"}),)),
    Rule("PR-13", "prepared", "fact_catalog_genre_baseline", "baseline measures", "Validity",
         "The genre baseline must be computable and inside the popularity scale.",
         "track_count >= 1 and 0 <= avg_popularity <= 100", "100%", "critical", "AR1",
         "AR1 compares Grammy tracks with their genre baseline; an empty or out-of-scale baseline would invalidate the KPI.",
         STOP,
         (("ExpectColumnValuesToBeBetween",
           {"column": "track_count", "min_value": 1, "__dataset__": "fact_catalog_genre_baseline"}),
          ("ExpectColumnValuesToBeBetween",
           {"column": "avg_popularity", "min_value": 0, "max_value": 100, "__dataset__": "fact_catalog_genre_baseline"}))),
    Rule("PR-14", "prepared", "dim_year", "year_key", "Validity (temporal)",
         "Prepared years stay within the Grammy history and the decade is derived correctly.",
         "% year_key in [1958, current year]", "100%", "critical", "AR1, AR2",
         "Post-transformation invariant of the time dimension.",
         STOP,
         (("ExpectColumnValuesToBeBetween",
           {"column": "year_key", "min_value": 1958, "max_value": _current_year, "__dataset__": "dim_year"}),)),
]

ALL_RULES: list[Rule] = SPOTIFY_RAW_RULES + GRAMMY_RAW_RULES + PREPARED_RULES
RULES_BY_ID: dict[str, Rule] = {r.rule_id: r for r in ALL_RULES}
RAW_DATASETS = {"spotify_raw": SPOTIFY_RAW_RULES, "grammys_raw": GRAMMY_RAW_RULES}
