"""TRANSFORM & INTEGRATE stage.

One engineering phase that cleans, standardises, harmonises, integrates and derives.
Every rule below is justified by profiling evidence or by a requirement, *not* by the
outcome of a validation (see docs/transformation_decisions.md). Invalid *values* such
as popularity = 150 are never "repaired" here: they are stopped upstream by the raw gate.

Output: the six prepared tables, already shaped like the dimensional model, written as
CSV files in ``<run_dir>/prepared`` plus a reconciliation report.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from . import schemas as S

log = logging.getLogger("airflow.task")

# ----------------------------------------------------------------------------
# text normalisation (shared by both sources -> harmonisation for integration)
# ----------------------------------------------------------------------------
_VERSION_SUFFIX = re.compile(
    r"\s-\s.*?(remaster|version|mix|edit|mono|stereo|live|single|bonus|deluxe|radio|acoustic|\b(19|20)\d\d\b).*$"
)


def _ascii_lower(value) -> str | None:
    if value is None or (isinstance(value, float) and np.isnan(value)) or pd.isna(value):
        return None
    text = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode().lower()
    return text


def norm_text(value) -> str | None:
    """Lower-case, ASCII-fold, '&' -> 'and', keep only [a-z0-9 ]."""
    text = _ascii_lower(value)
    if text is None:
        return None
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9 ]", "", text)
    return re.sub(r"\s+", " ", text).strip() or None


def norm_title(value) -> str | None:
    """Title key: also drops '(...)', '[...]' and ' - Remastered 2011'-style version suffixes."""
    text = _ascii_lower(value)
    if text is None:
        return None
    text = _VERSION_SUFFIX.sub("", text)
    text = re.sub(r"\(.*?\)|\[.*?\]", " ", text)
    return norm_text(text)


def clean_label(value) -> str | None:
    """Trim and collapse repeated whitespace; blank -> missing. Content is not altered."""
    if value is None or pd.isna(value):
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


# ----------------------------------------------------------------------------
# Grammy category harmonisation / derivation rules
# ----------------------------------------------------------------------------
_GENERAL_FIELD = {"record of the year", "album of the year", "song of the year", "best new artist"}
_PERSON_RE = re.compile(r"new artist|producer of the year|remixer of the year|songwriter of the year|classical producer")

_FAMILY_RULES: list[tuple[str, re.Pattern]] = [
    ("Music Video", re.compile(r"video|music film")),
    ("Children/Spoken/Comedy", re.compile(r"children|spoken|comedy|poetry|narration|storytelling")),
    ("Musical Theater/Soundtrack", re.compile(r"musical show|broadway|soundtrack|motion picture|original cast|visual media|musical theat|original score|cast show")),
    ("Classical", re.compile(r"classical|opera|orchestral|chamber|choral|symphon|instrumental soloist|small ensemble|conductor|contemporary composition")),
    ("Rap", re.compile(r"\brap\b|hip.hop")),
    ("R&B/Soul", re.compile(r"r&b|rhythm|\bsoul\b|urban")),
    ("Country/Bluegrass", re.compile(r"country|bluegrass")),
    ("Gospel/Christian", re.compile(r"gospel|christian|inspirational")),
    ("Latin", re.compile(r"latin|tejano|mariachi|tropical|banda|norte|mexican|salsa|merengue")),
    ("Reggae/World", re.compile(r"reggae|world music|polka|hawaiian|native american|zydeco|cajun|calypso|ethnic|traditional folk")),
    ("Folk/Blues/Americana", re.compile(r"folk|blues|americana|american roots")),
    ("Dance/Electronic", re.compile(r"dance|electronic|techno|disco")),
    ("Jazz", re.compile(r"jazz|big band")),
    ("Rock/Metal/Alternative", re.compile(r"\brock\b|metal|alternative|new wave|punk|grunge|r&r|new age")),
    ("Pop", re.compile(r"\bpop\b|vocal performance|vocal duo|vocal group|duo or group|solo performance|dance/pop")),
    ("Production/Technical", re.compile(r"producer|engineer|remix|package|notes|arrangement|art direction|historical|surround|recording for|album package|album cover|immersive audio|boxed")),
]


def classify_nominee_type(category: str) -> str:
    low = category.lower()
    if _PERSON_RE.search(low):
        return "person"
    if "album" in low:
        return "album"
    return "recording_or_work"


def classify_music_family(category: str) -> str:
    low = category.lower()
    if low in _GENERAL_FIELD:
        return "General Field"
    for family, pattern in _FAMILY_RULES:
        if pattern.search(low):
            return family
    return "Other"


# ----------------------------------------------------------------------------
# main entry point
# ----------------------------------------------------------------------------
def _canonical_categories(series: pd.Series) -> pd.Series:
    """Case variants of the same category -> the most frequent spelling (ties: alphabetical)."""
    frame = pd.DataFrame({"low": series.str.lower(), "spelling": series})
    counts = frame.groupby(["low", "spelling"]).size().reset_index(name="n")
    counts = counts.sort_values(["low", "n", "spelling"], ascending=[True, False, True])
    canon = counts.drop_duplicates("low").set_index("low")["spelling"]
    return frame["low"].map(canon)


def _md5(*parts) -> str:
    return hashlib.md5("|".join("" if p is None else str(p) for p in parts).encode("utf-8")).hexdigest()


def transform_and_integrate(spotify_meta: dict, grammys_meta: dict, run_dir: Path) -> dict:
    out_dir = run_dir / "prepared"
    out_dir.mkdir(parents=True, exist_ok=True)
    rec: dict = {"spotify": {}, "grammys": {}, "integration": {}}

    batch_key = hashlib.sha256(
        (spotify_meta["source_sha256"] + grammys_meta["source_sha256"]).encode()).hexdigest()[:16]
    log.info("[transform] batch_key=%s (hash of both source batches)", batch_key)

    # ------------------------------------------------------------------ SPOTIFY
    sp = pd.read_csv(spotify_meta["raw_path"], low_memory=False)
    rec["spotify"]["rows_in"] = len(sp)
    sp = sp.drop(columns=[c for c in sp.columns if c.startswith("Unnamed")], errors="ignore")

    for col in ["track_id", "artists", "album_name", "track_name", "track_genre"]:
        sp[col] = sp[col].map(clean_label)

    missing_id = sp[["track_id", "track_name", "artists"]].isna().any(axis=1)
    rec["spotify"]["dropped_missing_identity"] = int(missing_id.sum())
    sp = sp[~missing_id].copy()

    before = len(sp)
    sp = (sp.sort_values(["track_id", "track_genre", "popularity"], ascending=[True, True, False])
            .drop_duplicates(["track_id", "track_genre"]))
    rec["spotify"]["dropped_duplicate_track_genre_pairs"] = before - len(sp)

    # Profiling: the 157 rows with tempo = 0 ALSO have danceability = speechiness = time_signature = 0 (and valence 0):
    # one failure pattern of Spotify's audio analysis, not 157 independent "real" zeros. The affected measures are
    # unknown, so they become NULL (never imputed). Energy / loudness / acousticness of these rows look valid and stay.
    analysis_failed = (sp["tempo"] <= 0) & (sp["time_signature"] == 0)
    rec["spotify"]["audio_analysis_failed_rows"] = int(analysis_failed.sum())
    sp.loc[analysis_failed, ["tempo", "danceability", "valence"]] = np.nan
    rec["spotify"]["tempo_set_null_other"] = int((sp["tempo"] <= 0).sum())
    sp.loc[sp["tempo"] <= 0, "tempo"] = np.nan
    rec["spotify"]["duration_set_null"] = int((sp["duration_ms"] <= 0).sum())
    sp.loc[sp["duration_ms"] <= 0, "duration_ms"] = np.nan

    genre_count = sp.groupby("track_id")["track_genre"].nunique().rename("genre_count")
    tracks = (sp.sort_values(["track_id", "popularity", "track_genre"], ascending=[True, False, True])
                .drop_duplicates("track_id").merge(genre_count, on="track_id", how="left")
                .reset_index(drop=True))
    tracks = tracks.rename(columns={"track_genre": "primary_genre"})
    rec["spotify"]["track_genre_rows_after_dedup"] = before - rec["spotify"]["dropped_duplicate_track_genre_pairs"]
    rec["spotify"]["tracks_out"] = len(tracks)
    rec["spotify"]["tracks_in_several_genres"] = int((tracks["genre_count"] > 1).sum())

    tracks["title_key"] = tracks["track_name"].map(norm_title)
    tracks["artist_keys"] = tracks["artists"].map(
        lambda s: [k for k in (norm_text(a) for a in s.split(";")) if k])
    title_index: dict[str, list[int]] = {}
    for idx, key in enumerate(tracks["title_key"].tolist()):
        if key:
            title_index.setdefault(key, []).append(idx)
    log.info("[transform] spotify: rows_in=%d -> track-grain=%d (dropped identity=%d, duplicate pairs=%d)",
             rec["spotify"]["rows_in"], len(tracks), rec["spotify"]["dropped_missing_identity"],
             rec["spotify"]["dropped_duplicate_track_genre_pairs"])

    # ------------------------------------------------------------------ GRAMMYS
    gr = pd.read_csv(grammys_meta["raw_path"])
    rec["grammys"]["rows_in"] = len(gr)
    for col in ["title", "category", "nominee", "artist"]:
        gr[col] = gr[col].map(clean_label)

    missing_nominee = gr["nominee"].isna()
    rec["grammys"]["dropped_missing_nominee"] = int(missing_nominee.sum())
    gr = gr[~missing_nominee].copy()

    gr["category"] = _canonical_categories(gr["category"])
    rec["grammys"]["category_spellings_raw"] = int(pd.read_csv(grammys_meta["raw_path"], usecols=["category"])["category"].nunique())
    rec["grammys"]["categories_harmonised"] = int(gr["category"].nunique())
    gr["year"] = gr["year"].astype(int)

    gr["nominee_key"] = gr["nominee"].map(norm_title)
    gr["artist_key"] = gr["artist"].map(norm_text)
    gr["nomination_bk"] = [_md5(y, c.lower(), n, a) for y, c, n, a in
                           zip(gr["year"], gr["category"], gr["nominee"].map(norm_text), gr["artist_key"])]
    dup = gr.duplicated("nomination_bk", keep="first")
    rec["grammys"]["dropped_duplicate_business_key"] = int(dup.sum())
    gr = gr[~dup].copy()

    # artist recovery for MATCHING only: same year + same title with exactly one known artist
    known = (gr.dropna(subset=["artist_key", "nominee_key"]).groupby(["year", "nominee_key"])["artist_key"]
               .agg(lambda s: sorted(set(s))))
    lookup = {k: v[0] for k, v in known.items() if len(v) == 1}
    gr["artist_for_match"] = [a if a else lookup.get((y, n))
                              for a, y, n in zip(gr["artist_key"], gr["year"], gr["nominee_key"])]
    rec["grammys"]["artist_missing_before_recovery"] = int(gr["artist_key"].isna().sum())
    rec["grammys"]["artist_missing_after_recovery"] = int(pd.isna(gr["artist_for_match"]).sum())

    gr["nominee_type"] = gr["category"].map(classify_nominee_type)
    gr["music_family"] = gr["category"].map(classify_music_family)
    gr["decade"] = (gr["year"] // 10) * 10
    rec["grammys"]["rows_out"] = len(gr)
    log.info("[transform] grammys: rows_in=%d -> rows_out=%d (dropped missing nominee=%d, duplicate bk=%d); "
             "category spellings %d -> %d", rec["grammys"]["rows_in"], len(gr),
             rec["grammys"]["dropped_missing_nominee"], rec["grammys"]["dropped_duplicate_business_key"],
             rec["grammys"]["category_spellings_raw"], rec["grammys"]["categories_harmonised"])

    # ------------------------------------------------------------------ INTEGRATION
    statuses, chosen, n_candidates = [], [], []
    for nominee_key, art, ntype in zip(gr["nominee_key"], gr["artist_for_match"], gr["nominee_type"]):
        if ntype != "recording_or_work":
            statuses.append("not_matchable_album_or_person"); chosen.append(None); n_candidates.append(0); continue
        if not art:
            statuses.append("not_matchable_missing_artist"); chosen.append(None); n_candidates.append(0); continue
        pool = title_index.get(nominee_key or "", [])
        padded = f" {art} "
        cands = [i for i in pool if any(f" {k} " in padded for k in tracks.at[i, "artist_keys"])]
        if not cands:
            statuses.append("unmatched_artist_mismatch" if pool else "unmatched_no_title_candidate")
            chosen.append(None); n_candidates.append(0); continue
        top_pop = max(tracks.at[i, "popularity"] for i in cands)
        best = sorted((i for i in cands if tracks.at[i, "popularity"] == top_pop),
                      key=lambda i: tracks.at[i, "track_id"])[0]
        statuses.append("matched"); chosen.append(best); n_candidates.append(len(cands))
    gr["match_status"] = statuses
    gr["track_idx"] = chosen
    gr["match_candidates"] = n_candidates
    gr["spotify_matched"] = (gr["match_status"] == "matched").astype(int)

    rec["integration"]["rule"] = ("title key equality + any Spotify artist contained in the Grammy artist string; "
                                  "multiple candidates -> highest popularity, tie -> smallest track_id")
    rec["integration"]["status_counts"] = {k: int(v) for k, v in gr["match_status"].value_counts().items()}
    rec["integration"]["matched_nominations"] = int(gr["spotify_matched"].sum())
    rec["integration"]["match_rate_pct"] = round(100 * gr["spotify_matched"].mean(), 2)
    rec["integration"]["matched_with_more_than_one_candidate"] = int(((gr["match_candidates"] > 1)).sum())
    rec["integration"]["distinct_tracks_matched"] = int(gr["track_idx"].dropna().nunique())
    log.info("[transform] integration: matched=%d of %d (%.2f%%) | ambiguous (>1 candidate)=%d | status=%s",
             rec["integration"]["matched_nominations"], len(gr), rec["integration"]["match_rate_pct"],
             rec["integration"]["matched_with_more_than_one_candidate"], rec["integration"]["status_counts"])

    # ------------------------------------------------------------------ PREPARED TABLES
    # dim_year
    year_stats = gr.groupby("year").agg(rows=("nomination_bk", "size"), cats=("category", "nunique"),
                                        ceremony_title=("title", lambda s: s.mode().iloc[0] if not s.mode().empty else None))
    year_stats["records_per_category"] = (year_stats["rows"] / year_stats["cats"]).round(2)
    year_stats["coverage_type"] = np.where(year_stats["records_per_category"] >= 3, "all_nominees", "winners_only")
    dim_year = year_stats.reset_index().rename(columns={"year": "year_key"})
    dim_year["decade"] = (dim_year["year_key"] // 10) * 10
    dim_year["decade_label"] = dim_year["decade"].astype(str) + "s"
    dim_year = dim_year[S.PREPARED_COLUMNS["dim_year"]].sort_values("year_key").reset_index(drop=True)

    # dim_category (surrogate keys assigned in sorted business-key order -> deterministic)
    cats = gr[["category", "music_family", "nominee_type"]].drop_duplicates().sort_values("category").reset_index(drop=True)
    cats.insert(0, "category_key", np.arange(1, len(cats) + 1))
    dim_category = cats.rename(columns={"category": "category_name"})[S.PREPARED_COLUMNS["dim_category"]]

    # dim_genre (all genres of the catalogue + unknown member)
    genres = sorted(tracks["primary_genre"].dropna().unique())
    dim_genre = pd.DataFrame({"genre_key": np.arange(1, len(genres) + 1), "genre_name": genres})
    dim_genre = pd.concat([pd.DataFrame({"genre_key": [S.UNKNOWN_KEY], "genre_name": ["Unknown / not matched"]}),
                           dim_genre], ignore_index=True)
    genre_key_of = dict(zip(dim_genre["genre_name"], dim_genre["genre_key"]))

    # dim_track (only tracks referenced by at least one nomination + unknown member)
    used = sorted(int(i) for i in gr["track_idx"].dropna().unique())
    t = tracks.loc[used].sort_values("track_id").reset_index(drop=True)
    dim_track = pd.DataFrame({
        "track_key": np.arange(1, len(t) + 1),
        "spotify_track_id": t["track_id"], "track_name": t["track_name"], "artists": t["artists"],
        "album_name": t["album_name"], "genre_key": t["primary_genre"].map(genre_key_of),
        "genre_count": t["genre_count"].astype(int), "explicit": t["explicit"].astype(bool),
    })
    unknown_track = pd.DataFrame([{
        "track_key": S.UNKNOWN_KEY, "spotify_track_id": "UNKNOWN", "track_name": "Not matched in Spotify",
        "artists": None, "album_name": None, "genre_key": S.UNKNOWN_KEY, "genre_count": 0, "explicit": False}])
    dim_track = pd.concat([unknown_track, dim_track], ignore_index=True)[S.PREPARED_COLUMNS["dim_track"]]
    track_key_of_id = dict(zip(dim_track["spotify_track_id"], dim_track["track_key"]))

    # fact_grammy_nomination
    g = gr.sort_values("nomination_bk").reset_index(drop=True)
    sel = tracks.reindex(g["track_idx"].map(lambda v: None if pd.isna(v) else int(v)).tolist()).reset_index(drop=True)
    matched = g["spotify_matched"].astype(bool)
    cat_key_of = dict(zip(dim_category["category_name"], dim_category["category_key"]))
    fact = pd.DataFrame({
        "nomination_key": np.arange(1, len(g) + 1),
        "nomination_bk": g["nomination_bk"],
        "year_key": g["year"],
        "category_key": g["category"].map(cat_key_of),
        "track_key": [track_key_of_id[tid] if m else S.UNKNOWN_KEY
                      for tid, m in zip(sel["track_id"], matched)],
        "nominee": g["nominee"], "grammy_artist": g["artist"],
        "match_status": g["match_status"], "match_candidates": g["match_candidates"].astype(int),
        "spotify_matched": g["spotify_matched"].astype(int), "nomination_count": 1,
    })
    for col in ["popularity", "duration_ms", "danceability", "energy", "valence", "acousticness", "loudness", "tempo"]:
        fact[col] = np.where(matched, sel[col], np.nan)
    for col in ("popularity", "duration_ms"):          # integer measures: write 73, not 73.0, so COPY into INT works
        fact[col] = fact[col].round().astype("Int64")
    fact["batch_key"] = batch_key
    fact = fact[S.PREPARED_COLUMNS["fact_grammy_nomination"]]

    # fact_catalog_genre_baseline (reference population for AR1)
    base = tracks.groupby("primary_genre").agg(
        track_count=("track_id", "size"), avg_popularity=("popularity", "mean"),
        avg_danceability=("danceability", "mean"), avg_energy=("energy", "mean"),
        avg_valence=("valence", "mean"), avg_acousticness=("acousticness", "mean"),
        avg_loudness=("loudness", "mean")).reset_index()
    base["genre_key"] = base["primary_genre"].map(genre_key_of)
    for col in [c for c in base.columns if c.startswith("avg_")]:
        base[col] = base[col].round(4)
    base["batch_key"] = batch_key
    baseline = base[S.PREPARED_COLUMNS["fact_catalog_genre_baseline"]].sort_values("genre_key").reset_index(drop=True)

    tables = {"dim_year": dim_year, "dim_category": dim_category, "dim_genre": dim_genre,
              "dim_track": dim_track, "fact_grammy_nomination": fact,
              "fact_catalog_genre_baseline": baseline}
    for name, df in tables.items():
        df.to_csv(out_dir / f"{name}.csv", index=False)

    rec["prepared_rows"] = {n: int(len(d)) for n, d in tables.items()}
    rec["batch_key"] = batch_key
    rec["generated_at_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (out_dir / "reconciliation.json").write_text(json.dumps(rec, indent=2, default=int), encoding="utf-8")
    log.info("[transform] prepared tables written to %s | rows=%s", out_dir, rec["prepared_rows"])

    return {
        "prepared_dir": str(out_dir),
        "batch_key": batch_key,
        "tables": rec["prepared_rows"],
        "expected_fact_rows": int(rec["grammys"]["rows_out"]),
        "reconciliation_path": str(out_dir / "reconciliation.json"),
        "spotify_extract": spotify_meta, "grammys_extract": grammys_meta,
    }
