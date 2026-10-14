"""Build (and optionally execute) notebooks/data_profiling.ipynb.

    python scripts/build_profiling_notebook.py            # build + execute
    python scripts/build_profiling_notebook.py --no-exec  # build only
"""
import sys
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
cells = []
md = lambda t: cells.append(nbf.v4.new_markdown_cell(t.strip("\n")))
code = lambda t: cells.append(nbf.v4.new_code_cell(t.strip("\n")))

md("""
# Workshop-2 - Data profiling (Spotify + Grammy)

**Purpose.** Profiling *discovers* what the data look like and what could go wrong (Topic 2.1). It does **not** clean data and it
does **not** create rules automatically: every finding below is interpreted and then linked to a quality rule (`src/quality_rules.py`)
or to a documented transformation (`docs/transformation_decisions.md`).

Three profiling types (class 2.1): **structural** (schema, types, row counts), **content** (nulls, ranges, distributions),
**relationship** (keys, duplicates, cross-source consistency).

Analytical requirements that guide what we inspect: **AR1** popularity of Grammy-recognised tracks vs genre baseline by decade,
**AR2** audio profile by decade, **AR3** genres / Grammy families that concentrate recognised tracks.
""")
code("""
import sys, re, warnings
from pathlib import Path
import numpy as np, pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from IPython.display import display

warnings.filterwarnings("ignore")
ROOT = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
sys.path.insert(0, str(ROOT))
from src import transform as T

EVID = ROOT / "docs" / "evidence" / "profiling"
EVID.mkdir(parents=True, exist_ok=True)
pd.set_option("display.width", 200); pd.set_option("display.max_columns", 40)

sp = pd.read_csv(ROOT / "data/raw/spotify_dataset.csv", low_memory=False)
gr = pd.read_csv(ROOT / "data/raw/the_grammy_awards.csv")
print("Spotify:", sp.shape, "| Grammy:", gr.shape)
""")
md("## 1. Structural profiling")
code("""
def structure(df, name):
    t = pd.DataFrame({"dtype": df.dtypes.astype(str), "non_null": df.notna().sum(), "distinct": df.nunique(dropna=True)})
    t.insert(0, "dataset", name); return t
struct = pd.concat([structure(sp, "spotify"), structure(gr, "grammy")])
struct.to_csv(EVID / "structure.csv")
display(struct)
""")
md("""
**Reading.** Spotify has an anonymous export index (`Unnamed: 0`) with no business meaning. `artists` packs several artists into one string
separated by `;`. Grammy `winner` is boolean, `year` integer, dates are ISO text.
""")
md("## 2. Completeness (content profiling)")
code("""
def nulls(df, name):
    t = pd.DataFrame({"missing": df.isna().sum(), "missing_pct": (df.isna().mean() * 100).round(3)})
    t.insert(0, "dataset", name); return t[t.missing > 0]
miss = pd.concat([nulls(sp, "spotify"), nulls(gr, "grammy")])
miss.to_csv(EVID / "missingness.csv"); display(miss)
""")
code("""
# WHY is Grammy.artist missing? -> structural, by category type
gr["_cat"] = gr.category.str.lower().str.strip()
gr["_type"] = gr._cat.map(T.classify_nominee_type)
why = gr.groupby("_type").agg(rows=("artist", "size"), artist_missing_pct=("artist", lambda s: round(100 * s.isna().mean(), 1)))
display(why)
print("Song Of The Year rows without artist:", int(gr[gr._cat == "song of the year"].artist.isna().sum()), "of", int((gr._cat == "song of the year").sum()))
print("Nominee missing:", gr[gr.nominee.isna()].category.unique().tolist())
why.to_csv(EVID / "grammy_artist_missing_by_type.csv")
""")
md("""
**Interpretation.** The 38% missing `artist` is **not random**: it is concentrated in categories that honour songwriters, composers,
producers or technical credits (Song Of The Year 70/70, Opera Recording, Album Notes, Instrumental Composition, Country Song...). Person categories
are 99% missing and recordings 42%. Missing `artist` cannot be "fixed" without inventing data; we (a) tolerate it with a *warning* threshold
(rule **GR-06**) and (b) recover it only for *matching* from other rows of the same year+title (documented transformation). The 6 missing `nominee`
rows (4 "Remixer of the Year", 2 "Best New Country & Western Artist") are unidentifiable records -> dropped in transformation, rule **GR-05**.
Spotify has exactly **one** row with no name/artist/album -> rule **SP-08**.
""")
md("## 3. Validity - ranges and impossible values")
code("""
num = ["popularity","duration_ms","danceability","energy","valence","acousticness","loudness","tempo","speechiness","liveness","instrumentalness"]
rng = sp[num].describe().T[["min","25%","50%","75%","max"]]
rng["zeros"] = (sp[num] == 0).sum()
display(rng); rng.to_csv(EVID / "spotify_numeric_ranges.csv")
print("tempo == 0:", int((sp.tempo == 0).sum()), "| duration_ms == 0:", int((sp.duration_ms == 0).sum()),
      "| popularity == 0:", int((sp.popularity == 0).sum()), f"({(sp.popularity == 0).mean():.1%})")
print("time_signature values:", sp.time_signature.value_counts().sort_index().to_dict())
print("Grammy year range:", gr.year.min(), "-", gr.year.max(), "| distinct years:", gr.year.nunique())
""")
md("""
**Interpretation.**
* `popularity` 0-100, `danceability/energy/valence/acousticness` 0-1 and `loudness` -49.5..+4.5: **all inside documented scales** -> strict rules
  **SP-05/SP-06** (critical) and tolerant **SP-07**. The rules were written from the *scale documentation*, not from today's min/max.
* **157 tracks with tempo = 0** are physically impossible, and the profile shows the *same 157 rows* also have danceability = speechiness =
  time_signature = 0: one failure pattern of Spotify's audio analysis (mostly sparse ambient / classical tracks), not 157 real zeros. -> rule **SP-10**
  (warning) + transformation sets tempo, danceability and valence of those rows to NULL (never imputed). The single duration = 0 row is the record that has no name.
* 14% of tracks have popularity 0. That is a *valid* value of the scale (no recent plays) -> **not** treated as an error; "suspicious does not mean wrong".
""")
md("## 4. Uniqueness and grain")
code("""
rep_ids = int(sp.track_id.duplicated().sum()); pair_dups = int(sp.duplicated(["track_id","track_genre"]).sum())
print(f"Spotify rows: {len(sp):,} | distinct track_id: {sp.track_id.nunique():,} | repeated track_id rows: {rep_ids:,}")
print(f"Repeated (track_id, track_genre) pairs: {pair_dups} -> rows involved: {int(sp.duplicated(['track_id','track_genre'], keep=False).sum())}")
d = sp.groupby("track_id").agg(n=("track_id", "size"), pops=("popularity", "nunique"), genres=("track_genre", "nunique"))
print("track_ids with >1 row:", int((d.n > 1).sum()), "| with different popularity across rows:", int((d.pops > 1).sum()))
key = ["year","category","nominee","artist"]
print("Grammy duplicated (year, category, nominee, artist):", int(gr.duplicated(key).sum()))
""")
md("""
**Interpretation (grain).** The Spotify source grain is **track x genre** (a song listed in several genres appears several times), *not* one row per track.
Repeated `track_id` is therefore expected; only exact repeats of `(track_id, genre)` are duplicates (**SP-04**, warning). Counting rows as tracks
would over-weight multi-genre songs -> transformation collapses to **one row per track**, keeping the highest-popularity copy (it also defines the *primary genre*).
Grammy has no duplicated business key (**GR-09**).
""")
md("## 5. Consistency of categories and the Grammy `winner` flag")
code("""
low = gr.category.str.lower().str.replace(r"\\s+", " ", regex=True).str.strip()
print("category spellings:", gr.category.nunique(), "-> after case/space folding:", low.nunique())
print(gr.winner.value_counts(dropna=False).to_dict())
per = gr.groupby(["year","category"]).size().groupby("year").mean().round(2)
display(per.tail(6).to_frame("avg rows per category"))
""")
code("""
fig, ax = plt.subplots(figsize=(10, 3.4))
gr.groupby("year").size().plot(ax=ax, color="#2a6f97")
ax.set_title("Grammy rows per year - 2019 is a 5x outlier"); ax.set_ylabel("rows"); ax.grid(alpha=.3)
fig.tight_layout(); fig.savefig(EVID / "grammy_rows_per_year.png", dpi=130); plt.close(fig)
from IPython.display import Image; display(Image(EVID / "grammy_rows_per_year.png"))
""")
md("""
**Interpretation - the most important finding.** `winner = True` in **100%** of rows, yet 2019 contains ~5 rows per category (all nominees)
while 1958-2018 contain ~1 (winners only). The flag cannot separate winners from nominees and the **row grain differs by year**.
Decisions: (1) **no requirement uses `winner`**; rows are "Grammy recognitions"; (2) `dim_year.coverage_type` = `winners_only` / `all_nominees`
makes the difference explicit; (3) rule **GR-08** (warning) *documents* the limitation and is **expected to fail** on this batch, proving the
"warning -> continue" policy. 638 category spellings collapse to 626 (case/spacing variants) -> harmonised in transformation.
""")
code("""
# whitespace consistency (feeds transformation T2)
for df, name, cols in [(sp, "spotify", ["artists","album_name","track_name","track_genre"]), (gr, "grammy", ["category","nominee","artist","title"])]:
    for c in cols:
        v = df[c].dropna()
        print(f"{name}.{c}: leading/trailing spaces={int((v != v.str.strip()).sum())} | double spaces={int(v.str.contains('  ').sum())}")
""")
md("## 6. Relationship profiling - can the two sources be integrated?")
code("""
sp2 = sp.dropna(subset=["track_name","artists"]).copy()
sp2["title_key"] = sp2.track_name.map(T.norm_title)
gr2 = gr.dropna(subset=["nominee"]).copy()
gr2["title_key"] = gr2.nominee.map(T.norm_title)
shared = set(sp2.title_key.dropna()) & set(gr2.title_key.dropna())
print("distinct normalised titles in common:", len(shared), "| Grammy rows whose title exists in Spotify:", int(gr2.title_key.isin(shared).sum()))
print("There is NO shared key -> integration needs title + artist matching (fuzzy by normalisation, deterministic).")
""")
code("""
# Run the real transformation logic on the raw files to quantify integration quality (same code as the pipeline)
import tempfile, hashlib, json
tmp = Path(tempfile.mkdtemp())
(tmp / "raw").mkdir()
sp.to_csv(tmp / "raw/spotify_raw.csv", index=False)
g_in = gr.drop(columns=[c for c in gr.columns if c.startswith("_")]).copy(); g_in.insert(0, "row_id", range(1, len(g_in) + 1))
g_in.to_csv(tmp / "raw/grammys_raw.csv", index=False)
sm = {"raw_path": str(tmp / "raw/spotify_raw.csv"), "source_sha256": "profiling"}
gm = {"raw_path": str(tmp / "raw/grammys_raw.csv"), "source_sha256": "profiling"}
meta = T.transform_and_integrate(sm, gm, tmp)
rec = json.load(open(meta["reconciliation_path"]))
status = pd.Series(rec["integration"]["status_counts"]).rename("nominations").to_frame()
status["pct"] = (100 * status.nominations / status.nominations.sum()).round(2)
display(status); status.to_csv(EVID / "integration_status.csv")
print("matched with >1 Spotify candidate:", rec["integration"]["matched_with_more_than_one_candidate"],
      "| distinct tracks matched:", rec["integration"]["distinct_tracks_matched"])
""")
code("""
fact = pd.read_csv(Path(meta["prepared_dir"]) / "fact_grammy_nomination.csv")
fact["decade"] = (fact.year_key // 10) * 10
by_dec = fact.groupby("decade").agg(nominations=("nomination_key", "size"), matched=("spotify_matched", "sum"))
by_dec["match_rate_pct"] = (100 * by_dec.matched / by_dec.nominations).round(1)
display(by_dec); by_dec.to_csv(EVID / "match_rate_by_decade.csv")
ax = by_dec.match_rate_pct.plot(kind="bar", figsize=(8, 3), color="#52b788"); ax.set_ylabel("% matched"); ax.set_title("Integration coverage by decade")
plt.tight_layout(); plt.savefig(EVID / "match_rate_by_decade.png", dpi=130); plt.close()
""")
md("""
**Interpretation.** Only ~6% of Grammy records can be linked to a Spotify track. The rest are not matchable on purpose (albums, people,
no performing artist) or simply absent from the 114k-track catalogue. That is an **honest property of the data**, so the KPIs are
computed on matched recordings only and the **match coverage itself is a KPI**. Match counts per decade (5 in the 1950s ... 90 in the 2010s)
are small in the oldest decades -> the dashboard shows `n` next to every average. 115 matches have several candidate Spotify versions: the
deterministic tie-break (highest popularity) is documented and monitored by rule **PR-09**.
""")
md("## 7. Findings -> risk -> dimension -> rule -> decision")
code("""
findings = pd.DataFrame([
 ("Spotify grain is track x genre (24,259 repeated track_id)", "Counting rows as tracks double-counts songs", "Uniqueness", "SP-04 (warning)", "TRANSFORM: collapse to one row per track; primary genre = highest-popularity copy"),
 ("450 exact duplicate (track_id, genre) pairs", "Inflated genre baseline", "Uniqueness", "SP-04 (warning, >=99%)", "TRANSFORM: drop exact repeats, log count"),
 ("1 row with no track_name / artists / album", "Cannot be matched or labelled", "Completeness", "SP-08 (warning)", "TRANSFORM: drop, log count"),
 ("157 rows tempo = danceability = speechiness = time_signature = 0 (audio analysis failed)", "Impossible values bias AR2 averages", "Validity", "SP-10 (warning)", "TRANSFORM: tempo, danceability, valence -> NULL (not imputed)"),
 ("popularity / audio features inside documented scales", "One out-of-scale value corrupts KPIs", "Validity", "SP-05, SP-06 (critical)", "RAW GATE: stop; never auto-correct"),
 ("14% popularity = 0", "Looks suspicious", "Accuracy (n/a)", "none", "KEEP: valid value of the scale"),
 ("Grammy winner = True in 100% of rows; 2019 lists all nominees", "Winners cannot be distinguished; grain differs by year", "Consistency", "GR-08 (warning, expected to fail)", "DESIGN: do not use winner; dim_year.coverage_type"),
 ("38% artist missing (songwriter / composer / technical categories)", "Matching impossible for those rows", "Completeness", "GR-06 (warning, >=50%)", "TRANSFORM: recover from same year+title for matching only; flag status"),
 ("6 nominee missing (4 Remixer, 2 New Country & Western Artist)", "Unidentifiable record", "Completeness", "GR-05 (warning)", "TRANSFORM: drop, log count"),
 ("638 category spellings vs 629 case-folded", "Fragmented dim_category", "Consistency", "PR-02 (critical, unique category_name)", "TRANSFORM: canonical spelling = most frequent"),
 ("No shared key between sources", "Integration is approximate", "Consistency", "PR-05, PR-08, PR-09, PR-10", "TRANSFORM: normalised title + artist; monitor coverage"),
], columns=["finding", "risk", "dimension", "rule", "decision"])
findings.to_csv(EVID / "findings_to_rules.csv", index=False)
display(findings.style.set_properties(**{"text-align": "left"}))
""")
nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}})
out = ROOT / "notebooks" / "data_profiling.ipynb"
out.parent.mkdir(exist_ok=True)
if "--no-exec" not in sys.argv:
    from nbconvert.preprocessors import ExecutePreprocessor
    ExecutePreprocessor(timeout=900, kernel_name="python3").preprocess(nb, {"metadata": {"path": str(out.parent)}})
nbf.write(nb, out)
print("notebook written:", out)
