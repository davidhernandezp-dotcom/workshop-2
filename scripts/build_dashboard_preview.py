"""Render a PNG preview of the dashboard straight from the warehouse views.

This is only a PREVIEW / fallback for the README. The official dashboard must be built in a BI tool
connected to the DW (see docs/dashboard_guide.md).
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src import config  # noqa: E402

engine = create_engine(config.db_url(config.DW_DB_NAME))
q = lambda v, extra="": pd.read_sql(f"SELECT * FROM dw.{v} {extra}", engine)
head = q("v_kpi_headline").iloc[0]
pop, aud = q("v_kpi_popularity_by_decade"), q("v_kpi_audio_by_decade")
genre = q("v_kpi_genre_summary", "LIMIT 10")
fam = q("v_kpi_family_summary")
fam = fam[fam.matched_nominations > 0].head(10)

fig = plt.figure(figsize=(15, 9.2))
fig.suptitle("Grammy-recognised tracks on Spotify - dashboard PREVIEW (official dashboard: BI tool on the DW)", fontsize=13, fontweight="bold")
gs = fig.add_gridspec(3, 3, height_ratios=[0.5, 1.6, 1.6], hspace=0.55, wspace=0.75)

cards = [("Grammy records in DW", f"{int(head.total_nominations):,}"),
         ("Matched to a Spotify track", f"{int(head.matched_nominations)}  ({head.match_rate_pct}%)"),
         ("Avg popularity (matched)", f"{head.avg_popularity_matched}"),
         ]
for i, (t, v) in enumerate(cards):
    ax = fig.add_subplot(gs[0, i]); ax.axis("off")
    ax.text(0.5, 0.65, v, ha="center", fontsize=20, fontweight="bold", color="#1b4965")
    ax.text(0.5, 0.15, t, ha="center", fontsize=10, color="#555")
    ax.add_patch(plt.Rectangle((0.02, 0.0), 0.96, 1.0, fill=False, ec="#bbb", transform=ax.transAxes))

ax = fig.add_subplot(gs[1, 0:2])
x = range(len(pop)); w = 0.38
ax.bar([i - w / 2 for i in x], pop.avg_popularity, w, label="Grammy tracks", color="#2a6f97")
ax.bar([i + w / 2 for i in x], pop.avg_genre_baseline_popularity, w, label="Genre baseline (catalogue)", color="#a9d6e5")
for i, (n, l) in enumerate(zip(pop.matched_nominations, pop.avg_popularity_lift)):
    ax.text(i, pop.avg_popularity.iloc[i] + 1.5, f"n={n}\n{l:+.1f}", ha="center", fontsize=8)
ax.set_xticks(list(x)); ax.set_xticklabels(pop.decade_label); ax.set_ylim(0, 85)
ax.set_title("AR1 - Popularity vs genre baseline by decade (label = n, lift)"); ax.legend(fontsize=8); ax.grid(axis="y", alpha=.3)

ax = fig.add_subplot(gs[1, 2])
for col, lab in [("avg_danceability", "danceability"), ("avg_energy", "energy"), ("avg_valence", "valence"), ("avg_acousticness", "acousticness")]:
    ax.plot(aud.decade_label, aud[col], marker="o", label=lab)
ax.set_title("AR2 - Audio profile by decade"); ax.legend(fontsize=8); ax.grid(alpha=.3); ax.tick_params(axis="x", labelsize=8)

ax = fig.add_subplot(gs[2, 0:2])
g = genre.iloc[::-1]
ax.barh(g.primary_genre, g.matched_nominations, color="#52b788")
for i, (n, l) in enumerate(zip(g.matched_nominations, g.avg_popularity_lift)):
    ax.text(n + 0.3, i, f"lift {l:+.1f}", va="center", fontsize=8)
ax.set_xlim(0, g.matched_nominations.max() * 1.3)
ax.set_title("AR3 - Spotify genres with most Grammy-recognised matches (top 10)"); ax.grid(axis="x", alpha=.3)

ax = fig.add_subplot(gs[2, 2])
f = fam.iloc[::-1]
ax.barh(f.music_family, f.matched_nominations, color="#e9c46a")
ax.set_title("AR3 - Matches by Grammy music family"); ax.tick_params(axis="y", labelsize=8); ax.grid(axis="x", alpha=.3)

out = ROOT / "docs" / "evidence" / "dashboard_preview.png"
fig.savefig(out, dpi=120, bbox_inches="tight")
print("written", out)
