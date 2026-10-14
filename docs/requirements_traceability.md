# Analytical requirements and traceability

## Business context
A music-industry analyst wants to know whether winning or being nominated for a **Grammy** is reflected in how a track is
consumed on **Spotify**, and whether the "Grammy sound" has changed over time. Neither source answers this alone:
Spotify has no award information and the Grammy table has no popularity, audio features or genre.

| Source | Format / access | Grain | Rows | Strength | Limitation (from profiling) |
|---|---|---|---|---|---|
| Spotify tracks | CSV, `data/raw/spotify_dataset.csv` | track x genre | 114,000 (89,741 distinct `track_id`) | popularity, 9 audio features, genre | no release date; repeated tracks per genre |
| Grammy awards | PostgreSQL table `grammy_source.public.grammy_awards` | award record | 4,810 | year, category, nominee, artist | `winner` is True in 100% of rows; 2019 lists all nominees; 38% artist missing |

## Analytical requirements

| ID | Question | Why both sources | KPI / metric | Level of detail |
|---|---|---|---|---|
| **AR1** | Do Grammy-recognised tracks have higher Spotify popularity than the typical track of their genre, and how does that vary by decade of recognition? | Recognition (Grammy) x popularity and genre baseline (Spotify) | avg popularity; **popularity lift** = popularity - genre baseline | decade, genre |
| **AR2** | How has the audio profile (danceability, energy, valence, acousticness, loudness) of Grammy-recognised tracks changed across decades? | Decade (Grammy) x audio features (Spotify) | avg of each feature | decade |
| **AR3** | Which Spotify genres and Grammy music families concentrate recognised tracks, and how well can the sources be integrated? | Category/family (Grammy) x genre (Spotify) | matched nominations; match rate; avg popularity | genre, music family |

`winner` is deliberately **not** used by any requirement (see profiling: the flag is unreliable). Rows are "Grammy recognitions".

## Required attributes

| Requirement | Grammy attributes | Spotify attributes |
|---|---|---|
| AR1 | year, category, nominee, artist | track_id, track_name, artists, popularity, track_genre |
| AR2 | year, nominee, artist | danceability, energy, valence, acousticness, loudness, tempo |
| AR3 | category, nominee, artist | track_genre, track_name, artists |

Attributes **not** carried to the DW: Grammy `winner`, `workers`, `img`, `published_at`, `updated_at`; Spotify `key`, `mode`, `speechiness`,
`liveness`, `instrumentalness`, `time_signature`, `Unnamed: 0`. They serve no requirement.

## Traceability matrix

| Requirement | Profiling evidence | Rules | Transformation | DW objects | Dashboard visual |
|---|---|---|---|---|---|
| AR1 | popularity 0-100; grain track x genre; 24,259 repeated ids | SP-03/04/05/09, GR-03/04, PR-03..06, PR-13 | collapse to track grain, primary genre, genre baseline | `fact_grammy_nomination`, `fact_catalog_genre_baseline`, `dim_year`, `dim_track`, `dim_genre` | V1 popularity vs baseline by decade |
| AR2 | features in 0-1; 157 failed analyses | SP-06/07/10, PR-06/07 | failed-analysis rows -> NULL | `fact_grammy_nomination` measures, `dim_year` | V2 audio profile by decade |
| AR3 | 114 genres; 626 categories; 6.3% match rate | SP-09/12, GR-04/06, PR-05/08/09/10/11/12 | category harmonisation, music family, matching | `dim_category`, `dim_genre`, `dim_track`, `match_status` | V3 genres, V4 families, KPI match rate |
