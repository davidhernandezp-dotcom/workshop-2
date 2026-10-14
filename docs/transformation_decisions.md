# Transformation decisions

Each decision is justified by profiling evidence or by a requirement, never by "to make validation pass". Counts come from
`docs/evidence/runs/run1_success/reconciliation.json`.

| # | Decision | Evidence | Requirement | Rows affected | Rule |
|---|---|---|---|---|---|
| T1 | Drop export index `Unnamed: 0` | no business meaning | - | column | - |
| T2 | Trim / collapse whitespace in text columns (content unchanged) | 8 Spotify `artists` values with leading/trailing spaces; Grammy double spaces in 2 `category`, 7 `nominee`, 1 `artist` values (and in every `title`) | AR3 | few | - |
| T3 | Drop Spotify row with no track_name/artists/album | exactly 1 row; unmatchable | AR1-3 | 1 | SP-08 |
| T4 | Drop exact duplicate `(track_id, track_genre)` pairs (keep highest popularity) | 450 pairs | AR3 | 450 | SP-04 |
| T5 | **Audio-analysis failure -> NULL** for `tempo`, `danceability`, `valence` | the same 157 rows have tempo = danceability = speechiness = time_signature = 0 | AR2 | 157 | SP-10 |
| T6 | Collapse Spotify to **one row per track**; primary genre = genre of highest-popularity copy; `genre_count` kept | grain is track x genre (24,259 repeated ids) | AR1, AR3 | 113,549 -> 89,740 tracks | SP-04 |
| T7 | Drop Grammy rows without `nominee` | 6 rows (4 Remixer, 2 New Country & Western Artist); unidentifiable | AR1-3 | 6 | GR-05 |
| T8 | Harmonise category spellings (most frequent spelling per case-folded key) | 638 spellings -> 626 | AR3 | 12 spellings | PR-02 |
| T9 | Derive `decade`, `music_family` (keyword rules), `nominee_type`, `coverage_type` | AR3 needs families; 2019 grain differs | AR2, AR3 | all | PR-11, PR-12 |
| T10 | Recover missing `artist` **for matching only** from another row with the same year + title (only if exactly one artist) | 1,834 missing before | AR1-3 | 254 recovered (1,834 -> 1,580) | GR-06 |
| T11 | Integrate: normalised title equality AND any Spotify artist contained in the Grammy artist string; ties -> highest popularity | no shared key | AR1-3 | 304 matched / 4,804 | PR-05, PR-08..10 |
| T12 | Build `fact_catalog_genre_baseline` from **all** 89,740 tracks | AR1 needs a comparison population | AR1 | 113 genres | PR-13 |

## Deliberately NOT done
* `popularity = 0` (14% of tracks) is **kept**: a valid value of the scale.
* Out-of-range values (e.g. popularity 150) are **never repaired**; the raw gate stops the run.
* `winner` is not imputed or interpreted.
* No imputation of any missing numeric value.

## Normalisation used for matching
Lower-case, ASCII folding (`Beyonce`), `&` -> `and`, punctuation removed, `(...)`/`[...]` removed, and version suffixes such as
` - Remastered 2011`, ` - Radio Edit` removed from **titles**. Unit-tested in `tests/test_pipeline_rules.py`.

## Effect of T5 (why it matters)
After adding T5, the 1960s average danceability moved from 0.545 to 0.605 and valence from 0.561 to 0.623: at least one matched track
carried the failed-analysis zeros and was silently biasing AR2.
