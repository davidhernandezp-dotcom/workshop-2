-- =====================================================================
-- DATA WAREHOUSE  -  database `music_dw`, schema `dw`
-- Star schema (fact constellation) for "Spotify popularity & audio profile of
-- Grammy-recognised tracks".  Idempotent: safe to run on every load.
--
-- FACT 1  fact_grammy_nomination      grain = one Grammy award record
--                                     (year x category x nominee x artist)
-- FACT 2  fact_catalog_genre_baseline grain = one Spotify primary genre (reference
--                                     population used to compute "popularity lift")
-- DIMS    dim_year, dim_category, dim_genre, dim_track  (+ unknown member -1)
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS dw.dim_year (
    year_key              INTEGER PRIMARY KEY,                 -- the calendar year is its own key
    decade                INTEGER NOT NULL,
    decade_label          TEXT    NOT NULL,
    ceremony_title        TEXT,
    coverage_type         TEXT    NOT NULL CHECK (coverage_type IN ('winners_only','all_nominees')),
    records_per_category  NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS dw.dim_category (
    category_key   INTEGER PRIMARY KEY,
    category_name  TEXT NOT NULL UNIQUE,                       -- business key (harmonised spelling)
    music_family   TEXT NOT NULL,
    nominee_type   TEXT NOT NULL CHECK (nominee_type IN ('recording_or_work','album','person'))
);

CREATE TABLE IF NOT EXISTS dw.dim_genre (
    genre_key   INTEGER PRIMARY KEY,
    genre_name  TEXT NOT NULL UNIQUE                           -- business key
);

CREATE TABLE IF NOT EXISTS dw.dim_track (
    track_key         INTEGER PRIMARY KEY,
    spotify_track_id  TEXT NOT NULL UNIQUE,                    -- business key
    track_name        TEXT,
    artists           TEXT,
    album_name        TEXT,
    genre_key         INTEGER NOT NULL REFERENCES dw.dim_genre (genre_key),
    genre_count       INTEGER,
    explicit          BOOLEAN
);

CREATE TABLE IF NOT EXISTS dw.fact_grammy_nomination (
    nomination_key    INTEGER PRIMARY KEY,
    nomination_bk     CHAR(32) NOT NULL UNIQUE,                -- md5(year|category|nominee|artist): the grain
    year_key          INTEGER  NOT NULL REFERENCES dw.dim_year (year_key),
    category_key      INTEGER  NOT NULL REFERENCES dw.dim_category (category_key),
    track_key         INTEGER  NOT NULL REFERENCES dw.dim_track (track_key),   -- -1 = not matched
    nominee           TEXT     NOT NULL,                       -- degenerate dimension
    grammy_artist     TEXT,                                    -- degenerate dimension
    match_status      TEXT     NOT NULL,                       -- degenerate dimension (integration outcome)
    match_candidates  INTEGER  NOT NULL,
    spotify_matched   SMALLINT NOT NULL CHECK (spotify_matched IN (0,1)),
    nomination_count  SMALLINT NOT NULL DEFAULT 1,
    popularity        INTEGER,
    duration_ms       INTEGER,
    danceability      DOUBLE PRECISION,
    energy            DOUBLE PRECISION,
    valence           DOUBLE PRECISION,
    acousticness      DOUBLE PRECISION,
    loudness          DOUBLE PRECISION,
    tempo             DOUBLE PRECISION,
    batch_key         TEXT     NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_fact_nom_year     ON dw.fact_grammy_nomination (year_key);
CREATE INDEX IF NOT EXISTS ix_fact_nom_category ON dw.fact_grammy_nomination (category_key);
CREATE INDEX IF NOT EXISTS ix_fact_nom_track    ON dw.fact_grammy_nomination (track_key);

CREATE TABLE IF NOT EXISTS dw.fact_catalog_genre_baseline (
    genre_key         INTEGER PRIMARY KEY REFERENCES dw.dim_genre (genre_key),
    track_count       INTEGER NOT NULL CHECK (track_count > 0),
    avg_popularity    DOUBLE PRECISION NOT NULL,
    avg_danceability  DOUBLE PRECISION,
    avg_energy        DOUBLE PRECISION,
    avg_valence       DOUBLE PRECISION,
    avg_acousticness  DOUBLE PRECISION,
    avg_loudness      DOUBLE PRECISION,
    batch_key         TEXT NOT NULL
);

-- ------------------------------------------------------------------ audit / observability
CREATE TABLE IF NOT EXISTS dw.audit_load (
    batch_key        TEXT PRIMARY KEY,         -- hash of the two source batches: same input => same key
    first_run_id     TEXT NOT NULL,
    last_run_id      TEXT NOT NULL,
    first_loaded_at  TIMESTAMPTZ NOT NULL,
    last_loaded_at   TIMESTAMPTZ NOT NULL,
    load_count       INTEGER NOT NULL,         -- how many times this same batch was (re)loaded
    rows_loaded      JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.audit_validation_result (
    batch_key           TEXT NOT NULL,
    gate                TEXT NOT NULL,
    dataset             TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    expectation_type    TEXT NOT NULL,
    column_name         TEXT,
    severity            TEXT NOT NULL,
    success             BOOLEAN NOT NULL,
    unexpected_count    BIGINT,
    unexpected_percent  DOUBLE PRECISION,
    observed_value      TEXT
);
CREATE INDEX IF NOT EXISTS ix_audit_val_batch ON dw.audit_validation_result (batch_key);

-- ------------------------------------------------------------------ BI-facing views
CREATE OR REPLACE VIEW dw.v_nomination_detail AS
SELECT f.nomination_key, y.year_key AS grammy_year, y.decade, y.decade_label, y.coverage_type,
       c.category_name, c.music_family, c.nominee_type,
       f.nominee, f.grammy_artist, f.match_status, f.spotify_matched, f.match_candidates,
       t.spotify_track_id, t.track_name, t.artists, t.album_name, t.explicit,
       g.genre_name AS primary_genre,
       f.popularity, f.danceability, f.energy, f.valence, f.acousticness, f.loudness, f.tempo, f.duration_ms,
       b.avg_popularity AS genre_baseline_popularity,
       (f.popularity - b.avg_popularity) AS popularity_lift
FROM dw.fact_grammy_nomination f
JOIN dw.dim_year      y ON y.year_key     = f.year_key
JOIN dw.dim_category  c ON c.category_key = f.category_key
JOIN dw.dim_track     t ON t.track_key    = f.track_key
JOIN dw.dim_genre     g ON g.genre_key    = t.genre_key
LEFT JOIN dw.fact_catalog_genre_baseline b ON b.genre_key = t.genre_key;

-- AR1: popularity of Grammy-recognised tracks vs. their genre baseline, by decade
CREATE OR REPLACE VIEW dw.v_kpi_popularity_by_decade AS
SELECT decade, decade_label,
       COUNT(*)                                        AS matched_nominations,
       ROUND(AVG(popularity)::numeric, 2)              AS avg_popularity,
       ROUND(AVG(genre_baseline_popularity)::numeric, 2) AS avg_genre_baseline_popularity,
       ROUND(AVG(popularity_lift)::numeric, 2)         AS avg_popularity_lift
FROM dw.v_nomination_detail
WHERE spotify_matched = 1
GROUP BY decade, decade_label
ORDER BY decade;

-- AR2: audio profile of Grammy-recognised tracks by decade
CREATE OR REPLACE VIEW dw.v_kpi_audio_by_decade AS
SELECT decade, decade_label,
       COUNT(*)                                   AS matched_nominations,
       ROUND(AVG(danceability)::numeric, 3)       AS avg_danceability,
       ROUND(AVG(energy)::numeric, 3)             AS avg_energy,
       ROUND(AVG(valence)::numeric, 3)            AS avg_valence,
       ROUND(AVG(acousticness)::numeric, 3)       AS avg_acousticness,
       ROUND(AVG(loudness)::numeric, 2)           AS avg_loudness_db
FROM dw.v_nomination_detail
WHERE spotify_matched = 1
GROUP BY decade, decade_label
ORDER BY decade;

-- AR3 (a): genres that concentrate Grammy-recognised tracks
CREATE OR REPLACE VIEW dw.v_kpi_genre_summary AS
SELECT primary_genre,
       COUNT(*)                                      AS matched_nominations,
       COUNT(DISTINCT spotify_track_id)              AS distinct_tracks,
       ROUND(AVG(popularity)::numeric, 2)            AS avg_popularity,
       ROUND(AVG(genre_baseline_popularity)::numeric, 2) AS genre_baseline_popularity,
       ROUND(AVG(popularity_lift)::numeric, 2)       AS avg_popularity_lift
FROM dw.v_nomination_detail
WHERE spotify_matched = 1
GROUP BY primary_genre
ORDER BY matched_nominations DESC, primary_genre;

-- AR3 (b): Grammy music families, with match coverage
CREATE OR REPLACE VIEW dw.v_kpi_family_summary AS
SELECT music_family,
       COUNT(*)                                                       AS total_nominations,
       SUM(spotify_matched)                                           AS matched_nominations,
       ROUND(100.0 * SUM(spotify_matched) / COUNT(*), 2)              AS match_rate_pct,
       ROUND(AVG(popularity) FILTER (WHERE spotify_matched = 1)::numeric, 2) AS avg_popularity_matched
FROM dw.v_nomination_detail
GROUP BY music_family
ORDER BY matched_nominations DESC, total_nominations DESC;

-- Data-product reliability: how much of the Grammy side could be integrated
CREATE OR REPLACE VIEW dw.v_kpi_match_coverage AS
SELECT match_status,
       COUNT(*)                                             AS nominations,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)   AS pct_of_nominations
FROM dw.fact_grammy_nomination
GROUP BY match_status
ORDER BY nominations DESC;

CREATE OR REPLACE VIEW dw.v_kpi_headline AS
SELECT COUNT(*)                                                     AS total_nominations,
       SUM(spotify_matched)                                         AS matched_nominations,
       ROUND(100.0 * SUM(spotify_matched) / COUNT(*), 2)            AS match_rate_pct,
       ROUND(AVG(popularity) FILTER (WHERE spotify_matched = 1)::numeric, 2) AS avg_popularity_matched,
       ROUND(AVG(popularity_lift) FILTER (WHERE spotify_matched = 1)::numeric, 2) AS avg_popularity_lift,
       (SELECT batch_key FROM dw.audit_load ORDER BY last_loaded_at DESC LIMIT 1) AS latest_batch_key
FROM dw.v_nomination_detail;
