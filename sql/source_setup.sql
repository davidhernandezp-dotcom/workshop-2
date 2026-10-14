-- =====================================================================
-- SOURCE PREPARATION  (NOT the final ETL load)
-- Creates the operational Grammy source table inside the relational
-- source database `grammy_source`. The pipeline extracts from this table.
-- Types mirror the provided CSV; no cleaning is applied here.
-- =====================================================================
CREATE TABLE IF NOT EXISTS public.grammy_awards (
    row_id        BIGSERIAL PRIMARY KEY,   -- operational key = CSV row order
    year          SMALLINT,
    title         TEXT,
    published_at  TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ,
    category      TEXT,
    nominee       TEXT,
    artist        TEXT,
    workers       TEXT,
    img           TEXT,
    winner        BOOLEAN
);

COMMENT ON TABLE public.grammy_awards IS
  'Operational copy of the_grammy_awards.csv. Loaded by scripts/load_grammy_source.py (source preparation).';
