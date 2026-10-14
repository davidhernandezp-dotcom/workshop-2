"""Create deliberately damaged copies of the sources for the controlled-failure test.

The ORIGINAL files / tables are never modified.

  * data/raw/spotify_dataset_bad.csv  -> 100 rows with popularity = 150 (valid range is 0-100)
                                         violates SP-05 (CRITICAL)
  * grammy_source.public.grammy_awards_bad -> copy of the table with 30 rows whose year = 2099
                                         violates GR-03 (CRITICAL)

Usage (inside the Airflow container):
    python /opt/airflow/scripts/make_bad_batches.py            # both
    python /opt/airflow/scripts/make_bad_batches.py spotify    # only the CSV
    python /opt/airflow/scripts/make_bad_batches.py grammy     # only the table
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src import config  # noqa: E402


def make_spotify_bad() -> None:
    src = config.RAW_DIR / config.SPOTIFY_DEFAULT_FILE
    dst = config.RAW_DIR / "spotify_dataset_bad.csv"
    df = pd.read_csv(src, low_memory=False)
    df.loc[df.index[:100], "popularity"] = 150          # impossible on the 0-100 Spotify scale
    df.to_csv(dst, index=False)
    print(f"[spotify] {dst.name} created: 100 rows with popularity=150 (original untouched)")


def make_grammy_bad() -> None:
    conn = psycopg2.connect(host=config.DB_HOST, port=config.DB_PORT, user=config.DB_USER,
                            password=config.DB_PASSWORD, dbname=config.SOURCE_DB_NAME)
    with conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.grammy_awards_bad")
        cur.execute("CREATE TABLE public.grammy_awards_bad AS TABLE public.grammy_awards")
        cur.execute("UPDATE public.grammy_awards_bad SET year = 2099 WHERE row_id <= 30")
        cur.execute("SELECT count(*), count(*) FILTER (WHERE year = 2099) FROM public.grammy_awards_bad")
        total, bad = cur.fetchone()
    conn.close()
    print(f"[grammy] table public.grammy_awards_bad created: {total} rows, {bad} with year=2099 (original untouched)")


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "both"
    if which in ("both", "spotify"):
        make_spotify_bad()
    if which in ("both", "grammy"):
        make_grammy_bad()
