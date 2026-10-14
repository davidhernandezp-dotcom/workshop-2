"""Schemas and controlled vocabularies shared by transform, validation and load."""
from __future__ import annotations

# ---------------------------------------------------------------- raw contracts
SPOTIFY_REQUIRED_COLUMNS = [
    "track_id", "artists", "album_name", "track_name", "popularity", "duration_ms",
    "explicit", "danceability", "energy", "loudness", "acousticness", "valence",
    "tempo", "track_genre",
]
GRAMMY_REQUIRED_COLUMNS = ["year", "title", "category", "nominee", "artist", "winner"]

# ------------------------------------------------------- prepared (DW-shaped) tables
PREPARED_COLUMNS: dict[str, list[str]] = {
    "dim_year": ["year_key", "decade", "decade_label", "ceremony_title",
                 "coverage_type", "records_per_category"],
    "dim_category": ["category_key", "category_name", "music_family", "nominee_type"],
    "dim_genre": ["genre_key", "genre_name"],
    "dim_track": ["track_key", "spotify_track_id", "track_name", "artists",
                  "album_name", "genre_key", "genre_count", "explicit"],
    "fact_grammy_nomination": [
        "nomination_key", "nomination_bk", "year_key", "category_key", "track_key",
        "nominee", "grammy_artist", "match_status", "match_candidates",
        "spotify_matched", "nomination_count", "popularity", "duration_ms",
        "danceability", "energy", "valence", "acousticness", "loudness", "tempo",
        "batch_key",
    ],
    "fact_catalog_genre_baseline": [
        "genre_key", "track_count", "avg_popularity", "avg_danceability",
        "avg_energy", "avg_valence", "avg_acousticness", "avg_loudness", "batch_key",
    ],
}

# Load order respects foreign keys (dimensions first, facts last).
LOAD_ORDER = ["dim_year", "dim_category", "dim_genre", "dim_track",
              "fact_grammy_nomination", "fact_catalog_genre_baseline"]

UNKNOWN_KEY = -1

# ------------------------------------------------------------ controlled vocabularies
MUSIC_FAMILIES = [
    "General Field", "Pop", "Rock/Metal/Alternative", "Rap", "R&B/Soul",
    "Country/Bluegrass", "Folk/Blues/Americana", "Jazz", "Classical",
    "Latin", "Reggae/World", "Gospel/Christian", "Dance/Electronic",
    "Musical Theater/Soundtrack", "Children/Spoken/Comedy", "Music Video",
    "Production/Technical", "Other",
]
NOMINEE_TYPES = ["recording_or_work", "album", "person"]
COVERAGE_TYPES = ["winners_only", "all_nominees"]
MATCH_STATUSES = [
    "matched", "unmatched_no_title_candidate", "unmatched_artist_mismatch",
    "not_matchable_album_or_person", "not_matchable_missing_artist",
]
