#!/bin/bash
# Executed once by the official postgres image the first time the volume is created.
# Creates the two analytical databases used by the workshop.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE grammy_source;
    CREATE DATABASE music_dw;
EOSQL
