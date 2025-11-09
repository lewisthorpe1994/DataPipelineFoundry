#!/bin/bash
set -euo pipefail

export PGUSER="${POSTGRES_USER}"
export PGPASSWORD="${POSTGRES_PASSWORD}"

echo "Restoring dvdrental dataset into ${POSTGRES_DB}"
pg_restore --verbose --no-owner --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" /docker-entrypoint-initdb.d/dvdrental
