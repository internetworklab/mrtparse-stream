#!/bin/bash


set -euo pipefail

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."

echo "==> (Re)-Applying schema..."

source .env
docker run \
  --network pgsql-prod \
  --rm \
  -e "PGPASSWORD=${PG_PASSWORD}" \
  -v ../../schema.sql:/schema.sql:ro \
  -it postgres:18-trixie \
    psql -v ON_ERROR_STOP=1 -h "${PG_HOST}" -U "${PG_USER}" \
    -d postgres \
    -c "drop database if exists ${PG_DBNAME}" \
    -c "create database ${PG_DBNAME}"

docker run \
  --network pgsql-prod \
  --rm \
  -e "PGPASSWORD=${PG_PASSWORD}" \
  -v ../../schema.sql:/schema.sql:ro \
  -it postgres:18-trixie \
    psql -v ON_ERROR_STOP=1 -d "${PG_DBNAME}" -h "${PG_HOST}" -U "${PG_USER}" -f "/schema.sql"
