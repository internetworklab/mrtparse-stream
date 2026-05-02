#!/bin/bash


set -euo pipefail

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."

echo "==> (Re)-Applying schema..."

source .env
docker run \
  --network pgsql-test \
  --rm \
  -e "PGPASSWORD=${TEST_PG_PASSWORD}" \
  -v ./schema.sql:/schema.sql:ro \
  -it postgres:18-trixie \
    psql -v ON_ERROR_STOP=1 -h "${TEST_PG_HOST}" -U "${TEST_PG_USER}" \
    -d postgres \
    -c "drop database if exists ${TEST_PG_DBNAME}" \
    -c "create database ${TEST_PG_DBNAME}"

docker run \
  --network pgsql-test \
  --rm \
  -e "PGPASSWORD=${TEST_PG_PASSWORD}" \
  -v ./schema.sql:/schema.sql:ro \
  -it postgres:18-trixie \
    psql -v ON_ERROR_STOP=1 -d "${TEST_PG_DBNAME}" -h "${TEST_PG_HOST}" -U "${TEST_PG_USER}" -f "/schema.sql" 
