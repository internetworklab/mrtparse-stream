#!/bin/bash

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."

source .env
docker run \
  --network pgsql-test \
  --rm \
  -e "PGPASSWORD=${TEST_PG_PASSWORD}" \
  -it postgres:18-trixie \
    psql -h "${TEST_PG_HOST}" -U "${TEST_PG_USER}" "${TEST_PG_DBNAME}"

