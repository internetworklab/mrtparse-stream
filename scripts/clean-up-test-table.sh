#!/bin/bash

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."


source .env
docker run \
  --network pgsql-test \
  --rm \
  -e "PGPASSWORD=${TEST_PG_PASSWORD}" \
  -v ./schema.sql:/schema.sql:ro \
  -it postgres:18-trixie \
    psql -v ON_ERROR_STOP=1 -d "${TEST_PG_DBNAME}" -h "${TEST_PG_HOST}" -U "${TEST_PG_USER}" \
      -c 'truncate table mrt_entries;'
