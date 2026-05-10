#!/bin/bash

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."

source .env
docker run \
  --network pgsql-prod \
  --rm \
  -e "PGPASSWORD=${PG_PASSWORD}" \
  -it postgres:18-trixie \
    psql -h "${PG_HOST}" -U "${PG_USER}" "${PG_DBNAME}"
