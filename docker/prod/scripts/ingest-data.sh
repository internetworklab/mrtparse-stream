#!/bin/bash

set -euo pipefail

script_path=$(realpath $0)
script_dir=$(dirname $script_path)

cd "$script_dir/.."

DATA_DIR=${DATA_DIR:-$(realpath "${script_dir}/../../..")}

printf 'DATA_DIR=%s\n' "$DATA_DIR"

mkdir -p "$DATA_DIR"

run_ingest() {
  docker run \
    --network pgsql-prod \
    --pull never \
    --rm \
    -v ./.env:/app/.env:ro \
    -v "${DATA_DIR}:/app/data:ro" \
    -w /app \
    --entrypoint mrtparse-ingest \
    mrtparse:latest \
      --pg-user-env=PG_USER \
      --pg-pass-env=PG_PASSWORD \
      --pg-hostport-env=PG_HOSTPORT \
      --pg-dbname-env=PG_DBNAME \
      "$@"
}

echo "Downloading master4_latest.mrt.bz2..."
curl -k -L -o "${DATA_DIR}/master4_latest.mrt.bz2" --proxy http://192.168.250.2:1081 \
    https://mrt.collector.dn42/master4_latest.mrt.bz2

echo "Ingesting master4_latest.mrt.bz2..."
run_ingest --provider dn42-master4 --sink postgres /app/data/master4_latest.mrt.bz2

echo "Downloading master6_latest.mrt.bz2..."
curl -k -L -o "${DATA_DIR}/master6_latest.mrt.bz2" --proxy http://192.168.250.2:1081 \
    https://mrt.collector.dn42/master6_latest.mrt.bz2

echo "Ingesting master6_latest.mrt.bz2..."
run_ingest --provider dn42-master6 --sink postgres /app/data/master6_latest.mrt.bz2

echo "Downloading latest-bview.gz..."
curl -o "${DATA_DIR}/latest-bview.gz" -L https://data.ris.ripe.net/rrc00/latest-bview.gz

echo "Ingesting latest-bview.gz..."
run_ingest --provider ripe-ris-rrc0 --sink postgres /app/data/latest-bview.gz
