#!/usr/bin/env bash
set -euo pipefail

TMPDIR="$(mktemp -d)"
cd "$TMPDIR"

printf "Cd into temp dir: %s" "${TMPDIR}"

source "$HOME/.ingest/.env"

curl -k -L -O --proxy http://192.168.250.2:1081 \
    https://mrt.collector.dn42/master6_latest.mrt.bz2

/root/go/bin/ingest --provider="dn42-master6" --sink postgres ./master6_latest.mrt.bz2
