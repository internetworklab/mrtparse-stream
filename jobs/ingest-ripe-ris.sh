#!/usr/bin/env bash
set -euo pipefail

TMPDIR="$(mktemp -d)"
cd "$TMPDIR"

printf "Cd into temp dir: %s" "${TMPDIR}"

source "$HOME/.ingest/.env"

curl -O -L https://data.ris.ripe.net/rrc00/latest-bview.gz

/root/go/bin/ingest --provider="ripe-ris" --sink postgres ./latest-bview.gz
