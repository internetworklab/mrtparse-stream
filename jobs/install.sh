#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_NAME="ingest-mrt-data"

systemctl link "${SCRIPT_DIR}/${UNIT_NAME}.service"
systemctl link "${SCRIPT_DIR}/${UNIT_NAME}.timer"
systemctl enable --now "${UNIT_NAME}.timer"

echo "Timer '${UNIT_NAME}' installed and started."
systemctl status "${UNIT_NAME}.timer" --no-pager
