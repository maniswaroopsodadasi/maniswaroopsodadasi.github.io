#!/usr/bin/env bash
# Run one publish cycle from this folder (loads .env if present).
set -euo pipefail
cd "$(dirname "$0")"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
exec python3 full_automation_system.py --once
