#!/usr/bin/env bash
# One-shot push when HTTPS credentials are not in Keychain (e.g. Cursor automation).
# Usage:
#   export GITHUB_TOKEN=ghp_xxxx   # classic PAT: repo scope
#   ./push_with_token.sh
# Or put GITHUB_TOKEN in .env (same folder; never commit .env)

set -euo pipefail
cd "$(dirname "$0")"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

if [[ -z "${GITHUB_TOKEN:-}" ]]; then
  echo "Missing GITHUB_TOKEN."
  echo "Create a classic Personal Access Token (repo scope) at:"
  echo "  https://github.com/settings/tokens"
  echo "Then either:"
  echo "  export GITHUB_TOKEN=ghp_xxxx"
  echo "  ./push_with_token.sh"
  echo "Or add GITHUB_TOKEN=... to .env in this folder."
  exit 1
fi

REPO="maniswaroopsodadasi/maniswaroopsodadasi.github.io"
exec git push -u "https://x-access-token:${GITHUB_TOKEN}@github.com/${REPO}.git" main
