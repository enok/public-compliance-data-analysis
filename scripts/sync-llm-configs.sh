#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TOOLKIT_ROOT="$(python3 - "$REPO_ROOT/.agents" <<'PY'
from pathlib import Path
import sys

agents = Path(sys.argv[1])
if not agents.exists():
    raise SystemExit(f"Missing .agents link at {agents}")

print(agents.resolve().parent)
PY
)"

SYNC_SCRIPT="$TOOLKIT_ROOT/scripts/sync-tool-configs.sh"

if [[ ! -f "$SYNC_SCRIPT" ]]; then
  for candidate_root in "${DEV_TOOLS_ROOT:-}" "/mnt/hgfs/shared/dev-tools"; do
    [[ -n "$candidate_root" ]] || continue
    if [[ -f "$candidate_root/scripts/sync-tool-configs.sh" ]]; then
      SYNC_SCRIPT="$candidate_root/scripts/sync-tool-configs.sh"
      break
    fi
  done
fi

if [[ ! -f "$SYNC_SCRIPT" ]]; then
  echo "Could not locate scripts/sync-tool-configs.sh." >&2
  echo "Expected a symlinked .agents directory or DEV_TOOLS_ROOT to point at the shared toolkit." >&2
  exit 1
fi

exec bash "$SYNC_SCRIPT" "$REPO_ROOT"
