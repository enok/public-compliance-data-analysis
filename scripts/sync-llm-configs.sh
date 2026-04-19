#!/usr/bin/env bash
# sync-llm-configs.sh — regenerate AGENTS.md / CLAUDE.md / copilot-instructions.md
# Compatible with: Linux, macOS, Git Bash on Windows
# Requires: bash 4+ (macOS ships bash 3; use Homebrew bash or run via 'bash sync-llm-configs.sh')
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Locate the LLM toolkit root — try multiple strategies in order:
#   1. Sibling directory (most common setup: both repos share the same parent)
#   2. DEV_TOOLS_ROOT env var
#   3. python3 Path.resolve() on .agents (works on Linux/macOS with real symlinks)
#   4. Known hgfs/shared path (VM shared folder fallback)
# ---------------------------------------------------------------------------
TOOLKIT_ROOT=""

# 1. Sibling directory heuristic + common home locations (Linux, macOS, Windows Git Bash)
for candidate in \
    "$REPO_ROOT/../llm-toolkit-project" \
    "$REPO_ROOT/../llm-toolkit" \
    "$HOME/llm-toolkit-project" \
    "$HOME/llm-toolkit" \
    "$HOME/Developer/llm-toolkit-project" \
    "$HOME/Developer/llm-toolkit" \
    "$HOME/Projects/llm-toolkit-project" \
    "$HOME/Projects/llm-toolkit"; do
  if [[ -f "$candidate/scripts/sync-tool-configs.sh" ]]; then
    TOOLKIT_ROOT="$(cd "$candidate" && pwd)"
    break
  fi
done

# 2. DEV_TOOLS_ROOT env var
if [[ -z "$TOOLKIT_ROOT" && -n "${DEV_TOOLS_ROOT:-}" ]]; then
  if [[ -f "$DEV_TOOLS_ROOT/scripts/sync-tool-configs.sh" ]]; then
    TOOLKIT_ROOT="$(cd "$DEV_TOOLS_ROOT" && pwd)"
  fi
fi

# 3. python3 Path.resolve() on .agents (real symlinks on Linux/macOS only)
if [[ -z "$TOOLKIT_ROOT" ]] && command -v python3 &>/dev/null; then
  TOOLKIT_ROOT="$(python3 - "$REPO_ROOT/.agents" 2>/dev/null <<'PY' || true
from pathlib import Path
import sys
agents = Path(sys.argv[1])
if agents.exists():
    resolved = agents.resolve().parent
    if (resolved / "scripts" / "sync-tool-configs.sh").exists():
        print(resolved)
PY
  )"
fi

# 4. Known hgfs shared folder path (VM fallback)
if [[ -z "$TOOLKIT_ROOT" ]]; then
  for candidate in "/mnt/hgfs/shared/data-science/tcc/code/llm-toolkit-project" "/mnt/hgfs/shared/dev-tools"; do
    if [[ -f "$candidate/scripts/sync-tool-configs.sh" ]]; then
      TOOLKIT_ROOT="$candidate"
      break
    fi
  done
fi

if [[ -z "$TOOLKIT_ROOT" ]]; then
  echo "Could not locate the LLM toolkit (sync-tool-configs.sh not found)." >&2
  echo "Tried:" >&2
  echo "  - Sibling of this repo: $REPO_ROOT/../llm-toolkit-project" >&2
  echo "  - \$HOME/llm-toolkit-project and \$HOME/llm-toolkit" >&2
  echo "  - \$DEV_TOOLS_ROOT (if set)" >&2
  echo "  - python3 Path.resolve() on .agents" >&2
  echo "  - /mnt/hgfs/shared/data-science/tcc/code/llm-toolkit-project" >&2
  echo "" >&2
  echo "Fix: set DEV_TOOLS_ROOT=/path/to/llm-toolkit-project or place the toolkit" >&2
  echo "     as a sibling directory next to this repo." >&2
  exit 1
fi

SYNC_SCRIPT="$TOOLKIT_ROOT/scripts/sync-tool-configs.sh"

exec bash "$SYNC_SCRIPT" "$REPO_ROOT"
