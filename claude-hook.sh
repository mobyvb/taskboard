#!/usr/bin/env bash
# Claude Code hook -> taskboard. Usage: claude-hook.sh <event_type>
# Reads the hook JSON from stdin, attaches PANE_LOC as metadata, posts the event.
set -uo pipefail

EVENT_TYPE="${1:-unknown}"
SERVER="${TASKBOARD_URL:-http://127.0.0.1:8723}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

input="$(cat)"

pane_loc="${PANE_LOC:-}"
if [[ -z "$pane_loc" ]]; then
  scripts_dir="${TASKBOARD_SCRIPTS_PATH:-$SCRIPT_DIR/scripts}"
  pane_loc="$("$scripts_dir/get-pane-location" 2>/dev/null || true)"
fi

jq -n \
  --arg type "$EVENT_TYPE" \
  --arg pane "$pane_loc" \
  --arg scope "${TASK_SCOPE:-}" \
  --argjson hook "$input" \
  '{
    event_type: $type,
    title: (if $scope != "" then $scope else ($hook.cwd // "" | split("/") | last) end),
    description: ($hook.message // ""),
    metadata: {
      pane_loc: $pane,
      session_id: ($hook.session_id // ""),
      cwd: ($hook.cwd // "")
    }
  }' | curl -sS -m 2 -X POST "$SERVER/api/events" \
        -H 'Content-Type: application/json' -d @- >/dev/null || true

exit 0
