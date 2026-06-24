# taskboard

Local web UI for the task queue: shows whitelisted task files from
`$TASK_QUEUE_PATH`, tracks Claude Code worker events (in memory), and can jump
tmux focus to a worker's pane via `scripts/goto-pane-location`.

## Setup

Clone this repo anywhere, then set environment variables:

| Variable | Required | Description |
|---|---|---|
| `TASK_QUEUE_PATH` | yes | root directory of your task queue |
| `TASKBOARD_SCRIPTS_PATH` | for `go run` only | absolute path to the `scripts/` directory in this repo |

## Shell setup

Drop this into your `~/.zshrc` or `~/.bashrc` (edit the two paths). It sets the
env vars the server uses and auto-exports `PANE_LOC` for every new tmux pane, so
the UI's "goto" button can jump tmux focus to a worker without any manual steps:

```sh
# --- taskboard ---
export TASK_QUEUE_PATH=/path/to/tasks                    # root of your task queue
export TASKBOARD_SCRIPTS_PATH=/path/to/taskboard/scripts # helper scripts dir
# source set-pane-location on every new shell; it's a no-op outside tmux
source "$TASKBOARD_SCRIPTS_PATH/set-pane-location"
```

`PANE_LOC` is captured once per shell, so split/new panes get their own value
automatically. If you move a pane or rename its window, re-`source
set-pane-location` to refresh.

## Run

Build and run (scripts/ is auto-discovered next to the compiled binary):

```sh
cd /path/to/taskboard
go build -o taskboard .
TASK_QUEUE_PATH=/path/to/tasks ./taskboard   # http://127.0.0.1:8723
```

Or with `go run` (set `TASKBOARD_SCRIPTS_PATH` since there is no binary to locate scripts/ from):

```sh
cd /path/to/taskboard
TASK_QUEUE_PATH=/path/to/tasks TASKBOARD_SCRIPTS_PATH=/path/to/taskboard/scripts go run .
```

Flags: `-port 8723`, `-allow context.txt` (repeatable filename whitelist),
`-scripts /path/to/taskboard/scripts` (directory containing helper scripts; overridden by `TASKBOARD_SCRIPTS_PATH` env var),
`-goto-script PATH` (explicit path to `goto-pane-location`; overrides `-scripts`),
`-data ~/.taskboard` (persistence dir: `events.jsonl` + `read.txt`; events and the
read marker survive restarts, workers are rebuilt by replaying events).

## API

| Endpoint | Description |
|---|---|
| `GET /api/files` | whitelisted files under `$TASK_QUEUE_PATH` (recursive) |
| `GET /api/file?path=REL` | file contents |
| `POST /api/events` | `{event_type, title, description, metadata}` — metadata is arbitrary JSON |
| `GET /api/events` | `{events, last_read}` — events with id ≤ `last_read` are read |
| `POST /api/read` | `{id}` — mark events up to and including `id` as read |
| `GET /api/workers` | active workers (keyed by `metadata.session_id`/`pane_loc`; removed on `session_end`) |
| `GET /api/stream` | SSE stream of new events (drives UI toasts/notifications) |
| `POST /api/goto` | `{target: "slug-@N-%N"}` — runs `goto-pane-location target` |

## Claude Code hooks

`claude-hook.sh` (requires `jq`) reads the hook JSON from stdin and posts an
event with `pane_loc` metadata — taken from `$PANE_LOC` (set via
`scripts/set-pane-location`) or `scripts/get-pane-location`. The UI shows a
"goto" button next to any event/worker carrying `pane_loc`.

Add to `~/.claude/settings.json` (replace `/path/to/taskboard` with your clone location):

```json
{
  "hooks": {
    "Stop":         [{"hooks": [{"type": "command", "command": "/path/to/taskboard/claude-hook.sh stop"}]}],
    "Notification": [{"hooks": [{"type": "command", "command": "/path/to/taskboard/claude-hook.sh notification"}]}],
    "SessionStart": [{"hooks": [{"type": "command", "command": "/path/to/taskboard/claude-hook.sh session_start"}]}],
    "SessionEnd":   [{"hooks": [{"type": "command", "command": "/path/to/taskboard/claude-hook.sh session_end"}]}]
  }
}
```

Note: `PANE_LOC` is per-shell. With the [Shell setup](#shell-setup) snippet it is
exported automatically; otherwise the hook falls back to `scripts/get-pane-location`
(resolved next to `claude-hook.sh`, or via `TASKBOARD_SCRIPTS_PATH` if set).
