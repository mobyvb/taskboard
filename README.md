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
`-data ~/.taskboard` (persistence dir: `events.jsonl` + `panes.json`; events are
replayed on restart, pane state — ack/tab/etc. — is snapshotted separately).

## API

A pane is identified by `pane_loc` (falling back to `session_id` only for
events that somehow lack it) and is the persistent record for a tmux pane's
lifecycle: it tracks ack/unread state, tab assignment, and the most recent
session/notification, surviving across however many claude sessions come and
go in that pane.

| Endpoint | Description |
|---|---|
| `GET /api/files` | whitelisted files under `$TASK_QUEUE_PATH` (recursive) |
| `GET /api/file?path=REL` | file contents |
| `POST /api/events` | `{event_type, title, description, metadata}` — metadata is arbitrary JSON |
| `GET /api/events` | raw event log (retained events, newest last) |
| `GET /api/panes` | all panes (ack/tab/session state; not removed on `session_end`) |
| `DELETE /api/panes` | `{key}` — forget a pane entirely (including its tab assignment) |
| `POST /api/panes/ack` | `{key, unread}` — set a pane's ack state; global everywhere it's shown |
| `POST /api/panes/tab` | `{key, tab}` — assign a pane to a tab (`tab: ""` = Home) |
| `POST /api/panes/unack-all` | mark every pane unread again (undo a batch of acks) |
| `GET /api/stream` | SSE stream of new events (drives UI toasts/notifications) |
| `POST /api/goto` | `{target: "slug-@N-%N"}` — runs `goto-pane-location target` |
| `POST /api/pane/capture` | `{pane: "%N", lines?: N}` — pane contents; `lines` adds N lines of scrollback (`capture-pane -p -S -N`, capped) |
| `POST /api/pane/keys` | `{pane: "%N", text: "..."}` (literal) or `{pane: "%N", key: "Enter"}` (named key) |

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
