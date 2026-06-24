# taskboard

Local web UI for the task queue: shows whitelisted task files from
`$TASK_QUEUE_PATH`, tracks Claude Code worker events (in memory), and can jump
tmux focus to a worker's pane via `scripts/goto-pane-location`.

## Run

Build and run (scripts/ is auto-discovered next to the binary):

```sh
cd ~/dev/tasks/taskboard-repo/taskboard
go build -o taskboard .
TASK_QUEUE_PATH=~/dev/tasks ./taskboard   # http://127.0.0.1:8723
```

Or with `go run` (set `UTILS_PATH` since there is no binary to locate scripts/ from):

```sh
cd ~/dev/tasks/taskboard-repo/taskboard
TASK_QUEUE_PATH=~/dev/tasks UTILS_PATH=~/dev/tasks/taskboard-repo/taskboard/scripts go run .
```

Flags: `-port 8723`, `-allow context.txt` (repeatable filename whitelist),
`-utils ./scripts` (directory containing helper scripts; overridden by `UTILS_PATH` env var),
`-goto-script PATH` (explicit path to `goto-pane-location`; overrides `-utils`),
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

Add to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "Stop":         [{"hooks": [{"type": "command", "command": "~/dev/tasks/taskboard-repo/taskboard/claude-hook.sh stop"}]}],
    "Notification": [{"hooks": [{"type": "command", "command": "~/dev/tasks/taskboard-repo/taskboard/claude-hook.sh notification"}]}],
    "SessionStart": [{"hooks": [{"type": "command", "command": "~/dev/tasks/taskboard-repo/taskboard/claude-hook.sh session_start"}]}],
    "SessionEnd":   [{"hooks": [{"type": "command", "command": "~/dev/tasks/taskboard-repo/taskboard/claude-hook.sh session_end"}]}]
  }
}
```

Note: `PANE_LOC` is per-shell, so run `. scripts/set-pane-location` (or export
it) in the pane before starting `claude`, otherwise the hook falls back to
`scripts/get-pane-location`. Set `UTILS_PATH` to the `scripts/` directory if
running `claude-hook.sh` from outside the repo.
