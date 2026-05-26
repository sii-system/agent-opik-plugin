# OpenCode → Opik realtime tracing

OpenCode plugin that streams session activity to Opik. The TS plugin
(`opik-trace.ts`) acts only as a trigger — on each lifecycle event it spawns
the Python hook detached, and the hook reads OpenCode's `opencode.db` to emit
Opik traces / spans. Failures are swallowed (fail-open) so tracing never breaks
an OpenCode run.

## Layout

```
harness/opencode/
├── opik-trace.ts             OpenCode plugin; fires events into the hook
└── README.md                 You are here

src/sii_opik_plugin/opencode/
└── opencode_realtime_hook.py the tracer — reads opencode.db, emits Opik spans
```

## Prerequisites

- Node-capable `opencode` install
- Python 3 with `opik>=1.0.0`, `uuid6`, `socksio` (see `requirements.txt` at
  the repo root)
- An Opik backend reachable from wherever opencode runs

```
pip install -r requirements.txt
```

## Install

OpenCode loads plugins from `~/.config/opencode/plugin/*.ts`, so the plugin file
must live there. The Python hook can stay in this repo and be referenced by
absolute path via `OPENCODE_OPIK_HOOK_SCRIPT` — that env var controls what
`opik-trace.ts` spawns at runtime.

1. Copy the plugin into OpenCode's plugin directory:

   ```
   mkdir -p ~/.config/opencode/plugin
   cp harness/opencode/opik-trace.ts ~/.config/opencode/plugin/
   ```

2. Point the plugin at the in-repo Python hook (otherwise it defaults to
   `~/.config/opencode/plugin/opencode_realtime_hook.py`):

   ```
   export OPENCODE_OPIK_HOOK_SCRIPT=/ABSOLUTE/PATH/TO/sii-opik-plugin/src/sii_opik_plugin/opencode/opencode_realtime_hook.py
   ```

   To use a specific interpreter (e.g. a venv), also set
   `OPENCODE_OPIK_PYTHON=/path/to/python`.

3. Export your Opik credentials and project:

   ```
   export OPIK_URL_OVERRIDE="http://localhost:5173/api/"
   export OPIK_PROJECT_NAME="opencode-realtime"
   ```

4. Run `opencode` as usual — traces appear in Opik as the session progresses.

To disable tracing for a single run: `TRACE_TO_OPIK=false opencode ...`.

## Uninstall

```
rm ~/.config/opencode/plugin/opik-trace.ts
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPIK_PROJECT_NAME` | — | Target Opik project name |
| `OPIK_TRACE_NAME` | auto `opencode_trace_…` | Literal `trace.name` |
| `OPENCODE_OPIK_HOOK_SCRIPT` | `~/.config/opencode/plugin/opencode_realtime_hook.py` | Path to the Python hook spawned by the plugin |
| `OPENCODE_OPIK_PYTHON` | `python3` | Interpreter used to run the hook |
| `OPENCODE_DB_PATH` | auto-discovered | Override path to `opencode.db` |
| `TRACE_TO_OPIK` | `true` | Enable/disable tracing for a run |
| `OC_OPIK_DEBUG` | `false` | Verbose hook + plugin logging |
| `OC_OPIK_DRY_RUN` | `false` | Run the hook without sending to Opik |
| `OC_OPIK_MAX_TEXT_CHARS` | `20000` | Max characters captured per text field |

Opik credentials are picked up from the standard `OPIK_*` environment
variables.

## How it works

`opik-trace.ts` registers OpenCode hooks and fires a one-shot JSON payload over
stdin to the Python hook on each event:

| OpenCode event | Hook event |
|----------------|------------|
| `tool.execute.after` | `tool_complete` (throttled in Python) |
| `experimental.session.compacting` | `session_compacting` |
| `message.updated` (role=user) | `user_prompt` |
| `session.idle` | `session_idle` |
| `session.completed` | `session_end` |

The Python hook reads `opencode.db`, parses turns / tool calls, and upserts a
session-level trace plus per-turn / per-tool spans to Opik. State is persisted
under `~/.opencode/state/` (`opik_realtime_state.json`, lock, and
`opik_realtime.log`).

## Development

From the repo root:

```
pip install -r requirements.txt
pip install pytest
pytest tests/
```

`tests/conftest.py` puts `src/` on `sys.path` so tests can
`from sii_opik_plugin.opencode import opencode_realtime_hook` without the
package being pip-installed.
