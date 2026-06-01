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
└── opencode_realtime_trace.py the tracer — reads opencode.db, emits Opik spans
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

OpenCode auto-loads plugins from `~/.config/opencode/plugins/`. Use `install-opencode.sh`
from `installers/opencode/` — it copies both the TS plugin and the Python hook
there, so the install is self-contained (no `OPENCODE_OPIK_HOOK_SCRIPT` needed).

```
# from installers/opencode/ — one step: installs deps if missing, then copies
# opik-trace.ts + opencode_realtime_trace.py into ~/.config/opencode/plugins/
./install-opencode.sh install
```

Then export your Opik credentials and project, and run `opencode`:

```
export OPIK_URL_OVERRIDE="http://localhost:5173/api/"
export OPIK_PROJECT_NAME="opencode-realtime"
opencode    # traces appear in Opik as the session progresses
```

To vendor a different hook source instead of the in-repo one, set
`OPENCODE_OPIK_HOOK_SCRIPT=/path/to/hook.py` before `install`; to pin an
interpreter, set `OPENCODE_OPIK_PYTHON=/path/to/python`. Disable tracing for a
single run with `TRACE_TO_OPIK=false opencode ...`.

Other subcommands:

| Command | Purpose |
|---------|---------|
| `./install-opencode.sh status` | Show resolved paths, deps, and install state |
| `./install-opencode.sh tail-log` | Tail `~/.opencode/state/opik_realtime.log` |
| `./install-opencode.sh clear` | Reset hook state + logs (backs up first) |

## Uninstall

```
./install-opencode.sh uninstall
```

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPIK_PROJECT_NAME` | — | Target Opik project name |
| `OPIK_TRACE_NAME` | auto `opencode_trace_…` | Literal `trace.name` |
| `OPENCODE_OPIK_HOOK_SCRIPT` | `~/.config/opencode/plugins/opencode_realtime_trace.py` | Path to the Python hook spawned by the plugin |
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
`from sii_opik_plugin.opencode import opencode_realtime_trace` without the
package being pip-installed.
