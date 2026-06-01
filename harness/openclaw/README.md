# OpenClaw → Opik realtime tracing

OpenClaw plugin that streams session activity to Opik. Hooks act only as
triggers — the actual data comes from the session JSONL transcript that
OpenClaw writes under `~/.openclaw/agents/<agent>/sessions/<id>.jsonl`,
which avoids hook payload races (missing `sessionKey`, premature cleanup,
concurrent overwrites).

## Layout

```
harness/openclaw/
├── index.ts                       TS plugin entry; registers hooks
├── src/bridge.ts                  Spawns the Python tracer
├── openclaw.plugin.json           Plugin manifest + config schema
├── package.json / tsconfig.json   Build config
└── README.md                      You are here

src/sii_opik_plugin/openclaw/
└── openclaw_opik_tracer.py        Python tracer (incremental JSONL parser
                                   → Opik traces / spans)
```

## Prerequisites

- Node.js ≥ 22, npm
- Python 3 with `opik>=1.0.0` (see `requirements.txt` at the repo root)
- A working `openclaw` install
- Opik API key + workspace

## Install

Use `install-openclaw.sh` from `installers/openclaw/`. It runs the deps + build +
register steps and resolves the plugin path under `harness/openclaw/` (no
`/ABSOLUTE/PATH/TO/...`).

```
# from installers/openclaw/
./install-openclaw.sh install    # deps (if missing) + npm build + openclaw plugins install --link (+ enable)
./install-openclaw.sh config     # prints the 'openclaw config set ...' commands to run
```

`install` registers with `--link`, which is required for this layout: the
plugin directory holds the TS bridge while the Python tracer lives at the repo
root under `src/sii_opik_plugin/openclaw/`. The built `dist/src/bridge.js`
resolves that tracer automatically — don't move `dist/` away from here.

`config` prints the credential commands prefilled with this repo's
`.venv/bin/python` (if present) as `pythonPath`; paste your Opik key in and
run them, ending with `openclaw gateway restart`. Full config schema is in
`openclaw.plugin.json`.

Run the steps individually with `./install-openclaw.sh deps | build | register`, and
`./install-openclaw.sh status` to check what's in place. Override the interpreter for
deps with `OPENCLAW_OPIK_PYTHON=/path/to/python`.

## Verify

Run any OpenClaw session, then check:

- The Opik project for a new trace, with turn / tool / sub-agent spans.
- The tracer log if traces don't show up:
  ```
  tail -f ~/.openclaw/state/opik_tracer.log
  ```
- Local state lives at `~/.openclaw/state/opik_tracer_state.json`.

## Uninstall

```
./install-openclaw.sh uninstall
```

(Runs `openclaw plugins uninstall openclaw-opik-tracer` + `openclaw gateway restart`.)

## How it works

On every registered hook, `index.ts` collects session context (agent
id, session id, transcript path) and spawns
`src/sii_opik_plugin/openclaw/openclaw_opik_tracer.py` detached,
piping the event JSON over stdin. The Python tracer:

1. Reads the session JSONL transcript from `committed_offset` (resumable).
2. Parses turns / LLM calls / tool calls / sub-agents.
3. Upserts a session-level trace + per-turn / per-tool / per-subagent spans
   to Opik.
4. Persists offsets and accumulators to `opik_tracer_state.json` under a
   two-phase commit so a crash mid-flush doesn't double-emit or lose turns.

Hooks subscribed: `session_start`, `session_end`, `before_reset`,
`before_agent_start`, `before_agent_reply`, `llm_input`, `llm_output`,
`before_tool_call`, `after_tool_call`, `before_compaction`,
`after_compaction`, `agent_end`, `subagent_spawning`,
`subagent_delivery_target`, `subagent_spawned`, `subagent_ended`.

## Development

From the repo root:

```
pip install -r requirements.txt
pip install pytest
pytest tests/test_openclaw_opik_tracer.py
```

`tests/conftest.py` puts `src/` on `sys.path` so the tests import
`sii_opik_plugin.openclaw.openclaw_opik_tracer` without the
package being pip-installed.

For TS changes, `npm run dev` runs `tsc --watch` against `harness/openclaw/`.

## Troubleshooting

- **Plugin installs but no Opik data**: check `pythonPath` is correct,
  that python has `opik` installed, and `~/.openclaw/state/opik_tracer.log`
  for stack traces.
- **`python exited 1` in OpenClaw logs**: usually missing `opik` package on
  the configured `pythonPath`. Install it into that interpreter.
- **Want to test parsing without hitting Opik**: set
  `plugins.entries.openclaw-opik-tracer.config.dryRun true` — spans get
  logged to the tracer log instead of sent.
- **Want full conversation history in Opik input fields**: set
  `config.includeHistory true`. Off by default to keep payloads small.

## Plugin metadata

- id: `openclaw-opik-tracer`
- entry: `dist/index.js`
- `pluginApi`: `>=2026.4.8`
- `minGatewayVersion`: `2026.4.8`

If your OpenClaw is older than that, upgrade OpenClaw first or the
plugin will refuse to load.
