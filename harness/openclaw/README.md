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

From this directory (`harness/openclaw/`):

1. Build the TS plugin:
   ```
   npm install
   npm run build
   ```
   This produces `dist/index.js` and `dist/src/bridge.js`. The bridge
   resolves the Python tracer at
   `<repo>/src/sii_opik_plugin/openclaw/openclaw_opik_tracer.py`
   automatically — do not move `dist/` away from this directory.

2. Install the Python tracer's deps. From the repo root:
   ```
   pip install -r requirements.txt
   ```
   If you use a venv, note its `python` path — you'll point the plugin
   at it via `pythonPath` below.

3. Register the plugin with OpenClaw from the repo checkout:
   ```
   openclaw plugins install --link /ABSOLUTE/PATH/TO/sii-opik-plugin/harness/openclaw
   ```
   `--link` is required for this layout. The plugin directory contains the
   TS bridge, while the Python tracer lives at the repo root under
   `src/sii_opik_plugin/openclaw/`. A copied install of only
   `harness/openclaw/` will not include that tracer, so hooks will fail to
   spawn the Python script.

4. Configure it. The minimum is your Opik credentials:
   ```
   openclaw config set plugins.entries.openclaw-opik-tracer.enabled true
   openclaw config set plugins.entries.openclaw-opik-tracer.config.opikApiKey   "<YOUR_KEY>"
   openclaw config set plugins.entries.openclaw-opik-tracer.config.opikWorkspace "default"
   openclaw config set plugins.entries.openclaw-opik-tracer.config.opikProjectName "openclaw"
   openclaw config set plugins.entries.openclaw-opik-tracer.config.pythonPath  "/path/to/python"
   ```
   Full config schema is in `openclaw.plugin.json`.

5. Restart OpenClaw:
   ```
   openclaw gateway restart
   ```

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
openclaw plugins uninstall openclaw-opik-tracer
openclaw gateway restart
```

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
