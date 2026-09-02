# agent-opik-plugin

Realtime tracing of agent-harness sessions to [Opik](https://github.com/comet-ml/opik).
One Python tracer per harness lives under `src/sii_opik_plugin/<harness>/`, with
a thin install helper at `harness/<harness>/install-<harness>.sh`.

Supported harnesses: **claude-code**, **opencode**, **openclaw**.

## Install

Use the top-level dispatcher to install whichever harness(es) you want:

```
./install-plugin.sh install claude-code          # one
./install-plugin.sh install claude-code opencode # several
./install-plugin.sh install all                  # all three
./install-plugin.sh                              # interactive picker
```

Other commands fan out the same way:

```
./install-plugin.sh list                # harnesses + host-CLI presence + state
./install-plugin.sh status              # status for all (or name harnesses)
./install-plugin.sh uninstall opencode
./install-plugin.sh config openclaw     # forwarded to a harness's installer
```

`install` installs the Python deps (`requirements.txt`) only if they're missing,
then runs each harness's setup. After install:

- **claude-code** — restart Claude Code so new sessions load the hooks.
- **opencode** — export `OPIK_URL_OVERRIDE` + `OPIK_PROJECT_NAME`, then run `opencode`.
- **openclaw** — run `./install-plugin.sh config openclaw` and paste in your Opik API key.

Span write batching is opt-in to reduce Opik backend / ClickHouse load in
managed deployments. Set `OPIK_SPAN_BATCH_ENABLED=true` to batch span writes;
otherwise tracers use the legacy single-span create/update path. Batches are
capped at 5 spans per request by default; set `OPIK_SPAN_BATCH_SIZE` to tune
the cap.

Each harness can also be driven directly via its own
`harness/<name>/install-<name>.sh` (e.g. `install-claude.sh`,
`install-opencode.sh`, `install-openclaw.sh`); see each harness directory's
`README.md` for harness-specific details.

## Export traces

`export/export_opik_traces.py` pulls traces back **out** of an Opik project (the
inverse of the realtime tracer) and writes one JSON file per trace, shaped as a
flat span list `[root_trace, span1, span2, …]` — the format the downstream
converters (`opik_trace_to_codetracer.py`,
`convert_trace_to_session_parquet.py`) read.

```
python export/export_opik_traces.py \
  --out-dir ./out \
  [--project NAME] [--opik-url URL] [--workspace NAME] [--api-key KEY] \
  [--filter 'tags contains "tb-task"'] \
  [--max-results 1000] [--max-spans 5000] [--overwrite]
```

Target resolution (the script reads these itself and passes them to the client,
since the SDK only honors `OPIK_URL_OVERRIDE`, not plain `OPIK_URL`):

- **URL**: `--opik-url` → `$OPIK_URL` → error
- **project**: `--project` → `$OPIK_PROJECT_NAME` → error
- **workspace / api-key**: optional; fall back to `~/.opik.config` defaults.

The resolved `url / workspace / project` is logged before fetching. Output is
`<out-dir>/<trace_id>.json` per trace plus `<out-dir>/manifest.json` (header +
per-trace index). Existing files are skipped unless `--overwrite`.

Note: the exported JSON is the verbatim trace/span content. The SFT converter
consumes the `llm` spans directly; the CodeTracer converter expects tool calls
embedded as `tool_call` blocks in assistant messages, so traces that record tool
calls as separate `tool` spans need a flattening pass first.
