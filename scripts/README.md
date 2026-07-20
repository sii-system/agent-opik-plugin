# Opik trace export, import, and replay

The scripts in this directory support three related workflows:

1. Export one trace from an Opik project.
2. Import an exported trace into another Opik project.
3. Replay the exported OpenAI-compatible LLM requests against a target model
   service.

Run the examples below from the repository root. They use reserved example domains and placeholder identifiers. Do not commit real API keys, internal URLs, project IDs, or trace IDs.

## Dependencies

Trace export and import use `uvx`. The migration script creates an isolated environment containing the pinned Opik CLI and SOCKS support, so no project virtual environment is required:

```bash
uvx --version
```

Trace replay uses Bash, `curl`, and `jq`. The replay script checks `curl` and `jq` before running and, when one is missing, installs it through Homebrew, apt, dnf, or yum. Automatic installation can be disabled:

```bash
AUTO_INSTALL_DEPS=0 bash scripts/replay_opik_trace.sh --help
```

`curl` handles HTTP, HTTPS, and SOCKS proxy environment variables directly; the replay path does not require Python or `socksio`.

## Export one trace

Provide the source Opik API URL, trace ID, source project name, and an output directory:

```bash
SOURCE_URL='https://opik-source.example/api/' \
TRACE_ID='trace-id-to-export' \
PROJECT='source-project-name' \
EXPORT_DIR='trace-export' \
bash scripts/migrate_opik_trace.sh export
```

The export directory contains a manifest, project metadata, and a trace JSON file. Its generic layout is:

```text
trace-export/
└── default/
    └── projects/
        └── PROJECT_ID/
            ├── project.json
            ├── export_manifest.db
            └── trace_TRACE_ID.json
```

Keep the whole export directory if it may later be imported. Only the `trace_*.json` file is required for LLM replay.

## Import an exported trace

Set `PROJECT` to the source project name represented by the export and `TARGET_PROJECT` to the project name that should receive the trace:

```bash
DEST_URL='https://opik-destination.example/api/' \
PROJECT='source-project-name' \
TARGET_PROJECT='destination-project-name' \
EXPORT_DIR='trace-export' \
bash scripts/migrate_opik_trace.sh import
```

## Inspect an exported trace before replay

Replay is dry-run by default. It discovers LLM spans, sorts them by capture time, and prints the round number, accumulated message count, streaming mode, effective model, captured status, and span ID without making network requests:

```bash
TRACE_FILE='trace-export/default/projects/PROJECT_ID/trace_TRACE_ID.json'
bash scripts/replay_opik_trace.sh "$TRACE_FILE"
```

## Replay the complete multi-round sequence

The authorization value in an Opik export may be redacted. Supply a current token through an environment variable and explicitly add `--execute`:

```bash
export OPENAI_API_KEY='replace-with-current-token'

bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example'
```

`--base-url` selects the target service. Pass the origin—scheme, host, and optional port—without the captured request path. The script appends the path stored in each span, normally `/v1/chat/completions`.

By default, every replay request keeps the model name captured in its request body. Use `--model` to replay the same requests against another model served by the selected base URL:

```bash
bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example' \
  --model 'target-model-name'
```

This changes only the request body's `model` field. The captured messages, tools, tool choice, streaming setting, and other generation parameters remain unchanged.

## Authentication and custom headers

`OPENAI_API_KEY` is sent as `Authorization: Bearer ...` by default. To use a different environment variable:

```bash
export REPLAY_API_KEY='replace-with-current-token'

bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example' \
  --api-key-env REPLAY_API_KEY
```

For a nonstandard or already-prefixed authorization value, disable the default Bearer header and map a header from an environment variable:

```bash
export REPLAY_AUTHORIZATION='Bearer replace-with-current-token'

bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example' \
  --api-key-env '' \
  --header-env Authorization=REPLAY_AUTHORIZATION
```

Non-secret headers may be supplied directly with repeatable `--header NAME=VALUE` options. Prefer `--header-env` for secrets so they do not appear in the process command line.

## Session affinity and repeated replay

The replay script creates a fresh session ID by default and uses it for both `X-Session-ID` and `X-Session-Affinity` across the selected sequence. Session behavior can be changed with one of these mutually exclusive options:

```text
--session-id ID             use a caller-provided ID
--reuse-original-session    reuse the ID stored in the trace
--no-session-headers        omit both session headers
```

Examples:

```bash
# Replay the full sequence five times with a one-second inter-request delay
bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example' \
  --repeat 5 \
  --delay 1

# Repeatedly replay only requests that failed in the original trace
bash scripts/replay_opik_trace.sh "$TRACE_FILE" \
  --execute \
  --base-url 'https://llm-gateway.example' \
  --only-errors \
  --repeat 10
```

## Replay semantics and results

Each LLM span contains the complete request snapshot for that round, including all messages accumulated up to that point. The script sends those snapshots in timestamp order. It does not insert newly generated replay responses into later requests, because doing that would change the captured prompt sizes and server workload.

Unless `--output-dir` is supplied, each execution creates a timestamped directory under `replay-results/` containing:

- `summary.jsonl`: request URL, model, round, HTTP status, duration, TTFT,
  detected error, and response filename;
- `*.response`: the complete raw JSON or SSE response for each request.

A request is marked failed when it receives HTTP 4xx/5xx, an SSE error object, a curl/network error, or a streaming response that closes without `data: [DONE]`. The last condition is important for failures that occur after the server has already returned HTTP 200.

Use `--stop-on-error` to stop at the first replay failure. Without it, later captured requests are still replayed because each request snapshot is self-contained. The script exits with status `2` if any replay request failed.

Run the built-in CLI help for the complete option list:

```bash
bash scripts/replay_opik_trace.sh --help
```
