# Claude Code → Opik realtime tracing

Manual install. Subscribes 8 Claude Code lifecycle events to the realtime tracer
at `src/sii_opik_plugin/claude_code/claude_realtime_trace.py`.

## Install

1. Pick a scope for the hooks:
   - **User scope** (all projects): `~/.claude/settings.json`
   - **Project scope** (this checkout only): `<project>/.claude/settings.json`

2. Open `settings.example.json` in this directory. Copy its `"hooks"` block
   into your chosen `settings.json`. If `settings.json` already has a `"hooks"`
   key, merge the event arrays — Claude Code runs every entry registered for an
   event, so additions don't conflict with existing hooks.

3. Replace every occurrence of `/ABSOLUTE/PATH/TO/sii-opik-plugin` with the
   absolute path to your clone of this repo. Example:

   ```
   /ABSOLUTE/PATH/TO/sii-opik-plugin
   →
   /Users/you/code/sii-opik-plugin
   ```

4. Install Python deps so the hook can import `opik`:

   ```
   pip install -r requirements.txt
   ```

5. Restart your Claude Code session. New sessions pick up the hooks on launch.

## Uninstall

Delete the 8 event entries from your `settings.json`.

## Notes

- `SessionEnd` is wrapped in a `bash -lc` one-liner that spools stdin to a
  temp file and re-invokes the hook with `--payload-file` under `nohup`. This
  detaches trace finalization from the hook process so a cancellation by the
  Claude Code runner on shutdown doesn't truncate the trace.
- The hook script reads its event payload from stdin and the event name from
  argv[1]. No env vars are required by the hook itself; Opik credentials are
  picked up from the standard `OPIK_*` environment variables.

## Development

From the repo root:

```
pip install -r requirements.txt
pip install pytest
pytest tests/
```

`tests/conftest.py` puts `src/` on `sys.path` so the tests can
`from sii_opik_plugin.claude_code import claude_realtime_trace` without
the package being pip-installed.
