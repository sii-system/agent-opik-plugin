# Claude Code → Opik realtime tracing

Manual install. Subscribes 8 Claude Code lifecycle events to the realtime tracer
at `src/sii_opik_plugin/claude_code/claude_realtime_trace.py`.

## Install

Use `install-claude.sh` in this directory. It resolves the hook script by its own
location, so there is no `/ABSOLUTE/PATH/TO/...` placeholder to edit.

```
# One step: installs Python deps if missing, then merges the 8 hooks
./install-claude.sh install
```

Hooks go into `~/.claude/settings.json`, so tracing applies to every Claude
Code session. `install` first checks whether `opik`/`uuid6`/`socksio` are
importable and runs `pip` only if they're missing. The merge is
non-destructive: existing hooks and other settings keys are kept, and
re-running `install` won't duplicate entries. Restart your Claude Code session
afterwards — new sessions pick up the hooks on launch.

Other subcommands:

| Command | Purpose |
|---------|---------|
| `./install-claude.sh hooks` | Print the resolved hooks JSON to paste by hand |
| `./install-claude.sh status` | Show resolved paths, deps, and install state |
| `./install-claude.sh tail-log` | Tail `~/.claude/state/opik_hook.log` |
| `./install-claude.sh clear` | Reset hook state + log (backs up first) |

To pin a specific interpreter, set `CC_OPIK_PYTHON=/path/to/python` before
running. `settings.example.json` is kept as a manual-merge reference.

## Uninstall

```
./install-claude.sh uninstall
```

Removes only this tracer's entries from `~/.claude/settings.json` and prunes
emptied event arrays, leaving your other hooks intact.

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
