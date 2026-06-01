# sii-opik-plugin

Realtime tracing of agent-harness sessions to [Opik](https://github.com/comet-ml/opik).
One Python tracer per harness lives under `src/sii_opik_plugin/<harness>/`, with
a thin install helper at `installers/<harness>/install-<harness>.sh`.

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

Each harness can also be driven directly via its own
`installers/<name>/install-<name>.sh` (e.g. `install-claude.sh`,
`install-opencode.sh`, `install-openclaw.sh`); see each harness directory's
`README.md` for harness-specific details.
