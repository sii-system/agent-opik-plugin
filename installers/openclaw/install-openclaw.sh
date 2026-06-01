#!/usr/bin/env bash
# Manual installer for the OpenClaw → Opik realtime tracer.
#
# Builds the TS plugin and registers it with OpenClaw via `--link` so the
# bundled Python tracer (resolved from the repo root by dist/src/bridge.js)
# stays reachable. Opik credentials are not hardcoded — `config` prints the
# commands to run with your own key.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
# Harness assets (TS plugin, dist/, openclaw.plugin.json) live under harness/,
# not next to this installer — build/link/register all operate on this dir.
HARNESS_DIR="$REPO_ROOT/harness/openclaw"
REQUIREMENTS="$REPO_ROOT/requirements.txt"
PLUGIN_ID="openclaw-opik-tracer"

PYTHON_BIN="${OPENCLAW_OPIK_PYTHON:-python3}"
# Prefer the repo venv's interpreter as the plugin's pythonPath default.
if [[ -x "$REPO_ROOT/.venv/bin/python" ]]; then
  PYTHON_PATH_DEFAULT="$REPO_ROOT/.venv/bin/python"
else
  PYTHON_PATH_DEFAULT="$(command -v "$PYTHON_BIN" || echo python3)"
fi

STATE_DIR="$HOME/.openclaw/state"
LOG_FILE="$STATE_DIR/opik_tracer.log"
STATE_FILE="$STATE_DIR/opik_tracer_state.json"

usage() {
  cat <<EOF
Usage:
  $(basename "$0") <command>

Commands:
  install     deps note + build + register (then run 'config')
  deps        Install the Python dependencies (requirements.txt)
  build       npm install && npm run build (produces dist/)
  register    openclaw plugins install --link $HARNESS_DIR  (+ enable)
  config      Print the 'openclaw config set ...' commands to run
  uninstall   openclaw plugins uninstall $PLUGIN_ID (+ gateway restart)
  status      Show resolved paths, deps, build, and tracer state
  tail-log    Tail the tracer log
  clear       Reset tracer state + log (timestamped backups)

Examples:
  $(basename "$0") install
  $(basename "$0") config
  $(basename "$0") status
EOF
}

require_openclaw() {
  command -v openclaw >/dev/null 2>&1 || { echo "openclaw CLI not found on PATH" >&2; exit 1; }
}

# True when opik + uuid6 + socksio are all importable by PYTHON_BIN.
deps_present() {
  "$PYTHON_BIN" - <<'PY'
import importlib.util, sys
sys.exit(0 if all(importlib.util.find_spec(m) for m in ("opik", "uuid6", "socksio")) else 1)
PY
}

cmd_deps() {
  [[ -f "$REQUIREMENTS" ]] || { echo "requirements.txt not found: $REQUIREMENTS" >&2; exit 1; }
  "$PYTHON_BIN" -m pip install -r "$REQUIREMENTS"
}

# Install deps only if missing — called by `install` so it's a single step.
ensure_deps() {
  if deps_present; then
    echo "deps: already present for $PYTHON_BIN (skipping pip)"
  else
    echo "deps: installing from requirements.txt..."
    cmd_deps
  fi
}

cmd_build() {
  command -v npm >/dev/null 2>&1 || { echo "npm not found on PATH" >&2; exit 1; }
  ( cd "$HARNESS_DIR" && npm install && npm run build )
  echo "built: $HARNESS_DIR/dist/index.js"
}

cmd_register() {
  require_openclaw
  [[ -f "$HARNESS_DIR/dist/index.js" ]] || { echo "dist/ missing — run '$(basename "$0") build' first" >&2; exit 1; }
  openclaw plugins install --link "$HARNESS_DIR"
  openclaw config set "plugins.entries.$PLUGIN_ID.enabled" true
  echo "registered + enabled: $PLUGIN_ID"
  echo "Next: $(basename "$0") config   (set your Opik credentials)"
}

cmd_config() {
  cat <<EOF
# Set your Opik credentials, then restart the gateway:
openclaw config set plugins.entries.$PLUGIN_ID.config.opikApiKey      "<YOUR_KEY>"
openclaw config set plugins.entries.$PLUGIN_ID.config.opikWorkspace   "default"
openclaw config set plugins.entries.$PLUGIN_ID.config.opikProjectName "openclaw"
openclaw config set plugins.entries.$PLUGIN_ID.config.pythonPath      "$PYTHON_PATH_DEFAULT"
openclaw gateway restart
# Full config schema: $HARNESS_DIR/openclaw.plugin.json
EOF
}

cmd_install() {
  echo "==> deps"; ensure_deps
  echo "==> build"; cmd_build
  echo "==> register"; cmd_register
}

cmd_uninstall() {
  require_openclaw
  openclaw plugins uninstall "$PLUGIN_ID" || true
  openclaw gateway restart || true
}

cmd_status() {
  echo "repo_root:   $REPO_ROOT"
  echo "plugin_dir:  $HARNESS_DIR"
  echo "python:      $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
  echo "pythonPath:  $PYTHON_PATH_DEFAULT"
  echo "openclaw:    $(command -v openclaw 2>/dev/null || echo '<not found>')"
  echo "dist_built:  $([[ -f "$HARNESS_DIR/dist/index.js" ]] && echo true || echo false)"
  echo
  echo "deps:"
  "$PYTHON_BIN" - <<'PY'
import importlib.util
for name in ("opik", "uuid6", "socksio"):
    print(f"  {name}_installed={importlib.util.find_spec(name) is not None}")
PY
  echo
  echo "files:"
  for path in "$LOG_FILE" "$STATE_FILE"; do
    if [[ -f "$path" ]]; then
      echo "  $(basename "$path")=true size=$(wc -c < "$path" | tr -d ' ')"
    else
      echo "  $(basename "$path")=false"
    fi
  done
}

cmd_tail_log() {
  mkdir -p "$STATE_DIR"; touch "$LOG_FILE"; tail -f "$LOG_FILE"
}

cmd_clear() {
  mkdir -p "$STATE_DIR"
  local stamp; stamp="$(date '+%Y%m%d-%H%M%S')"
  for path in "$STATE_FILE" "$LOG_FILE"; do
    [[ -f "$path" ]] && cp "$path" "${path}.bak-${stamp}"
  done
  printf '{}\n' > "$STATE_FILE"; : > "$LOG_FILE"
  echo "cleared $STATE_FILE"; echo "cleared $LOG_FILE"; echo "backup suffix: .bak-${stamp}"
}

main() {
  case "${1:-}" in
    install)   cmd_install ;;
    deps)      cmd_deps ;;
    build)     cmd_build ;;
    register)  cmd_register ;;
    config)    cmd_config ;;
    uninstall) cmd_uninstall ;;
    status)    cmd_status ;;
    tail-log)  cmd_tail_log ;;
    clear)     cmd_clear ;;
    -h|--help|help|"") usage ;;
    *) echo "unknown command: $1" >&2; usage; exit 1 ;;
  esac
}

main "$@"
