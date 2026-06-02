#!/usr/bin/env bash
# Manual installer for the OpenCode → Opik realtime tracer.
#
# OpenCode auto-loads plugins from ~/.config/opencode/plugins/. This copies the
# TS plugin and the Python hook into that directory so the install is
# self-contained (no OPENCODE_OPIK_HOOK_SCRIPT needed). Set that env var before
# `install` to vendor a different hook source instead.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
# Harness assets (the TS plugin) live under harness/, not next to this installer.
HARNESS_DIR="$REPO_ROOT/harness/opencode"
REQUIREMENTS="$REPO_ROOT/requirements.txt"
PYTHON_BIN="${OPENCODE_OPIK_PYTHON:-python3}"

PLUGIN_SRC="$HARNESS_DIR/opik-trace.ts"
# Match the env var the plugin reads at runtime (opik-trace.ts). Setting it
# changes both what `install` copies and what OpenCode executes.
HOOK_SRC="${OPENCODE_OPIK_HOOK_SCRIPT:-$REPO_ROOT/src/sii_opik_plugin/opencode/opencode_realtime_trace.py}"

PLUGIN_DIR="$HOME/.config/opencode/plugins"
PLUGIN_DST="$PLUGIN_DIR/opik-trace.ts"
HOOK_DST="$PLUGIN_DIR/opencode_realtime_trace.py"

STATE_DIR="$HOME/.opencode/state"
LOG_FILE="$STATE_DIR/opik_realtime.log"
STATE_FILE="$STATE_DIR/opik_realtime_state.json"
PLUGIN_LOG="$STATE_DIR/opik_plugin.log"

usage() {
  cat <<EOF
Usage:
  $(basename "$0") <command>

Commands:
  install     Install deps if missing, then copy plugin + hook into $PLUGIN_DIR
  uninstall   Remove plugin + hook from $PLUGIN_DIR
  deps        Force-(re)install the Python dependencies (requirements.txt)
  status      Show resolved paths, deps, and install state
  tail-log    Tail the hook log
  clear       Reset hook state + logs (timestamped backups)

Examples:
  $(basename "$0") install
  $(basename "$0") status
  $(basename "$0") tail-log
EOF
}

cmd_install() {
  [[ -f "$PLUGIN_SRC" ]] || { echo "plugin source not found: $PLUGIN_SRC" >&2; exit 1; }
  [[ -f "$HOOK_SRC" ]]   || { echo "hook source not found: $HOOK_SRC" >&2; exit 1; }
  ensure_deps
  mkdir -p "$PLUGIN_DIR" "$STATE_DIR"
  install -m 0644 "$PLUGIN_SRC" "$PLUGIN_DST"
  install -m 0755 "$HOOK_SRC"   "$HOOK_DST"
  echo "installed: $PLUGIN_DST"
  echo "installed: $HOOK_DST"
  echo "Export your OPIK_* credentials and OPIK_PROJECT_NAME, then run opencode."
}

cmd_uninstall() {
  local removed=0
  for path in "$PLUGIN_DST" "$HOOK_DST"; do
    if [[ -f "$path" ]]; then rm -f "$path"; echo "removed: $path"; removed=1; fi
  done
  (( removed == 0 )) && echo "(nothing to remove)" || true
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

cmd_status() {
  echo "repo_root:  $REPO_ROOT"
  echo "plugin_src: $PLUGIN_SRC"
  echo "hook_src:   $HOOK_SRC"
  echo "plugin_dst: $PLUGIN_DST"
  echo "hook_dst:   $HOOK_DST"
  echo "python:     $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
  echo
  echo "deps:"
  "$PYTHON_BIN" - <<'PY'
import importlib.util
for name in ("opik", "uuid6", "socksio"):
    print(f"  {name}_installed={importlib.util.find_spec(name) is not None}")
PY
  echo
  echo "files:"
  for label_path in \
    "plugin_installed:$PLUGIN_DST" \
    "hook_installed:$HOOK_DST" \
    "log_exists:$LOG_FILE" \
    "plugin_log_exists:$PLUGIN_LOG" \
    "state_exists:$STATE_FILE"; do
    label="${label_path%%:*}"; path="${label_path#*:}"
    if [[ -f "$path" ]]; then
      printf "  %s=true size=%s\n" "$label" "$(wc -c < "$path" | tr -d ' ')"
    else
      printf "  %s=false\n" "$label"
    fi
  done
}

cmd_tail_log() {
  mkdir -p "$STATE_DIR"; touch "$LOG_FILE"; tail -f "$LOG_FILE"
}

cmd_clear() {
  mkdir -p "$STATE_DIR"
  local stamp; stamp="$(date '+%Y%m%d-%H%M%S')"
  for path in "$STATE_FILE" "$LOG_FILE" "$PLUGIN_LOG"; do
    [[ -f "$path" ]] && cp "$path" "${path}.bak-${stamp}"
  done
  printf '{}\n' > "$STATE_FILE"; : > "$LOG_FILE"; : > "$PLUGIN_LOG"
  echo "cleared $STATE_FILE"; echo "cleared $LOG_FILE"; echo "cleared $PLUGIN_LOG"
  echo "backup suffix: .bak-${stamp}"
}

main() {
  case "${1:-}" in
    install)   cmd_install ;;
    uninstall) cmd_uninstall ;;
    deps)      cmd_deps ;;
    status)    cmd_status ;;
    tail-log)  cmd_tail_log ;;
    clear)     cmd_clear ;;
    -h|--help|help|"") usage ;;
    *) echo "unknown command: $1" >&2; usage; exit 1 ;;
  esac
}

main "$@"
