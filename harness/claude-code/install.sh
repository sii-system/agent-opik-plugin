#!/usr/bin/env bash
# Manual installer for the Claude Code → Opik realtime tracer.
#
# Resolves the in-repo hook script by its own location, so there is no
# /ABSOLUTE/PATH/TO/... placeholder to hand-edit. Can either print the
# resolved hooks JSON for you to paste, or merge it directly into a
# Claude Code settings.json (user or project scope) with a backup.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
HOOK_SCRIPT="${CC_OPIK_HOOK_SCRIPT:-$REPO_ROOT/src/sii_opik_plugin/claude_code/claude_realtime_trace.py}"
PYTHON_BIN="${CC_OPIK_PYTHON:-python3}"
REQUIREMENTS="$REPO_ROOT/requirements.txt"

USER_SETTINGS="$HOME/.claude/settings.json"
PROJECT_SETTINGS="$PWD/.claude/settings.json"

STATE_DIR="$HOME/.claude/state"
LOG_FILE="$STATE_DIR/opik_hook.log"
STATE_FILE="$STATE_DIR/opik_hook_state.json"

usage() {
  cat <<EOF
Usage:
  $(basename "$0") <command> [--user | --project]

Commands:
  hooks                 Print the resolved Claude Code hooks JSON to paste
  install   [scope]     Merge the hooks into a settings.json (backup first)
  uninstall [scope]     Remove this tracer's hooks from a settings.json
  deps                  Install the Python dependencies (requirements.txt)
  status    [scope]     Show resolved paths, deps, and whether hooks are installed
  tail-log              Tail the hook log
  clear                 Reset hook state + log (timestamped backups)

Scope (for install/uninstall/status), default --user:
  --user                $USER_SETTINGS
  --project             $PROJECT_SETTINGS

Examples:
  $(basename "$0") install --user
  $(basename "$0") hooks
  $(basename "$0") status --project
EOF
}

# Resolve the target settings.json from a scope flag.
target_settings() {
  case "${1:-}" in
    --project) printf '%s' "$PROJECT_SETTINGS" ;;
    --user|"") printf '%s' "$USER_SETTINGS" ;;
    *) echo "unknown scope: $1 (use --user or --project)" >&2; exit 1 ;;
  esac
}

# Shared Python helper. Builds the canonical hooks object from HOOK_SCRIPT +
# PYTHON_BIN and applies a mode against a settings file. Modes:
#   print              -> emit {"hooks": {...}} to stdout
#   install <path>     -> merge into <path> (backup), de-dupe our entries
#   uninstall <path>   -> drop our entries, prune emptied events
#   check <path>       -> print "installed" / "absent"
run_py() {
  HOOK_SCRIPT="$HOOK_SCRIPT" PYTHON_BIN="$PYTHON_BIN" "$PYTHON_BIN" - "$@" <<'PY'
import json, os, sys, time
from pathlib import Path

hook = os.environ["HOOK_SCRIPT"]
py = os.environ["PYTHON_BIN"]

# Events fired into the realtime tracer. SessionEnd is wrapped so trace
# finalization is detached from the Claude Code runner's shutdown.
SIMPLE_EVENTS = [
    "UserPromptSubmit", "PostToolUse", "PostToolUseFailure", "PreCompact",
    "Stop", "SubagentStart", "SubagentStop",
]
SESSION_END_CMD = (
    f"bash -lc 'payload=$(mktemp /tmp/cc-opik-sessionend-XXXXXX.json); "
    f'cat > "$payload"; nohup {py} {hook} SessionEnd --payload-file "$payload" '
    f">/dev/null 2>&1 &'"
)

def entry(cmd):
    return {"hooks": [{"type": "command", "command": cmd}]}

def canonical():
    h = {ev: [entry(f"{py} {hook} {ev}")] for ev in SIMPLE_EVENTS}
    h["SessionEnd"] = [entry(SESSION_END_CMD)]
    return h

def is_ours(matcher):
    for hk in matcher.get("hooks", []):
        if hook in (hk.get("command") or ""):
            return True
    return False

def load(path):
    p = Path(path)
    if not p.exists():
        return {}
    txt = p.read_text(encoding="utf-8").strip()
    return json.loads(txt) if txt else {}

def write_backup(path):
    p = Path(path)
    if p.exists():
        bak = f"{path}.bak-{time.strftime('%Y%m%d-%H%M%S')}"
        Path(bak).write_text(p.read_text(encoding="utf-8"), encoding="utf-8")
        return bak
    return None

mode = sys.argv[1]

if mode == "print":
    print(json.dumps({"hooks": canonical()}, indent=2))
    sys.exit(0)

path = sys.argv[2]

if mode == "check":
    data = load(path)
    hooks = data.get("hooks", {})
    found = any(is_ours(m) for arr in hooks.values() for m in arr)
    print("installed" if found else "absent")
    sys.exit(0)

data = load(path)
hooks = data.setdefault("hooks", {})

if mode == "install":
    for ev, matchers in canonical().items():
        existing = [m for m in hooks.get(ev, []) if not is_ours(m)]
        hooks[ev] = existing + matchers
elif mode == "uninstall":
    for ev in list(hooks.keys()):
        kept = [m for m in hooks[ev] if not is_ours(m)]
        if kept:
            hooks[ev] = kept
        else:
            del hooks[ev]
    if not hooks:
        data.pop("hooks", None)
else:
    print(f"unknown mode: {mode}", file=sys.stderr)
    sys.exit(2)

bak = write_backup(path)
Path(path).parent.mkdir(parents=True, exist_ok=True)
Path(path).write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
print(f"wrote: {path}")
if bak:
    print(f"backup: {bak}")
PY
}

cmd_install() {
  local target; target="$(target_settings "${1:-}")"
  [[ -f "$HOOK_SCRIPT" ]] || { echo "hook not found: $HOOK_SCRIPT" >&2; exit 1; }
  run_py install "$target"
  echo "Restart your Claude Code session to pick up the hooks."
}

cmd_uninstall() {
  local target; target="$(target_settings "${1:-}")"
  [[ -f "$target" ]] || { echo "(no settings file at $target)"; return; }
  run_py uninstall "$target"
}

cmd_deps() {
  [[ -f "$REQUIREMENTS" ]] || { echo "requirements.txt not found: $REQUIREMENTS" >&2; exit 1; }
  "$PYTHON_BIN" -m pip install -r "$REQUIREMENTS"
}

cmd_status() {
  local target; target="$(target_settings "${1:-}")"
  echo "repo_root:   $REPO_ROOT"
  echo "hook_script: $HOOK_SCRIPT"
  [[ -f "$HOOK_SCRIPT" ]] && echo "  hook_exists=true" || echo "  hook_exists=false"
  echo "python:      $PYTHON_BIN ($($PYTHON_BIN --version 2>&1))"
  echo
  echo "deps:"
  "$PYTHON_BIN" - <<'PY'
import importlib.util
for name in ("opik", "uuid6", "socksio"):
    print(f"  {name}_installed={importlib.util.find_spec(name) is not None}")
PY
  echo
  echo "settings ($target):"
  if [[ -f "$target" ]]; then
    echo "  exists=true hooks=$(run_py check "$target")"
  else
    echo "  exists=false"
  fi
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
    hooks)     run_py print ;;
    install)   shift; cmd_install "${1:-}" ;;
    uninstall) shift; cmd_uninstall "${1:-}" ;;
    deps)      cmd_deps ;;
    status)    shift; cmd_status "${1:-}" ;;
    tail-log)  cmd_tail_log ;;
    clear)     cmd_clear ;;
    -h|--help|help|"") usage ;;
    *) echo "unknown command: $1" >&2; usage; exit 1 ;;
  esac
}

main "$@"
