#!/usr/bin/env bash
# Top-level installer for the sii-opik-plugin harness tracers.
#
# Thin dispatcher over harness/<name>/install-<name>.sh — it doesn't reimplement
# any install logic, only forwards a command to one or more per-harness installer
# scripts and surfaces post-install notes. Run bare for an interactive picker,
# or pass a command + harness names for scripting.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Known harnesses, in display order. Each has a harness/<name>/install-<name>.sh.
HARNESS_NAMES=(claude-code opencode openclaw)

SELECTED=()

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [command] [harness ... | all]

Commands:
  install      Install selected harness tracers (default when run bare)
  uninstall    Remove selected harness tracers
  status       Show status for selected harnesses (all if none given)
  list         List harnesses, host-CLI presence, and install state
  <other>      Forwarded to each selected harness's install-<name>.sh
               (e.g. config, deps, clear, tail-log)

Harnesses:  ${HARNESS_NAMES[*]}   (or 'all')

Run with no arguments to pick harnesses interactively and install them.

Examples:
  $(basename "$0") install claude-code opencode
  $(basename "$0") install all
  $(basename "$0") status
  $(basename "$0") uninstall opencode
  $(basename "$0") config openclaw
EOF
}

# Host CLI that each harness traces.
harness_cli() {
  case "$1" in
    claude-code) echo claude ;;
    opencode)    echo opencode ;;
    openclaw)    echo openclaw ;;
  esac
}

# Per-harness installer script name (lives in harness/<name>/).
harness_script() {
  case "$1" in
    claude-code) echo install-claude.sh ;;
    opencode)    echo install-opencode.sh ;;
    openclaw)    echo install-openclaw.sh ;;
  esac
}

cli_state() {
  command -v "$(harness_cli "$1")" >/dev/null 2>&1 && echo found || echo missing
}

# Lightweight install-state probe (kept here so `list` is cheap; per-harness
# `status` remains the source of truth for detail).
harness_state() {
  case "$1" in
    claude-code)
      local s="$HOME/.claude/settings.json"
      if [[ -f "$s" ]] && grep -q "claude_realtime_trace.py" "$s" 2>/dev/null; then
        echo installed
      else
        echo absent
      fi ;;
    opencode)
      [[ -f "$HOME/.config/opencode/plugin/opik-trace.ts" ]] && echo installed || echo absent ;;
    openclaw)
      [[ -f "$ROOT_DIR/harness/openclaw/dist/index.js" ]] && echo built || echo not-built ;;
  esac
}

is_known() {
  local h
  for h in "${HARNESS_NAMES[@]}"; do
    [[ "$1" == "$h" ]] && return 0
  done
  return 1
}

post_install_hint() {
  case "$1" in
    claude-code) echo "  hint: restart Claude Code so new sessions load the hooks" ;;
    opencode)    echo "  hint: export OPIK_URL_OVERRIDE + OPIK_PROJECT_NAME before running opencode" ;;
    openclaw)    echo "  hint: run '$(basename "$0") config openclaw' to set your Opik API key" ;;
  esac
}

cmd_list() {
  printf "%-12s  %-18s  %s\n" "HARNESS" "HOST CLI" "STATE"
  local h
  for h in "${HARNESS_NAMES[@]}"; do
    printf "%-12s  %-18s  %s\n" "$h" "$(harness_cli "$h") ($(cli_state "$h"))" "$(harness_state "$h")"
  done
}

# Populate SELECTED from an interactive prompt. Returns non-zero if nothing
# was chosen.
pick_harnesses() {
  SELECTED=()
  echo "Available harnesses:"
  local i=1 h
  for h in "${HARNESS_NAMES[@]}"; do
    printf "  %d) %-12s host:%s(%s)  state:%s\n" \
      "$i" "$h" "$(harness_cli "$h")" "$(cli_state "$h")" "$(harness_state "$h")"
    i=$((i + 1))
  done
  printf "Select harnesses (e.g. '1 3', 'all', or Enter to cancel): "
  local line; read -r line || true
  [[ -z "$line" ]] && { echo "(nothing selected)"; return 1; }
  if [[ "$line" == all ]]; then
    SELECTED=("${HARNESS_NAMES[@]}")
    return 0
  fi
  local tok idx
  for tok in $line; do
    if [[ "$tok" =~ ^[0-9]+$ ]]; then
      idx=$((tok - 1))
      if [[ $idx -ge 0 && $idx -lt ${#HARNESS_NAMES[@]} ]]; then
        SELECTED+=("${HARNESS_NAMES[$idx]}")
      else
        echo "  out of range: $tok"
      fi
    elif is_known "$tok"; then
      SELECTED+=("$tok")
    else
      echo "  ignoring: $tok"
    fi
  done
  [[ ${#SELECTED[@]} -gt 0 ]]
}

# Run <command> against each named harness installer.
forward() {
  local cmd="$1"; shift
  local h dir script
  for h in "$@"; do
    dir="$ROOT_DIR/harness/$h"
    script="$(harness_script "$h")"
    if [[ ! -f "$dir/$script" ]]; then
      echo "==> $h: no installer at $dir/$script" >&2
      continue
    fi
    echo "==> $h: $cmd"
    if bash "$dir/$script" "$cmd"; then
      [[ "$cmd" == install ]] && post_install_hint "$h"
    else
      echo "  ($h: '$cmd' failed or unsupported)" >&2
    fi
    echo
  done
}

main() {
  # Bare run: interactive pick, then install.
  if [[ $# -eq 0 ]]; then
    pick_harnesses || { echo "aborted."; exit 1; }
    forward install "${SELECTED[@]}"
    return
  fi

  local cmd="$1"; shift
  case "$cmd" in
    list)            cmd_list; return ;;
    help|-h|--help)  usage; return ;;
  esac

  local harnesses=()
  if [[ $# -gt 0 ]]; then
    local a
    for a in "$@"; do
      if [[ "$a" == all ]]; then
        harnesses=("${HARNESS_NAMES[@]}")
        break
      fi
      if is_known "$a"; then
        harnesses+=("$a")
      else
        echo "unknown harness: $a (known: ${HARNESS_NAMES[*]}, all)" >&2
        exit 1
      fi
    done
  elif [[ "$cmd" == status ]]; then
    harnesses=("${HARNESS_NAMES[@]}")
  else
    pick_harnesses || { echo "aborted."; exit 1; }
    harnesses=("${SELECTED[@]}")
  fi

  forward "$cmd" "${harnesses[@]}"
}

main "$@"
