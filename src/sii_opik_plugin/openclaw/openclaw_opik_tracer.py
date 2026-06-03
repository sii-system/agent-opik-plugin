#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Shanghai Innovation Institute
"""
openclaw -> Opik tracer via incremental JSONL parsing.

This script is spawned by the TS plugin shell on each hook event.
It reads the hook event from stdin, incrementally parses the openclaw
session JSONL file, and emits Opik spans.

Unlike opik-openclaw which relies on hook event payloads (and suffers from
missing sessionKey, concurrent overwrites, and premature cleanup), this
tracer reads the authoritative JSONL transcript directly.

Events handled:
  session_start / session_end / before_reset
  before_agent_start / llm_input / llm_output / after_tool_call / agent_end
  before_tool_call / before_compaction / after_compaction
  subagent_spawning / subagent_delivery_target / subagent_spawned / subagent_ended
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

try:
    from opik import Opik
    from opik import id_helpers
except Exception:
    Opik = None
    id_helpers = None

_SPAN_BATCHING_SRC_ROOT = Path(__file__).resolve().parents[2]
if (_SPAN_BATCHING_SRC_ROOT / "sii_opik_plugin" / "span_batching.py").exists():
    sys.path.insert(0, str(_SPAN_BATCHING_SRC_ROOT))

try:
    from sii_opik_plugin.span_batching import (
        flush_span_batch,
        queue_span_snapshot,
        span_batch_env_names,
        update_queued_span,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {"sii_opik_plugin", "sii_opik_plugin.span_batching"}:
        raise
    from span_batching import (
        flush_span_batch,
        queue_span_snapshot,
        span_batch_env_names,
        update_queued_span,
    )

try:
    from uuid6 import uuid7 as _uuid7
except Exception:
    _uuid7 = None

# ── Config ────────────────────────────────────────────────────────────────────

STATE_DIR = Path.home() / ".openclaw" / "state"
STATE_FILE = STATE_DIR / "opik_tracer_state.json"
LOCK_FILE = STATE_DIR / "opik_tracer_state.lock"
LOG_FILE = STATE_DIR / "opik_tracer.log"

DEBUG = os.environ.get("OC_OPIK_DEBUG", "").lower() == "true"
DRY_RUN = os.environ.get("OC_OPIK_DRY_RUN", "").lower() == "true"
MAX_TEXT_CHARS = int(os.environ.get("OC_OPIK_MAX_TEXT_CHARS", "20000"))
DEFAULT_PROJECT = os.environ.get("OPIK_PROJECT_NAME", "openclaw")
FLUSH_INTERVAL_S = 5
SPAN_BATCH_ENV_NAMES = span_batch_env_names()
PROCESS_TIMEOUT_S = int(os.environ.get("OC_OPIK_PROCESS_TIMEOUT_S", "15"))
PINCHBENCH_TASK_ID = (os.environ.get("PINCHBENCH_TASK_ID") or "").strip()
PINCHBENCH_RUN_ID = (os.environ.get("PINCHBENCH_RUN_ID") or "").strip()
ORPHAN_SESSION_GC_AGE_S = 3600

# ── Logging ───────────────────────────────────────────────────────────────────

def _log(level: str, message: str) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{stamp} [{level}] {message}\n"
        with LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass

def info(msg: str) -> None:
    _log("INFO", msg)

def warn(msg: str) -> None:
    _log("WARN", msg)

def debug(msg: str) -> None:
    if DEBUG:
        _log("DEBUG", msg)


class ProcessTimeout(RuntimeError):
    pass


def _install_process_timeout() -> None:
    if PROCESS_TIMEOUT_S <= 0:
        return
    if not hasattr(signal, "SIGALRM"):
        return

    def _on_alarm(_signum: int, _frame: Any) -> None:
        raise ProcessTimeout(f"tracer exceeded {PROCESS_TIMEOUT_S}s")

    signal.signal(signal.SIGALRM, _on_alarm)
    signal.alarm(PROCESS_TIMEOUT_S)


def _clear_process_timeout() -> None:
    if hasattr(signal, "SIGALRM"):
        signal.alarm(0)

# ── Env helpers ───────────────────────────────────────────────────────────────

def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None

def apply_opik_env_overrides() -> None:
    override_url = _env_first("OPIK_URL_OVERRIDE", "OPIK_URL")
    if override_url and not os.environ.get("OPIK_URL"):
        os.environ["OPIK_URL"] = override_url
    api_key = _env_first("OPIK_API_KEY_OVERRIDE", "OPIK_API_KEY")
    if api_key and not os.environ.get("OPIK_API_KEY"):
        os.environ["OPIK_API_KEY"] = api_key
    workspace = _env_first("OPIK_WORKSPACE_OVERRIDE", "OPIK_WORKSPACE")
    if workspace and not os.environ.get("OPIK_WORKSPACE"):
        os.environ["OPIK_WORKSPACE"] = workspace

# ── File locking ──────────────────────────────────────────────────────────────

import fcntl

class FileLock:
    def __init__(self, path: Path):
        self._path = path
        self._fd: int | None = None

    def __enter__(self) -> "FileLock":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(str(self._path), os.O_CREAT | os.O_RDWR)
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *args: Any) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None

# ── State persistence ─────────────────────────────────────────────────────────
#
# Pinchbench runs each openclaw agent synchronously per task, so hooks for task
# N+1 do not arrive before task N has flushed and finalized. File-keyed session
# state remains correct even if that changes, but the legacy migration and GC
# assumptions below should be revisited if openclaw starts multiplexing tasks.

@dataclass
class SessionState:
    # Two-phase offset: committed = confirmed flushed to Opik,
    # pending = handler computed but Opik flush not yet confirmed.
    # On startup recovery, replay from committed_offset if pending > committed.
    committed_offset: int = 0
    pending_offset: int = 0
    emitted_turns: int = 0
    pending_emitted_turns: int = 0
    trace_id: str | None = None
    trace_name: str | None = None
    trace_start_ts: str | None = None
    last_turn_ts: str | None = None
    last_flush_time: float = 0.0
    turn_number: int = 0
    prev_usage_snapshot: dict[str, int] | None = None
    committed_usage_snapshot: dict[str, int] | None = None
    # Whether agent_end has been processed (trace finalized)
    completed: bool = False
    # Session transcript path (for recovery)
    session_file: str = ""
    # Logical session/thread identity used for Opik thread_id grouping.
    session_key: str = ""
    # Session-wide accumulators (committed values only — safe for metadata)
    session_total_llm_calls: int = 0
    session_total_tool_calls: int = 0
    session_total_subagent_calls: int = 0
    session_tool_success: int = 0
    session_tool_error: int = 0
    session_models: list[str] = field(default_factory=list)
    # Token accumulators
    session_api_billed_input: int = 0
    session_api_billed_output: int = 0
    session_api_billed_cache_read: int = 0
    session_api_billed_cache_creation: int = 0
    session_incremental_input: int = 0
    session_incremental_output: int = 0
    session_incremental_cache_read: int = 0
    session_incremental_cache_creation: int = 0
    # When recovery first detected this session as stale (0 = not stale).
    # Used for two-tier recovery: allow_partial only after prolonged staleness.
    stale_since: float = 0.0
    current_turn_span_id: str | None = None
    current_turn_start_ts: str | None = None
    current_turn_index: int = 0
    pending_tool_calls: dict[str, float] = field(default_factory=dict)
    compaction_span_id: str | None = None
    compaction_start_ts: str | None = None
    compactions_count: int = 0
    pending_subagents: dict[str, dict[str, Any]] = field(default_factory=dict)
    subagent_delivery: dict[str, dict[str, Any]] = field(default_factory=dict)
    ended_reason: str | None = None
    resumed_from: str | None = None
    session_end_info: dict[str, Any] = field(default_factory=dict)
    # Pending stats delta — accumulated during flush_turns, merged on commit,
    # discarded on rollback.  Kept as a plain dict so it serializes easily.
    _pending_stats: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubagentState:
    agent_id: str
    parent_session_key: str = ""
    parent_session_file: str = ""
    agent_span_id: str = ""
    subagent_label: str = ""
    subagent_mode: str = ""
    requester_origin: dict[str, Any] = field(default_factory=dict)
    spawn_mode: str = ""
    expects_completion_msg: bool = False
    transcript_path: str = ""
    turn_start_offset: int = 0
    emitted_turns: int = 0
    prev_usage_snapshot: dict[str, int] | None = None
    finished: bool = False
    started_at: str = ""
    end_reason: str = ""
    end_outcome: str = ""
    ended_at: str = ""
    error: str = ""
    target_kind: str = ""
    send_farewell: bool = False


def _load_global_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            _migrate_legacy_state(state)
            return state
    except Exception as exc:
        debug(f"load_global_state failed: {exc}")
    return {}

def _save_global_state(state: dict[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        debug(f"save_global_state failed: {exc}")


def _looks_like_session_file(value: str) -> bool:
    return value.endswith(".jsonl") or "/" in value or "\\" in value


def _normalize_session_file(value: str) -> str:
    if not value:
        return ""
    try:
        return str(Path(value).expanduser().resolve(strict=False))
    except Exception:
        return str(Path(value).expanduser())


def _find_state_key_by_session_key(global_state: dict[str, Any], session_key: str) -> str:
    if not session_key:
        return ""
    sessions = global_state.get("sessions", {})
    active_matches: list[str] = []
    fallback_matches: list[str] = []
    for state_key, raw in sessions.items():
        if str(raw.get("session_key", "")) != session_key:
            continue
        fallback_matches.append(state_key)
        if not bool(raw.get("completed", False)):
            active_matches.append(state_key)
    if len(active_matches) == 1:
        return active_matches[0]
    if len(fallback_matches) == 1:
        return fallback_matches[0]
    return ""


def resolve_state_key(global_state: dict[str, Any], session_key: str, session_file: str) -> str:
    normalized_file = _normalize_session_file(session_file)
    sessions = global_state.get("sessions", {})
    if normalized_file and normalized_file in sessions:
        return normalized_file
    existing = _find_state_key_by_session_key(global_state, session_key)
    if existing:
        raw = sessions.get(existing, {})
        existing_file = _normalize_session_file(str(raw.get("session_file", "")))
        existing_completed = bool(raw.get("completed", False))
        if not normalized_file:
            return existing
        if existing_file == normalized_file:
            return existing
        if not existing_completed and not existing_file:
            return existing
    if normalized_file:
        return normalized_file
    return session_key or ""


def _save_bound_session_state(
    global_state: dict[str, Any], state_key: str, session: SessionState
) -> str:
    normalized_file = _normalize_session_file(session.session_file)
    # Once the transcript path is known, persist under the file key so later
    # consumers can match the exact session_file deterministically.
    target_key = normalized_file or state_key
    if normalized_file:
        sessions = global_state.get("sessions", {})
        stale_keys: list[str] = []
        for existing_key, raw in sessions.items():
            if existing_key == target_key or not isinstance(raw, dict):
                continue
            if str(raw.get("session_key", "")) != session.session_key:
                continue
            existing_file = _normalize_session_file(str(raw.get("session_file", "")))
            if not existing_file:
                stale_keys.append(existing_key)
        for existing_key in stale_keys:
            sessions.pop(existing_key, None)
    save_session_state(global_state, target_key, session)
    return target_key


def _load_bound_session_state(
    global_state: dict[str, Any], session_key: str, session_file: str = ""
) -> tuple[str, SessionState]:
    state_key = resolve_state_key(global_state, session_key, session_file)
    session = load_session_state(global_state, state_key)
    session.session_key = session_key or session.session_key
    normalized_file = _normalize_session_file(session_file)
    if normalized_file:
        session.session_file = normalized_file
    return state_key, session


def _event_time(event: dict[str, Any]) -> datetime:
    raw = event.get("timestamp")
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw) / 1000.0, timezone.utc)
    if isinstance(raw, str) and raw:
        return parse_ts(raw)
    return datetime.now(timezone.utc)


def _subagent_session(global_state: dict[str, Any], session_key: str) -> SubagentState | None:
    if not session_key:
        return None
    return load_subagent_state(global_state, session_key)


def _is_child_session(global_state: dict[str, Any], session_key: str) -> bool:
    sub = _subagent_session(global_state, session_key)
    return bool(sub and not sub.finished)


def _update_child_subagent_from_event(global_state: dict[str, Any], event: dict[str, Any]) -> None:
    sub = _subagent_session(global_state, event.get("sessionKey", ""))
    if not sub:
        return
    transcript_path = _normalize_session_file(str(event.get("sessionFile", "")))
    if transcript_path:
        sub.transcript_path = transcript_path
    if not sub.started_at:
        sub.started_at = _event_time(event).isoformat()
    save_subagent_state(global_state, event["sessionKey"], sub)


def _resolve_parent_trace_id(
    global_state: dict[str, Any], session_key: str, session_file: str = ""
) -> tuple[str, str, SessionState] | tuple[None, None, None]:
    state_key, session = _load_bound_session_state(global_state, session_key, session_file)
    if not state_key:
        return None, None, None
    if not session.trace_start_ts:
        session.trace_start_ts = datetime.now(timezone.utc).isoformat()
    trace_id = session_trace_id(session)
    state_key = _save_bound_session_state(global_state, state_key, session)
    return state_key, trace_id, session


def _migrate_legacy_state(global_state: dict[str, Any]) -> None:
    sessions = global_state.get("sessions", {})
    subagents = global_state.get("subagents", {})
    migrated = 0

    for state_key, raw in sessions.items():
        if not isinstance(raw, dict):
            continue
        if str(state_key).startswith("agent:"):
            if not raw.get("completed", False):
                migrated += 1
            raw["completed"] = True
            raw["session_key"] = str(raw.get("session_key") or state_key)
        elif raw.get("session_file"):
            raw["session_file"] = _normalize_session_file(str(raw.get("session_file", "")))

    for raw in subagents.values():
        if not isinstance(raw, dict):
            continue
        parent_file = _normalize_session_file(str(raw.get("parent_session_file", "")))
        if parent_file:
            raw["parent_session_file"] = parent_file
        parent_key = str(raw.get("parent_session_key", ""))
        if parent_key.startswith("agent:") and (not parent_file or parent_file not in sessions):
            raw["finished"] = True

    if migrated:
        if not global_state.get("_legacy_session_key_migration_logged"):
            info(f"opik-tracer: migrated {migrated} legacy sessionKey-keyed state entries to completed")
        global_state["_legacy_session_key_migration_logged"] = True

def load_session_state(global_state: dict[str, Any], key: str) -> SessionState:
    raw = global_state.get("sessions", {}).get(key, {})
    # Backward compat: old state has turn_start_offset, new has committed_offset/pending_offset
    legacy_offset = int(raw.get("turn_start_offset", 0))
    committed = int(raw.get("committed_offset", legacy_offset))
    pending = int(raw.get("pending_offset", legacy_offset))
    legacy_emitted = int(raw.get("emitted_turns", 0))
    return SessionState(
        committed_offset=committed,
        pending_offset=pending,
        emitted_turns=int(raw.get("emitted_turns", legacy_emitted)),
        pending_emitted_turns=int(raw.get("pending_emitted_turns", legacy_emitted)),
        trace_id=raw.get("trace_id"),
        trace_name=raw.get("trace_name"),
        trace_start_ts=raw.get("trace_start_ts"),
        last_turn_ts=raw.get("last_turn_ts"),
        last_flush_time=float(raw.get("last_flush_time", 0.0)),
        turn_number=int(raw.get("turn_number", 0)),
        prev_usage_snapshot=raw.get("prev_usage_snapshot"),
        committed_usage_snapshot=raw.get("committed_usage_snapshot", raw.get("prev_usage_snapshot")),
        completed=bool(raw.get("completed", False)),
        session_file=_normalize_session_file(str(raw.get("session_file", key if _looks_like_session_file(str(key)) else ""))),
        session_key=str(raw.get("session_key", key if not _looks_like_session_file(str(key)) else "")),
        session_total_llm_calls=int(raw.get("session_total_llm_calls", 0)),
        session_total_tool_calls=int(raw.get("session_total_tool_calls", 0)),
        session_total_subagent_calls=int(raw.get("session_total_subagent_calls", 0)),
        session_tool_success=int(raw.get("session_tool_success", 0)),
        session_tool_error=int(raw.get("session_tool_error", 0)),
        session_models=raw.get("session_models", []),
        session_api_billed_input=int(raw.get("session_api_billed_input", 0)),
        session_api_billed_output=int(raw.get("session_api_billed_output", 0)),
        session_api_billed_cache_read=int(raw.get("session_api_billed_cache_read", 0)),
        session_api_billed_cache_creation=int(raw.get("session_api_billed_cache_creation", 0)),
        session_incremental_input=int(raw.get("session_incremental_input", 0)),
        session_incremental_output=int(raw.get("session_incremental_output", 0)),
        session_incremental_cache_read=int(raw.get("session_incremental_cache_read", 0)),
        session_incremental_cache_creation=int(raw.get("session_incremental_cache_creation", 0)),
        stale_since=float(raw.get("stale_since", 0.0)),
        current_turn_span_id=raw.get("current_turn_span_id"),
        current_turn_start_ts=raw.get("current_turn_start_ts"),
        current_turn_index=int(raw.get("current_turn_index", 0)),
        pending_tool_calls=dict(raw.get("pending_tool_calls", {})),
        compaction_span_id=raw.get("compaction_span_id"),
        compaction_start_ts=raw.get("compaction_start_ts"),
        compactions_count=int(raw.get("compactions_count", 0)),
        pending_subagents=dict(raw.get("pending_subagents", {})),
        subagent_delivery=dict(raw.get("subagent_delivery", {})),
        ended_reason=raw.get("ended_reason"),
        resumed_from=raw.get("resumed_from"),
        session_end_info=dict(raw.get("session_end_info", {})),
        _pending_stats=raw.get("_pending_stats", {}) or {},
    )

def save_session_state(global_state: dict[str, Any], key: str, session: SessionState) -> None:
    if "sessions" not in global_state:
        global_state["sessions"] = {}
    global_state["sessions"][key] = {
        "committed_offset": session.committed_offset,
        "pending_offset": session.pending_offset,
        "emitted_turns": session.emitted_turns,
        "pending_emitted_turns": session.pending_emitted_turns,
        "trace_id": session.trace_id,
        "trace_name": session.trace_name,
        "trace_start_ts": session.trace_start_ts,
        "last_turn_ts": session.last_turn_ts,
        "last_flush_time": session.last_flush_time,
        "turn_number": session.turn_number,
        "prev_usage_snapshot": session.prev_usage_snapshot,
        "committed_usage_snapshot": session.committed_usage_snapshot,
        "completed": session.completed,
        "session_file": session.session_file,
        "session_key": session.session_key,
        "session_total_llm_calls": session.session_total_llm_calls,
        "session_total_tool_calls": session.session_total_tool_calls,
        "session_total_subagent_calls": session.session_total_subagent_calls,
        "session_tool_success": session.session_tool_success,
        "session_tool_error": session.session_tool_error,
        "session_models": session.session_models,
        "session_api_billed_input": session.session_api_billed_input,
        "session_api_billed_output": session.session_api_billed_output,
        "session_api_billed_cache_read": session.session_api_billed_cache_read,
        "session_api_billed_cache_creation": session.session_api_billed_cache_creation,
        "session_incremental_input": session.session_incremental_input,
        "session_incremental_output": session.session_incremental_output,
        "session_incremental_cache_read": session.session_incremental_cache_read,
        "session_incremental_cache_creation": session.session_incremental_cache_creation,
        "stale_since": session.stale_since,
        "current_turn_span_id": session.current_turn_span_id,
        "current_turn_start_ts": session.current_turn_start_ts,
        "current_turn_index": session.current_turn_index,
        "pending_tool_calls": session.pending_tool_calls,
        "compaction_span_id": session.compaction_span_id,
        "compaction_start_ts": session.compaction_start_ts,
        "compactions_count": session.compactions_count,
        "pending_subagents": session.pending_subagents,
        "subagent_delivery": session.subagent_delivery,
        "ended_reason": session.ended_reason,
        "resumed_from": session.resumed_from,
        "session_end_info": session.session_end_info,
        "_pending_stats": session._pending_stats,
        "updated": datetime.now(timezone.utc).isoformat(),
    }

def load_subagent_state(global_state: dict[str, Any], child_key: str) -> SubagentState | None:
    raw = global_state.get("subagents", {}).get(child_key)
    if not raw:
        return None
    return SubagentState(
        agent_id=str(raw.get("agent_id", "")),
        parent_session_key=str(raw.get("parent_session_key", "")),
        parent_session_file=_normalize_session_file(str(raw.get("parent_session_file", ""))),
        agent_span_id=str(raw.get("agent_span_id", "")),
        subagent_label=str(raw.get("subagent_label", "")),
        subagent_mode=str(raw.get("subagent_mode", "")),
        requester_origin=dict(raw.get("requester_origin", {})),
        spawn_mode=str(raw.get("spawn_mode", "")),
        expects_completion_msg=bool(raw.get("expects_completion_msg", False)),
        transcript_path=str(raw.get("transcript_path", "")),
        turn_start_offset=int(raw.get("turn_start_offset", 0)),
        emitted_turns=int(raw.get("emitted_turns", 0)),
        prev_usage_snapshot=raw.get("prev_usage_snapshot"),
        finished=bool(raw.get("finished", False)),
        started_at=str(raw.get("started_at", "")),
        end_reason=str(raw.get("end_reason", "")),
        end_outcome=str(raw.get("end_outcome", "")),
        ended_at=str(raw.get("ended_at", "")),
        error=str(raw.get("error", "")),
        target_kind=str(raw.get("target_kind", "")),
        send_farewell=bool(raw.get("send_farewell", False)),
    )

def save_subagent_state(global_state: dict[str, Any], child_key: str, sub: SubagentState) -> None:
    if "subagents" not in global_state:
        global_state["subagents"] = {}
    global_state["subagents"][child_key] = {
        "agent_id": sub.agent_id,
        "parent_session_key": sub.parent_session_key,
        "parent_session_file": sub.parent_session_file,
        "agent_span_id": sub.agent_span_id,
        "subagent_label": sub.subagent_label,
        "subagent_mode": sub.subagent_mode,
        "requester_origin": sub.requester_origin,
        "spawn_mode": sub.spawn_mode,
        "expects_completion_msg": sub.expects_completion_msg,
        "transcript_path": sub.transcript_path,
        "turn_start_offset": sub.turn_start_offset,
        "emitted_turns": sub.emitted_turns,
        "prev_usage_snapshot": sub.prev_usage_snapshot,
        "finished": sub.finished,
        "started_at": sub.started_at,
        "end_reason": sub.end_reason,
        "end_outcome": sub.end_outcome,
        "ended_at": sub.ended_at,
        "error": sub.error,
        "target_kind": sub.target_kind,
        "send_farewell": sub.send_farewell,
    }

# ── Transcript data classes ───────────────────────────────────────────────────

@dataclass
class ToolUse:
    tool_use_id: str
    name: str
    input: Any
    timestamp: str

@dataclass
class ToolResult:
    tool_use_id: str
    content: str
    timestamp: str
    is_error: bool = False

@dataclass
class LLMCall:
    message_id: str
    model: str
    text: str
    reasoning: str = ""
    tool_uses: list[ToolUse] = field(default_factory=list)
    timestamp: str = ""
    usage: dict[str, int] = field(default_factory=dict)
    stop_reason: str | None = None
    start_timestamp: str = ""
    end_timestamp: str = ""
    incremental_usage: dict[str, int] = field(default_factory=dict)

@dataclass
class V3Turn:
    user_text: str
    user_timestamp: str
    llm_calls: list[LLMCall] = field(default_factory=list)
    tool_results: dict[str, ToolResult] = field(default_factory=dict)
    end_timestamp: str = ""
    end_offset: int = 0

@dataclass
class ReasoningRound:
    round_idx: int
    llm_items: list[tuple[int, LLMCall]] = field(default_factory=list)

# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_ts(value: str) -> datetime:
    if value:
        try:
            # Handle both ISO format and epoch millis
            if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
                return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, OSError):
            pass
    return datetime.now(timezone.utc)

def strip_model_date(model: str) -> str:
    if model and len(model) > 9:
        return re.sub(r"-\d{8}$", "", model)
    return model


def _session_thread_key(session: SessionState, fallback_session_key: str) -> str:
    return session.session_key or fallback_session_key


def _trace_model_name(session: SessionState, model: str = "") -> str:
    if model:
        return strip_model_date(model)
    if session.session_models:
        return strip_model_date(session.session_models[0])
    return ""


def _default_trace_name(
    session: SessionState, fallback_session_key: str, model: str = "", channel: str = "",
) -> str:
    model_name = _trace_model_name(session, model)
    if PINCHBENCH_TASK_ID and model_name:
        return f"{model_name} · {PINCHBENCH_TASK_ID}"
    if model_name:
        return " · ".join(part for part in [model_name, channel] if part)
    return session.trace_name or f"openclaw · {fallback_session_key[:12]}"

def truncate_text(value: str) -> str:
    return value[:MAX_TEXT_CHARS] if len(value) > MAX_TEXT_CHARS else value

_THINKING_TAG_RE = re.compile(r"<thinking>(.*?)</thinking>", re.DOTALL)

def _extract_text_and_reasoning(text: str, reasoning_parts: list[str]) -> str:
    def _collect(m: re.Match) -> str:
        reasoning_parts.append(m.group(1))
        return ""
    return _THINKING_TAG_RE.sub(_collect, text).strip()

# ── openclaw JSONL parsing ────────────────────────────────────────────────────
#
# openclaw JSONL format differs from Claude Code:
#   - Each line: {"type": "session"|"message", ...}
#   - Messages: {"type": "message", "id": "...", "message": {"role": "user"|"assistant"|"toolResult", ...}}
#   - role "toolResult" (not "tool_result")
#   - usage fields: "input"/"output" (not "input_tokens"/"output_tokens")
#   - tool calls in content: type "toolCall" or "toolUse"
#   - stopReason: "toolUse" (not "tool_use")

def _normalize_usage(raw_usage: dict[str, Any]) -> dict[str, int]:
    """Map openclaw usage fields to standard token field names."""
    if not raw_usage:
        return {}
    return {
        "input_tokens": int(raw_usage.get("input", 0) or raw_usage.get("input_tokens", 0) or 0),
        "output_tokens": int(raw_usage.get("output", 0) or raw_usage.get("output_tokens", 0) or 0),
        "cache_read_input_tokens": int(
            raw_usage.get("cacheRead", 0) or raw_usage.get("cache_read_input_tokens", 0) or 0
        ),
        "cache_creation_input_tokens": int(
            raw_usage.get("cacheWrite", 0) or raw_usage.get("cache_creation_input_tokens", 0) or 0
        ),
    }

def _is_tool_call_block(item: dict) -> bool:
    """Check if a content block is a tool call (openclaw uses several type names)."""
    return item.get("type") in ("toolCall", "toolUse", "tool_use", "functionCall")

def _extract_tool_call_id(item: dict) -> str:
    """Extract tool call ID from various field names."""
    return str(item.get("id", "") or item.get("toolCallId", "") or "")

def _is_continuation_message(text: str) -> bool:
    """Detect system-injected messages that should not start new turns."""
    return bool(re.match(r"^\s*<(task-notification|system-reminder)[\s>]", text))

def _parse_jsonl_turns(lines: list[tuple[str, int]]) -> list[V3Turn]:
    """Parse openclaw JSONL lines into V3Turn objects."""
    turns: list[V3Turn] = []
    current_turn: V3Turn | None = None
    current_msg_id: str | None = None
    current_llm: LLMCall | None = None

    for raw, line_end_offset in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        # Skip non-message records
        record_type = data.get("type", "")
        if record_type == "session":
            continue
        if record_type not in ("message", ""):
            # Also handle Claude Code format where type is the role directly
            if record_type not in ("user", "assistant", "tool_result"):
                continue

        # Extract the actual message object
        # openclaw wraps in {"type":"message","message":{...}}
        # Claude Code uses {"type":"assistant","message":{...}} or {"role":"user",...}
        if record_type == "message":
            msg = data.get("message", {})
        else:
            msg = data.get("message", data)

        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                msg = {}

        timestamp = str(data.get("timestamp", "") or msg.get("timestamp", ""))
        role = msg.get("role", record_type)

        # ── User message ──
        if role == "user":
            content = msg.get("content", "")
            is_tool_result = False

            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "tool_result":
                        is_tool_result = True
                        tool_use_id = item.get("tool_use_id", "")
                        is_error = bool(item.get("is_error", False))
                        result_content = item.get("content", "")
                        if isinstance(result_content, list):
                            result_content = " ".join(
                                i.get("text", "") for i in result_content
                                if isinstance(i, dict) and i.get("type") == "text"
                            )
                        elif not isinstance(result_content, str):
                            result_content = str(result_content)
                        if current_turn and tool_use_id:
                            current_turn.tool_results[tool_use_id] = ToolResult(
                                tool_use_id=tool_use_id,
                                content=result_content,
                                timestamp=timestamp,
                                is_error=is_error,
                            )
            if is_tool_result:
                continue

            # Flush pending LLM call
            if current_llm and current_turn:
                current_turn.llm_calls.append(current_llm)
                current_llm = None
                current_msg_id = None

            user_text = ""
            if isinstance(content, str):
                user_text = content
            elif isinstance(content, list):
                user_text = "\n".join(
                    item.get("text", "")
                    for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                )
            if not user_text.strip():
                continue
            if _is_continuation_message(user_text):
                continue

            if current_turn:
                _finalize_turn(current_turn)
                turns.append(current_turn)

            current_turn = V3Turn(
                user_text=user_text.strip(),
                user_timestamp=timestamp,
                end_offset=line_end_offset,
            )

        # ── Tool result message (openclaw uses role "toolResult") ──
        elif role in ("toolResult", "tool_result"):
            tool_call_id = str(msg.get("toolCallId", "") or msg.get("tool_use_id", "") or "")
            if not tool_call_id:
                continue
            # Create continuation turn if parsing started mid-file
            if current_turn is None:
                current_turn = V3Turn(
                    user_text="(continuation)",
                    user_timestamp=timestamp,
                    end_offset=line_end_offset,
                )
            result_content = msg.get("content", "")
            if isinstance(result_content, list):
                result_content = " ".join(
                    i.get("text", "") for i in result_content
                    if isinstance(i, dict) and i.get("type") == "text"
                )
            elif not isinstance(result_content, str):
                result_content = str(result_content)
            is_error = bool(msg.get("is_error", False) or msg.get("isError", False))
            current_turn.tool_results[tool_call_id] = ToolResult(
                tool_use_id=tool_call_id,
                content=result_content,
                timestamp=timestamp,
                is_error=is_error,
            )
            current_turn.end_offset = line_end_offset

        # ── Assistant message ──
        elif role == "assistant":
            # Create continuation turn if parsing started mid-file
            if current_turn is None:
                current_turn = V3Turn(
                    user_text="(continuation)",
                    user_timestamp=timestamp,
                    end_offset=line_end_offset,
                )
            msg_id = msg.get("id", "") or data.get("id", "")
            model = msg.get("model", "") or ""
            raw_usage = msg.get("usage", {}) or {}
            usage = _normalize_usage(raw_usage)
            stop_reason = msg.get("stopReason") or msg.get("stop_reason")

            content = msg.get("content", "")
            if isinstance(content, str):
                content_items = [{"type": "text", "text": content}] if content else []
            elif isinstance(content, list):
                content_items = content
            else:
                content_items = []

            text_parts: list[str] = []
            reasoning_parts: list[str] = []
            tool_uses: list[ToolUse] = []
            for item in content_items:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "text" and item.get("text"):
                    text_parts.append(_extract_text_and_reasoning(item["text"], reasoning_parts))
                elif item.get("type") == "thinking" and item.get("thinking"):
                    reasoning_parts.append(item["thinking"])
                elif _is_tool_call_block(item):
                    tool_uses.append(ToolUse(
                        tool_use_id=_extract_tool_call_id(item),
                        name=item.get("name", "tool"),
                        input=item.get("input", item.get("arguments", {})),
                        timestamp=timestamp,
                    ))

            # Merge with existing LLM call if same message_id (streaming chunks)
            if msg_id and msg_id == current_msg_id and current_llm:
                if new_text := "\n".join(t for t in text_parts if t):
                    current_llm.text = (current_llm.text + "\n" + new_text).lstrip("\n")
                if new_reasoning := "\n".join(reasoning_parts):
                    current_llm.reasoning = (current_llm.reasoning + "\n" + new_reasoning).lstrip("\n")
                current_llm.tool_uses.extend(tool_uses)
                current_llm.timestamp = timestamp
                current_llm.end_timestamp = timestamp
                if usage:
                    current_llm.usage = usage
                if stop_reason:
                    current_llm.stop_reason = stop_reason
            else:
                if current_llm:
                    current_turn.llm_calls.append(current_llm)
                current_msg_id = msg_id
                current_llm = LLMCall(
                    message_id=msg_id,
                    model=model,
                    text="\n".join(t for t in text_parts if t),
                    reasoning="\n".join(reasoning_parts),
                    tool_uses=tool_uses,
                    timestamp=timestamp,
                    start_timestamp=timestamp,
                    end_timestamp=timestamp,
                    usage=usage,
                    stop_reason=stop_reason,
                )
            current_turn.end_offset = line_end_offset

    # Flush final state
    if current_llm and current_turn:
        current_turn.llm_calls.append(current_llm)
    if current_turn:
        _finalize_turn(current_turn)
        turns.append(current_turn)

    return turns


def _finalize_turn(turn: V3Turn) -> None:
    if turn.llm_calls:
        turn.end_timestamp = turn.llm_calls[-1].end_timestamp or turn.llm_calls[-1].timestamp
    if turn.tool_results:
        last_tr = max(turn.tool_results.values(), key=lambda r: r.timestamp or "")
        if (last_tr.timestamp or "") > (turn.end_timestamp or ""):
            turn.end_timestamp = last_tr.timestamp


def _compute_incremental_usage(
    turns: list[V3Turn],
    prev_snapshot: dict[str, int] | None = None,
) -> dict[str, int]:
    """Compute per-LLM-call incremental usage (delta from previous)."""
    _KEYS = ("input_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")
    prev: dict[str, int] = dict(prev_snapshot) if prev_snapshot else {k: 0 for k in _KEYS}
    last_output_tokens = int(prev.get("output_tokens", 0) or 0)
    for turn in turns:
        for lc in turn.llm_calls:
            if not lc.usage:
                continue
            incremental: dict[str, int] = {}
            for k in _KEYS:
                cur = int(lc.usage.get(k, 0) or 0)
                incremental[k] = max(0, cur - prev.get(k, 0))
                prev[k] = cur
            out = int(lc.usage.get("output_tokens", 0) or 0)
            incremental["output_tokens"] = max(0, out - last_output_tokens)
            last_output_tokens = out
            lc.incremental_usage = incremental
    prev["output_tokens"] = last_output_tokens
    return dict(prev)


def _backfill_llm_starts(turns: list[V3Turn]) -> None:
    for turn in turns:
        prev_end_ts = turn.user_timestamp
        for lc in turn.llm_calls:
            llm_output_ts = lc.start_timestamp or lc.timestamp
            is_tool_only = bool(lc.tool_uses) and not lc.text.strip()
            if is_tool_only and prev_end_ts and prev_end_ts < llm_output_ts:
                lc.start_timestamp = prev_end_ts
            if lc.tool_uses:
                last_tr_ts = ""
                for tu in lc.tool_uses:
                    tr = turn.tool_results.get(tu.tool_use_id)
                    if tr and tr.timestamp and tr.timestamp > last_tr_ts:
                        last_tr_ts = tr.timestamp
                prev_end_ts = last_tr_ts or lc.end_timestamp or lc.timestamp
            else:
                prev_end_ts = lc.end_timestamp or lc.timestamp


def _collect_assistant_message_bounds(lines: list[str]) -> dict[str, tuple[str, str]]:
    bounds: dict[str, tuple[str, str]] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        record_type = data.get("type", "")
        if record_type == "message":
            msg = data.get("message", {})
        else:
            msg = data.get("message", data)
        if isinstance(msg, str):
            continue
        if msg.get("role") != "assistant":
            continue
        msg_id = msg.get("id") or data.get("id")
        ts = str(data.get("timestamp", "") or msg.get("timestamp", ""))
        if not msg_id or not ts:
            continue
        if msg_id in bounds:
            first_ts, _ = bounds[msg_id]
            bounds[msg_id] = (first_ts, ts)
        else:
            bounds[msg_id] = (ts, ts)
    return bounds


def _patch_llm_call_bounds(turns: list[V3Turn], lines: list[str]) -> None:
    bounds = _collect_assistant_message_bounds(lines)
    for turn in turns:
        for lc in turn.llm_calls:
            first_last = bounds.get(lc.message_id)
            if not first_last:
                lc.start_timestamp = lc.timestamp
                lc.end_timestamp = lc.timestamp
                continue
            first_ts, last_ts = first_last
            lc.start_timestamp = first_ts
            lc.end_timestamp = last_ts
            lc.timestamp = first_ts


def parse_transcript_segment(
    transcript_path: Path,
    byte_offset: int,
    prev_usage_snapshot: dict[str, int] | None = None,
) -> tuple[list[V3Turn], dict[str, int]]:
    """Read transcript from byte_offset and parse into V3Turn objects."""
    try:
        with transcript_path.open("rb") as f:
            f.seek(byte_offset)
            raw_bytes = f.read()
        raw_lines = raw_bytes.splitlines(keepends=True)
        lines: list[tuple[str, int]] = []
        rel_offset = 0
        for raw_line in raw_lines:
            rel_offset += len(raw_line)
            lines.append((raw_line.decode("utf-8", errors="replace"), rel_offset))
        turns = _parse_jsonl_turns(lines)
        text_lines = [line for line, _ in lines]
        _patch_llm_call_bounds(turns, text_lines)
        _backfill_llm_starts(turns)
        final_snapshot = _compute_incremental_usage(turns, prev_usage_snapshot)
        return turns, final_snapshot
    except Exception as exc:
        debug(f"parse_transcript_segment failed: {exc}")
        return [], dict(prev_usage_snapshot) if prev_usage_snapshot else {}


# ── ID generation ─────────────────────────────────────────────────────────────

def new_opik_id(anchor_time: datetime | None = None) -> str:
    if id_helpers is not None:
        try:
            return str(id_helpers.generate_id(anchor_time))
        except Exception:
            pass
    if _uuid7 is not None:
        try:
            return str(_uuid7())
        except Exception:
            pass
    return str(uuid.uuid4())


def _deterministic_span_id(trace_id: str, *parts: str) -> str:
    raw = "::".join([trace_id, *parts])
    digest = hashlib.sha256(raw.encode("utf-8")).digest()
    b = bytearray(digest[:16])
    b[6] = (b[6] & 0x0F) | 0x70  # version 7
    b[8] = (b[8] & 0x3F) | 0x80  # variant 10
    return str(uuid.UUID(bytes=bytes(b)))


def session_trace_id(session: SessionState) -> str:
    if session.trace_id:
        return session.trace_id
    session.trace_id = new_opik_id()
    return session.trace_id

# ── Opik API wrappers ─────────────────────────────────────────────────────────

def create_trace_if_possible(client: Any, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run create_trace name={kwargs.get('name')} id={kwargs.get('id')}")
        return
    client.rest_client.traces.create_trace(**kwargs)

_UPDATE_SPAN_UNSUPPORTED = frozenset({"start_time", "last_updated_at", "total_estimated_cost_version"})

def update_span_if_possible(client: Any, span_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run update_span id={span_id}")
        return
    filtered = {k: v for k, v in kwargs.items() if k not in _UPDATE_SPAN_UNSUPPORTED}
    if update_queued_span(span_id, filtered, SPAN_BATCH_ENV_NAMES):
        return
    client.rest_client.spans.update_span(span_id, **filtered)

def create_or_update_span(client: Any, span_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run upsert_span name={kwargs.get('name')} id={span_id}")
        return
    if queue_span_snapshot(span_id, kwargs, SPAN_BATCH_ENV_NAMES):
        return
    try:
        client.rest_client.spans.create_span(id=span_id, **kwargs)
    except Exception:
        update_kwargs = {k: v for k, v in kwargs.items() if k not in _UPDATE_SPAN_UNSUPPORTED}
        try:
            client.rest_client.spans.update_span(span_id, **update_kwargs)
        except Exception as exc:
            debug(f"upsert span failed for {span_id}: {exc}")


def ensure_trace(
    client: Any, project_name: str, trace_id: str, trace_name: str,
    session_key: str, start_time: datetime, end_time: datetime,
    session: SessionState,
) -> None:
    meta = _session_metadata(session, session_key)
    tags = _session_tags(session)
    try:
        create_trace_if_possible(
            client, id=trace_id, project_name=project_name, name=trace_name,
            start_time=start_time, end_time=end_time,
            input={"session_key": session_key},
            output={"status": "running"},
            metadata=meta, tags=tags, thread_id=session_key,
        )
    except Exception as exc:
        debug(f"ensure_trace failed: {exc}")


def ensure_session_trace(
    client: Any, project_name: str, session_key: str, session: SessionState
) -> str:
    trace_id = session_trace_id(session)
    trace_name = session.trace_name or _default_trace_name(session, session_key)
    start_time = parse_ts(session.trace_start_ts or "") or datetime.now(timezone.utc)
    ensure_trace(client, project_name, trace_id, trace_name, session_key, start_time, start_time, session)
    return trace_id


def upsert_lifecycle_span(
    client: Any,
    project_name: str,
    trace_id: str,
    span_id: str,
    name: str,
    start_time: datetime,
    end_time: datetime,
    metadata: dict[str, Any] | None = None,
    input_payload: dict[str, Any] | None = None,
    output_payload: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    parent_span_id: str | None = None,
) -> None:
    create_or_update_span(
        client,
        span_id,
        trace_id=trace_id,
        parent_span_id=parent_span_id or trace_id,
        project_name=project_name,
        name=name,
        type="general",
        start_time=start_time,
        end_time=end_time,
        input=input_payload or {},
        output=output_payload or {},
        metadata=metadata or {},
        tags=tags or ["openclaw", name],
    )


def close_current_turn_span(session: SessionState) -> None:
    # State-only clear. The turn span itself is owned by emit_turn (same
    # deterministic id), so writing a lifecycle payload here would clobber
    # its real input/output/metadata.
    session.current_turn_span_id = None
    session.current_turn_start_ts = None


def finalize_pending_subagent_attempts(
    client: Any,
    project_name: str,
    session_key: str,
    session: SessionState,
    end_time: datetime,
) -> None:
    if not session.pending_subagents:
        return
    trace_id = ensure_session_trace(client, project_name, session_key, session)
    for child_key, meta in list(session.pending_subagents.items()):
        span_id = str(meta.get("span_id") or _deterministic_span_id(trace_id, "subagent-attempt", child_key))
        start_time = parse_ts(str(meta.get("started_at", ""))) or end_time
        lifecycle_meta = {k: v for k, v in meta.items() if k != "span_id"}
        delivery_meta = session.subagent_delivery.get(child_key)
        if delivery_meta:
            lifecycle_meta["delivery"] = delivery_meta
        upsert_lifecycle_span(
            client,
            project_name,
            trace_id,
            span_id,
            "subagent.spawn_attempt",
            start_time,
            end_time,
            metadata=lifecycle_meta,
            output_payload={"status": "pending"},
            tags=["openclaw", "subagent", "spawn-attempt"],
        )


def _namespace_parts(id_namespace: str | Sequence[str] | None, session_key: str) -> tuple[str, ...]:
    if id_namespace is None:
        return (session_key,)
    if isinstance(id_namespace, str):
        return (id_namespace,)
    return tuple(str(part) for part in id_namespace)


def finalize_trace(
    client: Any, project_name: str, trace_id: str, trace_name: str,
    session_key: str, start_time: datetime, end_time: datetime,
    session: SessionState,
) -> None:
    meta = _session_metadata(session, session_key)
    meta["completed"] = True
    output = _session_output(session)
    tags = [*_session_tags(session), "completed"]

    if DRY_RUN:
        info(f"dry-run finalize_trace name={trace_name} id={trace_id}")
        return
    traces_api = getattr(getattr(client, "rest_client", None), "traces", None)
    payload = {
        "project_name": project_name, "name": trace_name,
        "start_time": start_time, "end_time": end_time,
        "input": {"session_key": session_key, "turns": session.emitted_turns},
        "output": output, "metadata": meta, "tags": tags, "thread_id": session_key,
    }
    for method_name in ["update_trace", "update", "upsert_trace"]:
        method = getattr(traces_api, method_name, None)
        if callable(method):
            try:
                method(trace_id, id=trace_id, **payload)
                return
            except TypeError:
                try:
                    method(id=trace_id, **payload)
                    return
                except Exception:
                    pass
            except Exception:
                pass
    try:
        create_trace_if_possible(client, id=trace_id, **payload)
    except Exception as exc:
        debug(f"finalize_trace fallback failed: {exc}")


def _session_metadata(session: SessionState, session_key: str) -> dict[str, Any]:
    models = sorted(session.session_models) if session.session_models else []
    snap = session.prev_usage_snapshot or {}
    meta = {
        "session_key": session_key,
        "source": "openclaw",
        "realtime": True,
        "models": models,
        "total_turns": session.emitted_turns,
        "total_llm_calls": session.session_total_llm_calls,
        "total_tool_calls": session.session_total_tool_calls,
        "total_subagent_calls": session.session_total_subagent_calls,
        "tool_success": session.session_tool_success,
        "tool_error": session.session_tool_error,
        "api_billed_input_tokens": session.session_api_billed_input,
        "api_billed_output_tokens": session.session_api_billed_output,
        "incremental_input_tokens": session.session_incremental_input,
        "incremental_output_tokens": session.session_incremental_output,
        "snapshot_input_tokens": int(snap.get("input_tokens", 0) or 0),
        "snapshot_output_tokens": int(snap.get("output_tokens", 0) or 0),
        "compactions_count": session.compactions_count,
        "pending_subagents": len(session.pending_subagents),
    }
    if session.session_file:
        meta["session_file"] = session.session_file
    if session.resumed_from:
        meta["resumed_from"] = session.resumed_from
    if session.ended_reason:
        meta["ended_reason"] = session.ended_reason
    if session.session_end_info:
        meta["session_end"] = session.session_end_info
    if PINCHBENCH_TASK_ID:
        meta["pinchbench.task_id"] = PINCHBENCH_TASK_ID
    if PINCHBENCH_RUN_ID:
        meta["pinchbench.run_id"] = PINCHBENCH_RUN_ID
    return meta

def _session_output(session: SessionState) -> dict[str, Any]:
    output = {
        "status": "completed",
        "total_turns": session.emitted_turns,
        "total_llm_calls": session.session_total_llm_calls,
        "total_tool_calls": session.session_total_tool_calls,
        "api_billed_input_tokens": session.session_api_billed_input,
        "api_billed_output_tokens": session.session_api_billed_output,
    }
    if session.ended_reason:
        output["ended_by"] = session.ended_reason
    return output

def _session_tags(session: SessionState) -> list[str]:
    models = sorted(session.session_models) if session.session_models else []
    tags = ["openclaw", "session", "realtime", *[f"model:{m}" for m in models]]
    if session.resumed_from:
        tags.append("resumed")
    if session.ended_reason:
        tags.append(f"ended_by:{session.ended_reason}")
    if session.compactions_count:
        tags.append(f"compactions:{session.compactions_count}")
    if PINCHBENCH_TASK_ID:
        tags.append(f"pinchbench.task_id:{PINCHBENCH_TASK_ID}")
    if PINCHBENCH_RUN_ID:
        tags.append(f"pinchbench.run_id:{PINCHBENCH_RUN_ID}")
    return tags


# ── Reasoning round grouping ─────────────────────────────────────────────────

def _group_reasoning_rounds(turn: V3Turn) -> list[ReasoningRound]:
    """Group LLM calls into reasoning rounds (text boundary = new round)."""
    rounds: list[ReasoningRound] = []
    current_round: ReasoningRound | None = None
    for idx, lc in enumerate(turn.llm_calls):
        has_text = bool(lc.text.strip())
        if has_text or current_round is None:
            current_round = ReasoningRound(round_idx=len(rounds))
            rounds.append(current_round)
        current_round.llm_items.append((idx, lc))
    return rounds


# ── Usage metadata ────────────────────────────────────────────────────────────

def build_usage_metadata(usage: dict[str, int]) -> dict[str, int]:
    return {
        "input_tokens": int(usage.get("input_tokens", 0) or 0),
        "output_tokens": int(usage.get("output_tokens", 0) or 0),
        "cache_read_input_tokens": int(usage.get("cache_read_input_tokens", 0) or 0),
        "cache_creation_input_tokens": int(usage.get("cache_creation_input_tokens", 0) or 0),
    }

def _opik_usage(usage_meta: dict[str, int]) -> dict[str, int]:
    return {
        "prompt_tokens": usage_meta.get("input_tokens", 0),
        "completion_tokens": usage_meta.get("output_tokens", 0),
        "total_tokens": usage_meta.get("input_tokens", 0) + usage_meta.get("output_tokens", 0),
    }

# ── Turn metadata ─────────────────────────────────────────────────────────────

def _turn_metadata(turn: V3Turn, session_key: str, turn_idx: int) -> dict[str, Any]:
    llm_count = len(turn.llm_calls)
    tool_count = sum(len(lc.tool_uses) for lc in turn.llm_calls)
    models = sorted({lc.model for lc in turn.llm_calls if lc.model})
    return {
        "session_key": session_key,
        "turn_idx": turn_idx,
        "llm_calls": llm_count,
        "tool_calls": tool_count,
        "tool_results": len(turn.tool_results),
        "models": models,
        "source": "openclaw",
    }

# ── Turn emission ─────────────────────────────────────────────────────────────

def emit_turn(
    client: Any,
    project_name: str,
    trace_id: str,
    session_key: str,
    turn: V3Turn,
    turn_idx: int,
    parent_span_id: str | None = None,
    id_namespace: str | Sequence[str] | None = None,
    depth: int = 0,
) -> datetime:
    """Emit a V3Turn as nested Opik spans: turn -> round -> llm -> tool."""
    if depth > 5:
        return parse_ts(turn.user_timestamp)

    turn_start = parse_ts(turn.user_timestamp)
    turn_end = parse_ts(turn.end_timestamp) if turn.end_timestamp else turn_start
    if turn_end < turn_start:
        turn_end = turn_start

    span_parent_id = parent_span_id or trace_id
    span_namespace = _namespace_parts(id_namespace, session_key)

    turn_span_id = _deterministic_span_id(
        trace_id, "session", *span_namespace, "turn", str(turn_idx)
    )
    turn_name = f"turn-{turn_idx + 1}"

    # Build turn input/output
    user_content = [{"type": "text", "text": truncate_text(turn.user_text)}]
    all_outputs: list[dict[str, Any]] = [{"role": "user", "content": user_content}]

    create_or_update_span(
        client, turn_span_id,
        trace_id=trace_id, parent_span_id=span_parent_id, project_name=project_name,
        name=turn_name, type="general",
        start_time=turn_start, end_time=turn_end,
        input={"messages": [{"role": "user", "content": user_content}]},
        output={},
        metadata=_turn_metadata(turn, session_key, turn_idx),
        tags=["openclaw", turn_name],
    )

    # Emit reasoning rounds
    for rnd in _group_reasoning_rounds(turn):
        first_llm = rnd.llm_items[0][1]
        round_start = parse_ts(first_llm.start_timestamp or first_llm.timestamp or turn.user_timestamp)
        last_llm = rnd.llm_items[-1][1]
        round_end = parse_ts(last_llm.end_timestamp or last_llm.timestamp)

        # Check if any tool result extends the round end
        for _, lc in rnd.llm_items:
            for tu in lc.tool_uses:
                tr = turn.tool_results.get(tu.tool_use_id)
                if tr and tr.timestamp:
                    tr_time = parse_ts(tr.timestamp)
                    if tr_time > round_end:
                        round_end = tr_time

        round_span_id = _deterministic_span_id(
            trace_id, "session", *span_namespace, "turn", str(turn_idx), "round", str(rnd.round_idx)
        )
        round_name = f"round-{rnd.round_idx + 1}"

        create_or_update_span(
            client, round_span_id,
            trace_id=trace_id, parent_span_id=turn_span_id, project_name=project_name,
            name=round_name, type="general",
            start_time=round_start, end_time=round_end,
            input={"messages": list(all_outputs)},
            output={},
            metadata={"round_idx": rnd.round_idx, "llm_calls": len(rnd.llm_items)},
            tags=["reasoning-round", round_name],
        )

        # Emit LLM calls within round
        for llm_idx, lc in rnd.llm_items:
            model_display = strip_model_date(lc.model)
            usage_meta = build_usage_metadata(lc.usage)
            llm_usage = _opik_usage(usage_meta)
            llm_start = parse_ts(lc.start_timestamp or lc.timestamp or turn.user_timestamp)
            llm_end = parse_ts(lc.end_timestamp or lc.timestamp)

            # Build assistant content
            assistant_content: list[dict[str, Any]] = []
            if lc.reasoning:
                assistant_content.append({"type": "reasoning", "text": truncate_text(lc.reasoning)})
            if lc.text:
                assistant_content.append({"type": "text", "text": truncate_text(lc.text)})
            for tu in lc.tool_uses:
                assistant_content.append({
                    "type": "tool_call", "name": tu.name,
                    "args": tu.input, "id": tu.tool_use_id,
                })

            llm_output = {"role": "assistant", "content": assistant_content}
            llm_span_id = _deterministic_span_id(
                trace_id, "session", *span_namespace, "turn", str(turn_idx),
                "round", str(rnd.round_idx), "llm", str(llm_idx)
            )

            llm_meta: dict[str, Any] = {
                "message_id": lc.message_id,
                "model": lc.model,
                "stop_reason": lc.stop_reason,
                **build_usage_metadata(lc.usage),
            }
            if lc.incremental_usage:
                for k, v in lc.incremental_usage.items():
                    llm_meta[f"incremental_{k}"] = v

            create_or_update_span(
                client, llm_span_id,
                trace_id=trace_id, parent_span_id=round_span_id, project_name=project_name,
                name=model_display or "llm",
                type="llm",
                model=lc.model,
                provider="openclaw",
                start_time=llm_start, end_time=llm_end,
                input={"messages": list(all_outputs)},
                output=llm_output,
                usage=llm_usage,
                metadata=llm_meta,
                tags=["llm", f"model:{model_display}"],
            )

            all_outputs.append(llm_output)

            # Emit tool spans under LLM
            for tu_idx, tu in enumerate(lc.tool_uses):
                tr = turn.tool_results.get(tu.tool_use_id)
                tool_start = parse_ts(tu.timestamp)
                tool_end = parse_ts(tr.timestamp) if tr else llm_end

                tool_span_id = _deterministic_span_id(
                    trace_id, "session", *span_namespace, "turn", str(turn_idx), "round", str(rnd.round_idx),
                    "llm", str(llm_idx), "tool", str(tu_idx)
                )

                tool_output: dict[str, Any] = {}
                tool_error_info: dict[str, Any] | None = None
                if tr:
                    if tr.is_error:
                        tool_output = {"error": truncate_text(tr.content)}
                        tool_error_info = {
                            "exception_type": "ToolError",
                            "message": truncate_text(tr.content),
                        }
                    else:
                        tool_output = {"result": truncate_text(tr.content)}

                tool_payload: dict[str, Any] = {
                    "trace_id": trace_id,
                    "parent_span_id": llm_span_id,
                    "project_name": project_name,
                    "name": tu.name,
                    "type": "tool",
                    "start_time": tool_start,
                    "end_time": tool_end,
                    "input": tu.input if isinstance(tu.input, dict) else {"args": tu.input},
                    "output": tool_output,
                    "metadata": {"tool_use_id": tu.tool_use_id, "tool_name": tu.name},
                    "tags": ["tool", f"tool:{tu.name}"],
                }
                if tool_error_info:
                    tool_payload["error_info"] = tool_error_info

                create_or_update_span(client, tool_span_id, **tool_payload)

                # Record tool result in outputs for context
                if tr:
                    all_outputs.append({
                        "role": "tool",
                        "tool_use_id": tu.tool_use_id,
                        "content": truncate_text(tr.content),
                    })

    # Update turn span with accumulated output
    try:
        update_span_if_possible(
            client, turn_span_id,
            output={"messages": all_outputs},
            end_time=turn_end,
        )
    except Exception:
        pass

    return turn_end


def create_or_update_subagent_span(
    client: Any,
    project_name: str,
    trace_id: str,
    parent_span_id: str,
    agent_span_id: str,
    child_key: str,
    child_agent_id: str,
    start_time: datetime,
    end_time: datetime,
    turn_count: int,
    metadata: dict[str, Any] | None = None,
    input_payload: dict[str, Any] | None = None,
    output_payload: dict[str, Any] | None = None,
) -> None:
    merged_meta = {
        "session_key": child_key,
        "agent_id": child_agent_id,
        "source": "openclaw-subagent",
        "turns": turn_count,
    }
    if metadata:
        merged_meta.update(metadata)
    create_or_update_span(
        client, agent_span_id,
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        project_name=project_name,
        name=f"subagent:{child_agent_id or child_key[:8]}",
        type="general",
        start_time=start_time,
        end_time=end_time,
        input=input_payload or {"session_key": child_key},
        output=output_payload or {"turns": turn_count},
        metadata=merged_meta,
        tags=["openclaw", "subagent"],
    )


# ── Flush logic ───────────────────────────────────────────────────────────────

def flush_turns(
    client: Any,
    project_name: str,
    session: SessionState,
    session_key: str,
    transcript_path: Path,
    allow_partial: bool = False,
) -> int:
    """Parse new turns from transcript and emit to Opik. Returns count emitted.

    Two-phase offset protocol:
      1. Read from committed_offset (not pending — pending may be stale from a
         crashed previous run, and deterministic span IDs make re-emit safe).
      2. Emit spans to Opik SDK buffer.
      3. Advance pending_offset and pending_emitted_turns.
      4. Caller must call commit_flush() after client.flush() succeeds to
         promote pending -> committed. On failure, call rollback_flush().
    """
    if not transcript_path.exists():
        return 0

    file_size = transcript_path.stat().st_size
    if file_size <= session.committed_offset:
        return 0

    # Always mark session as active when flush_turns is called,
    # even if no complete turns exist yet.  This prevents
    # recover_incomplete_sessions from treating the session as stale
    # while an agentic tool loop is still in progress.
    session.last_flush_time = time.time()

    turns, _ = parse_transcript_segment(
        transcript_path, session.committed_offset, session.committed_usage_snapshot,
    )
    if not turns:
        return 0

    # Collect contiguous complete turns from the start of the parsed segment.
    # Stop at the first incomplete turn so the offset never jumps over
    # unprocessed data (which would cause turn index misalignment on re-parse).
    completed: list[V3Turn] = []
    for turn in turns:
        if not turn.llm_calls:
            if allow_partial:
                continue  # skip empty turns in final flush
            break  # incomplete barrier in normal mode
        all_tools_have_results = all(
            tu.tool_use_id in turn.tool_results
            for lc in turn.llm_calls
            for tu in lc.tool_uses
        )
        # If the last LLM call has tool_uses, the LLM will respond again
        # after those tool results — the turn is not yet complete.
        last_llm_has_pending_tools = bool(turn.llm_calls[-1].tool_uses)
        is_complete = all_tools_have_results and not last_llm_has_pending_tools
        if is_complete or allow_partial:
            completed.append(turn)
        else:
            break  # stop at first incomplete turn to keep offset contiguous

    if not completed:
        return 0

    completed_snapshot = _compute_incremental_usage(completed, session.committed_usage_snapshot)

    trace_id = session_trace_id(session)
    trace_name = session.trace_name or _default_trace_name(session, session_key)

    # Ensure trace exists
    start_time = parse_ts(session.trace_start_ts or completed[0].user_timestamp)
    if not session.trace_start_ts:
        session.trace_start_ts = completed[0].user_timestamp
    end_time = parse_ts(completed[-1].end_timestamp or completed[-1].user_timestamp)

    ensure_trace(
        client, project_name, trace_id, trace_name,
        session_key, start_time, end_time, session,
    )

    # Emit each completed turn — use emitted_turns (committed count) as base index
    emitted = 0
    pending_stats: dict[str, Any] = {
        "llm_calls": 0, "tool_calls": 0,
        "tool_success": 0, "tool_error": 0,
        "models": [],
        "api_billed_input": 0, "api_billed_output": 0,
        "api_billed_cache_read": 0, "api_billed_cache_creation": 0,
        "incremental_input": 0, "incremental_output": 0,
        "incremental_cache_read": 0, "incremental_cache_creation": 0,
        "last_turn_ts": "",
    }
    for turn in completed:
        turn_idx = session.emitted_turns + emitted
        try:
            emit_turn(client, project_name, trace_id, session_key, turn, turn_idx)
            emitted += 1

            # Accumulate stats into pending delta (NOT into session directly)
            pending_stats["llm_calls"] += len(turn.llm_calls)
            tool_count = sum(len(lc.tool_uses) for lc in turn.llm_calls)
            pending_stats["tool_calls"] += tool_count
            pending_stats["tool_success"] += sum(
                1 for tr in turn.tool_results.values() if not tr.is_error
            )
            pending_stats["tool_error"] += sum(
                1 for tr in turn.tool_results.values() if tr.is_error
            )
            for lc in turn.llm_calls:
                if lc.model and lc.model not in pending_stats["models"]:
                    pending_stats["models"].append(lc.model)
                if lc.usage:
                    pending_stats["api_billed_input"] += int(lc.usage.get("input_tokens", 0) or 0)
                    pending_stats["api_billed_output"] += int(lc.usage.get("output_tokens", 0) or 0)
                    pending_stats["api_billed_cache_read"] += int(lc.usage.get("cache_read_input_tokens", 0) or 0)
                    pending_stats["api_billed_cache_creation"] += int(lc.usage.get("cache_creation_input_tokens", 0) or 0)
                if lc.incremental_usage:
                    pending_stats["incremental_input"] += int(lc.incremental_usage.get("input_tokens", 0) or 0)
                    pending_stats["incremental_output"] += int(lc.incremental_usage.get("output_tokens", 0) or 0)
                    pending_stats["incremental_cache_read"] += int(lc.incremental_usage.get("cache_read_input_tokens", 0) or 0)
                    pending_stats["incremental_cache_creation"] += int(lc.incremental_usage.get("cache_creation_input_tokens", 0) or 0)

            pending_stats["last_turn_ts"] = turn.end_timestamp or turn.user_timestamp

        except Exception as exc:
            warn(f"emit_turn failed turn_idx={turn_idx}: {exc}")

    # Advance PENDING offset only — committed stays until flush confirmed
    max_completed_offset = max(turn.end_offset for turn in completed)
    session.pending_offset = session.committed_offset + max_completed_offset
    session.pending_emitted_turns = session.emitted_turns + emitted
    session.prev_usage_snapshot = completed_snapshot
    session.last_flush_time = time.time()
    session._pending_stats = pending_stats

    return emitted


def commit_flush(session: SessionState) -> None:
    """Promote pending state to committed after Opik flush succeeds."""
    session.committed_offset = session.pending_offset
    session.emitted_turns = session.pending_emitted_turns
    session.committed_usage_snapshot = (
        dict(session.prev_usage_snapshot) if session.prev_usage_snapshot else None
    )
    # Merge pending stats delta into committed session accumulators
    ps = session._pending_stats
    if ps:
        session.session_total_llm_calls += ps.get("llm_calls", 0)
        session.session_total_tool_calls += ps.get("tool_calls", 0)
        session.session_tool_success += ps.get("tool_success", 0)
        session.session_tool_error += ps.get("tool_error", 0)
        for m in ps.get("models", []):
            if m not in session.session_models:
                session.session_models.append(m)
        session.session_api_billed_input += ps.get("api_billed_input", 0)
        session.session_api_billed_output += ps.get("api_billed_output", 0)
        session.session_api_billed_cache_read += ps.get("api_billed_cache_read", 0)
        session.session_api_billed_cache_creation += ps.get("api_billed_cache_creation", 0)
        session.session_incremental_input += ps.get("incremental_input", 0)
        session.session_incremental_output += ps.get("incremental_output", 0)
        session.session_incremental_cache_read += ps.get("incremental_cache_read", 0)
        session.session_incremental_cache_creation += ps.get("incremental_cache_creation", 0)
        if ps.get("last_turn_ts"):
            session.last_turn_ts = ps["last_turn_ts"]
        session._pending_stats = {}


def rollback_flush(session: SessionState) -> None:
    """Discard pending state on Opik flush failure — next run replays from committed."""
    session.pending_offset = session.committed_offset
    session.pending_emitted_turns = session.emitted_turns
    session.prev_usage_snapshot = (
        dict(session.committed_usage_snapshot) if session.committed_usage_snapshot else None
    )
    # Discard pending stats — they were never committed
    session._pending_stats = {}


def _flush_client(client: Any) -> bool:
    try:
        flush_span_batch(client, SPAN_BATCH_ENV_NAMES, log=info)
        if hasattr(client, "flush"):
            client.flush()
        elif hasattr(client, "end"):
            client.end()
        return True
    except Exception as exc:
        warn(f"opik flush failed: {exc}")
        return False


RECOVERY_STALE_THRESHOLD_S = 60  # only recover sessions idle for this long
RECOVERY_PARTIAL_THRESHOLD_S = 300  # allow_partial only after 5 min continuous staleness


def recover_incomplete_sessions(client: Any, project_name: str) -> None:
    """Replay uncommitted transcript data for sessions that were not finalized.

    Two-tier recovery:
      - Sessions stale < RECOVERY_PARTIAL_THRESHOLD_S: replay with
        allow_partial=False (only complete turns — safe for slow tools).
      - Sessions stale >= RECOVERY_PARTIAL_THRESHOLD_S: replay with
        allow_partial=True (force-flush truly abandoned sessions).

    This prevents a long-running tool call (>60s) from being prematurely
    split into a bogus continuation turn by another session's event.
    """
    try:
        now = time.time()
        with FileLock(LOCK_FILE):
            global_state = _load_global_state()
            sessions = global_state.get("sessions", {})
            recovered = 0
            replayed = 0
            gc_sessions: set[str] = set()
            gc_subagents = 0

            for state_key in list(sessions.keys()):
                session = load_session_state(global_state, state_key)
                thread_key = _session_thread_key(session, state_key)
                if session.completed or not session.session_file:
                    continue

                # Skip sessions that are still actively receiving events
                if session.last_flush_time and (now - session.last_flush_time) < RECOVERY_STALE_THRESHOLD_S:
                    # Clear stale marker — session became active again
                    if session.stale_since:
                        session.stale_since = 0.0
                        save_session_state(global_state, state_key, session)
                    continue

                transcript_path = Path(session.session_file)
                if not transcript_path.exists():
                    if session.last_flush_time and (now - session.last_flush_time) > ORPHAN_SESSION_GC_AGE_S:
                        gc_sessions.add(state_key)
                    continue

                file_size = transcript_path.stat().st_size
                if file_size <= session.committed_offset:
                    continue

                # Track continuous staleness.  Reset if the session was
                # active after the last stale mark (stale_since < last_flush_time).
                if not session.stale_since or session.stale_since < session.last_flush_time:
                    session.stale_since = now

                # Only allow partial flush for long-stale sessions (truly abandoned)
                allow_partial = (now - session.stale_since) >= RECOVERY_PARTIAL_THRESHOLD_S

                info(
                    f"recovery: replaying session={thread_key} "
                    f"committed={session.committed_offset} file_size={file_size} "
                    f"allow_partial={allow_partial}"
                )
                emitted = flush_turns(
                    client,
                    project_name,
                    session,
                    thread_key,
                    transcript_path,
                    allow_partial=allow_partial,
                )
                if emitted <= 0:
                    save_session_state(global_state, state_key, session)
                    continue

                replayed += emitted
                if _flush_client(client):
                    commit_flush(session)
                    recovered += 1
                else:
                    rollback_flush(session)
                save_session_state(global_state, state_key, session)

            for state_key in gc_sessions:
                session = load_session_state(global_state, state_key)
                session.completed = True
                gc_subagents += _gc_subagent_state(
                    global_state,
                    removed_parent_key=_session_thread_key(session, state_key),
                    removed_parent_file=session.session_file,
                )
                sessions.pop(state_key, None)

            if recovered:
                info(f"recovery: recovered {recovered} sessions replayed_turns={replayed}")
            if gc_sessions or gc_subagents:
                info(
                    "recovery: gc removed "
                    f"{len(gc_sessions)} stale sessions and {gc_subagents} stale subagents"
                )
            _save_global_state(global_state)
    except Exception as exc:
        warn(f"recovery scan failed: {exc}")


def _gc_subagent_state(
    global_state: dict[str, Any], removed_parent_key: str = "", removed_parent_file: str = "",
) -> int:
    subagents = global_state.get("subagents", {})
    removed = 0
    for child_key, raw in list(subagents.items()):
        if not isinstance(raw, dict):
            continue
        parent_key = str(raw.get("parent_session_key", ""))
        parent_file = _normalize_session_file(str(raw.get("parent_session_file", "")))
        if (
            (removed_parent_key and parent_key == removed_parent_key)
            or (removed_parent_file and parent_file == removed_parent_file)
        ):
            del subagents[child_key]
            removed += 1
    return removed


def _event_transcript_path(event: dict[str, Any], event_name: str) -> Path | None:
    session_file = _normalize_session_file(str(event.get("sessionFile", "")))
    if not session_file:
        debug(f"{event_name}: missing sessionFile for session={event.get('sessionKey', '')}")
        return None
    return Path(session_file)


# ── Event handlers ────────────────────────────────────────────────────────────

def handle_session_start(event: dict[str, Any], client: Any, project_name: str,
                         global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        debug(f"session_start: child session={session_key} tracked under parent")
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    if not session.trace_start_ts:
        session.trace_start_ts = datetime.now(timezone.utc).isoformat()
    if event.get("resumedFrom"):
        session.resumed_from = str(event.get("resumedFrom"))
    ensure_session_trace(client, project_name, session_key, session)
    _save_bound_session_state(global_state, state_key, session)
    debug(f"session_start: session={session_key}")


def handle_llm_input(event: dict[str, Any], client: Any, project_name: str,
                     global_state: dict[str, Any]) -> None:
    """Record session as active. Set trace name from model if first event."""
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        debug(f"llm_input: child session={session_key}")
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))

    model = event.get("model", "")
    channel = event.get("channelId", "")
    if not session.trace_name and model:
        session.trace_name = _default_trace_name(session, session_key, model, channel)

    if not session.trace_start_ts:
        session.trace_start_ts = datetime.now(timezone.utc).isoformat()

    # Mark as active so recovery skips this session
    session.last_flush_time = time.time()

    _save_bound_session_state(global_state, state_key, session)
    debug(f"llm_input: session={session_key} model={model}")


def handle_before_agent_start(event: dict[str, Any], client: Any, project_name: str,
                              global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    if not session.trace_start_ts:
        session.trace_start_ts = datetime.now(timezone.utc).isoformat()
    trace_id = ensure_session_trace(client, project_name, session_key, session)
    now = _event_time(event)
    # Clear any stale turn tracking from a prior turn; emit_turn owns the span.
    session.current_turn_span_id = None
    session.current_turn_start_ts = None
    # Predict the next turn_idx emit_turn will assign (emitted_turns + 0) so the
    # tool lifecycle spans parent to the *same* span emit_turn later upserts.
    # turn_number is a hook-fire counter that drifts from transcript turn count
    # (resumes, tool-driven continuations) and is unsafe to use for span ids.
    session.current_turn_index = session.pending_emitted_turns
    session.turn_number = max(session.turn_number, session.current_turn_index) + 1
    session.current_turn_span_id = _deterministic_span_id(
        trace_id, "session", session_key, "turn", str(session.current_turn_index)
    )
    session.current_turn_start_ts = now.isoformat()
    _save_bound_session_state(global_state, state_key, session)
    debug(f"before_agent_start: session={session_key} turn={session.current_turn_index}")


def handle_llm_output(event: dict[str, Any], client: Any, project_name: str,
                      global_state: dict[str, Any]) -> None:
    """Incremental parse and emit completed turns. Trace update happens post-commit in main()."""
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        debug(f"llm_output: child session={session_key}")
        return

    transcript_path = _event_transcript_path(event, "llm_output")
    if transcript_path is None:
        return
    state_key, session = _load_bound_session_state(global_state, session_key, str(transcript_path))

    emitted = flush_turns(client, project_name, session, session_key, transcript_path)
    if emitted > 0:
        close_current_turn_span(session)
    _save_bound_session_state(global_state, state_key, session)
    debug(f"llm_output: session={session_key} emitted={emitted}")


def handle_before_tool_call(event: dict[str, Any], client: Any, project_name: str,
                            global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        return

    tool_call_id = event.get("toolCallId")
    if not tool_call_id:
        return
    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    start_time = _event_time(event)
    session.pending_tool_calls[str(tool_call_id)] = start_time.timestamp()
    # No lifecycle span — emit_turn builds the tool span with correct nesting
    # (turn → round → llm → tool) from JSONL data after the tool completes.
    _save_bound_session_state(global_state, state_key, session)
    debug(f"before_tool_call: session={session_key} tool_call={tool_call_id}")


def handle_after_tool_call(event: dict[str, Any], client: Any, project_name: str,
                           global_state: dict[str, Any]) -> None:
    """Incremental parse on every tool completion (no throttling)."""
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        debug(f"after_tool_call: child session={session_key}")
        return

    transcript_path = _event_transcript_path(event, "after_tool_call")
    if transcript_path is None:
        return
    state_key, session = _load_bound_session_state(global_state, session_key, str(transcript_path))
    tool_call_id = str(event.get("toolCallId") or "")
    if tool_call_id:
        session.pending_tool_calls.pop(tool_call_id, None)
    # No lifecycle span — emit_turn builds the tool span with correct nesting
    # (turn → round → llm → tool) from JSONL data on this flush.
    emitted = flush_turns(client, project_name, session, session_key, transcript_path)
    _save_bound_session_state(global_state, state_key, session)
    debug(f"after_tool_call: session={session_key} emitted={emitted}")


def handle_before_compaction(event: dict[str, Any], client: Any, project_name: str,
                             global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    trace_id = ensure_session_trace(client, project_name, session_key, session)
    started_at = _event_time(event)
    session.compaction_span_id = _deterministic_span_id(trace_id, "compaction", str(session.compactions_count))
    session.compaction_start_ts = started_at.isoformat()
    upsert_lifecycle_span(
        client,
        project_name,
        trace_id,
        session.compaction_span_id,
        "compaction",
        started_at,
        started_at,
        metadata=dict(event.get("compaction") or {}),
        input_payload={"status": "started"},
        tags=["openclaw", "compaction"],
    )
    _save_bound_session_state(global_state, state_key, session)
    debug(f"before_compaction: session={session_key}")


def handle_after_compaction(event: dict[str, Any], client: Any, project_name: str,
                            global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    trace_id = ensure_session_trace(client, project_name, session_key, session)
    end_time = _event_time(event)
    span_id = session.compaction_span_id or _deterministic_span_id(trace_id, "compaction", str(session.compactions_count))
    start_time = parse_ts(session.compaction_start_ts or "") or end_time
    meta = dict(event.get("compaction") or {})
    if "messageCount" in meta and "compactedCount" in meta:
        try:
            meta["messagesDropped"] = int(meta["messageCount"]) - int(meta["compactedCount"])
        except Exception:
            pass
    upsert_lifecycle_span(
        client,
        project_name,
        trace_id,
        span_id,
        "compaction",
        start_time,
        end_time,
        metadata=meta,
        output_payload={"status": "completed"},
        tags=["openclaw", "compaction"],
    )
    session.compaction_span_id = None
    session.compaction_start_ts = None
    session.compactions_count += 1
    _save_bound_session_state(global_state, state_key, session)
    debug(f"after_compaction: session={session_key}")


def handle_before_reset(event: dict[str, Any], client: Any, project_name: str,
                        global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        return

    transcript_path = _event_transcript_path(event, "before_reset")
    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    session.ended_reason = str(event.get("resetReason") or "reset")
    if transcript_path and transcript_path.exists():
        flush_turns(client, project_name, session, session_key, transcript_path, allow_partial=True)
    now = _event_time(event)
    close_current_turn_span(session)
    finalize_pending_subagent_attempts(client, project_name, session_key, session, now)
    session.completed = True
    _save_bound_session_state(global_state, state_key, session)
    info(f"before_reset: session={session_key}")


def handle_agent_end(event: dict[str, Any], client: Any, project_name: str,
                     global_state: dict[str, Any]) -> None:
    """Final flush. Trace finalization is deferred to main() after commit_flush
    so that session stats are accurate."""
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        _update_child_subagent_from_event(global_state, event)
        debug(f"agent_end: child session={session_key}")
        return

    transcript_path = _event_transcript_path(event, "agent_end")
    if transcript_path is None:
        return
    state_key, session = _load_bound_session_state(global_state, session_key, str(transcript_path))

    # Final flush with partial turns allowed
    emitted = flush_turns(
        client, project_name, session, session_key, transcript_path, allow_partial=True,
    )

    # Mark for finalization — actual finalize_trace runs after commit_flush in main()
    now = _event_time(event)
    close_current_turn_span(session)
    finalize_pending_subagent_attempts(client, project_name, session_key, session, now)
    session.ended_reason = session.ended_reason or "agent_end"
    session.completed = True
    _save_bound_session_state(global_state, state_key, session)
    info(f"agent_end: session={session_key} emitted_now={emitted}")


def handle_session_end(event: dict[str, Any], client: Any, project_name: str,
                       global_state: dict[str, Any]) -> None:
    session_key = event["sessionKey"]
    if _is_child_session(global_state, session_key):
        sub = _subagent_session(global_state, session_key)
        if sub:
            sub.end_reason = sub.end_reason or str(event.get("sessionEndReason") or "subagent_done")
            sub.ended_at = sub.ended_at or _event_time(event).isoformat()
            save_subagent_state(global_state, session_key, sub)
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    reason = str(event.get("sessionEndReason") or "unknown")
    session.session_end_info = {
        "reason": reason,
        "messageCount": event.get("sessionEndMessageCount"),
        "durationMs": event.get("sessionEndDurationMs"),
        "nextSessionId": event.get("nextSessionId"),
        "nextSessionKey": event.get("nextSessionKey"),
        "transcriptArchived": event.get("transcriptArchived"),
    }
    session.ended_reason = session.ended_reason or reason
    transcript_path = _event_transcript_path(event, "session_end")
    if not session.completed and transcript_path and transcript_path.exists():
        flush_turns(client, project_name, session, session_key, transcript_path, allow_partial=True)
        now = _event_time(event)
        close_current_turn_span(session)
        finalize_pending_subagent_attempts(client, project_name, session_key, session, now)
        session.completed = True
    _save_bound_session_state(global_state, state_key, session)
    info(f"session_end: session={session_key} reason={reason}")


def handle_subagent_spawning(event: dict[str, Any], client: Any, project_name: str,
                             global_state: dict[str, Any]) -> None:
    child_key = str(event.get("childSessionKey") or "")
    session_key = event["sessionKey"]
    if not child_key:
        return

    parent_state_key, parent_trace_id, parent_session = _resolve_parent_trace_id(
        global_state, session_key, str(event.get("sessionFile", ""))
    )
    if not parent_trace_id or not parent_session:
        return
    state_key = parent_state_key or session_key
    sub = load_subagent_state(global_state, child_key) or SubagentState(agent_id=str(event.get("childAgentId", "")))
    sub.agent_id = str(event.get("childAgentId") or sub.agent_id)
    sub.parent_session_key = session_key
    sub.parent_session_file = parent_session.session_file
    sub.agent_span_id = sub.agent_span_id or _deterministic_span_id(parent_trace_id, "subagent-attempt", child_key)
    sub.subagent_label = str(event.get("subagentLabel") or sub.subagent_label)
    sub.subagent_mode = str(event.get("subagentMode") or sub.subagent_mode)
    sub.started_at = sub.started_at or _event_time(event).isoformat()
    save_subagent_state(global_state, child_key, sub)

    pending = {
        "child_session_key": child_key,
        "child_agent_id": sub.agent_id,
        "label": sub.subagent_label,
        "mode": sub.subagent_mode,
        "started_at": sub.started_at,
        "span_id": sub.agent_span_id,
    }
    parent_session.pending_subagents[child_key] = pending
    create_or_update_subagent_span(
        client,
        project_name,
        parent_trace_id,
        parent_trace_id,
        sub.agent_span_id,
        child_key,
        sub.agent_id,
        parse_ts(sub.started_at),
        parse_ts(sub.started_at),
        sub.emitted_turns,
        {
            "subagent_label": sub.subagent_label,
            "subagent_mode": sub.subagent_mode,
            "status": "started",
        },
        {"status": "started"},
    )
    _save_bound_session_state(global_state, state_key, parent_session)
    debug(f"subagent_spawning: child={child_key} parent={session_key}")


def handle_subagent_delivery_target(event: dict[str, Any], client: Any, project_name: str,
                                    global_state: dict[str, Any]) -> None:
    child_key = str(event.get("childSessionKey") or "")
    session_key = event["sessionKey"]
    if not child_key:
        return

    state_key, session = _load_bound_session_state(global_state, session_key, str(event.get("sessionFile", "")))
    delivery = dict(event.get("subagentDelivery") or {})
    session.subagent_delivery[child_key] = delivery
    sub = load_subagent_state(global_state, child_key) or SubagentState(agent_id="")
    sub.parent_session_key = session_key or sub.parent_session_key
    sub.parent_session_file = session.session_file or sub.parent_session_file
    sub.requester_origin = dict(delivery.get("requesterOrigin") or sub.requester_origin)
    sub.spawn_mode = str(delivery.get("spawnMode") or sub.spawn_mode)
    if delivery.get("expectsCompletionMessage") is not None:
        sub.expects_completion_msg = bool(delivery.get("expectsCompletionMessage"))
    save_subagent_state(global_state, child_key, sub)
    _save_bound_session_state(global_state, state_key, session)
    debug(f"subagent_delivery_target: child={child_key} parent={session_key}")


def handle_subagent_spawned(event: dict[str, Any], client: Any, project_name: str,
                            global_state: dict[str, Any]) -> None:
    """Register subagent for tracking."""
    child_key = event.get("childSessionKey")
    if not child_key:
        return
    session_key = event["sessionKey"]
    child_agent_id = str(event.get("childAgentId", ""))
    parent_state_key, parent_trace_id, parent_session = _resolve_parent_trace_id(
        global_state, session_key, str(event.get("sessionFile", ""))
    )
    if not parent_trace_id or not parent_session:
        return
    state_key = parent_state_key or session_key

    sub = load_subagent_state(global_state, child_key) or SubagentState(agent_id=child_agent_id)
    sub.agent_id = child_agent_id or sub.agent_id
    sub.parent_session_key = session_key or sub.parent_session_key
    sub.parent_session_file = parent_session.session_file or sub.parent_session_file
    sub.agent_span_id = sub.agent_span_id or _deterministic_span_id(parent_trace_id, "subagent-attempt", child_key)
    sub.started_at = sub.started_at or _event_time(event).isoformat()
    save_subagent_state(global_state, child_key, sub)
    parent_session.pending_subagents.setdefault(child_key, {
        "child_session_key": child_key,
        "child_agent_id": sub.agent_id,
        "started_at": sub.started_at,
        "span_id": sub.agent_span_id,
    })
    create_or_update_subagent_span(
        client,
        project_name,
        parent_trace_id,
        parent_trace_id,
        sub.agent_span_id,
        child_key,
        sub.agent_id,
        parse_ts(sub.started_at),
        parse_ts(sub.started_at),
        sub.emitted_turns,
        {"status": "spawned"},
        None,
        {"status": "spawned", "turns": sub.emitted_turns},
    )
    _save_bound_session_state(global_state, state_key, parent_session)
    debug(f"subagent_spawned: child={child_key} parent={session_key}")


def handle_subagent_ended(event: dict[str, Any], client: Any, project_name: str,
                          global_state: dict[str, Any]) -> None:
    """Parse subagent transcript and emit as child spans."""
    child_key = event.get("childSessionKey")
    if not child_key:
        return

    sub = load_subagent_state(global_state, child_key) or SubagentState(
        agent_id=str(event.get("childAgentId", "")),
        parent_session_key=str(event.get("sessionKey", "")),
        parent_session_file=_normalize_session_file(str(event.get("sessionFile", ""))),
    )
    if not sub.parent_session_key:
        sub.parent_session_key = str(event.get("sessionKey", ""))
    if not sub.parent_session_file:
        sub.parent_session_file = _normalize_session_file(str(event.get("sessionFile", "")))

    parent_state_key, parent_trace_id, parent_session = _resolve_parent_trace_id(
        global_state, sub.parent_session_key, sub.parent_session_file
    )
    if not parent_trace_id or not parent_session:
        sub.finished = True
        save_subagent_state(global_state, child_key, sub)
        return
    was_finished = sub.finished
    end_time = _event_time(event)
    sub.end_reason = str(event.get("subagentEndReason") or sub.end_reason or event.get("reason") or "")
    sub.end_outcome = str(event.get("subagentOutcome") or sub.end_outcome or event.get("outcome") or "")
    sub.ended_at = str(event.get("subagentEndedAt") or sub.ended_at or event.get("endedAt") or end_time.isoformat())
    sub.error = str(event.get("subagentError") or sub.error or event.get("error") or "")
    sub.target_kind = str(event.get("subagentTargetKind") or sub.target_kind or event.get("targetKind") or "")
    if event.get("subagentSendFarewell") is not None:
        sub.send_farewell = bool(event.get("subagentSendFarewell"))
    sub.agent_span_id = sub.agent_span_id or _deterministic_span_id(parent_trace_id, "subagent-attempt", child_key)
    sub.started_at = sub.started_at or sub.ended_at

    # Try to find subagent transcript
    if sub.transcript_path and Path(sub.transcript_path).exists():
        transcript_path = Path(sub.transcript_path)
    else:
        # Construct path from parent session file
        parent_file = _event_transcript_path(event, "subagent_ended")
        if parent_file and parent_file.exists():
            session_id = parent_file.stem
            subagent_file = parent_file.parent / session_id / "subagents" / f"agent-{sub.agent_id}.jsonl"
            if subagent_file.exists():
                transcript_path = subagent_file
                sub.transcript_path = str(subagent_file)
            else:
                transcript_path = None
        else:
            transcript_path = None

    turns: list[V3Turn] = []
    final_snapshot = sub.prev_usage_snapshot or {}
    if transcript_path and transcript_path.exists():
        turns, final_snapshot = parse_transcript_segment(
            transcript_path, sub.turn_start_offset, sub.prev_usage_snapshot
        )

    container_start = parse_ts(sub.started_at) if sub.started_at else end_time
    if turns:
        container_start = parse_ts(turns[0].user_timestamp)
    container_end = parse_ts(sub.ended_at) if sub.ended_at else end_time
    if turns:
        container_end = max(container_end, parse_ts(turns[-1].end_timestamp or turns[-1].user_timestamp))
    create_or_update_subagent_span(
        client,
        project_name,
        parent_trace_id,
        parent_trace_id,
        sub.agent_span_id,
        child_key,
        sub.agent_id,
        container_start,
        container_end,
        sub.emitted_turns + len(turns),
        {
            "subagent_label": sub.subagent_label,
            "subagent_mode": sub.subagent_mode,
            "requester_origin": sub.requester_origin,
            "spawn_mode": sub.spawn_mode,
            "expects_completion_msg": sub.expects_completion_msg,
            "end_reason": sub.end_reason,
            "end_outcome": sub.end_outcome,
            "ended_at": sub.ended_at,
            "error": sub.error,
            "target_kind": sub.target_kind,
            "send_farewell": sub.send_farewell,
        },
        None,
        {
            "turns": sub.emitted_turns + len(turns),
            "status": sub.end_outcome or "completed",
            "reason": sub.end_reason,
            "error": sub.error,
        },
    )

    # Emit subagent turns under parent trace with agent span as container
    for idx, turn in enumerate(turns):
        try:
            sub_turn_idx = sub.emitted_turns + idx
            emit_turn(
                client, project_name, parent_trace_id,
                child_key, turn, sub_turn_idx,
                parent_span_id=sub.agent_span_id,
                id_namespace=("subagent", child_key),
                depth=1,
            )
        except Exception as exc:
            debug(f"subagent turn emission failed: {exc}")

    if turns:
        sub.emitted_turns += len(turns)
        max_completed_offset = max(turn.end_offset for turn in turns)
        sub.turn_start_offset += max_completed_offset
    sub.prev_usage_snapshot = final_snapshot
    sub.finished = True
    if not was_finished:
        parent_session.session_total_subagent_calls += 1
    parent_session.pending_subagents.pop(child_key, None)
    parent_session.subagent_delivery.pop(child_key, None)
    save_subagent_state(global_state, child_key, sub)
    _save_bound_session_state(global_state, parent_state_key or sub.parent_session_key, parent_session)
    debug(f"subagent_ended: child={child_key} turns={len(turns)}")


# ── Main ──────────────────────────────────────────────────────────────────────

EVENT_HANDLERS = {
    "session_start": handle_session_start,
    "before_agent_start": handle_before_agent_start,
    "llm_input": handle_llm_input,
    "llm_output": handle_llm_output,
    "before_tool_call": handle_before_tool_call,
    "after_tool_call": handle_after_tool_call,
    "before_compaction": handle_before_compaction,
    "after_compaction": handle_after_compaction,
    "before_reset": handle_before_reset,
    "agent_end": handle_agent_end,
    "session_end": handle_session_end,
    "subagent_spawning": handle_subagent_spawning,
    "subagent_delivery_target": handle_subagent_delivery_target,
    "subagent_spawned": handle_subagent_spawned,
    "subagent_ended": handle_subagent_ended,
}


def main() -> int:
    _install_process_timeout()
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return 0
        event = json.loads(raw)
    except (json.JSONDecodeError, Exception) as exc:
        warn(f"failed to parse stdin event: {exc}")
        return 1

    event_name = event.get("event", "")
    handler = EVENT_HANDLERS.get(event_name)
    if not handler:
        debug(f"unknown event: {event_name}")
        return 0

    session_key = event.get("sessionKey")
    if not session_key:
        debug(f"no sessionKey in event: {event_name}")
        return 0

    apply_opik_env_overrides()

    # Apply config from plugin (passed via event.config)
    config = event.get("config", {})
    if config.get("dryRun"):
        global DRY_RUN
        DRY_RUN = True

    # Init Opik client
    if Opik is None:
        warn("opik SDK not installed, skipping")
        return 0

    project_name = (
        config.get("opikProjectName")
        or os.environ.get("OPIK_PROJECT_NAME")
        or DEFAULT_PROJECT
    )

    try:
        client = Opik(project_name=project_name)
    except Exception as exc:
        warn(f"Opik client init failed: {exc}")
        return 0

    recover_incomplete_sessions(client, project_name)

    # Run handler under file lock
    try:
        with FileLock(LOCK_FILE):
            global_state = _load_global_state()
            session_key = event.get("sessionKey", "")
            session_file = _normalize_session_file(str(event.get("sessionFile", "")))
            state_key = resolve_state_key(global_state, session_key, session_file)
            prior_completed = False
            sessions = global_state.get("sessions", {})
            if state_key and state_key in sessions:
                prior_completed = load_session_state(global_state, state_key).completed
            handler(event, client, project_name, global_state)
            state_key = resolve_state_key(global_state, session_key, session_file)
            if state_key and state_key in global_state.get("sessions", {}):
                session = load_session_state(global_state, state_key)
                thread_key = _session_thread_key(session, session_key)
                flush_ok = _flush_client(client)
                if flush_ok:
                    commit_flush(session)
                    # Post-commit: update trace with accurate committed stats
                    if session.trace_id:
                        trace_id = session.trace_id
                        trace_name = session.trace_name or _default_trace_name(session, thread_key)
                        start_time = parse_ts(session.trace_start_ts or "")
                        end_time = parse_ts(session.last_turn_ts or "")
                        if end_time < start_time:
                            end_time = datetime.now(timezone.utc)
                        if session.completed and not prior_completed:
                            # agent_end: finalize trace with final stats
                            finalize_trace(
                                client, project_name, trace_id, trace_name,
                                thread_key, start_time, end_time, session,
                            )
                            _flush_client(client)  # flush the finalize update
                        elif session.pending_emitted_turns > 0 or session.emitted_turns > 0:
                            # Real-time: update trace end_time / metadata
                            try:
                                ensure_trace(
                                    client, project_name, trace_id, trace_name,
                                    thread_key, start_time, end_time, session,
                                )
                            except Exception:
                                pass
                else:
                    rollback_flush(session)
                    session.completed = prior_completed
                save_session_state(global_state, state_key, session)
            _save_global_state(global_state)
    except ProcessTimeout as exc:
        warn(str(exc))
        return 1
    except Exception as exc:
        warn(f"handler {event_name} failed: {exc}")
        return 1
    finally:
        _clear_process_timeout()

    return 0


if __name__ == "__main__":
    sys.exit(main())
