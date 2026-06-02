#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Shanghai Innovation Institute
"""
Claude Code -> Opik realtime hook.

This script is designed to be called by Claude Code command hooks. It:
- reads the hook payload from stdin
- reads Claude transcript JSONL from the current-turn start offset
- parses turns with full LLM-call / tool / agent span detail (v3 logic)
- emits completed turns to Opik as nested spans under one session trace

Events handled:
  UserPromptSubmit  - create trace, record offset, create Turn span
  PostToolUse       - throttled flush (every 5s)
  PostToolUseFailure- same as PostToolUse
  SubagentStart     - record subagent id → parent mapping
  SubagentStop      - parse subagent transcript and emit as child spans
  PreCompact        - flush, create Compaction span, reset offset
  Stop              - full flush of current turn
  SessionEnd        - final flush + finalize trace

The script is fail-open by design: configuration or network problems should
never block Claude Code itself.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from opik import Opik
    from opik import id_helpers
except Exception:
    Opik = None
    id_helpers = None

try:
    from uuid6 import uuid7 as _uuid7
except Exception:
    _uuid7 = None


STATE_DIR = Path.home() / ".claude" / "state"
PROJECTS_DIR = Path.home() / ".claude" / "projects"
LOG_FILE = STATE_DIR / "opik_hook.log"
PROJECTS_LOG_FILE = PROJECTS_DIR / "opik_hook.log"
STATE_FILE = STATE_DIR / "opik_hook_state.json"
LOCK_FILE = STATE_DIR / "opik_hook_state.lock"
AGENTS_DIR = STATE_DIR / "agents"
LOGS_DIR = Path("/logs")
BACKUP_STATE_FILE = LOGS_DIR / "opik-runtime-state.json"
BACKUP_TRANSCRIPT_FILE = LOGS_DIR / "opik-runtime-transcript.jsonl"

DEBUG = os.environ.get("CC_OPIK_DEBUG", "").lower() == "true"
DRY_RUN = os.environ.get("CC_OPIK_DRY_RUN", "").lower() == "true"
MAX_TEXT_CHARS = int(os.environ.get("CC_OPIK_MAX_TEXT_CHARS", "20000"))
DEFAULT_PROJECT = os.environ.get("CC_OPIK_PROJECT", "claude-code-realtime")
FLUSH_INTERVAL_S = 5
TRANSCRIPT_WAIT_TIMEOUT_S = float(os.environ.get("CC_OPIK_TRANSCRIPT_WAIT_TIMEOUT_S", "2.0"))
TRANSCRIPT_WAIT_INTERVAL_S = float(os.environ.get("CC_OPIK_TRANSCRIPT_WAIT_INTERVAL_S", "0.05"))


# ── Runtime env helpers ───────────────────────────────────────────────────────

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


def runtime_context_metadata() -> dict[str, Any]:
    meta: dict[str, Any] = {}
    mapping = {
        "tb_task_id": _env_first("TB_TASK_ID"),
        "tb_run_id": _env_first("TB_RUN_ID"),
        "tb_dataset": _env_first("TB_DATASET"),
        "tb_trial_id": _env_first("TB_TRIAL_ID"),
        "opik_project_name": _env_first("OPIK_PROJECT_NAME", "CC_OPIK_PROJECT"),
        "opik_url": _env_first("OPIK_URL_OVERRIDE", "OPIK_URL"),
    }
    for key, value in mapping.items():
        if value:
            meta[key] = value
    return meta


# ── Logging ──────────────────────────────────────────────────────────────────

def _log(level: str, message: str) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{stamp} [{level}] {message}\n"
        for path in (LOG_FILE, PROJECTS_LOG_FILE):
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line)
    except Exception:
        pass


def debug(message: str) -> None:
    if DEBUG:
        _log("DEBUG", message)


def info(message: str) -> None:
    _log("INFO", message)


# ── File lock ─────────────────────────────────────────────────────────────────

class FileLock:
    def __init__(self, path: Path, timeout_s: float = 2.0):
        self.path = path
        self.timeout_s = timeout_s
        self._fh: Any | None = None

    def __enter__(self) -> "FileLock":
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a+", encoding="utf-8")
        try:
            import fcntl
            deadline = time.time() + self.timeout_s
            while True:
                try:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.time() > deadline:
                        break
                    time.sleep(0.05)
        except Exception:
            pass
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            import fcntl
            if self._fh is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            if self._fh is not None:
                self._fh.close()
        except Exception:
            pass


# ── Global state persistence ──────────────────────────────────────────────────

def load_state() -> dict[str, Any]:
    try:
        if not STATE_FILE.exists():
            return {}
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        debug(f"save_state failed: {exc}")


# ── Hook payload parsing ──────────────────────────────────────────────────────

def read_hook_payload() -> dict[str, Any]:
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def read_hook_payload_file(path: str) -> dict[str, Any]:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        if not raw.strip():
            return {}
        return json.loads(raw)
    except Exception:
        return {}


def _extract_payload_file_arg(argv: list[str]) -> str | None:
    if "--payload-file" not in argv:
        return None
    idx = argv.index("--payload-file")
    if idx + 1 >= len(argv):
        return None
    value = argv[idx + 1].strip()
    return value or None


def maybe_defer_session_end(payload: dict[str, Any], event_name: str, argv: list[str]) -> bool:
    """Spawn SessionEnd processing in a detached child and return quickly.

    Some runners cancel SessionEnd hooks aggressively. Deferring reduces the
    chance that the finalization path is killed before it updates trace status.
    """
    if event_name != "SessionEnd":
        return False
    if _extract_payload_file_arg(argv):
        return False
    if os.environ.get("CC_OPIK_DEFER_SESSIONEND", "true").lower() != "true":
        return False
    try:
        fd, payload_path = tempfile.mkstemp(prefix="cc-opik-sessionend-", suffix=".json")
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "SessionEnd",
            "--payload-file",
            payload_path,
        ]
        subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        info("SessionEnd deferred to detached worker")
        return True
    except Exception as exc:
        debug(f"failed to defer SessionEnd hook: {exc}")
        return False


def _transcript_has_terminal_error(transcript_path: Path | None) -> bool:
    """Detect aborted/error terminal outcomes that should not finalize as completed."""
    if not transcript_path or not transcript_path.exists():
        return False
    try:
        lines = transcript_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return False
    for raw in reversed(lines[-80:]):
        raw = raw.strip()
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except Exception:
            continue
        if event.get("type") == "result":
            result_text = str(event.get("result") or "")
            if bool(event.get("is_error")):
                return True
            if event.get("api_error_status") not in (None, ""):
                return True
            if result_text.startswith("API Error:"):
                return True
            terminal_reason = str(event.get("terminal_reason") or "")
            if terminal_reason and terminal_reason != "completed":
                return True
            return False
        if event.get("type") == "assistant":
            message = event.get("message") or {}
            if event.get("error"):
                return True
            if isinstance(message, dict):
                for item in message.get("content") or []:
                    if isinstance(item, dict) and str(item.get("text") or "").startswith("API Error:"):
                        return True
    return False


def extract_session_and_transcript(payload: dict[str, Any]) -> tuple[str | None, Path | None]:
    session_id = (
        payload.get("sessionId")
        or payload.get("session_id")
        or payload.get("session", {}).get("id")
    )
    transcript = (
        payload.get("transcriptPath")
        or payload.get("transcript_path")
        or payload.get("transcript", {}).get("path")
    )
    transcript_path = None
    if transcript:
        try:
            transcript_path = Path(transcript).expanduser().resolve()
        except Exception:
            transcript_path = None
    return session_id, transcript_path


def extract_agent_info(payload: dict[str, Any]) -> tuple[str, str, str | None]:
    agent_id = str(payload.get("agent_id") or payload.get("agentId") or "")
    agent_type = str(payload.get("agent_type") or payload.get("agentType") or "")
    agent_transcript = (
        payload.get("agent_transcript_path")
        or payload.get("agentTranscriptPath")
    )
    return agent_id, agent_type, str(agent_transcript) if agent_transcript else None


def extract_prompt(payload: dict[str, Any]) -> str:
    return str(payload.get("prompt") or payload.get("user_prompt") or "")


def extract_custom_instructions(payload: dict[str, Any]) -> str:
    return str(payload.get("custom_instructions") or payload.get("customInstructions") or "")


# Tags that indicate a "continuation" user message (not a real new user prompt).
# These should NOT start a new turn — they belong to the current turn.
_CONTINUATION_TAGS = ("task-notification", "system-reminder")

_CONTINUATION_RE = re.compile(
    r"^\s*<(" + "|".join(_CONTINUATION_TAGS) + r")[\s>]",
    re.DOTALL,
)


def _is_continuation_message(text: str) -> bool:
    """Return True if *text* is a system-injected continuation (task-notification,
    system-reminder, etc.) rather than a genuine user prompt.

    These messages appear with role=user in the transcript but should not split
    the current turn into a new one.
    """
    return bool(_CONTINUATION_RE.match(text))


def _infer_message_role(data: dict[str, Any], msg: dict[str, Any]) -> str:
    """Infer role for transcript items that omit message.role."""
    role = msg.get("role", "")
    if role in ("user", "assistant"):
        return role

    data_type = data.get("type", "")
    if data_type in ("user", "assistant"):
        return data_type

    content = msg.get("content")
    if isinstance(content, list):
        has_tool_result = any(
            isinstance(item, dict) and item.get("type") == "tool_result"
            for item in content
        )
        if has_tool_result:
            return "user"

        has_assistant_like = any(
            isinstance(item, dict) and item.get("type") in ("text", "thinking", "tool_use")
            for item in content
        )
        if has_assistant_like and msg.get("id"):
            return "assistant"

    if isinstance(content, str) and msg.get("id"):
        return "assistant"

    return ""


def hook_event_name(payload: dict[str, Any]) -> str:
    value = (
        payload.get("hook_event_name")
        or payload.get("hookEventName")
        or payload.get("event_name")
        or payload.get("eventName")
        or payload.get("event")
        or ""
    )
    if value:
        return str(value)
    if len(sys.argv) > 1 and sys.argv[1]:
        return str(sys.argv[1])
    if payload.get("tool_name") or payload.get("toolName"):
        return "PostToolUse"
    return "Stop"


def event_timestamp(payload: dict[str, Any]) -> str | None:
    candidates = [
        payload.get("timestamp"),
        payload.get("event_timestamp"),
        payload.get("eventTimestamp"),
        payload.get("time"),
        payload.get("hook_event_timestamp"),
        payload.get("hookEventTimestamp"),
    ]
    session_obj = payload.get("session")
    if isinstance(session_obj, dict):
        candidates.extend([
            session_obj.get("timestamp"),
            session_obj.get("started_at"),
            session_obj.get("ended_at"),
        ])
    for value in candidates:
        if isinstance(value, str) and value:
            return value
    return None


def wait_for_transcript(path: Path) -> bool:
    if path.exists():
        return True
    deadline = time.time() + max(0.0, TRANSCRIPT_WAIT_TIMEOUT_S)
    while time.time() < deadline:
        time.sleep(max(0.0, TRANSCRIPT_WAIT_INTERVAL_S))
        if path.exists():
            return True
    return path.exists()


def fallback_session_and_transcript_for_session_end() -> tuple[str | None, Path | None]:
    """Best-effort fallback when SessionEnd payload is empty/malformed.

    Some runtimes occasionally invoke SessionEnd with an empty payload. In task
    containers we generally have a single active transcript, so picking the most
    recently updated JSONL provides a practical recovery path.
    """
    try:
        candidates = sorted(
            PROJECTS_DIR.glob("*/*.jsonl"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except Exception:
        return None, None
    for path in candidates:
        try:
            if not path.is_file():
                continue
            session_id = path.stem
            if session_id:
                return session_id, path.resolve()
        except Exception:
            continue
    return None, None


def _subagent_state_path(key: str) -> Path:
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    return AGENTS_DIR / f"{key}_subagents.json"


# ── Session state ─────────────────────────────────────────────────────────────

def state_key(session_id: str, transcript_path: Path) -> str:
    raw = f"{session_id}::{transcript_path}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class SessionState:
    turn_start_offset: int = 0      # byte offset in transcript at start of current turn
    emitted_turns: int = 0
    trace_created: bool = False
    trace_name: str | None = None
    trace_id: str | None = None
    trace_finalized: bool = False
    trace_start_ts: str | None = None
    last_turn_ts: str | None = None
    last_flush_time: float = 0.0
    turn_number: int = 0
    turn_span_id: str | None = None
    # Incremental usage: previous LLM call's cumulative snapshot for delta computation
    prev_usage_snapshot: dict[str, int] | None = None
    # Running session-wide token totals (accumulated across flushes)
    session_api_billed_input: int = 0
    session_api_billed_output: int = 0
    session_api_billed_cache_read: int = 0
    session_api_billed_cache_creation: int = 0
    session_incremental_input: int = 0
    session_incremental_output: int = 0
    session_incremental_cache_read: int = 0
    session_incremental_cache_creation: int = 0
    session_total_llm_calls: int = 0
    session_total_tool_calls: int = 0
    session_total_subagent_calls: int = 0
    session_tool_success: int = 0
    session_tool_error: int = 0
    session_models: list[str] | None = None

    def __post_init__(self) -> None:
        if self.session_models is None:
            self.session_models = []


@dataclass
class SubagentState:
    agent_id: str
    agent_type: str = ""
    agent_span_id: str = ""
    transcript_path: str = ""
    turn_start_offset: int = 0
    emitted_turns: int = 0
    prev_usage_snapshot: dict[str, int] | None = None
    started_at: str = ""
    finished: bool = False
    parent_span_id: str | None = None
    last_end_ts: str | None = None


def load_subagent_states(key: str) -> dict[str, SubagentState]:
    path = _subagent_state_path(key)
    try:
        if not path.exists():
            return {}
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        states: dict[str, SubagentState] = {}
        for agent_id, item in raw.items():
            if not isinstance(item, dict):
                continue
            states[agent_id] = SubagentState(
                agent_id=str(item.get("agent_id") or agent_id),
                agent_type=str(item.get("agent_type") or ""),
                agent_span_id=str(item.get("agent_span_id") or ""),
                transcript_path=str(item.get("transcript_path") or ""),
                turn_start_offset=int(item.get("turn_start_offset", 0)),
                emitted_turns=int(item.get("emitted_turns", 0)),
                prev_usage_snapshot=item.get("prev_usage_snapshot")
                if isinstance(item.get("prev_usage_snapshot"), dict) else None,
                started_at=str(item.get("started_at") or ""),
                finished=bool(item.get("finished", False)),
                parent_span_id=item.get("parent_span_id"),
                last_end_ts=item.get("last_end_ts"),
            )
        return states
    except Exception as exc:
        debug(f"load_subagent_states failed: {exc}")
        return {}


def save_subagent_states(key: str, subagents: dict[str, SubagentState]) -> None:
    try:
        path = _subagent_state_path(key)
        tmp = path.with_suffix(".tmp")
        payload = {
            agent_id: {
                "agent_id": sa.agent_id,
                "agent_type": sa.agent_type,
                "agent_span_id": sa.agent_span_id,
                "transcript_path": sa.transcript_path,
                "turn_start_offset": sa.turn_start_offset,
                "emitted_turns": sa.emitted_turns,
                "prev_usage_snapshot": sa.prev_usage_snapshot,
                "started_at": sa.started_at,
                "finished": sa.finished,
                "parent_span_id": sa.parent_span_id,
                "last_end_ts": sa.last_end_ts,
            }
            for agent_id, sa in subagents.items()
        }
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
    except Exception as exc:
        debug(f"save_subagent_states failed: {exc}")


def delete_subagent_states(key: str) -> None:
    try:
        path = _subagent_state_path(key)
        if path.exists():
            path.unlink()
    except Exception:
        pass


def load_session_state(global_state: dict[str, Any], key: str) -> SessionState:
    raw = global_state.get(key, {})
    return SessionState(
        turn_start_offset=int(raw.get("turn_start_offset", 0)),
        emitted_turns=int(raw.get("emitted_turns", 0)),
        trace_created=bool(raw.get("trace_created", False)),
        trace_name=raw.get("trace_name"),
        trace_id=raw.get("trace_id"),
        trace_finalized=bool(raw.get("trace_finalized", False)),
        trace_start_ts=raw.get("trace_start_ts"),
        last_turn_ts=raw.get("last_turn_ts"),
        last_flush_time=float(raw.get("last_flush_time", 0.0)),
        turn_number=int(raw.get("turn_number", 0)),
        turn_span_id=raw.get("turn_span_id"),
        prev_usage_snapshot=raw.get("prev_usage_snapshot"),
        session_api_billed_input=int(raw.get("session_api_billed_input", 0)),
        session_api_billed_output=int(raw.get("session_api_billed_output", 0)),
        session_api_billed_cache_read=int(raw.get("session_api_billed_cache_read", 0)),
        session_api_billed_cache_creation=int(raw.get("session_api_billed_cache_creation", 0)),
        session_incremental_input=int(raw.get("session_incremental_input", 0)),
        session_incremental_output=int(raw.get("session_incremental_output", 0)),
        session_incremental_cache_read=int(raw.get("session_incremental_cache_read", 0)),
        session_incremental_cache_creation=int(raw.get("session_incremental_cache_creation", 0)),
        session_total_llm_calls=int(raw.get("session_total_llm_calls", 0)),
        session_total_tool_calls=int(raw.get("session_total_tool_calls", 0)),
        session_total_subagent_calls=int(raw.get("session_total_subagent_calls", 0)),
        session_tool_success=int(raw.get("session_tool_success", 0)),
        session_tool_error=int(raw.get("session_tool_error", 0)),
        session_models=raw.get("session_models") if isinstance(raw.get("session_models"), list) else [],
    )


def save_session_state(global_state: dict[str, Any], key: str, session: SessionState) -> None:
    global_state[key] = {
        "turn_start_offset": session.turn_start_offset,
        "emitted_turns": session.emitted_turns,
        "trace_created": session.trace_created,
        "trace_name": session.trace_name,
        "trace_id": session.trace_id,
        "trace_finalized": session.trace_finalized,
        "trace_start_ts": session.trace_start_ts,
        "last_turn_ts": session.last_turn_ts,
        "last_flush_time": session.last_flush_time,
        "turn_number": session.turn_number,
        "turn_span_id": session.turn_span_id,
        "prev_usage_snapshot": session.prev_usage_snapshot,
        "session_api_billed_input": session.session_api_billed_input,
        "session_api_billed_output": session.session_api_billed_output,
        "session_api_billed_cache_read": session.session_api_billed_cache_read,
        "session_api_billed_cache_creation": session.session_api_billed_cache_creation,
        "session_incremental_input": session.session_incremental_input,
        "session_incremental_output": session.session_incremental_output,
        "session_incremental_cache_read": session.session_incremental_cache_read,
        "session_incremental_cache_creation": session.session_incremental_cache_creation,
        "session_total_llm_calls": session.session_total_llm_calls,
        "session_total_tool_calls": session.session_total_tool_calls,
        "session_total_subagent_calls": session.session_total_subagent_calls,
        "session_tool_success": session.session_tool_success,
        "session_tool_error": session.session_tool_error,
        "session_models": session.session_models,
        "updated": datetime.now(timezone.utc).isoformat(),
    }


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def persist_runtime_backup_to_dir(
    *,
    logs_dir: Path,
    state: dict[str, Any],
    key: str,
    session_id: str,
    transcript_path: Path,
    project_name: str,
) -> None:
    try:
        backup_state_file = logs_dir / BACKUP_STATE_FILE.name
        backup_transcript_file = logs_dir / BACKUP_TRANSCRIPT_FILE.name
        session_raw = state.get(key, {})
        payload = {
            "key": key,
            "session_id": session_id,
            "project_name": project_name,
            "session_state": session_raw,
            "backup_state_path": str(backup_state_file),
            "backup_transcript_path": str(backup_transcript_file),
            "source_transcript_path": str(transcript_path),
            "updated": datetime.now(timezone.utc).isoformat(),
        }
        _atomic_write_text(backup_state_file, json.dumps(payload, indent=2, sort_keys=True))
        if transcript_path.exists():
            _atomic_write_text(
                backup_transcript_file,
                transcript_path.read_text(encoding="utf-8", errors="replace"),
            )
        info(
            "runtime backup updated "
            f"state={backup_state_file} transcript_exists={transcript_path.exists()}"
        )
    except Exception as exc:
        info(f"runtime backup write failed: {exc}")
        debug(f"persist_runtime_backup failed: {exc}")


def persist_runtime_backup(
    *,
    state: dict[str, Any],
    key: str,
    session_id: str,
    transcript_path: Path,
    project_name: str,
) -> None:
    # Normal hook execution writes to container /logs, which TB mounts back to host sessions/.
    persist_runtime_backup_to_dir(
        logs_dir=LOGS_DIR,
        state=state,
        key=key,
        session_id=session_id,
        transcript_path=transcript_path,
        project_name=project_name,
    )


def load_runtime_backup(logs_dir: Path) -> tuple[dict[str, Any] | None, Path | None]:
    try:
        state_path = logs_dir / BACKUP_STATE_FILE.name
        if not state_path.exists():
            return None, None
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        transcript_path = logs_dir / BACKUP_TRANSCRIPT_FILE.name
        return payload, transcript_path if transcript_path.exists() else None
    except Exception as exc:
        debug(f"load_runtime_backup failed: {exc}")
        return None, None


# ── V3 transcript dataclasses ─────────────────────────────────────────────────

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


@dataclass
class ReasoningRound:
    round_idx: int
    llm_items: list[tuple[int, LLMCall]] = field(default_factory=list)


# ── V3 transcript parsing ─────────────────────────────────────────────────────

def parse_ts(value: str) -> datetime:
    if value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def strip_model_date(model: str) -> str:
    if model and len(model) > 9:
        return re.sub(r"-\d{8}$", "", model)
    return model


def estimate_tokens(content: Any) -> int:
    if content is None:
        return 0
    text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    return max(1, len(text) // 4) if text else 0


def extract_agent_id_from_result(tool_result_content: str) -> str | None:
    m = re.search(r"agentId:\s*([A-Za-z0-9-]+)", tool_result_content)
    return m.group(1) if m else None


def extract_subagent_usage(tool_result_content: str) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    m = re.search(r"<usage>(.*?)</usage>", tool_result_content, re.DOTALL)
    if m:
        for line in m.group(1).strip().splitlines():
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            k, v = k.strip(), v.strip()
            try:
                usage[k] = int(v)
            except ValueError:
                usage[k] = v
    return usage


def find_subagent_dir(transcript_path: Path) -> Path | None:
    session_id = transcript_path.stem
    subagents_dir = transcript_path.parent / session_id / "subagents"
    return subagents_dir if subagents_dir.is_dir() else None


def load_subagent_meta(subagents_dir: Path, agent_id: str) -> dict[str, Any]:
    meta_path = subagents_dir / f"agent-{agent_id}.meta.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def resolve_subagent_transcript_path(
    master_transcript_path: Path,
    agent_id: str,
    agent_transcript: str | None = None,
) -> Path | None:
    if agent_transcript:
        try:
            return Path(agent_transcript).expanduser().resolve()
        except Exception:
            pass
    subagents_dir = find_subagent_dir(master_transcript_path)
    if not subagents_dir or not agent_id:
        return None
    candidate = subagents_dir / f"agent-{agent_id}.jsonl"
    return candidate if candidate.exists() else candidate


def find_nested_subagent_dir(agent_transcript_path: Path) -> Path | None:
    agent_root = agent_transcript_path.with_suffix("")
    nested = agent_root / "subagents"
    return nested if nested.is_dir() else None



_THINKING_TAG_RE = re.compile(r"<thinking>(.*?)</thinking>", re.DOTALL)


def _extract_text_and_reasoning(text: str, reasoning_parts: list[str]) -> str:
    """Extract <thinking>...</thinking> blocks into reasoning_parts, return remaining text."""
    def _collect(m: re.Match) -> str:
        reasoning_parts.append(m.group(1))
        return ""
    return _THINKING_TAG_RE.sub(_collect, text).strip()


def _parse_jsonl_turns(lines: list[str]) -> list[V3Turn]:
    """Parse a list of JSONL lines (from transcript) into V3Turn objects."""
    turns: list[V3Turn] = []
    current_turn: V3Turn | None = None
    current_msg_id: str | None = None
    current_llm: LLMCall | None = None

    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if data.get("type", "") in ("queue-operation", "file-history-snapshot"):
            continue

        timestamp = data.get("timestamp", "")
        msg = data.get("message", {})
        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                msg = {}

        role = _infer_message_role(data, msg)

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

            # Flush pending LLM call before starting a new user turn
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

            # System-injected messages (task-notification, system-reminder)
            # should NOT start a new turn — they belong to the current one.
            if _is_continuation_message(user_text):
                continue

            if current_turn:
                _finalize_turn(current_turn)
                turns.append(current_turn)

            current_turn = V3Turn(user_text=user_text.strip(), user_timestamp=timestamp)

        elif role == "assistant" and current_turn is not None:
            msg_id = msg.get("id", "")
            model = msg.get("model", "") or ""
            usage = msg.get("usage", {}) or {}
            stop_reason = msg.get("stop_reason")
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
                elif item.get("type") == "tool_use":
                    tool_uses.append(ToolUse(
                        tool_use_id=item.get("id", ""),
                        name=item.get("name", "tool"),
                        input=item.get("input", {}),
                        timestamp=timestamp,
                    ))

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
    """Compute per-LLM-call incremental token usage.

    Claude Code's usage.input_tokens is a cumulative snapshot (full context size),
    not incremental. Incremental = current - previous = new tokens added this step.
    output_tokens is already per-call incremental, copied as-is.

    Returns the final snapshot for persistence across hook invocations.
    """
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
            incremental["output_tokens"] = out
            last_output_tokens = out
            lc.incremental_usage = incremental
    # Persist output_tokens in snapshot so session metadata can read it
    prev["output_tokens"] = last_output_tokens
    return dict(prev)


def _backfill_llm_starts(turns: list[V3Turn]) -> None:
    """Backfill tool-only LLM calls to start at the previous step's end time.

    Tool-only assistant events do not have a visible text emission point,
    so we model their span from the previous step end to the tool-call timestamp.
    """
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
                if last_tr_ts:
                    prev_end_ts = last_tr_ts
                else:
                    prev_end_ts = lc.end_timestamp or lc.timestamp
            else:
                prev_end_ts = lc.end_timestamp or lc.timestamp


def _extract_assistant_parts_by_turn(lines: list[str]) -> list[list[dict[str, Any]]]:
    turns: list[list[dict[str, Any]]] = []
    current_turn_idx = -1
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if data.get("type", "") in ("queue-operation", "file-history-snapshot"):
            continue

        msg = data.get("message", {})
        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                msg = {}

        role = _infer_message_role(data, msg)
        timestamp = data.get("timestamp", "")
        if role == "user":
            content = msg.get("content", "")
            is_tool_result = False
            if isinstance(content, list):
                is_tool_result = any(
                    isinstance(item, dict) and item.get("type") == "tool_result"
                    for item in content
                )
            if is_tool_result:
                continue

            user_text = ""
            if isinstance(content, str):
                user_text = content
            elif isinstance(content, list):
                user_text = "\n".join(
                    item.get("text", "")
                    for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                )
            if user_text.strip():
                current_turn_idx += 1
                turns.append([])
            continue

        if role != "assistant" or current_turn_idx < 0:
            continue

        content = msg.get("content", "")
        if isinstance(content, str):
            content_items = [{"type": "text", "text": content}] if content else []
        elif isinstance(content, list):
            content_items = content
        else:
            content_items = []

        turns[current_turn_idx].append({
            "message_id": msg.get("id", ""),
            "model": msg.get("model", "") or "",
            "timestamp": timestamp,
            "usage": msg.get("usage", {}) or {},
            "stop_reason": msg.get("stop_reason"),
            "content_items": content_items,
        })
    return turns


def _rebuild_turn_llm_calls(turns: list[V3Turn], lines: list[str]) -> None:
    assistant_turns = _extract_assistant_parts_by_turn(lines)
    for turn, raw_parts in zip(turns, assistant_turns):
        rebuilt: list[LLMCall] = []
        i = 0
        while i < len(raw_parts):
            group = [raw_parts[i]]
            msg_id = raw_parts[i]["message_id"]
            i += 1
            while i < len(raw_parts) and raw_parts[i]["message_id"] == msg_id:
                group.append(raw_parts[i])
                i += 1

            tool_part_count = sum(
                1 for part in group
                if any(
                    isinstance(item, dict) and item.get("type") == "tool_use"
                    for item in part["content_items"]
                )
            )
            before_tool_text: list[str] = []
            phase_text: list[str] = []
            reasoning_parts: list[str] = []
            all_tool_uses: list[ToolUse] = []
            first_ts = group[0]["timestamp"]
            first_tool_ts = None
            last_ts = group[-1]["timestamp"]
            model = next((part["model"] for part in reversed(group) if part["model"]), "")
            usage = next((part["usage"] for part in reversed(group) if part["usage"]), {})
            stop_reason = next(
                (part["stop_reason"] for part in reversed(group) if part["stop_reason"]),
                None,
            )
            saw_tool = False

            for part in group:
                text_parts: list[str] = []
                tool_uses: list[ToolUse] = []
                for item in part["content_items"]:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") == "text" and item.get("text"):
                        text_parts.append(_extract_text_and_reasoning(item["text"], reasoning_parts))
                    elif item.get("type") == "thinking" and item.get("thinking"):
                        reasoning_parts.append(item["thinking"])
                    elif item.get("type") == "tool_use":
                        tool_uses.append(ToolUse(
                            tool_use_id=item.get("id", ""),
                            name=item.get("name", "tool"),
                            input=item.get("input", {}),
                            timestamp=part["timestamp"],
                        ))
                if tool_uses and first_tool_ts is None:
                    first_tool_ts = part["timestamp"]
                if not saw_tool and tool_uses:
                    saw_tool = True
                if saw_tool:
                    phase_text.extend(t for t in text_parts if t)
                    all_tool_uses.extend(tool_uses)
                else:
                    before_tool_text.extend(t for t in text_parts if t)

            combined_text = before_tool_text + phase_text
            rebuilt.append(LLMCall(
                message_id=msg_id,
                model=model,
                text="\n".join(combined_text),
                reasoning="\n".join(reasoning_parts),
                tool_uses=all_tool_uses,
                timestamp=first_ts,
                start_timestamp=first_ts,
                end_timestamp=last_ts,
                usage=usage,
                stop_reason=stop_reason,
            ))

        if rebuilt:
            turn.llm_calls = rebuilt
            _finalize_turn(turn)


def _collect_assistant_message_bounds(lines: list[str]) -> dict[str, tuple[str, str]]:
    """Return assistant message_id → (first_timestamp, last_timestamp) from raw lines."""
    bounds: dict[str, tuple[str, str]] = {}
    for raw in lines:
        raw = raw.strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        msg = data.get("message", {})
        if isinstance(msg, str):
            try:
                msg = json.loads(msg)
            except (json.JSONDecodeError, TypeError):
                msg = {}
        if _infer_message_role(data, msg) != "assistant":
            continue
        msg_id = msg.get("id")
        ts = data.get("timestamp", "")
        if not msg_id or not ts:
            continue
        if msg_id in bounds:
            first_ts, _ = bounds[msg_id]
            bounds[msg_id] = (first_ts, ts)
        else:
            bounds[msg_id] = (ts, ts)
    return bounds


def _patch_llm_call_bounds(turns: list[V3Turn], lines: list[str]) -> None:
    """Annotate LLM calls with accurate start/end timestamps from SSE streaming parts."""
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
    """Read transcript from byte_offset to end and parse into V3Turn objects.

    Returns (turns, final_usage_snapshot) where the snapshot can be persisted
    for incremental usage computation across hook invocations.
    """
    try:
        with transcript_path.open("rb") as f:
            f.seek(byte_offset)
            raw_bytes = f.read()
        lines = raw_bytes.decode("utf-8", errors="replace").splitlines()
        turns = _parse_jsonl_turns(lines)
        _rebuild_turn_llm_calls(turns, lines)
        _patch_llm_call_bounds(turns, lines)
        _backfill_llm_starts(turns)
        final_snapshot = _compute_incremental_usage(turns, prev_usage_snapshot)
        return turns, final_snapshot
    except Exception as exc:
        debug(f"parse_transcript_segment failed: {exc}")
        return [], dict(prev_usage_snapshot) if prev_usage_snapshot else {}


def load_subagent_transcript_from_path(agent_path: Path) -> tuple[list[V3Turn], dict[str, Any]] | None:
    """Load and parse a subagent transcript from a concrete agent JSONL path."""
    if not agent_path.exists():
        return None
    try:
        lines = agent_path.read_text(encoding="utf-8").splitlines()
        turns = _parse_jsonl_turns(lines)
        _rebuild_turn_llm_calls(turns, lines)
        _patch_llm_call_bounds(turns, lines)
        _backfill_llm_starts(turns)
        _compute_incremental_usage(turns)
        if not turns:
            return None
        meta = _build_subagent_meta(turns, find_nested_subagent_dir(agent_path))
        return turns, meta
    except Exception as exc:
        debug(f"load_subagent_transcript failed for {agent_path}: {exc}")
        return None


def load_subagent_transcript(subagents_dir: Path, agent_id: str) -> tuple[list[V3Turn], dict[str, Any]] | None:
    """Load and parse a subagent transcript, returning (turns, meta) like v3_round."""
    return load_subagent_transcript_from_path(subagents_dir / f"agent-{agent_id}.jsonl")


def _build_subagent_meta(
    turns: list[V3Turn],
    nested_subagents_dir: Path | None = None,
) -> dict[str, Any]:
    """Build aggregated metadata for a subagent transcript (mirrors v3_round's _refresh_meta_counts)."""
    total_llm_calls = sum(len(t.llm_calls) for t in turns)
    total_tool_calls = sum(sum(len(lc.tool_uses) for lc in t.llm_calls) for t in turns)
    meta: dict[str, Any] = {
        "turn_count": len(turns),
        "total_llm_calls": total_llm_calls,
        "total_tool_calls": total_tool_calls,
        "total_subagent_calls": sum(
            1 for t in turns for lc in t.llm_calls for tu in lc.tool_uses if tu.name == "Agent"
        ),
        "tool_success": sum(1 for t in turns for tr in t.tool_results.values() if not tr.is_error),
        "tool_error": sum(1 for t in turns for tr in t.tool_results.values() if tr.is_error),
        "api_billed_input_tokens": sum(
            int(lc.usage.get("input_tokens", 0) or 0) for t in turns for lc in t.llm_calls
        ),
        "api_billed_output_tokens": sum(
            int(lc.usage.get("output_tokens", 0) or 0) for t in turns for lc in t.llm_calls
        ),
        "api_billed_cache_creation_tokens": sum(
            int(lc.usage.get("cache_creation_input_tokens", 0) or 0) for t in turns for lc in t.llm_calls
        ),
        "api_billed_cache_read_tokens": sum(
            int(lc.usage.get("cache_read_input_tokens", 0) or 0) for t in turns for lc in t.llm_calls
        ),
        "incremental_input_tokens": sum(
            lc.incremental_usage.get("input_tokens", 0) for t in turns for lc in t.llm_calls
        ),
        "incremental_output_tokens": sum(
            lc.incremental_usage.get("output_tokens", 0) for t in turns for lc in t.llm_calls
        ),
        "incremental_cache_read_tokens": sum(
            lc.incremental_usage.get("cache_read_input_tokens", 0) for t in turns for lc in t.llm_calls
        ),
        "incremental_cache_creation_tokens": sum(
            lc.incremental_usage.get("cache_creation_input_tokens", 0) for t in turns for lc in t.llm_calls
        ),
        "models": sorted({
            strip_model_date(lc.model) for t in turns for lc in t.llm_calls
            if lc.model and lc.model != "<synthetic>"
        }),
    }
    # Snapshot tokens from last real LLM call
    snap = _turn_snap(turns[-1]) if turns else {}
    meta["snapshot_input_tokens"] = snap.get("snap_input", 0)
    meta["snapshot_cache_read_tokens"] = snap.get("snap_cache_read", 0)
    meta["snapshot_cache_creation_tokens"] = snap.get("snap_cache_write", 0)
    meta["snapshot_output_tokens"] = snap.get("snap_output", 0)
    snap_ctx = snap.get("snap_context", 0)
    meta["snapshot_context_tokens"] = snap_ctx
    meta["snapshot_total_tokens"] = snap_ctx + meta["snapshot_output_tokens"]
    meta.update(_aggregate_subagent_tokens(turns, nested_subagents_dir))
    return meta


# ── V3 span helpers ───────────────────────────────────────────────────────────

def _compute_turn_end(turn: V3Turn) -> datetime:
    if turn.end_timestamp:
        return parse_ts(turn.end_timestamp)
    candidates = [lc.end_timestamp or lc.timestamp for lc in turn.llm_calls if (lc.end_timestamp or lc.timestamp)]
    candidates.extend(tr.timestamp for tr in turn.tool_results.values() if tr.timestamp)
    if candidates:
        return max(parse_ts(c) for c in candidates)
    return parse_ts(turn.user_timestamp)


def _is_turn_complete(turn: V3Turn) -> bool:
    if not turn.llm_calls:
        return False
    last_llm = turn.llm_calls[-1]
    if not last_llm.stop_reason:
        return False
    for lc in turn.llm_calls:
        for tu in lc.tool_uses:
            if tu.tool_use_id not in turn.tool_results:
                return False
    return True


def _llm_end_time(turn: V3Turn, llm_call: LLMCall, fallback_ts: str) -> datetime:
    llm_end = parse_ts(llm_call.end_timestamp or llm_call.timestamp or fallback_ts)
    if llm_call.tool_uses:
        first_tr_ts = None
        for tu in llm_call.tool_uses:
            tr = turn.tool_results.get(tu.tool_use_id)
            if tr and tr.timestamp:
                if first_tr_ts is None or tr.timestamp < first_tr_ts:
                    first_tr_ts = tr.timestamp
        if first_tr_ts:
            llm_end = parse_ts(first_tr_ts)
    llm_start = parse_ts(llm_call.start_timestamp or llm_call.timestamp or fallback_ts)
    if llm_end < llm_start:
        llm_end = llm_start
    return llm_end


def _tool_times(tu: ToolUse, tr: ToolResult | None) -> tuple[datetime, datetime]:
    tool_start = parse_ts(tu.timestamp)
    tool_end = parse_ts((tr.timestamp if tr else tu.timestamp) or tu.timestamp)
    if tool_end < tool_start:
        tool_end = tool_start
    return tool_start, tool_end


def build_usage_metadata(usage: dict[str, Any]) -> dict[str, Any] | None:
    if not usage:
        return None
    raw_input = int(usage.get("input_tokens", 0) or 0)
    output_tokens = int(usage.get("output_tokens", 0) or 0)
    cache_read = int(usage.get("cache_read_input_tokens", 0) or 0)
    cache_creation = int(usage.get("cache_creation_input_tokens", 0) or 0)
    input_tokens = raw_input + cache_read + cache_creation
    total_tokens = input_tokens + output_tokens
    if total_tokens == 0:
        return None
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "input_token_details": {"cache_read": cache_read, "cache_creation": cache_creation},
    }


def _opik_usage(usage_meta: dict[str, Any] | None) -> dict[str, Any] | None:
    if not usage_meta:
        return None
    details = usage_meta.get("input_token_details") or {}
    result = {
        "prompt_tokens": int(usage_meta.get("input_tokens", 0) or 0),
        "completion_tokens": int(usage_meta.get("output_tokens", 0) or 0),
        "total_tokens": int(usage_meta.get("total_tokens", 0) or 0),
        "cache_read": int(details.get("cache_read", 0) or 0),
        "cache_creation": int(details.get("cache_creation", 0) or 0),
    }
    return {k: v for k, v in result.items() if v}


def _make_error_info(error_text: str | None) -> dict[str, str] | None:
    if not error_text:
        return None
    return {"exception_type": "ToolError", "message": error_text[:500], "traceback": error_text}


def _has_real_usage(lc: LLMCall) -> bool:
    if not lc.usage or lc.model == "<synthetic>":
        return False
    u = lc.usage
    return (int(u.get("input_tokens", 0) or 0)
            + int(u.get("output_tokens", 0) or 0)
            + int(u.get("cache_read_input_tokens", 0) or 0)
            + int(u.get("cache_creation_input_tokens", 0) or 0)) > 0


def _turn_snap(turn: V3Turn) -> dict[str, int]:
    for lc in reversed(turn.llm_calls):
        if not _has_real_usage(lc):
            continue
        u = lc.usage
        inp = int(u.get("input_tokens", 0) or 0)
        cr = int(u.get("cache_read_input_tokens", 0) or 0)
        cc = int(u.get("cache_creation_input_tokens", 0) or 0)
        out = int(u.get("output_tokens", 0) or 0)
        return {"snap_input": inp, "snap_cache_read": cr, "snap_cache_write": cc,
                "snap_output": out, "snap_context": inp + cr + cc}
    return {"snap_input": 0, "snap_cache_read": 0, "snap_cache_write": 0,
            "snap_output": 0, "snap_context": 0}


def _group_reasoning_rounds(turn: V3Turn) -> list[ReasoningRound]:
    """Group LLM calls into reasoning rounds by visible text boundaries."""
    rounds: list[ReasoningRound] = []
    current: ReasoningRound | None = None
    for llm_idx, llm_call in enumerate(turn.llm_calls, start=1):
        starts_new_round = bool(llm_call.text.strip())
        if current is None or starts_new_round:
            current = ReasoningRound(round_idx=len(rounds) + 1)
            rounds.append(current)
        current.llm_items.append((llm_idx, llm_call))
    return rounds


def _round_end_time(round_item: ReasoningRound, turn: V3Turn) -> datetime:
    last_tool_ts = ""
    for _, llm_call in round_item.llm_items:
        for tu in llm_call.tool_uses:
            tr = turn.tool_results.get(tu.tool_use_id)
            if tr and tr.timestamp and tr.timestamp > last_tool_ts:
                last_tool_ts = tr.timestamp
    if last_tool_ts:
        return parse_ts(last_tool_ts)
    last_llm = round_item.llm_items[-1][1]
    return _llm_end_time(turn, last_llm, turn.user_timestamp)


def _round_metadata(round_item: ReasoningRound, turn: V3Turn, session_id: str, turn_idx: int) -> dict[str, Any]:
    llm_calls = [llm_call for _, llm_call in round_item.llm_items]
    api_input = sum(int(lc.usage.get("input_tokens", 0) or 0) for lc in llm_calls)
    api_output = sum(int(lc.usage.get("output_tokens", 0) or 0) for lc in llm_calls)
    api_cache_read = sum(int(lc.usage.get("cache_read_input_tokens", 0) or 0) for lc in llm_calls)
    api_cache_creation = sum(int(lc.usage.get("cache_creation_input_tokens", 0) or 0) for lc in llm_calls)
    incremental_input = sum(lc.incremental_usage.get("input_tokens", 0) for lc in llm_calls)
    incremental_output = sum(lc.incremental_usage.get("output_tokens", 0) for lc in llm_calls)
    incremental_cache_read = sum(lc.incremental_usage.get("cache_read_input_tokens", 0) for lc in llm_calls)
    incremental_cache_creation = sum(lc.incremental_usage.get("cache_creation_input_tokens", 0) for lc in llm_calls)
    last_usage = llm_calls[-1].usage if llm_calls else {}
    snapshot_input = int(last_usage.get("input_tokens", 0) or 0)
    snapshot_cache_read = int(last_usage.get("cache_read_input_tokens", 0) or 0)
    snapshot_cache_creation = int(last_usage.get("cache_creation_input_tokens", 0) or 0)
    snapshot_output = int(last_usage.get("output_tokens", 0) or 0)
    tool_calls = sum(len(lc.tool_uses) for lc in llm_calls)
    round_start = parse_ts(llm_calls[0].start_timestamp or llm_calls[0].timestamp or turn.user_timestamp)
    round_end = _round_end_time(round_item, turn)
    duration_ms = max(0, int((round_end - round_start).total_seconds() * 1000))
    opener = next((lc.text.strip() for lc in llm_calls if lc.text.strip()), "")
    return {
        "thread_id": session_id,
        "turn_idx": turn_idx,
        "round_idx": round_item.round_idx,
        "duration_ms": duration_ms,
        "llm_calls": len(llm_calls),
        "tool_calls": tool_calls,
        "api_billed_input_tokens": api_input,
        "api_billed_output_tokens": api_output,
        "api_billed_cache_read_tokens": api_cache_read,
        "api_billed_cache_creation_tokens": api_cache_creation,
        "incremental_input_tokens": incremental_input,
        "incremental_output_tokens": incremental_output,
        "incremental_cache_read_tokens": incremental_cache_read,
        "incremental_cache_creation_tokens": incremental_cache_creation,
        "snapshot_input_tokens": snapshot_input,
        "snapshot_cache_read_tokens": snapshot_cache_read,
        "snapshot_cache_creation_tokens": snapshot_cache_creation,
        "snapshot_output_tokens": snapshot_output,
        "snapshot_context_tokens": snapshot_input + snapshot_cache_read + snapshot_cache_creation,
        "snapshot_total_tokens": snapshot_input + snapshot_cache_read + snapshot_cache_creation + snapshot_output,
        "has_visible_text": bool(opener),
        "starts_with_tool_only": bool(llm_calls and not llm_calls[0].text.strip()),
        "opener_text": opener[:240] if opener else "",
        "models": sorted({strip_model_date(lc.model) for lc in llm_calls if lc.model}),
        "realtime": True,
    }


def _aggregate_subagent_tokens(turns: list[V3Turn], subagents_dir: Path | None) -> dict[str, int]:
    """Recursively sum incremental tokens across all child agents."""
    totals = {
        "subagents_incremental_input_tokens": 0,
        "subagents_incremental_output_tokens": 0,
        "subagents_incremental_cache_read_tokens": 0,
        "subagents_incremental_cache_creation_tokens": 0,
        "subagents_incremental_total_tokens": 0,
    }
    if not subagents_dir:
        return totals
    visited: set[str] = set()

    def add_agent_tree(agent_id: str) -> None:
        if not agent_id or agent_id in visited:
            return
        visited.add(agent_id)
        result = load_subagent_transcript(subagents_dir, agent_id)
        if not result:
            return
        child_turns, child_meta = result
        totals["subagents_incremental_input_tokens"] += child_meta.get("incremental_input_tokens", 0)
        totals["subagents_incremental_output_tokens"] += child_meta.get("incremental_output_tokens", 0)
        totals["subagents_incremental_cache_read_tokens"] += child_meta.get("incremental_cache_read_tokens", 0)
        totals["subagents_incremental_cache_creation_tokens"] += child_meta.get("incremental_cache_creation_tokens", 0)
        totals["subagents_incremental_total_tokens"] += (
            child_meta.get("incremental_input_tokens", 0)
            + child_meta.get("incremental_output_tokens", 0)
            + child_meta.get("incremental_cache_read_tokens", 0)
            + child_meta.get("incremental_cache_creation_tokens", 0)
        )
        for turn in child_turns:
            for lc in turn.llm_calls:
                for tu in lc.tool_uses:
                    if tu.name != "Agent":
                        continue
                    tr = turn.tool_results.get(tu.tool_use_id)
                    if tr:
                        nested_id = extract_agent_id_from_result(tr.content)
                        if nested_id:
                            add_agent_tree(nested_id)

    for turn in turns:
        for lc in turn.llm_calls:
            for tu in lc.tool_uses:
                if tu.name != "Agent":
                    continue
                tr = turn.tool_results.get(tu.tool_use_id)
                if tr:
                    agent_id = extract_agent_id_from_result(tr.content)
                    if agent_id:
                        add_agent_tree(agent_id)
    return totals


def _turn_metadata(turn: V3Turn, session_id: str, turn_idx: int,
                   subagents_dir: Path | None) -> dict[str, Any]:
    turn_start = parse_ts(turn.user_timestamp)
    turn_end = _compute_turn_end(turn)
    duration_ms = max(0, int((turn_end - turn_start).total_seconds() * 1000))
    llm_count = len(turn.llm_calls)
    tool_count = sum(len(lc.tool_uses) for lc in turn.llm_calls)
    subagent_count = sum(1 for lc in turn.llm_calls for tu in lc.tool_uses if tu.name == "Agent")
    tool_ok = sum(1 for tr in turn.tool_results.values() if not tr.is_error)
    tool_err = sum(1 for tr in turn.tool_results.values() if tr.is_error)
    # API billed totals (sum of per-call usage)
    api_input = sum(int(lc.usage.get("input_tokens", 0) or 0) for lc in turn.llm_calls)
    api_output = sum(int(lc.usage.get("output_tokens", 0) or 0) for lc in turn.llm_calls)
    api_cache_read = sum(int(lc.usage.get("cache_read_input_tokens", 0) or 0) for lc in turn.llm_calls)
    api_cache_creation = sum(int(lc.usage.get("cache_creation_input_tokens", 0) or 0) for lc in turn.llm_calls)
    # Incremental totals
    incremental_input = sum(lc.incremental_usage.get("input_tokens", 0) for lc in turn.llm_calls)
    incremental_output = sum(lc.incremental_usage.get("output_tokens", 0) for lc in turn.llm_calls)
    incremental_cache_read = sum(lc.incremental_usage.get("cache_read_input_tokens", 0) for lc in turn.llm_calls)
    incremental_cache_creation = sum(lc.incremental_usage.get("cache_creation_input_tokens", 0) for lc in turn.llm_calls)
    snap = _turn_snap(turn)
    sa = _aggregate_subagent_tokens([turn], subagents_dir)
    models = sorted({strip_model_date(lc.model) for lc in turn.llm_calls
                     if lc.model and lc.model != "<synthetic>"})
    metadata = {
        "thread_id": session_id,
        "turn_idx": turn_idx,
        "duration_ms": duration_ms,
        "llm_calls": llm_count,
        "tool_calls": tool_count,
        "subagent_calls": subagent_count,
        "tool_success": tool_ok,
        "tool_error": tool_err,
        "api_billed_input_tokens": api_input,
        "api_billed_output_tokens": api_output,
        "api_billed_cache_read_tokens": api_cache_read,
        "api_billed_cache_creation_tokens": api_cache_creation,
        "incremental_input_tokens": incremental_input,
        "incremental_output_tokens": incremental_output,
        "incremental_cache_read_tokens": incremental_cache_read,
        "incremental_cache_creation_tokens": incremental_cache_creation,
        "snapshot_input_tokens": snap["snap_input"],
        "snapshot_cache_read_tokens": snap["snap_cache_read"],
        "snapshot_cache_creation_tokens": snap["snap_cache_write"],
        "snapshot_output_tokens": snap["snap_output"],
        "snapshot_context_tokens": snap["snap_context"],
        "snapshot_total_tokens": snap["snap_context"] + snap["snap_output"],
        **sa,
        "models": models,
        "realtime": True,
    }
    metadata.update(runtime_context_metadata())
    return metadata


def _agent_span_metadata(
    *,
    session_id: str | None,
    agent_id: str | None,
    agent_type: str,
    agent_desc: str,
    agent_file_meta: dict[str, Any],
    subagent_usage: dict[str, Any],
    child_meta: dict[str, Any],
    emitted_turns: int | None = None,
    finished: bool | None = None,
    realtime_placeholder: bool | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "agent_id": agent_id,
        "subagent_type": agent_file_meta.get("agentType", agent_type),
        "description": agent_file_meta.get("description", agent_desc),
        "inline_total_tokens": subagent_usage.get("total_tokens"),
        "subagent_tool_uses": subagent_usage.get("tool_uses"),
        "subagent_duration_ms": subagent_usage.get("duration_ms"),
    }
    if session_id:
        metadata["session_id"] = session_id
    if emitted_turns is not None:
        metadata["emitted_turns"] = emitted_turns
    if finished is not None:
        metadata["finished"] = finished
    if realtime_placeholder is not None:
        metadata["realtime_placeholder"] = realtime_placeholder
    metadata.update(child_meta)
    metadata.update(runtime_context_metadata())
    return metadata


def _agent_span_output(
    *,
    tool_output: str | None = None,
    subagent_usage: dict[str, Any] | None = None,
    child_meta: dict[str, Any] | None = None,
    status: str | None = None,
    emitted_turns: int | None = None,
) -> dict[str, Any]:
    output: dict[str, Any] = {}
    if tool_output is not None:
        output["result"] = tool_output
    if status is not None:
        output["status"] = status
    if emitted_turns is not None:
        output["emitted_turns"] = emitted_turns
    if subagent_usage:
        output["subagent_usage"] = subagent_usage
    if child_meta:
        output["usage_metadata"] = child_meta
    return output


def truncate_text(value: str) -> str:
    if len(value) <= MAX_TEXT_CHARS:
        return value
    return value[:MAX_TEXT_CHARS]


# ── ID helpers ────────────────────────────────────────────────────────────────

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
    """Generate a deterministic UUIDv7-compatible ID from structural position.

    UUIDv7 layout: 48-bit ms timestamp | 4-bit version (0111) | 12-bit rand_a
                   | 2-bit variant (10) | 62-bit rand_b
    We use SHA-256 to fill the random bits deterministically while keeping
    the version/variant bits correct so Opik accepts the ID.
    """
    raw = "::".join([trace_id, *parts])
    digest = hashlib.sha256(raw.encode("utf-8")).digest()  # 32 bytes
    # Build 16 bytes for UUID
    b = bytearray(digest[:16])
    # Set version = 7 (bits 48-51)
    b[6] = (b[6] & 0x0F) | 0x70
    # Set variant = 10xx (bits 64-65)
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def session_trace_id(session: SessionState) -> str:
    if session.trace_id:
        return session.trace_id
    session.trace_id = new_opik_id()
    return session.trace_id


# ── Opik API wrappers ─────────────────────────────────────────────────────────

def _session_metadata_from_state(session: SessionState, session_id: str) -> dict[str, Any]:
    """Build session-level metadata from accumulated SessionState."""
    models = sorted(session.session_models) if session.session_models else []
    # Snapshot tokens from last usage snapshot (current context window size)
    snap = session.prev_usage_snapshot or {}
    snap_input = int(snap.get("input_tokens", 0) or 0)
    snap_cache_read = int(snap.get("cache_read_input_tokens", 0) or 0)
    snap_cache_creation = int(snap.get("cache_creation_input_tokens", 0) or 0)
    snap_output = int(snap.get("output_tokens", 0) or 0)
    snap_context = snap_input + snap_cache_read + snap_cache_creation
    metadata = {
        "session_id": session_id,
        "source": "claude-code",
        "realtime": True,
        "realtime_version": "v2",
        "models": models,
        "total_turns": session.emitted_turns,
        "total_llm_calls": session.session_total_llm_calls,
        "total_tool_calls": session.session_total_tool_calls,
        "total_subagent_calls": session.session_total_subagent_calls,
        "tool_success": session.session_tool_success,
        "tool_error": session.session_tool_error,
        "api_billed_input_tokens": session.session_api_billed_input,
        "api_billed_output_tokens": session.session_api_billed_output,
        "api_billed_cache_read_tokens": session.session_api_billed_cache_read,
        "api_billed_cache_creation_tokens": session.session_api_billed_cache_creation,
        "incremental_input_tokens": session.session_incremental_input,
        "incremental_output_tokens": session.session_incremental_output,
        "incremental_cache_read_tokens": session.session_incremental_cache_read,
        "incremental_cache_creation_tokens": session.session_incremental_cache_creation,
        "snapshot_input_tokens": snap_input,
        "snapshot_cache_read_tokens": snap_cache_read,
        "snapshot_cache_creation_tokens": snap_cache_creation,
        "snapshot_output_tokens": snap_output,
        "snapshot_context_tokens": snap_context,
        "snapshot_total_tokens": snap_context + snap_output,
    }
    metadata.update(runtime_context_metadata())
    return metadata


def _session_output_from_state(session: SessionState, status: str = "completed") -> dict[str, Any]:
    """Build session-level output from accumulated SessionState."""
    return {
        "status": status,
        "total_turns": session.emitted_turns,
        "total_llm_calls": session.session_total_llm_calls,
        "total_tool_calls": session.session_total_tool_calls,
        "total_subagent_calls": session.session_total_subagent_calls,
        "tool_success": session.session_tool_success,
        "tool_error": session.session_tool_error,
        "api_billed_input_tokens": session.session_api_billed_input,
        "api_billed_output_tokens": session.session_api_billed_output,
        "api_billed_cache_read_tokens": session.session_api_billed_cache_read,
        "api_billed_cache_creation_tokens": session.session_api_billed_cache_creation,
        "incremental_input_tokens": session.session_incremental_input,
        "incremental_output_tokens": session.session_incremental_output,
        "incremental_cache_read_tokens": session.session_incremental_cache_read,
        "incremental_cache_creation_tokens": session.session_incremental_cache_creation,
    }


def _session_tags(session: SessionState) -> list[str]:
    """Build session-level tags including model tags."""
    models = sorted(session.session_models) if session.session_models else []
    tags = ["claude-code", "session", "realtime", *[f"model:{m}" for m in models]]
    task_id = _env_first("TB_TASK_ID")
    run_id = _env_first("TB_RUN_ID")
    if task_id:
        tags.append(f"tb-task:{task_id}")
    if run_id:
        tags.append(f"tb-run:{run_id}")
    return tags


def maybe_trace_name(transcript_path: Path) -> str | None:
    for parent in [transcript_path.parent, *transcript_path.parents]:
        meta_path = parent / "tb-metadata.json"
        if not meta_path.exists():
            continue
        try:
            data = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        task_id = data.get("task_id")
        if task_id:
            return str(task_id)
    return None


def create_trace_if_possible(client: Any, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run create_trace name={kwargs.get('name')} id={kwargs.get('id')}")
        return
    client.rest_client.traces.create_trace(**kwargs)


def create_span_if_possible(client: Any, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run create_span name={kwargs.get('name')} id={kwargs.get('id')}")
        return
    client.rest_client.spans.create_span(**kwargs)


_UPDATE_SPAN_UNSUPPORTED = frozenset({"start_time", "last_updated_at", "total_estimated_cost_version"})


def update_span_if_possible(client: Any, span_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run update_span id={span_id}")
        return
    filtered = {k: v for k, v in kwargs.items() if k not in _UPDATE_SPAN_UNSUPPORTED}
    client.rest_client.spans.update_span(span_id, **filtered)


def create_or_update_span_if_possible(client: Any, span_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run upsert_span name={kwargs.get('name')} id={span_id}")
        return
    try:
        client.rest_client.spans.create_span(id=span_id, **kwargs)
    except Exception as create_exc:
        debug(f"upsert: create failed for {span_id} ({create_exc}), trying update")
        # update_span doesn't accept start_time, total_estimated_cost_version,
        # last_updated_at — strip them before the fallback call
        update_kwargs = {k: v for k, v in kwargs.items() if k not in _UPDATE_SPAN_UNSUPPORTED}
        try:
            client.rest_client.spans.update_span(span_id, **update_kwargs)
        except Exception as update_exc:
            debug(f"upsert: update also failed for {span_id}: {update_exc}")
            raise RuntimeError(
                f"create/update span failed for {span_id}: create={create_exc}; update={update_exc}"
            ) from update_exc


def ensure_trace(
    client: Any,
    project_name: str,
    trace_id: str,
    trace_name: str,
    session_id: str,
    start_time: datetime,
    end_time: datetime,
    transcript_path: Path,
    session: SessionState | None = None,
) -> None:
    tags = _session_tags(session) if session else ["claude-code", "realtime"]
    meta: dict[str, Any] = {"source": "claude-code", "realtime": True,
                            "realtime_version": "v2",
                            "transcript_path": str(transcript_path)}
    if session:
        meta.update(_session_metadata_from_state(session, session_id))
    else:
        meta.update(runtime_context_metadata())
    try:
        create_trace_if_possible(
            client,
            id=trace_id,
            project_name=project_name,
            name=trace_name,
            start_time=start_time,
            end_time=end_time,
            input={"session_id": session_id},
            output={"status": "running"},
            metadata=meta,
            tags=tags,
            thread_id=session_id,
        )
    except Exception as exc:
        debug(f"create_trace skipped/failed: {exc}")


def finalize_trace(
    client: Any,
    project_name: str,
    trace_id: str,
    trace_name: str,
    session_id: str,
    start_time: datetime,
    end_time: datetime,
    transcript_path: Path,
    session: SessionState,
    status: str = "completed",
) -> bool:
    session_meta = _session_metadata_from_state(session, session_id)
    session_meta["completed"] = status == "completed"
    session_meta["transcript_path"] = str(transcript_path)
    trace_payload = {
        "project_name": project_name,
        "name": trace_name,
        "start_time": start_time,
        "end_time": end_time,
        "input": {"session_id": session_id, "turns": session.emitted_turns},
        "output": _session_output_from_state(session, status=status),
        "metadata": session_meta,
        "tags": [*_session_tags(session), status],
        "thread_id": session_id,
    }
    if DRY_RUN:
        info(f"dry-run finalize_trace name={trace_name} id={trace_id}")
        return True
    traces_api = getattr(getattr(client, "rest_client", None), "traces", None)
    # Update endpoints in some Opik SDK versions reject create-only fields such
    # as start_time. Keep a filtered payload for update-like methods.
    update_payload = {k: v for k, v in trace_payload.items() if k not in {"start_time"}}
    for method_name in ["update_trace", "update", "upsert_trace"]:
        method = getattr(traces_api, method_name, None)
        if callable(method):
            try:
                method(trace_id, id=trace_id, **update_payload)
                info(f"finalize_trace success via {method_name} trace_id={trace_id}")
                return True
            except TypeError:
                try:
                    method(id=trace_id, **update_payload)
                    info(f"finalize_trace success via {method_name}(id=...) trace_id={trace_id}")
                    return True
                except Exception as exc:
                    info(f"finalize_trace {method_name}(id=...) failed trace_id={trace_id}: {exc}")
            except Exception as exc:
                info(f"finalize_trace {method_name} failed trace_id={trace_id}: {exc}")
    try:
        create_trace_if_possible(client, id=trace_id, **trace_payload)
        info(f"finalize_trace fallback create_trace success trace_id={trace_id}")
        return True
    except Exception as exc:
        info(f"finalize_trace fallback create_trace failed trace_id={trace_id}: {exc}")
        return False


# ── V3 turn emission (REST spans) ─────────────────────────────────────────────

def emit_turn_v3(
    client: Any,
    project_name: str,
    trace_id: str,
    parent_span_id: str,
    session_id: str,
    turn: V3Turn,
    turn_idx: int,
    subagents_dir: Path | None,
    depth: int = 0,
    turn_prefix: str = "turn",
    existing_turn_span_id: str | None = None,
    key: str | None = None,
    span_scope: str = "root",
) -> datetime:
    """Emit a V3Turn as nested Opik spans: turn → round → llm → tools/agents."""
    if depth > 5:
        return parse_ts(turn.user_timestamp)

    turn_start = parse_ts(turn.user_timestamp)
    turn_end = _compute_turn_end(turn)
    if turn_end < turn_start:
        turn_end = turn_start

    turn_span_id = existing_turn_span_id or _deterministic_span_id(
        trace_id, span_scope, turn_prefix, str(turn_idx)
    )
    user_content = [{"type": "text", "text": turn.user_text[:MAX_TEXT_CHARS]}]
    turn_name = f"{turn_prefix}-{turn_idx}"
    base_tags = ["claude-code"] if depth == 0 else ["sub-agent", "turn"]
    turn_meta = _turn_metadata(turn, session_id, turn_idx, subagents_dir)
    turn_payload = {
        "trace_id": trace_id,
        "parent_span_id": parent_span_id,
        "project_name": project_name,
        "name": turn_name,
        "type": "general",
        "start_time": turn_start,
        "end_time": turn_end,
        "input": {"messages": [{"role": "user", "content": user_content}]},
        "output": {},
        "metadata": turn_meta,
        "tags": [*base_tags, turn_name],
    }

    if existing_turn_span_id:
        update_span_if_possible(client, turn_span_id, **turn_payload)
    else:
        create_or_update_span_if_possible(client, turn_span_id, **turn_payload)

    all_outputs: list[dict[str, Any]] = [{"role": "user", "content": user_content}]

    # Emit reasoning rounds (turn → round → llm → tool)
    for round_item in _group_reasoning_rounds(turn):
        first_llm = round_item.llm_items[0][1]
        round_start = parse_ts(first_llm.start_timestamp or first_llm.timestamp or turn.user_timestamp)
        round_end = _round_end_time(round_item, turn)
        round_span_id = _deterministic_span_id(
            trace_id, span_scope, turn_prefix, str(turn_idx), "round", str(round_item.round_idx)
        )
        round_name = f"round-{round_item.round_idx}"

        create_or_update_span_if_possible(
            client,
            round_span_id,
            trace_id=trace_id,
            parent_span_id=turn_span_id,
            project_name=project_name,
            name=round_name,
            type="general",
            start_time=round_start,
            end_time=round_end,
            input={"messages": list(all_outputs)},
            output={},
            metadata=_round_metadata(round_item, turn, session_id, turn_idx),
            tags=["reasoning-round", round_name],
        )

        round_outputs: list[dict[str, Any]] = []
        for llm_idx, llm_call in round_item.llm_items:
            model_display = strip_model_date(llm_call.model)
            usage_meta = build_usage_metadata(llm_call.usage)
            llm_usage = _opik_usage(usage_meta)
            llm_start = parse_ts(llm_call.start_timestamp or llm_call.timestamp or turn.user_timestamp)
            llm_end = _llm_end_time(turn, llm_call, turn.user_timestamp)

            assistant_content: list[dict[str, Any]] = []
            if llm_call.reasoning:
                assistant_content.append({"type": "reasoning", "text": llm_call.reasoning[:MAX_TEXT_CHARS]})
            if llm_call.text:
                assistant_content.append({"type": "text", "text": llm_call.text[:MAX_TEXT_CHARS]})
            for tu in llm_call.tool_uses:
                assistant_content.append({
                    "type": "tool_call",
                    "name": tu.name,
                    "args": tu.input,
                    "id": tu.tool_use_id,
                })

            llm_span_id = _deterministic_span_id(
                trace_id,
                span_scope,
                turn_prefix,
                str(turn_idx),
                "round",
                str(round_item.round_idx),
                "llm",
                str(llm_idx),
            )
            create_or_update_span_if_possible(
                client,
                llm_span_id,
                trace_id=trace_id,
                parent_span_id=round_span_id,
                project_name=project_name,
                name=model_display or "Claude",
                type="llm",
                start_time=llm_start,
                end_time=llm_end,
                input={"messages": list(all_outputs)},
                output={
                    "messages": [{"role": "assistant", "content": assistant_content}],
                    **({"usage_metadata": usage_meta} if usage_meta else {}),
                },
                metadata={
                    "stop_reason": llm_call.stop_reason,
                    "raw_usage": llm_call.usage,
                    "incremental_usage": llm_call.incremental_usage,
                    "timing_version": "v3-rebuilt-transcript",
                    "llm_index": llm_idx,
                    "message_id": llm_call.message_id,
                    "round_idx": round_item.round_idx,
                    "realtime": True,
                },
                model=model_display or None,
                provider="anthropic",
                usage=llm_usage,
            )
            llm_outputs = [{"role": "assistant", "content": assistant_content}]
            all_outputs.extend(llm_outputs)
            round_outputs.extend(llm_outputs)

            for tu in llm_call.tool_uses:
                tr = turn.tool_results.get(tu.tool_use_id)
                tool_output = tr.content if tr else "No result"
                tool_error = tool_output if (tr and tr.is_error) else None
                tool_start, tool_end = _tool_times(tu, tr)

                if tu.name == "Agent":
                    agent_input = tu.input or {}
                    agent_desc = agent_input.get("description", "sub-agent")
                    agent_prompt = agent_input.get("prompt", "")
                    agent_type = agent_input.get("subagent_type", "general-purpose")
                    agent_id = extract_agent_id_from_result(tool_output) if tr else None
                    subagent_usage = extract_subagent_usage(tool_output) if tr else {}
                    agent_file_meta: dict[str, Any] = {}
                    child_turns: list[V3Turn] = []
                    child_meta: dict[str, Any] = {}

                    if agent_id and subagents_dir:
                        agent_file_meta = load_subagent_meta(subagents_dir, agent_id)
                        loaded = load_subagent_transcript(subagents_dir, agent_id)
                        if loaded:
                            child_turns, child_meta = loaded
                    subagent_state = None
                    subagent_states = load_subagent_states(key) if (key and agent_id) else {}
                    if key and agent_id:
                        subagent_state = subagent_states.get(agent_id)

                    agent_span_id = (
                        subagent_state.agent_span_id
                        if subagent_state and subagent_state.agent_span_id
                        else _deterministic_span_id(
                            trace_id,
                            span_scope,
                            turn_prefix,
                            str(turn_idx),
                            "round",
                            str(round_item.round_idx),
                            "llm",
                            str(llm_idx),
                            "agent",
                            agent_id or tu.tool_use_id or agent_desc,
                        )
                    )
                    already_emitted = subagent_state.emitted_turns if subagent_state else 0
                    emitted_turns = max(already_emitted, len(child_turns)) if subagent_state else len(child_turns)
                    agent_meta = _agent_span_metadata(
                        session_id=session_id,
                        agent_id=agent_id,
                        agent_type=agent_type,
                        agent_desc=agent_desc,
                        agent_file_meta=agent_file_meta,
                        subagent_usage=subagent_usage,
                        child_meta=child_meta,
                        emitted_turns=emitted_turns if (subagent_state or child_turns) else None,
                        finished=subagent_state.finished if subagent_state else None,
                        realtime_placeholder=False if subagent_state else None,
                    )

                    agent_payload = {
                        "trace_id": trace_id,
                        "parent_span_id": llm_span_id,
                        "project_name": project_name,
                        "name": f"Agent: {agent_desc}",
                        "type": "general",
                        "start_time": tool_start,
                        "end_time": tool_end,
                        "input": {
                            "prompt": agent_prompt,
                            "description": agent_desc,
                            "subagent_type": agent_type,
                            **{k: v for k, v in agent_input.items()
                               if k not in ("prompt", "description", "subagent_type")},
                        },
                        "output": _agent_span_output(
                            tool_output=tool_output,
                            subagent_usage=subagent_usage,
                            child_meta=child_meta,
                            status="completed" if (subagent_state and subagent_state.finished) else None,
                            emitted_turns=emitted_turns if (subagent_state or child_turns) else None,
                        ),
                        "metadata": agent_meta,
                        "tags": ["sub-agent", f"agent-type:{agent_type}"],
                        "error_info": _make_error_info(tool_error),
                    }
                    # Upsert agent span (idempotent via deterministic ID)
                    create_or_update_span_if_possible(client, agent_span_id, **agent_payload)

                    # Update subagent state (single write at end)
                    if subagent_state is not None:
                        subagent_state.agent_span_id = agent_span_id
                        subagent_state.parent_span_id = llm_span_id
                        subagent_state.last_end_ts = tool_end.isoformat()

                    if child_turns:
                        agent_scope = f"agent:{agent_id or tu.tool_use_id}"
                        for sub_idx, sub_turn in enumerate(child_turns[already_emitted:], start=already_emitted + 1):
                            emit_turn_v3(
                                client=client,
                                project_name=project_name,
                                trace_id=trace_id,
                                parent_span_id=agent_span_id,
                                session_id=session_id,
                                turn=sub_turn,
                                turn_idx=sub_idx,
                                subagents_dir=subagents_dir,
                                depth=depth + 1,
                                turn_prefix="sub-turn",
                                key=key,
                                span_scope=agent_scope,
                            )
                        if subagent_state is not None:
                            subagent_state.emitted_turns = len(child_turns)

                    # Batch-save subagent state once after all child work
                    if subagent_state is not None and agent_id:
                        subagent_states[agent_id] = subagent_state
                        save_subagent_states(key, subagent_states)

                else:
                    input_est = estimate_tokens(
                        tu.name + json.dumps(tu.input, ensure_ascii=False) if tu.input else tu.name
                    )
                    output_est = estimate_tokens(tool_output)
                    tool_span_id = _deterministic_span_id(
                        trace_id,
                        span_scope,
                        turn_prefix,
                        str(turn_idx),
                        "round",
                        str(round_item.round_idx),
                        "llm",
                        str(llm_idx),
                        "tool",
                        tu.tool_use_id or tu.name,
                    )
                    create_or_update_span_if_possible(
                        client,
                        tool_span_id,
                        trace_id=trace_id,
                        parent_span_id=llm_span_id,
                        project_name=project_name,
                        name=tu.name,
                        type="tool",
                        start_time=tool_start,
                        end_time=tool_end,
                        input={"input": tu.input},
                        output={"output": tool_output[:MAX_TEXT_CHARS]},
                        metadata={"size_estimate": {
                            "input_tokens_est": input_est,
                            "output_tokens_est": output_est,
                            "total_tokens_est": input_est + output_est,
                        }},
                        error_info=_make_error_info(tool_error),
                    )

                tool_message = {
                    "role": "tool",
                    "tool_call_id": tu.tool_use_id,
                    "content": [{"type": "text", "text": tool_output[:MAX_TEXT_CHARS]}],
                }
                all_outputs.append(tool_message)
                round_outputs.append(tool_message)

        # Update round span with final outputs
        update_span_if_possible(
            client,
            round_span_id,
            trace_id=trace_id,
            project_name=project_name,
            parent_span_id=turn_span_id,
            output={"messages": round_outputs},
        )

    # Update turn span with final outputs
    update_span_if_possible(
        client,
        turn_span_id,
        trace_id=trace_id,
        project_name=project_name,
        parent_span_id=parent_span_id,
        output={"messages": [m for m in all_outputs if m.get("role") != "user"]},
    )

    return turn_end


# ── Session stats accumulation ─────────────────────────────────────────────────

def _accumulate_session_stats(
    session: SessionState,
    turns: list[V3Turn],
    subagents_dir: Path | None = None,
) -> None:
    """Accumulate per-turn stats into session-wide running totals, including subagent work."""
    for turn in turns:
        for lc in turn.llm_calls:
            session.session_total_llm_calls += 1
            session.session_total_tool_calls += len(lc.tool_uses)
            session.session_total_subagent_calls += sum(
                1 for tu in lc.tool_uses if tu.name == "Agent"
            )
            if _has_real_usage(lc):
                session.session_api_billed_input += int(lc.usage.get("input_tokens", 0) or 0)
                session.session_api_billed_output += int(lc.usage.get("output_tokens", 0) or 0)
                session.session_api_billed_cache_read += int(lc.usage.get("cache_read_input_tokens", 0) or 0)
                session.session_api_billed_cache_creation += int(lc.usage.get("cache_creation_input_tokens", 0) or 0)
            if lc.incremental_usage:
                session.session_incremental_input += lc.incremental_usage.get("input_tokens", 0)
                session.session_incremental_output += lc.incremental_usage.get("output_tokens", 0)
                session.session_incremental_cache_read += lc.incremental_usage.get("cache_read_input_tokens", 0)
                session.session_incremental_cache_creation += lc.incremental_usage.get("cache_creation_input_tokens", 0)
            if lc.model and lc.model != "<synthetic>":
                m = strip_model_date(lc.model)
                if m and m not in session.session_models:
                    session.session_models.append(m)
        for tr in turn.tool_results.values():
            if tr.is_error:
                session.session_tool_error += 1
            else:
                session.session_tool_success += 1

    # Roll up subagent tokens into session totals
    if subagents_dir:
        sa = _aggregate_subagent_tokens(turns, subagents_dir)
        session.session_incremental_input += sa.get("subagents_incremental_input_tokens", 0)
        session.session_incremental_output += sa.get("subagents_incremental_output_tokens", 0)
        session.session_incremental_cache_read += sa.get("subagents_incremental_cache_read_tokens", 0)
        session.session_incremental_cache_creation += sa.get("subagents_incremental_cache_creation_tokens", 0)


def _emit_subagent_turns(
    client: Any,
    project_name: str,
    trace_id: str,
    session_id: str,
    key: str,
    subagent: SubagentState,
    allow_partial: bool = False,
    base_depth: int = 1,
    reemit_all: bool = False,
) -> int:
    if not subagent.transcript_path or not subagent.agent_span_id:
        return 0
    transcript_path = Path(subagent.transcript_path)
    if not transcript_path.exists():
        return 0

    # Full re-read for reemit_all (SubagentStop), incremental for polling
    if reemit_all:
        offset, prev_snap = 0, None
    else:
        offset = subagent.turn_start_offset
        prev_snap = subagent.prev_usage_snapshot
    turns, final_snapshot = parse_transcript_segment(transcript_path, offset, prev_snap)
    if not turns:
        return 0

    if not allow_partial:
        turns = [turn for turn in turns if _is_turn_complete(turn)]
        if not turns:
            debug(f"subagent poll skipped: no completed turn for {subagent.agent_id}")
            return 0

    nested_subagents_dir = find_nested_subagent_dir(transcript_path)
    # For full re-emit, turn numbering starts at 1; for incremental, offset by already emitted
    idx_base = 0 if reemit_all else subagent.emitted_turns
    emitted = 0
    for i, turn in enumerate(turns):
        turn_number = idx_base + i + 1
        try:
            turn_end = emit_turn_v3(
                client=client,
                project_name=project_name,
                trace_id=trace_id,
                parent_span_id=subagent.agent_span_id,
                session_id=session_id,
                turn=turn,
                turn_idx=turn_number,
                subagents_dir=nested_subagents_dir,
                depth=base_depth,
                turn_prefix="sub-turn",
                key=key,
                span_scope=f"agent:{subagent.agent_id}",
            )
            subagent.last_end_ts = turn_end.isoformat()
            emitted += 1
        except Exception as exc:
            debug(f"emit_turn_v3 failed for subagent {subagent.agent_id}: {exc}")
            break

    if emitted or reemit_all:
        subagent.emitted_turns = max(subagent.emitted_turns, idx_base + emitted)
        subagent.prev_usage_snapshot = final_snapshot
        try:
            subagent.turn_start_offset = transcript_path.stat().st_size
        except Exception:
            pass
    return emitted


def _poll_active_subagents(
    client: Any,
    project_name: str,
    trace_id: str,
    session_id: str,
    key: str,
    allow_partial: bool = False,
) -> int:
    subagents = load_subagent_states(key)
    if not subagents:
        return 0

    emitted = 0
    dirty = False
    for subagent in subagents.values():
        if subagent.finished:
            continue
        if not subagent.agent_span_id:
            # Deferred agent — span not created yet; skip polling until
            # emit_turn_v3() lazily creates it from the parent turn
            continue
        if not subagent.transcript_path:
            continue
        count = _emit_subagent_turns(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            session_id=session_id,
            key=key,
            subagent=subagent,
            allow_partial=allow_partial,
            base_depth=1,
        )
        if count:
            emitted += count
            dirty = True

    if dirty:
        save_subagent_states(key, subagents)
    return emitted


def _finalize_subagent_span(
    client: Any,
    project_name: str,
    trace_id: str,
    session_id: str,
    subagent: SubagentState,
) -> None:
    transcript_path = Path(subagent.transcript_path) if subagent.transcript_path else None
    child_meta: dict[str, Any] = {}
    end_ts = subagent.last_end_ts or subagent.started_at or datetime.now(timezone.utc).isoformat()
    if transcript_path and transcript_path.exists():
        loaded = load_subagent_transcript_from_path(transcript_path)
        if loaded:
            _, child_meta = loaded
            if child_meta.get("turn_count", 0) > 0 and not subagent.last_end_ts:
                end_ts = datetime.now(timezone.utc).isoformat()

    metadata = _agent_span_metadata(
        session_id=session_id,
        agent_id=subagent.agent_id,
        agent_type=subagent.agent_type,
        agent_desc=child_meta.get("description", subagent.agent_type or "sub-agent"),
        agent_file_meta={},
        subagent_usage={},
        child_meta=child_meta,
        emitted_turns=subagent.emitted_turns,
        finished=True,
        realtime_placeholder=False,
    )

    update_span_if_possible(
        client,
        subagent.agent_span_id,
        trace_id=trace_id,
        project_name=project_name,
        parent_span_id=subagent.parent_span_id,
        end_time=parse_ts(end_ts),
        output=_agent_span_output(
            status="completed",
            emitted_turns=subagent.emitted_turns,
            child_meta=child_meta,
        ),
        metadata=metadata,
        tags=["sub-agent", "completed", *([f"agent-type:{subagent.agent_type}"] if subagent.agent_type else [])],
    )


# ── Flush logic ───────────────────────────────────────────────────────────────

def _flush_turns(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    state: dict[str, Any],
    key: str,
    allow_partial: bool = False,
) -> int:
    """Parse transcript from turn_start_offset and emit V3 spans. Returns turns emitted."""
    turns, final_snapshot = parse_transcript_segment(
        transcript_path, session.turn_start_offset, session.prev_usage_snapshot,
    )
    if not turns:
        return 0

    if not allow_partial:
        turns = [t for t in turns if _is_turn_complete(t)]
        if not turns:
            debug("flush skipped: no completed turn in current transcript segment")
            return 0

    subagents_dir = find_subagent_dir(transcript_path)
    trace_id = session_trace_id(session)

    if not session.trace_created:
        trace_name = session.trace_name or maybe_trace_name(transcript_path) or f"session-{session_id[:8]}"
        session.trace_name = trace_name
        start_ts = session.trace_start_ts or turns[0].user_timestamp
        session.trace_start_ts = start_ts
        ensure_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=parse_ts(start_ts),
            end_time=parse_ts(start_ts),
            transcript_path=transcript_path,
            session=session,
        )
        session.trace_created = True

    emitted = 0
    succeeded_turns: list[V3Turn] = []
    for idx, turn in enumerate(turns, start=1):
        turn_number = session.emitted_turns + idx
        try:
            turn_end = emit_turn_v3(
                client=client,
                project_name=project_name,
                trace_id=trace_id,
                parent_span_id=trace_id,
                session_id=session_id,
                turn=turn,
                turn_idx=turn_number,
                subagents_dir=subagents_dir,
                existing_turn_span_id=session.turn_span_id if idx == 1 else None,
                key=key,
            )
            session.last_turn_ts = turn_end.isoformat()
            emitted += 1
            succeeded_turns.append(turn)
        except Exception as exc:
            debug(f"emit_turn_v3 failed for turn {turn_number}: {exc}")
            break  # Stop on first failure to preserve offset ordering

    session.emitted_turns += emitted
    if emitted:
        # Persist incremental usage snapshot for next flush
        session.prev_usage_snapshot = final_snapshot
        # Accumulate session-wide stats from actually succeeded turns
        _accumulate_session_stats(session, succeeded_turns, subagents_dir)
        try:
            session.turn_start_offset = transcript_path.stat().st_size
        except Exception:
            pass
        session.turn_span_id = None
    debug(f"flush: emitted {emitted} turn(s) from offset {session.turn_start_offset}")
    return emitted


# ── Event handlers ────────────────────────────────────────────────────────────

def _on_user_prompt_submit(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    payload: dict[str, Any],
) -> None:
    """Record turn start offset and prepare turn metadata.

    System-injected messages (task-notification, system-reminder) trigger
    UserPromptSubmit but should NOT start a new turn — they are continuations
    of the current turn (e.g. background sub-agent completions).
    """
    trace_id = session_trace_id(session)
    trace_name = session.trace_name or maybe_trace_name(transcript_path) or f"session-{session_id[:8]}"
    session.trace_name = trace_name
    now_ts = event_timestamp(payload) or datetime.now(timezone.utc).isoformat()
    session.trace_start_ts = session.trace_start_ts or now_ts

    # Skip creating a new turn for continuation messages (task-notification,
    # system-reminder, etc.) — they belong to the current turn.
    prompt_text = extract_prompt(payload)
    if _is_continuation_message(prompt_text):
        debug(f"UserPromptSubmit: skipping new turn for continuation message (turn={session.turn_number})")
        return

    # Record current transcript size as start of this turn
    try:
        session.turn_start_offset = transcript_path.stat().st_size
    except Exception:
        session.turn_start_offset = 0

    session.turn_number += 1
    session.last_flush_time = time.time()

    if not session.trace_created:
        # Create the session trace at prompt time so long-running tasks are
        # visible in Opik as running before the first completed turn flushes.
        start_time = parse_ts(session.trace_start_ts or now_ts)
        ensure_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=start_time,
            end_time=start_time,
            transcript_path=transcript_path,
            session=session,
        )
        session.trace_created = True

    # Best-effort early turn span only when trace already exists.
    session.turn_span_id = new_opik_id(parse_ts(now_ts))
    if session.trace_created:
        create_span_if_possible(
            client,
            id=session.turn_span_id,
            trace_id=trace_id,
            parent_span_id=trace_id,
            project_name=project_name,
            name=f"turn-{session.turn_number}",
            type="general",
            start_time=parse_ts(now_ts),
            end_time=parse_ts(now_ts),
            input={"text": prompt_text, "turn_number": session.turn_number},
            output={},
            metadata={"session_id": session_id, "turn_number": session.turn_number},
        )
    debug(f"UserPromptSubmit: trace={trace_id} turn={session.turn_number} offset={session.turn_start_offset}")


def _on_tool_use(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    state: dict[str, Any],
    key: str,
) -> int:
    """Handle PostToolUse/PostToolUseFailure: throttled flush every FLUSH_INTERVAL_S."""
    now = time.time()
    if now - session.last_flush_time < FLUSH_INTERVAL_S:
        return 0
    debug(f"PostToolUse flush (elapsed={now - session.last_flush_time:.1f}s)")
    session.last_flush_time = now
    emitted = _flush_turns(
        client=client,
        project_name=project_name,
        session_id=session_id,
        session=session,
        transcript_path=transcript_path,
        state=state,
        key=key,
        allow_partial=False,
    )
    emitted += _poll_active_subagents(
        client=client,
        project_name=project_name,
        trace_id=session_trace_id(session),
        session_id=session_id,
        key=key,
        allow_partial=False,
    )
    return emitted


def _on_stop(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    state: dict[str, Any],
    key: str,
) -> int:
    """Handle Stop: flush visible work only; final status is owned by SessionEnd."""
    time.sleep(0.1)  # Brief delay to let transcript finish writing
    emitted = _flush_turns(
        client=client,
        project_name=project_name,
        session_id=session_id,
        session=session,
        transcript_path=transcript_path,
        state=state,
        key=key,
        allow_partial=True,
    )
    emitted += _poll_active_subagents(
        client=client,
        project_name=project_name,
        trace_id=session_trace_id(session),
        session_id=session_id,
        key=key,
        allow_partial=True,
    )
    if _transcript_has_terminal_error(transcript_path):
        info("Stop detected terminal error; leaving final status to SessionEnd")
    return emitted


def _on_session_end(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    state: dict[str, Any],
    key: str,
    payload_ts: str | None,
) -> int:
    """Handle SessionEnd: best-effort flush, then finalize and persist tombstone state."""
    emitted = _flush_turns(
        client=client,
        project_name=project_name,
        session_id=session_id,
        session=session,
        transcript_path=transcript_path,
        state=state,
        key=key,
        allow_partial=True,
    )
    emitted += _poll_active_subagents(
        client=client,
        project_name=project_name,
        trace_id=session_trace_id(session),
        session_id=session_id,
        key=key,
        allow_partial=True,
    )

    if _transcript_has_terminal_error(transcript_path):
        info("SessionEnd detected terminal error; skipping completed finalize")
    elif not session.trace_finalized:
        trace_id = session_trace_id(session)
        trace_name = session.trace_name or f"session-{session_id[:8]}"
        end_time = parse_ts(session.last_turn_ts or payload_ts or session.trace_start_ts or "")
        start_time = parse_ts(session.trace_start_ts or session.last_turn_ts or payload_ts or "")

        if not session.trace_created:
            ensure_trace(
                client=client,
                project_name=project_name,
                trace_id=trace_id,
                trace_name=trace_name,
                session_id=session_id,
                start_time=start_time,
                end_time=end_time,
                transcript_path=transcript_path,
                session=session,
            )
            session.trace_created = True

        finalized = finalize_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=start_time,
            end_time=end_time,
            transcript_path=transcript_path,
            session=session,
        )
        session.trace_finalized = bool(finalized)
        if finalized:
            info(f"SessionEnd finalized trace_id={trace_id}")
        else:
            info(f"SessionEnd finalize failed trace_id={trace_id}")

    # Keep a finalized tombstone in state to ignore any late/out-of-order hook
    # events that may arrive after SessionEnd.
    save_session_state(state, key, session)
    delete_subagent_states(key)
    save_state(state)
    debug("SessionEnd: finalized")
    return emitted


def _on_subagent_start(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    key: str,
    transcript_path: Path,
    payload: dict[str, Any],
) -> None:
    """Handle SubagentStart: create placeholder span and initialize subagent state."""
    agent_id, agent_type, agent_transcript = extract_agent_info(payload)
    if not agent_id:
        return
    debug(f"SubagentStart: agent_id={agent_id} type={agent_type}")
    started_at = event_timestamp(payload) or datetime.now(timezone.utc).isoformat()
    trace_id = session_trace_id(session)
    trace_name = session.trace_name or maybe_trace_name(transcript_path) or f"session-{session_id[:8]}"
    session.trace_name = trace_name
    if not session.trace_created:
        session.trace_start_ts = session.trace_start_ts or started_at
        ensure_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=parse_ts(session.trace_start_ts),
            end_time=parse_ts(session.trace_start_ts),
            transcript_path=transcript_path,
            session=session,
        )
        session.trace_created = True

    transcript = resolve_subagent_transcript_path(transcript_path, agent_id, agent_transcript)
    subagents = load_subagent_states(key)
    existing = subagents.get(agent_id)
    if existing:
        if agent_type and not existing.agent_type:
            existing.agent_type = agent_type
        if transcript and not existing.transcript_path:
            existing.transcript_path = str(transcript)
        if started_at and not existing.started_at:
            existing.started_at = started_at
        save_subagent_states(key, subagents)
        return

    subagents[agent_id] = SubagentState(
        agent_id=agent_id,
        agent_type=agent_type,
        agent_span_id="",
        transcript_path=str(transcript) if transcript else "",
        started_at=started_at,
        parent_span_id=session.turn_span_id or trace_id,
    )
    save_subagent_states(key, subagents)


def _on_subagent_stop(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    key: str,
    transcript_path: Path,
    payload: dict[str, Any],
) -> None:
    """Handle SubagentStop: final flush child transcript and close placeholder span."""
    agent_id, agent_type, agent_transcript = extract_agent_info(payload)
    debug(
        "SubagentStop observed: "
        f"agent_id={agent_id} type={agent_type} transcript={agent_transcript or ''}"
    )
    if not agent_id:
        return

    subagents = load_subagent_states(key)
    subagent = subagents.get(agent_id)
    if subagent is None:
        started_at = event_timestamp(payload) or datetime.now(timezone.utc).isoformat()
        resolved = resolve_subagent_transcript_path(transcript_path, agent_id, agent_transcript)
        subagent = SubagentState(
            agent_id=agent_id,
            agent_type=agent_type,
            agent_span_id="",
            transcript_path=str(resolved) if resolved else "",
            started_at=started_at,
            parent_span_id=session.turn_span_id or session_trace_id(session),
        )
        subagents[agent_id] = subagent
    elif agent_type and not subagent.agent_type:
        subagent.agent_type = agent_type
    if agent_transcript and not subagent.transcript_path:
        resolved = resolve_subagent_transcript_path(transcript_path, agent_id, agent_transcript)
        if resolved:
            subagent.transcript_path = str(resolved)

    subagent.finished = True

    # If agent span was never lazily created (deferred), create it now with
    # best-effort parent (turn placeholder).  emit_turn_v3 from the parent
    # turn flush will later upsert it under the correct llm span.
    if not subagent.agent_span_id:
        trace_id = session_trace_id(session)
        fallback_parent = subagent.parent_span_id or session.turn_span_id or trace_id
        started = subagent.started_at or datetime.now(timezone.utc).isoformat()
        subagent.agent_span_id = _deterministic_span_id(
            trace_id, "deferred-agent", subagent.agent_id,
        )
        create_or_update_span_if_possible(
            client,
            subagent.agent_span_id,
            trace_id=trace_id,
            parent_span_id=fallback_parent,
            project_name=project_name,
            name=f"Agent: {subagent.agent_type or 'sub-agent'}",
            type="general",
            start_time=parse_ts(started),
            end_time=parse_ts(started),
            input={"subagent_type": subagent.agent_type},
            output={"status": "running"},
            metadata={
                "session_id": session_id,
                "agent_id": subagent.agent_id,
                "subagent_type": subagent.agent_type,
                "deferred_fallback": True,
            },
            tags=["sub-agent", *([f"agent-type:{subagent.agent_type}"] if subagent.agent_type else [])],
        )
        debug(f"SubagentStop: force-created deferred agent span {subagent.agent_span_id}")

    _emit_subagent_turns(
        client=client,
        project_name=project_name,
        trace_id=session_trace_id(session),
        session_id=session_id,
        key=key,
        subagent=subagent,
        allow_partial=True,
        base_depth=1,
        reemit_all=True,
    )
    _finalize_subagent_span(
        client=client,
        project_name=project_name,
        trace_id=session_trace_id(session),
        session_id=session_id,
        subagent=subagent,
    )
    save_subagent_states(key, subagents)


def _on_compact(
    client: Any,
    project_name: str,
    session_id: str,
    session: SessionState,
    transcript_path: Path,
    state: dict[str, Any],
    key: str,
    payload: dict[str, Any],
) -> None:
    """Handle PreCompact: flush pending, create Compaction span, reset offset."""
    trace_id = session_trace_id(session)
    trace_name = session.trace_name or maybe_trace_name(transcript_path) or f"session-{session_id[:8]}"
    session.trace_name = trace_name

    if not session.trace_created:
        now_ts = datetime.now(timezone.utc).isoformat()
        session.trace_start_ts = session.trace_start_ts or now_ts
        ensure_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=parse_ts(session.trace_start_ts),
            end_time=parse_ts(session.trace_start_ts),
            transcript_path=transcript_path,
            session=session,
        )
        session.trace_created = True

    # Flush any pending content before compaction
    _flush_turns(
        client=client,
        project_name=project_name,
        session_id=session_id,
        session=session,
        transcript_path=transcript_path,
        state=state,
        key=key,
    )

    # Create compaction marker span
    custom_instructions = extract_custom_instructions(payload)
    compact_text = f"/compact {custom_instructions}" if custom_instructions else "/compact"
    compact_ts = parse_ts(datetime.now(timezone.utc).isoformat())
    compact_span_id = new_opik_id(compact_ts)

    create_span_if_possible(
        client,
        id=compact_span_id,
        trace_id=trace_id,
        parent_span_id=trace_id,
        project_name=project_name,
        name="Compaction",
        type="general",
        start_time=compact_ts,
        end_time=compact_ts,
        input={"text": compact_text},
        output={"status": "compacted"},
        metadata={"session_id": session_id},
    )

    # Reset offset so post-compaction content is read fresh
    try:
        session.turn_start_offset = transcript_path.stat().st_size
    except Exception:
        pass
    session.last_flush_time = time.time()
    debug(f"PreCompact: marker span created, offset reset to {session.turn_start_offset}")


# ── Main ──────────────────────────────────────────────────────────────────────

def replay_timeout_from_backup(logs_dir: Path) -> int:
    apply_opik_env_overrides()
    if Opik is None and not DRY_RUN:
        debug("opik package not installed for replay")
        return 0

    payload, transcript_path = load_runtime_backup(logs_dir)
    if not payload:
        debug(f"timeout replay skipped: missing backup in {logs_dir}")
        return 0

    session_id = str(payload.get("session_id") or "")
    key = str(payload.get("key") or "")
    project_name = str(payload.get("project_name") or _env_first("OPIK_PROJECT_NAME", "CC_OPIK_PROJECT") or DEFAULT_PROJECT)
    if not session_id or not key:
        debug("timeout replay skipped: missing session_id/key")
        return 0

    state = {key: payload.get("session_state") or {}}
    session = load_session_state(state, key)
    session.trace_finalized = False

    client: Any = None
    if not DRY_RUN:
        try:
            client = Opik(project_name=project_name)
        except Exception as exc:
            debug(f"failed to init Opik client for replay: {exc}")
            return 0

    emitted = 0
    try:
        if transcript_path and transcript_path.exists():
            emitted = _flush_turns(
                client=client,
                project_name=project_name,
                session_id=session_id,
                session=session,
                transcript_path=transcript_path,
                state=state,
                key=key,
                allow_partial=True,
            )

        trace_id = session_trace_id(session)
        trace_name = session.trace_name or f"session-{session_id[:8]}"
        now_iso = datetime.now(timezone.utc).isoformat()
        effective_transcript = transcript_path or Path(payload.get("backup_transcript_path") or BACKUP_TRANSCRIPT_FILE)
        end_time = parse_ts(session.last_turn_ts or now_iso)
        start_time = parse_ts(session.trace_start_ts or session.last_turn_ts or now_iso)

        if not session.trace_created:
            ensure_trace(
                client=client,
                project_name=project_name,
                trace_id=trace_id,
                trace_name=trace_name,
                session_id=session_id,
                start_time=start_time,
                end_time=end_time,
                transcript_path=effective_transcript,
                session=session,
            )
            session.trace_created = True

        finalized = finalize_trace(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            trace_name=trace_name,
            session_id=session_id,
            start_time=start_time,
            end_time=end_time,
            transcript_path=effective_transcript,
            session=session,
            status="timeout",
        )
        session.trace_finalized = bool(finalized)
        save_session_state(state, key, session)
        persist_runtime_backup_to_dir(
            logs_dir=logs_dir,
            state=state,
            key=key,
            session_id=session_id,
            transcript_path=effective_transcript if effective_transcript.exists() else (logs_dir / BACKUP_TRANSCRIPT_FILE.name),
            project_name=project_name,
        )
        if client is not None:
            try:
                client.flush()
            except Exception:
                pass
        info(f"ReplayTimeout emitted={emitted} finalized={finalized} trace_id={trace_id}")
        return 0
    finally:
        try:
            if client is not None:
                client.shutdown()
        except Exception:
            pass


def main() -> int:
    start = time.time()

    argv = sys.argv[1:]
    if argv and argv[0] == "ReplayTimeout":
        logs_dir = LOGS_DIR
        if "--logs-dir" in argv:
            idx = argv.index("--logs-dir")
            if idx + 1 < len(argv):
                logs_dir = Path(argv[idx + 1]).resolve()
        return replay_timeout_from_backup(logs_dir)

    if os.environ.get("TRACE_TO_OPIK", "").lower() != "true":
        return 0
    apply_opik_env_overrides()
    if Opik is None and not DRY_RUN:
        debug("opik package not installed")
        return 0

    argv = sys.argv[1:]
    payload_file = _extract_payload_file_arg(argv)
    payload = read_hook_payload_file(payload_file) if payload_file else read_hook_payload()
    session_id, transcript_path = extract_session_and_transcript(payload)
    event_name = hook_event_name(payload)
    payload_ts = event_timestamp(payload)

    if (not session_id or not transcript_path) and event_name == "SessionEnd":
        fb_session_id, fb_transcript_path = fallback_session_and_transcript_for_session_end()
        if fb_session_id and fb_transcript_path:
            session_id, transcript_path = fb_session_id, fb_transcript_path
            info(
                "SessionEnd payload missing session/transcript; "
                f"fallback to latest transcript session={session_id} path={transcript_path}"
            )

    if not session_id or not transcript_path:
        debug("missing session_id or transcript_path in hook payload")
        return 0
    if not wait_for_transcript(transcript_path):
        debug(f"transcript does not exist: {transcript_path}")
        return 0

    project_name = (
        _env_first("OPIK_PROJECT_NAME", "CC_OPIK_PROJECT")
        or DEFAULT_PROJECT
    )
    if maybe_defer_session_end(payload, event_name, argv):
        return 0

    client: Any = None
    if not DRY_RUN:
        try:
            client = Opik(project_name=project_name)
        except Exception as exc:
            debug(f"failed to init Opik client: {exc}")
            return 0

    emitted = 0
    try:
        with FileLock(LOCK_FILE):
            state = load_state()
            key = state_key(session_id, transcript_path)
            session = load_session_state(state, key)

            trace_name = session.trace_name or maybe_trace_name(transcript_path) or f"session-{session_id[:8]}"
            session.trace_name = trace_name

            # Persist a minimal backup before event-specific handling so timeout replay has host-side state.
            persist_runtime_backup(
                state=state,
                key=key,
                session_id=session_id,
                transcript_path=transcript_path,
                project_name=project_name,
            )

            # Ignore late/out-of-order events after trace has already been finalized.
            if session.trace_finalized and event_name not in ("UserPromptSubmit",):
                if event_name == "SessionEnd":
                    delete_subagent_states(key)
                    save_session_state(state, key, session)
                    save_state(state)
                debug(f"skip finalized session event={event_name}")
                return 0

            # --- Event dispatch (mirrors Go switch in main.go) ---
            if event_name == "UserPromptSubmit":
                _on_user_prompt_submit(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    transcript_path=transcript_path,
                    payload=payload,
                )
                save_session_state(state, key, session)
                save_state(state)

            elif event_name in ("PostToolUse", "PostToolUseFailure"):
                emitted = _on_tool_use(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    transcript_path=transcript_path,
                    state=state,
                    key=key,
                )
                save_session_state(state, key, session)
                save_state(state)

            elif event_name == "Stop":
                emitted = _on_stop(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    transcript_path=transcript_path,
                    state=state,
                    key=key,
                )
                save_session_state(state, key, session)
                save_state(state)

            elif event_name == "SessionEnd":
                emitted = _on_session_end(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    transcript_path=transcript_path,
                    state=state,
                    key=key,
                    payload_ts=payload_ts,
                )
                # state already saved inside _on_session_end

            elif event_name == "SubagentStart":
                _on_subagent_start(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    key=key,
                    transcript_path=transcript_path,
                    payload=payload,
                )
                save_session_state(state, key, session)
                save_state(state)

            elif event_name == "SubagentStop":
                _on_subagent_stop(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    key=key,
                    transcript_path=transcript_path,
                    payload=payload,
                )
                save_session_state(state, key, session)
                save_state(state)

            elif event_name == "PreCompact":
                _on_compact(
                    client=client,
                    project_name=project_name,
                    session_id=session_id,
                    session=session,
                    transcript_path=transcript_path,
                    state=state,
                    key=key,
                    payload=payload,
                )
                save_session_state(state, key, session)
                save_state(state)

            else:
                debug(f"unknown event: {event_name}")
                save_session_state(state, key, session)
                save_state(state)

        try:
            with FileLock(LOCK_FILE):
                latest_state = load_state()
                persist_runtime_backup(
                    state=latest_state,
                    key=key,
                    session_id=session_id,
                    transcript_path=transcript_path,
                    project_name=project_name,
                )
        except Exception as backup_exc:
            debug(f"runtime backup failed: {backup_exc}")

        try:
            if client is not None:
                client.flush()
        except Exception:
            pass

        duration_s = time.time() - start
        if emitted:
            info(
                f"emitted {emitted} turn(s) to Opik in {duration_s:.2f}s "
                f"(session={session_id}, event={event_name})"
            )
        return 0
    except Exception as exc:
        debug(f"unexpected failure: {exc}")
        return 0
    finally:
        if payload_file:
            try:
                os.remove(payload_file)
            except Exception:
                pass
        try:
            if client is not None:
                client.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
