#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 Shanghai Innovation Institute
"""
OpenCode -> Opik realtime hook.

Design:
- hook payload only triggers parsing; authoritative data comes from opencode.db
- session state is persisted across hook invocations
- turns are emitted incrementally as nested Opik spans
- sub-agent sessions are synced recursively when their child session rows exist

Expected hook payload:
- stdin JSON and/or argv[1] event name
- should include session_id and optionally db path

This script is fail-open: any parsing or Opik error returns 0 so it does not
block the editor/runtime hook pipeline.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import signal
import sqlite3
import sys
import threading
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


ROOT = Path(__file__).resolve().parent
STATE_DIR = Path.home() / ".opencode" / "state"
STATE_FILE = STATE_DIR / "opik_realtime_state.json"
LOCK_FILE = STATE_DIR / "opik_realtime_state.lock"
LOG_FILE = STATE_DIR / "opik_realtime.log"

DEBUG = os.environ.get("OC_OPIK_DEBUG", "").lower() == "true"
DRY_RUN = os.environ.get("OC_OPIK_DRY_RUN", "").lower() == "true"
MAX_TEXT_CHARS = int(os.environ.get("OC_OPIK_MAX_TEXT_CHARS", "20000"))
DEFAULT_PROJECT = os.environ.get("OC_OPIK_PROJECT", "opencode-realtime")
FLUSH_INTERVAL_S = float(
    os.environ.get("OC_OPIK_FLUSH_INTERVAL_S")
    or os.environ.get("OC_OPIK_FLUSH_INTERVAL")
    or "5"
)
# Hard wallclock budget for the whole hook invocation. Bounds the worst case
# where flush_turns / individual Opik span POSTs stall on a network hang —
# without this the hook can sit forever inside a single HTTP call. SIGALRM is
# Linux-only; on platforms that lack it the deadline is silently skipped.
HOOK_DEADLINE_S = float(os.environ.get("OC_OPIK_HOOK_DEADLINE_S", "60"))
# Separate bound for the SDK's background uploader flush. The flush runs in a
# daemon thread that we join with this timeout; if it doesn't return in time we
# log `flush=timeout` and exit (the leaked daemon dies with the process).
OPIK_FLUSH_TIMEOUT_S = float(os.environ.get("OC_OPIK_FLUSH_TIMEOUT_S", "10"))
SPAN_BATCH_ENV_NAMES = span_batch_env_names()


# System-injected content classification.
#
# Two persisted signals identify model-visible context that wasn't typed by
# the human:
#   1. TextPart.synthetic === true  (opencode message-v2.ts:65) — canonical.
#   2. XML-style injected blocks embedded in either a user TextPart's text
#      or a tool part's state.output string.
#
# The ephemeral wrapper at session/prompt.ts:582-599 mutates a clone of
# `msgs` and is NEVER persisted — DB tracers cannot observe it. Do not try
# to reconstruct it; the human bytes survive intact in the persisted text.
#
# This registry mirrors the Claude tracer's user-channel injected markers so
# Opik can display them as separate content items with subtype/tag instead of
# folding them into ordinary text. `<env>` is system-prompt content
# (session/system.ts:33), not user/tool injection, so it is deliberately not
# included here.
_INJECTED_TAGS = (
    # Continuation / reminder tags.
    "system-reminder", "task-notification",
    # Slash-command surface.
    "command-name", "command-message", "command-args",
    "local-command-stdout", "local-command-stderr", "local-command-caveat",
    # Lifecycle hooks.
    "user-prompt-submit-hook", "session-start-hook", "stop-hook",
    "post-tool-use-hook", "pre-tool-use-hook",
    # Built-in bash tool surface.
    "bash-input", "bash-stdout", "bash-stderr",
    # IDE / file context.
    "ide_opened_file", "ide_selection", "ide_diagnostics",
    "file-system",
    # Attachment-style surfaces seen in Claude traces and compatible harnesses.
    "available-skills", "nested-memory",
)
_INJECTED_BLOCK_RE = re.compile(
    r"<(?P<tag>" + "|".join(re.escape(t) for t in _INJECTED_TAGS) + r")>"
    r"(?P<body>.*?)</(?P=tag)>",
    re.DOTALL,
)
_INJECTED_TAG_PREFIX_RE = re.compile(
    r"<(?:" + "|".join(re.escape(t) for t in _INJECTED_TAGS) + r")>"
)
_TOOL_OUTPUT_INJECTED_TAGS = ("system-reminder",)
_TOOL_OUTPUT_INJECTED_BLOCK_RE = re.compile(
    r"<(?P<tag>" + "|".join(re.escape(t) for t in _TOOL_OUTPUT_INJECTED_TAGS) + r")>"
    r"(?P<body>.*?)</(?P=tag)>",
    re.DOTALL,
)
_TOOL_OUTPUT_INJECTED_TAG_PREFIX_RE = re.compile(
    r"<(?:" + "|".join(re.escape(t) for t in _TOOL_OUTPUT_INJECTED_TAGS) + r")>"
)
_TOOL_CONTENT_BLOCK_RE = re.compile(r"<content>.*?</content>", re.DOTALL)


def has_tool_output_injected_block(text: str) -> bool:
    return bool(_TOOL_OUTPUT_INJECTED_TAG_PREFIX_RE.search(text))


def has_injected_block(text: str) -> bool:
    return bool(_INJECTED_TAG_PREFIX_RE.search(text))


def _remove_injected_blocks(
    text: str,
    block_re: "re.Pattern[str]" = _INJECTED_BLOCK_RE,
) -> tuple[str, list[dict[str, str]]]:
    blocks: list[dict[str, str]] = []

    def _capture(m: "re.Match[str]") -> str:
        blocks.append({"tag": m.group("tag"), "content": m.group("body").strip()})
        return ""

    return block_re.sub(_capture, text), blocks


def extract_injected_blocks(text: str) -> tuple[str, list[dict[str, str]]]:
    """Return (text_with_tags_stripped, [{tag, content}, ...]).

    Scans the whole string so mixed content like
    "Fix the bug\\n<system-reminder>X</system-reminder>" returns
    ("Fix the bug", [{"tag": "system-reminder", "content": "X"}]).
    """
    stripped, blocks = _remove_injected_blocks(text)
    return stripped.strip(), blocks


def extract_injected_blocks_from_tool_output(text: str) -> tuple[str, list[dict[str, str]]]:
    """Lift injected blocks from tool output without scanning file contents.

    OpenCode's read output wraps file bytes in <content>...</content>. Literal
    reminder tags inside that region are user data, not injected context.
    """
    blocks: list[dict[str, str]] = []
    cleaned_parts: list[str] = []
    offset = 0

    for match in _TOOL_CONTENT_BLOCK_RE.finditer(text):
        cleaned, found = _remove_injected_blocks(
            text[offset:match.start()],
            _TOOL_OUTPUT_INJECTED_BLOCK_RE,
        )
        cleaned_parts.append(cleaned)
        cleaned_parts.append(match.group(0))
        blocks.extend(found)
        offset = match.end()

    cleaned, found = _remove_injected_blocks(text[offset:], _TOOL_OUTPUT_INJECTED_BLOCK_RE)
    cleaned_parts.append(cleaned)
    blocks.extend(found)
    return "".join(cleaned_parts).strip(), blocks


def _log(level: str, message: str) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(f"{stamp} [{level}] {message}\n")
    except Exception:
        pass


def debug(message: str) -> None:
    if DEBUG:
        _log("DEBUG", message)


def info(message: str) -> None:
    _log("INFO", message)


class HookTimeoutError(TimeoutError):
    """Raised by the SIGALRM handler when HOOK_DEADLINE_S is exceeded."""


def _alarm_handler(signum: int, frame: Any) -> None:
    raise HookTimeoutError(f"hook deadline {HOOK_DEADLINE_S}s exceeded")


def _install_hook_deadline(seconds: float) -> bool:
    if not hasattr(signal, "SIGALRM") or seconds <= 0:
        return False
    try:
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(max(1, int(seconds)))
        return True
    except (ValueError, OSError):
        return False


def _clear_hook_deadline() -> None:
    if not hasattr(signal, "SIGALRM"):
        return
    try:
        signal.alarm(0)
    except (ValueError, OSError):
        pass


def _flush_with_timeout(client: Any, timeout_s: float) -> str:
    """Run ``client.flush()`` in a daemon thread bounded by ``timeout_s``.

    Returns one of: ``skipped`` (no client / no flush method), ``ok``,
    ``timeout``, or ``error:<ExceptionClassName>``. Leaked threads are
    daemonized so they cannot block process exit.
    """
    if client is None or not hasattr(client, "flush"):
        return "skipped"
    result = {"status": "pending"}

    def _runner() -> None:
        try:
            client.flush()
            result["status"] = "ok"
        except Exception as exc:
            result["status"] = f"error:{exc.__class__.__name__}"

    thread = threading.Thread(target=_runner, name="opik-flush", daemon=True)
    thread.start()
    thread.join(timeout=max(0.1, timeout_s))
    if thread.is_alive():
        return "timeout"
    return result["status"]


def _describe_data_source(data_source: Any) -> str:
    if data_source is None:
        return "none"
    if isinstance(data_source, Path):
        return f"db:{data_source}"
    root = getattr(data_source, "root", None)
    if root is not None:
        return f"json:{root}"
    return f"{type(data_source).__name__}:{data_source!r}"


# ─────────────────────────────────────────────────────────────────
# Inlined DB read + Turn/Round/LLM/Tool tree builder.
#
# Self-contained, pure stdlib. Built to satisfy the design's parsing surface:
# open the OpenCode SQLite DB read-only, walk session/message/part rows, and
# assemble Turn → Round → AgentStep (LLM call) → ToolCall hierarchy with the
# three token views (api_billed / incremental / snapshot) per design §4.
#
# Critical: open_db uses `?mode=ro` per design §1 (lines 53-57). `immutable=1`
# is forbidden against a live WAL DB — it lies to SQLite about the writer and
# risks SQLITE_CORRUPT.
# ─────────────────────────────────────────────────────────────────


@dataclass
class ToolCall:
    call_id: str
    name: str
    start_time: str
    end_time: str
    input: Any = field(default_factory=dict)
    output: Any = None
    status: str = "unknown"
    title: str = ""
    tool_metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    is_subagent: bool = False
    child_session_id: str = ""
    child_agent: str = ""
    child_model: str = ""
    child_provider: str = ""
    child_description: str = ""
    child_prompt: str = ""


@dataclass
class AgentStep:
    step_index: int
    message_id: str
    model: str
    provider_id: str
    agent: str
    mode: str
    variant: str
    timestamp: str
    time_completed: str
    path_cwd: str = ""
    path_root: str = ""
    text: str = ""
    reasoning: str = ""
    tools: list[ToolCall] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)
    finish_reason: str | None = None
    cost: float | int | None = None
    # Set True iff the step's parts include a `step-finish` row. Canonical
    # signal of step completeness per DESIGN §3.3 — `incomplete` below is
    # derived from this and kept around for stats / debug payload readers.
    has_step_finish: bool = False
    incomplete: bool = False


@dataclass
class ParsedTurn:
    turn_index: int
    user_input: str
    timestamp: str
    is_system: bool = False
    models: set[str] = field(default_factory=set)
    agents: set[str] = field(default_factory=set)
    steps: list[AgentStep] = field(default_factory=list)
    final_output: str = ""
    usage: dict[str, int] = field(default_factory=lambda: {
        "input_tokens": 0, "output_tokens": 0,
        "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0,
        "reasoning_tokens": 0,
    })
    # Model-visible context that wasn't typed by the human (synthetic parts,
    # lifted <system-reminder> blocks, compaction/subtask parts).
    # Each entry: {"tag": str, "content": str, "ts": str, "source": str}
    # source ∈ {"synthetic_part", "tag_in_text", "tag_in_tool_output",
    #           "compaction_part", "subtask_part"}
    system_injected: list[dict[str, str]] = field(default_factory=list)
    # Parts that opencode persisted but excluded from LLM input (ignored=true).
    # Distinct from system_injected: the model never saw these.
    excluded_parts: list[dict[str, str]] = field(default_factory=list)


@dataclass
class SessionMeta:
    session_id: str
    title: str
    directory: str
    time_created: int
    time_updated: int
    version: str = ""


@dataclass
class ReasoningRound:
    round_idx: int
    step_items: list[tuple[int, AgentStep]] = field(default_factory=list)


@dataclass
class JsonStore:
    root: Path


def ms_to_iso(ms: int | None) -> str:
    if not ms:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def parse_ts(value: str) -> datetime:
    if value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def strip_model_date(model: str) -> str:
    return re.sub(r"-\d{8}$", "", model) if model else model


def stringify_content(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, dict):
                if item.get("type") == "text" and item.get("text"):
                    parts.append(str(item["text"]))
                else:
                    parts.append(json.dumps(item, ensure_ascii=False))
            else:
                parts.append(str(item))
        return "\n".join(part for part in parts if part)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, indent=2)
    return str(value)


def build_usage_metadata(usage: dict[str, Any]) -> dict[str, Any] | None:
    if not usage:
        return None
    input_tokens = int(usage.get("input_tokens", 0) or 0)
    output_tokens = int(usage.get("output_tokens", 0) or 0)
    reasoning_tokens = int(usage.get("reasoning_tokens", 0) or 0)
    cache_read = int(usage.get("cache_read_input_tokens", 0) or 0)
    cache_creation = int(usage.get("cache_creation_input_tokens", 0) or 0)
    total_tokens = input_tokens + output_tokens + reasoning_tokens + cache_read + cache_creation
    if total_tokens == 0:
        return None
    combined_input = input_tokens + cache_read + cache_creation
    total_tokens = combined_input + output_tokens + reasoning_tokens
    return {
        "billed_input_tokens": input_tokens,
        "input_tokens": combined_input,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "output_token_details": {"reasoning": reasoning_tokens},
        "input_token_details": {
            "cache_read": cache_read,
            "cache_creation": cache_creation,
        },
    }


def tokens_from_msg(msg_data: dict[str, Any]) -> dict[str, int]:
    t = msg_data.get("tokens") or {}
    cache = t.get("cache") or {}
    return {
        "input_tokens": int(t.get("input", 0) or 0),
        "output_tokens": int(t.get("output", 0) or 0),
        "reasoning_tokens": int(t.get("reasoning", 0) or 0),
        "cache_read_input_tokens": int(cache.get("read", 0) or 0),
        "cache_creation_input_tokens": int(cache.get("write", 0) or 0),
    }


def estimate_tokens(content: Any) -> int:
    if content is None:
        return 0
    text = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    return max(1, len(text) // 4) if text else 0


def step_end_timestamp(step: AgentStep) -> str:
    if step.time_completed:
        return step.time_completed
    if step.tools:
        return max(t.end_time or t.start_time or step.timestamp for t in step.tools)
    return step.timestamp


def accumulate_usage(target: dict[str, int], source: dict[str, int]) -> None:
    for k in ("input_tokens", "output_tokens", "cache_read_input_tokens",
              "cache_creation_input_tokens", "reasoning_tokens"):
        target[k] = target.get(k, 0) + source.get(k, 0)


_USAGE_TOKEN_KEYS = (
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "reasoning_tokens",
)


def last_real_usage(steps: list[AgentStep]) -> dict[str, int]:
    """Return the usage of the last step with a real LLM observation.

    DESIGN §4: snapshot comes from "the last LLM call with real usage in the
    turn". Definition of "real": `step.usage` exists AND has at least one
    non-zero token field. We deliberately do NOT trust `has_step_finish`
    alone — OpenCode can write a step-finish row with all token fields = 0
    (failed / aborted / retried calls with cost=0). Picking that row would
    reset the snapshot to 0 even though a prior completed call carries the
    real context. Completeness is `turn_is_incomplete`'s job; this function
    only cares whether there's real data to snapshot.
    """
    for step in reversed(steps):
        if not step.usage:
            continue
        if any(int(step.usage.get(k, 0) or 0) > 0 for k in _USAGE_TOKEN_KEYS):
            return step.usage
    return {}


# OpenCode token semantics (verified empirically against ~/.local/share/opencode/opencode.db):
#
#   `step-finish.tokens.{input, output, reasoning, cache.read, cache.write}` is
#   the **per-call billed amount** for that single API call — NOT a cumulative
#   running total. The series is non-monotonic across a session: e.g. a long
#   context prompt with 42k input followed by a smaller continuation with 8k
#   input. Each value is what the provider actually charged for that one call.
#
#   Therefore:
#     - `api_billed_*` per Turn / Round / Trace = sum across that scope's
#       step.usage values. This IS what you'd see on a provider bill.
#     - There is no meaningful "incremental delta" view — taking
#       `current.input - prev.input` subtracts two unrelated per-call values
#       and clamping to zero (as v1 of this code did) produces garbage.
#       The previous `incremental_usage` machinery has been removed.
#     - `snapshot_*` = the prompt size of the **last** real LLM call in the
#       turn (via `last_real_usage()`), useful as a "context pressure right
#       now" indicator.
#
#   DESIGN §4's three-views table conflated W1 (per-call billing) and W2
#   (cumulative snapshot) and was internally contradictory; the doc needs an
#   update to match this implementation.


def open_db(db_path: Path | str) -> sqlite3.Connection:
    # mode=ro is the correct read-only flag for a live WAL DB.
    # immutable=1 would lie to SQLite about a live writer (design §1, lines 53-57).
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def resolve_session(conn: sqlite3.Connection, session_id: str | None) -> SessionMeta:
    if isinstance(conn, JsonStore):
        return resolve_session_json(conn, session_id)

    if session_id:
        row = conn.execute(
            "SELECT id, title, directory, time_created, time_updated, version FROM session WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"Session not found: {session_id}")
    else:
        row = conn.execute(
            "SELECT id, title, directory, time_created, time_updated, version "
            "FROM session ORDER BY time_created DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise LookupError("No sessions found in database")
    return SessionMeta(
        session_id=row["id"],
        title=row["title"],
        directory=row["directory"],
        time_created=row["time_created"],
        time_updated=row["time_updated"],
        version=row["version"] or "",
    )


def session_exists(conn: sqlite3.Connection, session_id: str) -> bool:
    if isinstance(conn, JsonStore):
        return (conn.root / "session" / "info" / f"{session_id}.json").exists()

    row = conn.execute("SELECT 1 FROM session WHERE id = ?", (session_id,)).fetchone()
    return row is not None


def session_parent_id(conn: sqlite3.Connection | JsonStore, session_id: str) -> str | None:
    """Return the parent session_id when *session_id* is a sub-agent session.

    opencode's `task` tool spawns a child session row with `parent_id` set
    (schema in opencode/src/session/index.ts). When that child fires plugin
    events, the hook is invoked with the child's session_id. Returning
    non-None here lets `main` short-circuit so we don't emit a duplicate
    top-level trace — the parent's `sync_subagent_sessions` nests the child
    under the originating `task` tool span.
    """
    if isinstance(conn, JsonStore):
        info_path = conn.root / "session" / "info" / f"{session_id}.json"
        try:
            data = json.loads(info_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        parent = data.get("parentID") or data.get("parent_id")
        return str(parent) if parent else None

    try:
        row = conn.execute(
            "SELECT parent_id FROM session WHERE id = ?",
            (session_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        # parent_id column may be absent on older opencode schemas.
        return None
    if row is None:
        return None
    parent = row["parent_id"] if "parent_id" in row.keys() else None
    return str(parent) if parent else None


def load_messages(conn: sqlite3.Connection, session_id: str) -> list[dict[str, Any]]:
    if isinstance(conn, JsonStore):
        msg_dir = conn.root / "session" / "message" / session_id
        rows: list[dict[str, Any]] = []
        for path in sorted(msg_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                debug(f"failed to read message json {path}: {exc}")
                continue
            rows.append({"id": data.get("id") or path.stem, "data": data})
        rows.sort(key=lambda item: (
            ((item.get("data") or {}).get("time") or {}).get("created") or 0,
            item.get("id") or "",
        ))
        return rows

    rows = conn.execute(
        "SELECT id, data FROM message WHERE session_id = ? ORDER BY time_created, id",
        (session_id,),
    ).fetchall()
    return [{"id": row["id"], "data": json.loads(row["data"])} for row in rows]


def load_parts(conn: sqlite3.Connection, message_id: str) -> list[dict[str, Any]]:
    if isinstance(conn, JsonStore):
        session_root = conn.root / "session" / "part"
        candidates = list(session_root.glob(f"*/{message_id}/*.json"))
        parts: list[dict[str, Any]] = []
        for path in candidates:
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                debug(f"failed to read part json {path}: {exc}")
                continue
            parts.append(data)
        parts.sort(key=lambda part: (
            _part_sort_time(part),
            part.get("id") or "",
        ))
        return parts

    rows = conn.execute(
        "SELECT data FROM part WHERE message_id = ? ORDER BY time_created, id",
        (message_id,),
    ).fetchall()
    return [json.loads(row["data"]) for row in rows]


def _part_sort_time(part: dict[str, Any]) -> int:
    time_info = part.get("time") or {}
    if isinstance(time_info, dict):
        for key in ("created", "start", "end", "completed"):
            value = time_info.get(key)
            if isinstance(value, int):
                return value
    state_time = ((part.get("state") or {}).get("time") or {})
    if isinstance(state_time, dict):
        for key in ("start", "end"):
            value = state_time.get(key)
            if isinstance(value, int):
                return value
    return 0


def resolve_session_json(store: JsonStore, session_id: str | None) -> SessionMeta:
    info_dir = store.root / "session" / "info"
    if session_id:
        info_path = info_dir / f"{session_id}.json"
    else:
        infos = sorted(
            info_dir.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not infos:
            raise LookupError("No sessions found in JSON storage")
        info_path = infos[0]

    if not info_path.exists():
        raise LookupError(f"Session not found: {session_id}")
    data = json.loads(info_path.read_text(encoding="utf-8"))
    time_info = data.get("time") or {}
    directory = ""
    session_id = data.get("id") or info_path.stem
    for msg in load_messages(store, session_id):
        path_info = (msg.get("data") or {}).get("path") or {}
        directory = path_info.get("root") or path_info.get("cwd") or directory
        if directory:
            break
    return SessionMeta(
        session_id=session_id,
        title=data.get("title") or session_id,
        directory=directory,
        time_created=int(time_info.get("created") or 0),
        time_updated=int(time_info.get("updated") or time_info.get("created") or 0),
        version=data.get("version") or "",
    )


def parse_turns(conn: sqlite3.Connection, session_id: str | None) -> tuple[list[ParsedTurn], SessionMeta]:
    session_meta = resolve_session(conn, session_id)
    turns: list[ParsedTurn] = []
    current_turn: ParsedTurn | None = None

    for msg in load_messages(conn, session_meta.session_id):
        msg_data = msg["data"]
        role = msg_data.get("role")
        time_info = msg_data.get("time") or {}
        timestamp = ms_to_iso(time_info.get("created"))
        time_completed = ms_to_iso(time_info.get("completed")) if time_info.get("completed") else timestamp

        if role == "user":
            # Classify each persisted text part. Two signals identify
            # system-injected (model-visible-but-not-human) content:
            #   1. part.synthetic === true (opencode message-v2.ts:65) — canonical.
            #   2. XML-style injected blocks embedded in human text.
            # part.ignored === true is NOT system-injected: opencode strips
            # those before sending to the model (message-v2.ts:486). Track them
            # separately on excluded_parts so traces can still surface the
            # excluded content without misrepresenting model input.
            # The ephemeral wrapper at session/prompt.ts:582-599 lives on a
            # clone of msgs and never reaches the DB, so this branch cannot
            # observe it and must not synthesize it.
            human_chunks: list[str] = []
            injected_blocks: list[dict[str, str]] = []
            excluded_blocks: list[dict[str, str]] = []

            for part in load_parts(conn, msg["id"]):
                if part.get("type") != "text":
                    continue
                text = part.get("text") or ""
                if not text:
                    continue

                if part.get("synthetic") is True:
                    injected_blocks.append({
                        "tag": "synthetic",
                        "content": text.strip(),
                        "ts": timestamp,
                        "source": "synthetic_part",
                    })
                    continue

                if part.get("ignored") is True:
                    excluded_blocks.append({
                        "tag": "ignored",
                        "content": text.strip(),
                        "ts": timestamp,
                        "source": "ignored_part",
                    })
                    continue

                human_text, blocks = extract_injected_blocks(text)
                for b in blocks:
                    injected_blocks.append({
                        **b,
                        "ts": timestamp,
                        "source": "tag_in_text",
                    })
                if human_text:
                    human_chunks.append(human_text)

            combined_human = "\n".join(human_chunks).strip()

            if combined_human:
                current_turn = ParsedTurn(
                    turn_index=len(turns) + 1,
                    user_input=combined_human,
                    timestamp=timestamp,
                    is_system=False,
                )
                turns.append(current_turn)
            elif injected_blocks or excluded_blocks:
                # Pure-injected continuation. Attach to the in-progress turn
                # rather than opening a new one. If no turn exists yet, seed
                # one so subsequent assistant rows aren't dropped at the
                # `current_turn is None` guard below.
                if current_turn is None:
                    current_turn = ParsedTurn(
                        turn_index=len(turns) + 1,
                        user_input="",
                        timestamp=timestamp,
                        is_system=True,
                    )
                    turns.append(current_turn)

            if current_turn is not None:
                current_turn.system_injected.extend(injected_blocks)
                current_turn.excluded_parts.extend(excluded_blocks)
            continue

        if role != "assistant" or current_turn is None:
            continue

        model = msg_data.get("modelID") or "unknown-model"
        provider_id = msg_data.get("providerID") or ""
        agent_name = msg_data.get("agent") or ""
        mode = msg_data.get("mode") or ""
        variant = msg_data.get("variant") or ""
        path_info = msg_data.get("path") or {}
        msg_cost = msg_data.get("cost")
        msg_finish = msg_data.get("finish") or None
        msg_tokens = tokens_from_msg(msg_data)

        current_turn.models.add(model)
        if agent_name:
            current_turn.agents.add(agent_name)

        step = AgentStep(
            step_index=len(current_turn.steps) + 1,
            message_id=msg["id"],
            model=model,
            provider_id=provider_id,
            agent=agent_name,
            mode=mode,
            variant=variant,
            timestamp=timestamp,
            time_completed=time_completed,
            path_cwd=path_info.get("cwd") or "",
            path_root=path_info.get("root") or "",
            cost=msg_cost,
            finish_reason=msg_finish,
            usage=msg_tokens,
        )

        for part in load_parts(conn, msg["id"]):
            ptype = part.get("type")

            if ptype == "reasoning":
                reasoning = (part.get("text") or "").strip()
                if reasoning:
                    step.reasoning = (
                        f"{step.reasoning}\n\n{reasoning}".strip()
                        if step.reasoning else reasoning
                    )

            elif ptype == "text":
                text = (part.get("text") or "").strip()
                if text:
                    step.text = f"{step.text}\n\n{text}".strip() if step.text else text

            elif ptype == "tool":
                state = part.get("state") or {}
                state_time = state.get("time") or {}
                tool_start = ms_to_iso(state_time.get("start")) if state_time.get("start") else timestamp
                tool_end = ms_to_iso(state_time.get("end")) if state_time.get("end") else tool_start

                output = state.get("output")
                # OpenCode can append <system-reminder> to tool output. Keep
                # this narrower than the user-channel registry: generic bash
                # output can legitimately contain Claude-style XML snippets
                # in source diffs or logs.
                if isinstance(output, str) and has_tool_output_injected_block(output):
                    cleaned, blocks = extract_injected_blocks_from_tool_output(output)
                    if blocks:
                        for b in blocks:
                            current_turn.system_injected.append({
                                **b,
                                "ts": tool_end,
                                "source": "tag_in_tool_output",
                            })
                        output = cleaned
                error = None
                status = state.get("status") or "unknown"
                # opencode tool lifecycle: pending → running → completed | error.
                # Only the terminal failure states are errors; in-progress states
                # ("running"/"pending") and the transient "unknown" (status field
                # not yet written) must NOT be tagged as errors, otherwise every
                # mid-flight tool shows up red in the Opik UI under partial mode.
                if status in {"error", "failed"}:
                    error = stringify_content(output) or f"tool status={status}"

                tool_name = part.get("tool", "unknown")
                call_meta = state.get("metadata") or {}
                tool_input = state.get("input") or {}

                is_subagent = tool_name == "task"
                child_session_id = ""
                child_agent = ""
                child_model = ""
                child_provider = ""
                child_description = ""
                child_prompt = ""
                if is_subagent:
                    child_session_id = call_meta.get("sessionId") or ""
                    child_agent = call_meta.get("agent") or ""
                    child_model_info = call_meta.get("model") or {}
                    child_model = child_model_info.get("modelID") or ""
                    child_provider = child_model_info.get("providerID") or ""
                    child_description = call_meta.get("description") or tool_input.get("description") or ""
                    child_prompt = call_meta.get("prompt") or tool_input.get("prompt") or ""

                step.tools.append(ToolCall(
                    call_id=part.get("callID") or str(uuid.uuid4()),
                    name=tool_name,
                    start_time=tool_start,
                    end_time=tool_end,
                    input=tool_input,
                    output=output,
                    status=status,
                    title=part.get("title") or "",
                    tool_metadata=call_meta,
                    error=error,
                    is_subagent=is_subagent,
                    child_session_id=child_session_id,
                    child_agent=child_agent,
                    child_model=child_model,
                    child_provider=child_provider,
                    child_description=child_description,
                    child_prompt=child_prompt,
                ))

            elif ptype == "step-finish":
                step.has_step_finish = True
                tokens = part.get("tokens") or {}
                cache = tokens.get("cache") or {}
                step.usage = {
                    "input_tokens": int(tokens.get("input", 0) or 0),
                    "output_tokens": int(tokens.get("output", 0) or 0),
                    "reasoning_tokens": int(tokens.get("reasoning", 0) or 0),
                    "cache_read_input_tokens": int(cache.get("read", 0) or 0),
                    "cache_creation_input_tokens": int(cache.get("write", 0) or 0),
                }
                if part.get("reason"):
                    step.finish_reason = part["reason"]
                if part.get("cost") is not None:
                    step.cost = part["cost"]

            elif ptype == "compaction":
                # Compaction summary is model-visible context (subsequent
                # steps see the compacted history). agent/retry parts are
                # control-flow only and intentionally skipped.
                current_turn.system_injected.append({
                    "tag": "compaction",
                    "content": json.dumps(
                        {"auto": bool(part.get("auto"))},
                        ensure_ascii=False,
                    ),
                    "ts": timestamp,
                    "source": "compaction_part",
                })

            elif ptype == "subtask":
                # Subtask result text is fed back to the parent agent.
                content = (part.get("prompt") or "")[:MAX_TEXT_CHARS]
                current_turn.system_injected.append({
                    "tag": "subtask",
                    "content": content,
                    "ts": timestamp,
                    "source": "subtask_part",
                })

        if not any([step.text, step.reasoning, step.tools, step.usage]):
            continue

        # `incomplete` is the canonical "no step-finish observed" signal.
        # Kept as a derived field so `_session_stats` / debug payloads still work.
        step.incomplete = not step.has_step_finish
        if step.text:
            current_turn.final_output = step.text

        accumulate_usage(current_turn.usage, step.usage)
        current_turn.steps.append(step)

    for t in turns:
        if not t.steps and not t.is_system:
            t.is_system = True

    _reconstruct_runtime_injections(turns)

    return turns, session_meta


# Verbatim snapshots of opencode's reminder text (commit ~0.4.x). Embedded so
# traces show what the model actually saw rather than a synthetic placeholder.
# These can drift if upstream edits the prompt files — drift is acceptable
# because the `source` field flags entries as reconstructed.
_PLAN_MODE_REMINDER_TEXT = (
    "# Plan Mode - System Reminder\n\n"
    "CRITICAL: Plan mode ACTIVE - you are in READ-ONLY phase. STRICTLY FORBIDDEN: "
    "ANY file edits, modifications, or system changes. You may ONLY observe, "
    "analyze, and plan. Any modification attempt is a critical violation.\n\n"
    "## Responsibility\n\n"
    "Think, read, search, and delegate explore agents to construct a well-formed "
    "plan. Ask clarifying questions or weigh tradeoffs with the user before "
    "implementation.\n\n"
    "(Snapshot of opencode/session/prompt/plan.txt — injected via insertReminders "
    "every step while agent=='plan'; not persisted on the legacy flag-off path.)"
)
_BUILD_SWITCH_REMINDER_TEXT = (
    "Your operational mode has changed from plan to build. "
    "You are no longer in read-only mode. You are permitted to make file "
    "changes, run shell commands, and utilize your arsenal of tools as needed.\n\n"
    "(Snapshot of opencode/session/prompt/build-switch.txt — injected via "
    "insertReminders on the first build turn after plan; not persisted on the "
    "legacy flag-off path.)"
)


def _has_injection_with_marker(turn: ParsedTurn, marker: str) -> bool:
    return any(marker in (entry.get("content") or "") for entry in turn.system_injected)


def _reconstruct_runtime_injections(turns: list[ParsedTurn]) -> None:
    """Surface opencode reminders that aren't reliably persisted.

    Two reminders are injected by `session/prompt.ts::insertReminders` on
    every iteration but only land in the DB when the new plan-mode flag
    (`OPENCODE_EXPERIMENTAL_PLAN_MODE`) is active:
      - Plan-mode reminder (PROMPT_PLAN) while `agent == "plan"`.
      - Build-switch reminder (BUILD_SWITCH) on the first `build` turn after
        a `plan` turn.

    Without this reconstruction, plan-mode sessions show no system_injected
    even though the model saw a substantial reminder on every step. Entries
    are tagged source=`reconstructed_*` so consumers can tell them apart from
    actually-persisted captures.
    """
    prev_had_plan = False
    for turn in turns:
        agents_in_turn = {step.agent for step in turn.steps if step.agent}
        has_plan = "plan" in agents_in_turn
        has_build = "build" in agents_in_turn

        if has_plan and not _has_injection_with_marker(turn, "Plan Mode - System Reminder"):
            turn.system_injected.append({
                "tag": "system-reminder",
                "content": _PLAN_MODE_REMINDER_TEXT,
                "ts": turn.timestamp,
                "source": "reconstructed_plan_mode",
            })

        if (
            has_build
            and prev_had_plan
            and not _has_injection_with_marker(turn, "mode has changed from plan to build")
        ):
            turn.system_injected.append({
                "tag": "system-reminder",
                "content": _BUILD_SWITCH_REMINDER_TEXT,
                "ts": turn.timestamp,
                "source": "reconstructed_build_switch",
            })

        prev_had_plan = has_plan


def _compute_token_summary(turns: list[ParsedTurn]) -> dict[str, int]:
    all_steps = [s for t in turns for s in t.steps]
    t_in = sum(s.usage.get("input_tokens", 0) for s in all_steps)
    t_out = sum(s.usage.get("output_tokens", 0) for s in all_steps)
    t_cr = sum(s.usage.get("cache_read_input_tokens", 0) for s in all_steps)
    t_cw = sum(s.usage.get("cache_creation_input_tokens", 0) for s in all_steps)
    t_reason = sum(s.usage.get("reasoning_tokens", 0) for s in all_steps)
    last = last_real_usage(all_steps)
    e_in = last.get("input_tokens", 0)
    e_out = last.get("output_tokens", 0)
    e_cr = last.get("cache_read_input_tokens", 0)
    e_cw = last.get("cache_creation_input_tokens", 0)
    e_reason = last.get("reasoning_tokens", 0)
    return {
        "total_input": t_in, "total_output": t_out,
        "total_cache_read": t_cr, "total_cache_write": t_cw,
        "total_reasoning": t_reason,
        "snap_input": e_in, "snap_output": e_out,
        "snap_cache_read": e_cr, "snap_cache_write": e_cw,
        "snap_reasoning": e_reason,
    }


def _make_error_info(error_text: str | None) -> dict[str, str] | None:
    if not error_text:
        return None
    return {
        "exception_type": "ToolError",
        "message": error_text[:500],
        "traceback": error_text,
    }


def _opik_usage_ints(usage_meta: dict[str, Any] | None) -> dict[str, int] | None:
    if not usage_meta:
        return None
    details = usage_meta.get("input_token_details") or {}
    result = {
        "prompt_tokens": int(usage_meta.get("billed_input_tokens", usage_meta.get("input_tokens", 0)) or 0),
        "completion_tokens": int(usage_meta.get("output_tokens", 0) or 0),
        "total_tokens": int(usage_meta.get("total_tokens", 0) or 0),
        "cache_read": int(details.get("cache_read", 0) or 0),
        "cache_creation": int(details.get("cache_creation", 0) or 0),
    }
    return {k: v for k, v in result.items() if v}


def _compute_turn_end(turn: ParsedTurn) -> datetime:
    turn_end_ts = step_end_timestamp(turn.steps[-1]) if turn.steps else turn.timestamp
    turn_start = parse_ts(turn.timestamp)
    turn_end = parse_ts(turn_end_ts)
    if turn_end < turn_start:
        return turn_start
    return turn_end


def _step_llm_end_time(step: AgentStep) -> datetime:
    llm_start = parse_ts(step.timestamp)
    if step.tools:
        llm_end = min(parse_ts(tool.start_time) for tool in step.tools)
    else:
        llm_end = parse_ts(step.time_completed)
    if llm_end < llm_start:
        return llm_start
    return llm_end


def _group_reasoning_rounds(turn: ParsedTurn) -> list[ReasoningRound]:
    """Bound LLM calls into Rounds by visible-text emission (design §4)."""
    rounds: list[ReasoningRound] = []
    current: ReasoningRound | None = None
    for llm_idx, step in enumerate(turn.steps, start=1):
        starts_new_round = bool((step.text or "").strip())
        if current is None or starts_new_round:
            current = ReasoningRound(round_idx=len(rounds) + 1)
            rounds.append(current)
        current.step_items.append((llm_idx, step))
    return rounds


def _round_end_time(round_item: ReasoningRound) -> datetime:
    last_tool_end = ""
    for _, step in round_item.step_items:
        for tool in step.tools:
            tool_ts = tool.end_time or tool.start_time
            if tool_ts and tool_ts > last_tool_end:
                last_tool_end = tool_ts
    if last_tool_end:
        return parse_ts(last_tool_end)
    return _step_llm_end_time(round_item.step_items[-1][1])


def _round_metadata(round_item: ReasoningRound, turn: ParsedTurn, session_id: str) -> dict[str, Any]:
    steps = [step for _, step in round_item.step_items]
    api_input = sum(int(step.usage.get("input_tokens", 0) or 0) for step in steps)
    api_output = sum(int(step.usage.get("output_tokens", 0) or 0) for step in steps)
    api_cache_read = sum(int(step.usage.get("cache_read_input_tokens", 0) or 0) for step in steps)
    api_cache_creation = sum(int(step.usage.get("cache_creation_input_tokens", 0) or 0) for step in steps)
    api_reasoning = sum(int(step.usage.get("reasoning_tokens", 0) or 0) for step in steps)
    last_usage = last_real_usage(steps)
    snapshot_input = int(last_usage.get("input_tokens", 0) or 0)
    snapshot_cache_read = int(last_usage.get("cache_read_input_tokens", 0) or 0)
    snapshot_cache_creation = int(last_usage.get("cache_creation_input_tokens", 0) or 0)
    snapshot_output = int(last_usage.get("output_tokens", 0) or 0)
    snapshot_reasoning = int(last_usage.get("reasoning_tokens", 0) or 0)
    round_start = parse_ts(steps[0].timestamp)
    round_end = _round_end_time(round_item)
    duration_ms = max(0, int((round_end - round_start).total_seconds() * 1000))
    opener = next((step.text.strip() for step in steps if (step.text or "").strip()), "")
    return {
        "thread_id": session_id,
        "turn_idx": turn.turn_index,
        "round_idx": round_item.round_idx,
        "duration_ms": duration_ms,
        "llm_calls": len(steps),
        "tool_calls": sum(len(step.tools) for step in steps),
        "subagent_calls": sum(1 for step in steps for tool in step.tools if tool.is_subagent),
        "api_billed_input_tokens": api_input,
        "api_billed_output_tokens": api_output,
        "api_billed_cache_read_tokens": api_cache_read,
        "api_billed_cache_creation_tokens": api_cache_creation,
        "api_billed_reasoning_tokens": api_reasoning,
        "api_billed_total_tokens": api_input + api_output + api_cache_read + api_cache_creation + api_reasoning,
        "snapshot_input_tokens": snapshot_input,
        "snapshot_cache_read_tokens": snapshot_cache_read,
        "snapshot_cache_creation_tokens": snapshot_cache_creation,
        "snapshot_output_tokens": snapshot_output,
        "snapshot_reasoning_tokens": snapshot_reasoning,
        "snapshot_context_tokens": snapshot_input + snapshot_cache_read + snapshot_cache_creation,
        "snapshot_total_tokens": (
            snapshot_input + snapshot_cache_read + snapshot_cache_creation
            + snapshot_output + snapshot_reasoning
        ),
        "has_visible_text": bool(opener),
        "starts_with_tool_only": bool(steps and not (steps[0].text or "").strip()),
        "opener_text": opener[:240] if opener else "",
        "models": sorted({strip_model_date(step.model) for step in steps if step.model}),
        "step_indices": [step.step_index for step in steps],
        "agents": sorted({step.agent for step in steps if step.agent}),
    }


def _turn_metadata(turn: ParsedTurn, session_id: str, depth: int) -> dict[str, Any]:
    """Per-Turn metadata with api_billed / incremental / snapshot views (design §4)."""
    turn_start = parse_ts(turn.timestamp)
    turn_end = _compute_turn_end(turn)
    token_stats = _compute_token_summary([turn])
    duration_ms = max(0, int((turn_end - turn_start).total_seconds() * 1000))
    snapshot_context = (
        token_stats["snap_input"]
        + token_stats["snap_cache_read"]
        + token_stats["snap_cache_write"]
    )

    metadata: dict[str, Any] = {
        "thread_id": session_id,
        "session_id": session_id,
        "turn_idx": turn.turn_index,
        "turn_index": turn.turn_index,
        "is_system": turn.is_system,
        "depth": depth,
        "duration_ms": duration_ms,
        "models": sorted(turn.models),
        "agents": sorted(turn.agents),
        "step_count": len(turn.steps),
        "llm_calls": len(turn.steps),
        "tool_calls": sum(len(step.tools) for step in turn.steps),
        "subagent_calls": sum(1 for step in turn.steps for tool in step.tools if tool.is_subagent),
        "tool_success": sum(1 for step in turn.steps for tool in step.tools if not tool.error),
        "tool_error": sum(1 for step in turn.steps for tool in step.tools if tool.error),
        "token_accounting": "per-step",
        "api_billed_input_tokens": token_stats["total_input"],
        "api_billed_output_tokens": token_stats["total_output"],
        "api_billed_cache_read_tokens": token_stats["total_cache_read"],
        "api_billed_cache_creation_tokens": token_stats["total_cache_write"],
        "api_billed_reasoning_tokens": token_stats["total_reasoning"],
        "api_billed_total_tokens": (
            token_stats["total_input"] + token_stats["total_output"]
            + token_stats["total_cache_read"] + token_stats["total_cache_write"]
            + token_stats["total_reasoning"]
        ),
        "snapshot_input_tokens": token_stats["snap_input"],
        "snapshot_output_tokens": token_stats["snap_output"],
        "snapshot_cache_read_tokens": token_stats["snap_cache_read"],
        "snapshot_cache_creation_tokens": token_stats["snap_cache_write"],
        "snapshot_reasoning_tokens": token_stats["snap_reasoning"],
        "snapshot_context_tokens": snapshot_context,
        "snapshot_total_tokens": snapshot_context + token_stats["snap_output"] + token_stats["snap_reasoning"],
        # Design §4 short-form aliases (snap_*): "context occupancy at the moment
        # this turn finished," from the last LLM call with real usage.
        "snap_input": token_stats["snap_input"],
        "snap_cache_read": token_stats["snap_cache_read"],
        "snap_cache_write": token_stats["snap_cache_write"],
        "snap_output": token_stats["snap_output"],
        "snap_context": snapshot_context,
        "usage": turn.usage,
    }
    metadata["system_injected"] = [
        {
            "tag": e.get("tag", ""),
            "source": e.get("source", ""),
            "content": (e.get("content", "") or "")[:MAX_TEXT_CHARS],
            "ts": e.get("ts", ""),
        }
        for e in turn.system_injected
    ]
    metadata["system_injected_count"] = len(turn.system_injected)
    metadata["excluded_parts_count"] = len(turn.excluded_parts)
    return metadata


def _session_stats(turns: list[ParsedTurn]) -> dict[str, Any]:
    all_steps = [step for turn in turns for step in turn.steps]
    return {
        "all_steps": all_steps,
        "all_models": sorted({step.model for step in all_steps}),
        "all_agents": sorted({step.agent for step in all_steps if step.agent}),
        "total_tool_calls": sum(len(step.tools) for step in all_steps),
        "total_subagent_calls": sum(1 for step in all_steps for tool in step.tools if tool.is_subagent),
        "total_incomplete_steps": sum(1 for step in all_steps if step.incomplete),
        "tool_success": sum(1 for step in all_steps for tool in step.tools if not tool.error),
        "tool_error": sum(1 for step in all_steps for tool in step.tools if tool.error),
        "token_stats": _compute_token_summary(turns),
    }


def _session_metadata(session_meta: SessionMeta, turns: list[ParsedTurn]) -> dict[str, Any]:
    stats = _session_stats(turns)
    all_steps = stats["all_steps"]
    token_stats = stats["token_stats"]
    session_duration_ms = session_meta.time_updated - session_meta.time_created
    snapshot_context = (
        token_stats["snap_input"]
        + token_stats["snap_cache_read"]
        + token_stats["snap_cache_write"]
    )
    return {
        "session_id": session_meta.session_id,
        "session_title": session_meta.title,
        "session_directory": session_meta.directory,
        "session_duration_ms": session_duration_ms,
        "session_version": session_meta.version,
        "source": "opencode",
        "models": stats["all_models"],
        "agents": stats["all_agents"],
        "total_turns": len(turns),
        "total_llm_calls": len(all_steps),
        "total_tool_calls": stats["total_tool_calls"],
        "total_subagent_calls": stats["total_subagent_calls"],
        "incomplete_steps": stats["total_incomplete_steps"],
        "tool_success": stats["tool_success"],
        "tool_error": stats["tool_error"],
        "token_accounting": "per-step",
        "api_billed_input_tokens": token_stats["total_input"],
        "api_billed_output_tokens": token_stats["total_output"],
        "api_billed_cache_read_tokens": token_stats["total_cache_read"],
        "api_billed_cache_creation_tokens": token_stats["total_cache_write"],
        "api_billed_reasoning_tokens": token_stats["total_reasoning"],
        "api_billed_total_tokens": (
            token_stats["total_input"] + token_stats["total_output"]
            + token_stats["total_cache_read"] + token_stats["total_cache_write"]
            + token_stats["total_reasoning"]
        ),
        "snapshot_input_tokens": token_stats["snap_input"],
        "snapshot_output_tokens": token_stats["snap_output"],
        "snapshot_cache_read_tokens": token_stats["snap_cache_read"],
        "snapshot_cache_creation_tokens": token_stats["snap_cache_write"],
        "snapshot_reasoning_tokens": token_stats["snap_reasoning"],
        "snapshot_context_tokens": snapshot_context,
        "snapshot_total_tokens": snapshot_context + token_stats["snap_output"] + token_stats["snap_reasoning"],
    }


def _session_output(
    session_meta: SessionMeta,
    turns: list[ParsedTurn],
    status: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if status:
        # Keep status first so Opik thread/trace previews match Claude traces.
        data["status"] = status
    data.update(_session_metadata(session_meta=session_meta, turns=turns))
    data.pop("session_id", None)
    data.pop("source", None)
    data.pop("models", None)
    return data


def trace_input(turns: list[ParsedTurn]) -> str:
    """Match Claude trace semantics: trace input is the first user prompt.

    `turn.user_input` is already stripped of system-injected blocks
    (`<system-reminder>` etc.) by `extract_injected_blocks`, so the returned
    string is the human-typed prompt only — no extra filtering needed here.
    """
    for turn in turns:
        if turn.user_input:
            return turn.user_input
    return ""


def _tool_size_estimate(tool_name: str, tool_input: Any, tool_output: str) -> dict[str, int]:
    input_tokens_est = estimate_tokens(
        tool_name + json.dumps(tool_input, ensure_ascii=False) if tool_input else tool_name
    )
    output_tokens_est = estimate_tokens(tool_output)
    return {
        "input_tokens_est": input_tokens_est,
        "output_tokens_est": output_tokens_est,
        "total_tokens_est": input_tokens_est + output_tokens_est,
    }


# ─────────────────────────────────────────────────────────────────
# End of inlined DB-parse logic.
# ─────────────────────────────────────────────────────────────────


class FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> "FileLock":
        import fcntl

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR)
        deadline = time.time() + 2.0
        while True:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.time() >= deadline:
                    os.close(self.fd)
                    self.fd = None
                    raise TimeoutError(f"timed out acquiring lock: {self.path}")
                time.sleep(0.05)

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        import fcntl

        if self.fd is not None:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


@dataclass
class SessionState:
    session_id: str = ""
    trace_id: str | None = None
    # Deterministic bridging span between trace_id and per-turn spans. Without
    # it, turn spans pass an invalid parent_span_id and the whole subtree shows
    # up as orphans in the Opik UI.
    root_span_id: str | None = None
    # "opencode" (this hook owns the trace) | "external" (Harbor caller owns
    # it; we just attach our subtree under their span). Frozen on first mint.
    trace_owner: str = "opencode"
    external_parent_span_id: str | None = None
    trace_created: bool = False
    trace_name: str | None = None
    trace_start_ts: str | None = None
    # session_start_ts / session_end_ts model the *session* lifecycle (driven by
    # session_start / session_idle / finalize events). trace_start_ts/last_turn_ts
    # track *turn* activity. The two pairs can diverge when a session is created
    # but no turns have been parsed yet, so we persist both.
    session_start_ts: str | None = None
    session_end_ts: str | None = None
    last_turn_ts: str | None = None
    # Once trace_finalized is True, the dispatcher early-returns on subsequent
    # non-start events so late stragglers cannot reopen / overwrite the closed
    # trace. final_status is the status surfaced on the trace output + tags
    # (currently "completed"; reserved for future failure-status reporting).
    trace_finalized: bool = False
    final_status: str | None = None
    emitted_turn_count: int = 0
    emitted_turn_hashes: list[str] = field(default_factory=list)
    turn_number: int = 0
    last_flush_time: float = 0.0
    # Watermark of fully-complete turns; used by the tool_complete dispatch to
    # bypass FLUSH_INTERVAL_S the instant a turn finishes.
    completed_turn_count: int = 0
    session_api_billed_input: int = 0
    session_api_billed_output: int = 0
    session_api_billed_cache_read: int = 0
    session_api_billed_cache_creation: int = 0
    session_total_llm_calls: int = 0
    session_total_tool_calls: int = 0
    session_total_subagent_calls: int = 0
    session_tool_success: int = 0
    session_tool_error: int = 0
    session_models: list[str] = field(default_factory=list)
    child_sessions: dict[str, "ChildSessionState"] = field(default_factory=dict)
    completed: bool = False


@dataclass
class ChildSessionState:
    child_session_id: str
    parent_tool_call_id: str
    agent_span_id: str
    emitted_turn_count: int = 0
    emitted_turn_hashes: list[str] = field(default_factory=list)
    child_total_llm_calls: int = 0
    child_total_tool_calls: int = 0
    child_tool_success: int = 0
    child_tool_error: int = 0


def load_state() -> dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        debug(f"load_state failed: {exc}")
    return {}


def save_state(state: dict[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except Exception as exc:
        debug(f"save_state failed: {exc}")


def load_session_state(state: dict[str, Any], session_id: str) -> SessionState:
    raw = state.get("sessions", {}).get(session_id, {})
    child_sessions_raw = raw.get("child_sessions") if isinstance(raw.get("child_sessions"), dict) else {}
    child_sessions: dict[str, ChildSessionState] = {}
    for child_session_id, item in child_sessions_raw.items():
        if not isinstance(item, dict):
            continue
        child_sessions[child_session_id] = ChildSessionState(
            child_session_id=str(item.get("child_session_id") or child_session_id),
            parent_tool_call_id=str(item.get("parent_tool_call_id") or ""),
            agent_span_id=str(item.get("agent_span_id") or ""),
            emitted_turn_count=int(item.get("emitted_turn_count", 0)),
            emitted_turn_hashes=list(item.get("emitted_turn_hashes") or []),
            child_total_llm_calls=int(item.get("child_total_llm_calls", 0)),
            child_total_tool_calls=int(item.get("child_total_tool_calls", 0)),
            child_tool_success=int(item.get("child_tool_success", 0)),
            child_tool_error=int(item.get("child_tool_error", 0)),
        )
    return SessionState(
        session_id=session_id,
        trace_id=raw.get("trace_id"),
        root_span_id=raw.get("root_span_id"),
        trace_owner=str(raw.get("trace_owner") or "opencode"),
        external_parent_span_id=raw.get("external_parent_span_id"),
        trace_created=bool(raw.get("trace_created", False)),
        trace_name=raw.get("trace_name"),
        trace_start_ts=raw.get("trace_start_ts"),
        session_start_ts=raw.get("session_start_ts"),
        session_end_ts=raw.get("session_end_ts"),
        last_turn_ts=raw.get("last_turn_ts"),
        trace_finalized=bool(raw.get("trace_finalized", False)),
        final_status=raw.get("final_status"),
        emitted_turn_count=int(raw.get("emitted_turn_count", raw.get("emitted_turns", 0))),
        emitted_turn_hashes=list(raw.get("emitted_turn_hashes") or []),
        turn_number=int(raw.get("turn_number", 0)),
        last_flush_time=float(raw.get("last_flush_time", 0.0)),
        completed_turn_count=int(raw.get("completed_turn_count", 0)),
        session_api_billed_input=int(raw.get("session_api_billed_input", 0)),
        session_api_billed_output=int(raw.get("session_api_billed_output", 0)),
        session_api_billed_cache_read=int(raw.get("session_api_billed_cache_read", 0)),
        session_api_billed_cache_creation=int(raw.get("session_api_billed_cache_creation", 0)),
        session_total_llm_calls=int(raw.get("session_total_llm_calls", 0)),
        session_total_tool_calls=int(raw.get("session_total_tool_calls", 0)),
        session_total_subagent_calls=int(raw.get("session_total_subagent_calls", 0)),
        session_tool_success=int(raw.get("session_tool_success", 0)),
        session_tool_error=int(raw.get("session_tool_error", 0)),
        session_models=list(raw.get("session_models") or []),
        child_sessions=child_sessions,
        completed=bool(raw.get("completed", False)),
    )


def save_session_state(state: dict[str, Any], session_id: str, session: SessionState) -> None:
    state.setdefault("sessions", {})[session_id] = {
        "session_id": session.session_id,
        "trace_id": session.trace_id,
        "root_span_id": session.root_span_id,
        "trace_owner": session.trace_owner,
        "external_parent_span_id": session.external_parent_span_id,
        "trace_created": session.trace_created,
        "trace_name": session.trace_name,
        "trace_start_ts": session.trace_start_ts,
        "session_start_ts": session.session_start_ts,
        "session_end_ts": session.session_end_ts,
        "last_turn_ts": session.last_turn_ts,
        "trace_finalized": session.trace_finalized,
        "final_status": session.final_status,
        "emitted_turn_count": session.emitted_turn_count,
        "emitted_turn_hashes": session.emitted_turn_hashes,
        "turn_number": session.turn_number,
        "last_flush_time": session.last_flush_time,
        "completed_turn_count": session.completed_turn_count,
        "session_api_billed_input": session.session_api_billed_input,
        "session_api_billed_output": session.session_api_billed_output,
        "session_api_billed_cache_read": session.session_api_billed_cache_read,
        "session_api_billed_cache_creation": session.session_api_billed_cache_creation,
        "session_total_llm_calls": session.session_total_llm_calls,
        "session_total_tool_calls": session.session_total_tool_calls,
        "session_total_subagent_calls": session.session_total_subagent_calls,
        "session_tool_success": session.session_tool_success,
        "session_tool_error": session.session_tool_error,
        "session_models": session.session_models,
        "child_sessions": {
            child_session_id: {
                "child_session_id": child.child_session_id,
                "parent_tool_call_id": child.parent_tool_call_id,
                "agent_span_id": child.agent_span_id,
                "emitted_turn_count": child.emitted_turn_count,
                "emitted_turn_hashes": child.emitted_turn_hashes,
                "child_total_llm_calls": child.child_total_llm_calls,
                "child_total_tool_calls": child.child_total_tool_calls,
                "child_tool_success": child.child_tool_success,
                "child_tool_error": child.child_tool_error,
            }
            for child_session_id, child in session.child_sessions.items()
        },
        "completed": session.completed,
        "updated": datetime.now(timezone.utc).isoformat(),
    }


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def apply_opik_env_overrides() -> None:
    opik_url = os.environ.get("OPIK_URL")
    if opik_url:
        # OPIK_URL is the single user-facing source; mirror it for compatibility.
        os.environ["OPIK_URL_OVERRIDE"] = opik_url

    api_key = _env_first("OPIK_API_KEY_OVERRIDE", "OPIK_API_KEY")
    if api_key and not os.environ.get("OPIK_API_KEY"):
        os.environ["OPIK_API_KEY"] = api_key

    workspace = _env_first("OPIK_WORKSPACE_OVERRIDE", "OPIK_WORKSPACE")
    if workspace and not os.environ.get("OPIK_WORKSPACE"):
        os.environ["OPIK_WORKSPACE"] = workspace


def runtime_context_metadata() -> dict[str, Any]:
    """Surface terminal-bench / harbor provenance into trace metadata.

    Mirror of `realtime-trace-cc/claude_code_realtime_hook.py:runtime_context_metadata`.
    """
    meta: dict[str, Any] = {}
    mapping = {
        "tb_task_id": _env_first("TB_TASK_ID"),
        "tb_run_id": _env_first("TB_RUN_ID"),
        "tb_dataset": _env_first("TB_DATASET"),
        "tb_trial_id": _env_first("TB_TRIAL_ID"),
        "opik_trial_name": _env_first("OPIK_TRIAL_NAME"),
        "opik_project_name": _env_first("OPIK_PROJECT_NAME", "OC_OPIK_PROJECT"),
        "opik_url": _env_first("OPIK_URL_OVERRIDE", "OPIK_URL"),
    }
    for key, value in mapping.items():
        if value:
            meta[key] = value
    return meta


def read_hook_payload() -> dict[str, Any]:
    payload: dict[str, Any] = {}
    try:
        raw = sys.stdin.read()
        if raw.strip():
            payload = json.loads(raw)
    except Exception:
        payload = {}
    if len(sys.argv) > 1 and sys.argv[1]:
        payload.setdefault("event", sys.argv[1])
    return payload


def _deep_get(data: dict[str, Any], *path: str) -> Any:
    cur: Any = data
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def hook_event_name(payload: dict[str, Any]) -> str:
    raw = (
        payload.get("event")
        or payload.get("hook_event_name")
        or payload.get("hookEventName")
        or payload.get("type")
        or ""
    )
    return str(raw).strip().lower()


def event_timestamp(payload: dict[str, Any]) -> str | None:
    """Best-effort extraction of a hook event timestamp.

    Different opencode plugin shapes put the timestamp under different keys;
    final / session_idle events sometimes omit it entirely (handled by the
    dispatcher, which falls back to `now()`).
    """
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


def extract_session_id(payload: dict[str, Any]) -> str | None:
    value = (
        payload.get("session_id")
        or payload.get("sessionId")
        or _deep_get(payload, "session", "id")
        or _env_first("OPENCODE_SESSION_ID", "OC_SESSION_ID")
    )
    return str(value) if value else None


def resolve_db_path(payload: dict[str, Any]) -> Path | None:
    raw = (
        payload.get("db")
        or payload.get("db_path")
        or payload.get("dbPath")
        or payload.get("database")
        or payload.get("databasePath")
        or _deep_get(payload, "session", "db_path")
        or _env_first("OPENCODE_DB_PATH", "OC_DB_PATH", "OPENCODE_DB")
    )
    candidates: list[Path] = []
    if raw:
        candidates.append(Path(str(raw)).expanduser())
    home = Path.home()
    candidates.extend([
        home / ".opencode" / "opencode.db",
        home / ".config" / "opencode" / "opencode.db",
        home / ".local" / "share" / "opencode" / "opencode.db",
    ])
    for path in candidates:
        try:
            if path.exists():
                return path.resolve()
        except Exception:
            continue
    return None


def resolve_json_store(payload: dict[str, Any], session_id: str | None) -> JsonStore | None:
    raw = (
        payload.get("storage")
        or payload.get("storage_path")
        or payload.get("storagePath")
        or _env_first("OPENCODE_STORAGE_PATH", "OC_STORAGE_PATH")
    )
    candidates: list[Path] = []
    if raw:
        candidates.append(Path(str(raw)).expanduser())
    home = Path.home()
    candidates.extend([
        home / ".local" / "share" / "opencode" / "project" / "global" / "storage",
        home / ".local" / "share" / "opencode" / "storage",
    ])
    candidates.extend((home / ".local" / "share" / "opencode" / "project").glob("*/storage"))

    for path in candidates:
        try:
            if not (path / "session").exists():
                continue
            if session_id and not (path / "session" / "info" / f"{session_id}.json").exists():
                continue
            return JsonStore(path.resolve())
        except Exception:
            continue
    return None


def event_is_final(event_name: str) -> bool:
    return event_name in {
        "agent_end",
        "session.completed",
        "session_completed",
        "session_end",
        "session.stop",
        "stop",
        "end",
        "completed",
    }


def event_is_flush(event_name: str) -> bool:
    if event_is_final(event_name):
        return True
    return event_name in {
        "after_tool_call",
        "tool_complete",
        "tool_error",
        "tool.after",
        "tool_result",
        "session_compacting",
        "session_idle",
        "llm_output",
        "assistant_message",
        "message.assistant",
    }


def event_is_start(event_name: str) -> bool:
    return event_name in {
        "session_start",
        "session.start",
        "llm_input",
        "prompt",
        "user_prompt",
        "message.user",
        "user_message",
    }


def truncate_text(value: str) -> str:
    return value[:MAX_TEXT_CHARS] if len(value) > MAX_TEXT_CHARS else value


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


def deterministic_span_id(trace_id: str, *parts: str) -> str:
    raw = "::".join([trace_id, *parts])
    digest = hashlib.sha256(raw.encode("utf-8")).digest()
    data = bytearray(digest[:16])
    data[6] = (data[6] & 0x0F) | 0x70
    data[8] = (data[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(data)))


def session_trace_id(session: SessionState) -> str:
    if session.trace_id:
        return session.trace_id
    # DEPRECATED external-parent path. Harbor stopped injecting
    # OPIK_PARENT_TRACE_ID / OPIK_PARENT_SPAN_ID in agent-fleet PR #84
    # ("Add readable functions for opencode agent") — see
    # OpikOpenCodeHarbor.run() for the rationale: each opencode session now
    # owns one independent trace, matching the Claude hook shape. This
    # branch is preserved so that if Harbor ever needs to restore parent-
    # span nesting, the wiring doesn't have to be re-implemented. NEW CODE
    # SHOULD NOT depend on it. Ownership is still frozen on first mint —
    # subsequent restarts read trace_owner from the persisted state and
    # stay in the chosen mode.
    parent_trace = os.environ.get("OPIK_PARENT_TRACE_ID")
    parent_span = os.environ.get("OPIK_PARENT_SPAN_ID")
    if parent_trace and parent_span:
        session.trace_id = parent_trace
        session.trace_owner = "external"
        session.external_parent_span_id = parent_span
    else:
        session.trace_id = new_opik_id()
        session.trace_owner = "opencode"
    return session.trace_id


def session_root_span_id(session: SessionState) -> str:
    # Deterministic so a restart mid-session updates the existing root span
    # instead of creating a duplicate. Single bridging span between trace_id
    # and the per-turn spans — without it, turn spans pass an invalid
    # parent_span_id and the whole subtree shows up as orphans.
    if session.root_span_id:
        return session.root_span_id
    session.root_span_id = deterministic_span_id(
        session_trace_id(session), "opencode-session-root"
    )
    return session.root_span_id


def create_trace_if_possible(client: Any, **kwargs: Any) -> bool:
    if DRY_RUN:
        info(f"dry-run create_trace name={kwargs.get('name')} id={kwargs.get('id')}")
        return True
    try:
        client.rest_client.traces.create_trace(**kwargs)
        return True
    except Exception as exc:
        if "Trace already exists" in str(exc) or "status_code: 409" in str(exc):
            debug(f"create_trace already exists id={kwargs.get('id')}")
            return False
        raise


def update_trace_if_possible(client: Any, trace_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run update_trace name={kwargs.get('name')} id={trace_id}")
        return
    traces_api = getattr(getattr(client, "rest_client", None), "traces", None)
    update_trace = getattr(traces_api, "update_trace", None)
    if callable(update_trace):
        update_kwargs = dict(kwargs)
        update_kwargs.pop("start_time", None)
        try:
            update_trace(trace_id, **update_kwargs)
            return
        except Exception as exc:
            debug(f"update_trace failed id={trace_id}: {exc}")
    create_trace_if_possible(client, id=trace_id, **kwargs)


def create_span_if_possible(client: Any, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run create_span name={kwargs.get('name')} id={kwargs.get('id')}")
        return
    span_id = str(kwargs.get("id") or "")
    if span_id and queue_span_snapshot(span_id, _span_payload_for_write(kwargs, update=False), SPAN_BATCH_ENV_NAMES):
        return
    client.rest_client.spans.create_span(**kwargs)


_UPDATE_SPAN_UNSUPPORTED = frozenset({"start_time", "last_updated_at", "total_estimated_cost_version"})


def _span_payload_for_write(kwargs: dict[str, Any], *, update: bool) -> dict[str, Any]:
    payload = {
        k: v
        for k, v in kwargs.items()
        if not update or k not in _UPDATE_SPAN_UNSUPPORTED
    }
    # Standalone root spans live directly under the trace. Sending
    # parent_span_id=null on PATCH can hit a backend/nginx 405 path, while
    # omitting the field preserves the existing root relationship.
    if payload.get("parent_span_id") is None:
        payload.pop("parent_span_id", None)
    return payload


def update_span_if_possible(client: Any, span_id: str, **kwargs: Any) -> None:
    if DRY_RUN:
        info(f"dry-run update_span id={span_id}")
        return
    filtered = _span_payload_for_write(kwargs, update=True)
    if update_queued_span(span_id, filtered, SPAN_BATCH_ENV_NAMES):
        return
    client.rest_client.spans.update_span(span_id, **filtered)


def create_or_update_span(client: Any, span_id: str, **kwargs: Any) -> None:
    parent = kwargs.get("parent_span_id")
    trace = kwargs.get("trace_id")
    if parent is not None and parent == trace:
        # Opik treats trace_id purely as the "which trace" pointer; parent_span_id
        # must reference a real span. Passing trace_id as parent makes the span
        # an orphan and the whole subtree disappears from the trace tree.
        raise ValueError(
            f"create_or_update_span: parent_span_id == trace_id ({trace}); "
            "parent must be a real span id or None"
        )
    if DRY_RUN:
        info(f"dry-run upsert_span name={kwargs.get('name')} id={span_id}")
        return
    create_payload = _span_payload_for_write(kwargs, update=False)
    if queue_span_snapshot(span_id, create_payload, SPAN_BATCH_ENV_NAMES):
        return
    try:
        client.rest_client.spans.create_span(
            id=span_id,
            **create_payload,
        )
    except Exception as create_exc:
        filtered = _span_payload_for_write(kwargs, update=True)
        try:
            client.rest_client.spans.update_span(span_id, **filtered)
        except Exception as update_exc:
            debug(
                f"upsert_span failed id={span_id}: "
                f"create={create_exc}; update={update_exc}"
            )
            raise


def _trace_timestamp() -> str:
    return datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")


def _clean_trace_token(value: str) -> str:
    token = re.sub(r"\s+", "_", value.strip())
    token = token.replace("/", "_")
    token = re.sub(r"[^A-Za-z0-9_.:-]+", "_", token)
    return token.strip("_") or "unknown"


def benchmark_task_key() -> str | None:
    """Pick a stable task identifier for the trace name when running under TB.

    Falls back through TB_TASK_ID > OPENCLAW_SESSION_KEY > OPIK_TRIAL_NAME;
    if none are set, a single-task TB_INCLUDE_TASKS / INCLUDE_TASKS list also
    qualifies as a task key (this is how Harbor injects single-task runs).
    """
    key = (
        _env_first("TB_TASK_ID")
        or _env_first("OPENCLAW_SESSION_KEY", "OPENCLAW_SESSIONKEY")
        or _env_first("OPIK_TRIAL_NAME")
    )
    if key:
        return key

    include_tasks = _env_first("TB_INCLUDE_TASKS", "INCLUDE_TASKS")
    if include_tasks:
        parts = [part.strip() for part in include_tasks.split(",") if part.strip()]
        if len(parts) == 1:
            return parts[0]
    return None


def is_benchmark_context() -> bool:
    return bool(
        _env_first(
            "TB_TASK_ID",
            "OPENCLAW_SESSION_KEY",
            "OPENCLAW_SESSIONKEY",
            "TB_RUN_ID",
            "TB_DATASET",
            "TB_TRIAL_ID",
            "OPIK_TRIAL_NAME",
            "TB_INCLUDE_TASKS",
            "INCLUDE_TASKS",
        )
    )


def _legacy_trace_name(trace_name: str | None, session_meta: Any) -> bool:
    if not trace_name:
        return False
    title = (getattr(session_meta, "title", "") or "").strip()
    session_id = getattr(session_meta, "session_id", "") or ""
    return trace_name in {
        f"opencode · {title[:80]}" if title else "",
        f"opencode · {session_id[:8]}" if session_id else "",
    }


def trace_name_for_session(session: SessionState, session_meta: Any) -> str:
    """Resolve trace.name.

    Resolution order:
      1. cached on session.trace_name (unless it's a legacy
         "opencode · <slug>" placeholder, which we replace)
      2. benchmark mode -> "task({task_key})_{YYYYMMDD_HHMMSS}" where
         task_key := TB_TASK_ID > OPENCLAW_SESSION_KEY > OPIK_TRIAL_NAME
         > single-element TB_INCLUDE_TASKS; if no key but TB env vars are
         set, falls back to session_id[:8]
      3. title-based default -> "opencode · {title[:80]}" or "opencode · {session_id[:8]}"
    """
    if session.trace_name and not _legacy_trace_name(session.trace_name, session_meta):
        return session.trace_name

    task_key = benchmark_task_key()
    if not task_key and is_benchmark_context():
        task_key = session_meta.session_id[:8]
    if task_key:
        session.trace_name = f"task({_clean_trace_token(task_key)})_{_trace_timestamp()}"
        return session.trace_name

    if session.trace_name:
        return session.trace_name
    title = (session_meta.title or "").strip()
    if title:
        session.trace_name = f"opencode · {title[:80]}"
    else:
        session.trace_name = f"opencode · {session_meta.session_id[:8]}"
    return session.trace_name


def session_tags(turns: list[Any]) -> list[str]:
    stats = _session_stats(turns)
    tags = ["opencode", "session", "realtime", *[f"model:{m}" for m in stats["all_models"]]]
    task_id = _env_first("TB_TASK_ID")
    run_id = _env_first("TB_RUN_ID")
    trial_id = _env_first("TB_TRIAL_ID")
    trial_name = _env_first("OPIK_TRIAL_NAME")
    if task_id:
        tags.append(f"tb-task:{task_id}")
    if run_id:
        tags.append(f"tb-run:{run_id}")
    if trial_id:
        tags.append(f"tb-trial:{trial_id}")
    if trial_name:
        tags.append(f"harbor-trial:{trial_name}")
    return tags


def _final_status(session: SessionState, completed: bool) -> str | None:
    """Status string surfaced on trace output/tags when the session finalizes.

    Currently always "completed"; reserved for future failure-status tagging.
    """
    if session.final_status:
        return session.final_status
    return "completed" if completed else None


def _session_tags_with_status(turns: list[Any], status: str | None) -> list[str]:
    tags = session_tags(turns)
    if status:
        tags.append(status)
    return tags


def _refresh_session_accumulators(session: SessionState, turns: list[Any]) -> None:
    stats = _session_stats(turns)
    token_stats = stats["token_stats"]
    session.session_api_billed_input = token_stats["total_input"]
    session.session_api_billed_output = token_stats["total_output"]
    session.session_api_billed_cache_read = token_stats["total_cache_read"]
    session.session_api_billed_cache_creation = token_stats["total_cache_write"]
    session.session_total_llm_calls = sum(len(turn.steps) for turn in turns)
    session.session_total_tool_calls = stats["total_tool_calls"]
    session.session_total_subagent_calls = stats["total_subagent_calls"]
    session.session_tool_success = stats["tool_success"]
    session.session_tool_error = stats["tool_error"]
    session.session_models = sorted(stats["all_models"])


def _refresh_child_accumulators(child_state: ChildSessionState, turns: list[Any]) -> None:
    stats = _session_stats(turns)
    child_state.child_total_llm_calls = sum(len(turn.steps) for turn in turns)
    child_state.child_total_tool_calls = stats["total_tool_calls"]
    child_state.child_tool_success = stats["tool_success"]
    child_state.child_tool_error = stats["tool_error"]


def hash_turn(turn: Any) -> str:
    payload = {
        "hash_schema": 2,
        "turn_index": turn.turn_index,
        "user_input": turn.user_input,
        "timestamp": turn.timestamp,
        "is_system": turn.is_system,
        "final_output": turn.final_output,
        "usage": turn.usage,
        "steps": [
            {
                "step_index": step.step_index,
                "message_id": step.message_id,
                "model": step.model,
                "provider_id": step.provider_id,
                "agent": step.agent,
                "mode": step.mode,
                "variant": step.variant,
                "timestamp": step.timestamp,
                "time_completed": step.time_completed,
                "path_cwd": step.path_cwd,
                "path_root": step.path_root,
                "cost": step.cost,
                "finish_reason": step.finish_reason,
                "usage": step.usage,
                "incomplete": step.incomplete,
                "reasoning": step.reasoning,
                "text": step.text,
                "tools": [
                    {
                        "call_id": tool.call_id,
                        "name": tool.name,
                        "start_time": tool.start_time,
                        "end_time": tool.end_time,
                        "input": tool.input,
                        "output": tool.output,
                        "status": tool.status,
                        "title": tool.title,
                        "tool_metadata": tool.tool_metadata,
                        "error": tool.error,
                        "is_subagent": tool.is_subagent,
                        "child_session_id": tool.child_session_id,
                        "child_agent": tool.child_agent,
                        "child_model": tool.child_model,
                        "child_provider": tool.child_provider,
                        "child_description": tool.child_description,
                        "child_prompt": tool.child_prompt,
                    }
                    for tool in step.tools
                ],
            }
            for step in turn.steps
        ],
        "system_injected": [
            {
                "tag": entry.get("tag", ""),
                "source": entry.get("source", ""),
                "content": entry.get("content", ""),
                "ts": entry.get("ts", ""),
            }
            for entry in turn.system_injected
        ],
        "excluded_parts": [
            {
                "tag": entry.get("tag", ""),
                "source": entry.get("source", ""),
                "content": entry.get("content", ""),
                "ts": entry.get("ts", ""),
            }
            for entry in turn.excluded_parts
        ],
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def upsert_trace(
    client: Any,
    project_name: str,
    session: SessionState,
    session_meta: Any,
    turns: list[Any],
    completed: bool,
    force_create: bool = False,
) -> None:
    if not turns:
        return
    trace_id = session_trace_id(session)
    trace_name = trace_name_for_session(session, session_meta)
    status = _final_status(session, completed)
    start_time = parse_ts(session.session_start_ts or session.trace_start_ts or turns[0].timestamp)
    end_time = parse_ts(
        session.session_end_ts if completed else (session.last_turn_ts or turns[-1].timestamp)
    )
    # Clamp: clock-skew or out-of-order events can push end_time before
    # start_time; Opik rejects such ranges, so collapse them to a point in time.
    if end_time < start_time:
        end_time = start_time
    _refresh_session_accumulators(session, turns)
    trace_input_text = trace_input(turns)
    metadata = {
        **_session_metadata(session_meta, turns),
        **runtime_context_metadata(),
        # Align Opik Threads with the Claude hook: each opencode session
        # owns one trace, thread_id == trace_id (UUID), and the readable
        # trace name doubles as the thread name in the UI.
        "thread_id": trace_id,
        "thread_name": trace_name,
        "first_message": trace_input_text,
        "realtime": True,
        "realtime_version": "v1",
        "completed": completed,
        "final_status": status,
        "session_start_ts": session.session_start_ts,
        "session_end_ts": session.session_end_ts,
    }
    payload = {
        "project_name": project_name,
        "name": trace_name,
        "start_time": start_time,
        "end_time": end_time,
        "input": trace_input_text,
        "output": _session_output(session_meta, turns, status=status),
        "metadata": metadata,
        "tags": _session_tags_with_status(turns, status),
        "thread_id": trace_id,
    }
    # `force_create=True` is used on the FIRST flush per session to seed the
    # trace via create_trace_if_possible directly. Going through update_trace
    # on a non-existent trace_id silently no-ops on some Opik backends (and
    # hangs on others), so the trace would never appear in the traces tab.
    # Subsequent flushes use the update path which works once the trace exists.
    if force_create:
        create_trace_if_possible(client, id=trace_id, **payload)
        session.trace_created = True
        return
    update_trace_if_possible(client, trace_id, **payload)


def emit_session_root_span(
    client: Any,
    project_name: str,
    session: SessionState,
    session_meta: Any,
    turns: list[Any],
    session_id: str,
    completed: bool,
) -> str:
    # Bridges trace_id and the per-turn spans. Idempotent via deterministic
    # span id; safe to call on every flush. Without this span, turn spans
    # would have no real parent and the whole subtree shows up as orphans
    # in the Opik UI.
    trace_id = session_trace_id(session)
    root_span_id = session_root_span_id(session)
    start_time = parse_ts(session.trace_start_ts) if session.trace_start_ts else None
    end_ts = session.session_end_ts if completed else (session.last_turn_ts or session.trace_start_ts)
    end_time = parse_ts(end_ts) if end_ts else None
    if start_time and end_time and end_time < start_time:
        end_time = start_time
    status = _final_status(session, completed)
    # External mode: parent is the Harbor agent-run span. Standalone: None
    # so the root sits directly under the trace.
    parent_span_id = (
        session.external_parent_span_id
        if session.trace_owner == "external"
        else None
    )
    create_or_update_span(
        client,
        root_span_id,
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        project_name=project_name,
        name="opencode session",
        type="general",
        start_time=start_time,
        end_time=end_time,
        input=trace_input(turns),
        output=_session_output(session_meta, turns, status=status) if completed else {},
        metadata={
            **_session_metadata(session_meta, turns),
            "trace_owner": session.trace_owner,
            "external_parent_span_id": session.external_parent_span_id,
            "realtime": True,
            "completed": completed,
            "final_status": status,
            "session_start_ts": session.session_start_ts,
            "session_end_ts": session.session_end_ts,
        },
        tags=[*_session_tags_with_status(turns, status), "session-root"],
    )
    return root_span_id


def turn_is_incomplete(turn: Any) -> bool:
    # DESIGN §3.3: complete iff every step has step-finish AND every tool has
    # a non-unknown status. Read the canonical flag directly, not the derived
    # `step.incomplete` (which mirrors `not has_step_finish`).
    if not turn.steps:
        return True
    for step in turn.steps:
        if not getattr(step, "has_step_finish", False):
            return True
        for tool in step.tools:
            if (tool.status or "unknown") == "unknown":
                return True
    return False


def turn_user_content(turn: Any) -> list[dict[str, Any]]:
    content: list[dict[str, Any]] = []
    if turn.user_input:
        content.append({"type": "text", "text": truncate_text(turn.user_input)})

    for entry in turn.system_injected:
        tag = entry.get("tag") or "system"
        body = truncate_text(entry.get("content", "") or "")
        content.append({
            "type": "text",
            "text": f"<{tag}>\n{body}\n</{tag}>",
            "subtype": "system_injected",
            "tag": tag,
        })

    if not content:
        content.append({"type": "text", "text": ""})
    return content


def emit_turn(
    client: Any,
    project_name: str,
    trace_id: str,
    parent_span_id: str,
    conn: Any,
    session_id: str,
    turn: Any,
    depth: int = 0,
    turn_prefix: str = "turn",
    span_scope: str = "root",
    is_child: bool = False,
) -> datetime:
    if depth > 5:
        # Return turn END so callers (flush_turns sets last_turn_ts from this)
        # don't drive trace end_time backwards on deep recursion bail-out.
        return _compute_turn_end(turn)

    turn_start = parse_ts(turn.timestamp)
    turn_end = _compute_turn_end(turn)
    turn_span_id = deterministic_span_id(trace_id, span_scope, turn_prefix, str(turn.turn_index))
    turn_name = f"{turn_prefix}-{turn.turn_index}"
    user_content = turn_user_content(turn)
    all_outputs: list[dict[str, Any]] = [{"role": "user", "content": user_content}]

    create_or_update_span(
        client,
        turn_span_id,
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        project_name=project_name,
        name=turn_name,
        type="general",
        start_time=turn_start,
        end_time=turn_end,
        input={"messages": [{"role": "user", "content": user_content}]},
        output={},
        metadata=_turn_metadata(turn, session_id, depth),
        tags=(["sub-agent", "turn"] if is_child else ["opencode", turn_name]),
    )

    conversation: list[dict[str, Any]] = [{"role": "user", "content": user_content}]

    for round_item in _group_reasoning_rounds(turn):
        first_step = round_item.step_items[0][1]
        round_start = parse_ts(first_step.timestamp)
        round_end = _round_end_time(round_item)
        round_span_id = deterministic_span_id(
            trace_id,
            span_scope,
            turn_prefix,
            str(turn.turn_index),
            "round",
            str(round_item.round_idx),
        )
        create_or_update_span(
            client,
            round_span_id,
            trace_id=trace_id,
            parent_span_id=turn_span_id,
            project_name=project_name,
            name=f"round-{round_item.round_idx}",
            type="general",
            start_time=round_start,
            end_time=round_end,
            input={"messages": list(conversation)},
            output={},
            metadata=_round_metadata(round_item, turn, session_id),
            tags=["reasoning-round", f"round-{round_item.round_idx}"],
        )

        round_outputs: list[dict[str, Any]] = []

        for llm_idx, step in round_item.step_items:
            model_display = strip_model_date(step.model)
            usage_meta = build_usage_metadata(step.usage)
            llm_usage = _opik_usage_ints(usage_meta)
            llm_start = parse_ts(step.timestamp)
            llm_end = _step_llm_end_time(step)
            assistant_content: list[dict[str, Any]] = []

            if step.reasoning:
                assistant_content.append({"type": "reasoning", "text": truncate_text(step.reasoning)})
            if step.text:
                assistant_content.append({"type": "text", "text": truncate_text(step.text)})
            for tool in step.tools:
                assistant_content.append({
                    "type": "tool_call",
                    "name": tool.name,
                    "args": tool.input,
                    "id": tool.call_id,
                })

            llm_span_id = deterministic_span_id(
                trace_id,
                span_scope,
                turn_prefix,
                str(turn.turn_index),
                "round",
                str(round_item.round_idx),
                "llm",
                str(llm_idx),
            )
            create_or_update_span(
                client,
                llm_span_id,
                trace_id=trace_id,
                parent_span_id=round_span_id,
                project_name=project_name,
                name=model_display or step.model or "llm",
                type="llm",
                start_time=llm_start,
                end_time=llm_end,
                input={"messages": list(conversation)},
                output={
                    "messages": [{"role": "assistant", "content": assistant_content}],
                    **({"usage_metadata": usage_meta} if usage_meta else {}),
                },
                metadata={
                    "message_id": step.message_id,
                    "finish_reason": step.finish_reason,
                    "agent": step.agent,
                    "mode": step.mode,
                    "variant": step.variant,
                    "cost": step.cost,
                    "raw_usage": step.usage,
                    "realtime": True,
                    "llm_index": llm_idx,
                    "round_idx": round_item.round_idx,
                },
                model=model_display or None,
                provider=step.provider_id or "opencode",
                usage=llm_usage,
            )

            assistant_message = {"role": "assistant", "content": assistant_content}
            conversation.append(assistant_message)
            round_outputs.append(assistant_message)

            for tool in step.tools:
                tool_output = stringify_content(tool.output)
                tool_start = parse_ts(tool.start_time)
                tool_end = parse_ts(tool.end_time)
                if tool_end < tool_start:
                    tool_end = tool_start

                if tool.is_subagent:
                    # Sub-agent sits at Round level — sibling of LLM call,
                    # not a tool result. (DESIGN §4)
                    agent_span_id = deterministic_span_id(
                        trace_id,
                        span_scope,
                        turn_prefix,
                        str(turn.turn_index),
                        "round",
                        str(round_item.round_idx),
                        "agent",
                        tool.child_session_id or tool.call_id,
                    )
                    create_or_update_span(
                        client,
                        agent_span_id,
                        trace_id=trace_id,
                        parent_span_id=round_span_id,
                        project_name=project_name,
                        name=f"task:{tool.child_agent or tool.child_description[:30] or 'subagent'}",
                        type="general",
                        start_time=tool_start,
                        end_time=tool_end,
                        input={
                            "description": tool.child_description,
                            "prompt": tool.child_prompt,
                            "agent": tool.child_agent,
                            "model": tool.child_model,
                        } if tool.child_prompt else {"input": tool.input},
                        output={"result": truncate_text(tool_output)},
                        metadata={
                            "is_subagent": True,
                            "child_session_id": tool.child_session_id,
                            "child_agent": tool.child_agent,
                            "child_model": tool.child_model,
                            "child_provider": tool.child_provider,
                            "child_description": tool.child_description,
                        },
                        error_info=_make_error_info(tool.error),
                        tags=["sub-agent"],
                    )
                else:
                    tool_span_id = deterministic_span_id(
                        trace_id,
                        span_scope,
                        turn_prefix,
                        str(turn.turn_index),
                        "round",
                        str(round_item.round_idx),
                        "llm",
                        str(llm_idx),
                        "tool",
                        tool.call_id or tool.name,
                    )
                    create_or_update_span(
                        client,
                        tool_span_id,
                        trace_id=trace_id,
                        parent_span_id=llm_span_id,
                        project_name=project_name,
                        name=tool.name,
                        type="tool",
                        start_time=tool_start,
                        end_time=tool_end,
                        input={"tool_call_id": tool.call_id, "input": tool.input},
                        output={"output": truncate_text(tool_output)},
                        metadata={
                            "status": tool.status,
                            "title": tool.title,
                            "tool_call_id": tool.call_id,
                            "tool_metadata": tool.tool_metadata,
                            "size_estimate": _tool_size_estimate(tool.name, tool.input, tool_output),
                        },
                        error_info=_make_error_info(tool.error),
                    )

                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool.call_id,
                    "content": [{"type": "text", "text": truncate_text(tool_output)}],
                }
                conversation.append(tool_message)
                round_outputs.append(tool_message)

        update_span_if_possible(
            client,
            round_span_id,
            trace_id=trace_id,
            project_name=project_name,
            parent_span_id=turn_span_id,
            output={"messages": round_outputs},
        )

    update_span_if_possible(
        client,
        turn_span_id,
        trace_id=trace_id,
        project_name=project_name,
        parent_span_id=parent_span_id,
        output={
            "messages": [item for item in conversation if item.get("role") != "user"],
            "final_output": truncate_text(turn.final_output or ""),
            "usage": turn.usage,
        },
    )
    return turn_end


def sync_subagent_sessions(
    client: Any,
    project_name: str,
    trace_id: str,
    conn: Any,
    state: dict[str, Any],
    session: SessionState,
    parent_session_id: str,
    turns: list[Any],
    span_scope: str,
    turn_prefix: str,
    depth: int,
    allow_partial: bool = False,
) -> None:
    if depth > 5:
        return

    for turn in turns:
        for round_item in _group_reasoning_rounds(turn):
            for _llm_idx, step in round_item.step_items:
                for tool in step.tools:
                    if not tool.is_subagent or not tool.child_session_id:
                        continue
                    if not session_exists(conn, tool.child_session_id):
                        continue

                    child_sid = tool.child_session_id
                    child_state = session.child_sessions.get(child_sid)

                    child_turns, _child_meta = parse_turns(conn, tool.child_session_id)
                    if child_state is None:
                        child_state = ChildSessionState(
                            child_session_id=child_sid,
                            parent_tool_call_id=tool.call_id,
                            agent_span_id=deterministic_span_id(
                                trace_id,
                                span_scope,
                                turn_prefix,
                                str(turn.turn_index),
                                "round",
                                str(round_item.round_idx),
                                "agent",
                                tool.child_session_id or tool.call_id,
                            ),
                        )
                        session.child_sessions[child_sid] = child_state
                    else:
                        child_state.parent_tool_call_id = child_state.parent_tool_call_id or tool.call_id

                    child_state.agent_span_id = deterministic_span_id(
                        trace_id,
                        span_scope,
                        turn_prefix,
                        str(turn.turn_index),
                        "round",
                        str(round_item.round_idx),
                        "agent",
                        tool.child_session_id or tool.call_id,
                    )
                    _refresh_child_accumulators(child_state, child_turns)

                    for idx, child_turn in enumerate(child_turns):
                        child_hash = hash_turn(child_turn)
                        if idx < child_state.emitted_turn_count:
                            if idx < len(child_state.emitted_turn_hashes) and child_state.emitted_turn_hashes[idx] == child_hash:
                                continue
                        elif not allow_partial and turn_is_incomplete(child_turn):
                            # Mirror the parent's `flush_turns` policy: when the
                            # parent is flushing in partial mode (the common
                            # live-trace case), let in-progress sub-agent turns
                            # appear too. Without this, a long-running sub-agent
                            # would show only its placeholder agent span until
                            # every step lands — and a sub-agent that ends with
                            # an interrupted step never emits any child turns.
                            break

                        emit_turn(
                            client=client,
                            project_name=project_name,
                            trace_id=trace_id,
                            parent_span_id=child_state.agent_span_id,
                            conn=conn,
                            session_id=tool.child_session_id,
                            turn=child_turn,
                            depth=depth + 1,
                            turn_prefix="sub-turn",
                            span_scope=f"{span_scope}:child:{tool.child_session_id}",
                            is_child=True,
                        )
                        if idx < child_state.emitted_turn_count:
                            child_state.emitted_turn_hashes[idx] = child_hash
                        else:
                            child_state.emitted_turn_count = idx + 1
                            child_state.emitted_turn_hashes.append(child_hash)

                    child_state.emitted_turn_hashes = child_state.emitted_turn_hashes[:child_state.emitted_turn_count]

                    if child_turns:
                        sync_subagent_sessions(
                            client=client,
                            project_name=project_name,
                            trace_id=trace_id,
                            conn=conn,
                            state=state,
                            session=session,
                            parent_session_id=tool.child_session_id,
                            turns=child_turns,
                            span_scope=f"{span_scope}:child:{tool.child_session_id}",
                            turn_prefix="sub-turn",
                            depth=depth + 1,
                            allow_partial=allow_partial,
                        )


def _peek_new_completed_turn(
    db_path: Path | JsonStore,
    session_id: str,
    session: SessionState,
) -> bool:
    """Return True if storage holds more fully-complete turns than the
    last-flush watermark. Used by the tool_complete dispatch to bypass the
    throttle exactly at turn boundaries, without committing to a full flush
    yet. Errors fail closed (return False) so we fall back to the time-based
    throttle gate."""
    try:
        conn = db_path if isinstance(db_path, JsonStore) else open_db(db_path)
    except Exception:
        return False
    try:
        turns, _ = parse_turns(conn, session_id)
        completed = sum(1 for t in turns if not turn_is_incomplete(t))
        return completed > session.completed_turn_count
    except Exception:
        return False
    finally:
        if not isinstance(conn, JsonStore):
            try:
                conn.close()
            except Exception:
                pass


def flush_turns(
    client: Any,
    project_name: str,
    state: dict[str, Any],
    session_id: str,
    session: SessionState,
    db_path: Path | JsonStore,
    allow_partial: bool = False,
    finalizing: bool = False,
) -> tuple[int, int]:
    """Emit any pending turns to Opik.

    Returns ``(emitted, turns_seen)`` where ``turns_seen`` is the number of
    turns parsed from the storage (regardless of whether they were already
    emitted) and ``emitted`` counts only NEW spans created in this call.
    Letting the caller log both makes "trace_created but emitted=0" debuggable
    without re-running.

    ``allow_partial`` controls whether in-progress turns (no step-finish yet
    on the trailing step) are emitted at all. Mid-run flushes pass True so the
    user sees spans appear live; the spans are idempotent under
    ``create_or_update_span`` and get updated as more parts land. ``finalizing``
    is the orthogonal "this is the last flush" signal — it tags the trace
    ``completed`` and flips ``session.completed`` so subsequent restarts know
    not to keep flushing this session.
    """
    conn = db_path if isinstance(db_path, JsonStore) else open_db(db_path)
    try:
        turns, session_meta = parse_turns(conn, session_id)
        if not turns:
            return (0, 0)

        trace_id = session_trace_id(session)
        session.trace_name = trace_name_for_session(session, session_meta)
        session.trace_start_ts = session.trace_start_ts or turns[0].timestamp
        session.session_start_ts = session.session_start_ts or session.trace_start_ts
        if finalizing:
            session.final_status = session.final_status or "completed"
            if not session.session_end_ts:
                session.session_end_ts = (
                    ms_to_iso(session_meta.time_updated)
                    if getattr(session_meta, "time_updated", 0)
                    else datetime.now(timezone.utc).isoformat()
                )

        # Create the trace BEFORE emitting any spans. If the trace doesn't
        # exist yet, posting spans first leaves them as orphans in Opik —
        # they show up in the spans tab but the traces tab stays empty.
        # `force_create=True` routes through create_trace_if_possible (not
        # update_trace, which silently no-ops or hangs against a non-existent
        # trace_id) and is the only thing that flips `session.trace_created`.
        # External mode (Harbor parent) skips trace create/update entirely —
        # the host already owns the trace and updating it would overwrite
        # Harbor's input/output/tags.
        if not session.trace_created and session.trace_owner == "opencode":
            upsert_trace(
                client=client,
                project_name=project_name,
                session=session,
                session_meta=session_meta,
                turns=turns,
                completed=False,
                force_create=True,
            )

        # Always upsert the session-root span before emitting turns; it is the
        # real parent every turn span points to. Idempotent across flushes
        # (deterministic id), so re-running just updates end_time/output.
        root_span_id = session_root_span_id(session)
        try:
            root_span_id = emit_session_root_span(
                client=client,
                project_name=project_name,
                session=session,
                session_meta=session_meta,
                turns=turns,
                session_id=session_id,
                completed=finalizing,
            )
        except Exception as exc:
            debug(f"session root span upsert failed id={root_span_id}: {exc}")

        emitted = 0
        for idx, turn in enumerate(turns):
            turn_hash = hash_turn(turn)
            if idx < session.emitted_turn_count:
                if idx < len(session.emitted_turn_hashes) and session.emitted_turn_hashes[idx] == turn_hash:
                    continue
            elif not allow_partial and turn_is_incomplete(turn):
                break

            turn_end = emit_turn(
                client=client,
                project_name=project_name,
                trace_id=trace_id,
                parent_span_id=root_span_id,
                conn=conn,
                session_id=session_id,
                turn=turn,
                depth=0,
                turn_prefix="turn",
                span_scope="root",
                is_child=False,
            )
            session.last_turn_ts = turn_end.isoformat()
            if idx < session.emitted_turn_count:
                session.emitted_turn_hashes[idx] = turn_hash
            else:
                session.emitted_turn_count = idx + 1
                session.emitted_turn_hashes.append(turn_hash)
                emitted += 1
            session.turn_number = max(session.turn_number, idx + 1)
        session.emitted_turn_hashes = session.emitted_turn_hashes[:session.emitted_turn_count]

        sync_subagent_sessions(
            client=client,
            project_name=project_name,
            trace_id=trace_id,
            conn=conn,
            state=state,
            session=session,
            parent_session_id=session_id,
            turns=turns,
            span_scope="root",
            turn_prefix="turn",
            depth=0,
            allow_partial=allow_partial,
        )

        if not session.last_turn_ts:
            last_turn = turns[min(max(session.emitted_turn_count, 1), len(turns)) - 1]
            session.last_turn_ts = _compute_turn_end(last_turn).isoformat()

        if session.trace_owner == "opencode":
            upsert_trace(
                client=client,
                project_name=project_name,
                session=session,
                session_meta=session_meta,
                turns=turns,
                completed=finalizing,
            )

        # Second pass on the root span now that session.last_turn_ts and any
        # final output are settled — first pass (above) seeded it so turn spans
        # had a real parent; this pass advances end_time and (on finalize)
        # writes the session summary.
        try:
            emit_session_root_span(
                client=client,
                project_name=project_name,
                session=session,
                session_meta=session_meta,
                turns=turns,
                session_id=session_id,
                completed=finalizing,
            )
        except Exception as exc:
            debug(f"session root span final upsert failed id={root_span_id}: {exc}")

        session.last_flush_time = time.time()
        session.completed = finalizing
        if finalizing:
            session.trace_finalized = True
        # Watermark of fully-complete turns, used by the tool_complete dispatch
        # to bypass FLUSH_INTERVAL_S the instant a turn finishes (so each turn's
        # trajectory appears in the trace tab promptly, not at next throttle tick).
        session.completed_turn_count = sum(1 for t in turns if not turn_is_incomplete(t))
        return (emitted, len(turns))
    finally:
        if not isinstance(conn, JsonStore):
            conn.close()


def ensure_trace_from_db(
    client: Any,
    project_name: str,
    session: SessionState,
    session_id: str,
    db_path: Path | JsonStore,
) -> None:
    conn = db_path if isinstance(db_path, JsonStore) else open_db(db_path)
    try:
        turns, session_meta = parse_turns(conn, session_id)
        if not turns:
            return
        session.trace_start_ts = session.trace_start_ts or turns[0].timestamp
        session.session_start_ts = session.session_start_ts or session.trace_start_ts
        session.last_turn_ts = session.last_turn_ts or _compute_turn_end(turns[-1]).isoformat()
        # Resolve trace_id so trace_owner is settled before we decide whether
        # to write the trace (external mode: never touch the trace; Harbor owns it).
        session_trace_id(session)
        if session.trace_owner == "opencode":
            upsert_trace(
                client=client,
                project_name=project_name,
                session=session,
                session_meta=session_meta,
                turns=turns,
                completed=False,
            )
    finally:
        if not isinstance(conn, JsonStore):
            conn.close()


def main() -> int:
    if os.environ.get("TRACE_TO_OPIK", "true").lower() != "true":
        return 0

    apply_opik_env_overrides()
    payload = read_hook_payload()
    event_name = hook_event_name(payload)
    payload_ts = event_timestamp(payload)
    # Final events sometimes omit a timestamp entirely; synthesize one now so
    # session_end_ts has a real value to record.
    if event_is_final(event_name) and not payload_ts:
        payload_ts = datetime.now(timezone.utc).isoformat()
        payload["hook_event_timestamp"] = payload_ts
    session_id = extract_session_id(payload)
    data_source = resolve_db_path(payload)
    if data_source is None:
        data_source = resolve_json_store(payload, session_id)

    if not session_id:
        debug("missing session_id in hook payload/env")
        return 0
    if data_source is None:
        debug("unable to resolve opencode storage path")
        return 0
    if Opik is None and not DRY_RUN:
        debug("opik sdk not installed")
        return 0

    # Sub-agent sessions (parent_id is set) are surfaced as nested spans under
    # the parent trace by sync_subagent_sessions. Skip top-level processing so
    # they don't also appear as standalone traces.
    parent_check_conn: Any = (
        data_source if isinstance(data_source, JsonStore) else open_db(data_source)
    )
    try:
        parent_id = session_parent_id(parent_check_conn, session_id)
    finally:
        if not isinstance(parent_check_conn, JsonStore):
            parent_check_conn.close()
    if parent_id:
        debug(f"skip sub-agent session={session_id} parent={parent_id}")
        return 0

    project_name = _env_first("OPIK_PROJECT_NAME", "OC_OPIK_PROJECT") or DEFAULT_PROJECT
    client: Any = None
    if not DRY_RUN:
        try:
            client = Opik(project_name=project_name)
        except Exception as exc:
            debug(f"failed to init Opik client: {exc}")
            return 0

    data_source_str = _describe_data_source(data_source)
    emitted = 0
    turns_seen = 0
    status = "ok"
    deadline_installed = _install_hook_deadline(HOOK_DEADLINE_S)

    try:
        with FileLock(LOCK_FILE):
            state = load_state()
            session = load_session_state(state, session_id)

            # Once finalized, the trace is closed. Late-arriving non-start
            # events (e.g. delayed tool_complete, post-finalize idle ticks)
            # must NOT reopen / overwrite the trace — they would clobber the
            # final status and re-extend the end time.
            if session.trace_finalized and not event_is_start(event_name):
                debug(f"skip finalized session event={event_name}")
                save_session_state(state, session_id, session)
                save_state(state)
                return 0

            if event_is_start(event_name):
                ensure_trace_from_db(client, project_name, session, session_id, data_source)
                save_session_state(state, session_id, session)
                save_state(state)
            elif event_is_final(event_name):
                session.session_end_ts = payload_ts or session.session_end_ts
                session.final_status = session.final_status or "completed"
                emitted, turns_seen = flush_turns(
                    client, project_name, state, session_id, session,
                    data_source, allow_partial=True, finalizing=True,
                )
                save_session_state(state, session_id, session)
                save_state(state)
            elif event_name == "tool_complete":
                # Throttle per design §2: tool_complete is debounced (5s) for
                # within-turn updates, but a turn-boundary crossing (new turn
                # just became complete) bypasses the throttle so the trajectory
                # appears in the trace tab as each turn finishes.
                now = time.time()
                new_completed_turn = _peek_new_completed_turn(data_source, session_id, session)
                if (
                    now - session.last_flush_time >= FLUSH_INTERVAL_S
                    or not session.emitted_turn_count
                    or new_completed_turn
                ):
                    emitted, turns_seen = flush_turns(
                        client, project_name, state, session_id, session,
                        data_source, allow_partial=True, finalizing=False,
                    )
                save_session_state(state, session_id, session)
                save_state(state)
            elif event_is_flush(event_name) or not event_name:
                emitted, turns_seen = flush_turns(
                    client, project_name, state, session_id, session,
                    data_source, allow_partial=True, finalizing=False,
                )
                save_session_state(state, session_id, session)
                save_state(state)
            else:
                ensure_trace_from_db(client, project_name, session, session_id, data_source)
                save_session_state(state, session_id, session)
                save_state(state)
    except HookTimeoutError as exc:
        # Soft signal: emit partial summary below, no re-raise. Spans created
        # before the alarm are already POSTed by Opik's REST calls; what we
        # lose is the in-flight call and any subsequent turns.
        status = "deadline_exceeded"
        debug(f"hook deadline exceeded: {exc}")
    except TimeoutError as exc:
        status = "lock_timeout"
        debug(f"hook skipped: {exc}")
    except Exception as exc:
        status = f"error:{exc.__class__.__name__}"
        debug(f"hook failed: {exc}")
    finally:
        # Cancel the wallclock alarm BEFORE the flush call — we use a separate
        # thread-join timeout for flush so the SDK background uploader can't
        # turn this finally into a new hang.
        if deadline_installed:
            _clear_hook_deadline()
        try:
            batch_status = flush_span_batch(client, SPAN_BATCH_ENV_NAMES, log=info)
        except Exception as exc:
            batch_status = f"error:{exc.__class__.__name__}"
            debug(f"span batch flush failed: {exc}")
        flush_status = _flush_with_timeout(client, OPIK_FLUSH_TIMEOUT_S)
        info(
            f"final flush: event={event_name or 'default'} session={session_id} "
            f"turns_seen={turns_seen} emitted={emitted} status={status} "
            f"data_source={data_source_str}"
        )
        info(f"span batch flush: {batch_status}")
        info(f"client flush: {flush_status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
