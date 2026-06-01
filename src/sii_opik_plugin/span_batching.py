"""Small shared helper for batching Opik span writes.

The realtime tracers produce complete span snapshots with deterministic IDs.
Sending those snapshots through ``POST /spans/batch`` avoids the backend's
single-span read-before-write merge path while preserving latest-row semantics.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable


_TRUE_VALUES = {"1", "true", "yes", "on", "enabled"}
_FALSE_VALUES = {"0", "false", "no", "off", "disabled"}
_DEFAULT_BATCH_SIZE = 20
_PENDING_SPANS: dict[str, dict[str, Any]] = {}


def env_flag_enabled(names: tuple[str, ...], default: bool = True) -> bool:
    for name in names:
        value = os.environ.get(name)
        if value is None or value.strip() == "":
            continue
        normalized = value.strip().lower()
        if normalized in _TRUE_VALUES:
            return True
        if normalized in _FALSE_VALUES:
            return False
    return default


def span_batch_env_names() -> tuple[str, ...]:
    return ("OPIK_SPAN_BATCH_ENABLED",)


def span_batch_size(default: int = _DEFAULT_BATCH_SIZE) -> int:
    value = os.environ.get("OPIK_SPAN_BATCH_SIZE")
    if value is None or value.strip() == "":
        return default

    try:
        parsed = int(value.strip())
    except ValueError:
        return default

    return max(1, parsed)


def pending_span_count() -> int:
    return len(_PENDING_SPANS)


def clear_pending_spans() -> None:
    _PENDING_SPANS.clear()


def queue_span_snapshot(
    span_id: str,
    payload: dict[str, Any],
    env_names: tuple[str, ...],
    *,
    default_enabled: bool = True,
) -> bool:
    if not env_flag_enabled(env_names, default=default_enabled):
        return False

    snapshot = dict(payload)
    snapshot["last_updated_at"] = datetime.now(timezone.utc)
    _PENDING_SPANS[span_id] = snapshot
    return True


def update_queued_span(
    span_id: str,
    updates: dict[str, Any],
    env_names: tuple[str, ...],
    *,
    default_enabled: bool = True,
) -> bool:
    if not env_flag_enabled(env_names, default=default_enabled):
        return False
    if span_id not in _PENDING_SPANS:
        return False

    _PENDING_SPANS[span_id].update(updates)
    _PENDING_SPANS[span_id]["last_updated_at"] = datetime.now(timezone.utc)
    return True


def _to_span_write(span_id: str, payload: dict[str, Any]) -> Any:
    data = {"id": span_id, **payload}
    try:
        from opik.rest_api.types import SpanWrite  # type: ignore

        return SpanWrite(**data)
    except Exception:
        return data


def _chunks(items: list[tuple[str, dict[str, Any]]], size: int) -> list[list[tuple[str, dict[str, Any]]]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def flush_span_batch(
    client: Any,
    env_names: tuple[str, ...],
    *,
    log: Callable[[str], None] | None = None,
    default_enabled: bool = True,
) -> str:
    if client is None:
        return "skipped"
    if not env_flag_enabled(env_names, default=default_enabled):
        return "disabled"
    if not _PENDING_SPANS:
        return "empty"

    spans_api = getattr(getattr(client, "rest_client", None), "spans", None)
    create_spans = getattr(spans_api, "create_spans", None)
    if callable(create_spans):
        pending = list(_PENDING_SPANS.items())
        batch_count = 0
        for chunk in _chunks(pending, span_batch_size()):
            batch = [_to_span_write(span_id, payload) for span_id, payload in chunk]
            create_spans(spans=batch)
            batch_count += 1

        count = len(pending)
        clear_pending_spans()
        if log is not None:
            log(f"span batch flush: {count} span(s) in {batch_count} request(s)")
        return f"ok:{count}"

    create_span = getattr(spans_api, "create_span", None)
    if not callable(create_span):
        return "unsupported"

    for span_id, payload in _PENDING_SPANS.items():
        single_payload = {k: v for k, v in payload.items() if k != "last_updated_at"}
        create_span(id=span_id, **single_payload)

    count = len(_PENDING_SPANS)
    clear_pending_spans()
    if log is not None:
        log(f"span batch fallback: {count} span(s)")
    return f"fallback:{count}"
