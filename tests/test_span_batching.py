from __future__ import annotations

from datetime import datetime

from sii_opik_plugin import span_batching
from sii_opik_plugin.opencode import opencode_realtime_trace as ort


class _FakeSpans:
    def __init__(self) -> None:
        self.created_batches = []
        self.created = []
        self.updated = []

    def create_spans(self, *, spans):
        self.created_batches.append(spans)

    def create_span(self, **kwargs):
        self.created.append(kwargs)

    def update_span(self, span_id, **kwargs):
        self.updated.append((span_id, kwargs))


class _FakeRestClient:
    def __init__(self) -> None:
        self.spans = _FakeSpans()


class _FakeClient:
    def __init__(self) -> None:
        self.rest_client = _FakeRestClient()


class _FakeLegacySpans:
    def __init__(self) -> None:
        self.created = []

    def create_span(self, **kwargs):
        self.created.append(kwargs)


class _FakeLegacyRestClient:
    def __init__(self) -> None:
        self.spans = _FakeLegacySpans()


class _FakeLegacyClient:
    def __init__(self) -> None:
        self.rest_client = _FakeLegacyRestClient()


def _span_value(item, key):
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key)


def setup_function() -> None:
    span_batching.clear_pending_spans()


def teardown_function() -> None:
    span_batching.clear_pending_spans()


def test_env_flag_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("OPIK_SPAN_BATCH_ENABLED", raising=False)

    assert span_batching.env_flag_enabled(("OPIK_SPAN_BATCH_ENABLED",)) is True


def test_env_flag_can_disable_batching(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "false")

    assert span_batching.env_flag_enabled(("OPIK_SPAN_BATCH_ENABLED",)) is False


def test_batch_size_defaults_and_can_be_overridden(monkeypatch):
    monkeypatch.delenv("OPIK_SPAN_BATCH_SIZE", raising=False)
    assert span_batching.span_batch_size() == 5

    monkeypatch.setenv("OPIK_SPAN_BATCH_SIZE", "2")
    assert span_batching.span_batch_size() == 2

    monkeypatch.setenv("OPIK_SPAN_BATCH_SIZE", "0")
    assert span_batching.span_batch_size() == 1

    monkeypatch.setenv("OPIK_SPAN_BATCH_SIZE", "invalid")
    assert span_batching.span_batch_size() == 5


def test_opencode_create_and_update_are_coalesced_into_one_batch(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "true")
    client = _FakeClient()
    span_id = "00000000-0000-7000-8000-000000000001"

    ort.create_or_update_span(
        client,
        span_id,
        trace_id="00000000-0000-7000-8000-000000000002",
        project_name="proj",
        parent_span_id=None,
        name="turn-1",
        type="general",
        start_time=datetime(2026, 1, 1),
        output={},
    )
    ort.update_span_if_possible(
        client,
        span_id,
        trace_id="00000000-0000-7000-8000-000000000002",
        project_name="proj",
        output={"messages": ["done"]},
    )

    status = span_batching.flush_span_batch(client, ort.SPAN_BATCH_ENV_NAMES)

    assert status == "ok:1"
    assert client.rest_client.spans.created == []
    assert client.rest_client.spans.updated == []
    assert len(client.rest_client.spans.created_batches) == 1
    batch = client.rest_client.spans.created_batches[0]
    assert len(batch) == 1
    item = batch[0]
    item_id = _span_value(item, "id")
    item_name = _span_value(item, "name")
    item_output = _span_value(item, "output")
    assert item_id == span_id
    assert item_name == "turn-1"
    assert item_output == {"messages": ["done"]}


def test_flush_chunks_by_batch_size(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "true")
    monkeypatch.setenv("OPIK_SPAN_BATCH_SIZE", "2")
    client = _FakeClient()
    trace_id = "00000000-0000-7000-8000-000000000100"

    for index in range(5):
        span_id = f"00000000-0000-7000-8000-{index + 1:012x}"
        assert span_batching.queue_span_snapshot(
            span_id,
            {
                "trace_id": trace_id,
                "project_name": "proj",
                "name": f"turn-{index}",
                "type": "general",
                "start_time": datetime(2026, 1, 1),
            },
            ("OPIK_SPAN_BATCH_ENABLED",),
        )

    status = span_batching.flush_span_batch(client, ("OPIK_SPAN_BATCH_ENABLED",))

    assert status == "ok:5"
    assert span_batching.pending_span_count() == 0
    assert [len(batch) for batch in client.rest_client.spans.created_batches] == [2, 2, 1]


def test_opencode_batching_can_be_disabled(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "false")
    client = _FakeClient()

    ort.create_or_update_span(
        client,
        "span-1",
        trace_id="trace-1",
        project_name="proj",
        name="turn-1",
        type="general",
        start_time=datetime(2026, 1, 1),
    )

    assert len(client.rest_client.spans.created) == 1
    assert span_batching.pending_span_count() == 0


def test_flush_falls_back_to_single_create_when_batch_api_is_missing(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "true")
    client = _FakeLegacyClient()

    assert span_batching.queue_span_snapshot(
        "span-1",
        {
            "trace_id": "trace-1",
            "project_name": "proj",
            "name": "turn-1",
            "type": "general",
            "start_time": datetime(2026, 1, 1),
        },
        ("OPIK_SPAN_BATCH_ENABLED",),
    )

    status = span_batching.flush_span_batch(client, ("OPIK_SPAN_BATCH_ENABLED",))

    assert status == "fallback:1"
    assert span_batching.pending_span_count() == 0
    assert len(client.rest_client.spans.created) == 1
    created = client.rest_client.spans.created[0]
    assert created["id"] == "span-1"
    assert created["name"] == "turn-1"
    assert "last_updated_at" not in created


def test_update_without_queued_snapshot_uses_patch(monkeypatch):
    monkeypatch.setenv("OPIK_SPAN_BATCH_ENABLED", "true")
    client = _FakeClient()

    ort.update_span_if_possible(
        client,
        "span-1",
        trace_id="trace-1",
        project_name="proj",
        output={"status": "completed"},
    )

    assert client.rest_client.spans.updated == [
        ("span-1", {"trace_id": "trace-1", "project_name": "proj", "output": {"status": "completed"}})
    ]
