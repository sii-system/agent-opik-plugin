"""Unit tests for openclaw_opik_tracer.

Covers pure helpers, payload normalisers, ID generation, state-key
resolution, and SessionState / SubagentState round-trips. End-to-end
hook handlers and Opik client wrappers are intentionally out of scope
— they require a live Opik client and full transcript fixtures.
"""

from __future__ import annotations

import os

import pytest

from sii_opik_plugin.openclaw import openclaw_opik_tracer as oot


# ── Env helpers ──────────────────────────────────────────────────────────────


class TestEnvFirst:
    def test_returns_first_non_empty(self, monkeypatch):
        monkeypatch.setenv("A", "")
        monkeypatch.setenv("B", "value-b")
        monkeypatch.setenv("C", "value-c")
        assert oot._env_first("A", "B", "C") == "value-b"

    def test_returns_none_when_all_unset(self, monkeypatch):
        for name in ("X", "Y", "Z"):
            monkeypatch.delenv(name, raising=False)
        assert oot._env_first("X", "Y", "Z") is None

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("FOO", "")
        assert oot._env_first("FOO") is None


class TestApplyOpikEnvOverrides:
    def _clear(self, monkeypatch):
        for name in (
            "OPIK_URL", "OPIK_URL_OVERRIDE",
            "OPIK_API_KEY", "OPIK_API_KEY_OVERRIDE",
            "OPIK_WORKSPACE", "OPIK_WORKSPACE_OVERRIDE",
        ):
            monkeypatch.delenv(name, raising=False)

    def test_override_promoted_when_target_unset(self, monkeypatch):
        self._clear(monkeypatch)
        monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://override.example")
        oot.apply_opik_env_overrides()
        assert os.environ["OPIK_URL"] == "https://override.example"

    def test_existing_target_is_not_clobbered(self, monkeypatch):
        self._clear(monkeypatch)
        monkeypatch.setenv("OPIK_URL", "https://primary.example")
        monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://override.example")
        oot.apply_opik_env_overrides()
        assert os.environ["OPIK_URL"] == "https://primary.example"

    def test_all_three_vars_promoted_together(self, monkeypatch):
        self._clear(monkeypatch)
        monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://o.example")
        monkeypatch.setenv("OPIK_API_KEY_OVERRIDE", "key-123")
        monkeypatch.setenv("OPIK_WORKSPACE_OVERRIDE", "ws-x")
        oot.apply_opik_env_overrides()
        assert os.environ["OPIK_URL"] == "https://o.example"
        assert os.environ["OPIK_API_KEY"] == "key-123"
        assert os.environ["OPIK_WORKSPACE"] == "ws-x"


# ── Session file helpers ─────────────────────────────────────────────────────


class TestLooksLikeSessionFile:
    @pytest.mark.parametrize("value", [
        "/tmp/session.jsonl",
        "C:\\openclaw\\session.jsonl",
        "relative/path/session.jsonl",
        "session.jsonl",
        "a/b",
    ])
    def test_matches_paths_and_jsonl(self, value):
        assert oot._looks_like_session_file(value) is True

    @pytest.mark.parametrize("value", [
        "sess-abc-123",
        "plain-id",
        "",
    ])
    def test_rejects_plain_ids(self, value):
        assert oot._looks_like_session_file(value) is False


class TestNormalizeSessionFile:
    def test_empty_returns_empty(self):
        assert oot._normalize_session_file("") == ""

    def test_expands_and_resolves(self, tmp_path, monkeypatch):
        f = tmp_path / "session.jsonl"
        f.touch()
        normalized = oot._normalize_session_file(str(f))
        assert normalized == str(f.resolve())

    def test_handles_nonexistent_paths(self, tmp_path):
        missing = tmp_path / "does-not-exist.jsonl"
        normalized = oot._normalize_session_file(str(missing))
        assert normalized.endswith("does-not-exist.jsonl")


# ── Timestamp / model helpers ────────────────────────────────────────────────


class TestParseTs:
    def test_parses_iso_with_offset(self):
        ts = oot.parse_ts("2024-01-02T03:04:05+00:00")
        assert ts.year == 2024 and ts.hour == 3

    def test_converts_zulu_suffix(self):
        ts = oot.parse_ts("2024-01-02T03:04:05Z")
        assert ts.tzinfo is not None
        assert ts.utcoffset().total_seconds() == 0

    def test_parses_epoch_millis_string(self):
        # 1_700_000_000_000 ms == 2023-11-14T22:13:20Z
        ts = oot.parse_ts("1700000000000")
        assert ts.year == 2023 and ts.month == 11

    @pytest.mark.parametrize("value", ["", "not-a-date"])
    def test_unparseable_falls_back_to_aware_now(self, value):
        ts = oot.parse_ts(value)
        assert ts.tzinfo is not None  # UTC fallback


class TestStripModelDate:
    def test_strips_trailing_8_digit_date(self):
        assert oot.strip_model_date("claude-sonnet-4-5-20251022") == "claude-sonnet-4-5"

    def test_no_date_unchanged(self):
        assert oot.strip_model_date("claude-sonnet-4-5") == "claude-sonnet-4-5"

    def test_short_string_unchanged(self):
        # Guarded by `len(model) > 9` — short strings skip the regex.
        assert oot.strip_model_date("gpt-4") == "gpt-4"

    def test_empty(self):
        assert oot.strip_model_date("") == ""


# ── Text helpers ─────────────────────────────────────────────────────────────


class TestTruncateText:
    def test_short_text_unchanged(self):
        assert oot.truncate_text("hello") == "hello"

    def test_long_text_clipped(self, monkeypatch):
        monkeypatch.setattr(oot, "MAX_TEXT_CHARS", 10)
        assert oot.truncate_text("a" * 25) == "a" * 10

    def test_exact_length_unchanged(self, monkeypatch):
        monkeypatch.setattr(oot, "MAX_TEXT_CHARS", 5)
        assert oot.truncate_text("abcde") == "abcde"


class TestExtractTextAndReasoning:
    def test_strips_thinking_tags_and_captures_them(self):
        reasoning: list[str] = []
        cleaned = oot._extract_text_and_reasoning(
            "before<thinking>secret thought</thinking>after", reasoning
        )
        assert cleaned == "beforeafter"
        assert reasoning == ["secret thought"]

    def test_captures_multiple_blocks(self):
        reasoning: list[str] = []
        oot._extract_text_and_reasoning(
            "<thinking>one</thinking>middle<thinking>two</thinking>", reasoning
        )
        assert reasoning == ["one", "two"]

    def test_dotall_across_newlines(self):
        reasoning: list[str] = []
        cleaned = oot._extract_text_and_reasoning(
            "head<thinking>line1\nline2</thinking>tail", reasoning
        )
        assert cleaned == "headtail"
        assert reasoning == ["line1\nline2"]

    def test_no_tags_returns_input_stripped(self):
        reasoning: list[str] = []
        assert oot._extract_text_and_reasoning("  plain text  ", reasoning) == "plain text"
        assert reasoning == []


class TestIsContinuationMessage:
    def test_task_notification_matches(self):
        assert oot._is_continuation_message("<task-notification>foo</task-notification>") is True

    def test_system_reminder_matches(self):
        assert oot._is_continuation_message("<system-reminder>foo</system-reminder>") is True

    def test_leading_whitespace_allowed(self):
        assert oot._is_continuation_message("\n  <system-reminder>foo</system-reminder>") is True

    def test_regular_user_text_does_not_match(self):
        assert oot._is_continuation_message("Hello, please run the tests") is False

    def test_unknown_tag_does_not_match(self):
        assert oot._is_continuation_message("<other-tag>foo</other-tag>") is False


# ── Tool block helpers ───────────────────────────────────────────────────────


class TestIsToolCallBlock:
    @pytest.mark.parametrize("type_name", ["toolCall", "toolUse", "tool_use", "functionCall"])
    def test_known_tool_types(self, type_name):
        assert oot._is_tool_call_block({"type": type_name}) is True

    @pytest.mark.parametrize("type_name", ["text", "toolResult", "image", ""])
    def test_other_types(self, type_name):
        assert oot._is_tool_call_block({"type": type_name}) is False

    def test_missing_type_key(self):
        assert oot._is_tool_call_block({}) is False


class TestExtractToolCallId:
    def test_id_field(self):
        assert oot._extract_tool_call_id({"id": "abc"}) == "abc"

    def test_tool_call_id_fallback(self):
        assert oot._extract_tool_call_id({"toolCallId": "xyz"}) == "xyz"

    def test_empty_when_missing(self):
        assert oot._extract_tool_call_id({}) == ""

    def test_id_wins_over_tool_call_id(self):
        assert oot._extract_tool_call_id({"id": "primary", "toolCallId": "fallback"}) == "primary"


# ── Usage helpers ────────────────────────────────────────────────────────────


class TestNormalizeUsage:
    def test_empty_returns_empty(self):
        assert oot._normalize_usage({}) == {}

    def test_openclaw_field_names(self):
        assert oot._normalize_usage({
            "input": 100,
            "output": 50,
            "cacheRead": 30,
            "cacheWrite": 20,
        }) == {
            "input_tokens": 100,
            "output_tokens": 50,
            "cache_read_input_tokens": 30,
            "cache_creation_input_tokens": 20,
        }

    def test_standard_field_names_fallback(self):
        assert oot._normalize_usage({
            "input_tokens": 1,
            "output_tokens": 2,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": 4,
        }) == {
            "input_tokens": 1,
            "output_tokens": 2,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": 4,
        }

    def test_openclaw_keys_win_over_standard(self):
        # When both shapes are present, the openclaw key (`input`) is preferred.
        result = oot._normalize_usage({"input": 99, "input_tokens": 1})
        assert result["input_tokens"] == 99


class TestBuildUsageMetadata:
    def test_fills_missing_keys_with_zero(self):
        assert oot.build_usage_metadata({"input_tokens": 5}) == {
            "input_tokens": 5,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }

    def test_empty(self):
        assert oot.build_usage_metadata({}) == {
            "input_tokens": 0,
            "output_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }


class TestOpikUsage:
    def test_sums_input_output(self):
        assert oot._opik_usage({"input_tokens": 10, "output_tokens": 4}) == {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 14,
        }

    def test_missing_keys_default_to_zero(self):
        assert oot._opik_usage({}) == {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        }


# ── Trace metadata helpers ───────────────────────────────────────────────────


class TestSessionMetadata:
    def test_session_affinity_uses_session_key(self):
        metadata = oot._session_metadata(oot.SessionState(), "session-123")

        assert metadata["session_key"] == "session-123"
        assert metadata["x-session-affinity"] == "session-123"


# ── ID helpers ───────────────────────────────────────────────────────────────


class TestDeterministicSpanId:
    def test_deterministic(self):
        a = oot._deterministic_span_id("trace-1", "turn", "0")
        b = oot._deterministic_span_id("trace-1", "turn", "0")
        assert a == b

    def test_different_inputs_different_ids(self):
        a = oot._deterministic_span_id("trace-1", "turn", "0")
        b = oot._deterministic_span_id("trace-1", "turn", "1")
        assert a != b

    def test_returns_uuid_v7_string(self):
        import uuid
        sid = oot._deterministic_span_id("trace-1", "turn", "0")
        parsed = uuid.UUID(sid)
        assert parsed.version == 7


class TestNewOpikId:
    def test_returns_uuid_string(self):
        import uuid
        sid = oot.new_opik_id()
        # Must be a parseable UUID regardless of backend (id_helpers/uuid7/uuid4).
        uuid.UUID(sid)


class TestSessionTraceId:
    def test_reuses_existing_trace_id(self):
        s = oot.SessionState(trace_id="existing-id")
        assert oot.session_trace_id(s) == "existing-id"

    def test_assigns_and_persists_when_missing(self):
        s = oot.SessionState()
        assert s.trace_id is None
        sid = oot.session_trace_id(s)
        assert sid
        assert s.trace_id == sid
        # Second call must return the same id (no churn).
        assert oot.session_trace_id(s) == sid


# ── State key resolution ─────────────────────────────────────────────────────


class TestFindStateKeyBySessionKey:
    def test_empty_session_key_returns_empty(self):
        assert oot._find_state_key_by_session_key({"sessions": {}}, "") == ""

    def test_single_active_match(self):
        global_state = {"sessions": {
            "/p/a.jsonl": {"session_key": "k", "completed": False},
        }}
        assert oot._find_state_key_by_session_key(global_state, "k") == "/p/a.jsonl"

    def test_prefers_active_over_completed(self):
        global_state = {"sessions": {
            "/p/done.jsonl":   {"session_key": "k", "completed": True},
            "/p/active.jsonl": {"session_key": "k", "completed": False},
        }}
        assert oot._find_state_key_by_session_key(global_state, "k") == "/p/active.jsonl"

    def test_multiple_active_matches_returns_empty(self):
        global_state = {"sessions": {
            "/p/a.jsonl": {"session_key": "k", "completed": False},
            "/p/b.jsonl": {"session_key": "k", "completed": False},
        }}
        assert oot._find_state_key_by_session_key(global_state, "k") == ""

    def test_falls_back_to_single_completed(self):
        global_state = {"sessions": {
            "/p/done.jsonl": {"session_key": "k", "completed": True},
        }}
        assert oot._find_state_key_by_session_key(global_state, "k") == "/p/done.jsonl"

    def test_no_match_returns_empty(self):
        global_state = {"sessions": {
            "/p/a.jsonl": {"session_key": "other", "completed": False},
        }}
        assert oot._find_state_key_by_session_key(global_state, "k") == ""


class TestResolveStateKey:
    def test_existing_file_key_wins(self, tmp_path):
        f = tmp_path / "s.jsonl"
        f.touch()
        normalized = oot._normalize_session_file(str(f))
        global_state = {"sessions": {normalized: {"session_key": "k", "completed": False}}}
        assert oot.resolve_state_key(global_state, "k", str(f)) == normalized

    def test_no_file_falls_back_to_session_key(self):
        assert oot.resolve_state_key({"sessions": {}}, "session-k", "") == "session-k"

    def test_no_match_returns_normalized_file(self, tmp_path):
        f = tmp_path / "s.jsonl"
        normalized = oot._normalize_session_file(str(f))
        assert oot.resolve_state_key({"sessions": {}}, "session-k", str(f)) == normalized

    def test_reuses_active_session_when_file_unknown(self, tmp_path):
        f = tmp_path / "s.jsonl"
        f.touch()
        # Existing session has no session_file yet and is not completed —
        # resolve_state_key should reuse it instead of creating a new entry.
        global_state = {"sessions": {
            "session-k": {"session_key": "session-k", "completed": False, "session_file": ""},
        }}
        assert oot.resolve_state_key(global_state, "session-k", str(f)) == "session-k"


# ── SessionState round-trips ─────────────────────────────────────────────────


class TestSessionStateRoundTrip:
    def test_default_round_trip(self):
        state: dict = {}
        oot.save_session_state(state, "k", oot.SessionState())
        loaded = oot.load_session_state(state, "k")
        assert loaded.committed_offset == 0
        assert loaded.pending_offset == 0
        assert loaded.emitted_turns == 0
        assert loaded.trace_id is None
        assert loaded.completed is False
        assert loaded.session_models == []
        assert loaded.pending_tool_calls == {}
        assert loaded.pending_subagents == {}

    def test_populated_round_trip(self):
        state: dict = {}
        original = oot.SessionState(
            committed_offset=512,
            pending_offset=600,
            emitted_turns=3,
            trace_id="t-1",
            trace_name="my-trace",
            trace_start_ts="2024-01-01T00:00:00Z",
            completed=False,
            session_file="/p/s.jsonl",
            session_key="sess-1",
            session_total_llm_calls=4,
            session_models=["claude-sonnet-4-5"],
            session_api_billed_input=1000,
            session_api_billed_output=500,
            pending_tool_calls={"tool-1": 1700000000.0},
        )
        oot.save_session_state(state, "/p/s.jsonl", original)
        loaded = oot.load_session_state(state, "/p/s.jsonl")
        assert loaded.committed_offset == 512
        assert loaded.pending_offset == 600
        assert loaded.emitted_turns == 3
        assert loaded.trace_id == "t-1"
        assert loaded.session_models == ["claude-sonnet-4-5"]
        assert loaded.session_api_billed_input == 1000
        assert loaded.pending_tool_calls == {"tool-1": 1700000000.0}

    def test_missing_key_returns_default(self):
        loaded = oot.load_session_state({"sessions": {}}, "absent")
        assert loaded.committed_offset == 0
        assert loaded.trace_id is None

    def test_legacy_turn_start_offset_maps_to_committed(self):
        """State written by an earlier tracer version only had
        `turn_start_offset`. Loading it must keep the offset alive under the
        new committed/pending fields."""
        state = {"sessions": {"k": {"turn_start_offset": 1234, "emitted_turns": 2}}}
        loaded = oot.load_session_state(state, "k")
        assert loaded.committed_offset == 1234
        assert loaded.pending_offset == 1234
        assert loaded.emitted_turns == 2

    def test_file_keyed_state_sets_session_file_field(self, tmp_path):
        """When the dict key looks like a session file (contains `/` or ends
        in `.jsonl`), load_session_state should populate `session_file`
        accordingly even if the raw dict has no explicit `session_file`."""
        f = tmp_path / "s.jsonl"
        f.touch()
        state = {"sessions": {str(f): {"session_key": "sess-x"}}}
        loaded = oot.load_session_state(state, str(f))
        assert loaded.session_file == str(f.resolve())
        assert loaded.session_key == "sess-x"


# ── SubagentState round-trips ────────────────────────────────────────────────


class TestSubagentStateRoundTrip:
    def test_load_missing_returns_none(self):
        assert oot.load_subagent_state({"subagents": {}}, "absent") is None

    def test_default_round_trip(self):
        state: dict = {}
        oot.save_subagent_state(state, "child-1", oot.SubagentState(agent_id="a1"))
        loaded = oot.load_subagent_state(state, "child-1")
        assert loaded is not None
        assert loaded.agent_id == "a1"
        assert loaded.finished is False
        assert loaded.requester_origin == {}

    def test_populated_round_trip(self):
        state: dict = {}
        original = oot.SubagentState(
            agent_id="a1",
            parent_session_key="parent-k",
            agent_span_id="span-1",
            subagent_label="reviewer",
            subagent_mode="run",
            expects_completion_msg=True,
            transcript_path="/p/child.jsonl",
            turn_start_offset=128,
            emitted_turns=2,
            started_at="2024-01-01T00:00:00Z",
            finished=True,
            end_reason="ok",
            end_outcome="ok",
            ended_at="2024-01-01T00:01:00Z",
            target_kind="subagent",
            send_farewell=True,
            requester_origin={"channel": "slack", "to": "user-1"},
        )
        oot.save_subagent_state(state, "child-1", original)
        loaded = oot.load_subagent_state(state, "child-1")
        assert loaded is not None
        assert loaded.parent_session_key == "parent-k"
        assert loaded.agent_span_id == "span-1"
        assert loaded.subagent_label == "reviewer"
        assert loaded.subagent_mode == "run"
        assert loaded.expects_completion_msg is True
        assert loaded.transcript_path == "/p/child.jsonl"
        assert loaded.turn_start_offset == 128
        assert loaded.emitted_turns == 2
        assert loaded.finished is True
        assert loaded.target_kind == "subagent"
        assert loaded.send_farewell is True
        assert loaded.requester_origin == {"channel": "slack", "to": "user-1"}
