"""Unit tests for opencode_realtime_trace.

Mirrors tests/test_claude_realtime_trace.py: covers pure helpers, the env
override layer, payload extractors, injected-block parsing, usage math, and
SessionState/ChildSessionState dataclass round-trips (including the legacy
`emitted_turns` key that load_session_state still accepts).
"""

from __future__ import annotations

import json

import pytest

from sii_opik_plugin.opencode import opencode_realtime_trace as ort


# ── Env helpers ──────────────────────────────────────────────────────────────

class TestEnvFirst:
    def test_returns_first_non_empty(self, monkeypatch):
        monkeypatch.setenv("A", "")
        monkeypatch.setenv("B", "value-b")
        monkeypatch.setenv("C", "value-c")
        assert ort._env_first("A", "B", "C") == "value-b"

    def test_returns_none_when_all_unset(self, monkeypatch):
        for name in ("X", "Y", "Z"):
            monkeypatch.delenv(name, raising=False)
        assert ort._env_first("X", "Y", "Z") is None

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("FOO", "")
        assert ort._env_first("FOO") is None


class TestApplyOpikEnvOverrides:
    def test_opik_url_is_mirrored_to_override(self, monkeypatch):
        monkeypatch.setenv("OPIK_URL", "https://primary.example")
        monkeypatch.delenv("OPIK_URL_OVERRIDE", raising=False)
        ort.apply_opik_env_overrides()
        import os
        assert os.environ["OPIK_URL_OVERRIDE"] == "https://primary.example"

    def test_api_key_override_promoted_when_target_unset(self, monkeypatch):
        monkeypatch.delenv("OPIK_API_KEY", raising=False)
        monkeypatch.setenv("OPIK_API_KEY_OVERRIDE", "key-override")
        ort.apply_opik_env_overrides()
        import os
        assert os.environ["OPIK_API_KEY"] == "key-override"

    def test_existing_api_key_is_not_clobbered(self, monkeypatch):
        monkeypatch.setenv("OPIK_API_KEY", "key-primary")
        monkeypatch.setenv("OPIK_API_KEY_OVERRIDE", "key-override")
        ort.apply_opik_env_overrides()
        import os
        assert os.environ["OPIK_API_KEY"] == "key-primary"


class TestRuntimeContextMetadata:
    _ALL = (
        "TB_TASK_ID", "TB_RUN_ID", "TB_DATASET", "TB_TRIAL_ID",
        "OPIK_TRIAL_NAME", "OPIK_PROJECT_NAME", "OC_OPIK_PROJECT",
        "OPIK_URL_OVERRIDE", "OPIK_URL",
    )

    def test_only_includes_non_empty_values(self, monkeypatch):
        for name in self._ALL:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("TB_TASK_ID", "task-1")
        monkeypatch.setenv("OPIK_URL", "https://primary.example")
        meta = ort.runtime_context_metadata()
        assert meta == {
            "tb_task_id": "task-1",
            "opik_url": "https://primary.example",
        }

    def test_project_name_falls_back_to_oc_var(self, monkeypatch):
        for name in self._ALL:
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("OC_OPIK_PROJECT", "oc-proj")
        assert ort.runtime_context_metadata() == {"opik_project_name": "oc-proj"}

    def test_empty_when_no_env(self, monkeypatch):
        for name in self._ALL:
            monkeypatch.delenv(name, raising=False)
        assert ort.runtime_context_metadata() == {}


# ── String / regex / time helpers ────────────────────────────────────────────

class TestStripModelDate:
    def test_strips_trailing_8_digit_date(self):
        assert ort.strip_model_date("claude-sonnet-4-5-20251022") == "claude-sonnet-4-5"

    def test_no_date_unchanged(self):
        assert ort.strip_model_date("gpt-5") == "gpt-5"

    def test_empty(self):
        assert ort.strip_model_date("") == ""


class TestEstimateTokens:
    def test_none_returns_zero(self):
        assert ort.estimate_tokens(None) == 0

    def test_empty_string_returns_zero(self):
        assert ort.estimate_tokens("") == 0

    def test_short_string_returns_one(self):
        assert ort.estimate_tokens("a") == 1

    def test_long_string_uses_quarter_length(self):
        assert ort.estimate_tokens("a" * 100) == 25

    def test_non_string_is_serialized(self):
        assert ort.estimate_tokens({"k": "v"}) > 0
        assert ort.estimate_tokens([1, 2, 3]) > 0


class TestParseTs:
    def test_parses_iso_with_offset(self):
        ts = ort.parse_ts("2024-01-02T03:04:05+00:00")
        assert ts.year == 2024 and ts.hour == 3

    def test_converts_zulu_suffix(self):
        ts = ort.parse_ts("2024-01-02T03:04:05Z")
        assert ts.tzinfo is not None
        assert ts.utcoffset().total_seconds() == 0

    @pytest.mark.parametrize("value", ["", "not-a-date"])
    def test_unparseable_falls_back_to_aware_now(self, value):
        ts = ort.parse_ts(value)
        assert ts.tzinfo is not None


class TestMsToIso:
    def test_zero_and_none_fall_back_to_now(self):
        # Falsy epoch (0 / None) means "no timestamp" → an aware now() string.
        assert ort.ms_to_iso(0).endswith("+00:00")
        assert ort.ms_to_iso(None).endswith("+00:00")

    def test_known_epoch_ms(self):
        # 1_000_000 ms == 1000 s past the epoch.
        assert ort.ms_to_iso(1_000_000) == "1970-01-01T00:16:40+00:00"


class TestStringifyContent:
    def test_none_is_empty(self):
        assert ort.stringify_content(None) == ""

    def test_str_passthrough(self):
        assert ort.stringify_content("hi") == "hi"

    def test_list_joins_text_items_and_jsonifies_others(self):
        out = ort.stringify_content([
            {"type": "text", "text": "hello"},
            {"type": "image", "url": "x"},
        ])
        assert out == 'hello\n' + json.dumps({"type": "image", "url": "x"})

    def test_dict_is_pretty_json(self):
        assert ort.stringify_content({"k": "v"}) == json.dumps(
            {"k": "v"}, ensure_ascii=False, indent=2
        )


# ── Usage math ───────────────────────────────────────────────────────────────

class TestBuildUsageMetadata:
    def test_combines_cache_into_input(self):
        meta = ort.build_usage_metadata({
            "input_tokens": 100,
            "output_tokens": 200,
            "cache_read_input_tokens": 50,
        })
        assert meta == {
            "billed_input_tokens": 100,
            "input_tokens": 150,
            "output_tokens": 200,
            "total_tokens": 350,
            "output_token_details": {"reasoning": 0},
            "input_token_details": {"cache_read": 50, "cache_creation": 0},
        }

    def test_empty_returns_none(self):
        assert ort.build_usage_metadata({}) is None

    def test_all_zero_returns_none(self):
        assert ort.build_usage_metadata({"input_tokens": 0, "output_tokens": 0}) is None


class TestTokensFromMsg:
    def test_reads_nested_token_shape(self):
        assert ort.tokens_from_msg({
            "tokens": {
                "input": 10, "output": 20, "reasoning": 5,
                "cache": {"read": 3, "write": 4},
            }
        }) == {
            "input_tokens": 10,
            "output_tokens": 20,
            "reasoning_tokens": 5,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": 4,
        }

    def test_missing_tokens_returns_all_zero(self):
        assert ort.tokens_from_msg({}) == {
            "input_tokens": 0,
            "output_tokens": 0,
            "reasoning_tokens": 0,
            "cache_read_input_tokens": 0,
            "cache_creation_input_tokens": 0,
        }


class TestAccumulateUsage:
    def test_adds_into_target_filling_missing_keys(self):
        target: dict = {"input_tokens": 5}
        ort.accumulate_usage(target, {"input_tokens": 10, "output_tokens": 2})
        assert target["input_tokens"] == 15
        assert target["output_tokens"] == 2
        assert target["reasoning_tokens"] == 0


# ── Injected-block parsing ───────────────────────────────────────────────────

class TestInjectedBlocks:
    def test_has_injected_block_detects_tag(self):
        assert ort.has_injected_block("<system-reminder>x</system-reminder>") is True
        assert ort.has_injected_block("plain text") is False

    def test_extract_strips_and_captures_mixed_content(self):
        stripped, blocks = ort.extract_injected_blocks(
            "Fix the bug\n<system-reminder>X</system-reminder>"
        )
        assert stripped == "Fix the bug"
        assert blocks == [{"tag": "system-reminder", "content": "X"}]

    def test_extract_no_tags_passthrough(self):
        assert ort.extract_injected_blocks("just text") == ("just text", [])

    def test_tool_output_preserves_reminders_inside_content(self):
        text = (
            "<system-reminder>OUT</system-reminder>"
            "<content>file <system-reminder>IN</system-reminder> data</content>"
        )
        cleaned, blocks = ort.extract_injected_blocks_from_tool_output(text)
        # The reminder outside <content> is lifted; the one inside file bytes stays.
        assert blocks == [{"tag": "system-reminder", "content": "OUT"}]
        assert "<system-reminder>IN</system-reminder>" in cleaned


# ── Payload extractors ───────────────────────────────────────────────────────

class TestDeepGet:
    def test_walks_nested_dicts(self):
        assert ort._deep_get({"session": {"id": "abc"}}, "session", "id") == "abc"

    def test_missing_path_returns_none(self):
        assert ort._deep_get({"session": {}}, "session", "id") is None

    def test_non_dict_midway_returns_none(self):
        assert ort._deep_get({"session": "x"}, "session", "id") is None


class TestHookEventName:
    def test_event_key_wins_and_is_lowercased(self):
        assert ort.hook_event_name({"event": "Session_Start"}) == "session_start"

    def test_falls_back_through_known_keys(self):
        assert ort.hook_event_name({"hookEventName": "Stop"}) == "stop"
        assert ort.hook_event_name({"type": "tool_result"}) == "tool_result"

    def test_empty_payload_is_empty_string(self):
        assert ort.hook_event_name({}) == ""


class TestEventTimestamp:
    def test_top_level_timestamp_key(self):
        assert ort.event_timestamp({"timestamp": "2024-01-01T00:00:00Z"}) == "2024-01-01T00:00:00Z"

    def test_nested_session_started_at(self):
        assert ort.event_timestamp(
            {"session": {"started_at": "2024-02-02T00:00:00Z"}}
        ) == "2024-02-02T00:00:00Z"

    def test_missing_returns_none(self):
        assert ort.event_timestamp({}) is None

    def test_empty_string_skipped(self):
        assert ort.event_timestamp(
            {"timestamp": "", "time": "2024-03-03T00:00:00Z"}
        ) == "2024-03-03T00:00:00Z"


class TestExtractSessionId:
    def test_snake_case_key(self):
        assert ort.extract_session_id({"session_id": "abc"}) == "abc"

    def test_camel_case_key(self):
        assert ort.extract_session_id({"sessionId": "abc"}) == "abc"

    def test_nested_session_object(self):
        assert ort.extract_session_id({"session": {"id": "abc"}}) == "abc"

    def test_env_fallback(self, monkeypatch):
        monkeypatch.delenv("OPENCODE_SESSION_ID", raising=False)
        monkeypatch.setenv("OC_SESSION_ID", "from-env")
        assert ort.extract_session_id({}) == "from-env"

    def test_missing_returns_none(self, monkeypatch):
        monkeypatch.delenv("OPENCODE_SESSION_ID", raising=False)
        monkeypatch.delenv("OC_SESSION_ID", raising=False)
        assert ort.extract_session_id({}) is None


# ── Event classification ─────────────────────────────────────────────────────

class TestEventClassification:
    @pytest.mark.parametrize("name", ["stop", "session_end", "agent_end", "completed"])
    def test_final_events(self, name):
        assert ort.event_is_final(name) is True

    def test_final_events_are_also_flush(self):
        assert ort.event_is_flush("stop") is True

    @pytest.mark.parametrize("name", ["after_tool_call", "tool_error", "session_idle"])
    def test_flush_only_events(self, name):
        assert ort.event_is_flush(name) is True
        assert ort.event_is_final(name) is False

    @pytest.mark.parametrize("name", ["session_start", "user_prompt", "message.user"])
    def test_start_events(self, name):
        assert ort.event_is_start(name) is True

    def test_unknown_event_is_neither(self):
        assert ort.event_is_final("noop") is False
        assert ort.event_is_flush("noop") is False
        assert ort.event_is_start("noop") is False


# ── Deterministic IDs ────────────────────────────────────────────────────────

class TestDeterministicSpanId:
    def test_deterministic_and_valid_uuid(self):
        import uuid
        a = ort.deterministic_span_id("trace-1", "opencode-session-root")
        b = ort.deterministic_span_id("trace-1", "opencode-session-root")
        assert a == b
        assert uuid.UUID(a).version == 7

    def test_different_parts_differ(self):
        assert ort.deterministic_span_id("t", "a") != ort.deterministic_span_id("t", "b")

    def test_root_span_id_is_derived_from_trace(self):
        session = ort.SessionState(session_id="s", trace_id="trace-1")
        assert ort.session_root_span_id(session) == ort.deterministic_span_id(
            "trace-1", "opencode-session-root"
        )


# ── State dataclass round-trips ──────────────────────────────────────────────

class TestSessionStateRoundTrip:
    def test_default_round_trip(self):
        state: dict = {}
        ort.save_session_state(state, "s1", ort.SessionState(session_id="s1"))
        loaded = ort.load_session_state(state, "s1")
        assert loaded.session_id == "s1"
        assert loaded.trace_created is False
        assert loaded.emitted_turn_count == 0
        assert loaded.session_models == []
        assert loaded.child_sessions == {}

    def test_populated_round_trip(self):
        state: dict = {}
        original = ort.SessionState(
            session_id="s1",
            trace_id="t-1",
            root_span_id="root-1",
            trace_created=True,
            trace_name="my-trace",
            emitted_turn_count=3,
            emitted_turn_hashes=["h1", "h2"],
            session_api_billed_input=1000,
            session_total_llm_calls=4,
            session_models=["gpt-5"],
        )
        ort.save_session_state(state, "s1", original)
        loaded = ort.load_session_state(state, "s1")
        assert loaded.trace_id == "t-1"
        assert loaded.root_span_id == "root-1"
        assert loaded.trace_created is True
        assert loaded.trace_name == "my-trace"
        assert loaded.emitted_turn_count == 3
        assert loaded.emitted_turn_hashes == ["h1", "h2"]
        assert loaded.session_api_billed_input == 1000
        assert loaded.session_total_llm_calls == 4
        assert loaded.session_models == ["gpt-5"]

    def test_missing_session_returns_default(self):
        assert ort.load_session_state({}, "absent").emitted_turn_count == 0

    def test_legacy_emitted_turns_key_is_accepted(self):
        """Regression: state written before the field was renamed to
        emitted_turn_count must still load via the emitted_turns fallback."""
        state = {"sessions": {"s1": {"emitted_turns": 7}}}
        loaded = ort.load_session_state(state, "s1")
        assert loaded.emitted_turn_count == 7


class TestChildSessionStateRoundTrip:
    def test_child_sessions_round_trip(self):
        state: dict = {}
        original = ort.SessionState(
            session_id="parent",
            child_sessions={
                "child-1": ort.ChildSessionState(
                    child_session_id="child-1",
                    parent_tool_call_id="tool-9",
                    agent_span_id="span-9",
                    emitted_turn_count=2,
                    emitted_turn_hashes=["c1"],
                    child_total_llm_calls=3,
                    child_tool_error=1,
                )
            },
        )
        ort.save_session_state(state, "parent", original)
        loaded = ort.load_session_state(state, "parent")
        child = loaded.child_sessions["child-1"]
        assert child.parent_tool_call_id == "tool-9"
        assert child.agent_span_id == "span-9"
        assert child.emitted_turn_count == 2
        assert child.emitted_turn_hashes == ["c1"]
        assert child.child_total_llm_calls == 3
        assert child.child_tool_error == 1

    def test_non_dict_child_entry_is_skipped(self):
        state = {"sessions": {"p": {"child_sessions": {"bad": "not-a-dict"}}}}
        loaded = ort.load_session_state(state, "p")
        assert loaded.child_sessions == {}
