"""Unit tests for claude_realtime_trace.

Covers pure helpers, payload extractors, state dataclass round-trips, and
forward-compat regression for keys removed in the dead-code cleanup
(SessionState.span_ids, SubagentState.deferred_create).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from sii_opik_plugin.claude_code import claude_realtime_trace as crt


# ── Env helpers ──────────────────────────────────────────────────────────────

class TestEnvFirst:
    def test_returns_first_non_empty(self, monkeypatch):
        monkeypatch.setenv("A", "")
        monkeypatch.setenv("B", "value-b")
        monkeypatch.setenv("C", "value-c")
        assert crt._env_first("A", "B", "C") == "value-b"

    def test_returns_none_when_all_unset(self, monkeypatch):
        for name in ("X", "Y", "Z"):
            monkeypatch.delenv(name, raising=False)
        assert crt._env_first("X", "Y", "Z") is None

    def test_empty_string_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("FOO", "")
        assert crt._env_first("FOO") is None


class TestApplyOpikEnvOverrides:
    def test_override_var_promoted_when_target_unset(self, monkeypatch):
        monkeypatch.delenv("OPIK_URL", raising=False)
        monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://override.example")
        crt.apply_opik_env_overrides()
        assert __import__("os").environ["OPIK_URL"] == "https://override.example"

    def test_existing_target_is_not_clobbered(self, monkeypatch):
        monkeypatch.setenv("OPIK_URL", "https://primary.example")
        monkeypatch.setenv("OPIK_URL_OVERRIDE", "https://override.example")
        crt.apply_opik_env_overrides()
        assert __import__("os").environ["OPIK_URL"] == "https://primary.example"


class TestRuntimeContextMetadata:
    def test_only_includes_non_empty_values(self, monkeypatch):
        for name in (
            "TB_TASK_ID", "TB_RUN_ID", "TB_DATASET", "TB_TRIAL_ID",
            "OPIK_PROJECT_NAME", "CC_OPIK_PROJECT",
            "OPIK_URL_OVERRIDE", "OPIK_URL",
        ):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv("TB_TASK_ID", "task-1")
        monkeypatch.setenv("OPIK_URL", "https://primary.example")
        meta = crt.runtime_context_metadata()
        assert meta == {
            "tb_task_id": "task-1",
            "opik_url": "https://primary.example",
        }

    def test_empty_when_no_env(self, monkeypatch):
        for name in (
            "TB_TASK_ID", "TB_RUN_ID", "TB_DATASET", "TB_TRIAL_ID",
            "OPIK_PROJECT_NAME", "CC_OPIK_PROJECT",
            "OPIK_URL_OVERRIDE", "OPIK_URL",
        ):
            monkeypatch.delenv(name, raising=False)
        assert crt.runtime_context_metadata() == {}


# ── Argv parsing ─────────────────────────────────────────────────────────────

class TestExtractPayloadFileArg:
    def test_present(self):
        assert crt._extract_payload_file_arg(
            ["cmd", "SessionEnd", "--payload-file", "/tmp/p.json"]
        ) == "/tmp/p.json"

    def test_absent(self):
        assert crt._extract_payload_file_arg(["cmd", "SessionEnd"]) is None

    def test_flag_at_end_with_no_value(self):
        assert crt._extract_payload_file_arg(["cmd", "--payload-file"]) is None

    def test_empty_value_returns_none(self):
        assert crt._extract_payload_file_arg(
            ["cmd", "--payload-file", "   "]
        ) is None


# ── String / regex helpers ───────────────────────────────────────────────────

class TestStripModelDate:
    def test_strips_trailing_8_digit_date(self):
        assert crt.strip_model_date("claude-sonnet-4-5-20251022") == "claude-sonnet-4-5"

    def test_no_date_unchanged(self):
        assert crt.strip_model_date("claude-sonnet-4-5") == "claude-sonnet-4-5"

    def test_empty(self):
        assert crt.strip_model_date("") == ""


class TestEstimateTokens:
    def test_none_returns_zero(self):
        assert crt.estimate_tokens(None) == 0

    def test_empty_string_returns_zero(self):
        assert crt.estimate_tokens("") == 0

    def test_short_string_returns_one(self):
        # max(1, len // 4) ensures non-empty content costs at least one token
        assert crt.estimate_tokens("a") == 1

    def test_long_string_uses_quarter_length(self):
        assert crt.estimate_tokens("a" * 100) == 25

    def test_non_string_is_supported(self):
        # Non-string content is serialized first; we only care that it
        # produces a positive estimate, not the exact arithmetic.
        assert crt.estimate_tokens({"k": "v"}) > 0
        assert crt.estimate_tokens([1, 2, 3]) > 0


class TestExtractAgentIdFromResult:
    def test_found(self):
        assert crt.extract_agent_id_from_result(
            "Subagent started: agentId: abc-123 status: ok"
        ) == "abc-123"

    def test_not_found(self):
        assert crt.extract_agent_id_from_result("no agent here") is None


class TestExtractSubagentUsage:
    def test_parses_usage_block(self):
        content = (
            "preamble\n<usage>\ninput_tokens: 100\noutput_tokens: 200\n"
            "cache_read_input_tokens: 50\n</usage>\nepilogue"
        )
        assert crt.extract_subagent_usage(content) == {
            "input_tokens": 100,
            "output_tokens": 200,
            "cache_read_input_tokens": 50,
        }

    def test_non_int_value_kept_as_string(self):
        content = "<usage>\nstop_reason: end_turn\n</usage>"
        assert crt.extract_subagent_usage(content) == {"stop_reason": "end_turn"}

    def test_missing_block_returns_empty(self):
        assert crt.extract_subagent_usage("no usage tag") == {}


class TestIsContinuationMessage:
    def test_task_notification_matches(self):
        assert crt._is_continuation_message("<task-notification>foo</task-notification>") is True

    def test_system_reminder_matches(self):
        assert crt._is_continuation_message("<system-reminder>foo</system-reminder>") is True

    def test_leading_whitespace_allowed(self):
        assert crt._is_continuation_message("\n  <system-reminder>foo</system-reminder>") is True

    def test_regular_user_text_does_not_match(self):
        assert crt._is_continuation_message("Hello, please run the tests") is False

    def test_unknown_tag_does_not_match(self):
        assert crt._is_continuation_message("<other-tag>foo</other-tag>") is False


class TestInferMessageRole:
    def test_explicit_role_wins(self):
        assert crt._infer_message_role({"type": "user"}, {"role": "assistant"}) == "assistant"

    def test_data_type_used_when_role_missing(self):
        assert crt._infer_message_role({"type": "user"}, {}) == "user"

    def test_tool_result_content_implies_user(self):
        msg = {"content": [{"type": "tool_result", "content": "ok"}]}
        assert crt._infer_message_role({}, msg) == "user"

    def test_assistant_like_content_with_id_implies_assistant(self):
        msg = {"id": "msg_1", "content": [{"type": "text", "text": "hi"}]}
        assert crt._infer_message_role({}, msg) == "assistant"

    def test_string_content_with_id_implies_assistant(self):
        assert crt._infer_message_role({}, {"id": "msg_1", "content": "hello"}) == "assistant"

    def test_no_signal_returns_empty(self):
        assert crt._infer_message_role({}, {}) == ""


class TestParseTs:
    def test_parses_iso_with_offset(self):
        ts = crt.parse_ts("2024-01-02T03:04:05+00:00")
        assert ts.year == 2024 and ts.hour == 3

    def test_converts_zulu_suffix(self):
        ts = crt.parse_ts("2024-01-02T03:04:05Z")
        assert ts.tzinfo is not None
        assert ts.utcoffset().total_seconds() == 0

    @pytest.mark.parametrize("value", ["", "not-a-date"])
    def test_unparseable_falls_back_to_aware_now(self, value):
        ts = crt.parse_ts(value)
        assert ts.tzinfo is not None  # UTC fallback


# ── Payload extractors ───────────────────────────────────────────────────────

class TestExtractSessionAndTranscript:
    def test_snake_case_keys(self, tmp_path):
        transcript = tmp_path / "session.jsonl"
        transcript.touch()
        sid, path = crt.extract_session_and_transcript({
            "session_id": "abc",
            "transcript_path": str(transcript),
        })
        assert sid == "abc"
        assert path == transcript.resolve()

    def test_camel_case_keys(self, tmp_path):
        transcript = tmp_path / "session.jsonl"
        transcript.touch()
        sid, path = crt.extract_session_and_transcript({
            "sessionId": "abc",
            "transcriptPath": str(transcript),
        })
        assert sid == "abc"
        assert path == transcript.resolve()

    def test_nested_session_object(self, tmp_path):
        transcript = tmp_path / "session.jsonl"
        transcript.touch()
        sid, path = crt.extract_session_and_transcript({
            "session": {"id": "abc"},
            "transcript": {"path": str(transcript)},
        })
        assert sid == "abc"
        assert path == transcript.resolve()

    def test_missing_returns_none(self):
        assert crt.extract_session_and_transcript({}) == (None, None)


class TestExtractAgentInfo:
    def test_snake_case(self):
        assert crt.extract_agent_info({
            "agent_id": "a1",
            "agent_type": "reviewer",
            "agent_transcript_path": "/tmp/t.jsonl",
        }) == ("a1", "reviewer", "/tmp/t.jsonl")

    def test_camel_case(self):
        assert crt.extract_agent_info({
            "agentId": "a1",
            "agentType": "reviewer",
            "agentTranscriptPath": "/tmp/t.jsonl",
        }) == ("a1", "reviewer", "/tmp/t.jsonl")

    def test_missing_transcript_is_none(self):
        assert crt.extract_agent_info({"agent_id": "a1"}) == ("a1", "", None)

    def test_empty_payload(self):
        assert crt.extract_agent_info({}) == ("", "", None)


class TestExtractPrompt:
    def test_prompt_key(self):
        assert crt.extract_prompt({"prompt": "hi"}) == "hi"

    def test_user_prompt_fallback(self):
        assert crt.extract_prompt({"user_prompt": "hi"}) == "hi"

    def test_empty(self):
        assert crt.extract_prompt({}) == ""


class TestExtractCustomInstructions:
    def test_snake_case(self):
        assert crt.extract_custom_instructions({"custom_instructions": "x"}) == "x"

    def test_camel_case(self):
        assert crt.extract_custom_instructions({"customInstructions": "x"}) == "x"

    def test_empty(self):
        assert crt.extract_custom_instructions({}) == ""


class TestHookEventName:
    def test_payload_takes_precedence_over_argv(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cmd", "Stop"])
        assert crt.hook_event_name({"hook_event_name": "PostToolUse"}) == "PostToolUse"

    def test_camel_case_payload_key(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cmd"])
        assert crt.hook_event_name({"hookEventName": "Stop"}) == "Stop"

    def test_argv_fallback(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cmd", "SessionEnd"])
        assert crt.hook_event_name({}) == "SessionEnd"

    def test_tool_payload_implies_post_tool_use(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cmd"])
        assert crt.hook_event_name({"tool_name": "Read"}) == "PostToolUse"

    def test_final_default_is_stop(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cmd"])
        assert crt.hook_event_name({}) == "Stop"


class TestEventTimestamp:
    def test_top_level_timestamp_key(self):
        assert crt.event_timestamp({"timestamp": "2024-01-01T00:00:00Z"}) == "2024-01-01T00:00:00Z"

    def test_nested_session_started_at(self):
        assert crt.event_timestamp({"session": {"started_at": "2024-02-02T00:00:00Z"}}) == "2024-02-02T00:00:00Z"

    def test_missing_returns_none(self):
        assert crt.event_timestamp({}) is None

    def test_empty_string_skipped(self):
        assert crt.event_timestamp({"timestamp": "", "time": "2024-03-03T00:00:00Z"}) == "2024-03-03T00:00:00Z"


# ── State key ────────────────────────────────────────────────────────────────

class TestStateKey:
    def test_deterministic(self, tmp_path):
        transcript = tmp_path / "s.jsonl"
        k1 = crt.state_key("sess-1", transcript)
        k2 = crt.state_key("sess-1", transcript)
        assert k1 == k2
        assert len(k1) == 64  # sha256 hex digest

    def test_different_session_id(self, tmp_path):
        transcript = tmp_path / "s.jsonl"
        assert crt.state_key("a", transcript) != crt.state_key("b", transcript)

    def test_different_transcript_path(self, tmp_path):
        a = tmp_path / "a.jsonl"
        b = tmp_path / "b.jsonl"
        assert crt.state_key("sess", a) != crt.state_key("sess", b)


# ── State dataclass round-trips ──────────────────────────────────────────────

class TestSessionStateRoundTrip:
    def test_default_round_trip(self):
        state: dict = {}
        original = crt.SessionState()
        crt.save_session_state(state, "k", original)
        loaded = crt.load_session_state(state, "k")
        assert loaded.turn_start_offset == 0
        assert loaded.emitted_turns == 0
        assert loaded.trace_created is False
        assert loaded.session_models == []

    def test_populated_round_trip(self):
        state: dict = {}
        original = crt.SessionState(
            turn_start_offset=512,
            emitted_turns=3,
            trace_created=True,
            trace_id="t-1",
            trace_name="my-trace",
            trace_finalized=False,
            session_api_billed_input=1000,
            session_api_billed_output=500,
            session_total_llm_calls=4,
            session_models=["claude-sonnet-4-5"],
        )
        crt.save_session_state(state, "k", original)
        loaded = crt.load_session_state(state, "k")
        assert loaded.turn_start_offset == 512
        assert loaded.emitted_turns == 3
        assert loaded.trace_created is True
        assert loaded.trace_id == "t-1"
        assert loaded.trace_name == "my-trace"
        assert loaded.session_api_billed_input == 1000
        assert loaded.session_total_llm_calls == 4
        assert loaded.session_models == ["claude-sonnet-4-5"]

    def test_missing_key_returns_default(self):
        assert crt.load_session_state({}, "absent").turn_start_offset == 0

    def test_legacy_span_ids_key_is_ignored(self):
        """Regression: state files persisted before span_ids was removed must
        still load — the now-unknown key should be silently dropped."""
        state = {
            "k": {
                "turn_start_offset": 42,
                "emitted_turns": 1,
                "span_ids": {"some": "id"},  # removed field, must not break load
            }
        }
        loaded = crt.load_session_state(state, "k")
        assert loaded.turn_start_offset == 42
        assert loaded.emitted_turns == 1


class TestSubagentStateRoundTrip:
    @pytest.fixture
    def isolated_agents_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(crt, "AGENTS_DIR", tmp_path / "agents")
        return tmp_path / "agents"

    def test_default_round_trip(self, isolated_agents_dir):
        original = {"a1": crt.SubagentState(agent_id="a1")}
        crt.save_subagent_states("key", original)
        loaded = crt.load_subagent_states("key")
        assert "a1" in loaded
        assert loaded["a1"].agent_id == "a1"
        assert loaded["a1"].agent_type == ""
        assert loaded["a1"].finished is False

    def test_populated_round_trip(self, isolated_agents_dir):
        original = {
            "a1": crt.SubagentState(
                agent_id="a1",
                agent_type="reviewer",
                agent_span_id="span-1",
                transcript_path="/tmp/t.jsonl",
                turn_start_offset=128,
                emitted_turns=2,
                started_at="2024-01-01T00:00:00Z",
                finished=True,
                parent_span_id="parent-1",
                last_end_ts="2024-01-01T00:01:00Z",
            )
        }
        crt.save_subagent_states("key", original)
        loaded = crt.load_subagent_states("key")
        sa = loaded["a1"]
        assert sa.agent_type == "reviewer"
        assert sa.agent_span_id == "span-1"
        assert sa.transcript_path == "/tmp/t.jsonl"
        assert sa.turn_start_offset == 128
        assert sa.emitted_turns == 2
        assert sa.finished is True
        assert sa.parent_span_id == "parent-1"
        assert sa.last_end_ts == "2024-01-01T00:01:00Z"

    def test_load_missing_file_returns_empty(self, isolated_agents_dir):
        assert crt.load_subagent_states("never-saved") == {}

    def test_delete_removes_file(self, isolated_agents_dir):
        crt.save_subagent_states("key", {"a1": crt.SubagentState(agent_id="a1")})
        path = crt._subagent_state_path("key")
        assert path.exists()
        crt.delete_subagent_states("key")
        assert not path.exists()

    def test_legacy_deferred_create_key_is_ignored(self, isolated_agents_dir):
        """Regression: persisted subagent files with the dropped
        deferred_create key must still load cleanly."""
        path = crt._subagent_state_path("key")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "a1": {
                "agent_id": "a1",
                "agent_type": "reviewer",
                "agent_span_id": "",
                "transcript_path": "",
                "turn_start_offset": 0,
                "emitted_turns": 0,
                "started_at": "",
                "finished": False,
                "deferred_create": True,  # removed field, must not break load
            }
        }))
        loaded = crt.load_subagent_states("key")
        assert "a1" in loaded
        assert loaded["a1"].agent_type == "reviewer"
