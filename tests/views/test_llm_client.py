"""Unit tests for okg.views.execution.llm — Stage 1.4.

Covers the framework spec's "LLMClient Protocol",
"LiteLLMClient retries transient failures", and
"LiteLLMClient emits OpenTelemetry spans" requirements.

Tests use an in-memory OTel exporter (no network) and patch
``litellm.completion`` to control retry behavior. No live API
calls.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import litellm
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from okg.views.execution.errors import LLMCallFailed
from okg.views.execution.llm import (
    DeterministicStubLLM,
    LiteLLMClient,
    LLMClient,
)


# ---------------------------------------------------------------------------
# OTel test harness
# ---------------------------------------------------------------------------


@pytest.fixture
def span_exporter():
    """Install an in-memory OTel exporter for the duration of the
    test, yielding the exporter so tests can inspect captured spans.

    NB: ``trace.set_tracer_provider`` is global state. Tests that
    use this fixture should be robust to spans from other in-flight
    code paths in the same process. We mitigate by clearing the
    exporter between tests.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    # Reach into the LLMClient module's tracer to override its
    # provider. Setting the global provider after the module-level
    # tracer was instantiated has no effect — we replace the
    # tracer on the module instead.
    from okg.views.execution import llm as llm_mod

    original_tracer = llm_mod.tracer
    llm_mod.tracer = provider.get_tracer("test")
    try:
        yield exporter
    finally:
        llm_mod.tracer = original_tracer


def _build_chat_response(text: str) -> MagicMock:
    """Build a litellm-style ChatCompletion response object."""
    msg = MagicMock()
    msg.content = text
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


# ---------------------------------------------------------------------------
# Protocol satisfaction
# ---------------------------------------------------------------------------


class TestLLMClientProtocol:
    def test_litellm_client_satisfies_protocol(self):
        client = LiteLLMClient(model="claude-sonnet-4-5")
        assert isinstance(client, LLMClient)

    def test_stub_satisfies_protocol(self):
        assert isinstance(DeterministicStubLLM(), LLMClient)


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_empty_model_string_rejected(self):
        with pytest.raises(ValueError, match="non-empty model"):
            LiteLLMClient(model="")

    def test_default_temperature_is_zero(self):
        c = LiteLLMClient(model="x")
        assert c._temperature == 0.0

    def test_default_max_retries_is_three(self):
        c = LiteLLMClient(model="x")
        assert c._max_retries == 3

    def test_default_timeout_is_sixty_seconds(self):
        c = LiteLLMClient(model="x")
        assert c._timeout_s == 60.0

    def test_non_positive_timeout_rejected(self):
        with pytest.raises(ValueError, match="timeout_s"):
            LiteLLMClient(model="x", timeout_s=0)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestSuccessPath:
    def test_complete_returns_litellm_response_text(self):
        with patch("litellm.completion") as mock_call:
            mock_call.return_value = _build_chat_response("X")
            client = LiteLLMClient(model="claude-sonnet-4-5")
            out = client.complete(system="S", user="U")
            assert out == "X"

    def test_complete_calls_litellm_with_temp_zero_and_messages(self):
        with patch("litellm.completion") as mock_call:
            mock_call.return_value = _build_chat_response("ok")
            client = LiteLLMClient(model="claude-sonnet-4-5")
            client.complete(system="SYS", user="USR")
            kwargs = mock_call.call_args.kwargs
            assert kwargs["model"] == "claude-sonnet-4-5"
            assert kwargs["temperature"] == 0.0
            assert kwargs["timeout"] == 60.0
            assert kwargs["messages"] == [
                {"role": "system", "content": "SYS"},
                {"role": "user", "content": "USR"},
            ]

    def test_complete_passes_configured_timeout_to_litellm(self):
        with patch("litellm.completion") as mock_call:
            mock_call.return_value = _build_chat_response("ok")
            client = LiteLLMClient(model="x", timeout_s=2.5)
            client.complete(system="SYS", user="USR")
            assert mock_call.call_args.kwargs["timeout"] == 2.5


# ---------------------------------------------------------------------------
# Retry behavior
# ---------------------------------------------------------------------------


class TestRetry:
    def test_rate_limit_then_success_retries_until_ok(self):
        # Two RateLimitErrors then a success. tenacity's
        # exponential backoff has min=1s, which would slow this
        # test materially. We monkeypatch tenacity.wait_exponential
        # within the module to wait 0s.
        with (
            patch("litellm.completion") as mock_call,
            patch("okg.views.execution.llm.tenacity.wait_exponential",
                  return_value=tenacity_zero_wait()),
        ):
            mock_call.side_effect = [
                litellm.RateLimitError(
                    "rate limited", model="x", llm_provider="anthropic",
                ),
                litellm.RateLimitError(
                    "rate limited again", model="x", llm_provider="anthropic",
                ),
                _build_chat_response("ok"),
            ]
            client = LiteLLMClient(model="x", max_retries=3)
            out = client.complete(system="S", user="U")
            assert out == "ok"
            assert mock_call.call_count == 3

    def test_exhausted_retry_budget_raises_LLMCallFailed_with_cause(self):
        with (
            patch("litellm.completion") as mock_call,
            patch("okg.views.execution.llm.tenacity.wait_exponential",
                  return_value=tenacity_zero_wait()),
        ):
            mock_call.side_effect = litellm.RateLimitError(
                "always rate limited",
                model="x",
                llm_provider="anthropic",
            )
            client = LiteLLMClient(model="x", max_retries=3)
            with pytest.raises(LLMCallFailed) as excinfo:
                client.complete(
                    system="S", user="U", fragment_id="frag-1",
                )
            assert "frag-1" in str(excinfo.value)
            assert isinstance(excinfo.value.__cause__, litellm.RateLimitError)
            assert mock_call.call_count == 3

    def test_non_retryable_exception_does_not_retry_but_still_wraps(self):
        # A generic ValueError is NOT retryable. tenacity raises it
        # immediately — but our complete() wraps any escaped
        # exception in LLMCallFailed regardless. Verify wrap +
        # call-count-1.
        with patch("litellm.completion") as mock_call:
            mock_call.side_effect = ValueError("bad request")
            client = LiteLLMClient(model="x", max_retries=3)
            with pytest.raises(LLMCallFailed) as excinfo:
                client.complete(system="S", user="U")
            assert isinstance(excinfo.value.__cause__, ValueError)
            assert mock_call.call_count == 1


def tenacity_zero_wait():
    """Build a tenacity wait policy that returns 0 on every call."""
    import tenacity
    return tenacity.wait_fixed(0)


# ---------------------------------------------------------------------------
# Rate-limit visibility in okg.llm_calls (coherent-parallelism P1)
# ---------------------------------------------------------------------------


class TestRateLimitVisibility:
    """Rate saturation must be queryable from the recorded row.

    Before this, three 429s followed by a success produced one clean
    ``success=true`` row — the retried attempts wrote nothing, so rate
    limiting was diagnosable only by correlating timings.
    """

    def _capture(self):
        return patch(
            "okg.views.execution.llm.record_llm_call",
        )

    def test_survived_rate_limiting_stamps_hits_on_the_success_row(self):
        with (
            patch("litellm.completion") as mock_call,
            patch("okg.views.execution.llm.tenacity.wait_exponential",
                  return_value=tenacity_zero_wait()),
            self._capture() as mock_record,
        ):
            mock_call.side_effect = [
                litellm.RateLimitError(
                    "rate limited", model="x", llm_provider="anthropic",
                ),
                litellm.RateLimitError(
                    "rate limited again", model="x", llm_provider="anthropic",
                ),
                _build_chat_response("ok"),
            ]
            client = LiteLLMClient(
                model="x", max_retries=3,
                tracking_dsn="postgres://unused/db",
            )
            out = client.complete(system="S", user="U", caller="test:rl")
            assert out == "ok"
            assert mock_record.call_count == 1
            kwargs = mock_record.call_args.kwargs
            assert kwargs["success"] is True
            assert kwargs["extras"]["rate_limit_hits"] == 2

    def test_clean_success_carries_no_rate_limit_stamp(self):
        with (
            patch("litellm.completion") as mock_call,
            self._capture() as mock_record,
        ):
            mock_call.return_value = _build_chat_response("ok")
            client = LiteLLMClient(
                model="x", tracking_dsn="postgres://unused/db",
            )
            client.complete(system="S", user="U", caller="test:rl")
            extras = mock_record.call_args.kwargs["extras"]
            assert not extras or "rate_limit_hits" not in extras

    def test_terminal_rate_limit_row_carries_status_code_and_hits(self):
        with (
            patch("litellm.completion") as mock_call,
            patch("okg.views.execution.llm.tenacity.wait_exponential",
                  return_value=tenacity_zero_wait()),
            self._capture() as mock_record,
        ):
            mock_call.side_effect = litellm.RateLimitError(
                "always rate limited", model="x", llm_provider="anthropic",
            )
            client = LiteLLMClient(
                model="x", max_retries=3,
                tracking_dsn="postgres://unused/db",
            )
            with pytest.raises(LLMCallFailed):
                client.complete(system="S", user="U", caller="test:rl")
            kwargs = mock_record.call_args.kwargs
            assert kwargs["success"] is False
            assert kwargs["error_kind"] == "RateLimitError"
            # max_retries=3 → 2 retried hits before the terminal one;
            # the terminal 429 is classified by error_kind + status.
            assert kwargs["extras"]["rate_limit_hits"] == 2
            assert kwargs["extras"]["provider_status_code"] == 429

    def test_non_rate_limit_failure_carries_no_hits(self):
        with (
            self._capture() as mock_record,
            patch("litellm.completion") as mock_call,
        ):
            mock_call.side_effect = ValueError("bad request")
            client = LiteLLMClient(
                model="x", max_retries=3,
                tracking_dsn="postgres://unused/db",
            )
            with pytest.raises(LLMCallFailed):
                client.complete(system="S", user="U", caller="test:rl")
            kwargs = mock_record.call_args.kwargs
            extras = kwargs["extras"]
            assert not extras or "rate_limit_hits" not in extras


# ---------------------------------------------------------------------------
# OpenTelemetry instrumentation
# ---------------------------------------------------------------------------


class TestOTelSpans:
    def test_successful_call_emits_one_span_with_attributes(self, span_exporter):
        with patch("litellm.completion") as mock_call:
            mock_call.return_value = _build_chat_response("hi")
            client = LiteLLMClient(model="claude-sonnet-4-5")
            client.complete(system="S", user="U", fragment_id="row:1")

        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        s = spans[0]
        assert s.name == "view.llm_call"
        assert s.attributes["model"] == "claude-sonnet-4-5"
        assert s.attributes["fragment_id"] == "row:1"
        # input_chars = len("S") + len("U") = 2
        assert s.attributes["input_chars"] == 2
        # output_chars = len("hi") = 2
        assert s.attributes["output_chars"] == 2
        assert s.status.is_ok or s.status.is_unset

    def test_failed_call_records_exception_on_span(self, span_exporter):
        with (
            patch("litellm.completion") as mock_call,
            patch("okg.views.execution.llm.tenacity.wait_exponential",
                  return_value=tenacity_zero_wait()),
        ):
            mock_call.side_effect = litellm.RateLimitError(
                "rate limited", model="x", llm_provider="anthropic",
            )
            client = LiteLLMClient(model="x", max_retries=2)
            with pytest.raises(LLMCallFailed):
                client.complete(system="S", user="U")

        spans = span_exporter.get_finished_spans()
        assert len(spans) == 1
        # Span should record at least one exception event.
        assert any(e.name == "exception" for e in spans[0].events)


# ---------------------------------------------------------------------------
# Stub
# ---------------------------------------------------------------------------


class TestDeterministicStub:
    def test_pure_function_of_inputs(self):
        stub = DeterministicStubLLM()
        a = stub.complete(system="S", user="U")
        b = stub.complete(system="S", user="U")
        assert a == b

    def test_different_inputs_yield_different_outputs(self):
        stub = DeterministicStubLLM()
        assert stub.complete(system="S1", user="U") != stub.complete(
            system="S2", user="U"
        )
        assert stub.complete(system="S", user="U1") != stub.complete(
            system="S", user="U2"
        )

    def test_model_attribute(self):
        assert DeterministicStubLLM().model == "stub"
