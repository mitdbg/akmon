"""Issue #907 — the text path takes call params and names truncation.

Two failures rode together. `LiteLLMClient.complete()` built its
`litellm.completion` call from four fixed fields, so a caller could
neither cap the output (`max_tokens`) nor pass provider options
(`reasoning={"enabled": False}`); and when a reasoning model spent its
whole output budget on the reasoning trace and returned an empty body,
the client raised with content-filter wording, so a truncation read as
a content-filter trip.

These tests assert against the kwargs the client actually hands to
`litellm.completion` — not against client attributes, which would pass
even if the plumbing dropped them.
"""
from __future__ import annotations

import pytest

from okg.views.execution.errors import LLMCallFailed, LLMResponseTruncated
from okg.views.execution.llm import LiteLLMClient


class _Message:
    def __init__(self, content: str | None) -> None:
        self.content = content


class _Choice:
    def __init__(self, content: str | None, finish_reason: str | None) -> None:
        self.message = _Message(content)
        self.finish_reason = finish_reason


class _Resp:
    """Minimal stand-in for a litellm ModelResponse."""

    usage = None
    _hidden_params: dict = {}

    def __init__(
        self, content: str | None = "ok", finish_reason: str | None = "stop"
    ) -> None:
        self.choices = [_Choice(content, finish_reason)]


class _CapturingLiteLLM:
    """Stands in for the lazily-imported `litellm` module.

    Records the kwargs of each `completion()` call. Carries the three
    exception classes `_call_with_retry` builds its tenacity policy
    from, so the real retry path runs unchanged.
    """

    APIError = type("APIError", (Exception,), {})
    RateLimitError = type("RateLimitError", (Exception,), {})
    Timeout = type("Timeout", (Exception,), {})

    def __init__(self, resp: _Resp | None = None) -> None:
        self.calls: list[dict] = []
        self._resp = resp or _Resp()

    def completion(self, **kwargs):
        self.calls.append(kwargs)
        return self._resp


@pytest.fixture
def fake_litellm(monkeypatch):
    """Install a capturing stand-in for the litellm module."""

    def _install(resp: _Resp | None = None) -> _CapturingLiteLLM:
        fake = _CapturingLiteLLM(resp)
        monkeypatch.setattr(
            "okg.views.execution.llm._litellm", lambda: fake
        )
        return fake

    return _install


# ---------------------------------------------------------------------------
# req.text-call-params-configurable
# ---------------------------------------------------------------------------


def test_max_output_tokens_reaches_the_provider_call(fake_litellm) -> None:
    fake = fake_litellm()
    client = LiteLLMClient(model="test/model", max_output_tokens=8192)

    client.complete(system="sys", user="usr", fragment_id="frag")

    assert fake.calls, "expected litellm.completion to be called"
    assert fake.calls[0]["max_tokens"] == 8192


def test_unset_max_output_tokens_sends_no_max_tokens_key(fake_litellm) -> None:
    fake = fake_litellm()
    client = LiteLLMClient(model="test/model")

    client.complete(system="sys", user="usr", fragment_id="frag")

    # Absent, not None. A provider that reads max_tokens=None as a
    # zero cap would turn an unset knob into a truncation.
    assert "max_tokens" not in fake.calls[0], fake.calls[0]


def test_completion_kwargs_reach_the_provider_call(fake_litellm) -> None:
    fake = fake_litellm()
    client = LiteLLMClient(
        model="test/model",
        completion_kwargs={
            "reasoning": {"enabled": False},
            "extra_body": {"provider": {"sort": "price"}},
        },
    )

    client.complete(system="sys", user="usr", fragment_id="frag")

    call = fake.calls[0]
    assert call["reasoning"] == {"enabled": False}
    assert call["extra_body"] == {"provider": {"sort": "price"}}
    # The client's own fields survive alongside them.
    assert call["model"] == "test/model"
    assert len(call["messages"]) == 2


def test_completion_kwargs_may_not_override_client_owned_fields() -> None:
    for key, value in (
        ("model", "other/model"),
        ("messages", []),
        ("temperature", 0.9),
        ("timeout", 5),
        ("max_tokens", 16),
    ):
        with pytest.raises(ValueError) as excinfo:
            LiteLLMClient(model="test/model", completion_kwargs={key: value})
        assert key in str(excinfo.value)


def test_completion_kwargs_may_not_break_the_response_contract() -> None:
    """Keys the client doesn't set but whose semantics it depends on.

    `stream=True` returns a CustomStreamWrapper with no `.choices`, so
    the response read dies with an AttributeError pointing at the
    parser rather than at the caller's config. `n=3` bills for three
    completions and silently drops two, since the client reads
    `choices[0]`. `num_retries` nests litellm's retry loop inside the
    tenacity one and multiplies `max_retries` without saying so.
    """
    for key, value in (("stream", True), ("n", 3), ("num_retries", 5)):
        with pytest.raises(ValueError) as excinfo:
            LiteLLMClient(model="test/model", completion_kwargs={key: value})
        assert key in str(excinfo.value)


def test_non_positive_max_output_tokens_is_rejected() -> None:
    for bad in (0, -1):
        with pytest.raises(ValueError, match="max_output_tokens"):
            LiteLLMClient(model="test/model", max_output_tokens=bad)


def test_completion_kwargs_must_be_a_dict() -> None:
    with pytest.raises(ValueError, match="completion_kwargs"):
        LiteLLMClient(model="test/model", completion_kwargs=["reasoning"])


def test_extractor_config_reaches_the_built_client() -> None:
    """The knob is reachable from deployment enricher config."""
    from okg.substrate.library.enrichers.extraction.base import LLMExtractor

    class _Extractor(LLMExtractor):
        name = "test_extractor_907"
        emits_typed_subtype = "claim"

    ext = _Extractor(
        confidence_floor=0.5,
        llm_model="test/model",
        llm_max_output_tokens=8192,
        llm_completion_kwargs={"reasoning": {"enabled": False}},
        input_quarantine_max_attempts=3,
    )

    assert isinstance(ext.llm, LiteLLMClient)
    assert ext.llm._max_output_tokens == 8192
    assert ext.llm._completion_kwargs == {"reasoning": {"enabled": False}}


def test_extractor_call_params_without_a_model_are_refused() -> None:
    """Config that would reach nothing is a loud error, not a no-op."""
    from okg.substrate.library.enrichers.extraction.base import LLMExtractor

    class _Extractor(LLMExtractor):
        name = "test_extractor_907_nomodel"
        emits_typed_subtype = "claim"

    with pytest.raises(ValueError, match="llm_model"):
        _Extractor(confidence_floor=0.5, llm_max_output_tokens=8192)


def test_extractor_call_params_with_an_injected_client_are_refused() -> None:
    """An explicit `llm=` wins over `llm_model`, so the params are dropped.

    This is the shape the guard exists for: a deployment manifest
    carrying both `llm_model` and `llm_max_output_tokens`, instantiated
    by a harness that also injects a client. The extractor then builds
    nothing, the cap reaches nothing, and the operator reads the config
    as applied.
    """
    from okg.substrate.library.enrichers.extraction.base import LLMExtractor
    from okg.views.execution.llm import DeterministicStubLLM

    class _Extractor(LLMExtractor):
        name = "test_extractor_907_injected"
        emits_typed_subtype = "claim"

    with pytest.raises(ValueError, match="reach nothing"):
        _Extractor(
            confidence_floor=0.5,
            llm=DeterministicStubLLM(),
            llm_model="test/model",
            llm_max_output_tokens=8192,
            input_quarantine_max_attempts=3,
        )


# ---------------------------------------------------------------------------
# req.truncation-is-diagnosed
# ---------------------------------------------------------------------------


def _records_of(client: LiteLLMClient) -> list[dict]:
    records: list[dict] = []
    client._record = lambda **kw: records.append(kw)  # type: ignore[assignment]
    return records


def test_empty_truncated_response_raises_a_truncation_error(
    fake_litellm,
) -> None:
    fake_litellm(_Resp(content="", finish_reason="length"))
    client = LiteLLMClient(model="test/model", max_output_tokens=2048)
    records = _records_of(client)

    with pytest.raises(LLMResponseTruncated) as excinfo:
        client.complete(system="sys", user="usr", fragment_id="frag-t")

    message = str(excinfo.value)
    assert "truncated" in message.lower()
    assert "2048" in message, "the message must name the cap that was hit"
    assert "reasoning" in message, "the message must name the remedy"
    assert "content-filter" not in message

    assert records and records[-1]["success"] is False
    assert records[-1]["error_kind"] == "TruncatedProviderResponse"


@pytest.mark.parametrize("reason", ["length", "MAX_TOKENS", "max_tokens", "Length"])
def test_native_truncation_spellings_are_recognized(
    fake_litellm, reason: str
) -> None:
    """litellm normalizes most providers to `length`, not all of them.

    A provider or passthrough that surfaces its native spelling —
    Gemini's `MAX_TOKENS`, for one — must not fall back through to the
    content-filter diagnosis this module exists to avoid.
    """
    fake_litellm(_Resp(content="", finish_reason=reason))
    client = LiteLLMClient(model="test/model")
    records = _records_of(client)

    with pytest.raises(LLMResponseTruncated):
        client.complete(system="sys", user="usr", fragment_id="frag-s")

    assert records[-1]["error_kind"] == "TruncatedProviderResponse"


def test_truncated_success_row_carries_the_flag(fake_litellm) -> None:
    """A billed success must still be separable in the roll-up."""
    fake_litellm(_Resp(content='[{"a": 1', finish_reason="length"))
    client = LiteLLMClient(model="test/model")
    records = _records_of(client)

    client.complete(system="sys", user="usr", fragment_id="frag-x")

    assert records[-1]["success"] is True
    assert records[-1]["usage"]["extras"]["truncated"] is True


def test_untruncated_success_row_has_no_truncated_extra(fake_litellm) -> None:
    fake_litellm(_Resp(content="real answer", finish_reason="stop"))
    client = LiteLLMClient(model="test/model")
    records = _records_of(client)

    client.complete(system="sys", user="usr", fragment_id="frag-y")

    assert "truncated" not in records[-1]["usage"]["extras"]


def test_truncation_error_is_catchable_as_the_existing_failure(
    fake_litellm,
) -> None:
    """Callers holding a watermark on LLMCallFailed keep working."""
    fake_litellm(_Resp(content="", finish_reason="length"))
    client = LiteLLMClient(model="test/model")
    _records_of(client)

    with pytest.raises(LLMCallFailed):
        client.complete(system="sys", user="usr", fragment_id="frag-t2")


def test_empty_body_without_finish_reason_keeps_the_content_filter_diagnosis(
    fake_litellm,
) -> None:
    fake_litellm(_Resp(content="", finish_reason=None))
    client = LiteLLMClient(model="test/model")
    records = _records_of(client)

    with pytest.raises(LLMCallFailed) as excinfo:
        client.complete(system="sys", user="usr", fragment_id="frag-e")

    assert not isinstance(excinfo.value, LLMResponseTruncated)
    assert "content-filter" in str(excinfo.value)
    assert records[-1]["error_kind"] == "EmptyProviderResponse"


def test_truncated_non_empty_response_is_returned_and_marked(
    fake_litellm,
) -> None:
    fake_litellm(_Resp(content='[{"a": 1', finish_reason="length"))
    client = LiteLLMClient(model="test/model")
    records = _records_of(client)

    out = client.complete(system="sys", user="usr", fragment_id="frag-p")

    assert out == '[{"a": 1'
    assert records[-1]["success"] is True
    assert client.last_call_stats is not None
    assert client.last_call_stats["truncated"] is True


def test_normal_completion_is_not_marked_truncated(fake_litellm) -> None:
    fake_litellm(_Resp(content="real answer", finish_reason="stop"))
    client = LiteLLMClient(model="test/model")
    _records_of(client)

    out = client.complete(system="sys", user="usr", fragment_id="frag-n")

    assert out == "real answer"
    assert client.last_call_stats is not None
    assert client.last_call_stats["truncated"] is False
