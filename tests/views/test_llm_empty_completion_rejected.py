"""Regression: an empty-string completion must be rejected, not billed.

Issue #693a. A provider can return a 200 response whose text body is the
empty string ``""`` (a content-filter trip that does not raise). The old
guard was ``if content is None``, so an empty string slipped through: it was
recorded as ``success=True`` (billed) and returned to the caller, which then
cached it as a stable fragment. The fix rejects any falsy completion
(``None`` or ``""``) loudly, records it as a failure, and never returns it.
"""
from __future__ import annotations

import pytest

from okg.views.execution.errors import LLMCallFailed
from okg.views.execution.llm import LiteLLMClient


class _Resp:
    """Minimal stand-in for a litellm ModelResponse."""

    usage = None
    _hidden_params: dict = {}


def _make_client(returned: str | None, records: list[dict]) -> LiteLLMClient:
    client = LiteLLMClient(model="test/model")

    def _fake_call_with_retry(system: str, user: str, retry_stats=None):
        return returned, _Resp()

    def _fake_record(**kwargs):
        records.append(kwargs)

    client._call_with_retry = _fake_call_with_retry  # type: ignore[assignment]
    client._record = _fake_record  # type: ignore[assignment]
    return client


def test_empty_string_completion_raises_and_is_not_billed() -> None:
    records: list[dict] = []
    client = _make_client("", records)

    with pytest.raises(LLMCallFailed) as excinfo:
        client.complete(system="sys", user="usr", fragment_id="frag-1")

    # Loud: the raised error names the model and the fragment/request.
    message = str(excinfo.value)
    assert "test/model" in message
    assert "frag-1" in message

    # Never recorded as a billed success.
    assert records, "expected a failure record to be written"
    assert all(r["success"] is False for r in records), records
    assert any(r["error_kind"] for r in records)


def test_none_completion_still_raises() -> None:
    records: list[dict] = []
    client = _make_client(None, records)

    with pytest.raises(LLMCallFailed):
        client.complete(system="sys", user="usr", fragment_id="frag-2")

    assert records
    assert all(r["success"] is False for r in records)


def test_nonempty_completion_is_returned_and_billed() -> None:
    records: list[dict] = []
    client = _make_client("real answer", records)

    out = client.complete(system="sys", user="usr", fragment_id="frag-3")

    assert out == "real answer"
    assert records
    assert records[-1]["success"] is True
