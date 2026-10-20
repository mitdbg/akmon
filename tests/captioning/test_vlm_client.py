"""Tests for VLMClient — req.vlm-client.

Verifies:
1. complete_image() constructs a litellm message with an image_url
   content block containing a base64 data URI.
2. Transient APIError triggers retry up to max_retries.
3. tracking_dsn=None skips record_llm_call.
"""

from __future__ import annotations

import base64
from unittest.mock import MagicMock, patch

import pytest

from okg.views.execution.llm import (
    LLMCallFailed,
    LLMResponseTruncated,
    StubVLMClient,
    VLMClient,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TINY_PNG = (
    b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
    b"\x00\x00\x00\x01\x08\x02\x00\x00\x00\x90wS\xde\x00\x00"
    b"\x00\x0cIDATx\x9cc\xf8\x0f\x00\x00\x01\x01\x00\x05\x18"
    b"\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
)


def _make_mock_resp(content: str = "caption text") -> MagicMock:
    resp = MagicMock()
    resp.choices[0].message.content = content
    resp.usage.prompt_tokens = 10
    resp.usage.completion_tokens = 5
    resp.usage.total_tokens = 15
    resp._hidden_params = {}
    return resp


# ---------------------------------------------------------------------------
# Test 1 — message shape: image_url content block with base64 data URI
# ---------------------------------------------------------------------------


def test_complete_image_sends_image_url_block() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1)

    captured_messages: list = []

    def fake_completion(**kwargs):
        captured_messages.extend(kwargs["messages"])
        return _make_mock_resp()

    with patch("litellm.completion", side_effect=fake_completion):
        with patch("okg.views.execution.llm._LITELLM", None):
            import litellm

            with patch.object(litellm, "completion", side_effect=fake_completion):
                # Patch the lazy _litellm() to return a module with our fake
                mock_litellm = MagicMock()
                mock_litellm.completion.side_effect = fake_completion
                mock_litellm.APIError = Exception
                mock_litellm.RateLimitError = Exception
                mock_litellm.Timeout = Exception

                with patch(
                    "okg.views.execution.llm._litellm", return_value=mock_litellm
                ):
                    result = client.complete_image(TINY_PNG, "describe this")

    assert result == "caption text"
    assert len(captured_messages) == 1
    msg = captured_messages[0]
    assert msg["role"] == "user"
    content_blocks = msg["content"]
    image_block = next(b for b in content_blocks if b["type"] == "image_url")
    url = image_block["image_url"]["url"]
    assert url.startswith("data:image/png;base64,")
    # Verify the base64 decodes back to the original bytes
    b64_part = url.split(",", 1)[1]
    assert base64.b64decode(b64_part) == TINY_PNG


def test_complete_image_sends_text_block() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1)
    captured_messages: list = []

    def fake_completion(**kwargs):
        captured_messages.extend(kwargs["messages"])
        return _make_mock_resp()

    mock_litellm = MagicMock()
    mock_litellm.completion.side_effect = fake_completion
    mock_litellm.APIError = RuntimeError
    mock_litellm.RateLimitError = RuntimeError
    mock_litellm.Timeout = RuntimeError

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        client.complete_image(TINY_PNG, "describe this image carefully")

    msg = captured_messages[0]
    text_block = next(b for b in msg["content"] if b["type"] == "text")
    assert text_block["text"] == "describe this image carefully"


# ---------------------------------------------------------------------------
# Test 2 — transient APIError triggers retry up to max_retries
# ---------------------------------------------------------------------------


def test_transient_api_error_retries_then_fails() -> None:
    max_retries = 3
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=max_retries)

    call_count = 0

    class FakeAPIError(Exception):
        pass

    def always_fail(**kwargs):
        nonlocal call_count
        call_count += 1
        raise FakeAPIError("transient")

    mock_litellm = MagicMock()
    mock_litellm.completion.side_effect = always_fail
    mock_litellm.APIError = FakeAPIError
    mock_litellm.RateLimitError = type("RLE", (Exception,), {})
    mock_litellm.Timeout = type("TO", (Exception,), {})

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with patch("tenacity.wait_exponential", return_value=lambda _: 0):
            with pytest.raises(LLMCallFailed):
                client.complete_image(TINY_PNG, "desc")

    assert call_count == max_retries


def test_transient_error_succeeds_on_retry() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=3)
    call_count = 0

    class FakeAPIError(Exception):
        pass

    def fail_twice_then_succeed(**kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise FakeAPIError("transient")
        return _make_mock_resp("ok on retry")

    mock_litellm = MagicMock()
    mock_litellm.completion.side_effect = fail_twice_then_succeed
    mock_litellm.APIError = FakeAPIError
    mock_litellm.RateLimitError = type("RLE", (Exception,), {})
    mock_litellm.Timeout = type("TO", (Exception,), {})

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with patch("tenacity.wait_exponential", return_value=lambda _: 0):
            result = client.complete_image(TINY_PNG, "desc")

    assert result == "ok on retry"
    assert call_count == 3


def test_terminal_rate_limit_exposes_retried_hits_in_call_stats() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=3)
    call_count = 0

    class FakeRateLimitError(Exception):
        status_code = 429

    def always_rate_limited(**kwargs):
        nonlocal call_count
        call_count += 1
        raise FakeRateLimitError("provider response must not persist")

    mock_litellm = MagicMock()
    mock_litellm.completion.side_effect = always_rate_limited
    mock_litellm.APIError = type("APIError", (Exception,), {})
    mock_litellm.RateLimitError = FakeRateLimitError
    mock_litellm.Timeout = type("TO", (Exception,), {})

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with patch("tenacity.wait_exponential", return_value=lambda _: 0):
            with pytest.raises(LLMCallFailed):
                client.complete_image(TINY_PNG, "desc")

    assert call_count == 3
    assert client.last_call_stats["extras"]["rate_limit_hits"] == 2
    assert client.last_call_stats["error_kind"] == "FakeRateLimitError"
    assert client.last_call_stats["extras"]["provider_status_code"] == 429
    assert "provider response must not persist" not in repr(client.last_call_stats)


# ---------------------------------------------------------------------------
# Test 2b — an empty-string reply is treated like a missing response and
# raises, rather than being accepted as a successful (empty) caption. Some
# providers return content="" on a content-filter trip instead of raising.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("empty", ["", None])
def test_empty_content_raises(empty) -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1)

    mock_litellm = MagicMock()
    mock_litellm.completion.return_value = _make_mock_resp(empty)
    mock_litellm.APIError = Exception
    mock_litellm.RateLimitError = Exception
    mock_litellm.Timeout = Exception

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with pytest.raises(LLMCallFailed, match="empty content"):
            client.complete_image(TINY_PNG, "desc")


def test_empty_truncated_content_is_named() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1)
    response = _make_mock_resp("")
    response.choices[0].finish_reason = "length"
    mock_litellm = MagicMock()
    mock_litellm.completion.return_value = response
    mock_litellm.APIError = Exception
    mock_litellm.RateLimitError = Exception
    mock_litellm.Timeout = Exception

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with pytest.raises(LLMResponseTruncated):
            client.complete_image(TINY_PNG, "desc")


def test_nonempty_truncated_content_stays_visible_in_call_stats() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1)
    response = _make_mock_resp("partial caption")
    response.choices[0].finish_reason = "max_tokens"
    mock_litellm = MagicMock()
    mock_litellm.completion.return_value = response
    mock_litellm.APIError = Exception
    mock_litellm.RateLimitError = Exception
    mock_litellm.Timeout = Exception

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        assert client.complete_image(TINY_PNG, "desc") == "partial caption"
    assert client.last_call_stats["truncated"] is True


# ---------------------------------------------------------------------------
# Test 3 — tracking_dsn=None skips record_llm_call
# ---------------------------------------------------------------------------


def test_no_tracking_dsn_skips_record_llm_call() -> None:
    client = VLMClient(model="chatgpt/gpt-4o", max_retries=1, tracking_dsn=None)

    mock_litellm = MagicMock()
    mock_litellm.completion.return_value = _make_mock_resp()
    mock_litellm.APIError = Exception
    mock_litellm.RateLimitError = Exception
    mock_litellm.Timeout = Exception

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with patch("okg.views.execution.llm.record_llm_call") as mock_record:
            client.complete_image(TINY_PNG, "desc")
            mock_record.assert_not_called()


def test_tracking_dsn_calls_record_llm_call() -> None:
    client = VLMClient(
        model="chatgpt/gpt-4o",
        max_retries=1,
        tracking_dsn="postgres://test/db",
    )

    mock_litellm = MagicMock()
    mock_litellm.completion.return_value = _make_mock_resp()
    mock_litellm.APIError = Exception
    mock_litellm.RateLimitError = Exception
    mock_litellm.Timeout = Exception

    with patch("okg.views.execution.llm._litellm", return_value=mock_litellm):
        with patch("okg.views.execution.llm.record_llm_call") as mock_record:
            client.complete_image(TINY_PNG, "desc", caller="test:caller")
            mock_record.assert_called_once()


# ---------------------------------------------------------------------------
# StubVLMClient
# ---------------------------------------------------------------------------


def test_stub_vlm_client_records_calls() -> None:
    stub = StubVLMClient(response="a caption")
    result = stub.complete_image(TINY_PNG, "describe")
    assert result == "a caption"
    assert len(stub.calls) == 1
    assert stub.calls[0]["image_len"] == len(TINY_PNG)
    assert stub.calls[0]["prompt"] == "describe"


def test_stub_vlm_client_deterministic_without_response() -> None:
    stub = StubVLMClient()
    r1 = stub.complete_image(TINY_PNG, "p")
    r2 = stub.complete_image(TINY_PNG, "p")
    assert r1 == r2
    assert r1.startswith("[STUB_VLM ")
