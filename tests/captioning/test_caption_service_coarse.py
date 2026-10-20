"""Tests for CaptionService coarse mode — req.caption-service-coarse."""
from __future__ import annotations

import json

from PIL import Image as PILImage

from okg.substrate.library.captioning.service import CaptionService, CaptionResult
from okg.views.execution.llm import StubVLMClient


def _tiny_image(w: int = 10, h: int = 10) -> PILImage.Image:
    return PILImage.new("RGB", (w, h), color=(128, 64, 32))


def _json_response(category: str, caption: str) -> str:
    return json.dumps({"category": category, "caption": caption})


def test_coarse_exactly_one_vlm_call() -> None:
    stub = StubVLMClient(response=_json_response("plot", "a nice caption"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    results = svc.caption(_tiny_image())
    assert len(results) == 1
    assert len(stub.calls) == 1


def test_coarse_chunk_source_is_vision_caption() -> None:
    stub = StubVLMClient(response=_json_response("other", "caption"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.chunk_source == "vision_caption"


def test_coarse_caption_model_populated() -> None:
    stub = StubVLMClient(response=_json_response("other", "x"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.caption_model == "chatgpt/gpt-4o"
    assert result.caption_model != ""


def test_coarse_caption_provider_populated() -> None:
    stub = StubVLMClient(response=_json_response("other", "x"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.caption_provider == "chatgpt"
    assert result.caption_provider != ""


def test_coarse_text_is_vlm_response() -> None:
    stub = StubVLMClient(response=_json_response("plot", "detailed caption text"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.text == "detailed caption text"


def test_coarse_returns_caption_result_instance() -> None:
    stub = StubVLMClient(response=_json_response("other", "y"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert isinstance(result, CaptionResult)


# --- category parsing ---

def test_coarse_json_parse_sets_category_and_text() -> None:
    stub = StubVLMClient(response=_json_response("plot", "a histogram"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "plot"
    assert result.text == "a histogram"


def test_coarse_json_parse_ui_element_category() -> None:
    stub = StubVLMClient(response=_json_response("ui_element", "a dashboard screenshot"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "ui_element"
    assert result.text == "a dashboard screenshot"


def test_coarse_category_preserved_verbatim() -> None:
    # Deployments define their own category vocabulary via a coarse_prompt
    # override; the service keeps whatever category the model returns
    # (normalized) instead of clamping to a built-in whitelist.
    stub = StubVLMClient(response=json.dumps({"category": "Diagram", "caption": "some figure"}))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "diagram"
    assert result.text == "some figure"


def test_coarse_json_parse_failure_fallback() -> None:
    raw = "This is not JSON at all."
    stub = StubVLMClient(response=raw)
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "other"
    assert result.text == raw


def test_coarse_partial_json_missing_caption_uses_raw() -> None:
    # category present but no caption key — text should be the raw reply
    raw = json.dumps({"category": "plot"})
    stub = StubVLMClient(response=raw)
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "plot"
    # caption key absent → falls back to the raw string (json.dumps output)
    assert result.text == raw


# --- context injection ---

def test_coarse_context_injected_into_prompt() -> None:
    stub = StubVLMClient(response=_json_response("plot", "histogram"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    context = "Section: Data Quality Metrics"
    svc.caption(_tiny_image(), context=context)
    assert len(stub.calls) == 1
    assert context in stub.calls[0]["prompt"]


def test_coarse_unclosed_markdown_fence_still_parsed() -> None:
    # Some models emit a leading ```json fence but truncate/omit the closing
    # fence. The body must still parse instead of leaking raw ``` markers.
    unclosed = "```json\n" + json.dumps({"category": "table", "caption": "three tables"})
    stub = StubVLMClient(response=unclosed)
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "table"
    assert result.text == "three tables"


def test_coarse_fenced_prose_strips_fence() -> None:
    # A model wraps its plain-prose caption in a markdown fence despite the
    # prompt saying not to. The reply is not JSON, so it lands in the except
    # branch, which must fall back to the FENCE-STRIPPED text — not the raw
    # reply that still carries the ``` markers.
    stub = StubVLMClient(response="```\nA bar chart of training loss.\n```")
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "other"
    assert result.text == "A bar chart of training loss."
    assert "```" not in result.text


def test_coarse_fenced_json_missing_caption_uses_clean() -> None:
    # A JSON envelope with no caption key, wrapped in a fence. The fallback
    # must be the fence-stripped body, not the raw fenced reply.
    stub = StubVLMClient(
        response="```json\n" + json.dumps({"category": "plot"}) + "\n```",
    )
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "plot"
    assert "```" not in result.text
    assert result.text == json.dumps({"category": "plot"})


def test_coarse_markdown_fenced_json_parsed_correctly() -> None:
    fenced = "```json\n" + json.dumps({"category": "plot", "caption": "histogram"}) + "\n```"
    stub = StubVLMClient(response=fenced)
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    result = svc.caption(_tiny_image())[0]
    assert result.category == "plot"
    assert result.text == "histogram"


def test_coarse_no_context_uses_base_prompt() -> None:
    stub = StubVLMClient(response=_json_response("other", "fig"))
    svc = CaptionService(mode="coarse", vlm=stub, model="chatgpt/gpt-4o")
    svc.caption(_tiny_image())
    # Context injection marker must not appear when no context was passed
    assert "This figure appears in the following document context" not in stub.calls[0]["prompt"]


# --- prompt override (deployment / datasource specific) ---

def test_coarse_prompt_override_replaces_default() -> None:
    custom = "CUSTOM DEPLOYMENT PROMPT: classify this DQM screenshot."
    stub = StubVLMClient(response=_json_response("ui_element", "a dashboard"))
    svc = CaptionService(
        mode="coarse", vlm=stub, model="chatgpt/gpt-4o", coarse_prompt=custom,
    )
    svc.caption(_tiny_image())
    sent = stub.calls[0]["prompt"]
    assert custom in sent
    # The built-in default framing must not be present once overridden.
    assert "Caption the visual elements of the provided image." not in sent


def test_coarse_prompt_override_still_appends_context() -> None:
    custom = "CUSTOM PROMPT"
    stub = StubVLMClient(response=_json_response("plot", "histogram"))
    svc = CaptionService(
        mode="coarse", vlm=stub, model="chatgpt/gpt-4o", coarse_prompt=custom,
    )
    context = "Section: Trigger Rates"
    svc.caption(_tiny_image(), context=context)
    sent = stub.calls[0]["prompt"]
    assert sent.startswith(custom)
    assert context in sent


def test_coarse_none_override_falls_back_to_default() -> None:
    stub = StubVLMClient(response=_json_response("other", "fig"))
    svc = CaptionService(
        mode="coarse", vlm=stub, model="chatgpt/gpt-4o", coarse_prompt=None,
    )
    svc.caption(_tiny_image())
    assert "Caption the visual elements of the provided image." in stub.calls[0]["prompt"]


def test_fine_mode_rejected() -> None:
    import pytest

    stub = StubVLMClient(response=_json_response("other", "x"))
    with pytest.raises(ValueError, match="coarse"):
        CaptionService(mode="fine", vlm=stub, model="chatgpt/gpt-4o")
