"""CaptionService — coarse vision-captioning.

Coarse mode: one VLM call per image → one CaptionResult carrying a
``category`` (plot / ui_element / other) and a ``caption`` description.

The VLMClient is injectable via the constructor for testing. The coarse
prompt defaults to :data:`_COARSE_PROMPT` but can be overridden per
deployment or per datasource via the ``coarse_prompt`` constructor
argument (sourced from the ``captioning:`` block in ``extractors.yaml``).
"""

from __future__ import annotations

import io
import json
import re
from dataclasses import dataclass
from typing import Literal


_COARSE_PROMPT = (
    "Caption the visual elements of the provided image. Describe everything "
    "you can draw from the image itself — the kind of visual (chart, diagram, "
    "table, photo, screenshot, equation, etc.), the data or relationships "
    "shown, axis labels and ranges, legends, series, rows and columns, "
    "annotations, and any notable patterns or structure. Report only what is "
    "visible in the image; do not invent content and do not describe "
    "surrounding page text. Write a concise, information-dense description as "
    "plain prose — no JSON, no markdown, no preamble."
)

_COARSE_CONTEXT_PREFIX = (
    "\n\nThis figure appears in the following document context "
    "(use it ONLY to disambiguate what the figure shows and which "
    "category it belongs to; do not invent content not visible in "
    "the image):\n"
)


_MARKDOWN_FENCE_RE = re.compile(r"^```[a-zA-Z]*\n?(.*?)\n?```$", re.DOTALL)


def _strip_markdown_fence(text: str) -> str:
    """Remove surrounding ```...``` fences that some models add despite instructions.

    Also tolerates a *partial* fence — a leading ```lang line with a missing or
    truncated closing fence — so a JSON body wrapped in an unclosed fence can
    still be parsed instead of leaking the raw ``` markers into the caption.
    """
    s = text.strip()
    m = _MARKDOWN_FENCE_RE.match(s)
    if m:
        return m.group(1).strip()
    if s.startswith("```"):
        # Drop the opening ```lang line, then a trailing ``` if present.
        s = s.split("\n", 1)[1] if "\n" in s else s[3:]
        if s.rstrip().endswith("```"):
            s = s.rstrip()[:-3]
        return s.strip()
    return text


# No fixed category vocabulary: the default prompt emits plain prose (no
# category), and deployments that want categories define their own set via a
# `coarse_prompt` override in extractors.yaml. Whatever category the model
# returns is preserved verbatim (normalized) rather than clamped to a
# built-in whitelist.


@dataclass
class CaptionResult:
    """One captioned image."""

    text: str
    chunk_source: str = "vision_caption"
    caption_model: str = ""
    caption_provider: str = ""
    category: str = ""
    # A non-empty provider response that stopped at its output limit is not a
    # complete caption. Keep the signal attached to the result so batched
    # callers can hold the source watermark instead of persisting a partial
    # replacement.
    truncated: bool = False


def _provider_from_model(model: str) -> str:
    """Extract provider prefix from a litellm model string."""
    if "/" in model:
        return model.split("/", 1)[0]
    return model


def _image_to_bytes(image: object, fmt: str = "PNG") -> bytes:
    buf = io.BytesIO()
    image.save(buf, format=fmt)  # type: ignore[union-attr]
    return buf.getvalue()


class CaptionService:
    """Vision captioning service (coarse mode).

    Args:
        mode: Only ``"coarse"`` is supported — one VLM call per image.
        vlm: A VLMClient (or any object with
            ``complete_image(bytes, str, *, ..., outcome_key_digest=None)
            -> str`` — the keyword is part of the contract since keyed
            outcomes landed; see ``okg.substrate.llm.outcomes``).
        model: litellm model string; stored on results for provenance.
        coarse_prompt: Optional prompt override. When ``None`` the
            built-in :data:`_COARSE_PROMPT` is used. Deployments and
            datasources supply this via the ``captioning:`` block in
            ``extractors.yaml`` to tune the prompt to their corpus.
    """

    def __init__(
        self,
        *,
        mode: Literal["coarse"] = "coarse",
        vlm: object,
        model: str = "",
        coarse_prompt: str | None = None,
    ) -> None:
        if mode != "coarse":
            raise ValueError(
                f"mode must be 'coarse' (fine mode is not supported), got {mode!r}"
            )
        self._mode = mode
        self._vlm = vlm
        self._model = model or getattr(vlm, "model", "")
        self._provider = _provider_from_model(self._model)
        self._coarse_prompt = coarse_prompt or _COARSE_PROMPT

    def caption(
        self,
        image: object,
        context: str = "",
        *,
        caller: str = "",
        fragment_id: str = "",
        generation_id: int | str | None = None,
    ) -> list[CaptionResult]:
        """Caption an image; return a single-element list of :class:`CaptionResult`.

        ``caller``/``fragment_id`` are forwarded to the VLM client verbatim.
        They are informational unless the client is wired with call
        tracking (``tracking_dsn``), in which case a non-empty ``caller``
        is required — see ``req.visual-artifacts.fail-loud-config``.
        """
        return [
            self._coarse(
                image,
                context,
                caller=caller,
                fragment_id=fragment_id,
                generation_id=generation_id,
            )
        ]

    def _coarse(
        self,
        image: object,
        context: str = "",
        *,
        caller: str = "",
        fragment_id: str = "",
        generation_id: int | str | None = None,
    ) -> CaptionResult:
        image_bytes = _image_to_bytes(image)
        prompt = self._coarse_prompt
        if context:
            prompt = self._coarse_prompt + _COARSE_CONTEXT_PREFIX + context
        # Stable outcome key (spec §9.2); a vision call's typed input is
        # the image bytes themselves — hashed, never embedded. Retention/
        # replay wiring for captioning is owned by a later child; the
        # digest lands on okg.llm_calls now so history is joinable.
        import hashlib as _hashlib

        from okg.substrate.llm.outcomes import OutcomeKey, prompt_content_hash

        key = OutcomeKey(
            step_id=caller or "captioning:coarse",
            operator_version="v1",
            model_id=self._model or "unknown",
            prompt_hash=prompt_content_hash(prompt),
            input_hash=_hashlib.sha256(image_bytes).hexdigest(),
        )
        raw = self._vlm.complete_image(  # type: ignore[union-attr]
            image_bytes,
            prompt,
            caller=caller,
            fragment_id=fragment_id,
            generation_id=generation_id,
            outcome_key_digest=key.digest,
        )
        clean = _strip_markdown_fence(raw)
        # A deployment prompt may ask for a JSON {category, caption} object; the
        # default prompt returns plain prose. Parse JSON when present and keep
        # the model's category verbatim (normalized); otherwise treat the whole
        # reply as the caption text.
        # Fall back to `clean` (fence-stripped), not `raw`: a fenced prose
        # reply — the exact case `_strip_markdown_fence` exists for — is not
        # valid JSON and lands in the except branch, and a JSON object may
        # lack a `caption` key. Using `raw` in either case would leak the
        # ```` ``` ```` markers back into the caption text.
        try:
            parsed = json.loads(clean)
            category = str(parsed.get("category", "other")).strip().lower() or "other"
            text = str(parsed.get("caption", clean))
        except (json.JSONDecodeError, ValueError, AttributeError):
            text = clean
            category = "other"
        stats = getattr(self._vlm, "last_call_stats", None)
        truncated = bool(stats.get("truncated")) if isinstance(stats, dict) else False
        return CaptionResult(
            text=text,
            category=category,
            chunk_source="vision_caption",
            caption_model=self._model,
            caption_provider=self._provider,
            truncated=truncated,
        )
