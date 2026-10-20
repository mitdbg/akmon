"""Shared recaption-reuse signature — the single source of truth for the
``vision_signature`` formula.

The signature is computed in TWO places that must agree byte-for-byte:
``DocCorpusSource`` predicts it at figure discovery, and
``VisualCaptionEnricher`` recomputes it before deciding whether to call the
VLM. When the two agree, a re-publish over an unchanged corpus reuses the
existing caption and issues zero VLM calls.

Before this module the formula lived twice, copied into each site from its own
private ``model``/``prompt``/``mode`` values. A one-character drift between the
copies had no error path: it would silently mark every existing caption stale,
re-caption the entire corpus, and re-burn every VLM call (project P0:
default-drift-as-silent-cost). Both sites now call
:func:`compute_vision_signature`, so drift is impossible by construction.
"""
from __future__ import annotations

import hashlib
import json

__all__ = ["vision_config_digest", "compute_vision_signature"]


def vision_config_digest(*, mode: str, model: str) -> str:
    """Deterministic digest of the captioning-config fields that change a
    caption's *content* (``mode`` + ``model``).

    Only these two belong here: they change what the VLM is asked and which
    model answers. The prompt is folded into the signature separately, and
    limit knobs (``min_image_px`` / ``max_images_per_doc``) are deliberately
    excluded — they choose *which* visuals get captioned, not what any caption
    says, so including them would needlessly bust the reuse cache.
    """
    payload = json.dumps({"mode": mode, "model": model}, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_vision_signature(
    *,
    content_sha256: str,
    render_sha256: str | None,
    model: str,
    provider: str,
    prompt: str,
    mode: str,
    context: str = "",
) -> str:
    """Recaption-reuse signature for one caption.

    Deterministic over the visual's content/render hash plus the
    model/provider/prompt/config that produced the caption. ``context`` (the
    per-visual document text injected into the VLM prompt) is folded only when
    non-empty, so signatures for context-less deployments stay byte-identical
    to pre-context runs (no spurious fleet-wide recaption).
    """
    prompt_digest = hashlib.sha256((prompt or "").encode("utf-8")).hexdigest()
    config_digest = vision_config_digest(mode=mode, model=model)
    parts = [
        content_sha256,
        render_sha256 or "",
        model,
        provider,
        prompt_digest,
        config_digest,
    ]
    if context:
        parts.append(hashlib.sha256(context.encode("utf-8")).hexdigest())
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
