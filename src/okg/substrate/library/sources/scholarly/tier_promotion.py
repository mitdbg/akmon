"""Tier 1/2/3 promotion machinery for the scholarly module.

Per umbrella D3 (Tier policy) + D7 (LLM extraction at operator-
triggered batch by default). The convention:

  - **Tier 1**: full text + structural (A1) + semantic (A2) extraction.
  - **Tier 2**: metadata + abstract only; no chunking or extraction.
  - **Tier 3**: citation stub (Paper node exists with `arxiv_id` or
    `doi`; no abstract).

Source-emission contract: each scholarly Paper NodeFact carries an
`attrs.tier: 1|2|3` field set by the emitting source. Defaults
(per source kind):

  - `BibReferencesSource`         → Tier 1 (foundational set)
  - `VenuePapersSource` / venue scanners → Tier 2 (metadata-tier)
  - `TalksSource`                 → Tier 1 (operator-curated talks)
  - Crossref / OpenAlex / Semantic Scholar
                                  → Tier 3 (citation stubs)

Promotion mechanism: a deployment writes per-paper promotion entries
into a `promotions.yaml` (or the `promotions:` block of
`source_registry.yaml`). On the next ingest cycle, the source that
emits the paper re-emits it with the new `attrs.tier`. Substrate
dedupe (idempotent `dedupe_key`) updates the existing Paper node in
place. When `attrs.tier=1` and `attrs.auto_extract=true` (or the
deployment's auto_extract setting is on for tier 1), the A1
structural extractor + A2 semantic extractor SHOULD fire on the same
publish cycle via the soft hooks below.

Soft hooks: `trigger_tier_1_extraction(paper_id)` attempts to import
A1's `PaperAnatomyEnricher` and A2's `ClaimEvidenceExtractor` from
their library paths. If either module is missing (A1 / A2 not yet
implemented in this repo state), the hook logs and no-ops gracefully.
Per the impl-A4 brief: "If A1/A2 aren't merged + implemented yet:
implement A4 fully, including the *call sites* for A1/A2 extraction
triggers, but make them tolerant of A1/A2 absence."
"""
from __future__ import annotations
from okg.substrate import fastyaml

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from okg.substrate.errors import SubstrateError

__all__ = [
    "PromotionConfig",
    "load_promotions_yaml",
    "trigger_tier_1_extraction",
    "is_a1_available",
    "is_a2_available",
]


_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class PromotionConfig:
    """A per-paper tier promotion entry.

    Read from `promotions.yaml` (or the `promotions:` block of
    `source_registry.yaml`) and consumed by the relevant scholarly
    source on the next ingest cycle.
    """

    paper_id: str
    tier: int
    auto_extract: bool = False
    note: str | None = None

    def __post_init__(self) -> None:
        if self.tier not in (1, 2, 3):
            raise SubstrateError(
                f"PromotionConfig: tier must be 1, 2, or 3; got {self.tier!r}",
                fix_hint="Set tier to one of {1, 2, 3}.",
                data={"paper_id": self.paper_id, "tier": self.tier},
            )


def load_promotions_yaml(path: str | Path) -> list[PromotionConfig]:
    """Load promotion entries from a YAML file.

    Expected shape (a list under `promotions:` or at the top level):

        promotions:
          - paper_id: "paper:doi:10.1145/..."
            tier: 1
            auto_extract: true
            note: "core POSTGRES paper"
          - paper_id: "paper:arxiv:2305.14283"
            tier: 1

    Fails loudly on malformed YAML, missing required fields, or
    invalid tier values.
    """
    import yaml

    yaml_path = Path(path).expanduser()
    if not yaml_path.is_file():
        raise SubstrateError(
            f"load_promotions_yaml: {str(yaml_path)!r} does not exist",
            fix_hint=(
                "Create the file with a `promotions:` list, or remove "
                "the reference from the deployment config if no "
                "promotions are intended yet."
            ),
            data={"path": str(yaml_path)},
        )
    try:
        raw = fastyaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SubstrateError(
            f"load_promotions_yaml: malformed YAML at {str(yaml_path)!r}: {exc}",
            fix_hint="Lint the YAML file before retrying.",
            data={"path": str(yaml_path), "yaml_error": str(exc)},
        ) from exc

    # Accept either a top-level list or a `promotions:` keyed list.
    if isinstance(raw, dict):
        entries = raw.get("promotions") or []
    elif isinstance(raw, list):
        entries = raw
    else:
        raise SubstrateError(
            f"load_promotions_yaml: {str(yaml_path)!r} must be a list or "
            f"a mapping with `promotions:` key; got {type(raw).__name__}",
            fix_hint="Wrap entries in a top-level list or under `promotions:`.",
        )

    out: list[PromotionConfig] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise SubstrateError(
                f"load_promotions_yaml: entry {idx} is not a mapping "
                f"(got {type(entry).__name__})",
            )
        paper_id = entry.get("paper_id")
        if not paper_id or not isinstance(paper_id, str):
            raise SubstrateError(
                f"load_promotions_yaml: entry {idx} missing `paper_id`",
                fix_hint="Every promotion entry requires a paper_id string.",
                data={"entry_index": idx},
            )
        tier = entry.get("tier")
        if tier is None:
            raise SubstrateError(
                f"load_promotions_yaml: entry {idx} (paper_id={paper_id!r}) "
                f"missing `tier`",
                fix_hint="Set tier to 1, 2, or 3.",
            )
        out.append(
            PromotionConfig(
                paper_id=paper_id,
                tier=int(tier),
                auto_extract=bool(entry.get("auto_extract", False)),
                note=entry.get("note"),
            )
        )
    return out


# ----------------------------------------------------------------------
# Soft hooks for A1 / A2 extraction triggers
# ----------------------------------------------------------------------


def is_a1_available() -> bool:
    """True iff A1's PaperAnatomyEnricher (paper structural extraction)
    is importable in this repo state.

    A1 is `extend-substrate-scholarly-with-paper-anatomy`. Until that
    proposal's implementation lands, this hook reports False and
    `trigger_tier_1_extraction` no-ops the A1 portion.
    """
    try:
        # Expected module path once A1 lands; absence indicates the
        # implementation hasn't been written yet.
        import importlib

        importlib.import_module(
            "okg.substrate.library.enrichers.paper_anatomy"
        )
        return True
    except ImportError:
        return False


def is_a2_available() -> bool:
    """True iff A2's ClaimEvidenceExtractor (semantic extraction) is
    importable.

    A2 is `extend-substrate-extraction-with-claim-evidence-extractors`.
    """
    try:
        import importlib

        importlib.import_module(
            "okg.substrate.library.enrichers.claim_evidence"
        )
        return True
    except ImportError:
        return False


def trigger_tier_1_extraction(
    paper_id: str,
    *,
    deployment: str | None = None,
    a1_kwargs: Optional[dict[str, Any]] = None,
    a2_kwargs: Optional[dict[str, Any]] = None,
) -> dict[str, str]:
    """Trigger A1 structural extraction + A2 semantic extraction on
    the named Tier-1 paper.

    Returns a dict naming which hooks fired ('ran' | 'skipped').
    Soft-hook policy: if A1 or A2 isn't importable yet, the
    corresponding hook is `skipped` with a logged note. The caller
    SHOULD log the result; the substrate's ingest cycle SHOULD treat
    a fully-skipped result as a no-op (not a failure).
    """
    out = {"a1": "skipped", "a2": "skipped"}
    a1_ok = is_a1_available()
    a2_ok = is_a2_available()

    if a1_ok:
        try:
            from okg.substrate.library.enrichers.paper_anatomy import (  # type: ignore[import-not-found]
                trigger as a1_trigger,
            )
            a1_trigger(paper_id=paper_id, deployment=deployment, **(a1_kwargs or {}))
            out["a1"] = "ran"
        except Exception as exc:  # pragma: no cover - reachable only post-A1
            _LOG.warning(
                "tier_promotion: A1 paper_anatomy trigger failed for "
                "paper_id=%s: %s",
                paper_id,
                exc,
            )
            out["a1"] = f"errored: {exc}"
    else:
        _LOG.info(
            "tier_promotion: A1 paper_anatomy not yet implemented; "
            "skipping structural extraction for paper_id=%s",
            paper_id,
        )

    if a2_ok:
        try:
            from okg.substrate.library.enrichers.claim_evidence import (  # type: ignore[import-not-found]
                trigger as a2_trigger,
            )
            a2_trigger(paper_id=paper_id, deployment=deployment, **(a2_kwargs or {}))
            out["a2"] = "ran"
        except Exception as exc:  # pragma: no cover - reachable only post-A2
            _LOG.warning(
                "tier_promotion: A2 claim_evidence trigger failed for "
                "paper_id=%s: %s",
                paper_id,
                exc,
            )
            out["a2"] = f"errored: {exc}"
    else:
        _LOG.info(
            "tier_promotion: A2 claim_evidence not yet implemented; "
            "skipping semantic extraction for paper_id=%s",
            paper_id,
        )

    return out


def filter_promotions_for_source(
    promotions: Iterable[PromotionConfig],
    *,
    source_paper_ids: Iterable[str],
) -> dict[str, PromotionConfig]:
    """Return only the promotions whose paper_id is in the given source
    set, keyed by paper_id. Useful for source classes to apply
    promotion overrides at emission time.
    """
    by_id = {p.paper_id: p for p in promotions}
    source_set = set(source_paper_ids)
    return {pid: cfg for pid, cfg in by_id.items() if pid in source_set}
