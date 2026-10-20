"""Canary `LLMClient` stubs that return fixed JSON candidates.

Use when verifying end-to-end A2 extractor wiring without an
external provider call. Each stub's output is JSON parseable
by the corresponding extractor's `parse_llm_*_response` helper.

Per the dogfood-canary loop (see
``cleanup-personal-okg-data-quality/findings/...``):
the personal-OKG memex needs to prove the trails
``Paper -> defines -> Concept <- implements_concept <- CodeSymbol``
and ``Paper -> contains -> Paragraph -> contains -> Claim``
close. These clients provide the deterministic LLM half so the
proof is reproducible without ANTHROPIC_API_KEY.

Wire in ``deployment.yaml``::

    enrichers:
      - class: okg.substrate.library.enrichers.extraction.concept_extractor.ConceptExtractor
        config:
          confidence_floor: 0.5
          llm_canary: true
          input_quarantine_max_attempts: 3

The base class (LLMExtractor) recognizes the ``llm_canary: true``
kwarg and dispatches to the right canary via ``canary_for_name``
based on the extractor's ``name`` class attribute.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Callable

logger = logging.getLogger(__name__)


# ----- Concept candidates (anchored to seeded canonical concepts) -----

_CANARY_CONCEPTS: tuple[dict, ...] = (
    {
        "name": "happens-before ordering",
        "relationship": "uses",
        "confidence": 0.9,
        "description": (
            "Lamport's partial order on events, the foundation of "
            "distributed timestamping."
        ),
    },
    {
        "name": "bitemporal data",
        "relationship": "defines",
        "confidence": 0.85,
        "description": (
            "The discipline of tracking both observed_at and "
            "valid_from/valid_to for each fact."
        ),
    },
    {
        "name": "principled deferral",
        "relationship": "defines",
        "confidence": 0.7,
        "description": (
            "Deferring extraction or invariant checks to a later "
            "phase rather than blocking ingest."
        ),
    },
    {
        "name": "as we may think",
        "relationship": "refines",
        "confidence": 0.6,
        "description": (
            "Vannevar Bush's memex vision, refined here as a "
            "personal-OKG: typed, queryable, agent-readable."
        ),
        "refines_concept_name": "happens-before ordering",
    },
)


class CanaryConceptLLM:
    """Returns 4 canned concept-JSON entries for any prompt."""

    model: str = "canary:concept-fixed"

    def complete(
        self, system: str, user: str, *,
        fragment_id: str = "", caller: str = "",
        generation_id: int | None = None,
    ) -> str:
        payload = json.dumps(list(_CANARY_CONCEPTS), separators=(",", ":"))
        logger.info(
            "CanaryConceptLLM -> %d concepts (fragment_id=%s)",
            len(_CANARY_CONCEPTS), fragment_id,
        )
        return payload


# ----- Claim candidates -----

_CANARY_CLAIMS: tuple[dict, ...] = (
    {
        "text": (
            "The substrate's bitemporal split between facts and "
            "live tables is load-bearing for principled deferral."
        ),
        "kind": "definitional",
        "confidence": 0.75,
    },
    {
        "text": (
            "A single-writer advisory lock linearizes concurrent "
            "agent writes into a total order via generation_id."
        ),
        "kind": "theoretical",
        "confidence": 0.7,
    },
)


class CanaryClaimLLM:
    """Returns 2 canned claim-JSON entries per paragraph/theorem/table."""

    model: str = "canary:claim-fixed"

    def complete(
        self, system: str, user: str, *,
        fragment_id: str = "", caller: str = "",
        generation_id: int | None = None,
    ) -> str:
        return json.dumps(list(_CANARY_CLAIMS), separators=(",", ":"))


# ----- Evidence candidates -----
# Evidence_extractor.extract_candidates feeds claim_ids; the
# canary returns 1 evidence per claim citing the first paragraph
# it can find. The shape is per-claim but we can't know the
# claim_id from inside the LLM stub — the extractor's structure
# is to FIND candidates pre-LLM and pass to emit. So this stub
# only needs to return JSON that survives the parser.

_EVIDENCE_REF_RE = re.compile(r"^- id=(\S+)\s+kind=", re.MULTILINE)


class CanaryEvidenceLLM:
    """Returns 1 canned evidence-JSON entry referencing the first
    candidate id found in the user prompt.

    The evidence_extractor's prompt embeds candidate items as
    `- id=<node_id> kind=<subtype> label=<...>`. The canary parses
    the first id out and returns `evidence_ref` pointing at it,
    so the extractor's `parse_llm_evidence_response` accepts the
    candidate. Without this parsing the canary couldn't know which
    candidate to reference.
    """

    model: str = "canary:evidence-fixed"

    def complete(
        self, system: str, user: str, *,
        fragment_id: str = "", caller: str = "",
        generation_id: int | None = None,
    ) -> str:
        m = _EVIDENCE_REF_RE.search(user or "")
        if not m:
            return "[]"
        ref = m.group(1)
        return json.dumps([{
            "evidence_ref": ref,
            "support_polarity": "supports",
            "confidence": 0.7,
        }], separators=(",", ":"))


# ----- Hypothesis candidates -----

_CANARY_HYPOTHESES: tuple[dict, ...] = (
    # The hypothesis_extractor's parser expects `statement` not
    # `text`. Per system schema: statement + confidence +
    # predicts_about_concept_names.
    {
        "statement": (
            "Bitemporal pinning lets readers see a stable snapshot "
            "while writers append concurrently without coordination."
        ),
        "predicts_about_concept_names": ["bitemporal data"],
        "confidence": 0.65,
    },
)


class CanaryHypothesisLLM:
    """Returns 1 canned hypothesis per paper_section."""

    model: str = "canary:hypothesis-fixed"

    def complete(
        self, system: str, user: str, *,
        fragment_id: str = "", caller: str = "",
        generation_id: int | None = None,
    ) -> str:
        return json.dumps(list(_CANARY_HYPOTHESES), separators=(",", ":"))


# ----- Dispatch -----

_BY_NAME: dict[str, Callable[[], object]] = {
    "concept_extractor":    CanaryConceptLLM,
    "claim_extractor":      CanaryClaimLLM,
    "evidence_extractor":   CanaryEvidenceLLM,
    "hypothesis_extractor": CanaryHypothesisLLM,
}


def canary_for_name(name: str):
    """Construct a canary LLM appropriate for the named extractor.

    Falls back to CanaryConceptLLM for back-compat (the original
    `llm_canary: true` semantics).
    """
    cls = _BY_NAME.get(name, CanaryConceptLLM)
    return cls()


__all__ = [
    "CanaryConceptLLM",
    "CanaryClaimLLM",
    "CanaryEvidenceLLM",
    "CanaryHypothesisLLM",
    "canary_for_name",
]
