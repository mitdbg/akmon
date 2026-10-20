"""LLM-driven semantic-extraction enrichers (A2).

Per `extend-substrate-extraction-with-claim-evidence-extractors`,
this package ships four LLM-driven enrichers (`claim_extractor`,
`concept_extractor`, `evidence_extractor`, `hypothesis_extractor`)
plus the substrate-level confidence-floor convention that they share
(`LLMExtractor` base + helpers).

The former `relation_extractor` (LLM-adjudicated claim
supports/contradicts edges + the disputed flag) was DELETED by ruling
D7 (consistency-single-engine dispositions, 2026-07-17): it was a
second conflict-detection mechanism outside the claim plane —
cross-claim contradiction detection is deployment-declared Σ
predicates over claims, decided by the deterministic checker. If LLM
pair-adjudication returns, it re-enters as a candidate generator
feeding declared checks, never as its own engine.
"""
from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    LLMExtractor,
    make_dropped_id,
)
from okg.substrate.library.enrichers.extraction.claim_extractor import (
    ClaimExtractor,
)
from okg.substrate.library.enrichers.extraction.concept_extractor import (
    ConceptExtractor,
)
from okg.substrate.library.enrichers.extraction.evidence_extractor import (
    EvidenceExtractor,
)
from okg.substrate.library.enrichers.extraction.hypothesis_extractor import (
    HypothesisExtractor,
)

__all__ = [
    "ClaimExtractor",
    "ConceptExtractor",
    "EvidenceExtractor",
    "ExtractionCandidate",
    "HypothesisExtractor",
    "LLMExtractor",
    "make_dropped_id",
]
