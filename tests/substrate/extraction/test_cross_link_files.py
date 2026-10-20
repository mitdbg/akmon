"""Verify the cross-link rule file A2 owns is well-formed YAML.

The file carries `rules: []` because the corresponding edges are
emitted by the LLM-driven EvidenceExtractor rather than by the
DeclarativeLinker. It exists as documentation of the rule slot + the
reserved file path for any future pattern-driven augmentation.

The former `claim_supports_claim.yaml` / `claim_contradicts_claim.yaml`
placeholders were DELETED with the relation mechanism by ruling D7
(consistency-single-engine dispositions, 2026-07-17): cross-claim
support/contradiction is the claim plane's job (deployment-declared
Σ predicates), not a parallel edge-emission mechanism.
"""
from __future__ import annotations

from pathlib import Path

import yaml

# Per `add-module-manifests-and-composition` (2026-05-19): the
# placeholder rule file lives next to its owning module (extraction)
# in `library/ontologies/extraction/cross_links/`.
_CROSS_LINKS = Path(
    "src/okg/substrate/library/ontologies/extraction/cross_links"
)


def _load(name: str) -> dict:
    return yaml.safe_load((_CROSS_LINKS / name).read_text())


def test_claim_supported_by_evidence_yaml_loads() -> None:
    data = _load("claim_supported_by_evidence.yaml")
    assert data is not None
    assert "rules" in data
    assert data["rules"] == []


def test_relation_placeholders_stay_deleted() -> None:
    # D7: the relation mechanism's reserved slots must not silently
    # reappear; resurrection requires a ratified decision, not a file.
    for name in (
        "claim_supports_claim.yaml",
        "claim_contradicts_claim.yaml",
    ):
        assert not (_CROSS_LINKS / name).exists()
