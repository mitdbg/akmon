"""Tests for the five Concept-hub cross-link rule files
shipped by `extend-substrate-concepts-as-universal-hub`.

The other three Concept-hub edges (paper_defines_concept,
paper_uses_concept, concept_refines_concept) are A2's
territory — their rule files don't ship from this child.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.linkers.rules import load_rules_file


_ONT = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library" / "ontologies"
)

# Per `add-module-manifests-and-composition` (2026-05-19): each
# cross-link YAML lives inside its owning module's `cross_links/`
# subdir, referenced from `module.yaml`'s `provides.intra_cross_link_rules`
# or `extends.<target>.cross_link_rules` block.
_RULE_PATH: dict[str, Path] = {
    "methodcard_embodies_concept.yaml":
        _ONT / "methodology" / "cross_links"
        / "methodcard_embodies_concept.yaml",
    "hypothesis_predicts_about_concept.yaml":
        _ONT / "reasoning" / "cross_links"
        / "hypothesis_predicts_about_concept.yaml",
    "code_symbol_implements_concept.yaml":
        _ONT / "code_graph" / "cross_links"
        / "code_symbol_implements_concept.yaml",
    "observation_evidence_for_concept.yaml":
        _ONT / "reasoning" / "cross_links"
        / "observation_evidence_for_concept.yaml",
    "experiment_concerns_concept.yaml":
        _ONT / "experimental" / "cross_links"
        / "experiment_concerns_concept.yaml",
}


RULE_FILES = tuple(_RULE_PATH.keys())


@pytest.mark.parametrize("filename", RULE_FILES)
def test_rule_file_exists_and_parses(filename: str) -> None:
    path = _RULE_PATH[filename]
    assert path.exists(), f"missing rule file: {path}"
    rules = load_rules_file(path)
    assert len(rules) == 1
    rule_id = rules[0].id
    expected_id = filename.removesuffix(".yaml")
    assert rule_id == expected_id, (
        f"rule id {rule_id!r} does not match filename {filename!r}"
    )


def test_methodcard_embodies_concept_shape() -> None:
    rules = load_rules_file(
        _RULE_PATH["methodcard_embodies_concept.yaml"]
    )
    rule = rules[0]
    assert rule.src.subtypes == ("method_card",)
    assert rule.src.field == "embodies_concept"
    assert rule.dst.subtypes == ("concept",)
    assert rule.dst.field == "name"
    assert rule.matcher == "exact"
    assert rule.edge == "embodies"
    assert rule.bidirectional is False


def test_hypothesis_predicts_about_concept_shape() -> None:
    rules = load_rules_file(
        _RULE_PATH["hypothesis_predicts_about_concept.yaml"]
    )
    rule = rules[0]
    assert rule.src.subtypes == ("hypothesis",)
    assert rule.src.field == "predicts_about"
    assert rule.dst.subtypes == ("concept",)
    assert rule.dst.field == "name"
    assert rule.matcher == "exact"
    assert rule.edge == "predicts_about"
    assert rule.bidirectional is False


def test_code_symbol_implements_concept_shape() -> None:
    """Uses `derive_from_path` matcher with the exact regex
    for the `@implements_concept: <slug>` docstring tag per
    umbrella D5."""
    rules = load_rules_file(
        _RULE_PATH["code_symbol_implements_concept.yaml"]
    )
    rule = rules[0]
    assert rule.src.subtypes == ("code_symbol",)
    assert rule.src.field == "docstring"
    assert rule.dst.subtypes == ("concept",)
    # Per dogfood-canary follow-up (2026-05-18): rule matches on
    # `slug` (kebab-case) not `name` (human-readable). The regex
    # captures a kebab slug; matching `name` produced 0 edges
    # because `Concept.name` has spaces while the capture doesn't.
    assert rule.dst.field == "slug"
    assert rule.matcher == "derive_from_path"
    assert rule.edge == "implements_concept"
    assert rule.bidirectional is False
    dfp = rule.dst.derive_from_path
    assert dfp is not None
    assert dfp.attr == "docstring"
    # Per dogfood-canary follow-up gap #4 (2026-05-18): multi_capture
    # mode uses `re.findall` over the whole string, so the regex
    # has no `(?s).*` prefix (which would constrain to one capture).
    assert dfp.regex == r"@implements_concept:\s*([a-z][a-z0-9-]*)"
    assert dfp.capture_field == "slug"
    assert dfp.multi_capture is True


def test_observation_evidence_for_concept_shape() -> None:
    rules = load_rules_file(
        _RULE_PATH["observation_evidence_for_concept.yaml"]
    )
    rule = rules[0]
    assert rule.src.subtypes == ("observation",)
    assert rule.src.field == "evidence_for"
    assert rule.dst.subtypes == ("concept",)
    assert rule.dst.field == "name"
    assert rule.matcher == "exact"
    assert rule.edge == "evidence_for"
    assert rule.bidirectional is False


def test_experiment_concerns_concept_shape() -> None:
    rules = load_rules_file(
        _RULE_PATH["experiment_concerns_concept.yaml"]
    )
    rule = rules[0]
    assert rule.src.subtypes == ("experiment",)
    assert rule.src.field == "concerns"
    assert rule.dst.subtypes == ("concept",)
    assert rule.dst.field == "name"
    assert rule.matcher == "exact"
    assert rule.edge == "concerns"
    assert rule.bidirectional is False


def test_docstring_regex_matches_canonical_tag() -> None:
    """The regex extracts the slug from the canonical tag
    form and rejects near-misses.

    Per dogfood-canary follow-up gap #4 (2026-05-18): the rule
    now uses `multi_capture: true` with `re.findall` over the
    whole docstring, so the test uses findall too. The regex
    no longer has the `(?s).*` prefix (which is incompatible
    with multi-capture semantics).
    """
    import re
    rules = load_rules_file(
        _RULE_PATH["code_symbol_implements_concept.yaml"]
    )
    pattern = re.compile(rules[0].dst.derive_from_path.regex)

    # Canonical match.
    matches = pattern.findall("@implements_concept: bitemporal-data")
    assert matches == ["bitemporal-data"]

    # Embedded in a multi-line docstring: findall scans the whole
    # string, no DOTALL prefix needed.
    matches = pattern.findall(
        "Docstring line 1.\n@implements_concept: happens-before\nMore prose."
    )
    assert matches == ["happens-before"]

    # MULTIPLE tags in one docstring — both captured.
    matches = pattern.findall(
        "Class docstring.\n\n"
        "@implements_concept: principled-deferral\n"
        "@implements_concept: bitemporal-data\n\n"
        "Body prose."
    )
    assert matches == ["principled-deferral", "bitemporal-data"]

    # Near-misses fail closed: no underscore.
    assert pattern.findall("@implementsconcept: foo") == []
    # Missing colon.
    assert pattern.findall("@implements_concept foo") == []
    # Uppercase slug — regex requires lowercase per kebab-case
    # discipline; findall returns empty.
    assert pattern.findall("@implements_concept: FOO") == []


def test_no_rule_targets_paper_or_self_referential_refines() -> None:
    """Regression guard: A2 owns the three rule files for
    paper_defines_concept, paper_uses_concept, and
    concept_refines_concept. This child SHALL NOT ship those
    files."""
    forbidden = {
        "paper_defines_concept.yaml",
        "paper_uses_concept.yaml",
        "concept_refines_concept.yaml",
    }
    # Sweep every module's cross_links/ subdir (post
    # `add-module-manifests-and-composition`).
    existing = {
        p.name for p in _ONT.glob("*/cross_links/*.yaml")
    }
    overlap = forbidden & existing
    assert not overlap, (
        f"A3 child shipped rule files that belong to A2: {overlap}"
    )
