"""Tests for the methodology→reasoning cross-link rule file.

Verifies the three DeclarativeLinker rules parse cleanly via
the substrate's rule loader, that MethodCard and Heuristic
deliberately share the `embodies` archetype, and that
AntiPattern uses `violates` as the negative counterpart.

Per `add-substrate-methodology-module`.
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.linkers.rules import load_rules_file


RULE_FILE = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library"
    / "ontologies" / "methodology" / "cross_links"
    / "methodology_to_reasoning.yaml"
)


def test_rule_file_exists() -> None:
    assert RULE_FILE.exists(), f"missing rule file: {RULE_FILE}"


def test_rule_file_parses_via_declarative_linker() -> None:
    """The DeclarativeLinker rule loader accepts the file
    and returns three rules with the expected IDs."""
    rules = load_rules_file(RULE_FILE)
    assert len(rules) == 3
    ids = {r.id for r in rules}
    assert ids == {
        "methodcard_embodies_principle",
        "heuristic_embodies_principle",
        "antipattern_violates_principle",
    }


def test_methodcard_embodies_principle_rule_shape() -> None:
    rules = load_rules_file(RULE_FILE)
    rule = next(r for r in rules if r.id == "methodcard_embodies_principle")
    assert rule.src.subtypes == ("method_card",)
    assert rule.src.field == "embodies_principle"
    assert rule.dst.subtypes == ("principle",)
    assert rule.dst.field == "slug"
    assert rule.matcher == "exact"
    assert rule.edge == "embodies"
    assert rule.bidirectional is False


def test_heuristic_embodies_principle_rule_shape() -> None:
    rules = load_rules_file(RULE_FILE)
    rule = next(r for r in rules if r.id == "heuristic_embodies_principle")
    assert rule.src.subtypes == ("heuristic",)
    assert rule.src.field == "embodies_principle"
    assert rule.dst.subtypes == ("principle",)
    assert rule.dst.field == "slug"
    assert rule.matcher == "exact"
    assert rule.edge == "embodies"
    assert rule.bidirectional is False


def test_antipattern_violates_principle_rule_shape() -> None:
    rules = load_rules_file(RULE_FILE)
    rule = next(r for r in rules if r.id == "antipattern_violates_principle")
    assert rule.src.subtypes == ("anti_pattern",)
    assert rule.src.field == "violates_principle"
    assert rule.dst.subtypes == ("principle",)
    assert rule.dst.field == "slug"
    assert rule.matcher == "exact"
    assert rule.edge == "violates"
    assert rule.bidirectional is False


def test_methodcard_and_heuristic_share_embodies_archetype() -> None:
    """Deliberate design choice per design.md D-meth-5:
    MethodCard and Heuristic both use `embodies` so a query
    'what embodies this Principle?' returns both subtypes.
    The subtype filter narrows when needed. Regression test
    against accidental archetype split."""
    rules = load_rules_file(RULE_FILE)
    by_id = {r.id: r for r in rules}
    assert (
        by_id["methodcard_embodies_principle"].edge
        == by_id["heuristic_embodies_principle"].edge
        == "embodies"
    )


def test_no_rule_targets_actor_or_other_archetype() -> None:
    """Every rule targets reasoning.Principle. Regression
    test against the rule file accidentally growing into
    multi-target territory; that would belong in a
    different cross-link file."""
    rules = load_rules_file(RULE_FILE)
    for rule in rules:
        assert rule.dst.subtypes == ("principle",), (
            f"rule {rule.id} targets non-Principle subtype: "
            f"{rule.dst.subtypes}"
        )
