"""A failure taxonomy for the consistency benchmark: WHY, not just WHERE.

PACT ``concord-failure-taxonomy``, req.tax.taxonomy.

The funnel already answers WHERE each injected error stopped — one of eight
terminal states, from lost-at-extraction to repaired. That was the right first
cut and it is the wrong last one: within a single state the failures have
different causes and need different fixes. ``detected_only`` covers both a
resolver that abstained on a tie and one that had no repair path at all;
``mis_repaired`` covers both settling on a value that held the majority and
picking the wrong target among several. The counts cannot tell them apart, so
they cannot say what to fix.

This module REFINES the terminal states. It does not replace them: every
category declares exactly one parent state, and a category assignment can never
move an error between states, so no published funnel figure changes meaning.

What a category is allowed to read
----------------------------------
Only the evidence a scored run already stores against the error: its terminal
state, its realized vote (how many renderings carried the corrupted value, how
many the clean one), its partition, its Σ constraint kind, and its injection
layer. Never the error's CLASS as a shortcut — "cardinality violations fail" is
a correlation to be discovered from the data, not a rule to be written into the
classifier that then rediscovers itself.

Unclassified is a result
------------------------
An error no rule matches is :data:`UNCLASSIFIED`, carried with the evidence
that was insufficient. It is never nearest-fitted into a neighbouring category.
A taxonomy that always classifies everything cannot be shown to be incomplete,
and the unclassified count is the only signal that it is.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

__all__ = [
    "CATEGORIES",
    "EVALUATION_CRITERIA",
    "FailureCategory",
    "UNCLASSIFIED",
    "classify_row",
    "classify_rows",
    "evaluate_taxonomy",
]

UNCLASSIFIED = "unclassified"


@dataclass(frozen=True)
class FailureCategory:
    """One mechanism, its parent state, its rule, and the fix it implies."""

    name: str
    parent_state: str
    reads: tuple[str, ...]
    remedy: str
    rule: Callable[[Mapping[str, Any]], bool]

    def matches(self, row: Mapping[str, Any]) -> bool:
        return row.get("state") == self.parent_state and self.rule(row)


def _vote(row: Mapping[str, Any]) -> tuple[int, int]:
    v = row.get("realized_vote") or {}
    return int(v.get("corrupted", 0)), int(v.get("clean", 0))


def _outnumbered(row: Mapping[str, Any]) -> bool:
    bad, good = _vote(row)
    return good > bad


def _tied(row: Mapping[str, Any]) -> bool:
    bad, good = _vote(row)
    return good == bad and good > 0


def _unopposed(row: Mapping[str, Any]) -> bool:
    """The corrupted value is the only value present — nothing to disagree."""
    _bad, good = _vote(row)
    return good == 0


def _holds_majority(row: Mapping[str, Any]) -> bool:
    bad, good = _vote(row)
    return bad > good and good > 0


#: The taxonomy. Order matters only within a parent state, and every rule is
#: written so that at most one fires — the DISCRIMINATING criterion checks that
#: claim against the data rather than trusting it.
CATEGORIES: tuple[FailureCategory, ...] = (
    # ---- extraction: already mechanism-level, kept verbatim ---------------- #
    FailureCategory(
        "extraction_lost_claim", "missed_at_extraction", ("state",),
        "make reading recover the claim, or accept the ceiling it imposes on "
        "everything downstream",
        lambda row: True),
    FailureCategory(
        "extraction_laundered_value", "laundered_at_extraction", ("state",),
        "make reading preserve the value it read, rather than normalising the "
        "error away into a plausible one",
        lambda row: True),
    FailureCategory(
        "extraction_lost_counterpart", "counterpart_lost", ("state",),
        "make reading preserve the CLEAN rendering; losing it leaves the lie "
        "unopposed and no checker can recover",
        lambda row: True),

    # ---- not detected ------------------------------------------------------ #
    FailureCategory(
        "no_contradiction_available", "not_detected",
        ("state", "realized_vote"),
        "nothing to fix in the checker: add redundancy so the fact is stated "
        "more than once, or add an external-knowledge check",
        _unopposed),
    FailureCategory(
        "contradiction_unflagged", "not_detected",
        ("state", "realized_vote"),
        "fix the checker: two renderings disagreed in the graph and no finding "
        "was raised",
        lambda row: not _unopposed(row)),

    # ---- detected but not repaired ---------------------------------------- #
    FailureCategory(
        "tie_abstained", "detected_only", ("state", "realized_vote"),
        "give the resolver a tiebreak that is not a count — source "
        "independence, authority, or recency",
        _tied),
    FailureCategory(
        "no_repair_path", "detected_only", ("state", "realized_vote"),
        "give this fault kind a repair operator: the conflict was resolvable "
        "by count and still nothing was chosen",
        lambda row: not _tied(row)),

    # ---- repaired to the wrong value --------------------------------------- #
    FailureCategory(
        "majority_wrong", "mis_repaired", ("state", "realized_vote"),
        "stop treating renderings as independent votes — weight by source "
        "independence or authority, or the popular value always wins",
        _holds_majority),
    FailureCategory(
        "tie_broken_wrong", "mis_repaired", ("state", "realized_vote"),
        "the tiebreak itself is wrong: with equal support it chose the "
        "corrupted value",
        _tied),
    FailureCategory(
        "minority_chosen", "mis_repaired", ("state", "realized_vote"),
        "the ranking is actively inverted: the corrupted value was outnumbered "
        "and still won, which is worse than counting",
        _outnumbered),
    FailureCategory(
        "unopposed_wrong", "mis_repaired", ("state", "realized_vote"),
        "nothing to rank against: the corrupted value was the only one present "
        "and was canonicalised, so redundancy is the fix",
        _unopposed),

    # ---- succeeded --------------------------------------------------------- #
    FailureCategory(
        "repaired_by_majority", "repaired", ("state", "realized_vote"),
        "none — this is the mechanism working: the clean value outnumbered the "
        "corrupted one",
        _outnumbered),
    FailureCategory(
        "repaired_against_majority", "repaired", ("state", "realized_vote"),
        "none — and notable: the repair was correct WITHOUT a clean majority, "
        "so something other than counting decided it",
        lambda row: not _outnumbered(row)),

    # ---- operator action ---------------------------------------------------- #
    FailureCategory(
        "operator_removed", "removed_from_scope", ("state",),
        "none — an operator correction removed the conflict; not the engine's "
        "outcome either way",
        lambda row: True),
)


def classify_row(row: Mapping[str, Any]) -> tuple[str, list[str]]:
    """(category, all matching category names). More than one match is a BUG in
    the taxonomy and is surfaced rather than resolved by declaration order."""
    hits = [c.name for c in CATEGORIES if c.matches(row)]
    if not hits:
        return UNCLASSIFIED, []
    return hits[0], hits


def classify_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        category, hits = classify_row(row)
        entry = {**row, "failure_category": category}
        if category is UNCLASSIFIED or len(hits) > 1:
            entry["classification_evidence"] = {
                "state": row.get("state"),
                "realized_vote": row.get("realized_vote"),
                "partition": row.get("partition"),
                "constraint_kind": row.get("constraint_kind"),
                "layer": row.get("layer"),
                "multiple_matches": hits if len(hits) > 1 else None,
            }
        out.append(entry)
    return out


#: The five criteria, PINNED before the scaled run (req.tax.evaluation). Each
#: yields a number, not a verdict — the report says what the taxonomy scored,
#: including where it scored badly.
EVALUATION_CRITERIA: tuple[str, ...] = (
    "total",           # every graded error gets exactly one category
    "non_vacuous",     # which categories the corpus actually populates
    "discriminating",  # no two categories select the same error set
    "stable",          # one signature maps to one category across seeds
    "actionable",      # observed categories name distinct remedies
)


def _signature(row: Mapping[str, Any]) -> tuple:
    bad, good = _vote(row)
    shape = ("unopposed" if good == 0 else "tied" if good == bad
             else "clean_majority" if good > bad else "corrupt_majority")
    return (row.get("corruption_class"), row.get("partition"),
            row.get("constraint_kind"), shape)


def evaluate_taxonomy(
    classified: Sequence[Mapping[str, Any]],
    *,
    categories: Sequence[FailureCategory] = CATEGORIES,
) -> dict[str, Any]:
    """Grade the taxonomy against the five pinned criteria. Able to fail."""
    total = len(classified)
    unclassified = [r for r in classified if r["failure_category"] == UNCLASSIFIED]
    multi = [r for r in classified
             if (r.get("classification_evidence") or {}).get("multiple_matches")]
    seen = Counter(r["failure_category"] for r in classified)
    declared = [c.name for c in categories]
    observed = [n for n in declared if seen.get(n)]
    unobserved = [n for n in declared if not seen.get(n)]

    # DISCRIMINATING: two categories that select the same set of errors are
    # aliases — a distinction the data never makes is not a distinction.
    members: dict[str, frozenset] = {}
    for name in observed:
        members[name] = frozenset(
            str(r.get("corruption_id")) for r in classified
            if r["failure_category"] == name
        )
    aliases = [
        sorted((a, b)) for i, a in enumerate(observed) for b in observed[i + 1:]
        if members[a] == members[b]
    ]

    # STABLE: one signature should map to one category.
    by_sig: dict[tuple, set] = defaultdict(set)
    for r in classified:
        by_sig[_signature(r)].add(r["failure_category"])
    split = {str(k): sorted(v) for k, v in by_sig.items() if len(v) > 1}
    stable_rate = (
        (len(by_sig) - len(split)) / len(by_sig) if by_sig else None
    )

    # ACTIONABLE: two observed categories naming the same remedy collapse.
    remedy = {c.name: c.remedy for c in categories}
    by_remedy: dict[str, list[str]] = defaultdict(list)
    for name in observed:
        by_remedy[remedy[name]].append(name)
    collapse = [sorted(v) for v in by_remedy.values() if len(v) > 1]

    return {
        "schema": "okg.synth.failure-taxonomy-evaluation.v1",
        "criteria": list(EVALUATION_CRITERIA),
        "total": {
            "graded": total,
            "classified": total - len(unclassified),
            "unclassified": len(unclassified),
            "rate": (total - len(unclassified)) / total if total else None,
            "multiple_matches": len(multi),
            "unclassified_evidence": [
                r.get("classification_evidence") for r in unclassified[:20]
            ],
        },
        "non_vacuous": {
            "declared": len(declared),
            "observed": observed,
            "unobserved": unobserved,
            "counts": {n: seen[n] for n in observed},
        },
        "discriminating": {"alias_pairs": aliases, "passed": not aliases},
        "stable": {
            "signatures": len(by_sig),
            "split_signatures": split,
            "rate": stable_rate,
            "passed": not split,
        },
        "actionable": {"collapse_candidates": collapse, "passed": not collapse},
    }
