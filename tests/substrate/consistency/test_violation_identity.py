"""PACT ``concord-spec-views-contract``, req.svc.identity — violation
identity and canonical-order witnesses.

Duplicate-row collapse, re-observation stability, judged-parser determinism
with the whole-subject fallback, clone/split non-credit, and canonical-queue
output determinism under shuffled arrival order within the initial batch and
within each discovery step, replayed from a recorded discovery structure.
"""
from __future__ import annotations

import random
import re

import pytest

from okg.substrate.consistency.violation_identity import (
    CanonicalQueue,
    IdentityError,
    WitnessParserRegistry,
    canonical_sort_key,
    compare_canonical,
    distinct_violations,
    formal_violation_identity,
    judged_violation_identity,
)


# --------------------------------------------------------------------------- #
# Formal identity.                                                             #
# --------------------------------------------------------------------------- #
def _row(**cols):
    return dict(cols)


def test_same_key_different_value_is_the_same_violation() -> None:
    """A re-observation with a different offending VALUE collapses: identity
    excludes value columns by construction."""
    first = formal_violation_identity(
        invariant_id="day-counts-match", invariant_version=5,
        subject="doc://hr/handbook",
        witness_key_columns=["document", "annotation_id"],
        row=_row(document="handbook", annotation_id="a-17",
                 observed="21", expected="25"),
    )
    reobserved = formal_violation_identity(
        invariant_id="day-counts-match", invariant_version=5,
        subject="doc://hr/handbook",
        witness_key_columns=["document", "annotation_id"],
        row=_row(document="handbook", annotation_id="a-17",
                 observed="19", expected="25"),
    )
    assert first == reobserved
    assert len(distinct_violations([first, reobserved])) == 1


def test_different_key_or_version_is_a_different_violation() -> None:
    base = dict(
        invariant_id="day-counts-match", invariant_version=5,
        subject="doc://hr/handbook",
        witness_key_columns=["annotation_id"],
    )
    a = formal_violation_identity(**base, row=_row(annotation_id="a-1"))
    b = formal_violation_identity(**base, row=_row(annotation_id="a-2"))
    c = formal_violation_identity(
        **{**base, "invariant_version": 6}, row=_row(annotation_id="a-1"))
    assert len(distinct_violations([a, b, c])) == 3


def test_missing_witness_key_column_refuses() -> None:
    with pytest.raises(IdentityError):
        formal_violation_identity(
            invariant_id="x", invariant_version=1, subject="s",
            witness_key_columns=["absent"], row=_row(present="1"),
        )


def test_clone_and_split_mutations_mint_no_credit() -> None:
    """Duplicated rows and split rows (same key, different junk values)
    count once — counting is over equivalence classes."""
    rows = [
        _row(document="handbook", annotation_id="a-17", observed="21"),
        _row(document="handbook", annotation_id="a-17", observed="21"),  # clone
        _row(document="handbook", annotation_id="a-17", observed="7"),   # split
        _row(document="handbook", annotation_id="a-17", observed="9",
             extra="junk"),                                              # split
    ]
    identities = [
        formal_violation_identity(
            invariant_id="day-counts-match", invariant_version=5,
            subject="doc://hr/handbook",
            witness_key_columns=["document", "annotation_id"], row=row,
        )
        for row in rows
    ]
    assert len(distinct_violations(identities)) == 1


# --------------------------------------------------------------------------- #
# Judged identity.                                                             #
# --------------------------------------------------------------------------- #
@pytest.fixture()
def parsers() -> WitnessParserRegistry:
    registry = WitnessParserRegistry()
    section = re.compile(r"^SECTION: (.+)$", re.MULTILINE)
    registry.register(
        "parser://section-header@v1", "regex",
        lambda text: tuple(section.findall(text)),
    )
    registry.register(
        "parser://always-raises@v1", "regex",
        lambda text: (_ for _ in ()).throw(ValueError("boom")),
    )
    return registry


def test_judged_parser_is_deterministic(parsers) -> None:
    verdict = "FAIL\nSECTION: vacation\nSECTION: sick-leave\n"
    first = judged_violation_identity(
        invariant_id="no-contradiction", invariant_version=1,
        subject="doc://hr/runbook", verdict_text=verdict,
        witness_parser="parser://section-header@v1", registry=parsers,
    )
    for _ in range(3):
        again = judged_violation_identity(
            invariant_id="no-contradiction", invariant_version=1,
            subject="doc://hr/runbook", verdict_text=verdict,
            witness_parser="parser://section-header@v1", registry=parsers,
        )
        assert again == first
    assert first.witness_key == ("vacation", "sick-leave")


def test_parser_failure_and_empty_extraction_fall_back_to_whole_subject(
    parsers,
) -> None:
    kwargs = dict(
        invariant_id="no-contradiction", invariant_version=1,
        subject="doc://hr/runbook", registry=parsers,
    )
    raised = judged_violation_identity(
        **kwargs, verdict_text="FAIL",
        witness_parser="parser://always-raises@v1",
    )
    empty = judged_violation_identity(
        **kwargs, verdict_text="FAIL — no sections named",
        witness_parser="parser://section-header@v1",
    )
    bare = judged_violation_identity(**kwargs, verdict_text="FAIL")
    # all three are the whole-subject violation: at most ONE per subject per
    # judged invariant.
    assert raised == empty == bare
    assert raised.witness_key == ()
    assert len(distinct_violations([raised, empty, bare])) == 1


def test_wrong_typed_parser_returns_are_refused_not_exploded(parsers) -> None:
    """EWD-SVC-8: a parser returning a bare string must not be exploded per
    character, and empty-string or non-string components refuse — silent
    identity corruption is worse than a loud authoring error."""
    parsers.register("parser://returns-string@v1", "regex", lambda t: "abc")
    parsers.register("parser://returns-empty-str@v1", "regex",
                     lambda t: ("",))
    parsers.register("parser://returns-int@v1", "regex", lambda t: (1,))
    for ref in ("parser://returns-string@v1", "parser://returns-empty-str@v1",
                "parser://returns-int@v1"):
        with pytest.raises(IdentityError):
            parsers.extract(ref, "FAIL")


def test_model_backed_parsers_are_refused_at_registration(parsers) -> None:
    with pytest.raises(IdentityError):
        parsers.register("parser://llm-extract@v1", "model", lambda t: (t,))
    with pytest.raises(IdentityError):  # re-registration is a new ref, never
        parsers.register("parser://section-header@v1", "regex",
                         lambda t: (t,))
    with pytest.raises(IdentityError):  # unknown ref never falls back silently
        parsers.extract("parser://nope@v1", "x")


# --------------------------------------------------------------------------- #
# The canonical order.                                                         #
# --------------------------------------------------------------------------- #
def test_prefix_rule_sorts_repair_immediately_after_its_evaluation() -> None:
    evaluation = canonical_sort_key(["inv-a", "subj-1"])
    violation = canonical_sort_key(["inv-a", "subj-1", "wk-1"])
    next_subject = canonical_sort_key(["inv-a", "subj-2"])
    assert compare_canonical(evaluation, violation) == -1   # prefix first
    assert compare_canonical(violation, next_subject) == -1  # before subj-2
    assert compare_canonical(evaluation, evaluation) == 0


def test_comparison_is_bytewise_locale_free() -> None:
    # 'Z' < 'a' in bytes; a locale-aware comparator would disagree.
    assert compare_canonical(
        canonical_sort_key(["Z"]), canonical_sort_key(["a"])
    ) == -1


def test_queue_output_is_deterministic_under_shuffled_arrival() -> None:
    """The recorded property: shuffle arrival order within the initial batch
    and within each discovery step (replayed from a recorded discovery
    structure) — the popped sequence is identical across shuffles."""
    initial = [("inv-b", "s2"), ("inv-a", "s1"), ("inv-a", "s2"),
               ("inv-c", "s1")]
    # the recorded discovery structure: popping an item discovers others,
    # inserted mid-pass at canonical position.
    discoveries = {
        ("inv-a", "s1"): [("inv-a", "s1", "wk2"), ("inv-a", "s1", "wk1")],
        ("inv-b", "s2"): [("inv-a", "s9"), ("inv-c", "s0")],
        ("inv-a", "s1", "wk1"): [("inv-b", "s0")],
    }

    def run(seed: int) -> list[tuple]:
        rng = random.Random(seed)
        queue = CanonicalQueue()
        batch = list(initial)
        rng.shuffle(batch)                      # shuffle the initial batch
        for item in batch:
            queue.push(item, item)
        popped: list[tuple] = []
        while len(queue):
            _key, item = queue.pop()
            popped.append(item)
            step = list(discoveries.get(item, ()))
            rng.shuffle(step)                   # shuffle each discovery step
            for discovered in step:
                queue.push(discovered, discovered)
        return popped

    reference = run(0)
    for seed in range(1, 8):
        assert run(seed) == reference
    # and the sequence respects the canonical order relative to what had
    # already been admitted: the violation wk-items follow their evaluation
    # immediately, before inv-a's later subjects.
    a_s1 = reference.index(("inv-a", "s1"))
    assert reference[a_s1 + 1] == ("inv-a", "s1", "wk1")
    assert reference[a_s1 + 2] == ("inv-a", "s1", "wk2")


def test_queue_refuses_empty_pop_and_empty_key() -> None:
    queue = CanonicalQueue()
    with pytest.raises(IdentityError):
        queue.pop()
    with pytest.raises(IdentityError):
        queue.push([], "x")
