"""PACT ``concord-formal-currency-t1`` — refinement tests binding the Lean
development's formal objects to their engine counterparts.

The mapping table in ``designs/formal-statements.md`` §4 is the contract:
each row here either witnesses the binding or exercises the engine mechanism
the formal side assumes. What the Lean proofs do NOT establish (ext for real
queries, version faithfulness, Δ over-approximation, recorded-correctness)
is exactly what these tests exercise empirically — the two halves are
complements, never substitutes.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from okg.bench.synth.e1_support_stress import (
    WorldState,
    build_world,
    term_value,
)
from okg.substrate.consistency.invariant_spec import (
    CLASSIFICATION_INSTANCE,
    CLASSIFICATION_KIND_LEVEL,
    CLASSIFICATION_WHOLE_GRAPH,
    SpecCertificate,
    SpecLoadRefused,
    classify_program,
)
from okg.substrate.consistency.reuse_certificates import (
    CertificateDependencies,
    ReuseCertificate,
    validate_certificate,
)
from tests.substrate.consistency._invariant_spec_support import (
    aggregate_program,
    annotation_reader_program,
    subject_local_program,
)

_LEAN_DIR = (
    Path(__file__).resolve().parents[3] / "formal" / "consistency" / "lean"
)
_LEAN_SOURCE = _LEAN_DIR / "Currency.lean"


# --------------------------------------------------------------------------- #
# The Lean artifact's structural guards (source-level; the build + axiom      #
# report are the recorded formal_check evidence).                              #
# --------------------------------------------------------------------------- #
def test_lean_sources_carry_no_sorry_or_admit() -> None:
    source = _LEAN_SOURCE.read_text()
    for token in ("sorry", "admit"):
        assert not re.search(rf"\b{token}\b", source), (
            f"the Lean development contains {token!r} — nothing is called "
            "machine-checked with a gap"
        )


def test_lean_statements_keep_the_reviewed_shapes() -> None:
    """The review-mandated statement shapes (formal-statements.md v2) are
    present verbatim in the source: the killRule's explicit conjunction over
    the survival disjunction, the basedAt0 base, and the two theorem names
    the evidence rows bind."""
    source = _LEAN_SOURCE.read_text()
    assert "def killRule" in source
    assert "def basedAt0" in source
    assert "theorem t1_skip_sound" in source
    assert "theorem currency" in source
    assert "theorem t1_cross_pass" in source
    # the parenthesized conjunction: survival disjunction AND the
    # surviving-⇒-untouched clause, both under the same quantifiers.
    assert "(L n c ∨ c.stamp = n+1)" in source
    assert "(L n c → disjoint c.eval.read (Δ n))" in source


# --------------------------------------------------------------------------- #
# Row: CollId ↔ the loader's classifier (the join demotion, by type).          #
# --------------------------------------------------------------------------- #
def _certificate(classification: str, read_collections):
    return SpecCertificate(
        invariant_id="inv", invariant_version=1, subject="s",
        classification=classification,
        read_facts=("fact:1",),
        read_collections=read_collections,
        result_digest="sha256:r", pass_stamp="pass:1",
        invariant_digest="sha256:i", spec_digest="sha256:s",
    )


def test_join_and_aggregate_queries_yield_no_collection_ids() -> None:
    """A join-defined enumeration has no `CollId`: the classifier demotes the
    query, and the certificate constructor refuses membership versions for
    it — the formal type restriction and the engine refusal are the same
    rule."""
    demoted = {
        "annotation-join": classify_program(
            annotation_reader_program("refine.join.v1", "day-count")
        ),
        "aggregate": classify_program(aggregate_program("refine.agg.v1")),
    }
    assert demoted["annotation-join"] == CLASSIFICATION_KIND_LEVEL
    assert demoted["aggregate"] == CLASSIFICATION_WHOLE_GRAPH
    for classification in demoted.values():
        with pytest.raises(SpecLoadRefused):
            _certificate(classification, (("coll:x", "v1"),))
    # a selections-only query keeps its CollIds.
    assert classify_program(
        subject_local_program("refine.local.v1")
    ) == CLASSIFICATION_INSTANCE
    assert _certificate(
        CLASSIFICATION_INSTANCE, (("coll:x", "v1"),)
    ).read_collections


# --------------------------------------------------------------------------- #
# Row: ReadSet ↔ Certificate read_facts + read_collections (shared shape).     #
# --------------------------------------------------------------------------- #
def _lean_declaration_body(source: str, keyword: str, name: str) -> str:
    """The named top-level declaration's OWN body, and nothing past it.

    A pattern of the form ``<keyword> <name>.*?<fields>`` under ``re.DOTALL``
    does NOT bind ``name``. Non-greedy only prefers the SHORTEST match; when
    ``name``'s own body stops matching, the search slides forward and matches
    the next declaration whose body happens to be identical. ``Currency.lean``
    declares ``ChangeSet`` with a byte-identical field list twenty-five lines
    after ``ReadSet``, so the sliding match reported success against a
    ``ReadSet`` whose fields had been replaced with unrelated ones (audit,
    2026-08-29 — the same slide an earlier review had already reported).

    This reads the declaration line and then consumes only the lines indented
    under it, stopping at the next line that starts in column zero. An
    assertion over the result therefore cannot be satisfied by any other
    declaration in the file.
    """
    lines = source.splitlines()
    head = f"{keyword} {name}"
    for index, line in enumerate(lines):
        if not line.startswith(head):
            continue
        rest = line[len(head):]
        if rest and (rest[0].isalnum() or rest[0] == "_"):
            continue  # a longer name that merely starts with this one
        body = [line]
        for following in lines[index + 1:]:
            if following.strip() and not following[0].isspace():
                break
            body.append(following)
        return "\n".join(body)
    raise AssertionError(
        f"the Lean source declares no top-level `{keyword} {name}`"
    )


#: The fields the Lean read set must declare, checked inside ITS OWN body.
_READSET_FIELDS = ("facts : Finset Fact", "colls : Finset CollId")


def test_readset_shape_matches_certificate_fields() -> None:
    """The Lean `ReadSet` is {facts, colls}; the engine certificate stores
    {read_facts, read_collections}.

    NOT field-for-field, and the difference is worth stating rather than
    glossing (this docstring read "Field-for-field" until the 2026-08-29
    audit). `facts` and `read_facts` are both fact ids and do correspond
    directly. `colls` holds collection NAMES only — the membership version
    lives in `State.mem : CollId -> Version`, which the read set indexes into —
    while `read_collections` stores the (name, version) PAIR, because the
    engine has no separate state object to read the version back out of at
    check time. The pair is the Lean `colls` entry together with the value
    `State.mem` held for it when the certificate was recorded, so the two carry
    the same information under a different split.
    """
    body = _lean_declaration_body(_LEAN_SOURCE.read_text(), "structure", "ReadSet")
    for field in _READSET_FIELDS:
        assert field in body, (
            f"the Lean ReadSet no longer declares `{field}`; its body is:\n{body}"
        )
    payload = _certificate(
        CLASSIFICATION_INSTANCE, (("annotations:hr:day-count", "v3"),)
    ).to_canonical_dict()
    assert payload["read_facts"] == ["fact:1"]
    assert payload["read_collections"] == [["annotations:hr:day-count", "v3"]]


def test_the_readset_check_fails_when_the_lean_readset_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NON-VACUITY: break the named declaration and the shape check must FAIL.

    This check is the only thing binding the machine-checked currency/T1
    development to the engine it is cited about, so a version of it that
    survives an arbitrary rewrite of the object it names binds nothing. Until
    2026-08-29 it did survive exactly that: the mutation below left it passing,
    because its pattern slid onto `ChangeSet`. The witness keeps `ChangeSet`
    intact on purpose — the slide target must still be present, or the mutation
    would prove nothing about anchoring.
    """
    original = _LEAN_SOURCE.read_text()
    mutated = original.replace(
        "structure ReadSet (Fact CollId : Type) where\n"
        "  facts : Finset Fact\n"
        "  colls : Finset CollId",
        "structure ReadSet (Fact CollId : Type) where\n"
        "  TOTALLY_WRONG : Nat\n"
        "  ALSO_WRONG : Bool",
    )
    assert mutated != original, "the mutation did not apply; the witness is vacuous"
    assert "structure ChangeSet" in mutated, "the slide target must survive"
    assert "  facts : Finset Fact\n  colls : Finset CollId" in mutated, (
        "ChangeSet must still carry the shape the old sliding pattern matched, "
        "or this witness does not exercise the slide"
    )
    broken = tmp_path / "Currency.lean"
    broken.write_text(mutated, encoding="utf-8")
    monkeypatch.setattr(
        "tests.substrate.consistency.test_formal_refinement._LEAN_SOURCE", broken
    )
    with pytest.raises(AssertionError, match="no longer declares"):
        test_readset_shape_matches_certificate_fields()


def test_the_declaration_reader_refuses_a_name_it_cannot_find() -> None:
    """A missing declaration REFUSES rather than returning an empty body that
    every `in` assertion would then vacuously fail or pass against."""
    with pytest.raises(AssertionError, match="no top-level"):
        _lean_declaration_body(_LEAN_SOURCE.read_text(), "structure", "NoSuchThing")


# --------------------------------------------------------------------------- #
# Row: version faithfulness — membership versions move with membership.        #
# --------------------------------------------------------------------------- #
def test_membership_version_bumps_on_insert_and_delete() -> None:
    """`S.mem c` must change whenever c's membership changes (the engine's
    selection-version bump obligation). Witnessed on the E1 world's
    membership terms: an insert AND a delete both move the version, and an
    unrelated attribute change does not."""
    world = build_world(11, updates=12)
    base = world.states[0]
    term = "members:doc"
    before = term_value(base, term)

    grown = WorldState(
        entities={**{k: dict(v) for k, v in base.entities.items()},
                  "doc-new": {"kind": "doc", "flag": "1", "val": "9"}},
        aliases=dict(base.aliases),
        relations=dict(base.relations),
        policies={k: dict(v) for k, v in base.policies.items()},
    )
    assert term_value(grown, term) != before

    survivors = {
        k: dict(v) for k, v in base.entities.items()
        if not (v.get("kind") == "doc" and k != "doc0")
    }
    shrunk = WorldState(
        entities=survivors, aliases=dict(base.aliases),
        relations=dict(base.relations),
        policies={k: dict(v) for k, v in base.policies.items()},
    )
    assert term_value(shrunk, term) != before

    bumped_entities = {k: dict(v) for k, v in base.entities.items()}
    bumped_entities["doc0"]["val"] = "77"
    bumped = WorldState(
        entities=bumped_entities, aliases=dict(base.aliases),
        relations=dict(base.relations),
        policies={k: dict(v) for k, v in base.policies.items()},
    )
    assert term_value(bumped, term) == before  # membership unchanged


# --------------------------------------------------------------------------- #
# Row: killRule — a touched named fact/version kills the certificate.          #
# --------------------------------------------------------------------------- #
def _deps(digest: str) -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={"fact:1": digest, "members:doc": "v1"},
        operator_versions={"check": "v1"},
        model_prompt_keys={},
        envelope_fingerprint="env",
        constraint_set={"check": "v1"},
        generator_versions={},
        policy_fingerprint="pol",
        precedence_fingerprint="prec",
        competitor_bounds={},
        catalog_version_id="cat",
    )


def test_certificate_invalidates_when_a_named_read_moves() -> None:
    certificate = ReuseCertificate(
        deployment="refine", generation_id="g1", step_id="s1",
        comparison={
            "kind": "degenerate_unchanged_inputs",
            "incumbent_outcome": "carry_forward",
            "incumbent_basis": "unchanged_inputs",
            "unchanged_inputs": {"statement": "refinement fixture",
                                 "dependency_classes_matched": []},
        },
        dependencies=_deps("d1"),
    )
    assert validate_certificate(_deps("d1"), certificate).valid
    verdict = validate_certificate(_deps("d2"), certificate)
    assert not verdict.valid
    assert "input_digests" in verdict.invalidated_classes


# --------------------------------------------------------------------------- #
# Row: Fact/Value serialization round-trips into fixture states.               #
# --------------------------------------------------------------------------- #
def test_dependency_snapshot_round_trips_byte_stably() -> None:
    deps = _deps("d1")
    round_tripped = CertificateDependencies.from_json_dict(
        json.loads(json.dumps(deps.to_json_dict(), sort_keys=True))
    )
    assert round_tripped.canonical_json() == deps.canonical_json()
    digest = hashlib.sha256(
        deps.canonical_json().encode("utf-8")
    ).hexdigest()
    assert hashlib.sha256(
        round_tripped.canonical_json().encode("utf-8")
    ).hexdigest() == digest


# --------------------------------------------------------------------------- #
# §5: the counterexample classes are discharged by the premises.               #
# --------------------------------------------------------------------------- #
def test_counterexample_classes_never_construct_a_certified_read_set() -> None:
    """Absence-style reads are covered only where a CollId exists
    (selections); joins and aggregates classify demoted and never construct
    a `ReadSet` with membership versions — asserting the classifier + the
    constructor refusal per class family."""
    for program, expected in (
        (annotation_reader_program("refine.cx.join", "k"),
         CLASSIFICATION_KIND_LEVEL),
        (aggregate_program("refine.cx.agg"), CLASSIFICATION_WHOLE_GRAPH),
    ):
        classification = classify_program(program)
        assert classification == expected
        with pytest.raises(SpecLoadRefused):
            _certificate(classification, (("coll:k", "v1"),))
