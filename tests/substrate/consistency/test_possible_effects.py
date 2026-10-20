"""The possible-effects compiler over declared write envelopes (spec §9.4).

PACT ``consistency-resolution-aware-reuse``, task.reuse.effects-compiler
(req.reuse.sound-effects). This module lands the two witnesses named VERBATIM by
docs/develop/architecture/consistency.md §9.4 —
``test_omitted_effect_is_caught_by_harness`` and
``test_unbounded_envelope_yields_run_always`` — plus the compiler's finite-
envelope, ir_locality-agreement, typed-refusal, and sound-skip obligations from
the task's verification clause.

The fast tests (no DB) exercise the pure compiler + skip helper against the
in-tree ``consistency-fixture`` deployment's real IR checks. The two postgres
witnesses run the live equivalence harness
(:func:`okg.substrate.qa.complete_reference.run_complete_reference` +
:func:`okg.substrate.consistency.equivalence_differ.diff_resolved_states`) over a
real production publish cycle on the fixture — the same idioms as
tests/substrate/consistency/test_equivalence_harness.py and
tests/substrate/consistency/test_claim_plane_wire.py. The equivalence
machinery (``complete_reference`` / ``equivalence_differ``) is consumed READ-ONLY;
this module compiles summaries and asks the skip question, it does not
re-implement diffing.
"""
from __future__ import annotations

import dataclasses
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency.equivalence_differ import (  # noqa: E402
    FAMILY_EFFECTIVE,
    FAMILY_RESOLUTION,
    VERDICT_EQUAL,
    VERDICT_MISMATCH,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.consistency.ir_check import (  # noqa: E402
    IrConsistencyCheckSpec,
    load_ir_consistency_checks,
)
from okg.substrate.consistency.ir_locality import (  # noqa: E402
    CLAIM_STATE_FULL_REASON,
    derive_frontier_plan,
)
from okg.substrate.consistency.possible_effects import (  # noqa: E402
    REFUSE_BUCKET_OVERLAP,
    REFUSE_CHECK_OVERLAP,
    REFUSE_RUN_ALWAYS,
    REFUSE_UNKNOWN_TERM,
    REFUSE_UNSTATED_CHANGE,
    SKIP_APPROVED,
    UNSTATED_CHANGE,
    BucketReach,
    ChangedStateTerms,
    PossibleEffects,
    RunAlways,
    UnstatedChange,
    check_surface_reach,
    compile_possible_effects,
    skip_is_justifiable,
)
from okg.substrate.deployments import inject_deployment_path  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    bucket_row_key,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.producer_authority import (  # noqa: E402
    normalize_write_envelope,
)
from okg.substrate.qa.complete_reference import (  # noqa: E402
    OutcomeReplaySource,
    run_complete_reference,
)
from okg.substrate.transform import (  # noqa: E402
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
)
from okg.substrate.transform.expr import (  # noqa: E402
    and_,
    col,
    eq,
    is_null,
    lit,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_KEY1 = "pref|k1"
_KEY2 = "pref|k2"
_POLICY = "pol:fixture"

_CONTESTED_CHECK = "consistency-fixture.ir.claim_state.contested_bucket.v1"
_RETIRED_CHECK = "consistency-fixture.ir.claim_state.canonical_retired_target.v1"


def _fixture_specs() -> list[IrConsistencyCheckSpec]:
    """The real consistency-fixture IR checks (the Σ read surfaces the compiler
    propagates envelopes through). Loading imports the program module, so the
    deployment root must be on sys.path — DB-free."""
    inject_deployment_path(_DEP_DIR)
    return load_ir_consistency_checks(_DEP_DIR)


# --------------------------------------------------------------------------- #
# Fast, pure-logic witnesses (no Postgres).                                    #
# --------------------------------------------------------------------------- #
def test_finite_envelope_compiles_summary_across_fixture_checks() -> None:
    """A finite claim envelope compiles to a summary (never RunAlways) whose
    reach names the touched fixture checks + the concrete bucket it writes — the
    'summaries compile for finite-envelope steps across fixture constraint sets'
    clause."""
    specs = _fixture_specs()
    assert {s.check_id for s in specs} == {_CONTESTED_CHECK, _RETIRED_CHECK}

    env = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
        source_name="registry-producer",
    )
    effects = compile_possible_effects(
        env, checks=specs, claim_predicates=[_PREDICATE],
        producer_id="registry-producer",
    )
    assert isinstance(effects, PossibleEffects)
    # both claim-state checks are touched (recorded in check_ids); the bucket is
    # the one concrete (predicate, key) the envelope declares.
    assert effects.claim_state_check_ids == {_CONTESTED_CHECK, _RETIRED_CHECK}
    assert effects.check_ids == {_CONTESTED_CHECK, _RETIRED_CHECK}
    assert not effects.graph_check_ids  # claim-state checks are not coarse graph terms
    assert effects.bucket_reach == BucketReach(
        concrete=frozenset({(_PREDICATE, _KEY1)})
    )
    # the selection terms the bucket implies == the bucket reach (v1, no scoring).
    assert effects.selection_reach() == effects.bucket_reach
    print(f"\n[possible-effects] finite envelope -> {effects.to_canonical_dict()}")


def test_over_approximation_bounds_are_conservative_not_guessed() -> None:
    """Where the key space is not enumerable the bucket dimension is a declared-
    conservative bound (predicate-wildcard when the predicate is known, all-buckets
    when it is not) — over-approximation, never a narrowing guess."""
    specs = _fixture_specs()

    # predicate known, key space open -> predicate-wildcard (every key under it).
    env_wild = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}]}, source_name="p-wild",
    )
    eff_wild = compile_possible_effects(
        env_wild, checks=specs, claim_predicates=[_PREDICATE], producer_id="p-wild",
    )
    assert isinstance(eff_wild, PossibleEffects)
    assert eff_wild.bucket_reach == BucketReach(predicates=frozenset({_PREDICATE}))
    assert eff_wild.bucket_reach.touches(_PREDICATE, "any-key-at-all")

    # predicate unknown -> the top all-buckets sentinel (still finite envelope).
    env_all = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}]}, source_name="p-all",
    )
    eff_all = compile_possible_effects(env_all, checks=specs, producer_id="p-all")
    assert isinstance(eff_all, PossibleEffects)
    assert eff_all.bucket_reach == BucketReach(all_buckets=True)
    assert eff_all.bucket_reach.touches("any_predicate", "any_key")


def test_unbounded_envelope_yields_run_always() -> None:
    """§9.4 witness (named verbatim): an envelope with unbounded ``claim_keys``
    compiles to the RunAlways sentinel — no summary exists, the step runs — and
    ``skip_is_justifiable`` refuses categorically with the typed run_always
    reason. RunAlways is a sentinel TYPE, never None."""
    specs = _fixture_specs()
    env = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [],
         "unbounded": ["claim_keys"]},
        source_name="unbounded-producer",
    )
    result = compile_possible_effects(
        env, checks=specs, claim_predicates=[_PREDICATE],
        producer_id="unbounded-producer",
    )
    assert isinstance(result, RunAlways)
    assert result is not None
    assert result.unbounded_dimensions == ("claim_keys",)
    assert result.digest()  # canonical-JSON serializable with a stable digest

    # refuses categorically — even against an EXPLICIT "nothing changed", the
    # most permissive statement a caller can make.
    judgment = skip_is_justifiable(
        result, changed_state_terms=ChangedStateTerms.nothing_changed(),
    )
    assert judgment.approved is False
    assert judgment.reason == REFUSE_RUN_ALWAYS
    print(f"\n[possible-effects] unbounded envelope -> {result.to_canonical_dict()}; "
          f"skip refused: {judgment.reason}")


def _subject_local_program() -> TransformProgram:
    """FROM nodes_live only (single node surface, no joins) -> subject_local."""
    return TransformProgram(
        program_id="fixture.possible_effects.subject_local",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "page_probe",
            "normalized_predicate": "page_flag",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.WHERE, {"expr": eq(col("n.subtype"), lit("page"))}),
                        output="f", inputs=("n",)),
                Binding(Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                        output="offending", inputs=("f",)),
                Binding(Op(OpKind.EMIT, {"target": "offending"}), inputs=("offending",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN, {"values": ["offending"]}),
        ),),
    )


def _incident_edges_program() -> TransformProgram:
    """FROM nodes_live + edges_live (orphan anti-join) -> node_with_incident_edges."""
    return TransformProgram(
        program_id="fixture.possible_effects.incident_edges",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"check": {
            "predicate_key": "orphan_probe",
            "normalized_predicate": "orphan",
            "participant_roles": {"page": "node_id"},
            "severity": "low",
            "normalized_value_columns": ["node_id"],
        }},
        blocks=(Block(
            block_id="entry",
            bindings=(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}),
                        output="n"),
                Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}),
                        output="e"),
                Binding(Op(OpKind.JOIN, {"how": "left", "on": and_(
                    eq(col("e.dst"), col("n.node_id")),
                    eq(col("e.edge_type"), lit("links_to")))}),
                        output="lj", inputs=("n", "e")),
                Binding(Op(OpKind.WHERE, {"expr": and_(
                    is_null(col("e.edge_id")),
                    eq(col("n.subtype"), lit("page")))}),
                        output="orph", inputs=("lj",)),
                Binding(Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                        output="offending", inputs=("orph",)),
                Binding(Op(OpKind.EMIT, {"target": "offending"}), inputs=("offending",)),
            ),
            terminator=Terminator(TerminatorKind.RETURN, {"values": ["offending"]}),
        ),),
    )


def _graph_spec(name: str, program: TransformProgram) -> IrConsistencyCheckSpec:
    return IrConsistencyCheckSpec(
        name=name, check_id=f"fixture.possible_effects.{name}",
        predicate_family="fixture_structure", confidence_type="deterministic_boolean",
        program=program, program_ref=f"fixture.possible_effects.{name}",
    )


def test_reach_agrees_with_ir_locality_on_graph_surface_check() -> None:
    """The compiler's surface classification of a nodes_live-based check AGREES
    with ir_locality's frontier plan for the same program (assert consistency, not
    reimplementation): a single-node-surface check is subject_local and the
    compiler sees only nodes_live; a node+incident-edges check is
    node_with_incident_edges and the compiler sees edges too. A node-writing
    envelope's touched-check set includes exactly those graph checks."""
    sl = _graph_spec("subject_local", _subject_local_program())
    ie = _graph_spec("incident_edges", _incident_edges_program())

    for spec, expect_edges, expect_mode in (
        (sl, False, "subject_local"),
        (ie, True, "node_with_incident_edges"),
    ):
        reach = check_surface_reach(spec)
        plan = derive_frontier_plan(
            spec.program, check_attrs=spec.program.attrs["check"],
            claim_state_surfaces=spec.claim_state_surface_names(),
        )
        # the compiler and ir_locality read the same FROM ops -> they must agree.
        assert reach.reads_nodes_live is True
        assert reach.reads_edges_live is expect_edges
        assert plan.mode == expect_mode
        # a graph-surface plan is neither claim-state nor no-surface.
        assert plan.reason != CLAIM_STATE_FULL_REASON
        assert not reach.reads_claim_state
        print(f"\n[possible-effects] {spec.check_id}: reach(edges={reach.reads_edges_live}) "
              f"== ir_locality.mode={plan.mode}")

    # a node-writing envelope touches exactly the two nodes_live graph checks.
    env = normalize_write_envelope(
        {"nodes": [{"subtype": "page"}]}, source_name="page-producer",
    )
    effects = compile_possible_effects(env, checks=[sl, ie], producer_id="page-producer")
    assert isinstance(effects, PossibleEffects)
    assert effects.graph_check_ids == {sl.check_id, ie.check_id}
    assert not effects.claim_state_check_ids


def test_reach_agrees_with_ir_locality_on_claim_state_check() -> None:
    """The compiler classifies the fixture's claim-state checks as claim-state
    (never graph), agreeing with ir_locality, which runs them FULL with the
    claim-state reason. A claim-writing envelope touches them; a graph-only
    envelope does not."""
    specs = _fixture_specs()
    for spec in specs:
        reach = check_surface_reach(spec)
        plan = derive_frontier_plan(
            spec.program, check_attrs=spec.program.attrs["check"],
            claim_state_surfaces=spec.claim_state_surface_names(),
        )
        assert reach.reads_claim_state
        assert not reach.reads_nodes_live and not reach.reads_edges_live
        assert not reach.unclassified_surfaces
        assert plan.mode == "full" and plan.reason == CLAIM_STATE_FULL_REASON

    # a graph-only envelope (edges) does NOT touch the claim-state checks.
    graph_env = normalize_write_envelope(
        {"edges": [{"src_subtype": "page", "edge_type": "links_to",
                    "dst_subtype": "page"}]},
        source_name="graph-only",
    )
    eff = compile_possible_effects(graph_env, checks=specs, producer_id="graph-only")
    assert isinstance(eff, PossibleEffects)
    assert not eff.claim_state_check_ids and not eff.graph_check_ids


def test_overlap_refusal_reasons_are_typed_and_named() -> None:
    """Every refusal carries a typed, named reason and names what forced it:
    a claim-bucket overlap, a graph-check overlap, an unknown term, or run_always.
    A genuinely disjoint summary is approved."""
    specs = _fixture_specs()
    env = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
        source_name="registry-producer",
    )
    effects = compile_possible_effects(
        env, checks=specs, claim_predicates=[_PREDICATE], producer_id="registry-producer",
    )
    assert isinstance(effects, PossibleEffects)

    # bucket overlap -> REFUSE_BUCKET_OVERLAP, naming the bucket.
    j_bucket = skip_is_justifiable(
        effects,
        changed_state_terms=ChangedStateTerms(buckets=frozenset({(_PREDICATE, _KEY1)})),
    )
    assert not j_bucket.approved and j_bucket.reason == REFUSE_BUCKET_OVERLAP
    assert j_bucket.overlapping_buckets == ((_PREDICATE, _KEY1),)

    # graph-check overlap -> REFUSE_CHECK_OVERLAP, naming the check.
    graph_env = normalize_write_envelope(
        {"nodes": [{"subtype": "page"}]}, source_name="page-producer",
    )
    sl = _graph_spec("subject_local", _subject_local_program())
    graph_effects = compile_possible_effects(
        graph_env, checks=[sl], producer_id="page-producer",
    )
    j_check = skip_is_justifiable(
        graph_effects,
        changed_state_terms=ChangedStateTerms(graph_check_ids=frozenset({sl.check_id})),
    )
    assert not j_check.approved and j_check.reason == REFUSE_CHECK_OVERLAP
    assert j_check.overlapping_checks == (sl.check_id,)

    # unknown term -> REFUSE_UNKNOWN_TERM (a dependency the analysis could not place).
    j_unknown = skip_is_justifiable(
        effects,
        changed_state_terms=ChangedStateTerms(unknown=True, unknown_reason="mystery dep"),
    )
    assert not j_unknown.approved and j_unknown.reason == REFUSE_UNKNOWN_TERM
    assert "mystery" in j_unknown.detail

    # genuinely disjoint (a different key bucket) -> APPROVED.
    j_ok = skip_is_justifiable(
        effects,
        changed_state_terms=ChangedStateTerms(buckets=frozenset({(_PREDICATE, _KEY2)})),
    )
    assert j_ok.approved and j_ok.reason == SKIP_APPROVED


# =========================================================================== #
# An UNSTATED change is not "nothing changed"                                  #
# (PACT consistency-unstated-change-is-not-no-change, req.unstated.fail-closed) #
# =========================================================================== #
def _bounded_claim_effects() -> PossibleEffects:
    """A finite claim-write summary over _KEY1 — the subject of the two witnesses
    below, which differ ONLY in the change statement handed to the ladder."""
    effects = compile_possible_effects(
        normalize_write_envelope(
            {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
            source_name="registry-producer",
        ),
        checks=_fixture_specs(),
        claim_predicates=[_PREDICATE],
        producer_id="registry-producer",
    )
    assert isinstance(effects, PossibleEffects)
    return effects


def test_unstated_change_refuses_a_skip() -> None:
    """THE WITNESS: silence is not the empty set. A change statement that was never
    made refuses the justified skip with its OWN typed reason — never the
    permissive reading that made omission a second, unearned skip mechanism
    alongside certificates.

    Before the fix an omitted statement arrived as ``ChangedStateTerms()``, whose
    set-disjointness test is VACUOUSLY satisfied, so this same summary was
    APPROVED (reason ``approved``)."""
    effects = _bounded_claim_effects()

    judgment = skip_is_justifiable(effects, changed_state_terms=UNSTATED_CHANGE)
    print(f"\n[unstated/refuse] {judgment.reason}: {judgment.detail}")
    assert judgment.approved is False
    assert judgment.reason == REFUSE_UNSTATED_CHANGE
    # the refusal is distinguishable from every other refusal cause.
    assert judgment.reason not in {
        REFUSE_BUCKET_OVERLAP, REFUSE_CHECK_OVERLAP, REFUSE_RUN_ALWAYS,
        REFUSE_UNKNOWN_TERM, SKIP_APPROVED,
    }
    # ...and it says what to do instead, so the fix is not a mystery refusal.
    assert "nothing_changed()" in judgment.detail

    # A freshly constructed UnstatedChange behaves identically (the singleton is a
    # convenience, not the carrier of the semantics).
    assert skip_is_justifiable(
        effects, changed_state_terms=UnstatedChange(reason="derivation not wired"),
    ).reason == REFUSE_UNSTATED_CHANGE


def test_explicit_nothing_changed_still_licenses_a_skip() -> None:
    """THE CONTROL: the permissive meaning SURVIVES. A caller that truthfully
    declares "nothing changed" keeps its reuse — this must pass both before and
    after the fix, or the fix traded one bug for another (a steady-state cycle
    that can no longer skip anything)."""
    effects = _bounded_claim_effects()

    judgment = skip_is_justifiable(
        effects, changed_state_terms=ChangedStateTerms.nothing_changed(),
    )
    print(f"\n[unstated/control] nothing_changed -> {judgment.reason}")
    assert judgment.approved is True
    assert judgment.reason == SKIP_APPROVED


def test_the_two_meanings_can_never_compare_equal() -> None:
    """The load-bearing structural property: "nothing changed" and "not stated" are
    different TYPES, so no ``==``, set membership, ``or``-default, or ``dict.get``
    fallback can silently promote one into the other. This is what stops the
    defect class ("absence dressed as agreement") from being reintroduced by a
    later refactor that reaches for a permissive default."""
    nothing = ChangedStateTerms.nothing_changed()
    assert nothing != UNSTATED_CHANGE
    assert UNSTATED_CHANGE != nothing
    assert not isinstance(UNSTATED_CHANGE, ChangedStateTerms)
    assert not isinstance(nothing, UnstatedChange)
    # ...and the permissive one is not falsy-by-accident either: a caller writing
    # `terms or UNSTATED_CHANGE` still gets the explicit statement it was given.
    assert (nothing or UNSTATED_CHANGE) is nothing
    print("\n[unstated/distinct] nothing_changed() and UNSTATED_CHANGE never "
          "compare equal")


def test_an_ambiguous_empty_statement_cannot_be_CONSTRUCTED() -> None:
    """The accident is designed out, not documented away: a bare
    ``ChangedStateTerms()`` — the shape that used to mean "permissive" — is
    REFUSED at construction with a message naming both meanings. A caller cannot
    express the wrong one by accident; it must pick.

    This is why the fix cannot be undone by a callsite that "just wants a
    default": there is no empty default to reach for."""
    with pytest.raises(SubstrateError) as ambiguous:
        ChangedStateTerms()
    print(f"\n[unstated/unconstructible] {ambiguous.value}")
    assert "ambiguous" in str(ambiguous.value)
    assert "nothing changed" in str(ambiguous.value)
    assert "not stated" in str(ambiguous.value)

    # a self-contradicting statement is refused too (nothing changed AND a term).
    with pytest.raises(SubstrateError) as contradiction:
        ChangedStateTerms(
            declares_nothing_changed=True,
            buckets=frozenset({(_PREDICATE, _KEY1)}),
        )
    assert "contradicts itself" in str(contradiction.value)

    # naming ANY term is unambiguous and stays constructible.
    assert ChangedStateTerms(buckets=frozenset({(_PREDICATE, _KEY1)})).buckets
    assert ChangedStateTerms(graph_check_ids=frozenset({"chk"})).graph_check_ids
    assert ChangedStateTerms(unknown=True, unknown_reason="why").unknown


def test_summary_digest_is_stable_and_canonical() -> None:
    """A summary is canonical-JSON serializable with a stable digest (the reuse
    certificate records it): two compiles of the same declarations share a digest;
    a bucket difference changes it."""
    specs = _fixture_specs()
    env = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
        source_name="registry-producer",
    )
    a = compile_possible_effects(env, checks=specs, claim_predicates=[_PREDICATE],
                                 producer_id="registry-producer")
    b = compile_possible_effects(env, checks=specs, claim_predicates=[_PREDICATE],
                                 producer_id="registry-producer")
    assert isinstance(a, PossibleEffects) and isinstance(b, PossibleEffects)
    assert a.digest() == b.digest()
    tampered = dataclasses.replace(a, bucket_reach=BucketReach())
    assert tampered.digest() != a.digest()


# --------------------------------------------------------------------------- #
# Scratch-DB factory + production seeding (test_equivalence_harness idiom).     #
# --------------------------------------------------------------------------- #
_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)


def _swap_dbname(base_dsn: str, dbname: str) -> str:
    parts = urlsplit(base_dsn)
    return urlunsplit(parts._replace(path="/" + dbname))


def _create_scratch_db(base_dsn: str, dbname: str) -> str:
    if not dbname.startswith("okg_test_"):
        raise AssertionError(f"unsafe scratch db name {dbname!r}")
    maint = _swap_dbname(base_dsn, "postgres")
    with psycopg.connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        cur.execute(f'CREATE DATABASE "{dbname}"')
    scratch = _swap_dbname(base_dsn, dbname)
    with psycopg.connect(scratch, autocommit=True) as conn, conn.cursor() as cur:
        for ext in _SCRATCH_EXTENSIONS:
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
    return scratch


def _drop_scratch_db(base_dsn: str, dbname: str) -> None:
    from okg.substrate.db.pool import close_all

    close_all()
    maint = _swap_dbname(base_dsn, "postgres")
    with psycopg.connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')


@pytest.fixture
def scratch_db(okg_migrated: str) -> Iterator[Callable[[], str]]:
    """Mint a fresh okg_test_* scratch DB per call (for each complete reference)
    and drop every one at teardown."""
    base_dsn = okg_migrated
    created: list[str] = []

    def _make() -> str:
        dbname = f"okg_test_pe_{uuid.uuid4().hex[:10]}"
        dsn = _create_scratch_db(base_dsn, dbname)
        created.append(dbname)
        return dsn

    try:
        yield _make
    finally:
        for dbname in created:
            try:
                _drop_scratch_db(base_dsn, dbname)
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass


def _install_production_catalog(dsn: str) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(_DEP_DIR)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _claim_fact(
    node_id: str, value: str, *, key: str, source: str, source_class: str,
    op: str = "I", **signals: Any,
) -> NodeFact:
    attrs = {
        "predicate": _PREDICATE, "normalized_key": key, "value": value,
        "scope": _SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1}, op=op)


def _functional_policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": _CONSTRAINT,
             "policy_kind": "functional", "predicate": _PREDICATE}
    attrs.update({k: str(v) for k, v in extra.items()})
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _publish(dsn: str) -> dict[str, Any]:
    from okg.substrate.publisher import Publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(dsn, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    return out


def _extract(dsn: str, out: dict[str, Any]) -> ResolvedStateSnapshot:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=out["compat_generation_id"],
            chronos_generation_id=out["generation_id"],
        )


def _reference_snapshot(
    scratch_dsn: str, net_facts: list[NodeFact],
) -> ResolvedStateSnapshot:
    """Run the complete reference over the pinned FULL payload in a fresh scratch
    DB and snapshot its resolved state (the harness's reference path)."""
    result = run_complete_reference(
        net_facts,
        deployment_dir=_DEP_DIR,
        scratch_dsn=scratch_dsn,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )
    assert not result.excluded, result.exclusions
    with psycopg.connect(scratch_dsn, autocommit=True) as conn:
        return ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=result.compat_generation_id,
            chronos_generation_id=result.generation_id,
        )


# --------------------------------------------------------------------------- #
# §9.4 adversarial soundness witness (named verbatim).                          #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_omitted_effect_is_caught_by_harness(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """§9.4 witness (named verbatim): removing a real effect from a summary makes
    the reuse decision detectably WRONG, and the equivalence harness catches it —
    the guard is load-bearing.

    A two-step scenario on the fixture where producer-shaped input for bucket B
    changes (a decisive registry claim E flips B from contested to canonical):

    * the HONEST summary's reach overlaps B, so ``skip_is_justifiable`` REFUSES;
      executing the update normally reproduces the reference -> harness EQUAL;
    * a TAMPERED summary with B omitted APPROVES the skip; simulating that skip
      (the update runs WITHOUT applying E, so the production state stays at the
      pre-update generation) while the complete reference over the pinned FULL
      payload sees E -> ``diff_resolved_states`` returns MISMATCH naming the
      divergent objects (the missing claim E, the diverged bucket resolution)."""
    _install_production_catalog(clean_runtime)
    specs = _fixture_specs()

    sfx = uuid.uuid4().hex[:8]
    c, d, e = f"claim:c:{sfx}", f"claim:d:{sfx}", f"claim:e:{sfx}"
    pol = f"{_POLICY}:{sfx}"
    prior_facts = [
        _claim_fact(c, "vC", key=_KEY1, source="page:c", source_class="wiki"),
        _claim_fact(d, "vD", key=_KEY1, source="page:d", source_class="wiki"),
        _functional_policy(pol),
    ]
    update_facts = [
        _claim_fact(e, "vE", key=_KEY1, source="registry:rec:1",
                    source_class="registry", source_authority=10),
        _functional_policy(pol, rev=2, review_note="registry onboarded"),
    ]

    # gen1 = the prior state (bucket B contested). Snapshot it BEFORE the update:
    # this IS the materialized state a wrongly-approved skip would leave behind.
    _apply(clean_runtime, prior_facts)
    out1 = _publish(clean_runtime)
    snap_skip = _extract(clean_runtime, out1)

    # gen2 = run the update normally (apply E) -> bucket B canonical.
    _apply(clean_runtime, update_facts)
    out2 = _publish(clean_runtime)
    snap_run = _extract(clean_runtime, out2)

    # the complete reference over the pinned FULL payload (sees E).
    net_facts = [
        _claim_fact(c, "vC", key=_KEY1, source="page:c", source_class="wiki"),
        _claim_fact(d, "vD", key=_KEY1, source="page:d", source_class="wiki"),
        _claim_fact(e, "vE", key=_KEY1, source="registry:rec:1",
                    source_class="registry", source_authority=10),
        _functional_policy(pol, rev=2, review_note="registry onboarded"),
    ]
    ref_snap = _reference_snapshot(scratch_db(), net_facts)

    # the E-producer's HONEST possible-effects: writes claims for bucket B.
    env = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
        source_name="registry-producer",
    )
    honest = compile_possible_effects(
        env, checks=specs, claim_predicates=[_PREDICATE], producer_id="registry-producer",
    )
    assert isinstance(honest, PossibleEffects)
    bucket_b = (_PREDICATE, _KEY1)
    changed = ChangedStateTerms(buckets=frozenset({bucket_b}))

    # HONEST summary overlaps B -> refuse the skip -> execute normally -> EQUAL.
    assert honest.bucket_reach.touches(*bucket_b)
    j_honest = skip_is_justifiable(honest, changed_state_terms=changed)
    assert not j_honest.approved and j_honest.reason == REFUSE_BUCKET_OVERLAP
    v_run = diff_resolved_states(ref_snap, snap_run)
    assert v_run.verdict == VERDICT_EQUAL, v_run.to_json_dict()

    # TAMPERED summary with B omitted -> approves the (unsound) skip.
    tampered = dataclasses.replace(honest, bucket_reach=BucketReach())
    j_tampered = skip_is_justifiable(tampered, changed_state_terms=changed)
    assert j_tampered.approved, j_tampered

    # the wrongly-approved skip leaves the pre-update state -> MISMATCH, and the
    # harness NAMES the divergent objects (this is exactly the wrongness the
    # omission caused — the guard that refused it is load-bearing).
    v_skip = diff_resolved_states(ref_snap, snap_skip)
    assert v_skip.verdict == VERDICT_MISMATCH, v_skip.to_json_dict()
    eff_diff = v_skip.artifact.families[FAMILY_EFFECTIVE]
    assert e in eff_diff.removed, ("reference has E; the skip-state is missing it",
                                   eff_diff.to_json_dict())
    res_diff = v_skip.artifact.families[FAMILY_RESOLUTION]
    assert bucket_row_key(_PREDICATE, _KEY1) in res_diff.changed, res_diff.to_json_dict()
    print(
        f"\n[possible-effects/omitted-effect] honest REFUSES (reason="
        f"{j_honest.reason}) -> executed -> EQUAL; tampered APPROVES -> skip -> "
        f"MISMATCH names removed claim {e!r} + diverged bucket resolution. "
        f"The guard is load-bearing."
    )


@pytest.mark.postgres
def test_disjoint_skip_is_sound_and_stays_equal(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """The sound-skip counterpart: a step whose HONEST summary is disjoint from
    the change is APPROVED, and the executed skip stays EQUAL.

    Two buckets under the constraint: the update changes bucket B1 (a decisive
    claim flips it) while bucket B2 is untouched. A producer that writes only B2
    has a summary disjoint from B1, so ``skip_is_justifiable`` APPROVES skipping
    it (contrast: a producer writing B1 is REFUSED). Because B2 genuinely did not
    change, the executed update — which carries B2 forward — reproduces the
    complete reference exactly (EQUAL), and B2's resolution row is byte-identical
    before and after the update (so reusing the prior B2 is provably sound)."""
    _install_production_catalog(clean_runtime)

    sfx = uuid.uuid4().hex[:8]
    c1, d1, e1 = f"claim:c1:{sfx}", f"claim:d1:{sfx}", f"claim:e1:{sfx}"
    g2, h2 = f"claim:g2:{sfx}", f"claim:h2:{sfx}"
    pol = f"{_POLICY}:{sfx}"

    prior_facts = [
        # B1 = (predicate, KEY1): C1, D1 tie -> contested.
        _claim_fact(c1, "vC", key=_KEY1, source="page:c", source_class="wiki"),
        _claim_fact(d1, "vD", key=_KEY1, source="page:d", source_class="wiki"),
        # B2 = (predicate, KEY2): G2 (authority 10) beats H2 -> canonical G2.
        _claim_fact(g2, "vG", key=_KEY2, source="registry:g", source_class="registry",
                    source_authority=10),
        _claim_fact(h2, "vH", key=_KEY2, source="page:h", source_class="wiki",
                    source_authority=1),
        _functional_policy(pol),
    ]
    update_facts = [
        # the update changes ONLY B1: decisive E1 flips it to canonical.
        _claim_fact(e1, "vE", key=_KEY1, source="registry:e", source_class="registry",
                    source_authority=10),
        _functional_policy(pol, rev=2, review_note="registry onboarded"),
    ]
    net_facts = [
        _claim_fact(c1, "vC", key=_KEY1, source="page:c", source_class="wiki"),
        _claim_fact(d1, "vD", key=_KEY1, source="page:d", source_class="wiki"),
        _claim_fact(g2, "vG", key=_KEY2, source="registry:g", source_class="registry",
                    source_authority=10),
        _claim_fact(h2, "vH", key=_KEY2, source="page:h", source_class="wiki",
                    source_authority=1),
        _claim_fact(e1, "vE", key=_KEY1, source="registry:e", source_class="registry",
                    source_authority=10),
        _functional_policy(pol, rev=2, review_note="registry onboarded"),
    ]

    _apply(clean_runtime, prior_facts)
    out1 = _publish(clean_runtime)
    snap_prior = _extract(clean_runtime, out1)

    _apply(clean_runtime, update_facts)
    out2 = _publish(clean_runtime)
    snap_after = _extract(clean_runtime, out2)

    ref_snap = _reference_snapshot(scratch_db(), net_facts)
    specs = _fixture_specs()

    # the B2-producer's summary is disjoint from the changed bucket B1 -> APPROVE.
    env_b2 = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY2]},
        source_name="b2-producer",
    )
    effects_b2 = compile_possible_effects(
        env_b2, checks=specs, claim_predicates=[_PREDICATE], producer_id="b2-producer",
    )
    changed_b1 = ChangedStateTerms(buckets=frozenset({(_PREDICATE, _KEY1)}))
    j_b2 = skip_is_justifiable(effects_b2, changed_state_terms=changed_b1)
    assert j_b2.approved and j_b2.reason == SKIP_APPROVED, j_b2

    # contrast: a producer that writes the CHANGED bucket B1 is refused.
    env_b1 = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim"}], "claim_keys": [_KEY1]},
        source_name="b1-producer",
    )
    effects_b1 = compile_possible_effects(
        env_b1, checks=specs, claim_predicates=[_PREDICATE], producer_id="b1-producer",
    )
    j_b1 = skip_is_justifiable(effects_b1, changed_state_terms=changed_b1)
    assert not j_b1.approved and j_b1.reason == REFUSE_BUCKET_OVERLAP, j_b1

    # the executed skip stays EQUAL: the update carried B2 forward, and the full
    # run reproduces the complete reference exactly.
    v_after = diff_resolved_states(ref_snap, snap_after)
    assert v_after.verdict == VERDICT_EQUAL, v_after.to_json_dict()

    # and reusing the prior B2 is provably sound: B2's resolution row is byte-
    # identical before and after the update (the skip would reuse an identical row).
    rk2 = bucket_row_key(_PREDICATE, _KEY2)
    assert rk2 in snap_prior.resolution_outcomes, sorted(snap_prior.resolution_outcomes)
    assert (snap_prior.resolution_outcomes[rk2]
            == snap_after.resolution_outcomes[rk2]), "B2 changed across the update"
    assert (snap_after.resolution_outcomes[rk2]
            == ref_snap.resolution_outcomes[rk2]), "B2 differs from the reference"
    # B1 (the changed bucket) DID move: contested -> canonical.
    rk1 = bucket_row_key(_PREDICATE, _KEY1)
    assert snap_prior.resolution_outcomes[rk1]["outcome"] != \
        snap_after.resolution_outcomes[rk1]["outcome"]
    print(
        "\n[possible-effects/sound-skip] B2-producer APPROVED (disjoint from B1), "
        "B1-producer REFUSED; executed update EQUAL to reference; B2 resolution "
        "byte-identical prior==after==reference -> the approved skip is sound."
    )
