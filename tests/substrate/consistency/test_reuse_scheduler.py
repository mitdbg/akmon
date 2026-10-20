"""The resolution-aware reuse scheduler — worklist to a fixed point + the loud
fallback ladder (spec §9.6), riding the publisher DBOS workflow.

PACT ``consistency-resolution-aware-reuse``, task.reuse.scheduler
(req.reuse.protocol + req.reuse.single-mechanism). The four witnesses named
VERBATIM by docs/develop/architecture/consistency.md §9.6 land here:

* ``test_commit_refused_before_fixed_point`` — commit refuses while a reached
  step remains; it succeeds only at the fixed point;
* ``test_unknown_dependency_discards_into_complete_execution`` — a reached step
  with no declared envelope discards the tentative pass and runs the force-full
  complete path (``run_dependency_baseline``), writing zero certificates;
* ``test_operator_failure_aborts_leaving_prior_state_intact`` — a step that
  raises aborts the update: no checkpoint, no certificate rows for the aborted
  generation;
* ``test_crash_mid_update_resumes_or_discards`` — the pass is deterministic, so a
  step re-executed at the DBOS boundary yields the SAME certificates (byte-equal
  ``ReusePassResult`` digest); the maintenance state commits atomically with the
  checkpoint — either both exist or neither.

Plus: a mixed run/skip fixed point (certificates only for skips, every outcome
cause-recorded), a valid-prior-certificate skip, an invalidated-certificate
forcing a run, RunAlways forcing a run, a discarded pass writing zero
certificates, the opt-in flag consumed by the stack, DBOS-step registration, the
outside-cone enumeration, and the equivalence pre-witness (a scheduler-governed
publish with a genuine skip equals a complete-reference execution).

The DBOS idiom follows tests/substrate/runtime/test_publisher_workflow.py; the
live publish + scratch idiom follows tests/substrate/consistency/
test_claim_plane_wire.py and test_equivalence_harness.py.
"""
from __future__ import annotations

import dataclasses
import json
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest

from okg.substrate.consistency.possible_effects import (
    REFUSE_UNSTATED_CHANGE,
    ChangedStateTerms,
)
from okg.substrate.consistency.reuse_certificates import (
    COMPARISON_KIND_DEGENERATE,
    DEGENERATE_COMPARISON_KEYS,
    DEPENDENCY_CLASSES,
    RESOLUTION_V1_FACT_COVERAGE,
    CertificateDependencies,
    ReuseCertificate,
    ReuseCertificateStore,
    validate_certificate,
)
from okg.substrate.consistency.reuse_scheduler import (
    ACTION_RUN,
    ACTION_SKIP,
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_CERTIFICATE_VALID,
    CAUSE_EFFECTS_OVERLAP,
    CAUSE_ENVELOPE_VIOLATION,
    CAUSE_OPERATOR_FAILURE,
    CAUSE_RUN_ALWAYS,
    CAUSE_SKIP_DOMINANCE,
    CAUSE_SKIP_JUSTIFIED,
    CAUSE_UNKNOWN_DEPENDENCY,
    CAUSE_UNSTATED_CHANGE,
    COMPARISON_KIND_RESOLUTION,
    DOMINANCE_APPROVED,
    FALLBACK_CAUSES,
    OUTSIDE_CONE,
    REFUSE_DOMINANCE_AUTHORITY,
    REFUSE_DOMINANCE_BASIS,
    REFUSE_DOMINANCE_HIGHER_SIGNAL,
    REFUSE_DOMINANCE_UNBOUNDED,
    SKIP_CAUSES,
    TERMINAL_ABORT,
    TERMINAL_DISCARD,
    TERMINAL_FIXED_POINT,
    CommitBeforeFixedPoint,
    CompetitorBound,
    DominanceInput,
    IncumbentDecision,
    ReusePass,
    ReuseStep,
    build_degenerate_comparison,
    commit_gate,
    derive_competitor_bound,
    dominance_is_justifiable,
    run_reuse_pass,
)
from okg.substrate.producer_authority import normalize_write_envelope
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"

_CATALOG_CHILD_TABLES = (
    "invariants",
    "identifier_patterns",
    "aliases",
    "narrowings",
    "subtype_ancestors",
    "subtypes",
)


@pytest.fixture(autouse=True)
def _drop_catalog_versions_this_test_wrote(
    request: pytest.FixtureRequest,
) -> Iterator[None]:
    """Remove every catalog version a database test in this file applies.

    `clean_runtime` truncates the runtime tables but keeps the ontology
    catalog. The consistency-fixture deployment loads the shipped
    `extraction` module, whose `document_chunk` declares
    `searchable_text_fields: ["text"]`; left behind, that declaration
    changes how a later test on the same worker stores `document_chunk`
    text (the doc-corpus windowed text read then fails to rejoin its
    chunks). Tests without `clean_runtime` touch no database and are
    left alone.
    """
    if "clean_runtime" not in request.fixturenames:
        yield
        return
    import psycopg
    from psycopg import sql

    from tests.substrate.conftest import reset_okg_test_runtime

    dsn = request.getfixturevalue("clean_runtime")
    with psycopg.connect(dsn, autocommit=True) as conn:
        baseline = conn.execute(
            "SELECT coalesce(max(catalog_version_id), 0) FROM okg.catalog_versions"
        ).fetchone()[0]
    yield
    reset_okg_test_runtime(dsn)
    with psycopg.connect(dsn) as conn, conn.transaction():
        for table in _CATALOG_CHILD_TABLES:
            conn.execute(
                sql.SQL("DELETE FROM okg.{} WHERE catalog_version_id > %s").format(
                    sql.Identifier(table)
                ),
                (baseline,),
            )
        conn.execute(
            "DELETE FROM okg.catalog_versions WHERE catalog_version_id > %s",
            (baseline,),
        )
        left = conn.execute(
            "SELECT count(*) FROM okg.subtypes WHERE catalog_version_id > %s",
            (baseline,),
        ).fetchone()[0]
    assert left == 0, f"{left} catalog subtype rows survived the cleanup"


_PREDICATE = "preferred_target"


# --------------------------------------------------------------------------- #
# In-memory capture connection — the pass records skips through record_skip     #
# (the ONLY skip path); this captures the write shape without Postgres.         #
# --------------------------------------------------------------------------- #
class _CaptureCursor:
    def __init__(self, sink: list[tuple[Any, ...]]) -> None:
        self._sink = sink

    def __enter__(self) -> "_CaptureCursor":
        return self

    def __exit__(self, *_a: Any) -> bool:
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self._sink.append((sql, params))

    def fetchone(self) -> Any:
        return None


class _CaptureConn:
    def __init__(self) -> None:
        self.rows: list[tuple[Any, ...]] = []
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _CaptureCursor:
        return _CaptureCursor(self.rows)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


# --------------------------------------------------------------------------- #
# Builders.                                                                     #
# --------------------------------------------------------------------------- #
def _claim_envelope(keys: tuple[str, ...] = ("k1",)) -> Any:
    return normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": list(keys)},
        source_name="producer",
    )


def _unbounded_envelope() -> Any:
    return normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "unbounded": ["claim_keys"]},
        source_name="producer",
    )


def _step(
    step_id: str, keys: tuple[str, ...] = ("k1",), *, unbounded: bool = False,
) -> ReuseStep:
    return ReuseStep(
        step_id=step_id,
        envelope=_unbounded_envelope() if unbounded else _claim_envelope(keys),
        producer_id=step_id,
        claim_predicates=(_PREDICATE,),
    )


def _facts(tag: str = "base") -> CertificateDependencies:
    return CertificateDependencies(
        input_digests={"in": f"digest:{tag}"},
        operator_versions={"op": "v1"},
        model_prompt_keys={},
        envelope_fingerprint=f"env:{tag}",
        constraint_set={"chk": "cs:1"},
        generator_versions={"gen": "v1"},
        policy_fingerprint="pol:1",
        precedence_fingerprint="prec:1",
        competitor_bounds={"bound": tag},
        catalog_version_id="catver:1",
    )


def _comparison() -> dict[str, Any]:
    return {
        "kind": COMPARISON_KIND_RESOLUTION,
        "incumbent_outcome": "selected",
        "incumbent_basis": "source_authority",
        "effects_summary_digest": "effects:prior",
        "competitor_bound": {"statement": "prior bound"},
        "tie_break": None,
        "fact_coverage": dict(RESOLUTION_V1_FACT_COVERAGE),
    }


def _overlap(key: str = "k1") -> ChangedStateTerms:
    return ChangedStateTerms(buckets=frozenset({(_PREDICATE, key)}))


def _disjoint(key: str = "k9") -> ChangedStateTerms:
    return ChangedStateTerms(buckets=frozenset({(_PREDICATE, key)}))


def _incumbent() -> dict[str, Any]:
    return {"outcome": "selected", "basis": "source_authority"}


# =========================================================================== #
# Pure witnesses (no DB): the decision ladder + the commit gate.               #
# =========================================================================== #
def test_mixed_run_skip_fixed_point_every_outcome_cause_recorded() -> None:
    """A mixed pass reaches a fixed point: one step skips behind a fresh
    certificate, one step runs on an effects overlap. Certificates are written
    ONLY for skips, and EVERY outcome carries a declared cause."""
    conn = _CaptureConn()
    steps = {"keep": _step("keep", ("k1",)), "recompute": _step("recompute", ("k2",))}
    result = run_reuse_pass(
        conn,
        deployment=_DEPLOYMENT,
        generation_id="7",
        worklist_seed=["keep", "recompute"],
        steps=steps,
        checks=(),
        changed_terms={"keep": _disjoint("z"), "recompute": _overlap("k2")},
        current_facts={"keep": _facts("keep"), "recompute": _facts("recompute")},
        incumbent_selection={"keep": _incumbent()},
    )

    print("\n[reuse-sched/mixed] terminal=", result.terminal,
          "cause_counts=", result.cause_counts())
    for o in sorted(result.outcomes, key=lambda o: o.step_id):
        print(f"[reuse-sched/mixed]   {o.step_id}: action={o.action} "
              f"cause={o.cause} cert={str(o.certificate_digest)[:12]}")

    assert result.reached_fixed_point
    assert result.terminal == TERMINAL_FIXED_POINT
    by_step = {o.step_id: o for o in result.outcomes}
    assert by_step["keep"].action == ACTION_SKIP
    assert by_step["keep"].cause == CAUSE_SKIP_JUSTIFIED
    assert by_step["keep"].certificate_digest                     # skip -> cert
    assert by_step["recompute"].action == ACTION_RUN
    assert by_step["recompute"].cause == CAUSE_EFFECTS_OVERLAP
    assert by_step["recompute"].certificate_digest is None        # run -> no cert
    # certificates only for skips (one INSERT).
    assert len(conn.rows) == 1
    assert "INSERT INTO okg.reuse_certificates" in conn.rows[0][0]
    # every outcome carries a declared cause; an outcome without one is impossible.
    for o in result.outcomes:
        assert o.cause in FALLBACK_CAUSES
    # deterministic: a second identical pass shares the byte-stable digest.
    conn2 = _CaptureConn()
    again = run_reuse_pass(
        conn2, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep", "recompute"], steps=steps, checks=(),
        changed_terms={"keep": _disjoint("z"), "recompute": _overlap("k2")},
        current_facts={"keep": _facts("keep"), "recompute": _facts("recompute")},
        incumbent_selection={"keep": _incumbent()},
    )
    assert again.digest() == result.digest()


def test_valid_prior_certificate_skip() -> None:
    """A step with a fully valid prior certificate SKIPS (cause
    ``certificate_valid``) — the certificate is re-recorded for this generation
    with refreshed dependency facts (§9.6 (a))."""
    conn = _CaptureConn()
    facts = _facts("stable")
    prior = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="6", step_id="keep",
        comparison=_comparison(), dependencies=facts,
    )
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep")}, checks=(),
        prior_certificates={"keep": prior},
        changed_terms={"keep": _overlap("k1")},  # overlap would force a run...
        current_facts={"keep": facts},           # ...but the valid prior cert wins
        incumbent_selection={"keep": _incumbent()},
    )
    outcome = result.outcomes[0]
    print(f"\n[reuse-sched/valid-prior] {outcome.step_id}: {outcome.action} "
          f"cause={outcome.cause} prior_gen={outcome.detail.get('prior_generation_id')}")
    assert result.reached_fixed_point
    assert outcome.action == ACTION_SKIP
    assert outcome.cause == CAUSE_CERTIFICATE_VALID
    assert outcome.detail["prior_generation_id"] == "6"
    # re-recorded under THIS generation.
    _sql, params = conn.rows[0]
    assert params[1] == "7"       # generation_id column
    assert params[2] == "keep"    # step_id column


def test_invalidated_certificate_forces_run() -> None:
    """Mutating one recorded dependency class invalidates the prior certificate;
    with an overlapping change the step then RUNS (§9.6 (a)->(b)), and the outcome
    names the invalidated class."""
    conn = _CaptureConn()
    prior_facts = _facts("v1")
    # current facts differ in exactly one class -> the prior cert is invalid.
    current = CertificateDependencies(
        input_digests={"in": "digest:CHANGED"},   # <- the one mutated class
        operator_versions={"op": "v1"},
        model_prompt_keys={},
        envelope_fingerprint="env:v1",
        constraint_set={"chk": "cs:1"},
        generator_versions={"gen": "v1"},
        policy_fingerprint="pol:1",
        precedence_fingerprint="prec:1",
        competitor_bounds={"bound": "v1"},
        catalog_version_id="catver:1",
    )
    prior = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="6", step_id="keep",
        comparison=_comparison(), dependencies=prior_facts,
    )
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep", ("k1",))}, checks=(),
        prior_certificates={"keep": prior},
        changed_terms={"keep": _overlap("k1")},   # overlap -> effects refuse skip
        current_facts={"keep": current},
        incumbent_selection={"keep": _incumbent()},
    )
    outcome = result.outcomes[0]
    print(f"\n[reuse-sched/invalidated] {outcome.step_id}: {outcome.action} "
          f"cause={outcome.cause} invalidated={outcome.detail.get('prior_certificate_invalidated')}")
    assert outcome.action == ACTION_RUN
    assert outcome.cause == CAUSE_EFFECTS_OVERLAP
    assert outcome.detail["prior_certificate_invalidated"] == ["input_digests"]
    assert conn.rows == []     # a run writes no certificate


def test_run_always_forces_run() -> None:
    """An unbounded envelope compiles to RunAlways — the step can never be
    certified skippable and RUNS (cause ``run_always``, §9.6 (b))."""
    conn = _CaptureConn()
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["free"], steps={"free": _step("free", unbounded=True)},
        checks=(), changed_terms={"free": _disjoint("z")},  # disjoint, yet runs
        current_facts={"free": _facts("free")},
    )
    outcome = result.outcomes[0]
    print(f"\n[reuse-sched/run-always] {outcome.step_id}: {outcome.action} "
          f"cause={outcome.cause} unbounded={outcome.detail.get('unbounded_dimensions')}")
    assert outcome.action == ACTION_RUN
    assert outcome.cause == CAUSE_RUN_ALWAYS
    assert "claim_keys" in outcome.detail["unbounded_dimensions"]
    assert conn.rows == []


# =========================================================================== #
# Bounded-dominance rung — the CENTREPIECE changed-inputs skip                  #
# (task.reuse.live-proof, spec §9.5 resolution_dominance / §9.7).              #
# =========================================================================== #
_SOURCE_AUTHORITY_BASIS = "declared_source_authority"


def _incumbent_decision(authority: int = 10) -> IncumbentDecision:
    return IncumbentDecision(
        bucket=(_PREDICATE, "k1"), outcome="selected",
        basis=_SOURCE_AUTHORITY_BASIS, authority=authority,
    )


def test_dominance_rung_approves_only_a_provable_loss() -> None:
    """The bounded-dominance rung approves ONLY when the incumbent won decisively
    by declared source authority AND the competitor's authority ceiling is
    STRICTLY below it (conservative-or-refuse; §9.5)."""
    incumbent = _incumbent_decision(authority=10)
    approved = dominance_is_justifiable(
        incumbent, CompetitorBound(source_classes=("crawl",), authority_max=0)
    )
    print(f"\n[dominance/approve] {approved.reason} detail={approved.detail}")
    assert approved.approved and approved.reason == DOMINANCE_APPROVED
    assert approved.detail["incumbent_worst"]["authority"] == 10
    assert approved.detail["competitor_best"]["authority"] == 0


def test_dominance_rung_refuses_conservatively() -> None:
    """Every case the analysis cannot PROVE refuses with a typed reason — a wrong
    basis, an unbounded competitor ceiling, a ceiling not strictly below the
    incumbent (a tie cannot be settled by authority), or a higher-precedence
    signal escape. Refusal is always sound (running the step is never wrong)."""
    incumbent = _incumbent_decision(authority=10)
    # wrong basis (the incumbent won by something other than source authority).
    other_basis = IncumbentDecision(
        bucket=(_PREDICATE, "k1"), outcome="selected",
        basis="operator_precedent", authority=10,
    )
    assert dominance_is_justifiable(
        other_basis, CompetitorBound(("crawl",), 0)
    ).reason == REFUSE_DOMINANCE_BASIS
    # unbounded competitor authority.
    assert dominance_is_justifiable(
        incumbent, CompetitorBound(("crawl",), None)
    ).reason == REFUSE_DOMINANCE_UNBOUNDED
    # a TIE on authority cannot be settled by authority.
    assert dominance_is_justifiable(
        incumbent, CompetitorBound(("crawl",), 10)
    ).reason == REFUSE_DOMINANCE_AUTHORITY
    # a higher-precedence signal escape (supersession / effective date / precedent).
    for bound in (
        CompetitorBound(("crawl",), 0, can_win_supersession=True),
        CompetitorBound(("crawl",), 0, can_win_effective_date=True),
        CompetitorBound(("crawl",), 0, precedent_preferred=True),
    ):
        assert dominance_is_justifiable(incumbent, bound).reason == (
            REFUSE_DOMINANCE_HIGHER_SIGNAL
        )
    print("\n[dominance/refuse] wrong-basis / unbounded / tie / higher-signal all "
          "refuse")


def test_derive_competitor_bound_comes_from_the_envelope() -> None:
    """The competitor bound is DERIVED FROM the declared envelope (§9.3): the
    closed source-authority vocabulary's max is the ceiling; an envelope with no
    source-authority vocabulary (or an unbounded dimension) yields no bound, so
    dominance must refuse."""
    bounded = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": ["k1"],
         "value_vocabularies": {"source_class": ["crawl"],
                                "source_authority": ["0", "1"]}},
        source_name="crawl",
    )
    bound = derive_competitor_bound(bounded)
    assert bound is not None
    assert bound.source_classes == ("crawl",) and bound.authority_max == 1
    # no source-authority vocabulary -> unprovable ceiling -> None (refuse).
    no_auth = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": ["k1"], "value_vocabularies": {"source_class": ["crawl"]}},
        source_name="crawl",
    )
    assert derive_competitor_bound(no_auth) is None
    # an unbounded envelope bounds nothing.
    unbounded = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "unbounded": ["claim_keys"]},
        source_name="crawl",
    )
    assert derive_competitor_bound(unbounded) is None
    print("\n[dominance/derive] bound from envelope vocab; unbounded/no-auth -> None")


def test_dominance_skip_records_resolution_dominance_certificate() -> None:
    """A changed input that OVERLAPS the incumbent bucket (set-disjointness
    refuses) is skipped behind a ``resolution_dominance`` certificate when the
    dominance rung approves — cause ``skip_dominance``, the comparison recording
    incumbent-worst vs competitor-best. Without a dominance input the SAME overlap
    RUNS (the rung only ever rescues, never loosens)."""
    envelope = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": ["k1"],
         "value_vocabularies": {"source_class": ["crawl"],
                                "source_authority": ["0"]}},
        source_name="crawl",
    )
    step = ReuseStep(step_id="crawl", envelope=envelope, producer_id="crawl",
                     claim_predicates=(_PREDICATE,))
    dominance = {
        "crawl": DominanceInput(
            incumbents={(_PREDICATE, "k1"): _incumbent_decision(10)},
            competitor_bound=derive_competitor_bound(envelope),
        )
    }
    conn = _CaptureConn()
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["crawl"], steps={"crawl": step}, checks=(),
        changed_terms={"crawl": _overlap("k1")},   # overlaps the incumbent bucket
        current_facts={"crawl": _facts("crawl")},
        incumbent_selection={"crawl": {"outcome": "selected",
                                       "basis": _SOURCE_AUTHORITY_BASIS}},
        dominance_inputs=dominance,
    )
    outcome = result.outcomes[0]
    print(f"\n[dominance/skip] {outcome.step_id}: {outcome.action}/{outcome.cause} "
          f"dominated={outcome.detail.get('dominated_buckets')}")
    assert outcome.action == ACTION_SKIP
    assert outcome.cause == CAUSE_SKIP_DOMINANCE
    assert outcome.cause in SKIP_CAUSES
    # the recorded comparison is resolution_dominance with the bounded-dominance
    # incumbent-worst vs competitor-best statement.
    _sql, params = conn.rows[0]
    comparison = json.loads(params[4])
    assert comparison["kind"] == COMPARISON_KIND_RESOLUTION
    assert comparison["competitor_bound"]["kind"] == "bounded_dominance"
    assert comparison["competitor_bound"]["per_bucket"][0][
        "incumbent_worst"]["authority"] == 10

    # without a dominance input the SAME overlap RUNS (never a silent skip).
    conn2 = _CaptureConn()
    ran = run_reuse_pass(
        conn2, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["crawl"], steps={"crawl": step}, checks=(),
        changed_terms={"crawl": _overlap("k1")},
        current_facts={"crawl": _facts("crawl")},
        incumbent_selection={"crawl": {"outcome": "selected",
                                       "basis": _SOURCE_AUTHORITY_BASIS}},
    )
    assert ran.outcomes[0].action == ACTION_RUN
    assert ran.outcomes[0].cause == CAUSE_EFFECTS_OVERLAP
    assert conn2.rows == []


def test_dominance_refusal_falls_through_to_run() -> None:
    """A dominance input whose competitor could TIE the incumbent authority does
    not rescue the overlap — the step RUNS (conservative-or-refuse), no certificate
    written."""
    envelope = normalize_write_envelope(
        {"nodes": [{"subtype": "consistency_claim", "clause_id": "c"}],
         "claim_keys": ["k1"],
         "value_vocabularies": {"source_class": ["crawl"],
                                "source_authority": ["10"]}},   # TIES the incumbent
        source_name="crawl",
    )
    step = ReuseStep(step_id="crawl", envelope=envelope, producer_id="crawl",
                     claim_predicates=(_PREDICATE,))
    conn = _CaptureConn()
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["crawl"], steps={"crawl": step}, checks=(),
        changed_terms={"crawl": _overlap("k1")},
        current_facts={"crawl": _facts("crawl")},
        incumbent_selection={"crawl": {"outcome": "selected",
                                       "basis": _SOURCE_AUTHORITY_BASIS}},
        dominance_inputs={"crawl": DominanceInput(
            incumbents={(_PREDICATE, "k1"): _incumbent_decision(10)},
            competitor_bound=derive_competitor_bound(envelope))},
    )
    print(f"\n[dominance/refuse-run] {result.outcomes[0].action}/"
          f"{result.outcomes[0].cause}")
    assert result.outcomes[0].action == ACTION_RUN
    assert result.outcomes[0].cause == CAUSE_EFFECTS_OVERLAP
    assert conn.rows == []


# =========================================================================== #
# Degenerate (unchanged-inputs) carry-forward — the single skip mechanism      #
# (task.reuse.subsume-carry-forward, spec §9.5).                               #
# =========================================================================== #
def test_degenerate_unchanged_inputs_records_a_certificate() -> None:
    """A cone step whose current dependency facts are byte-identical to the prior
    generation's (``prior_facts``) and that has NO valid prior certificate SKIPS
    behind a DEGENERATE certificate — the unchanged-inputs carry-forward no longer
    bypasses the certificate mechanism (§9.5). The comparison declares the
    degenerate kind; the recorded dependencies enumerate all ten classes."""
    conn = _CaptureConn()
    facts = _facts("stable")
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep")}, checks=(),
        current_facts={"keep": facts},
        prior_facts={"keep": facts},              # byte-identical -> degenerate
        prior_generation_id="6",
        incumbent_selection={"keep": {"outcome": "carry_forward",
                                      "basis": "unchanged_inputs"}},
    )
    outcome = result.outcomes[0]
    print(f"\n[reuse-sched/degenerate] {outcome.step_id}: {outcome.action} "
          f"cause={outcome.cause} unchanged={outcome.detail.get('unchanged_inputs')} "
          f"prior_gen={outcome.detail.get('prior_generation_id')}")
    assert result.reached_fixed_point
    assert outcome.action == ACTION_SKIP
    assert outcome.cause == CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED
    assert outcome.certificate_digest                        # skip -> cert
    assert outcome.detail["unchanged_inputs"] is True
    assert outcome.detail["prior_generation_id"] == "6"
    # the recorded row carries a DEGENERATE-kind comparison + all ten classes.
    assert len(conn.rows) == 1
    _sql, params = conn.rows[0]
    comparison = json.loads(params[4])
    dependencies = json.loads(params[5])
    print("[reuse-sched/degenerate] comparison=", comparison)
    assert comparison["kind"] == COMPARISON_KIND_DEGENERATE
    assert set(DEGENERATE_COMPARISON_KEYS) <= set(comparison)
    assert set(dependencies) == set(DEPENDENCY_CLASSES)
    # deterministic: a second identical pass shares the byte-stable digest.
    conn2 = _CaptureConn()
    again = run_reuse_pass(
        conn2, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep")}, checks=(),
        current_facts={"keep": facts}, prior_facts={"keep": facts},
        prior_generation_id="6",
        incumbent_selection={"keep": {"outcome": "carry_forward",
                                      "basis": "unchanged_inputs"}},
    )
    assert again.digest() == result.digest()


def test_changed_facts_break_the_degenerate_skip() -> None:
    """The degenerate rung fires ONLY on byte-identity. Change one dependency
    class between the prior manifest and the current facts and the step is no
    longer a degenerate carry-forward: with a disjoint change it falls through to
    a justified skip, with an overlapping change it RUNS (§9.5 -> §9.6)."""
    # disjoint change -> not degenerate, effects justify a fresh skip.
    conn = _CaptureConn()
    prior = _facts("prior")
    current = dataclasses.replace(prior, input_digests={"in": "digest:CHANGED"})
    disjoint = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep", ("k1",))}, checks=(),
        current_facts={"keep": current}, prior_facts={"keep": prior},
        changed_terms={"keep": _disjoint("z")},
        incumbent_selection={"keep": _incumbent()},
    )
    print(f"\n[reuse-sched/degenerate-broken] disjoint -> {disjoint.outcomes[0].cause}")
    assert disjoint.outcomes[0].cause == CAUSE_SKIP_JUSTIFIED

    # overlapping change -> not degenerate, the step RUNS.
    conn = _CaptureConn()
    overlapping = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["keep"], steps={"keep": _step("keep", ("k1",))}, checks=(),
        current_facts={"keep": current}, prior_facts={"keep": prior},
        changed_terms={"keep": _overlap("k1")},
        incumbent_selection={"keep": _incumbent()},
    )
    print(f"[reuse-sched/degenerate-broken] overlap -> "
          f"{overlapping.outcomes[0].action}/{overlapping.outcomes[0].cause}")
    assert overlapping.outcomes[0].action == ACTION_RUN
    assert overlapping.outcomes[0].cause == CAUSE_EFFECTS_OVERLAP


def test_three_skip_flavors_are_distinguishable() -> None:
    """The three SKIP flavors are DISTINCT causes in one ReusePassResult so
    accounting can tell them apart (§9.5 / §9.6): a revalidated prior certificate
    (``certificate_valid``), a degenerate unchanged-inputs carry-forward
    (``certificate_degenerate_unchanged``), and a freshly justified changed-inputs
    skip (``skip_justified``)."""
    conn = _CaptureConn()
    facts_valid = _facts("valid")
    facts_degen = _facts("degen")
    facts_justified = _facts("just")
    prior = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="6", step_id="valid",
        comparison=_comparison(), dependencies=facts_valid,
    )
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["valid", "degen", "justified"],
        steps={"valid": _step("valid", ("k1",)),
               "degen": _step("degen", ("k2",)),
               "justified": _step("justified", ("k3",))},
        checks=(),
        prior_certificates={"valid": prior},        # valid -> certificate_valid
        prior_facts={"degen": facts_degen},          # byte-identical -> degenerate
        changed_terms={"justified": _disjoint("z")},  # disjoint -> skip_justified
        current_facts={"valid": facts_valid, "degen": facts_degen,
                       "justified": facts_justified},
        incumbent_selection={"degen": _incumbent(), "justified": _incumbent()},
    )
    by_step = {o.step_id: o for o in result.outcomes}
    print("\n[reuse-sched/three-flavors] cause_counts=", result.cause_counts())
    for o in sorted(result.outcomes, key=lambda o: o.step_id):
        print(f"[reuse-sched/three-flavors]   {o.step_id}: {o.action} cause={o.cause}")
    assert result.reached_fixed_point
    assert by_step["valid"].cause == CAUSE_CERTIFICATE_VALID
    assert by_step["degen"].cause == CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED
    assert by_step["justified"].cause == CAUSE_SKIP_JUSTIFIED
    # all three are distinct SKIPs behind certificates.
    assert all(by_step[s].action == ACTION_SKIP
               for s in ("valid", "degen", "justified"))
    assert {o.cause for o in result.skips} == {
        CAUSE_CERTIFICATE_VALID, CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
        CAUSE_SKIP_JUSTIFIED,
    }
    assert {o.cause for o in result.skips} <= SKIP_CAUSES
    counts = result.cause_counts()
    assert counts[CAUSE_CERTIFICATE_VALID] == 1
    assert counts[CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED] == 1
    assert counts[CAUSE_SKIP_JUSTIFIED] == 1


# =========================================================================== #
# An UNSTATED change is not "nothing changed" — the run/skip accounting        #
# (PACT consistency-unstated-change-is-not-no-change, req.unstated.fail-closed) #
# =========================================================================== #
_FIRST_PUBLISH_STEPS = 33


def _first_publish_pass(
    changed_terms: dict[str, Any] | None,
) -> tuple[Any, _CaptureConn]:
    """A FIRST PUBLISH over 33 reached steps: no prior certificates, no prior
    facts, so the certificate and degenerate rungs cannot fire and every step is
    decided by the justified-skip rung alone. The ONLY input that varies between
    the witness and its control is the change statement."""
    step_ids = [f"step_{i:02d}" for i in range(_FIRST_PUBLISH_STEPS)]
    conn = _CaptureConn()
    result = run_reuse_pass(
        conn,
        deployment=_DEPLOYMENT,
        generation_id="1",
        worklist_seed=step_ids,
        steps={s: _step(s, (f"key-{s}",)) for s in step_ids},
        checks=(),
        current_facts={s: _facts(s) for s in step_ids},
        incumbent_selection={s: _incumbent() for s in step_ids},
        changed_terms=changed_terms,
    )
    return result, conn


def test_unstated_change_runs_every_reached_step_on_a_first_publish() -> None:
    """THE WITNESS (req.unstated.fail-closed ACCEPTANCE 1): a first publish with no
    prior certificate, no prior facts, and an UNSTATED change RUNS every reached
    step and writes ZERO certificates.

    MEASURED BEFORE THE FIX: the same pass approved 33 of 33 justified skips,
    each behind a freshly written ``skip_justified`` certificate — keeping prior
    results that do not exist. Omitting ``changed_terms`` arrived at the ladder as
    an empty ``ChangedStateTerms()``, whose set-disjointness test is vacuously
    satisfied, so the omission itself licensed the skip. That made silence a
    SECOND skip mechanism next to certificates, which are supposed to be the only
    one.

    The run cause is its OWN member (``unstated_change``), not ``effects_overlap``:
    nothing overlapped, because nothing was stated. The ledger must not report a
    proven overlap it never proved."""
    result, conn = _first_publish_pass(None)   # <- changed_terms OMITTED entirely

    approved_skips = [o for o in result.skips if o.cause == CAUSE_SKIP_JUSTIFIED]
    print(f"\n[reuse-sched/unstated] steps={len(result.outcomes)} "
          f"justified_skips={len(approved_skips)} "
          f"cause_counts={result.cause_counts()} certs={len(conn.rows)}")
    assert result.reached_fixed_point           # refusing to skip is not a failure
    assert len(result.outcomes) == _FIRST_PUBLISH_STEPS
    # 33 of 33 approved BEFORE -> 0 of 33 now.
    assert approved_skips == []
    assert result.skips == ()
    assert all(o.action == ACTION_RUN for o in result.outcomes)
    assert result.cause_counts() == {CAUSE_UNSTATED_CHANGE: _FIRST_PUBLISH_STEPS}
    # zero certificates: no skip was licensed, so none was recorded.
    assert conn.rows == []
    # every refusal names the missing statement, not a fabricated overlap.
    for outcome in result.outcomes:
        assert outcome.detail["refusal_reason"] == REFUSE_UNSTATED_CHANGE
        assert "overlapping_buckets" not in outcome.detail
        assert "overlapping_checks" not in outcome.detail


def test_explicit_nothing_changed_still_skips_every_step() -> None:
    """THE CONTROL (req.unstated.fail-closed ACCEPTANCE 2): the SAME first publish
    whose caller explicitly declares "nothing changed" still skips all 33 steps
    behind justified certificates.

    This passes both before and after the fix — it is what proves the fix did not
    buy fail-closedness by charging the honest steady-state caller its reuse. The
    only difference from the witness above is that the caller SAID something."""
    nothing = ChangedStateTerms.nothing_changed()
    step_ids = [f"step_{i:02d}" for i in range(_FIRST_PUBLISH_STEPS)]
    result, conn = _first_publish_pass({s: nothing for s in step_ids})

    approved_skips = [o for o in result.skips if o.cause == CAUSE_SKIP_JUSTIFIED]
    print(f"\n[reuse-sched/nothing-changed] steps={len(result.outcomes)} "
          f"justified_skips={len(approved_skips)} certs={len(conn.rows)}")
    assert result.reached_fixed_point
    assert len(approved_skips) == _FIRST_PUBLISH_STEPS
    assert result.cause_counts() == {CAUSE_SKIP_JUSTIFIED: _FIRST_PUBLISH_STEPS}
    assert len(conn.rows) == _FIRST_PUBLISH_STEPS


def test_a_step_missing_from_the_statement_map_refuses_alone() -> None:
    """Fail-closed is PER STEP, and it is the map LOOKUP that is fail-closed, not
    just the ``changed_terms=None`` case. A caller that states two of three steps
    keeps its two decisions and loses only the step it said nothing about — so a
    partially-derived change statement degrades into extra work, never into an
    unearned skip.

    This is the regression guard for the worklist growing at runtime: a dependent
    step discovered by ``step_runner`` is by construction absent from the map the
    caller built, and must RUN rather than skip on the omission."""
    conn = _CaptureConn()
    step_ids = ["stated_disjoint", "stated_overlap", "unstated"]
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=step_ids,
        steps={s: _step(s, ("k1",)) for s in step_ids},
        checks=(),
        current_facts={s: _facts(s) for s in step_ids},
        incumbent_selection={s: _incumbent() for s in step_ids},
        changed_terms={
            "stated_disjoint": _disjoint("z"),   # stated, disjoint  -> SKIP
            "stated_overlap": _overlap("k1"),    # stated, overlaps  -> RUN
            # "unstated" deliberately absent    # not stated        -> RUN
        },
    )
    by_step = {o.step_id: o for o in result.outcomes}
    print("\n[reuse-sched/partial-statement] cause_counts=", result.cause_counts())
    for o in sorted(result.outcomes, key=lambda o: o.step_id):
        print(f"[reuse-sched/partial-statement]   {o.step_id}: {o.action}/{o.cause}")
    assert result.reached_fixed_point
    assert by_step["stated_disjoint"].action == ACTION_SKIP
    assert by_step["stated_disjoint"].cause == CAUSE_SKIP_JUSTIFIED
    assert by_step["stated_overlap"].action == ACTION_RUN
    assert by_step["stated_overlap"].cause == CAUSE_EFFECTS_OVERLAP
    assert by_step["unstated"].action == ACTION_RUN
    assert by_step["unstated"].cause == CAUSE_UNSTATED_CHANGE
    # the three run/skip causes stay distinguishable in one ledger.
    assert result.cause_counts() == {
        CAUSE_SKIP_JUSTIFIED: 1, CAUSE_EFFECTS_OVERLAP: 1, CAUSE_UNSTATED_CHANGE: 1,
    }
    # exactly one certificate — the one skip that was licensed.
    assert len(conn.rows) == 1


def test_unstated_change_leaves_the_other_two_rungs_untouched() -> None:
    """THE THIRD ACCEPTANCE: the rungs that do NOT rest on the change statement
    keep working when the statement is missing. A valid prior certificate and a
    byte-identical prior-facts carry-forward are both decided BEFORE the
    justified-skip rung is reached, so both still skip with ``changed_terms``
    omitted entirely — their justification never was the change statement."""
    conn = _CaptureConn()
    facts_valid = _facts("valid")
    facts_degen = _facts("degen")
    prior = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="6", step_id="valid",
        comparison=_comparison(), dependencies=facts_valid,
    )
    result = run_reuse_pass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["valid", "degen", "unstated"],
        steps={"valid": _step("valid", ("k1",)),
               "degen": _step("degen", ("k2",)),
               "unstated": _step("unstated", ("k3",))},
        checks=(),
        prior_certificates={"valid": prior},
        prior_facts={"degen": facts_degen},
        current_facts={"valid": facts_valid, "degen": facts_degen,
                       "unstated": _facts("unstated")},
        incumbent_selection={"degen": _incumbent()},
        # changed_terms omitted for ALL THREE steps.
    )
    by_step = {o.step_id: o for o in result.outcomes}
    print("\n[reuse-sched/rungs-untouched] cause_counts=", result.cause_counts())
    assert result.reached_fixed_point
    assert by_step["valid"].cause == CAUSE_CERTIFICATE_VALID
    assert by_step["degen"].cause == CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED
    assert by_step["valid"].action == ACTION_SKIP
    assert by_step["degen"].action == ACTION_SKIP
    # only the step that reached the justified rung is refused.
    assert by_step["unstated"].action == ACTION_RUN
    assert by_step["unstated"].cause == CAUSE_UNSTATED_CHANGE


def test_degenerate_certificate_same_shape_and_invalidation() -> None:
    """A degenerate certificate has the SAME record shape as any certificate (all
    ten dependency classes, a validated comparison) and invalidates through the
    SAME :func:`validate_certificate` path — witnessed on at least three classes
    (§9.5). Validity is a function of the DEPENDENCIES, never the comparison
    kind."""
    facts = _facts("degen")
    cert = ReuseCertificate(
        deployment=_DEPLOYMENT, generation_id="7", step_id="claim_projection",
        comparison=build_degenerate_comparison(
            incumbent_outcome="carry_forward", incumbent_basis="unchanged_inputs",
        ),
        dependencies=facts,
    )
    # same record shape: degenerate kind + every dependency class.
    assert cert.comparison["kind"] == COMPARISON_KIND_DEGENERATE
    assert set(DEGENERATE_COMPARISON_KEYS) <= set(cert.comparison)
    assert set(cert.dependencies.to_json_dict()) == set(DEPENDENCY_CLASSES)
    # byte-identical facts -> valid through the SAME validate path.
    assert validate_certificate(facts, cert).valid
    # mutate at least three distinct classes; each invalidates exactly itself.
    mutations = {
        "input_digests": dataclasses.replace(
            facts, input_digests={"in": "digest:CHANGED"}),
        "catalog_version_id": dataclasses.replace(
            facts, catalog_version_id="catver:CHANGED"),
        "policy_fingerprint": dataclasses.replace(
            facts, policy_fingerprint="pol:CHANGED"),
    }
    assert len(mutations) >= 3
    for cls, mutated in mutations.items():
        outcome = validate_certificate(mutated, cert)
        print(f"[reuse-sched/degenerate-invalidate] mutate {cls} -> "
              f"invalidated={sorted(outcome.invalidated_classes)}")
        assert outcome.invalidated_classes == frozenset({cls}), cls
        assert not outcome.valid


def test_policy_and_precedence_fingerprint_changes_force_the_step_to_run() -> None:
    """task.repair.differential / spec §9.8 (the scheduler-side half): the carried
    selections ride the claim_projection STEP's certificate — degenerate when the
    whole step's inputs are unchanged, invalidated when they are not. A policy /
    precedence / generator edit moves the step's ``policy_fingerprint`` /
    ``precedence_fingerprint`` / ``generator_versions`` dependency class, so the
    prior certificate is invalidated on EXACTLY that class and, with the
    corresponding claim delta, the step RUNS (no per-bucket certificate — the
    mechanism stays step-level). The derivation-side recorded-cause full path
    (precedent_scope_unprovable / policy_change) is witnessed in
    tests/substrate/consistency/test_repair_selection.py."""
    prior = _facts("prior")
    mutations = {
        "policy_fingerprint": dataclasses.replace(
            prior, policy_fingerprint="pol:CHANGED"),
        "precedence_fingerprint": dataclasses.replace(
            prior, precedence_fingerprint="prec:CHANGED"),
        "generator_versions": dataclasses.replace(
            prior, generator_versions={"gen": "v2"}),
    }
    for cls, current in mutations.items():
        cert = ReuseCertificate(
            deployment=_DEPLOYMENT, generation_id="6", step_id="claim_projection",
            comparison=_comparison(), dependencies=prior,
        )
        # the certificate invalidates on EXACTLY the moved class.
        validity = validate_certificate(current, cert)
        assert validity.invalidated_classes == frozenset({cls}), cls
        assert not validity.valid
        # with the corresponding claim delta the step RUNS (not a degenerate carry).
        conn = _CaptureConn()
        result = run_reuse_pass(
            conn, deployment=_DEPLOYMENT, generation_id="7",
            worklist_seed=["claim_projection"],
            steps={"claim_projection": _step("claim_projection", ("k1",))}, checks=(),
            prior_certificates={"claim_projection": cert},
            prior_facts={"claim_projection": prior},   # would be degenerate...
            current_facts={"claim_projection": current},  # ...but the class moved
            changed_terms={"claim_projection": _overlap("k1")},
            incumbent_selection={"claim_projection": _incumbent()},
        )
        outcome = result.outcomes[0]
        print(f"\n[reuse-sched/fingerprint] mutate {cls} -> {outcome.action}/"
              f"{outcome.cause} invalidated="
              f"{outcome.detail.get('prior_certificate_invalidated')}")
        assert outcome.action == ACTION_RUN
        assert outcome.cause == CAUSE_EFFECTS_OVERLAP
        assert outcome.detail["prior_certificate_invalidated"] == [cls]
        assert conn.rows == []      # a run writes no certificate


def test_commit_refused_before_fixed_point() -> None:
    """§9.6: the maintenance state commits ONLY at the fixed point. A run that
    discovers a dependent leaves the worklist non-empty — the commit gate refuses
    — until the dependent is processed."""
    conn = _CaptureConn()
    steps = {
        "producer": _step("producer", ("k1",)),
        "dependent": _step("dependent", ("k9",)),
    }
    reuse_pass = ReusePass(
        conn, deployment=_DEPLOYMENT, generation_id="7",
        worklist_seed=["producer"], steps=steps, checks=(),
        changed_terms={"producer": _overlap("k1"), "dependent": _disjoint("z")},
        current_facts={"producer": _facts("producer"), "dependent": _facts("dep")},
        incumbent_selection={"dependent": _incumbent()},
        # RUNNING the producer discovers the dependent (its observed output).
        step_runner=lambda step_id: (["dependent"] if step_id == "producer" else []),
    )

    first = reuse_pass.step()          # process the producer -> RUN -> discover dep
    print(f"\n[reuse-sched/commit-gate] after step 1: outcome={first.action} "
          f"pending={reuse_pass.pending} at_fixed_point={reuse_pass.at_fixed_point}")
    assert first.action == ACTION_RUN
    assert reuse_pass.pending == ("dependent",)
    assert not reuse_pass.at_fixed_point
    # commit is REFUSED while the discovered dependent is unprocessed.
    with pytest.raises(CommitBeforeFixedPoint):
        commit_gate(reuse_pass)

    reuse_pass.run_to_fixed_point()    # process the dependent -> SKIP -> fixed point
    print(f"[reuse-sched/commit-gate] at fixed point: pending={reuse_pass.pending} "
          f"outcomes={[(o.step_id, o.action) for o in reuse_pass.outcomes]}")
    assert reuse_pass.at_fixed_point
    assert reuse_pass.pending == ()
    commit_gate(reuse_pass)            # now it succeeds — no raise
    dep_outcome = {o.step_id: o for o in reuse_pass.outcomes}["dependent"]
    assert dep_outcome.action == ACTION_SKIP


def test_outside_cone_is_enumerated_with_reasons() -> None:
    """The single-mechanism requirement demands the out-of-cone artifacts be
    ENUMERATED (asserted by a test, not implied). Every family carries a
    one-line reason."""
    assert set(OUTSIDE_CONE) == {
        "sources", "generic_enrichers", "native_views", "rollups", "embeddings",
    }
    for family, reason in OUTSIDE_CONE.items():
        assert isinstance(reason, str) and len(reason) > 10, family
    print("\n[reuse-sched/outside-cone]", sorted(OUTSIDE_CONE))


def test_opt_in_flag_consumed_by_the_stack(tmp_path: Path) -> None:
    """The publish-cycle wiring is opt-in per deployment: the fixture stack reads
    the flag ``True`` and a deployment that omits it reads ``False`` — so no other
    deployment's cycle changes (req.reuse.single-mechanism)."""
    import yaml

    from okg.substrate.publisher.deployment_stack import (
        load_resolution_aware_reuse_enabled,
    )

    assert load_resolution_aware_reuse_enabled(_DEP_DIR) is True

    plain = tmp_path / "plain"
    plain.mkdir()
    (plain / "deployment.yaml").write_text(yaml.safe_dump({"name": "plain"}))
    assert load_resolution_aware_reuse_enabled(plain) is False
    print("\n[reuse-sched/opt-in] fixture=True plain=False")


def test_post_cycle_seam_disabled_leaves_non_opted_cycle_unchanged(
    tmp_path: Path,
) -> None:
    """The publisher-workflow seam is opt-in: for a deployment that does NOT set
    the flag it returns an explicit ``disabled`` marker and touches nothing (no
    connection to ``dsn``) — so no other deployment's cycle changes this wave."""
    import yaml

    from okg.substrate.runtime.reuse_pass_workflow import run_post_cycle_reuse_pass

    plain = tmp_path / "plain"
    plain.mkdir()
    (plain / "deployment.yaml").write_text(yaml.safe_dump({"name": "plain"}))

    out = run_post_cycle_reuse_pass(
        dsn="postgres://must-not-connect",   # a disabled seam never dials the DB
        deployment="plain",
        summary={"status": "published", "compat_generation_id": 1},
        deployment_dir=plain,
    )
    print("\n[reuse-sched/seam-disabled]", out)
    assert out == {"reuse_pass": "disabled", "deployment": "plain"}


def test_post_cycle_caller_states_its_change_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.unstated.caller: the publisher's post-cycle hook DECLARES what changed
    instead of relying on an omission — and this is the guard that catches it
    regressing to silence.

    It captures the ``changed_terms`` the callsite actually hands the scheduler,
    so the assertion is on the CALL, not on a comment or a source string. Before
    the fix the callsite passed nothing and the scheduler manufactured a
    permissive empty statement per step; that safety was incidental, resting on
    ``prior_facts`` making the degenerate rung fire first. The v1 fixture cone
    genuinely changes nothing, so the truthful statement here is
    ``nothing_changed()`` for every cone step — and the pass still behaves exactly
    as it did (the degenerate rung, unaffected by the statement, still decides)."""
    from okg.substrate.consistency.possible_effects import UnstatedChange
    from okg.substrate.runtime import reuse_pass_workflow

    captured: dict[str, Any] = {}

    def _capture(_conn: Any, **kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"reuse_pass": "committed", "certificates_committed": 0}

    class _CtxConn(_CaptureConn):
        def __enter__(self) -> "_CtxConn":
            return self

        def __exit__(self, *_a: Any) -> bool:
            return False

    monkeypatch.setattr(reuse_pass_workflow, "run_reuse_pass_in_cycle", _capture)
    monkeypatch.setattr(
        reuse_pass_workflow, "poisoned_producers_for_generation",
        lambda *_a, **_k: frozenset(),
    )
    # `_run_fixture_cone_pass` imports the substrate connection factory
    # inside the function, so the factory's own `connect` is the seam; a fake
    # psycopg module in `sys.modules` would not be consulted, because
    # `okg.substrate.db.connection` bound the real psycopg at import time.
    from okg.substrate.db import connection as db_connection

    monkeypatch.setattr(db_connection, "connect", lambda *_a, **_k: _CtxConn())

    reuse_pass_workflow._run_fixture_cone_pass(
        dsn="postgres://captured", deployment=_DEPLOYMENT, generation_id="7",
    )

    statements = captured.get("changed_terms")
    print("\n[reuse-sched/caller-states-change] changed_terms=", statements)
    # a statement is made, for EVERY step in the cone — not one step, not None.
    assert statements is not None, (
        "_run_fixture_cone_pass regressed to SILENCE: it handed the scheduler no "
        "change statement, so the justified-skip rung would decide on an "
        "omission rather than on something the callsite is accountable for "
        f"(kwargs passed: {sorted(captured)})"
    )
    assert set(statements) == set(captured["steps"])
    # ...and it is the EXPLICIT permissive one, which is the truth for this cone.
    for step_id, statement in statements.items():
        assert isinstance(statement, ChangedStateTerms), step_id
        assert statement.declares_nothing_changed is True, step_id
        assert not isinstance(statement, UnstatedChange), step_id
    # behaviour is unchanged: prior_facts still equals current_facts, so the
    # degenerate carry-forward rung — not the change statement — decides.
    assert captured["prior_facts"] == captured["current_facts"]


def test_reuse_pass_registers_as_a_dbos_step() -> None:
    """The reuse pass registers as a named DBOS step (the publisher-workflow
    idiom); the accessor finds it and it runs the pass."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime.reuse_pass_workflow import (
        REUSE_PASS_STEP_NAME,
        _DECORATED,
        register_reuse_pass_step,
        reuse_pass_step,
    )

    with mock_dbos_context():
        register_reuse_pass_step(deployment=_DEPLOYMENT)
        assert REUSE_PASS_STEP_NAME in _DECORATED
        step = reuse_pass_step()
        assert callable(step)

        conn = _CaptureConn()
        result = step(
            conn, generation_id="7", worklist_seed=["keep"],
            steps={"keep": _step("keep")}, checks=(),
            changed_terms={"keep": _disjoint("z")},
            current_facts={"keep": _facts("keep")},
            incumbent_selection={"keep": _incumbent()},
        )
    assert result.reached_fixed_point
    assert result.outcomes[0].action == ACTION_SKIP
    print(f"\n[reuse-sched/dbos-step] step ran: {result.outcomes[0].action} "
          f"cause={result.outcomes[0].cause} digest={result.digest()[:12]}")


# =========================================================================== #
# DB witnesses (Postgres): transactional commit, discard, abort, crash, live.  #
# =========================================================================== #
psycopg = pytest.importorskip("psycopg")


def _checkpoint_writer(deployment: str, generation_id: str) -> Callable[[Any], Any]:
    """A checkpoint writer that records a repair-selection row — one of the
    maintenance-state artifacts §9.6 commits atomically with the certificates.
    Returns the generation id it stamped."""

    def _write(conn: Any) -> str:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.repair_selections
                  (deployment_name, generation_id, bucket_key, outcome,
                   selection_digest)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (deployment, generation_id, "checkpoint", "selected",
                 f"digest:{generation_id}"),
            )
        return generation_id

    return _write


def _checkpoint_exists(dsn: str, deployment: str, generation_id: str) -> bool:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.repair_selections "
            "WHERE deployment_name = %s AND generation_id = %s",
            (deployment, generation_id),
        )
        return int(cur.fetchone()[0]) > 0


def _certificate_count(dsn: str, deployment: str, generation_id: str) -> int:
    store = ReuseCertificateStore(deployment=deployment)
    with psycopg.connect(dsn, autocommit=True) as conn:
        return len(store.for_generation(conn, generation_id))


@pytest.mark.postgres
def test_operator_failure_aborts_leaving_prior_state_intact(
    clean_runtime: str,
) -> None:
    """§9.6: a step that raises when run ABORTS the update leaving prior state
    intact — no checkpoint, and no certificate rows for the aborted generation
    (the tentative skip recorded before the failure is rolled back)."""
    from okg.substrate.runtime.reuse_pass_workflow import run_reuse_pass_in_cycle

    dep = f"reuse-abort-{uuid.uuid4().hex[:8]}"
    gen = "7"
    steps = {
        "keep": _step("keep", ("k1",)),        # skips first -> tentative cert
        "boom": _step("boom", ("k2",)),        # runs -> its runner raises
    }

    def runner(step_id: str) -> list[str]:
        if step_id == "boom":
            raise RuntimeError("operator down mid-update")
        return []

    conn = psycopg.connect(clean_runtime)      # autocommit=False -> one transaction
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=dep, generation_id=gen,
            worklist_seed=["keep", "boom"], steps=steps, checks=(),
            changed_terms={"keep": _disjoint("z"), "boom": _overlap("k2")},
            current_facts={"keep": _facts("keep"), "boom": _facts("boom")},
            incumbent_selection={"keep": _incumbent()},
            step_runner=runner,
            checkpoint_writer=_checkpoint_writer(dep, gen),
        )
    finally:
        conn.close()

    print(f"\n[reuse-sched/abort] reuse_pass={summary['reuse_pass']} "
          f"cause={summary['fallback_cause']} failed_step={summary['failed_step']} "
          f"certs_committed={summary['certificates_committed']}")
    result = summary["result"]
    assert result.terminal == TERMINAL_ABORT
    assert result.fallback_cause == CAUSE_OPERATOR_FAILURE
    assert result.failed_step == "boom"
    assert summary["certificates_committed"] == 0
    # prior state intact: no checkpoint, no certificate rows for the aborted gen.
    assert _certificate_count(clean_runtime, dep, gen) == 0
    assert not _checkpoint_exists(clean_runtime, dep, gen)


@pytest.mark.postgres
def test_discarded_pass_writes_zero_certificates(clean_runtime: str) -> None:
    """An out-of-envelope output (an ``okg.envelope_violations`` poisoning row for
    the step's producer) DISCARDS the pass into complete execution — the tentative
    certificate is rolled back, so the discarded pass writes zero (§9.6)."""
    from okg.substrate.producer_authority import record_envelope_violation
    from okg.substrate.runtime.reuse_pass_workflow import (
        poisoned_producers_for_generation,
        run_reuse_pass_in_cycle,
    )

    dep = f"reuse-discard-{uuid.uuid4().hex[:8]}"
    gen = "7"
    # Record a poisoning envelope violation for the producer of "poisoned".
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_envelope_violation(
            conn, deployment=dep, generation_id=gen, producer_id="poisoned",
            code="producer_node_subtype_not_allowed", record_kind="node",
            detail={"reason": "out-of-envelope node subtype"},
        )
        poisoned = poisoned_producers_for_generation(
            conn, deployment=dep, generation_id=gen,
        )
    assert poisoned == frozenset({"poisoned"})

    completed: list[str] = []
    conn = psycopg.connect(clean_runtime)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=dep, generation_id=gen,
            worklist_seed=["keep", "poisoned"],
            steps={"keep": _step("keep", ("k1",)),
                   "poisoned": _step("poisoned", ("k2",))},
            checks=(),
            changed_terms={"keep": _disjoint("z")},
            current_facts={"keep": _facts("keep"), "poisoned": _facts("poisoned")},
            incumbent_selection={"keep": _incumbent()},
            poisoned_producers=poisoned,
            complete_executor=lambda: completed.append("force_full") or "complete",
        )
    finally:
        conn.close()

    print(f"\n[reuse-sched/discard] reuse_pass={summary['reuse_pass']} "
          f"cause={summary['fallback_cause']} certs={summary['certificates_committed']} "
          f"complete_execution={summary['complete_execution']}")
    assert summary["result"].terminal == TERMINAL_DISCARD
    assert summary["fallback_cause"] == CAUSE_ENVELOPE_VIOLATION
    assert summary["certificates_committed"] == 0
    assert completed == ["force_full"]                    # complete path executed
    assert _certificate_count(clean_runtime, dep, gen) == 0   # zero, structurally


@pytest.mark.postgres
def test_crash_mid_update_resumes_or_discards(clean_runtime: str) -> None:
    """§9.6: the pass rides the DBOS workflow as a resumable step. A crash between
    the scheduler step and the checkpoint leaves NEITHER; the deterministic step
    re-executed at the boundary yields the SAME certificates (byte-equal digest),
    and the commit lands the certificates atomically with the checkpoint — either
    both or neither, never a half-commit."""
    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.runtime.reuse_pass_workflow import (
        commit_maintenance_state,
        register_reuse_pass_step,
        reuse_pass_step,
    )

    dep = f"reuse-crash-{uuid.uuid4().hex[:8]}"
    gen = "7"
    inputs = dict(
        generation_id=gen, worklist_seed=["s1", "s2"],
        steps={"s1": _step("s1", ("k1",)), "s2": _step("s2", ("k2",))}, checks=(),
        changed_terms={"s1": _disjoint("z1"), "s2": _disjoint("z2")},
        current_facts={"s1": _facts("s1"), "s2": _facts("s2")},
        incumbent_selection={"s1": _incumbent(), "s2": _incumbent()},
        cert_store=ReuseCertificateStore(deployment=dep),
    )

    with mock_dbos_context():
        register_reuse_pass_step(deployment=dep)
        step = reuse_pass_step()

        # ---- run 1: the scheduler step records certificates, then the worker is
        #      killed BEFORE the checkpoint commit (the transaction is lost).
        conn1 = psycopg.connect(clean_runtime)
        result1 = step(conn1, **inputs)
        digest1 = result1.digest()
        conn1.rollback()                       # crash: nothing committed
        conn1.close()
        # NEITHER the checkpoint nor the certificates exist.
        assert _certificate_count(clean_runtime, dep, gen) == 0
        assert not _checkpoint_exists(clean_runtime, dep, gen)

        # ---- resume: the step re-executes at the boundary and reaches the SAME
        #      decisions (byte-equal digest), then the checkpoint commit lands.
        conn2 = psycopg.connect(clean_runtime)
        try:
            result2 = step(conn2, **inputs)
            digest2 = result2.digest()
            assert digest2 == digest1          # deterministic resume
            commit = commit_maintenance_state(
                conn2, result2, checkpoint_writer=_checkpoint_writer(dep, gen),
            )
        finally:
            conn2.close()

    print(f"\n[reuse-sched/crash] digest(run1)={digest1[:12]} digest(resume)={digest2[:12]} "
          f"committed_certs={commit['certificates_committed']}")
    # BOTH the checkpoint and the certificates now exist (atomic commit).
    assert commit["certificates_committed"] == 2
    assert _certificate_count(clean_runtime, dep, gen) == 2
    assert _checkpoint_exists(clean_runtime, dep, gen)


# --------------------------------------------------------------------------- #
# Scratch-DB factory (test_equivalence_harness idiom) for the complete path.   #
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
    base_dsn = okg_migrated
    created: list[str] = []

    def _make() -> str:
        dbname = f"okg_test_reuse_{uuid.uuid4().hex[:10]}"
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


# --------------------------------------------------------------------------- #
# Fixture fact builders (test_claim_plane_wire idiom).                          #
# --------------------------------------------------------------------------- #
def _install_catalog(dsn: str) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(_DEP_DIR)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _fixture_facts() -> list[Any]:
    from okg.substrate.library.sources.base import NodeFact

    scope = "HCAL/Run3"

    def claim(node_id: str, value: str, *, key: str, source: str,
              source_class: str, **signals: Any) -> NodeFact:
        attrs = {
            "predicate": _PREDICATE, "normalized_key": key, "value": value,
            "scope": scope, "scope_state": "known",
            "claim_provenance": "derived", "source_id": source,
            "source_class": source_class,
        }
        attrs.update({k: str(v) for k, v in signals.items()})
        return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})

    sfx = uuid.uuid4().hex[:8]
    key1 = f"key|one:{sfx}"
    return [
        claim(f"claim:a:{sfx}", "vX", key=key1, source="page:1",
              source_class="release_notes", source_authority=10),
        claim(f"claim:b:{sfx}", "vY", key=key1, source="page:2",
              source_class="wiki", source_authority=1),
        NodeFact(f"pol:{sfx}", "predicate_policy",
                 {"enabled": "true", "policy_id": "unique_preferred",
                  "policy_kind": "functional", "predicate": _PREDICATE},
                 {"id": f"pol:{sfx}"}, {"v": 1}),
    ]


@pytest.mark.postgres
def test_unknown_dependency_discards_into_complete_execution(
    clean_runtime: str, scratch_db: Callable[[], str],
) -> None:
    """§9.6: a reached step with NO declared envelope is an unknown dependency —
    the tentative pass is discarded and the COMPLETE path executes (the force-full
    lever, ``run_dependency_baseline``: every derivation full with a recorded
    cause), writing zero certificates for the discarded pass."""
    from okg.substrate.qa.complete_reference import (
        OutcomeReplaySource,
        assert_every_check_full_sweep,
        assert_every_derivation_reran_full,
        run_dependency_baseline,
    )
    from okg.substrate.runtime.reuse_pass_workflow import run_reuse_pass_in_cycle

    dep = f"reuse-unknown-{uuid.uuid4().hex[:8]}"
    gen = "7"
    facts = _fixture_facts()
    scratch = scratch_db()

    def complete_executor() -> Any:
        return run_dependency_baseline(
            facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch,
            outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
        )

    conn = psycopg.connect(clean_runtime)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=dep, generation_id=gen,
            # "orphan" is reached (seeded) but has NO envelope -> unknown dependency.
            worklist_seed=["keep", "orphan"],
            steps={"keep": _step("keep", ("k1",))},   # note: "orphan" absent
            checks=(),
            changed_terms={"keep": _disjoint("z")},
            current_facts={"keep": _facts("keep")},
            incumbent_selection={"keep": _incumbent()},
            complete_executor=complete_executor,
        )
    finally:
        conn.close()

    complete = summary["complete_execution"]
    print(f"\n[reuse-sched/unknown-dep] reuse_pass={summary['reuse_pass']} "
          f"cause={summary['fallback_cause']} failed_step={summary['failed_step']} "
          f"certs={summary['certificates_committed']} "
          f"complete_gen={complete.generation_id} mode={complete.mode}")
    assert summary["result"].terminal == TERMINAL_DISCARD
    assert summary["fallback_cause"] == CAUSE_UNKNOWN_DEPENDENCY
    assert summary["failed_step"] == "orphan"
    assert summary["certificates_committed"] == 0
    # the complete path executed the force-full lever: every derivation full.
    assert complete.mode == "dependency_baseline"
    assert_every_derivation_reran_full(complete)
    assert_every_check_full_sweep(complete)
    # zero certificates for the discarded pass.
    assert _certificate_count(clean_runtime, dep, gen) == 0


@pytest.mark.postgres
def test_scheduler_governed_publish_equals_complete_reference(
    clean_runtime: str, scratch_db: Callable[[], str],
) -> None:
    """The equivalence PRE-witness (the full series is task.reuse.live-proof): a
    scheduler-governed publish that records at least one genuine skip produces the
    SAME resolved state a complete-reference execution produces — recording the
    certificate does not perturb the resolved state (§9.6 / §9.1).

    The pass declares ``nothing_changed()`` per cone step. That is the truth here
    (nothing mutates the fixture between the publish above and this pass) and it is
    what makes the skip GENUINE. Previously the call omitted ``changed_terms``
    entirely and got its skips from the omission — this test was one of the places
    pinning the defect that PACT consistency-unstated-change-is-not-no-change
    fixes."""
    from okg.substrate.consistency.equivalence_differ import (
        VERDICT_EQUAL,
        ResolvedStateSnapshot,
        diff_resolved_states,
    )
    from okg.substrate.publisher import Publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )
    from okg.substrate.qa.complete_reference import (
        OutcomeReplaySource,
        run_complete_reference,
    )
    from okg.substrate.runtime.reuse_pass_workflow import (
        build_claim_plane_cone,
        run_reuse_pass_in_cycle,
    )

    _install_catalog(clean_runtime)
    facts = _fixture_facts()

    # ---- a real fixture publish (the differential production path) -----------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)
    stack = build_deployment_publisher_stack(
        dsn=clean_runtime, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(clean_runtime, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    prod_compat = str(out["compat_generation_id"])
    prod_chronos = str(out["generation_id"])

    # ---- the scheduler pass over the published claim-plane cone --------------
    steps, seed, cone_facts, incumbent = build_claim_plane_cone(_DEPLOYMENT)
    cert_store = ReuseCertificateStore(deployment=_DEPLOYMENT)
    conn = psycopg.connect(clean_runtime)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=_DEPLOYMENT, generation_id=prod_compat,
            worklist_seed=seed, steps=steps, current_facts=cone_facts,
            # EXPLICIT: nothing changed between the publish and this pass.
            changed_terms={
                step_id: ChangedStateTerms.nothing_changed() for step_id in steps
            },
            incumbent_selection=incumbent, cert_store=cert_store,
            # checkpoint already committed by the publish above; commit the certs.
            checkpoint_writer=lambda _conn: prod_compat,
        )
    finally:
        conn.close()

    skips = summary["result"].skips
    print(f"\n[reuse-sched/equivalence] reuse_pass={summary['reuse_pass']} "
          f"skips={[o.step_id for o in skips]} "
          f"certs_committed={summary['certificates_committed']}")
    assert summary["result"].reached_fixed_point
    assert summary["certificates_committed"] >= 1       # a genuine recorded skip
    assert _certificate_count(clean_runtime, _DEPLOYMENT, prod_compat) >= 1

    # ---- the complete-reference execution over the SAME pinned facts ---------
    scratch = scratch_db()
    reference_result = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        candidate = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT, generation_id=prod_compat,
            chronos_generation_id=prod_chronos,
        )
    with psycopg.connect(scratch, autocommit=True) as conn:
        reference = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=reference_result.compat_generation_id,
            chronos_generation_id=reference_result.generation_id,
        )

    verdict = diff_resolved_states(reference, candidate)
    print(f"[reuse-sched/equivalence] verdict={verdict.verdict} "
          f"prod_gen={prod_compat} ref_gen={reference_result.compat_generation_id}")
    assert verdict.verdict == VERDICT_EQUAL, verdict.to_json_dict()
    # the recorded skip's certificate enumerates every dependency class.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = cert_store.for_generation(conn, prod_compat)
    assert recorded
    assert set(recorded[0].dependencies.to_json_dict()) == set(DEPENDENCY_CLASSES)


@pytest.mark.postgres
def test_no_cone_step_skips_without_certificate(
    clean_runtime: str, scratch_db: Callable[[], str],
) -> None:
    """End-to-end (task.reuse.subsume-carry-forward, §9.5): publish, then run the
    claim-plane cone with the prior generation's manifest UNCHANGED. Every cone
    step skips behind a DEGENERATE certificate — there is no cone path that skips
    a step without recording one — and the resolved state is EQUAL to a
    complete-reference execution. Mutating one dependency class then makes that
    step RUN next cycle (its degenerate certificate is invalidated), while the
    untouched steps' degenerate certificates revalidate."""
    from okg.substrate.consistency.equivalence_differ import (
        VERDICT_EQUAL,
        ResolvedStateSnapshot,
        diff_resolved_states,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )
    from okg.substrate.qa.complete_reference import (
        OutcomeReplaySource,
        run_complete_reference,
    )
    from okg.substrate.runtime.reuse_pass_workflow import (
        CLAIM_EFFECTIVE_STEP,
        build_claim_plane_cone,
        run_reuse_pass_in_cycle,
    )

    _install_catalog(clean_runtime)
    facts = _fixture_facts()

    # ---- a real fixture publish (produces the resolved state) ----------------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)
    stack = build_deployment_publisher_stack(
        dsn=clean_runtime, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(clean_runtime, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    prod_compat = str(out["compat_generation_id"])
    prod_chronos = str(out["generation_id"])

    # ---- the cone pass with the prior manifest UNCHANGED -> degenerate skips --
    steps, seed, cone_facts, incumbent = build_claim_plane_cone(_DEPLOYMENT)
    cert_store = ReuseCertificateStore(deployment=_DEPLOYMENT)
    conn = psycopg.connect(clean_runtime)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=_DEPLOYMENT, generation_id=prod_compat,
            worklist_seed=seed, steps=steps, current_facts=cone_facts,
            prior_facts=cone_facts,               # unchanged prior manifest
            prior_generation_id="prior",
            incumbent_selection=incumbent, cert_store=cert_store,
            checkpoint_writer=lambda _conn: prod_compat,  # checkpoint already done
        )
    finally:
        conn.close()

    result = summary["result"]
    assert result.reached_fixed_point
    # every cone step SKIPPED behind a DEGENERATE certificate; none ran, and no
    # skip is without a certificate digest (the single skip mechanism, §9.5).
    assert result.skips
    assert not result.runs
    assert {o.cause for o in result.skips} == {
        CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED
    }
    for o in result.skips:
        assert o.certificate_digest, o.step_id

    # ---- the recorded rows: degenerate kind, all ten classes (printed) -------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = cert_store.for_generation(conn, prod_compat)
        rows = {
            r["step_id"]: r
            for r in cert_store.quotable_rows(conn, generation_id=prod_compat)
        }
    assert len(recorded) == len(result.skips)
    # every skipped step has a queryable certificate row (no dangling skip).
    assert {c.step_id for c in recorded} == {o.step_id for o in result.skips}
    print(f"\n[reuse-sched/no-skip-without-cert] degenerate certificate rows at "
          f"gen {prod_compat}:")
    for cert in sorted(recorded, key=lambda c: c.step_id):
        row = rows[cert.step_id]
        assert cert.comparison["kind"] == COMPARISON_KIND_DEGENERATE
        assert set(cert.dependencies.to_json_dict()) == set(DEPENDENCY_CLASSES)
        assert row["dependency_classes"] == len(DEPENDENCY_CLASSES)
        print(f"   {cert.step_id}: kind={cert.comparison['kind']} "
              f"digest={cert.digest[:16]} classes={row['dependency_classes']} "
              f"incumbent={row['incumbent_outcome']}/{row['incumbent_basis']}")

    # ---- resolved state EQUAL vs the complete-reference execution -------------
    scratch = scratch_db()
    reference_result = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        candidate = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT, generation_id=prod_compat,
            chronos_generation_id=prod_chronos,
        )
    with psycopg.connect(scratch, autocommit=True) as conn:
        reference = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=reference_result.compat_generation_id,
            chronos_generation_id=reference_result.generation_id,
        )
    verdict = diff_resolved_states(reference, candidate)
    print(f"[reuse-sched/no-skip-without-cert] verdict={verdict.verdict} "
          f"prod_gen={prod_compat} ref_gen={reference_result.compat_generation_id}")
    assert verdict.verdict == VERDICT_EQUAL, verdict.to_json_dict()

    # ---- mutate ONE dependency class -> that step RUNS next cycle -------------
    #      The recorded degenerate certificates become the prior certificates. The
    #      mutated step's certificate is invalidated (input_digests moved) and,
    #      with the corresponding claim delta, the step RUNS; the untouched steps'
    #      degenerate certificates revalidate (certificate_valid).
    prior_certs = {c.step_id: c for c in recorded}
    mutated_step = CLAIM_EFFECTIVE_STEP
    mutated_facts = dict(cone_facts)
    mutated_facts[mutated_step] = dataclasses.replace(
        cone_facts[mutated_step], input_digests={"changed": "digest:MUTATED"},
    )
    cap = _CaptureConn()
    next_result = run_reuse_pass(
        cap, deployment=_DEPLOYMENT, generation_id=f"{prod_compat}-next",
        worklist_seed=seed, steps=steps,
        prior_certificates=prior_certs,
        prior_facts=cone_facts,                   # last generation's manifest
        current_facts=mutated_facts,
        changed_terms={mutated_step: ChangedStateTerms(
            buckets=frozenset({(_PREDICATE, "mutated")}))},
        incumbent_selection=incumbent,
    )
    by_step = {o.step_id: o for o in next_result.outcomes}
    print("[reuse-sched/no-skip-without-cert] mutation cycle: "
          + ", ".join(f"{s}={by_step[s].action}/{by_step[s].cause}" for s in seed))
    assert by_step[mutated_step].action == ACTION_RUN
    assert by_step[mutated_step].cause == CAUSE_EFFECTS_OVERLAP
    assert by_step[mutated_step].detail["prior_certificate_invalidated"] == [
        "input_digests"
    ]
    # the untouched cone steps still skip (their degenerate certificate revalidates).
    for sid in seed:
        if sid != mutated_step:
            assert by_step[sid].action == ACTION_SKIP, sid
            assert by_step[sid].cause == CAUSE_CERTIFICATE_VALID, sid
