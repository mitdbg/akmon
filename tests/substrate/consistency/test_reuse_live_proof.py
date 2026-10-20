"""Live equivalence with resolution-aware REUSE enabled — the same-result payoff.

PACT ``consistency-resolution-aware-reuse``, task.reuse.live-proof
(req.reuse.live-equivalence). The program's same-result evidence: over a recorded
series of real updates on the in-tree ``consistency-fixture`` deployment, a
SCHEDULER-GOVERNED differential publish returns EXACTLY the resolved state a
complete-reference execution returns (§9.1) — including the CENTREPIECE genuine
changed-inputs skip, where a producer step whose input changed is skipped behind
a bounded-dominance certificate and the differential run never extracts a loser
the reference contains, a §9.7 difference in RAW EXTRACTION that leaves the
resolved DECISION exactly equal.

The recorded series exercises the four shapes (task.reuse.live-proof):

* update 0 — a fresh conflict (registry beats wiki by declared source authority):
  first publish, the FULL path, verdict EQUAL;
* update 1 — a PURE bucket-scoped incremental (a higher-authority registry claim
  arrives for the same bucket): the differential publish runs claim_projection
  INCREMENTAL and the selection family CARRIES, verdict EQUAL including selection;
* update 2 — an unchanged no-claim-delta update (a policy re-affirmation): every
  claim-plane cone step skips behind a DEGENERATE certificate, verdict plain EQUAL;
* update 3 — THE GENUINE CHANGED-INPUTS SKIP: a crawl batch arrives; the crawl
  claim for the decisive bucket is skipped behind a ``resolution_dominance``
  certificate (its every permitted output loses to the incumbent's declared source
  authority), so the differential run never extracts it, while the dependency
  baseline DOES run the step and the complete reference contains the superseded
  loser. The differential-vs-reference verdict is EQUAL-with-certified-differences
  (never a silent plain-equal); the baseline ran the step (a real deviation from
  dependency execution).

The genuine skip is the LAST update deliberately: the withheld loser is a
persistent difference in the differential run's raw extraction (it is never
re-applied), so any later update would inherit it — putting the skip last keeps
every earlier update a clean plain-EQUAL while still witnessing the payoff. The
verification clause mandates the four shapes, EQUAL on every non-excluded update,
one certificate-backed skip the baseline ran, and published generations — not a
particular order.

Seeding + scratch idioms follow test_equivalence_harness.py and
test_reuse_scheduler.py.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency.equivalence_differ import (  # noqa: E402
    FAMILY_EFFECTIVE,
    FAMILY_FINDING,
    FAMILY_RESOLUTION,
    FAMILY_SELECTION,
    VERDICT_EQUAL,
    VERDICT_MISMATCH,
    CertifiedDifferences,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.consistency.reuse_certificates import (  # noqa: E402
    COMPARISON_KIND_RESOLUTION,
    ReuseCertificateStore,
)
from okg.substrate.consistency.reuse_scheduler import (  # noqa: E402
    CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED,
    CAUSE_SKIP_DOMINANCE,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    bucket_row_key,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.qa.equivalence_series import (  # noqa: E402
    COMPARISON_BASELINE,
    COMPARISON_DIFFERENTIAL,
    ReuseConeResult,
    ReuseGovernedUpdate,
    load_series_verdicts,
    run_reuse_governed_series,
)
from okg.substrate.runtime.reuse_pass_workflow import (  # noqa: E402
    CLAIM_PRODUCER_STEP,
    build_claim_plane_cone,
    build_dominance_skip_cone,
    run_reuse_pass_in_cycle,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.deployment_catalog_claim import (  # noqa: E402
    load_and_claim_deployment_catalog,
)

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_KEY1 = "pref|k1"                     # the decisive incumbent bucket
_KEY2 = "pref|k2"                     # a fresh bucket the crawl batch extracts into
_SOURCE_AUTHORITY_BASIS = "declared_source_authority"
_INCUMBENT_AUTHORITY = 20             # claim G's declared source authority (winner)

_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)


# --------------------------------------------------------------------------- #
# Scratch-DB factory (test_equivalence_harness idiom).                          #
# --------------------------------------------------------------------------- #
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
        dbname = f"okg_test_liveproof_{uuid.uuid4().hex[:10]}"
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
# Production provisioning + fact builders.                                     #
# --------------------------------------------------------------------------- #
def _install_production_catalog(dsn: str) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(_DEP_DIR)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _claim(
    node_id: str, value: str, *, key: str, source: str, source_class: str,
    **signals: Any,
) -> NodeFact:
    attrs = {
        "predicate": _PREDICATE, "normalized_key": key, "value": value,
        "scope": _SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": _CONSTRAINT,
             "policy_kind": "functional", "predicate": _PREDICATE}
    attrs.update({k: str(v) for k, v in extra.items()})
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


# The evolving-bucket node ids (stable across the series so upserts land).
_E, _W, _G = "claim:e", "claim:w", "claim:g"
_CR1, _CR2 = "claim:cr1", "claim:cr2"
_POLICY = "pol:fixture"


def _degenerate_cone(
    production_dsn: str, deployment: str, production_generation: str,
) -> ReuseConeResult:
    """Run the claim-plane cone with the prior manifest UNCHANGED — every cone step
    skips behind a DEGENERATE certificate (the unchanged-inputs carry-forward). No
    resolved-state divergence, so no certified differences."""
    steps, seed, facts, incumbent = build_claim_plane_cone(deployment)
    store = ReuseCertificateStore(deployment=deployment)
    conn = psycopg.connect(production_dsn)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=deployment, generation_id=production_generation,
            worklist_seed=seed, steps=steps, current_facts=facts,
            prior_facts=facts, prior_generation_id="prior",
            incumbent_selection=incumbent, cert_store=store,
            checkpoint_writer=lambda _c: production_generation,
        )
    finally:
        conn.close()
    return ReuseConeResult(reuse_pass=summary, certified_differences=None)


def _dominance_cone(
    production_dsn: str, deployment: str, production_generation: str,
) -> ReuseConeResult:
    """Run the CENTREPIECE bounded-dominance skip cone: the crawl claim for the
    decisive bucket K1 is skipped behind a ``resolution_dominance`` certificate (its
    every permitted output loses to the incumbent's declared source authority). The
    certificate's covered bucket licenses the differential run's never-extracted
    loser (§9.7)."""
    cone = build_dominance_skip_cone(
        deployment, predicate=_PREDICATE, normalized_key=_KEY1,
        incumbent_authority=_INCUMBENT_AUTHORITY,
        incumbent_basis=_SOURCE_AUTHORITY_BASIS,
        competitor_source_classes=["crawl"], competitor_authority_max=0,
    )
    store = ReuseCertificateStore(deployment=deployment)
    conn = psycopg.connect(production_dsn)
    try:
        summary = run_reuse_pass_in_cycle(
            conn, deployment=deployment, generation_id=production_generation,
            worklist_seed=cone.worklist_seed, steps=cone.steps,
            current_facts=cone.current_facts, changed_terms=cone.changed_terms,
            incumbent_selection=cone.incumbent_selection,
            dominance_inputs=cone.dominance_inputs, cert_store=store,
            checkpoint_writer=lambda _c: production_generation,
        )
    finally:
        conn.close()
    result = summary["result"]
    dominance_skips = [o for o in result.skips if o.cause == CAUSE_SKIP_DOMINANCE]
    if not dominance_skips:
        raise AssertionError(
            "the dominance cone did not record a bounded-dominance skip; "
            f"outcomes={[(o.step_id, o.action, o.cause) for o in result.outcomes]}"
        )
    certified = CertifiedDifferences(
        covered_buckets=frozenset({cone.covered_bucket_row_key}),
        certificate_digests=tuple(o.certificate_digest for o in dominance_skips),
    )
    return ReuseConeResult(
        reuse_pass=summary, certified_differences=certified,
        skipped_producer_step=CLAIM_PRODUCER_STEP,
    )


def _series_updates() -> list[ReuseGovernedUpdate]:
    """The recorded scheduler-governed series — the four shapes."""
    return [
        # 0) fresh conflict: registry (auth 10) beats wiki (auth 1) — FULL path.
        ReuseGovernedUpdate(
            label="fresh-conflict",
            production_facts=[
                _claim(_E, "vE", key=_KEY1, source="registry:rec:1",
                       source_class="registry", source_authority=10),
                _claim(_W, "vW", key=_KEY1, source="page:w", source_class="wiki",
                       source_authority=1),
                _policy(_POLICY),
            ],
        ),
        # 1) PURE bucket-scoped incremental: a higher-authority registry claim.
        ReuseGovernedUpdate(
            label="incremental-higher-authority",
            production_facts=[
                _claim(_G, "vG", key=_KEY1, source="registry:rec:2",
                       source_class="registry", source_authority=_INCUMBENT_AUTHORITY),
            ],
        ),
        # 2) unchanged no-claim-delta: a policy re-affirmation -> DEGENERATE skips.
        ReuseGovernedUpdate(
            label="unchanged-policy-reaffirm",
            production_facts=[_policy(_POLICY, rev=2, review_note="reaffirmed")],
            run_cone=_degenerate_cone,
        ),
        # 3) THE GENUINE CHANGED-INPUTS SKIP: a crawl batch. The crawl claim for the
        #    decisive bucket K1 is dominance-skipped (withheld from production); the
        #    crawl claim for the fresh bucket K2 is extracted normally (the carrier
        #    delta that produces the generation) — K2 has no incumbent to dominate.
        ReuseGovernedUpdate(
            label="genuine-changed-inputs-skip",
            production_facts=[
                _claim(_CR2, "vCR2", key=_KEY2, source="crawl:page:2",
                       source_class="crawl", source_authority=0),
            ],
            snapshot_extra_facts=[
                _claim(_CR1, "vCR1", key=_KEY1, source="crawl:page:1",
                       source_class="crawl", source_authority=0),
            ],
            run_cone=_dominance_cone,
        ),
    ]


def _proj_mode(record: Any) -> dict[str, Any]:
    return record.work_summary["candidate"]["derivation_modes"]["claim_projection"]


# =========================================================================== #
# The req.reuse.live-equivalence witness — the four-update series end to end.   #
# =========================================================================== #
@pytest.mark.postgres
def test_reuse_governed_series_equals_complete_reference(
    clean_runtime: str, scratch_db: Callable[[], str],
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Over the recorded scheduler-governed series, EVERY non-excluded verdict is
    EQUAL (§9.1): the reuse-enabled differential run returns EXACTLY the resolved
    state the complete reference returns, INCLUDING the genuine changed-inputs skip
    (EQUAL-with-certified-differences). At least one certificate-backed skip's step
    the dependency baseline ran (a real deviation from dependency execution)."""
    # The series publishes through build_deployment_publisher with the
    # deployment's own catalog identity, so claim and load the catalog the way
    # an operator does (`okg catalog ownership claim`, then `okg catalog load
    # --apply`), with the deployment's DSN pointed at, and checked against, the
    # test database.
    load_and_claim_deployment_catalog(_DEP_DIR, clean_runtime, monkeypatch, capsys)

    result = run_reuse_governed_series(
        _series_updates(),
        deployment_dir=_DEP_DIR,
        production_dsn=clean_runtime,
        scratch_dsn_factory=scratch_db,
        persist=True,
    )

    assert len(result.outcomes) == 4, "the series must run the four shapes"
    dist = result.verdict_distribution()
    gens = result.production_generations()
    print("\n[live-proof] verdict distribution (all comparisons):", dist)
    for o in result.outcomes:
        cert_digests = (
            [d[:12] for d in o.certified_differences.certificate_digests]
            if o.certified_differences else []
        )
        skips = (
            [f"{s.step_id}:{s.cause}" for s in o.reuse_pass["result"].skips]
            if o.reuse_pass else []
        )
        print(
            f"[live-proof] update[{o.update_index}] {o.label!r} "
            f"prod_gen={o.production_generation} ref_gen={o.reference_ref} "
            f"base_gen={o.baseline_ref} diff={o.differential_verdict.verdict}"
            f"{'(+certified)' if o.is_certified_equal else ''} "
            f"base={o.baseline_verdict.verdict} | "
            f"effective={o.effective_count} resolution={o.resolution_count} "
            f"selections={o.selection_count} findings={o.finding_count} "
            f"skips={skips} cert_digests={cert_digests} "
            f"baseline_ran_skipped={o.baseline_ran_skipped_step}"
        )

    # ---- every non-excluded verdict is EQUAL (the same-result contract) ------
    assert dist[ "mismatch" ] == 0, "a mismatch in the real series is a defect"
    assert dist["excluded"] == 0
    assert dist["equal"] == 8       # 4 updates x 2 comparisons, all EQUAL
    for o in result.outcomes:
        assert o.differential_verdict.verdict == VERDICT_EQUAL, (
            f"update {o.update_index} differential != reference: "
            f"{o.differential_verdict.to_json_dict()}"
        )
        assert o.baseline_verdict.verdict == VERDICT_EQUAL, (
            f"update {o.update_index} baseline != reference"
        )

    o0, o1, o2, o3 = result.outcomes

    # ---- update 0: fresh conflict -> FULL first publish, plain EQUAL ----------
    assert _proj_mode(o0.records[0])["full_reason"] == "first_publish"
    assert not o0.is_certified_equal

    # ---- update 1: PURE bucket-scoped incremental, selection family carries ---
    proj1 = _proj_mode(o1.records[0])
    assert proj1["mode"] == "incremental", proj1
    assert proj1["full_reason"] is None, proj1
    assert not o1.is_certified_equal          # plain EQUAL, no certified facet
    # the incumbent flipped to the higher-authority registry claim G, selection
    # carries the winner (not a full-recompute shaping).
    rk1 = bucket_row_key(_PREDICATE, _KEY1)
    sel1 = o1.differential_snapshot.selections[rk1]
    assert sel1["outcome"] == "selected" and sel1["winner_claim_id"] == _G

    # ---- update 2: unchanged no-claim-delta -> DEGENERATE certificate skips ---
    assert o2.reuse_pass is not None
    skip_causes_2 = {s.cause for s in o2.reuse_pass["result"].skips}
    assert skip_causes_2 == {CAUSE_CERTIFICATE_DEGENERATE_UNCHANGED}, skip_causes_2
    assert not o2.reuse_pass["result"].runs
    assert not o2.is_certified_equal          # plain EQUAL

    # ---- update 3: THE GENUINE CHANGED-INPUTS SKIP ---------------------------
    # (a) a certificate is present with the DOMINANCE comparison;
    dominance_skips = [
        s for s in o3.reuse_pass["result"].skips if s.cause == CAUSE_SKIP_DOMINANCE
    ]
    assert dominance_skips, "expected a bounded-dominance skip"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recorded = ReuseCertificateStore(
            deployment=_DEPLOYMENT
        ).for_generation(conn, o3.production_generation)
    dom_certs = [
        c for c in recorded
        if c.comparison.get("competitor_bound", {}).get("kind") == "bounded_dominance"
    ]
    assert dom_certs, "no resolution_dominance certificate recorded for the skip"
    cert = dom_certs[0]
    assert cert.comparison["kind"] == COMPARISON_KIND_RESOLUTION
    assert cert.comparison["incumbent_basis"] == _SOURCE_AUTHORITY_BASIS
    # incumbent-worst vs competitor-best is recorded per overlapped bucket.
    per_bucket = cert.comparison["competitor_bound"]["per_bucket"]
    assert per_bucket[0]["incumbent_worst"]["authority"] == _INCUMBENT_AUTHORITY
    assert cert.comparison["competitor_bound"]["competitor_best"]["authority_max"] == 0
    # (b) the step was NOT run in the differential execution: its loser is absent
    #     from the differential's effective set but present in the reference's;
    # (c) the step WAS run by the dependency baseline (its output present there).
    assert o3.baseline_ran_skipped_step, (
        "the dependency baseline must have run the skipped step (its loser present "
        "there) while the differential did not"
    )
    diff_eff = set(o3.differential_snapshot.effective_claims)
    ref_eff = set(o3.reference_snapshot.effective_claims)
    base_eff = set(o3.baseline_snapshot.effective_claims)
    assert diff_eff < ref_eff and base_eff == ref_eff
    never_extracted = ref_eff - diff_eff
    assert len(never_extracted) == 1, never_extracted
    # the verdict is EQUAL-with-certified-differences (a distinct facet, never a
    # silent plain-equal), and the licensed divergence names the never-extracted
    # loser + the covering certificate.
    assert o3.differential_verdict.verdict == VERDICT_EQUAL
    assert o3.is_certified_equal
    facet_objects = {
        d["object"] for d in o3.differential_verdict.certified_differences
        if d.get("kind") == "never_extracted_claim"
    }
    assert facet_objects == never_extracted
    facet_digests = {
        d["digest"] for d in o3.differential_verdict.certified_differences
        if d.get("kind") == "covering_certificate"
    }
    assert cert.digest in facet_digests
    # the licensed divergence spans effective + the K1 loser membership + the
    # derived selection digest — the resolved DECISION (winner G) is unchanged.
    assert o3.differential_snapshot.resolution_outcomes[rk1]["winner_claim_id"] == _G
    assert o3.reference_snapshot.resolution_outcomes[rk1]["winner_claim_id"] == _G

    # ---- generations are distinct + published (generation-describe style) ----
    assert len(set(gens)) == 4, gens
    chronos = [o.production_chronos_generation for o in result.outcomes]
    assert len(set(chronos)) == 4, chronos
    print(f"[live-proof] published compat generations={gens} "
          f"(all reached 'published'; run raises otherwise)")

    # ---- stored + queryable per generation, with the certified facet ---------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stored = load_series_verdicts(conn, _DEPLOYMENT)
    assert len(stored) == 8, [r.production_generation for r in stored]
    assert not [r for r in stored if r.release_blocking]   # zero mismatches
    certified_rows = [r for r in stored if r.is_certified_equal]
    assert len(certified_rows) == 1, "exactly the genuine-skip differential row"
    row = certified_rows[0]
    assert row.comparison == COMPARISON_DIFFERENTIAL
    assert row.production_generation == o3.production_generation
    assert any(
        d.get("kind") == "covering_certificate" for d in row.certified_differences
    )
    # the baseline row for the same update is a PLAIN equal (baseline ran the step).
    base_row = next(
        r for r in stored
        if r.production_generation == o3.production_generation
        and r.comparison == COMPARISON_BASELINE
    )
    assert base_row.verdict == "equal" and not base_row.is_certified_equal
    print(f"[live-proof] stored {len(stored)} verdicts; certified-equal rows="
          f"{len(certified_rows)} (the genuine skip); release_blocking rows=0")

    # ---- charged-work ledger populated + savings report renders from rows -----
    #      (PACT consistency-maintenance-accounting, task.accounting.ledger +
    #       task.accounting.report — the real-fixture-series graph_report witness).
    from okg.substrate.telemetry.work_ledger import (
        WORK_CATEGORIES,
        load_work_ledger,
        render_savings_report,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ledger = load_work_ledger(conn, _DEPLOYMENT)
        report = render_savings_report(conn, _DEPLOYMENT)

    # every update wrote all three execution modes x six adjudicated categories,
    # anchored to that update's (distinct) production generation.
    assert {r.mode for r in ledger} == {
        "differential", "complete_reference", "dependency_baseline",
    }
    assert len(ledger) == len(result.outcomes) * 3 * len(WORK_CATEGORIES)
    # validation is structural: TRUE iff a from-scratch counterfactual mode.
    assert all(r.validation == (r.mode != "differential") for r in ledger)
    # the model_calls category joined okg.llm_calls (0 calls on this fixture — a
    # measured zero, never a gap).
    assert all(
        r.measured for r in ledger if r.category == "model_calls"
    )
    # the production charged figure counts differential (validation=FALSE) only.
    assert report.production_total_units == report.charges["differential"].total_units
    printed = report.render()
    assert _DEPLOYMENT in printed
    assert "complete_reference" in printed and "dependency_baseline" in printed
    print("\n[live-proof] SAVINGS REPORT (rendered from work_ledger rows alone):")
    print(printed)


# =========================================================================== #
# The differ-extension pair — certified <-> EQUAL-with-facet; uncovered <->     #
# MISMATCH — over a REAL published resolved state (§9.7 both ways).             #
# =========================================================================== #
@pytest.mark.postgres
def test_certified_differences_both_ways_over_a_real_state(
    clean_runtime: str, scratch_db: Callable[[], str],
) -> None:
    """Extract a REAL fixture resolved state where a decisive incumbent wins K1,
    then synthesise the never-extracted-loser divergence into a COPY (the exact
    shape the differential run produces): WITH a covering certificate the differ
    renders EQUAL-with-certified-differences; WITHOUT it (or with the certificate
    not covering the bucket) the SAME divergence is a MISMATCH."""
    _install_production_catalog(clean_runtime)

    # A real published state: K1 decisive (registry beats wiki), plus the loser CR1.
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.publisher import Publisher
    from okg.substrate.publisher.deployment_stack import (
        build_deployment_publisher_stack,
    )

    facts = [
        _claim(_E, "vE", key=_KEY1, source="registry:rec:1",
               source_class="registry", source_authority=10),
        _claim(_W, "vW", key=_KEY1, source="page:w", source_class="wiki",
               source_authority=1),
        _claim(_CR1, "vCR1", key=_KEY1, source="crawl:page:1",
               source_class="crawl", source_authority=0),
        _policy(_POLICY),
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)
    stack = build_deployment_publisher_stack(
        dsn=clean_runtime, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(clean_runtime, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        reference = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=out["compat_generation_id"],
            chronos_generation_id=out["generation_id"],
        )
    rk1 = bucket_row_key(_PREDICATE, _KEY1)
    assert _CR1 in reference.effective_claims, "the crawl loser must be present"
    assert reference.resolution_outcomes[rk1]["winner_claim_id"] == _E

    # The differential run never extracted CR1: remove it from the effective set
    # AND from K1's loser membership (the projection derives the loser lists from
    # the effective set) — the exact §9.7 raw-extraction shape.
    state = reference.state_json_dict()
    del state[FAMILY_EFFECTIVE][_CR1]
    res = state[FAMILY_RESOLUTION][rk1]
    res["ranked_claim_ids"] = [c for c in res["ranked_claim_ids"] if c != _CR1]
    res["superseded_claim_ids"] = [
        c for c in res["superseded_claim_ids"] if c != _CR1
    ]
    sel = state[FAMILY_SELECTION][rk1]
    sel["distrusted_claim_ids"] = [
        c for c in sel["distrusted_claim_ids"] if c != _CR1
    ]
    differential = ResolvedStateSnapshot(
        deployment=reference.deployment, generation_id=reference.generation_id,
        chronos_generation_id=reference.chronos_generation_id,
        effective_claims=state[FAMILY_EFFECTIVE],
        resolution_outcomes=state[FAMILY_RESOLUTION],
        selections=state[FAMILY_SELECTION],
        selection_digest="differential-digest-differs",  # the derived scalar moves
        open_findings=state[FAMILY_FINDING],
    )

    covering = CertifiedDifferences(
        covered_buckets=frozenset({rk1}), certificate_digests=("cert:abc123",),
    )

    # WITH a covering certificate -> EQUAL-with-certified-differences.
    certified = diff_resolved_states(
        reference, differential, certified_differences=covering
    )
    print(f"\n[live-proof/differ-pair] covered -> {certified.verdict} "
          f"certified_equal={certified.is_certified_equal}")
    assert certified.verdict == VERDICT_EQUAL and certified.is_certified_equal
    assert any(
        d.get("object") == _CR1 for d in certified.certified_differences
    )

    # WITHOUT the certificate -> the SAME divergence is a MISMATCH.
    plain = diff_resolved_states(reference, differential)
    print(f"[live-proof/differ-pair] uncovered -> {plain.verdict}")
    assert plain.verdict == VERDICT_MISMATCH
    assert FAMILY_EFFECTIVE in plain.artifact.families
    assert _CR1 in plain.artifact.families[FAMILY_EFFECTIVE].removed

    # A certificate that does NOT cover K1 licenses nothing -> MISMATCH.
    wrong = CertifiedDifferences(
        covered_buckets=frozenset({bucket_row_key(_PREDICATE, "other")}),
    )
    wrong_verdict = diff_resolved_states(
        reference, differential, certified_differences=wrong
    )
    print(f"[live-proof/differ-pair] wrong-bucket cert -> {wrong_verdict.verdict}")
    assert wrong_verdict.verdict == VERDICT_MISMATCH

    # A DECISION change (the winner flips) is NEVER licensed, even under coverage.
    decision_change = ResolvedStateSnapshot(
        deployment=reference.deployment, generation_id=reference.generation_id,
        chronos_generation_id=reference.chronos_generation_id,
        effective_claims=state[FAMILY_EFFECTIVE],
        resolution_outcomes={
            rk1: {**reference.resolution_outcomes[rk1], "winner_claim_id": _W}
        },
        selections=state[FAMILY_SELECTION],
        selection_digest="x", open_findings=state[FAMILY_FINDING],
    )
    decision_verdict = diff_resolved_states(
        reference, decision_change, certified_differences=covering
    )
    print(f"[live-proof/differ-pair] winner-flip -> {decision_verdict.verdict}")
    assert decision_verdict.verdict == VERDICT_MISMATCH
