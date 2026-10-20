"""The three-way equivalence harness over a recorded series (the live-run task).

PACT ``consistency-complete-equivalence``, task.harness.live-run
(req.harness.live-series). This is the witness named VERBATIM by
docs/develop/architecture/consistency.md §9.1:
``test_differential_equals_complete_on_recorded_series`` — the same-result
contract, run over a recorded series of real publication updates on the in-tree
``consistency-fixture`` deployment.

The harness (:func:`okg.substrate.qa.equivalence_series.run_equivalence_series`)
joins the three components this child landed: the differential production path
(a real ``Publisher.cycle`` on the production DB — the idiom of
test_claim_plane_wire.py), the complete reference + dependency baseline over the
cumulative pinned snapshot in fresh scratch DBs
(``okg.substrate.qa.complete_reference``), and the single equivalence differ
(``equivalence_differ.diff_resolved_states``). Every verdict is persisted into
``okg.equivalence_verdicts`` and read back with ``load_series_verdicts``.

The recorded series exercises THREE genuinely different shapes on ONE evolving
bucket, so the differential engine takes a different full-recompute path each
time AND no open finding is ever carried forward unchanged (every open finding is
opened / reopened / resolved at its own generation):

* update 0 — new claims incl. a conflict: two tying claims -> CONTESTED bucket;
  the ``contested_bucket`` check opens a finding. (full cause: first_publish)
* update 1 — a decisive-authority arrival that flips resolution (the
  resolution-clears-violation shape): a decisive claim with a RETIRED value plus
  a Σ re-affirmation -> bucket CANONICAL; the contested finding resolves and the
  ``canonical_retired_target`` check opens. (full cause: policy_change)
* update 2 — a retraction: the decisive claim is retracted (op='R') -> the bucket
  is CONTESTED again; the retired finding resolves, the contested finding
  reopens. (full cause: deletion_shape — the recorded-cause full path)

Every non-excluded verdict must be EQUAL: a mismatch in the real series is a
release-blocking defect, not a statistic (§9.1). Seeding + publish wiring follow
tests/substrate/consistency/test_claim_plane_wire.py and
tests/substrate/consistency/test_complete_reference.py.
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
    VERDICT_EXCLUDED,
    VERDICT_MISMATCH,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.qa.complete_reference import OutcomeProbe  # noqa: E402
from okg.substrate.qa.equivalence_series import (  # noqa: E402
    COMPARISON_BASELINE,
    COMPARISON_DIFFERENTIAL,
    EQUIVALENCE_VERDICTS_TABLE,
    SeriesUpdate,
    VerdictRecord,
    load_series_verdicts,
    run_equivalence_series,
    store_equivalence_verdicts,
)

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_KEY = "pref|k1"                       # one evolving bucket for the whole series
_C, _D, _E = "claim:c", "claim:d", "claim:e"
_POLICY = "pol:fixture"
_RETIRED_VALUE = "retired:old-cluster"  # E's value — trips canonical_retired_target

_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)

_NORM_PRED_CONTESTED = "claim_bucket_contested"
_NORM_PRED_RETIRED = "canonical_target_retired"


# --------------------------------------------------------------------------- #
# Scratch-DB factory (copied from test_complete_reference.py — the runner       #
# provisions each fresh okg_test_* DB; the fixture drops them at teardown).     #
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
    """Yield a factory that mints a fresh okg_test_* scratch DB per call and
    drops every one at teardown — one for each complete reference / dependency
    baseline run the series performs."""
    base_dsn = okg_migrated
    created: list[str] = []

    def _make() -> str:
        dbname = f"okg_test_equiv_{uuid.uuid4().hex[:10]}"
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
# Production provisioning + fact builders (test_claim_plane_wire idiom).        #
# --------------------------------------------------------------------------- #
def _install_production_catalog(dsn: str) -> None:
    """Provision the production DB's ontology from the ACTUAL consistency-fixture
    deployment (the same compose the scratch runner applies), so production and
    every scratch share one catalog."""
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
    )

    composed = compose_catalog(_DEP_DIR)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=_DEP_DIR,
        command="test equivalence production catalog apply",
        writer_scope="deployment_root",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(
            conn,
            identity=identity,
            force=True,
            allow_scope_change=True,
        )
        apply_composed_catalog(conn, composed, identity=identity)


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
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1},
                    op=op)


def _functional_policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": _CONSTRAINT,
             "policy_kind": "functional", "predicate": _PREDICATE}
    attrs.update({k: str(v) for k, v in extra.items()})
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


def _retract_claim(node_id: str, *, rev: int = 2) -> NodeFact:
    return NodeFact(node_id, "consistency_claim", {}, {"id": node_id}, {"v": rev},
                    op="R")


def _series_updates() -> list[SeriesUpdate]:
    """The recorded series — three genuinely different shapes on bucket K1."""
    return [
        SeriesUpdate(
            label="new-claims-conflict",
            facts=[
                _claim_fact(_C, "vC", key=_KEY, source="page:c", source_class="wiki"),
                _claim_fact(_D, "vD", key=_KEY, source="page:d", source_class="wiki"),
                _functional_policy(_POLICY),
            ],
        ),
        SeriesUpdate(
            label="decisive-authority-flip",
            facts=[
                _claim_fact(_E, _RETIRED_VALUE, key=_KEY, source="registry:rec:1",
                            source_class="registry", source_authority=10),
                # A Σ re-affirmation forces a FULL projection recompute so r* is
                # materialized on the differential path (resolution-clears shape).
                _functional_policy(_POLICY, rev=2, review_note="registry onboarded"),
            ],
        ),
        SeriesUpdate(
            label="retraction-full-path",
            facts=[_retract_claim(_E)],  # op='R' -> deletion_shape full recompute
        ),
    ]


class _RaiseOnCompleteLLM:
    """A model client that raises on ``complete`` — proves replay never samples
    fresh, so a probe MISS is a NAMED exclusion, never a silent sample."""

    model = "fixture-model:v1"

    def complete(self, system: str, user: str, **_kw: Any) -> str:
        raise AssertionError(
            "replay must never sample fresh — llm.complete was called"
        )


def _missing_key_probe() -> OutcomeProbe:
    """A probe whose recorded outcome is absent (the fixture records none), so
    the reference/baseline replay yields a NAMED ReplayExclusion."""
    return OutcomeProbe(
        step_id="enricher:fixture_probe",
        operator_version="v1",
        model_id="fixture-model:v1",
        system_prompt="SYSTEM: extract the preferred target",
        user_prompt="USER: page body",
        typed_input={"fragment": "frag-1", "body": "the target is X"},
        llm=_RaiseOnCompleteLLM(),
    )


def _proj_full_reason(record: VerdictRecord) -> Any:
    """The claim_projection full-recompute cause recorded on a differential
    record's candidate work summary (the honest per-step cause)."""
    return (
        record.work_summary["candidate"]["derivation_modes"]
        ["claim_projection"]["full_reason"]
    )


def _finding_predicates(snapshot: ResolvedStateSnapshot) -> set[str]:
    return {
        str(f.get("normalized_predicate"))
        for f in snapshot.open_findings.values()
    }


def _bucket_rk() -> str:
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        bucket_row_key,
    )

    return bucket_row_key(_PREDICATE, _KEY)


# --------------------------------------------------------------------------- #
# The named §9.1 witness.                                                       #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_differential_equals_complete_on_recorded_series(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """Over a recorded series of >=3 real updates on the fixture deployment,
    EVERY non-excluded verdict is EQUAL (the same-result contract, §9.1): the
    differential production path returns EXACTLY the resolved state the complete
    reference returns, and the dependency baseline does too. Verdicts are stored
    and queryable per generation with correct release_blocking flags."""
    _install_production_catalog(clean_runtime)

    result = run_equivalence_series(
        _series_updates(),
        deployment_dir=_DEP_DIR,
        production_dsn=clean_runtime,
        scratch_dsn_factory=scratch_db,
        persist=True,
    )

    # ---- at least THREE updates, each a genuinely different shape ------------
    assert len(result.outcomes) == 3, "the series must run >=3 real updates"

    dist = result.verdict_distribution()
    gens = result.production_generations()
    print("\n[equiv-harness] verdict distribution (all comparisons):", dist)
    print(f"[equiv-harness] production generations: {gens} "
          f"reference refs: {[o.reference_ref for o in result.outcomes]}")
    for o in result.outcomes:
        print(
            f"[equiv-harness] update[{o.update_index}] {o.label!r} "
            f"prod_gen={o.production_generation} ref_gen={o.reference_ref} "
            f"diff={o.differential_verdict.verdict} "
            f"base={o.baseline_verdict.verdict} | "
            f"effective={o.effective_count} resolution={o.resolution_count} "
            f"selections={o.selection_count} findings={o.finding_count} "
            f"proj_cause={_proj_full_reason(o.records[0])} "
            f"finding_predicates={sorted(_finding_predicates(o.differential_snapshot))}"
        )

    # ---- every non-excluded verdict is EQUAL (the contract) -----------------
    assert dist == {"equal": 6, "mismatch": 0, "excluded": 0}, dist
    for o in result.outcomes:
        assert o.differential_verdict.verdict == VERDICT_EQUAL, (
            f"update {o.update_index} differential != reference: "
            f"{o.differential_verdict.to_json_dict()}"
        )
        assert o.baseline_verdict.verdict == VERDICT_EQUAL, (
            f"update {o.update_index} baseline != reference: "
            f"{o.baseline_verdict.to_json_dict()}"
        )

    # ---- genuinely different shapes: distinct full-recompute causes ----------
    causes = [_proj_full_reason(o.records[0]) for o in result.outcomes]
    assert causes == ["first_publish", "policy_change", "deletion_shape"], causes

    # ---- genuinely different shapes: the resolved state + findings evolve ----
    o0, o1, o2 = result.outcomes
    # update 0: contested bucket -> a contested finding, unresolved selection.
    assert o0.effective_count == 2 and o0.resolution_count == 1
    assert o0.selection_count == 1 and o0.finding_count == 1
    assert _finding_predicates(o0.differential_snapshot) == {_NORM_PRED_CONTESTED}
    assert o0.differential_snapshot.selections[_bucket_rk()]["outcome"] == "unresolved"
    # update 1: decisive flip -> canonical bucket, retired finding, selected r*.
    assert o1.effective_count == 3 and o1.resolution_count == 1
    assert o1.finding_count == 1
    assert _finding_predicates(o1.differential_snapshot) == {_NORM_PRED_RETIRED}
    sel1 = o1.differential_snapshot.selections[_bucket_rk()]
    assert sel1["outcome"] == "selected"
    assert sel1["winner_claim_id"] == _E
    assert sorted(sel1["distrusted_claim_ids"]) == sorted([_C, _D])
    # update 2: retraction -> contested again, contested finding reopened.
    assert o2.effective_count == 2 and o2.resolution_count == 1
    assert o2.finding_count == 1
    assert _finding_predicates(o2.differential_snapshot) == {_NORM_PRED_CONTESTED}
    assert o2.differential_snapshot.selections[_bucket_rk()]["outcome"] == "unresolved"

    # ---- the three production generations are distinct + published -----------
    assert len(set(gens)) == 3, gens
    # okg generation describe confirmation (in-test): the publish reached
    # 'published' for each update (run_equivalence_series raises otherwise) and
    # the chronos checkpoints are distinct.
    chronos = [o.production_chronos_generation for o in result.outcomes]
    assert len(set(chronos)) == 3, chronos

    # ---- stored + queryable per generation, correct release_blocking ---------
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stored = load_series_verdicts(conn, _DEPLOYMENT)
    assert len(stored) == 6, [r.production_generation for r in stored]
    by_key = {(r.production_generation, r.comparison): r for r in stored}
    for o in result.outcomes:
        for comparison in (COMPARISON_DIFFERENTIAL, COMPARISON_BASELINE):
            rec = by_key[(o.production_generation, comparison)]
            assert rec.verdict == "equal", (o.production_generation, comparison)
            assert rec.release_blocking is False   # equal is never blocking
            assert rec.artifact is None            # equal carries no artifact
            assert rec.reference_ref == o.reference_ref
            assert rec.work_summary is not None     # charged-work ledger present
    # zero release-blocking rows in a clean series (zero unexplained mismatches).
    assert not [r for r in stored if r.release_blocking]
    print(f"[equiv-harness] stored {len(stored)} verdicts; "
          f"release_blocking rows=0 (zero unexplained mismatches)")


# --------------------------------------------------------------------------- #
# A mismatch is stored release-blocking and names the object.                   #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_mismatch_is_stored_release_blocking(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """Seed a surgical divergence into a COPY of a real resolved state (never
    corrupt the production run), diff it, and assert the STORED verdict row
    carries release_blocking=true and its artifact NAMES the divergent object.
    Also witnesses that release_blocking is STRUCTURAL — the DB CHECK forbids a
    mismatch row that is not blocking."""
    _install_production_catalog(clean_runtime)

    # One real update; do NOT persist its (equal) verdicts — we store only the
    # seeded mismatch below.
    result = run_equivalence_series(
        _series_updates()[:1],
        deployment_dir=_DEP_DIR,
        production_dsn=clean_runtime,
        scratch_dsn_factory=scratch_db,
        persist=False,
    )
    outcome = result.outcomes[0]
    assert outcome.differential_verdict.verdict == VERDICT_EQUAL  # real run agrees

    reference = outcome.reference_snapshot
    assert _C in reference.effective_claims, "expected the seeded claim present"

    # Surgical divergence into a COPY (state_json_dict deep-copies each row).
    state = reference.state_json_dict()
    state[FAMILY_EFFECTIVE][_C]["value"] = "SURGICALLY-CORRUPTED"
    mutated = ResolvedStateSnapshot(
        deployment=reference.deployment,
        generation_id=reference.generation_id,
        chronos_generation_id=reference.chronos_generation_id,
        effective_claims=state[FAMILY_EFFECTIVE],
        resolution_outcomes=state[FAMILY_RESOLUTION],
        selections=state[FAMILY_SELECTION],
        selection_digest=state["selection_digest"],
        open_findings=state[FAMILY_FINDING],
    )
    # The production DB itself is untouched — the reference is still equal to it.
    assert reference.effective_claims[_C]["value"] == "vC"

    verdict = diff_resolved_states(reference, mutated)
    assert verdict.verdict == VERDICT_MISMATCH
    assert _C in verdict.artifact.families[FAMILY_EFFECTIVE].changed
    assert verdict.artifact.families[FAMILY_EFFECTIVE].changed[_C]["value"] == {
        "reference": "vC", "candidate": "SURGICALLY-CORRUPTED"
    }

    record = VerdictRecord(
        deployment=result.deployment,
        update_index=outcome.update_index,
        production_generation=outcome.production_generation,
        reference_ref=outcome.reference_ref,
        comparison=COMPARISON_DIFFERENTIAL,
        verdict="mismatch",
        release_blocking=True,
        artifact=verdict.artifact.to_json_dict(),
        work_summary=None,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store_equivalence_verdicts(conn, [record])
        stored = load_series_verdicts(conn, result.deployment)

    assert len(stored) == 1
    row = stored[0]
    assert row.verdict == "mismatch"
    assert row.release_blocking is True             # a mismatch blocks release
    assert _C in row.artifact["families"][FAMILY_EFFECTIVE]["changed"]  # named
    assert "SURGICALLY-CORRUPTED" in str(row.artifact)
    print(
        f"\n[equiv-harness/mismatch] stored release-blocking verdict "
        f"gen={row.production_generation} names={_C!r} "
        f"artifact_field=value"
    )

    # release_blocking is STRUCTURAL, both in the record constructor and the DB:
    with pytest.raises(Exception, match="release_blocking"):
        VerdictRecord(
            deployment=result.deployment, update_index=0,
            production_generation="999", reference_ref="1",
            comparison=COMPARISON_DIFFERENTIAL, verdict="mismatch",
            release_blocking=False, artifact={}, work_summary=None,
        )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.CheckViolation):
            cur.execute(
                f"INSERT INTO {EQUIVALENCE_VERDICTS_TABLE} "
                "(deployment_name, update_index, production_generation, "
                " reference_ref, "
                " comparison, verdict, release_blocking) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (result.deployment, 0, "999", "1",
                 COMPARISON_DIFFERENTIAL, "mismatch", False),
            )
    print("[equiv-harness/mismatch] DB CHECK rejects a non-blocking mismatch row")


# --------------------------------------------------------------------------- #
# An excluded update is stored separately and never counted as agreement.       #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_excluded_update_stored_separately_never_equal(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """An update whose replay cannot resolve a key stores verdict=excluded with
    the missing keys NAMED, and the series summary reports it in its OWN bucket —
    never in the equal count (§9.7)."""
    _install_production_catalog(clean_runtime)

    base = _series_updates()
    probe = _missing_key_probe()
    updates = [
        base[0],  # equal (contested bucket)
        SeriesUpdate(
            label="decisive-authority-flip-EXCLUDED",
            facts=base[1].facts,
            outcome_probe=probe,   # replay miss -> EXCLUDED
        ),
    ]

    result = run_equivalence_series(
        updates,
        deployment_dir=_DEP_DIR,
        production_dsn=clean_runtime,
        scratch_dsn_factory=scratch_db,
        persist=True,
    )

    dist = result.verdict_distribution()
    print(f"\n[equiv-harness/excluded] verdict distribution: {dist}")
    # update 0 -> 2 equal; update 1 -> 2 excluded; excluded is its OWN bucket.
    assert dist == {"equal": 2, "mismatch": 0, "excluded": 2}, dist

    equal_update, excluded_update = result.outcomes
    assert equal_update.differential_verdict.verdict == VERDICT_EQUAL
    assert excluded_update.differential_verdict.verdict == VERDICT_EXCLUDED
    assert excluded_update.baseline_verdict.verdict == VERDICT_EXCLUDED

    missing = probe.key().digest
    named = {k["key_digest"] for k in excluded_update.differential_verdict.excluded_keys}
    assert missing in named, "the excluded verdict must name the missing key"
    # never counted as agreement — structural on the verdict itself.
    assert excluded_update.differential_verdict.counted_as_agreement is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stored = load_series_verdicts(conn, result.deployment)
    excluded_rows = [r for r in stored if r.verdict == "excluded"]
    equal_rows = [r for r in stored if r.verdict == "equal"]
    assert len(excluded_rows) == 2 and len(equal_rows) == 2, stored
    for row in excluded_rows:
        assert row.release_blocking is False            # excluded never blocks
        assert row.production_generation == excluded_update.production_generation
        keys = {k["key_digest"] for k in row.artifact["excluded_keys"]}
        assert missing in keys                          # missing key NAMED
    for row in equal_rows:
        assert row.production_generation == equal_update.production_generation
    print(
        f"[equiv-harness/excluded] excluded gen={excluded_update.production_generation} "
        f"names missing key={missing[:16]}...; equal gen="
        f"{equal_update.production_generation} (separate bucket, never agreement)"
    )
