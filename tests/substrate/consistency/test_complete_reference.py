"""Complete-execution reference + dependency-baseline witnesses.

PACT ``consistency-complete-equivalence`` (task.harness.complete-runner,
req.harness.complete-reference; task.harness.dep-baseline,
req.harness.dependency-baseline). Spec obligations named verbatim in
docs/develop/architecture/consistency.md §9.7:
``test_cold_start_determinism`` and ``test_excluded_update_named_never_counted``.

Every run executes the entire declared pipeline for the in-tree
``consistency-fixture`` deployment into a FRESH ``okg_test_*`` scratch database
(the runner provisions schema + catalog, applies the pinned facts, copies the
pinned recorded outcomes, and runs the real publish cycle under replay). The
runner reads no prior maintenance state — two cold starts from the same pin are
byte-equal — and never silently falls back: an incomplete pin, a mis-named or
dirty scratch DB, and an injected operator failure each fail loud and typed.

The four-family state dump used to prove equality is PRIVATE to
``okg.substrate.qa.complete_reference`` (the repo's single equivalence *differ*
is task.harness.differ's ``consistency/equivalence_differ.py``; the live-series
task consolidates). These witnesses compare two runs of that private dump.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Iterator, Callable
from urllib.parse import urlsplit, urlunsplit

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    FULL_RECOMPUTE_CAUSES,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.llm.outcomes import OutcomeKey  # noqa: E402
from okg.substrate.qa.complete_reference import (  # noqa: E402
    IncompleteSnapshotPin,
    OutcomeProbe,
    OutcomeReplaySource,
    ReferenceExecutionAborted,
    ScratchGuardViolation,
    assert_every_check_full_sweep,
    assert_every_derivation_reran_full,
    run_complete_reference,
    run_dependency_baseline,
)

_DEPLOYMENT = "consistency-fixture"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"

_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)


# --------------------------------------------------------------------------- #
# Pinned fact payload — FIXED ids so two cold starts see byte-identical input.#
# --------------------------------------------------------------------------- #
def _claim_fact(
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


def _functional_policy(node_id: str) -> NodeFact:
    return NodeFact(
        node_id, "predicate_policy",
        {"enabled": "true", "policy_id": _CONSTRAINT,
         "policy_kind": "functional", "predicate": _PREDICATE},
        {"id": node_id}, {"v": 1},
    )


def _seed_facts() -> list[NodeFact]:
    """Decisive bucket key1 (A authority 10 beats B authority 1 -> canonical A,
    B distrusted) + contested bucket key2 (C, D tie -> unresolved). Identical to
    test_claim_plane_wire's shape, but with FIXED ids (no random suffix) so the
    pin is byte-stable across runs."""
    return [
        _claim_fact("claim:a", "vX", key="key|one", source="page:1",
                    source_class="release_notes", source_authority=10),
        _claim_fact("claim:b", "vY", key="key|one", source="page:2",
                    source_class="wiki", source_authority=1),
        _claim_fact("claim:c", "vP", key="key|two", source="page:3",
                    source_class="wiki"),
        _claim_fact("claim:d", "vQ", key="key|two", source="page:4",
                    source_class="wiki"),
        _functional_policy("pol:fixture"),
    ]


# --------------------------------------------------------------------------- #
# Scratch-DB factory: create/drop uniquely-named okg_test_* databases safely. #
# --------------------------------------------------------------------------- #
def _swap_dbname(base_dsn: str, dbname: str) -> str:
    parts = urlsplit(base_dsn)
    return urlunsplit(parts._replace(path="/" + dbname))


def _create_scratch_db(base_dsn: str, dbname: str) -> str:
    if not dbname.startswith("okg_test_"):  # match the ephemeral-DB guard
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
    """Yield a factory that mints a fresh, uniquely-named okg_test_* scratch
    database (with the substrate extensions installed) and drops every one at
    teardown. Each call returns a fresh DSN the runner provisions into."""
    base_dsn = okg_migrated
    created: list[str] = []

    def _make() -> str:
        dbname = f"okg_test_cref_{uuid.uuid4().hex[:10]}"
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


def _empty_source() -> OutcomeReplaySource:
    """The fixture has no model-backed step: an explicit empty replay pin."""
    return OutcomeReplaySource(deployment=_DEPLOYMENT, rows=())


# --------------------------------------------------------------------------- #
# Named spec witnesses (consistency-model.md §9.7).                           #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_cold_start_determinism(scratch_db: Callable[[], str]) -> None:
    """Two references from the same pinned snapshot + replay are BYTE-EQUAL.

    Proves the reference provably reads no prior maintenance state: two cold
    starts into two independent fresh scaffolds produce identical resolved
    state across all four families."""
    facts = _seed_facts()
    src = _empty_source()

    r1 = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=src,
    )
    r2 = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=src,
    )

    # Both produced a published reference over the two contested/decisive buckets.
    assert r1.state_dump["effective_claims"], "no effective claims materialized"
    assert r1.state_dump["resolution_outcomes"], "no bucket resolutions"
    assert r1.state_dump["repair_selections"], "no repair selections recorded"
    print(
        f"\n[complete-reference] r1 gen={r1.generation_id} "
        f"effective={len(r1.state_dump['effective_claims'])} "
        f"resolutions={len(r1.state_dump['resolution_outcomes'])} "
        f"repairs={len(r1.state_dump['repair_selections'])} "
        f"open_findings={len(r1.state_dump['open_findings'])}"
    )

    # Distinct generations, byte-equal resolved state (the determinism witness).
    assert r1.generation_id != r2.generation_id, "expected two fresh scaffolds"
    assert r1.state_dump == r2.state_dump
    assert r1.state_dump_digest == r2.state_dump_digest
    assert not r1.excluded and not r2.excluded
    print(f"[complete-reference] cold-start digest={r1.state_dump_digest[:16]}")

    # The result is canonical-JSON serializable (the live-series task consumes
    # it); two byte-equal runs serialize to the same state dump + digest.
    import json

    rt1 = json.loads(r1.to_canonical_json())
    rt2 = json.loads(r2.to_canonical_json())
    assert rt1["mode"] == "complete_reference"
    assert rt1["state_dump"] == rt2["state_dump"]
    assert rt1["state_dump_digest"] == rt2["state_dump_digest"] == r1.state_dump_digest


@pytest.mark.postgres
def test_excluded_update_named_never_counted(
    scratch_db: Callable[[], str],
) -> None:
    """A replay-store deletion turns the affected step into a recorded, NAMED
    exclusion surfaced on the result — never a silent fresh sample.

    Witness shape (spec-sanctioned): the fixture has no model-backed step, so we
    exercise the OutcomeRecorder replay contract directly against the scratch
    store via a probe. The probe's LLM client RAISES on ``complete``, so any
    fresh sample would blow up loudly — proving replay never samples fresh, on
    both the hit and the miss."""

    class _RaiseOnCompleteLLM:
        model = "fixture-model:v1"

        def complete(self, system: str, user: str, **_kw: Any) -> str:
            raise AssertionError(
                "replay must never sample fresh — llm.complete was called"
            )

    probe = OutcomeProbe(
        step_id="enricher:fixture_probe",
        operator_version="v1",
        model_id="fixture-model:v1",
        system_prompt="SYSTEM: extract the preferred target",
        user_prompt="USER: page body",
        typed_input={"fragment": "frag-1", "body": "the target is X"},
        llm=_RaiseOnCompleteLLM(),
    )
    key: OutcomeKey = probe.key()
    facts = _seed_facts()

    # Present: the recorded outcome is pinned -> replayed, no exclusion.
    recorded_row = {
        "key_digest": key.digest,
        "step_id": probe.step_id,
        "operator_version": probe.operator_version,
        "model_id": probe.model_id,
        "prompt_hash": key.prompt_hash,
        "input_hash": key.input_hash,
        "content": "RECORDED-OUTCOME",
    }
    hit = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=OutcomeReplaySource(
            deployment=_DEPLOYMENT, rows=[recorded_row]
        ),
        outcome_probe=probe,
    )
    assert hit.exclusions == [], f"unexpected exclusions: {hit.exclusions}"
    assert not hit.excluded
    assert hit.probe_replayed_content == "RECORDED-OUTCOME"
    print(f"\n[exclusion] hit: replayed content={hit.probe_replayed_content!r}")

    # Deleted: the same key is absent from the replay store -> NAMED exclusion,
    # the update is EXCLUDED, and no fresh sample is taken.
    miss = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=_empty_source(),
        outcome_probe=probe,
    )
    assert miss.excluded, "a replay miss must mark the update EXCLUDED"
    assert miss.probe_replayed_content is None
    assert len(miss.exclusions) == 1
    excl = miss.exclusions[0]
    assert excl.step_id == probe.step_id
    assert excl.key_digest == key.digest        # the missing key is NAMED
    assert excl.fragment_id == probe.fragment_id
    print(
        f"[exclusion] miss: named exclusion step={excl.step_id!r} "
        f"key_digest={excl.key_digest[:16]} (never counted as agreement)"
    )


# --------------------------------------------------------------------------- #
# task.harness.complete-runner verification clauses.                          #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_runner_refuses_incomplete_snapshot_or_replay_pin() -> None:
    """The runner refuses to start on an incomplete pin — before touching any
    database (no partial provisioning)."""
    good_scratch = _swap_dbname(
        "postgres://postgres:okg@127.0.0.1:5433/okg_test",
        "okg_test_cref_neverused",
    )
    # No fact payload.
    with pytest.raises(IncompleteSnapshotPin):
        run_complete_reference(
            None, deployment_dir=_DEP_DIR, scratch_dsn=good_scratch,
            outcome_store_source=_empty_source(),
        )
    # A DSN string is not an explicit fact payload.
    with pytest.raises(IncompleteSnapshotPin):
        run_complete_reference(
            "postgres://prod/db", deployment_dir=_DEP_DIR,
            scratch_dsn=good_scratch, outcome_store_source=_empty_source(),
        )
    # Empty fact payload.
    with pytest.raises(IncompleteSnapshotPin):
        run_complete_reference(
            [], deployment_dir=_DEP_DIR, scratch_dsn=good_scratch,
            outcome_store_source=_empty_source(),
        )
    # Missing outcome source (a required pin component).
    with pytest.raises(IncompleteSnapshotPin):
        run_complete_reference(
            _seed_facts(), deployment_dir=_DEP_DIR, scratch_dsn=good_scratch,
            outcome_store_source=None,
        )
    # Unreadable outcome source.
    with pytest.raises(IncompleteSnapshotPin):
        OutcomeReplaySource.from_dsn(
            "postgres://postgres:nope@127.0.0.1:5599/does_not_exist",
            _DEPLOYMENT,
        )


@pytest.mark.postgres
def test_runner_refuses_misnamed_scratch_dsn() -> None:
    """A scratch DSN that does not name an okg_test_* database is refused before
    any provisioning (never mutate a non-throwaway DB)."""
    with pytest.raises(ScratchGuardViolation):
        run_complete_reference(
            _seed_facts(), deployment_dir=_DEP_DIR,
            scratch_dsn="postgres://postgres:okg@127.0.0.1:5433/production_graph",
            outcome_store_source=_empty_source(),
        )


@pytest.mark.postgres
def test_runner_refuses_dirty_scratch_db(
    scratch_db: Callable[[], str],
) -> None:
    """A scratch DB that already holds a published generation is refused: the
    reference must provably read no prior maintenance state."""
    dsn = scratch_db()
    facts = _seed_facts()
    first = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=dsn,
        outcome_store_source=_empty_source(),
    )
    assert first.generation_id  # the first run published into this scaffold
    with pytest.raises(ScratchGuardViolation):
        run_complete_reference(
            facts, deployment_dir=_DEP_DIR, scratch_dsn=dsn,
            outcome_store_source=_empty_source(),
        )


@pytest.mark.postgres
def test_injected_operator_failure_aborts_with_no_reference(
    scratch_db: Callable[[], str],
) -> None:
    """An operator failure (a maintained derivation that raises) aborts loudly
    with NO published generation in the scratch DB — there is no partial
    reference."""
    import dataclasses

    from okg.substrate.derived_artifacts.derivation_registry import (
        DerivationRegistry,
    )

    def _inject_failure(registry: Any) -> Any:
        def _raise(_ctx: Any) -> Any:
            raise RuntimeError("injected operator failure")

        broken = DerivationRegistry()
        for row in registry.rows():
            spec = dataclasses.replace(
                row.spec, full_recount=_raise, incremental=_raise
            )
            broken.register(dataclasses.replace(row, spec=spec))
        return broken

    dsn = scratch_db()
    with pytest.raises(ReferenceExecutionAborted):
        run_complete_reference(
            _seed_facts(), deployment_dir=_DEP_DIR, scratch_dsn=dsn,
            outcome_store_source=_empty_source(),
            derivation_registry_transform=_inject_failure,
        )

    # Assert absence: no published generation beyond the bootstrap seed.
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.graph_generations "
            " WHERE generation_id > 0 AND status = 'published'"
        )
        published = int(cur.fetchone()[0])
    assert published == 0, f"a partial reference was emitted ({published} gens)"
    print("\n[abort] injected operator failure -> 0 published generations")


# --------------------------------------------------------------------------- #
# task.harness.dep-baseline verification clause.                              #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_dependency_baseline_matches_reference(
    scratch_db: Callable[[], str],
) -> None:
    """With reuse disabled, every dependent reruns (execution evidence) and the
    resolved state equals the complete reference on the fixture series."""
    facts = _seed_facts()
    src = _empty_source()

    ref = run_complete_reference(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=src,
    )
    base = run_dependency_baseline(
        facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch_db(),
        outcome_store_source=src,
    )

    # Resolved state equals the reference (Dep_V == reference).
    assert base.state_dump == ref.state_dump
    assert base.state_dump_digest == ref.state_dump_digest

    # Every dependent reran: full recompute with a recorded cause, and every
    # check ran a full sweep (scoped=false). Assert on the execution evidence
    # already on the result — no new ledger.
    assert base.derivation_modes, "baseline recorded no maintained derivations"
    assert_every_derivation_reran_full(base)
    assert_every_check_full_sweep(base)
    for step in base.derivation_modes.values():
        assert step.full_reason == "forced", (
            f"{step.derivation_id} full_reason={step.full_reason!r}, "
            f"expected 'forced' (reuse disabled)"
        )

    # The reference ran full too (fresh scaffold) but NOT via the forced lever —
    # this is what distinguishes the two modes' execution evidence.
    for step in ref.derivation_modes.values():
        assert step.full_reason in FULL_RECOMPUTE_CAUSES
        assert step.full_reason != "forced"

    print(
        f"\n[dep-baseline] derivations={sorted(base.derivation_modes)} "
        f"checks_scoped_false={[c.check_id for c in base.check_coverage]} "
        f"digest={base.state_dump_digest[:16]} == reference"
    )
