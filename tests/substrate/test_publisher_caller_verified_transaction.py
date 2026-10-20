"""Real PostgreSQL proof for caller-verified atomic publication."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
import uuid
from datetime import datetime, timezone

import psycopg
import pytest
from chronos_core.branching import BranchSession, ChronosBranchContext

from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    GovernanceProfile,
    Materialization,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    ForceInputs,
    MaintainResult,
    SentinelSpec,
)
from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.enrichers._fact_emitters import emit_node_fact
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publisher.native_view_stage import NativeViewStage
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer, seed_catalog


pytestmark = pytest.mark.postgres


class VerificationFault(RuntimeError):
    """Injected after checkpoint creation and before publication commit."""


class NativeViewReadFault(RuntimeError):
    """Stop a native-view helper after its Chronos store is constructed."""


class _AtomicFirstRunEnricher:
    name = "atomic-first-run"
    atomic_publish_transaction_compatible = True
    publisher_skip_when_clean = True
    reads_subtypes = ("concept",)
    reads_edge_types = ()
    emits_edge_types = ()
    emits_typed_subtype = "concept"
    requires_narrowings = ()
    declares_node_provenance = True
    nondeterministic = False
    write_envelope = {
        "nodes": [{"subtype": "concept"}],
        "nondeterministic": False,
    }

    def __init__(self, *, input_node_id: str, output_node_id: str) -> None:
        self.input_node_id = input_node_id
        self.output_node_id = output_node_id
        self.contexts = []

    def enrich(self, conn, *, generation_id, incremental=None):
        assert incremental is not None
        self.contexts.append(incremental)
        assert incremental.is_first_run is True
        assert self.input_node_id in incremental.dirty_node_ids
        outcome = emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id=self.output_node_id,
            subtype="concept",
            attrs={"derived": True},
            dedupe_key=f"{self.name}:{self.output_node_id}:{generation_id}",
            source_record_id={"node_id": self.output_node_id},
            source_revision={"generation_id": str(generation_id)},
            run_id=str(uuid.uuid4()),
            observed_at=datetime.now(timezone.utc),
            derived_from=[self.input_node_id],
            support_policy="all_members",
        )
        assert outcome.emitted is True
        return EnrichResult(self.name, n_edges_emitted=0)


class _LifecycleTransactionAttackEnricher(_AtomicFirstRunEnricher):
    """Compatible enricher that attempts to take ownership from a lifecycle hook."""

    def __init__(self, *, attack_hook: str, attack_action: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.attack_hook = attack_hook
        self.attack_action = attack_action
        self.attack_armed = False
        self.publisher_conn: psycopg.Connection | None = None
        self.can_skip_calls = 0

    def enrich(self, conn, *, generation_id, incremental=None):
        if not self.contexts:
            return super().enrich(
                conn,
                generation_id=generation_id,
                incremental=incremental,
            )
        self.contexts.append(incremental)
        return EnrichResult(self.name, n_edges_emitted=0)

    def _attack(self, conn: psycopg.Connection) -> None:
        if not self.attack_armed:
            return
        self.attack_armed = False
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.atomic_callback_markers(marker) VALUES (%s)",
                (f"{self.attack_hook}:{self.attack_action}",),
            )
        with pytest.raises(psycopg.ProgrammingError):
            getattr(conn, self.attack_action)()

    def _set_publisher_context(self, **kwargs) -> None:
        del kwargs
        if self.attack_hook == "context":
            assert self.publisher_conn is not None
            self._attack(self.publisher_conn)

    def can_skip_clean(self, conn, ctx) -> bool:
        del ctx
        self.can_skip_calls += 1
        if self.attack_hook == "can_skip_clean":
            self._attack(conn)
        return True

    def _finish_publisher_transaction(self, conn, *, commit: bool) -> None:
        if self.attack_hook == "finish" and commit:
            self._attack(conn)


def _seed_first_run_input(dsn: str, node_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["concept"],
            narrowings=[("concept", "contains", "concept")],
        )
        run_id = str(uuid.uuid4())
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source, source_record_id,
                   source_revision, observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'concept', '{}'::jsonb, 'atomic_fixture',
                        %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
                """,
                (
                    node_id,
                    json.dumps({"node_id": node_id}),
                    json.dumps({"run_id": run_id}),
                    f"atomic-fixture:{node_id}:{run_id}",
                    run_id,
                ),
            )
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    node_id=node_id,
                    subtype="concept",
                    attrs={"fixture": "atomic-first-run"},
                    source_record_id={"node_id": node_id},
                    source_revision={"run_id": run_id},
                )
            ],
            source="atomic_fixture",
        )


def _node_fact_count(dsn: str, node_id: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts WHERE node_id = %s",
            (node_id,),
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _latest_generation(dsn: str, deployment: str):
    with psycopg.connect(dsn, autocommit=True) as conn:
        return ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )


def _branch_segment(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT current_segment_id "
            "FROM _chronos_branch_interval_branches "
            "WHERE branch_id = 'default'"
        )
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _prepare_callback_marker_table(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS okg.atomic_callback_markers "
            "(marker text PRIMARY KEY)"
        )


def _callback_marker_count(dsn: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.atomic_callback_markers")
        row = cur.fetchone()
    assert row is not None
    return int(row[0])


def _prepare_faulting_derivation(dsn: str, table: str) -> DerivationRow:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.context.db.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            "(k text PRIMARY KEY, v bigint NOT NULL)"
        )
        store.context.db.commit()
        store.context.register_table(table, ["k"])

    sentinel = SentinelSpec(pk_columns=["k"], cadence_column="v")

    def full_recount(ctx) -> MaintainResult:
        del ctx
        return MaintainResult(
            upserts=[{"k": "fault-row", "v": 1}],
            deletes=[],
            cadence_counter=0,
            dirty_bucket_count=None,
            mode="full_recount",
        )

    spec = DerivationSpec(
        derivation_id="atomic_faulting_derivation",
        sentinel=sentinel,
        oracle=CorrectnessOracle("SELECT 'fault-row' AS k, 1 AS v", ["k"], ["v"]),
        force_full_predicates=[],
        full_recount=full_recount,
        incremental=full_recount,
        self_heal_every=25,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[
            Materialization(
                kind="side_table",
                storage=ChronosRegisteredTable(table=table, ensure_fn=lambda store: None),
                sentinel=sentinel,
            )
        ],
        governance=GovernanceProfile(
            determinism=DeterminismClass.DETERMINISTIC
        ),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        make_inputs=lambda scope, force: ForceInputs(
            change_inputs=scope,
            force_full=force,
            scope_available=scope is not None,
        ),
    )


def test_verifier_failure_rolls_back_real_checkpoint_and_pointer(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "caller-verified-rollback"
    identity = bind_test_catalog_writer(clean_runtime, deployment)
    prior_segment = _branch_segment(clean_runtime)
    observed: dict[str, str] = {}
    finalized: list[bool] = []
    original_finalize = ChronosBranchContext.finalize_external_transaction

    def record_finalize(self, *, committed: bool) -> None:
        finalized.append(committed)
        original_finalize(self, committed=committed)

    monkeypatch.setattr(
        ChronosBranchContext,
        "finalize_external_transaction",
        record_finalize,
    )

    def fail_after_checkpoint(conn: psycopg.Connection, generation_id: str) -> None:
        latest = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        ).latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        assert latest is not None
        assert str(latest.checkpoint_id) == generation_id
        observed["generation_id"] = generation_id
        raise VerificationFault("fail after checkpoint before commit")

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with pytest.raises(VerificationFault, match="before commit"):
            Publisher(
                clean_runtime,
                deployment,
                catalog_identity=identity,
            ).cycle(
                conn=conn,
                atomic_precommit_verifier=fail_after_checkpoint,
            )

    assert observed["generation_id"]
    assert finalized == [False]
    assert _latest_generation(clean_runtime, deployment) is None
    assert _branch_segment(clean_runtime) == prior_segment


def test_first_run_enricher_fact_checkpoint_and_pointer_roll_back_together(
    clean_runtime: str,
) -> None:
    deployment = "caller-verified-first-run-enricher"
    input_node_id = "atomic:source"
    output_node_id = "atomic:derived"
    _seed_first_run_input(clean_runtime, input_node_id)
    identity = bind_test_catalog_writer(clean_runtime, deployment)
    enricher = _AtomicFirstRunEnricher(
        input_node_id=input_node_id,
        output_node_id=output_node_id,
    )
    prior_segment = _branch_segment(clean_runtime)
    observed: dict[str, str] = {}

    def fail_after_enricher_and_checkpoint(
        conn: psycopg.Connection,
        generation_id: str,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE node_id = %s",
                (output_node_id,),
            )
            assert int(cur.fetchone()[0]) == 1
        latest = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        ).latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        assert latest is not None
        assert str(latest.checkpoint_id) == generation_id
        observed["generation_id"] = generation_id
        raise VerificationFault("fail after enricher fact and checkpoint")

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with pytest.raises(VerificationFault, match="enricher fact"):
            Publisher(
                clean_runtime,
                deployment,
                enrichers=[enricher],
                catalog_identity=identity,
            ).cycle(
                conn=conn,
                atomic_precommit_verifier=fail_after_enricher_and_checkpoint,
            )

    assert observed["generation_id"]
    assert len(enricher.contexts) == 1
    assert _node_fact_count(clean_runtime, input_node_id) == 1
    assert _node_fact_count(clean_runtime, output_node_id) == 0
    assert _latest_generation(clean_runtime, deployment) is None
    assert _branch_segment(clean_runtime) == prior_segment


@pytest.mark.parametrize("attack_hook", ["context", "can_skip_clean", "finish"])
@pytest.mark.parametrize("attack_action", ["commit", "rollback"])
def test_every_enricher_lifecycle_hook_cannot_end_caller_transaction(
    clean_runtime: str,
    attack_hook: str,
    attack_action: str,
) -> None:
    deployment = f"atomic-hook-{attack_hook}-{attack_action}"
    input_node_id = f"atomic:{attack_hook}:{attack_action}:source"
    output_node_id = f"atomic:{attack_hook}:{attack_action}:derived"
    _seed_first_run_input(clean_runtime, input_node_id)
    _prepare_callback_marker_table(clean_runtime)
    identity = bind_test_catalog_writer(clean_runtime, deployment)
    enricher = _LifecycleTransactionAttackEnricher(
        attack_hook=attack_hook,
        attack_action=attack_action,
        input_node_id=input_node_id,
        output_node_id=output_node_id,
    )

    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[enricher],
        catalog_identity=identity,
    )

    verifier_reached = False

    def fail_after_hook(conn: psycopg.Connection, generation_id: str) -> None:
        nonlocal verifier_reached
        del generation_id
        assert conn.info.transaction_status is psycopg.pq.TransactionStatus.INTRANS
        if attack_hook == "can_skip_clean":
            ctx = enricher.contexts[-1]
            catalog_version_id = ctx.current_catalog_version_id
            assert catalog_version_id is not None
            clean_ctx = replace(
                ctx,
                is_first_run=False,
                dirty_node_ids=frozenset(),
                dirty_edge_ids=frozenset(),
                last_status="success",
                prev_catalog_version_id=catalog_version_id,
            )
            assert publisher._should_skip_clean_enricher(
                conn,
                enricher,
                clean_ctx,
                catalog_version_id=catalog_version_id,
                caller_owned_transaction=True,
            )
        verifier_reached = True
        raise VerificationFault("later verifier failure")

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        enricher.publisher_conn = conn
        enricher.attack_armed = True
        with pytest.raises(VerificationFault, match="later verifier failure"):
            publisher.cycle(
                conn=conn,
                atomic_precommit_verifier=fail_after_hook,
            )

    assert verifier_reached is True
    assert enricher.attack_armed is False
    assert _callback_marker_count(clean_runtime) == 0
    assert _latest_generation(clean_runtime, deployment) is None


def test_failsoft_derivation_fault_rolls_back_savepoint_not_publish_transaction(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "atomic-failsoft-derivation"
    table = "okg.atomic_faulting_derivation"
    row = _prepare_faulting_derivation(clean_runtime, table)
    identity = bind_test_catalog_writer(clean_runtime, deployment)
    original_upsert_rows = BranchSession.upsert_rows
    fault_injected = False

    def write_then_fault(self, target_table, rows):
        nonlocal fault_injected
        result = original_upsert_rows(self, target_table, rows)
        if target_table == table and not fault_injected:
            fault_injected = True
            raise RuntimeError("fault after derivation write")
        return result

    monkeypatch.setattr(BranchSession, "upsert_rows", write_then_fault)
    verifier_reached = False
    outer_txid: int | None = None

    def fail_after_checkpoint(conn: psycopg.Connection, generation_id: str) -> None:
        nonlocal verifier_reached
        verifier_reached = True
        assert conn.info.transaction_status is psycopg.pq.TransactionStatus.INTRANS
        with conn.cursor() as cur:
            cur.execute("SELECT txid_current()")
            assert int(cur.fetchone()[0]) == outer_txid
        store = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        )
        # The failed NON_GATING row was rolled back to its savepoint, while the
        # outer cycle continued far enough to create its checkpoint.
        assert store.checkout("default").query(f"SELECT * FROM {table}") == []
        latest = store.latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        assert latest is not None
        assert str(latest.checkpoint_id) == generation_id
        raise VerificationFault("fail after fail-soft derivation")

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT txid_current()")
            outer_txid = int(cur.fetchone()[0])
        with pytest.raises(VerificationFault, match="fail-soft derivation"):
            Publisher(
                clean_runtime,
                deployment,
                maintained_derivations=[row],
                catalog_identity=identity,
            ).cycle(
                conn=conn,
                atomic_precommit_verifier=fail_after_checkpoint,
            )

    assert fault_injected is True
    assert verifier_reached is True
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert store.checkout("default").query(f"SELECT * FROM {table}") == []
    assert _latest_generation(clean_runtime, deployment) is None


def test_verifier_success_commits_real_checkpoint_once(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "caller-verified-success"
    identity = bind_test_catalog_writer(clean_runtime, deployment)
    observed: list[str] = []
    finalized: list[bool] = []
    original_finalize = ChronosBranchContext.finalize_external_transaction

    def record_finalize(self, *, committed: bool) -> None:
        finalized.append(committed)
        original_finalize(self, committed=committed)

    monkeypatch.setattr(
        ChronosBranchContext,
        "finalize_external_transaction",
        record_finalize,
    )

    def verify_pointer(conn: psycopg.Connection, generation_id: str) -> None:
        latest = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
        ).latest_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )
        assert latest is not None
        assert str(latest.checkpoint_id) == generation_id
        observed.append(generation_id)

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        result = Publisher(
            clean_runtime,
            deployment,
            catalog_identity=identity,
        ).cycle(
            conn=conn,
            atomic_precommit_verifier=verify_pointer,
        )

    assert observed == [result["generation_id"]]
    assert finalized == [True]
    latest = _latest_generation(clean_runtime, deployment)
    assert latest is not None
    assert str(latest.checkpoint_id) == result["generation_id"]


def test_atomic_verifier_requires_caller_transaction() -> None:
    publisher = Publisher(
        "postgresql://unused",
        "caller-verified-validation",
    )

    with pytest.raises(ValueError, match="supplied connection"):
        publisher.cycle(atomic_precommit_verifier=lambda conn, generation: None)

    with pytest.raises(ValueError, match="autocommit=False"):
        publisher.cycle(
            conn=SimpleNamespace(autocommit=True),
            atomic_precommit_verifier=lambda conn, generation: None,
        )


def test_atomic_verifier_rejects_unreviewed_enricher_before_publish(
    clean_runtime: str,
) -> None:
    class _UnreviewedEnricher(_AtomicFirstRunEnricher):
        name = "unreviewed-enricher"
        atomic_publish_transaction_compatible = False

    deployment = "caller-verified-incompatible-enricher"
    input_node_id = "atomic:unreviewed-source"
    _seed_first_run_input(clean_runtime, input_node_id)
    publisher = Publisher(
        clean_runtime,
        deployment,
        enrichers=[
            _UnreviewedEnricher(
                input_node_id=input_node_id,
                output_node_id="atomic:must-not-emit",
            )
        ],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    prior_segment = _branch_segment(clean_runtime)

    with psycopg.connect(clean_runtime, autocommit=False) as conn:
        with pytest.raises(
            ValueError,
            match="incompatible enrichers: unreviewed-enricher",
        ):
            publisher.cycle(
                conn=conn,
                atomic_precommit_verifier=lambda conn, generation: None,
            )

    assert _node_fact_count(clean_runtime, "atomic:must-not-emit") == 0
    assert _latest_generation(clean_runtime, deployment) is None
    assert _branch_segment(clean_runtime) == prior_segment


def test_native_view_chronos_reads_inherit_caller_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_autocommit: list[bool] = []

    class _FaultingGraph:
        def query(self, sql, params=None):
            del sql, params
            raise NativeViewReadFault

    class _Store:
        def checkout(self, branch):
            del branch
            return _FaultingGraph()

    def from_connection(conn, *, ensure_schema, autocommit):
        del conn
        assert ensure_schema is False
        observed_autocommit.append(autocommit)
        return _Store()

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        staticmethod(from_connection),
    )
    stage = NativeViewStage([], deployment_name="caller-verified-native-view")
    conn = SimpleNamespace(autocommit=False)

    with pytest.raises(NativeViewReadFault):
        stage._collect_source_records(conn, ["atomic:source"])
    with pytest.raises(NativeViewReadFault):
        stage._project_view_cites(
            conn,
            view_node_id="view:atomic",
            view_def_id="atomic",
            definition_id="view:atomic",
            fragments=[],
            resolver=lambda anchor_id: anchor_id,
            generation_id=1,
        )

    assert observed_autocommit == [False, False]
