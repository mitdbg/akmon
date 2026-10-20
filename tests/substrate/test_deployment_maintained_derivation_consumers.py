"""Deployment-facing maintained-derivation consumer contract (#444 slice 7)."""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRegistry,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    GovernanceProfile,
    Materialization,
    PublishCycleDeltas,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    CallableCorrectnessOracle,
    DerivationSpec,
    ForceInputs,
    GenericChronosKeptTable,
    MaintainContext,
    MaintainResult,
    SentinelSpec,
    parity_check,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    SENTINEL_TOKEN,
)
from okg.substrate.graph_store import (
    ChronosGraphStore,
    _edge_id,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"
_TABLE = "okg.deployment_currency_rollup"
_SENTINEL = SentinelSpec(pk_columns=["currency"], cadence_column="record_count")
_DDL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
  currency text NOT NULL,
  conflict_kind text NOT NULL,
  record_count bigint NOT NULL,
  PRIMARY KEY (currency)
)
"""


def _seed_catalog_version(dsn: str) -> None:
    from tests.substrate._db_helpers import seed_catalog_version

    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)


def _node(node_id: str, currency: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype="document",
        attrs={"currency": currency},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _edge(src: str, dst: str, edge_type: str = "references") -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        attrs={},
        source_record_id={"id": f"{src}->{dst}:{edge_type}"},
        source_revision={"v": 1},
        op="I",
    )


def _apply(dsn: str, facts: list[NodeFact | EdgeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source="deployment-fixture", strict_endpoints=False)


def _storage() -> GenericChronosKeptTable:
    return GenericChronosKeptTable(
        table=_TABLE,
        primary_key=["currency"],
        create_table_sql=_DDL,
        columns=["currency", "conflict_kind", "record_count"],
    )


def _currency_truth(checkout: Any) -> dict[tuple[str], tuple[int, str]]:
    rows = checkout.query(
        """
        SELECT attrs->>'currency' AS currency, count(*)::bigint AS record_count
          FROM okg.graph_nodes
         WHERE subtype = 'document'
           AND attrs ? 'currency'
         GROUP BY attrs->>'currency'
         ORDER BY attrs->>'currency'
        """
    )
    return {
        (str(row["currency"]),): (
            int(row["record_count"]),
            "duplicate" if int(row["record_count"]) > 1 else "unique",
        )
        for row in rows
    }


def _kept_rows(checkout: Any) -> dict[tuple[str], tuple[int, str]]:
    rows = checkout.query(
        f"""
        SELECT currency, record_count, conflict_kind
          FROM {_TABLE}
         WHERE currency <> '{SENTINEL_TOKEN}'
         ORDER BY currency
        """
    )
    return {
        (str(row["currency"]),): (
            int(row["record_count"]),
            str(row["conflict_kind"]),
        )
        for row in rows
    }


def _result_from_truth(
    truth: dict[tuple[str], tuple[int, str]],
    *,
    cadence_counter: int,
    mode: str,
    dirty_bucket_count: int | None,
) -> MaintainResult:
    return MaintainResult(
        upserts=[
            {
                "currency": key[0],
                "record_count": value[0],
                "conflict_kind": value[1],
            }
            for key, value in sorted(truth.items())
        ],
        deletes=[],
        cadence_counter=cadence_counter,
        dirty_bucket_count=dirty_bucket_count,
        mode=mode,
    )


def _row(cycles: list[PublishCycleDeltas] | None = None) -> DerivationRow:
    oracle = CallableCorrectnessOracle(
        _currency_truth,
        key_columns=["currency"],
        value_columns=["record_count", "conflict_kind"],
        label="deployment_currency_rollup",
    )

    def _full(ctx: MaintainContext) -> MaintainResult:
        return _result_from_truth(
            _currency_truth(ctx.checkout),
            cadence_counter=0,
            mode="full_recount",
            dirty_bucket_count=None,
        )

    def _incremental(ctx: MaintainContext) -> MaintainResult:
        cycle = ctx.change_inputs
        dirty_bucket_count = (
            len(cycle.changed_node_ids or ()) + len(cycle.changed_edge_ids or ())
            if isinstance(cycle, PublishCycleDeltas)
            else 0
        )
        return _result_from_truth(
            _currency_truth(ctx.checkout),
            cadence_counter=1,
            mode="incremental",
            dirty_bucket_count=dirty_bucket_count,
        )

    def _make_inputs(cycle: PublishCycleDeltas) -> ForceInputs:
        if cycles is not None:
            cycles.append(cycle)
        return ForceInputs(
            change_inputs=cycle,
            force_full=cycle.force_full,
            scope_available=cycle.dirty_scope is not None,
        )

    spec = DerivationSpec(
        derivation_id="deployment_currency_rollup",
        sentinel=_SENTINEL,
        oracle=oracle,
        force_full_predicates=[],
        full_recount=_full,
        incremental=_incremental,
        self_heal_every=1_000,
    )
    return DerivationRow(
        spec=spec,
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node", "graph_edge"})),
        materializations=[
            Materialization(
                kind="side_table",
                storage=_storage(),
                sentinel=_SENTINEL,
            )
        ],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        timing_key="deployment_currency_rollup",
        make_cycle_inputs=_make_inputs,
    )


def _registry(row: DerivationRow) -> DerivationRegistry:
    registry = DerivationRegistry()
    registry.register(row)
    return registry


def _metadata(dsn: str, generation_id: int | str) -> dict[str, Any]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return dict(store.get_generation(generation_id).metadata or {})


def _checkpoint_id(dsn: str, generation_id: int | str) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return str(store.get_generation(generation_id).checkpoint_id)


def test_registration_routing_through_publisher_cycle(clean_runtime: str) -> None:
    _seed_catalog_version(clean_runtime)
    cycles: list[PublishCycleDeltas] = []
    registry = _registry(_row(cycles))

    _apply(clean_runtime, [_node("doc:usd-1", "USD"), _node("doc:eur-1", "EUR")])
    r1 = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        maintained_derivations=registry,
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    ).cycle()
    assert r1["status"] == "published", r1.get("blocked_reason")

    _apply(clean_runtime, [_edge("doc:usd-1", "doc:eur-1")])
    r2 = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        maintained_derivations=registry,
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    ).cycle()
    assert r2["status"] == "published", r2.get("blocked_reason")

    assert len(cycles) == 2
    cycle = cycles[-1]
    assert cycle.branch == _DEPLOYMENT
    meta2 = _metadata(clean_runtime, r2["generation_id"])
    assert cycle.generation_id == meta2["target_generation_id"]
    assert cycle.prior_generation_checkpoint == _checkpoint_id(
        clean_runtime,
        r1["generation_id"],
    )
    assert cycle.dirty_scope is not None
    assert cycle.force_full is False
    assert cycle.changed_node_ids == ()
    assert cycle.changed_edge_ids == (
        int(_edge_id("doc:usd-1", "references", "doc:eur-1")),
    )

    summary = meta2["maintained_derivations"]["deployment_currency_rollup"]
    assert summary["status"] == "maintained"
    assert summary["mode"] == "incremental"
    assert summary["fallback_reason"] is None
    assert summary["dirty_bucket_count"] == 1
    assert r2["maintained_derivations"]["deployment_currency_rollup"] == summary


def test_callable_oracle_admitted_and_catches_drift(clean_runtime: str) -> None:
    _seed_catalog_version(clean_runtime)
    row = _row()
    _registry(row)
    _apply(clean_runtime, [_node("doc:usd-1", "USD"), _node("doc:usd-2", "USD")])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout(_DEPLOYMENT)
        expected = {("USD",): (2, "duplicate")}
        assert row.spec.oracle.recompute(checkout) == expected
        assert parity_check(row.spec.oracle, checkout, expected).ok

        drift = {("USD",): (1, "unique")}
        result = parity_check(row.spec.oracle, checkout, drift)
        assert not result.ok
        assert result.mismatched == [("USD",)]


def test_generic_kept_table_supports_as_of_generation(
    clean_runtime: str,
) -> None:
    _seed_catalog_version(clean_runtime)
    row = _row()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        storage = row.materializations[0].storage
        storage.ensure_registered(store)
        store.ensure_ontology_rollup_table()
        store.ensure_console_count_rollup_table()

        store.apply_facts(
            [_node("doc:usd-1", "USD")],
            source="deployment-fixture",
            strict_endpoints=False,
        )
        from okg.substrate.derived_artifacts.maintained_derivation import maintain

        maintain(
            conn,
            spec=row.spec,
            storage=storage,
            branch=_DEPLOYMENT,
            generation_id=1,
            prior_generation_checkpoint=None,
            inputs=ForceInputs(
                change_inputs=None,
                force_full=False,
                scope_available=True,
            ),
        )
        gen1_checkpoint = store.checkpoint_generation(
            1,
            branch=_DEPLOYMENT,
            deployment_name=_DEPLOYMENT,
        )

        store.apply_facts(
            [_node("doc:eur-1", "EUR")],
            source="deployment-fixture",
            strict_endpoints=False,
        )
        maintain(
            conn,
            spec=row.spec,
            storage=storage,
            branch=_DEPLOYMENT,
            generation_id=2,
            prior_generation_checkpoint=gen1_checkpoint,
            inputs=ForceInputs(
                change_inputs=PublishCycleDeltas(
                    branch=_DEPLOYMENT,
                    generation_id=2,
                    prior_generation_checkpoint=gen1_checkpoint,
                    dirty_scope=(
                        {"document": ["doc:eur-1"]},
                        {},
                        True,
                        None,
                        {},
                        None,
                    ),
                    force_full=False,
                    changed_node_ids=("doc:eur-1",),
                    changed_edge_ids=(),
                ),
                force_full=False,
                scope_available=True,
            ),
        )
        store.checkpoint_generation(
            2,
            branch=_DEPLOYMENT,
            deployment_name=_DEPLOYMENT,
        )

        assert _kept_rows(store.checkout_generation(1)) == {
            ("USD",): (1, "unique"),
        }
        assert _kept_rows(store.checkout(_DEPLOYMENT)) == {
            ("EUR",): (1, "unique"),
            ("USD",): (1, "unique"),
        }


def test_generic_kept_table_rejects_registered_primary_key_mismatch(
    clean_runtime: str,
) -> None:
    _seed_catalog_version(clean_runtime)
    row = _row()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        row.materializations[0].storage.ensure_registered(store)

        wrong = GenericChronosKeptTable(
            table=_TABLE,
            primary_key=["conflict_kind"],
            create_table_sql=_DDL,
            columns=["currency", "conflict_kind", "record_count"],
        )
        with pytest.raises(ValueError, match="primary key"):
            wrong.ensure_registered(store)
