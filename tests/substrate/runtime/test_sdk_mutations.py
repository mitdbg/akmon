"""Tests for SDK mutation helpers (Phase 2 §3.1, §3.3, §3.4).

Uses :func:`bind_test_context` to drive the helpers without a full
DBOS singleton. The helpers issue real SQL against a per-test
Postgres DB; the substrate-side worker-write generation + bitemporal
projection paths are exercised end-to-end.
"""
from __future__ import annotations

import uuid

import psycopg
import pytest
from tests.substrate.runtime._helpers import _swap_dbname


pytestmark = pytest.mark.dbos_integration


def _graph_head(conn):
    """Return a Chronos checkout of the default-branch head, the way the
    interval backend reconstructs current graph state. SDK mutations apply
    to this head via ChronosGraphStore.apply_facts."""
    from okg.substrate.graph_store import ChronosGraphStore

    return ChronosGraphStore.from_connection(
        conn, ensure_schema=False,
    ).checkout()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def test_db_dsn(admin_dsn: str):
    """Per-test DB with the minimum schema the SDK mutation helpers
    touch: catalog_versions, graph_generations, node_facts, edge_facts,
    the Chronos graph (graph_nodes/graph_edges), plus the Phase 2 cost
    ledger / caps.

    The chronos-native cutover dropped okg.nodes_live / okg.edges_live;
    the SDK mutation helpers now record an audit fact then apply the edit
    to the Chronos graph via ChronosGraphStore.apply_facts, so the live
    tables are gone and the chronos graph store is initialized instead."""
    db_name = "okg_sdk_mutations_test"
    dsn = _swap_dbname(admin_dsn, db_name)

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            cur.execute(f'CREATE DATABASE "{db_name}"')

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
            cur.execute("CREATE SCHEMA okg")
            cur.execute(
                "CREATE TABLE okg.catalog_versions ("
                "catalog_version_id BIGSERIAL PRIMARY KEY, "
                "status TEXT NOT NULL)"
            )
            cur.execute(
                "INSERT INTO okg.catalog_versions (status) VALUES ('applied')"
            )
            cur.execute(
                "CREATE TABLE okg.graph_generations ("
                "generation_id BIGSERIAL PRIMARY KEY, "
                "deployment_name TEXT, "
                "status TEXT NOT NULL CHECK (status IN "
                "  ('pending','published','blocked','worker_write')), "
                "catalog_version_id BIGINT NOT NULL "
                "  REFERENCES okg.catalog_versions(catalog_version_id), "
                "correlation_id TEXT NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )
            # Fact tables (non-hypertable; matches schema columns).
            cur.execute(
                "CREATE TABLE okg.node_facts ("
                "fact_id BIGSERIAL, "
                "node_id TEXT NOT NULL, "
                "subtype TEXT NOT NULL, "
                "attrs JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "source TEXT NOT NULL, "
                "source_record_id JSONB NOT NULL, "
                "source_revision JSONB NOT NULL, "
                "observed_at TIMESTAMPTZ NOT NULL, "
                "dedupe_key TEXT NOT NULL, "
                "run_id UUID NOT NULL, "
                "op CHAR(1) NOT NULL CHECK (op IN ('I','R')), "
                "PRIMARY KEY (fact_id, observed_at), "
                "UNIQUE (dedupe_key, observed_at))"
            )
            cur.execute(
                "CREATE TABLE okg.edge_facts ("
                "fact_id BIGSERIAL, "
                "edge_id BIGINT NOT NULL, "
                "src TEXT NOT NULL, "
                "dst TEXT NOT NULL, "
                "edge_type TEXT NOT NULL, "
                "attrs JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "provenance TEXT NOT NULL, "
                "confidence REAL NOT NULL DEFAULT 1.0, "
                "source TEXT NOT NULL, "
                "source_record_id JSONB NOT NULL, "
                "source_revision JSONB NOT NULL, "
                "observed_at TIMESTAMPTZ NOT NULL, "
                "dedupe_key TEXT NOT NULL, "
                "run_id UUID NOT NULL, "
                "op CHAR(1) NOT NULL CHECK (op IN ('I','R')), "
                "PRIMARY KEY (fact_id, observed_at), "
                "UNIQUE (dedupe_key, observed_at))"
            )
            cur.execute(
                "CREATE TABLE okg.deployment_cost_ledger ("
                "scope TEXT NOT NULL, "
                "consumption_id TEXT NOT NULL, "
                "usd_consumed NUMERIC NOT NULL CHECK (usd_consumed >= 0), "
                "workflow_id TEXT, "
                "observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
                "PRIMARY KEY (scope, consumption_id))"
            )
            cur.execute(
                "CREATE TABLE okg.deployment_cost_caps ("
                "scope TEXT PRIMARY KEY, "
                "cap_usd NUMERIC NOT NULL CHECK (cap_usd >= 0), "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"
            )

    # Initialize the Chronos graph store + branch infra (creates
    # okg.graph_nodes / okg.graph_edges and the public._chronos_* tables)
    # so the SDK mutation helpers' apply_facts call resolves.
    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=True)

    yield dsn

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity WHERE datname = %s "
                "AND pid <> pg_backend_pid()",
                (db_name,),
            )
            cur.execute(f'DROP DATABASE IF EXISTS "{db_name}"')


# ---------------------------------------------------------------------------
# emit_node_fact
# ---------------------------------------------------------------------------


def test_emit_node_fact_inserts_fact_and_live(test_db_dsn):
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            fact_id = emit_node_fact(
                "candidate-1", "Candidate",
                {"text": "hello"}, dedupe_key="dk-1",
            )

        # Verify fact row.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_id, subtype, source FROM okg.node_facts "
                "WHERE fact_id = %s",
                (fact_id,),
            )
            n, s, src = cur.fetchone()
            assert n == "candidate-1"
            assert s == "Candidate"
            assert src.startswith("_deployment:test:")

        # Verify the node landed in the Chronos graph (graph_nodes), read
        # through a checkout of the default-branch head.
        rows = _graph_head(conn).query(
            "SELECT subtype, attrs->>'text' AS text "
            "FROM okg.graph_nodes WHERE node_id = :nid",
            {"nid": "candidate-1"},
        )
        assert len(rows) == 1
        assert rows[0]["subtype"] == "Candidate"
        assert rows[0]["text"] == "hello"


def test_emit_node_fact_second_emit_wins(test_db_dsn):
    """A second emit_node_fact for the same node upserts it in the
    Chronos graph (last-write-wins): the single graph node reflects the
    latest emit. The chronos-native cutover replaced the bitemporal
    valid_from_gen/valid_to_gen live-row window (close-prior-open-new)
    with a single current node per id in okg.graph_nodes, and the audit
    history is the okg.node_facts log."""
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            emit_node_fact(
                "n", "Subtype", {"v": 1}, dedupe_key="d1",
            )
        # Fresh context so a new worker-write gen is allocated.
        with bind_test_context(deployment="test", conn=conn):
            emit_node_fact(
                "n", "Subtype", {"v": 2}, dedupe_key="d2",
            )

        # The Chronos graph holds exactly one current node for id 'n',
        # carrying the second emit's attrs (last write wins).
        graph_rows = _graph_head(conn).query(
            "SELECT attrs->>'v' AS v FROM okg.graph_nodes WHERE node_id = 'n'"
        )
        # Both emits are recorded in the immutable audit log.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.node_facts WHERE node_id = 'n'"
            )
            fact_count = cur.fetchone()[0]

    assert len(graph_rows) == 1
    assert graph_rows[0]["v"] == "2"   # last write wins in the graph
    assert fact_count == 2             # both emits durably audited


def test_emit_node_fact_requires_context(test_db_dsn):
    from okg.deployment import emit_node_fact
    from okg.deployment._internal.context import MissingContextError

    with pytest.raises(MissingContextError, match="deployment"):
        emit_node_fact("n", "S", {}, dedupe_key="d")


def test_dual_mutation_lands_at_same_generation(test_db_dsn):
    """Three emit_* calls in the SAME bound context share one worker-write
    generation. Phase 0 C1 correctness.

    Post chronos-native cutover the graph rows in okg.graph_nodes /
    graph_edges no longer carry the per-row valid_from_gen the bitemporal
    live tables did, so the shared-generation invariant is asserted at its
    source — the cached worker-write generation — and the three edits are
    confirmed present in the Chronos graph head."""
    from okg.deployment import emit_edge_fact, emit_node_fact
    from okg.deployment._internal.context import (
        bind_test_context,
        current_worker_write_generation,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            emit_node_fact("n-a", "A", {}, dedupe_key="dn-a")
            gen_after_first = current_worker_write_generation()
            emit_node_fact("n-b", "B", {}, dedupe_key="dn-b")
            emit_edge_fact(
                "n-a", "n-b", "rel", {}, dedupe_key="de-ab",
            )
            # All three emits share the one cached worker-write generation.
            gen_after_all = current_worker_write_generation()

        graph = _graph_head(conn)
        n_nodes = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        n_edges = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )

    assert gen_after_all == gen_after_first, (
        "expected dual-mutation to share one worker-write generation, got "
        f"{gen_after_first} then {gen_after_all}"
    )
    assert n_nodes == 2 and n_edges == 1  # both nodes + the edge landed


# ---------------------------------------------------------------------------
# emit_edge_fact
# ---------------------------------------------------------------------------


def test_emit_edge_fact_inserts_edge_with_stable_id(test_db_dsn):
    from okg.deployment import emit_edge_fact
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            fact_id_a = emit_edge_fact(
                "n-1", "n-2", "rel", {}, dedupe_key="de-1",
            )

        with conn.cursor() as cur:
            cur.execute(
                "SELECT edge_id, src, dst, edge_type FROM okg.edge_facts "
                "WHERE fact_id = %s",
                (fact_id_a,),
            )
            edge_id_a, src, dst, et = cur.fetchone()

        # Re-issue the same logical edge in a separate context. The
        # second emit will collide on (dedupe_key, observed_at) only
        # in the same instant; here we use a different dedupe_key so
        # the fact_id is new but edge_id is identical.
        with bind_test_context(deployment="test", conn=conn):
            fact_id_b = emit_edge_fact(
                "n-1", "n-2", "rel", {}, dedupe_key="de-2",
            )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT edge_id FROM okg.edge_facts WHERE fact_id = %s",
                (fact_id_b,),
            )
            edge_id_b = cur.fetchone()[0]

    assert (src, dst, et) == ("n-1", "n-2", "rel")
    assert edge_id_a == edge_id_b, (
        "edge_id must be deterministic over (src, edge_type, dst)"
    )


def test_emit_edge_fact_rejects_bad_provenance(test_db_dsn):
    from okg.deployment import emit_edge_fact
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(ValueError, match="provenance"):
                emit_edge_fact(
                    "a", "b", "rel", {}, dedupe_key="d",
                    provenance="totally-made-up",
                )


def test_emit_edge_fact_rejects_bad_confidence(test_db_dsn):
    from okg.deployment import emit_edge_fact
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with pytest.raises(ValueError, match="confidence"):
                emit_edge_fact(
                    "a", "b", "rel", {}, dedupe_key="d",
                    confidence=1.5,
                )


# ---------------------------------------------------------------------------
# update_node_attrs
# ---------------------------------------------------------------------------


def test_update_node_attrs_merges_over_current(test_db_dsn):
    """update_node_attrs reads the node's current attrs from the Chronos
    graph, shallow-merges new_attrs over them, and re-emits the merged
    node (which upserts okg.graph_nodes).

    The chronos-native cutover removed the valid_from_gen-keyed optimistic
    concurrency (version_tuple / ConcurrentModificationError); the surviving
    contract is the read-merge-upsert exercised here."""
    from okg.deployment import emit_node_fact, update_node_attrs
    from okg.deployment._internal.context import bind_test_context

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            emit_node_fact(
                "n", "S", {"k": "v0", "keep": "yes"}, dedupe_key="d0",
            )

        # A merge update: 'k' is overwritten, 'keep' is preserved.
        with bind_test_context(deployment="test", conn=conn):
            update_node_attrs("n", {"k": "v1"})

        rows = _graph_head(conn).query(
            "SELECT attrs->>'k' AS k, attrs->>'keep' AS keep "
            "FROM okg.graph_nodes WHERE node_id = 'n'"
        )

    assert len(rows) == 1
    assert rows[0]["k"] == "v1"      # overwritten by the update
    assert rows[0]["keep"] == "yes"  # untouched attrs preserved (shallow merge)


# ---------------------------------------------------------------------------
# cost_budget (§3.4)
# ---------------------------------------------------------------------------


def test_cost_budget_consume_durable(test_db_dsn):
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context

    scope = f"test.scope.{uuid.uuid4().hex[:8]}"
    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with cost_budget(usd=10.0, scope=scope) as budget:
                budget.consume(1.5, consumption_id="step-1")
                budget.consume(2.0, consumption_id="step-2")
                assert budget.consumed == pytest.approx(3.5)
                assert budget.available == pytest.approx(6.5)

        # State persists across the context exit.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT sum(usd_consumed) "
                "FROM okg.deployment_cost_ledger WHERE scope = %s",
                (scope,),
            )
            assert float(cur.fetchone()[0]) == pytest.approx(3.5)


def test_cost_budget_consume_idempotent_on_retry(test_db_dsn):
    """Two consumes with the same consumption_id collide on PK; the
    second is a no-op. Cumulative cost stays at the first value."""
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context

    scope = f"test.scope.{uuid.uuid4().hex[:8]}"
    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with cost_budget(usd=10.0, scope=scope) as budget:
                budget.consume(1.23, consumption_id="same-step")
                budget.consume(1.23, consumption_id="same-step")
                assert budget.consumed == pytest.approx(1.23)


def test_cost_budget_exhaustion_raises(test_db_dsn):
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context
    from okg.deployment._internal.cost_budget import BudgetExhaustedError

    scope = f"test.scope.{uuid.uuid4().hex[:8]}"
    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with cost_budget(usd=1.0, scope=scope) as budget:
                budget.consume(0.6, consumption_id="s1")
                with pytest.raises(BudgetExhaustedError) as exc_info:
                    budget.consume(0.5, consumption_id="s2")
                assert exc_info.value.available_usd == pytest.approx(0.4)


def test_cost_budget_extend_raises_cap(test_db_dsn):
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context

    scope = f"test.scope.{uuid.uuid4().hex[:8]}"
    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with cost_budget(usd=1.0, scope=scope) as budget:
                assert budget.cap == pytest.approx(1.0)
                new_cap = budget.extend(5.0)
                assert new_cap == pytest.approx(6.0)
                # We can now consume past the original cap.
                budget.consume(4.5, consumption_id="ok-now")
                assert budget.consumed == pytest.approx(4.5)


# ---------------------------------------------------------------------------
# bind_sdk_context vs bind_test_context provenance
# ---------------------------------------------------------------------------


def test_bind_sdk_context_resolves_a_real_workflow_id(test_db_dsn):
    """The production binder must not stamp a placeholder workflow id.

    Outside DBOS, resolution falls to the documented `_local_$pid`
    fallback — never `"test-wf"`, which is the TEST wrapper's pin and
    was leaking into `okg.deployment_cost_ledger.workflow_id` when the
    spend path used `bind_test_context`.
    """
    import os

    from okg.deployment._internal.context import (
        bind_sdk_context, current_context,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with bind_sdk_context(deployment="test", conn=conn):
            ctx = current_context()
            assert ctx.workflow_id == f"_local_{os.getpid()}"
            assert ctx.workflow_id != "test-wf"


def test_spend_settlement_ledger_row_carries_real_workflow_id(test_db_dsn):
    """ModelCallSpend.settle → ledger row's workflow_id is provenance,
    not the test wrapper's `"test-wf"` placeholder (regression:
    production settlements were recording `test-wf` verbatim)."""
    import os

    from okg.substrate.llm.spend import ModelCallSpend

    scope = f"test.spend.{uuid.uuid4().hex[:8]}"
    spend = ModelCallSpend(
        deployment="test", scope=scope,
        cap_usd=5.0, estimated_usd_per_call=0.5,
    )
    with psycopg.connect(test_db_dsn) as conn:
        admission = spend.reserve(conn)
        assert admission.admitted
        spend.settle(conn, admission, 0.25)
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT workflow_id FROM okg.deployment_cost_ledger "
                "WHERE scope = %s",
                (scope,),
            )
            rows = cur.fetchall()
    assert rows, (
        "settlement wrote no ledger row — settle() swallowed a failure; "
        "check its error log output"
    )
    assert rows[0][0] == f"_local_{os.getpid()}"
    assert rows[0][0] != "test-wf"


def test_settlement_keeps_recording_after_the_cap_is_crossed(test_db_dsn):
    """Probe-shaped regression for the review P0: two runs, one scope,
    ledger total STRICTLY INCREASING.

    Admission is per-run; the ledger primitive's cap check is lifetime.
    Before the fix, settlement went through the enforced path, so once a
    scope's ledger sum crossed the cap every later settlement raised
    BudgetExhaustedError, was swallowed, and the ledger froze forever
    while admitted calls kept spending every run.
    """
    from okg.substrate.llm.spend import ModelCallSpend

    scope_owner = f"test.spendcap.{uuid.uuid4().hex[:8]}"

    def one_run(conn) -> None:
        spend = ModelCallSpend(
            deployment="test", scope=scope_owner,
            cap_usd=0.10, estimated_usd_per_call=0.05,
        )
        for _ in range(2):  # exactly fills the cap each run
            admission = spend.reserve(conn)
            assert admission.admitted
            spend.settle(conn, admission, None)

    totals = []
    with psycopg.connect(test_db_dsn) as conn:
        for _ in range(3):
            one_run(conn)
            conn.commit()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT coalesce(sum(usd_consumed), 0) "
                    "FROM okg.deployment_cost_ledger WHERE scope = %s",
                    (scope_owner,),
                )
                totals.append(float(cur.fetchone()[0]))
    assert totals == pytest.approx([0.10, 0.20, 0.30]), (
        f"ledger must record every admitted run's spend; got {totals} — "
        "a frozen total means settlement is being cap-refused again"
    )


def test_cost_budget_consume_enforce_cap_false_records_past_cap(test_db_dsn):
    from okg.deployment import cost_budget
    from okg.deployment._internal.context import bind_test_context
    from okg.deployment._internal.cost_budget import BudgetExhaustedError

    scope = f"test.scope.{uuid.uuid4().hex[:8]}"
    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            with cost_budget(usd=1.0, scope=scope) as budget:
                budget.consume(0.9, consumption_id="s1")
                # Enforced path still refuses…
                with pytest.raises(BudgetExhaustedError):
                    budget.consume(0.5, consumption_id="s2")
                # …the accounting path records unconditionally.
                total = budget.consume(
                    0.5, consumption_id="s2", enforce_cap=False,
                )
                assert total == pytest.approx(1.4)


def test_release_returns_headroom_and_writes_nothing(test_db_dsn):
    """A reservation whose call was never issued (rate refusal after a
    successful reserve) must release, not settle: settling wrote a ledger
    row for money never spent."""
    from okg.substrate.llm.spend import ModelCallSpend

    scope = f"test.release.{uuid.uuid4().hex[:8]}"
    spend = ModelCallSpend(
        deployment="test", scope=scope,
        cap_usd=0.10, estimated_usd_per_call=0.06,
    )
    with psycopg.connect(test_db_dsn) as conn:
        first = spend.reserve(conn)
        assert first.admitted
        # Cap headroom is exhausted for a second call…
        assert not spend.reserve(conn).admitted
        # …until the un-issued first reservation is released.
        spend.release(first)
        second = spend.reserve(conn)
        assert second.admitted
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.deployment_cost_ledger "
                "WHERE scope = %s", (scope,),
            )
            assert cur.fetchone()[0] == 0


def test_bind_test_context_still_pins_the_test_workflow_id(test_db_dsn):
    """The test wrapper keeps its stable pin so existing assertions on
    provenance columns stay deterministic."""
    from okg.deployment._internal.context import (
        bind_test_context, current_context,
    )

    with psycopg.connect(test_db_dsn) as conn:
        with bind_test_context(deployment="test", conn=conn):
            assert current_context().workflow_id == "test-wf"
