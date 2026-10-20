"""Rotating-floor Chronos index regression and byte-identity oracles."""
from __future__ import annotations

import json
from pathlib import Path
import uuid

import psycopg
import pytest
from psycopg import sql

from okg.substrate.db.cold_history import (
    _full_index_name,
    cold_history_health_report,
    cold_history_report,
    hot_index_name,
    rotate_hot_indexes,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.profiling.context import profiling_context
from okg.substrate.profiling.policy import resolve_policy
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _reset_prior_rotation(dsn: str) -> None:
    """Restore full indexes when this reusable test DB ran the test before."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        if conn.execute(
            "SELECT to_regclass('okg.chronos_hot_index_manifest')"
        ).fetchone()[0] is None:
            return
        hot_names = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT hot_index_name "
                "FROM okg.chronos_hot_index_manifest"
            ).fetchall()
        ]
        conn.execute(
            "UPDATE okg.chronos_hot_index_manifest SET state = 'retired' "
            "WHERE state <> 'retired'"
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.ensure_interval_storage_upgrade()
        for hot_name in hot_names:
            conn.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier(hot_name)
                )
            )
        conn.execute("TRUNCATE okg.chronos_hot_index_manifest")


def _rows(store: ChronosGraphStore, generation: str) -> bytes:
    rows = store.checkout_generation(generation).query(
        """
        SELECT edge_id, src, dst, edge_type, attrs, source
          FROM okg.graph_edges
         ORDER BY edge_id
        """
    )
    return json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()


def test_cold_history_compression_preserves_asof_and_fork_reads(
    clean_runtime: str,
) -> None:
    _reset_prior_rotation(clean_runtime)
    suffix = uuid.uuid4().hex[:8]
    left, right = f"doc:{suffix}:left", f"doc:{suffix}:right"
    checkpoints: list[str] = []
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(left, "document", {"v": 1}, {"id": left}, {"v": 1}),
                NodeFact(right, "document", {"v": 1}, {"id": right}, {"v": 1}),
                EdgeFact(
                    left,
                    right,
                    "related",
                    attrs={"v": 1},
                    source_record_id={"id": "edge"},
                    source_revision={"v": 1},
                ),
            ],
            source="fixture",
            strict_endpoints=True,
        )
        for ordinal in range(3):
            checkpoints.append(
                store.checkpoint_generation(
                    f"cold-history-{suffix}-{ordinal}",
                    deployment_name="cold-history-test",
                    status="published",
                )
            )
        store.create_branch(
            f"experiment:{suffix}",
            from_branch="default",
            metadata={"base_generation_id": checkpoints[-1]},
        )
        before = {checkpoint: _rows(store, checkpoint) for checkpoint in checkpoints}
        fork_before = json.dumps(
            store.checkout(f"experiment:{suffix}").query(
                "SELECT edge_id, src, dst, edge_type, attrs, source "
                "FROM okg.graph_edges ORDER BY edge_id"
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()

    baseline = cold_history_report(clean_runtime, headroom_generations=1)
    floor = int(baseline["floor"]["floor"])
    assert floor != int(baseline["floor"]["open_interval_value"])
    edge_table = next(
        row["physical_table"] for row in baseline["indexes"]
        if row["logical_table"] == "okg.graph_edges"
    )
    full_name = _full_index_name(edge_table, "edge_identity_visible")
    hot_name = hot_index_name(edge_table, "edge_identity_visible", floor)

    proof = rotate_hot_indexes(
        clean_runtime,
        headroom_generations=1,
        logical_tables=["okg.graph_edges"],
        suffixes=["edge_identity_visible"],
        activate=False,
    )
    assert proof["indexes"][0]["state"] == "proof"
    with psycopg.connect(clean_runtime) as conn:
        assert conn.execute("SELECT to_regclass(%s)", (full_name,)).fetchone()[0]
        assert conn.execute("SELECT to_regclass(%s)", (hot_name,)).fetchone()[0]

    activated = rotate_hot_indexes(
        clean_runtime,
        headroom_generations=1,
        logical_tables=["okg.graph_edges"],
        suffixes=["edge_identity_visible"],
        activate=True,
    )
    assert activated["indexes"][0]["state"] == "active"
    health = cold_history_health_report(clean_runtime)
    assert health["ok"] is True
    assert health["active_index_count"] == 1

    repeated = rotate_hot_indexes(
        clean_runtime,
        headroom_generations=1,
        logical_tables=["okg.graph_edges"],
        suffixes=["edge_identity_visible"],
        activate=False,
    )
    assert repeated["indexes"][0]["state"] == "active"
    assert repeated["indexes"][0]["already_active"] is True

    # A later floor-specific definition replaces and physically retires the
    # prior hot index only after the new definition is valid/authoritative.
    replacement = rotate_hot_indexes(
        clean_runtime,
        headroom_generations=2,
        logical_tables=["okg.graph_edges"],
        suffixes=["edge_identity_visible"],
        activate=True,
    )
    replacement_row = replacement["indexes"][0]
    assert replacement_row["state"] == "active"
    assert replacement_row["retired_hot_indexes"] == [hot_name]
    retired_hot_name = hot_name
    hot_name = replacement_row["hot_index_name"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute("SELECT to_regclass(%s)", (full_name,)).fetchone()[0] is None
        assert conn.execute("SELECT to_regclass(%s)", (hot_name,)).fetchone()[0]
        assert (
            conn.execute(
                "SELECT to_regclass(%s)", (retired_hot_name,)
            ).fetchone()[0]
            is None
        )

        # Reconciliation validates the active hot definition and must not
        # recreate the old full index non-concurrently.
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.ensure_interval_storage_upgrade()
        assert conn.execute("SELECT to_regclass(%s)", (full_name,)).fetchone()[0] is None
        assert {checkpoint: _rows(store, checkpoint) for checkpoint in checkpoints} == before
        fork_after = json.dumps(
            store.checkout(f"experiment:{suffix}").query(
                "SELECT edge_id, src, dst, edge_type, attrs, source "
                "FROM okg.graph_edges ORDER BY edge_id"
            ),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        assert fork_after == fork_before

        conn.execute(
            """
            UPDATE okg.chronos_hot_index_manifest
               SET open_interval_value = open_interval_value + 1
             WHERE state = 'active'
            """
        )
    broken_health = cold_history_health_report(clean_runtime)
    assert broken_health["ok"] is False
    assert "chronos_open_interval_value_drift" in {
        row["code"] for row in broken_health["findings"]
    }


def test_cold_history_rejects_unknown_suffix_before_mutation(
    clean_runtime: str,
) -> None:
    with pytest.raises(Exception, match="unknown or non-rotatable"):
        rotate_hot_indexes(
            clean_runtime,
            headroom_generations=1,
            suffixes=["edge_not_real"],
        )


def _latest_published_counts(dsn: str, deployment: str) -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest = store.latest_generation(
            deployment_name=deployment,
            status="published",
        )
    assert latest is not None
    metadata = dict(latest.metadata or {})
    return {
        "node_count": int(metadata["node_count"]),
        "edge_count": int(metadata["edge_count"]),
    }


def test_cold_history_publish_query_count_flat_at_10_and_900_generations(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OKG_PROFILE_DIR", str(tmp_path / "profiles"))
    # Exercise the same fixed-cost periodic integrity stages at both history
    # depths; otherwise ordinal 900 naturally lands on the 100-generation IVM
    # reconciliation while ordinal 10 does not.
    monkeypatch.setenv("OKG_IVM_INTERVAL_RECONCILE_EVERY", "1")
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")
    _reset_prior_rotation(clean_runtime)
    deployment = f"cold-history-count-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
    publisher = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    publisher.cycle()  # Warm one-time schema/metadata setup outside the gate.
    # A publish checks its predecessor's recorded graph counts against the
    # effective-attribute intervals (the interval backfill guard). The
    # synthetic history generations below change no graph rows, so each one
    # records the warm publish's counts, as a publisher-made generation would.
    counts = _latest_published_counts(clean_runtime, deployment)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for ordinal in range(9):
            store.checkpoint_generation(
                f"cold-history-count-{deployment}-small-{ordinal}",
                deployment_name=deployment,
                status="published",
                metadata=counts,
            )
    physical_tables = {
        row["physical_table"]
        for row in cold_history_report(
            clean_runtime,
            headroom_generations=1,
        )["indexes"]
    }

    def measured_cycle() -> tuple[int, int, dict[str, object]]:
        with profiling_context(
            deployment=deployment,
            target_kind="publish",
            command="cold_history_query_count_gate",
            policy=resolve_policy(None),
            force_deep=True,
        ) as profile:
            outcome = publisher.cycle()
        segment_query_count = sum(
            1
            for query in profile.queries
            if any(
                physical_table in str(query.sql_shape or "")
                for physical_table in physical_tables
            )
        )
        return profile.query_count, segment_query_count, outcome

    control_total, control_count, control = measured_cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        # Warm publish + nine explicit checkpoints + measured control = 11;
        # 889 more yields a store with 900 retained published generations.
        for ordinal in range(889):
            store.checkpoint_generation(
                f"cold-history-count-{deployment}-large-{ordinal}",
                deployment_name=deployment,
                status="published",
                metadata=counts,
            )
    history_total, history_count, history = measured_cycle()

    rotate_hot_indexes(
        clean_runtime,
        headroom_generations=100,
        activate=True,
    )
    # Advance the retained lineage once so the post-rotation cycle exercises
    # the same publish path instead of legitimately taking no-op reuse.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.checkpoint_generation(
            f"cold-history-count-{deployment}-rotated-trigger",
            deployment_name=deployment,
            status="published",
            metadata=counts,
        )
    rotated_total, rotated_count, rotated = measured_cycle()

    assert control_count > 0
    assert history_count == control_count
    assert rotated_count == control_count
    assert history_total == control_total
    assert rotated_total == control_total
    # Total profiling counts include fixed-cost telemetry/manifest work outside
    # the graph segments. Pin those too for context; the design's authoritative
    # count gate is the SQL filtered to the two resolved physical segment names.
    assert control_total >= control_count
    for outcome in (control, history, rotated):
        assert outcome["status"] in {"published", "no_op"}
        assert outcome["nodes_added"] == 0
        assert outcome["nodes_retracted"] == 0
        assert outcome["edges_added"] == 0
        assert outcome["edges_retracted"] == 0

    _reset_prior_rotation(clean_runtime)


def test_cold_history_rejects_same_name_with_wrong_columns(
    clean_runtime: str,
) -> None:
    _reset_prior_rotation(clean_runtime)
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["document"],
            narrowings=[("document", "related", "document")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    f"doc:{suffix}",
                    "document",
                    {"v": 1},
                    {"id": suffix},
                    {"v": 1},
                ),
            ],
            source="fixture",
            strict_endpoints=True,
        )
        for ordinal in range(3):
            store.checkpoint_generation(
                f"cold-history-definition-{suffix}-{ordinal}",
                deployment_name="cold-history-test",
                status="published",
            )

    baseline = cold_history_report(clean_runtime, headroom_generations=1)
    floor = int(baseline["floor"]["floor"])
    edge_table = next(
        row["physical_table"]
        for row in baseline["indexes"]
        if row["logical_table"] == "okg.graph_edges"
    )
    full_name = _full_index_name(edge_table, "edge_identity_visible")
    hot_name = hot_index_name(edge_table, "edge_identity_visible", floor)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            sql.SQL(
                "CREATE INDEX {} ON public.{} (src, live_lo, live_hi, edge_id) "
                "WHERE deleted = FALSE AND live_hi > {}"
            ).format(
                sql.Identifier(hot_name),
                sql.Identifier(edge_table),
                sql.Literal(floor),
            )
        )

    with pytest.raises(
        SubstrateError,
        match="existing hot visibility index has the wrong definition",
    ):
        rotate_hot_indexes(
            clean_runtime,
            headroom_generations=1,
            logical_tables=["okg.graph_edges"],
            suffixes=["edge_identity_visible"],
            activate=True,
        )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT to_regclass(%s)", (full_name,)
        ).fetchone()[0]
        conn.execute(
            sql.SQL("DROP INDEX CONCURRENTLY {}").format(
                sql.Identifier(hot_name)
            )
        )

    activated = rotate_hot_indexes(
        clean_runtime,
        headroom_generations=1,
        logical_tables=["okg.graph_edges"],
        suffixes=["edge_identity_visible"],
        activate=True,
    )
    assert activated["indexes"][0]["state"] == "active"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP INDEX CONCURRENTLY {}").format(
                sql.Identifier(hot_name)
            )
        )
        conn.execute(
            sql.SQL(
                "CREATE INDEX {} ON public.{} (src, live_lo, live_hi, edge_id) "
                "WHERE deleted = FALSE AND live_hi > {}"
            ).format(
                sql.Identifier(hot_name),
                sql.Identifier(edge_table),
                sql.Literal(floor),
            )
        )

    health = cold_history_health_report(clean_runtime)
    assert health["ok"] is False
    assert "chronos_hot_index_manifest_mismatch" in {
        row["code"] for row in health["findings"]
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(
            SubstrateError,
            match="active Chronos hot index is absent or invalid",
        ):
            store = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            )
            store.ensure_interval_storage_upgrade()

    _reset_prior_rotation(clean_runtime)
