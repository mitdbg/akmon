from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import logging
import re
import threading
import time

import psycopg
import pytest
from psycopg import sql

from okg.substrate.cli.parser_groups import register_generation_parser
from okg.substrate.cli.generation import _cmd_generation_compact
from okg.substrate.errors import GenerationCompacted, SubstrateError
from okg.substrate.generation_resolver import resolve_okg_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal.common import EDGE_TABLE, NODE_TABLE
from okg.substrate.graph_store_internal.generation_ordinal import (
    resolve_generation_ordinal,
)
from okg.substrate.history_compaction import (
    _binding_drift,
    _history_protection_lock,
    apply_history_compaction_plan,
    check_history_retention,
    create_history_compaction_plan,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.runtime.source_sync_priority import (
    publisher_publish_advisory_keys,
)
from tests.pytest.catalog_fixture_builders import seed_catalog


ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.postgres


def test_retention_compaction_formal_refinement_files_are_bound() -> None:
    module = ROOT / "formal/chronos/ChronosRetentionCompaction.tla"
    checker = ROOT / "scripts/check_formal_chronos_retention_compaction.py"

    model = module.read_text(encoding="utf-8")
    checker_text = checker.read_text(encoding="utf-8")

    assert "CurrentAndProtectedReadsPreserved" in model
    assert "PublishAndCompactionMutuallyExclusive" in model
    assert "PlanBindingsFreshWhileMutating" in model
    assert "TableBatchesAreAtomicAndOrdered" in model
    assert "HistoricalReadsAreCompleteOrRefused" in model
    assert "RetiredGenerationRefusalIsStable" in model
    assert "ChronosRetentionCompactionStalePlanCounterexample.cfg" in checker_text
    assert "ChronosRetentionCompactionReadInterleavingCounterexample.cfg" in checker_text
    assert "expect_counterexample=True" in checker_text


def test_compaction_cli_requires_exact_plan_confirmation() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    register_generation_parser(subparsers, add_json_flag=lambda value: None)

    dry_run = parser.parse_args(["generation", "compact", "--deployment", "demo"])
    assert dry_run.apply is False
    assert dry_run.confirm is None
    apply = parser.parse_args(
        ["generation", "compact", "--deployment", "demo", "--apply", "--confirm", "plan-id"]
    )
    assert apply.apply is True
    assert apply.confirm == "plan-id"


def test_cli_rejects_offline_reclaim_before_any_logical_apply(capsys: pytest.CaptureFixture[str]) -> None:
    import argparse

    result = _cmd_generation_compact(
        argparse.Namespace(
            deployment="demo",
            apply=True,
            confirm="plan-id",
            offline_reclaim=True,
            workers_stopped=False,
        ),
        as_json=True,
    )
    assert result == 1
    assert "requires --workers-stopped before compaction starts" in capsys.readouterr().err


def test_resume_accepts_only_its_own_tombstone_vector_change() -> None:
    plan = {
        "database_identity": {"dbname": "test"},
        "deployment_name": "demo",
        "branch": "default",
        "policy_digest": "sha256:policy",
        "head_checkpoint": "g3",
        "checkpoint_vector": ["g1", "g2", "g3"],
        "retired_checkpoints": ["g1"],
        "physical_tables": {"nodes": "n", "edges": "e"},
        "catalog_version_id": 7,
        "protection_set": ["g2", "g3"],
        "binding_digest": "sha256:original",
    }
    current = {
        **plan,
        "checkpoint_vector": ["g2", "g3"],
        "binding_digest": "sha256:after-tombstones",
    }

    assert _binding_drift(plan, current, progress={"tombstones": {"complete": True}}) == {}
    current["head_checkpoint"] = "g4"
    assert "head_checkpoint" in _binding_drift(
        plan, current, progress={"tombstones": {"complete": True}}
    )


def test_tombstone_first_apply_resumes_and_preserves_current_checkout(
    clean_runtime: str,
) -> None:
    deployment = "history-compaction-test"
    checkpoint_ids: list[str] = []
    with psycopg.connect(clean_runtime) as catalog_conn:
        # Seed the subtype this test writes facts under. `clean_runtime`
        # preserves the ontology catalog but the migrated catalog anchor
        # carries no subtypes, so looking one up here only ever succeeded
        # when an unrelated test happened to leave a `claim` subtype behind
        # in the persistent test database.
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for index in range(6):
            seed.apply_facts(
                [NodeFact("n:hot", "claim", {"revision": index}, {}, {})]
            )
            checkpoint_ids.append(
                seed.checkpoint_generation(
                    "100" if index == 0 else f"history-{index}",
                    deployment_name=deployment,
                    catalog_version_id=active_catalog,
                    metadata={"target_generation_id": 100 + index},
                )
            )
        current_before = seed.checkout_generation(checkpoint_ids[-1]).query(
            "SELECT node_id, attrs FROM okg.graph_nodes"
        )
    finally:
        seed.close()

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {"claim": "latest-only"},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()[0]
        physical_ident = sql.Identifier(*str(physical).split("."))
        rows_before = int(
            conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(physical_ident)
            ).fetchone()[0]
        )
        store.create_branch(
            "retention-child",
            metadata={"base_generation_id": checkpoint_ids[0]},
        )
        with pytest.raises(SubstrateError, match="child/fork branch exists"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )
        store.delete_branch("retention-child")
        conn.execute(
            """
            INSERT INTO okg.history_generation_pins
              (deployment_name, branch_id, pin_name, checkpoint_id)
            VALUES (%s, 'default', 'named-audit-pin', %s)
            """,
            (deployment, checkpoint_ids[0]),
        )
        conn.commit()
        with pytest.raises(SubstrateError, match="crosses protected generations"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )
        conn.execute(
            "DELETE FROM okg.history_generation_pins WHERE pin_name = 'named-audit-pin'"
        )
        conn.execute(
            """
            INSERT INTO okg.history_generation_pins
              (deployment_name, branch_id, pin_name, checkpoint_id, expires_at)
            VALUES (%s, 'default', 'active-reader-lease', %s, now() + interval '5 minutes')
            """,
            (deployment, checkpoint_ids[0]),
        )
        conn.commit()
        with pytest.raises(SubstrateError, match="crosses protected generations"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )
        conn.execute(
            "UPDATE okg.history_generation_pins SET expires_at = now() - interval '1 second' "
            "WHERE pin_name = 'active-reader-lease'"
        )
        conn.commit()
        missing_physical = conn.execute(
            sql.SQL(
                "UPDATE {} SET subtype = NULL WHERE ctid = "
                "(SELECT ctid FROM {} LIMIT 1) RETURNING node_id, live_lo"
            ).format(physical_ident, physical_ident)
        ).fetchone()
        conn.commit()
        with pytest.raises(SubstrateError, match="physical history is missing"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )
        conn.execute(
            sql.SQL("UPDATE {} SET subtype = 'claim' WHERE node_id = %s AND live_lo = %s").format(
                physical_ident
            ),
            missing_physical,
        )
        conn.commit()
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        assert plan["retired_checkpoints"] == checkpoint_ids[:-2]

        with pytest.raises(SubstrateError, match="different deployment"):
            apply_history_compaction_plan(
                conn,
                store=store,
                plan_id=plan["plan_id"],
                confirm=plan["plan_id"],
                apply=True,
                policy_raw=policy,
                expected_deployment_name="not-the-selected-deployment",
            )
        assert conn.execute(
            "SELECT count(*) FROM okg.compacted_generations WHERE plan_id = %s",
            (plan["plan_id"],),
        ).fetchone()[0] == 0

        reader = store.checkout_generation(checkpoint_ids[0])
        assert reader.query("SELECT node_id FROM okg.graph_nodes")
        with pytest.raises(SubstrateError, match="crosses protected generations"):
            apply_history_compaction_plan(
                conn,
                store=store,
                plan_id=plan["plan_id"],
                confirm=plan["plan_id"],
                apply=True,
                policy_raw=policy,
            )
        assert reader.query("SELECT node_id FROM okg.graph_nodes")
        reader.close()

        nodes_eligible = int(plan["logical_estimates"]["nodes"]["rows_eligible"])
        assert nodes_eligible > 2, plan["logical_estimates"]
        with pytest.raises(RuntimeError, match="committed nodes batch"):
            apply_history_compaction_plan(
                conn,
                store=store,
                plan_id=plan["plan_id"],
                confirm=plan["plan_id"],
                apply=True,
                policy_raw=policy,
                batch_size=2,
                crash_after_committed_batch="nodes",
            )
        partial = conn.execute(
            "SELECT progress->'nodes' FROM okg.history_compaction_plans WHERE plan_id = %s",
            (plan["plan_id"],),
        ).fetchone()[0]
        assert partial["complete"] is False
        assert 0 < int(partial["deleted"]) < nodes_eligible

        with pytest.raises(GenerationCompacted) as excinfo:
            resolve_okg_generation(
                conn,
                deployment_name=deployment,
                requested=checkpoint_ids[0],
            )
        assert excinfo.value.code == "generation_compacted"
        assert excinfo.value.data["plan_id"] == plan["plan_id"]
        for read in (
            lambda: store.get_generation(checkpoint_ids[0]),
            lambda: store.checkout_generation(checkpoint_ids[0]),
            lambda: store.get_generation(100),
            lambda: store.checkout_generation(100),
            lambda: resolve_generation_ordinal(store.context.db, checkpoint_ids[0]),
            lambda: resolve_generation_ordinal(store.context.db, 100),
        ):
            with pytest.raises(GenerationCompacted) as direct_exc:
                read()
            assert direct_exc.value.code == "generation_compacted"
        with pytest.raises(GenerationCompacted):
            store.resolve_generation(100, deployment_name=deployment)

        result = apply_history_compaction_plan(
            conn,
            store=store,
            plan_id=plan["plan_id"],
            confirm=plan["plan_id"],
            apply=True,
            policy_raw=policy,
            batch_size=2,
        )
        assert result["progress"]["tombstones"]["complete"] is True
        assert result["progress"]["nodes"] == {
            "complete": True,
            "deleted": nodes_eligible,
        }
        assert "effective" not in result["progress"]
        conn.execute(
            "INSERT INTO okg.history_generation_pins "
            "(deployment_name, branch_id, pin_name, checkpoint_id, expires_at) "
            "VALUES (%s, 'default', 'read:expired-cleanup-witness', %s, "
            "now() - interval '1 minute')",
            (deployment, checkpoint_ids[-1]),
        )
        conn.commit()
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("SET LOCAL ROLE app_ro")
            cur.execute(
                "SELECT okg.compacted_generation_record(%s, %s)",
                (checkpoint_ids[0], deployment),
            )
            assert cur.fetchone()[0]["checkpoint_id"] == checkpoint_ids[0]
            cur.execute(
                "SELECT okg.acquire_history_generation_read_lease(%s, 'default', "
                "'read:wrong-deployment', %s)",
                ("other-deployment", checkpoint_ids[-1]),
            )
            assert cur.fetchone()[0] is False
            cur.execute(
                "SELECT okg.acquire_history_generation_read_lease(%s, 'wrong-branch', "
                "'read:wrong-branch', %s)",
                (deployment, checkpoint_ids[-1]),
            )
            assert cur.fetchone()[0] is False
            cur.execute(
                "SELECT okg.acquire_history_generation_read_lease(%s, 'default', "
                "'read:app-ro-proof', %s)",
                (deployment, checkpoint_ids[-1]),
            )
            assert cur.fetchone()[0] is True
            cur.execute(
                "SELECT okg.release_history_generation_read_lease(%s, 'default', "
                "'read:app-ro-proof')",
                (deployment,),
            )
            assert cur.fetchone()[0] is True
        assert conn.execute(
            "SELECT count(*) FROM okg.history_generation_pins "
            "WHERE pin_name = 'read:expired-cleanup-witness'"
        ).fetchone()[0] == 0
        assert store.checkout_generation(checkpoint_ids[-1]).query(
            "SELECT node_id, attrs FROM okg.graph_nodes"
        ) == current_before
        rows_after = int(
            conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(physical_ident)
            ).fetchone()[0]
        )
        assert rows_after < rows_before
        other_checkpoint = store.checkpoint_generation(
            "other-deployment-live-target-100",
            deployment_name="other-deployment",
            catalog_version_id=active_catalog,
            metadata={"target_generation_id": 100},
        )
        assert (
            store.resolve_generation(
                100,
                deployment_name="other-deployment",
            ).checkpoint_id
            == other_checkpoint
        )


def test_branch_creation_refuses_while_compaction_protection_lock_is_held(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime) as lock_conn, psycopg.connect(clean_runtime) as branch_conn:
        store = ChronosGraphStore.from_connection(branch_conn, ensure_schema=False)
        with _history_protection_lock(lock_conn):
            with pytest.raises(SubstrateError, match="protection state is being changed"):
                store.create_branch("racing-retention-child")


def test_raw_branch_from_checkpoint_cannot_cross_tombstone_barrier(
    clean_runtime: str,
) -> None:
    deployment = "history-compaction-raw-branch-race"
    with psycopg.connect(clean_runtime) as seed_conn:
        catalog = seed_catalog(seed_conn, subtypes=["claim"])
        seed_store = ChronosGraphStore.from_connection(
            seed_conn, ensure_schema=False
        )
        checkpoints: list[str] = []
        for revision in range(3):
            seed_store.apply_facts(
                [NodeFact("n:raw-race", "claim", {"revision": revision}, {}, {})]
            )
            checkpoints.append(
                seed_store.checkpoint_generation(
                    f"raw-race-{revision}",
                    deployment_name=deployment,
                    catalog_version_id=catalog,
                )
            )
        policy = {
            "default": "latest-only",
            "audit_tail_generations": 1,
            "nodes": {"claim": "latest-only"},
            "edges": {},
        }
        plan = create_history_compaction_plan(
            seed_conn,
            store=seed_store,
            deployment_name=deployment,
            policy_raw=policy,
        )

    with (
        psycopg.connect(clean_runtime) as apply_conn,
        psycopg.connect(clean_runtime) as branch_conn,
        psycopg.connect(clean_runtime) as probe_conn,
    ):
        apply_store = ChronosGraphStore.from_connection(
            apply_conn, ensure_schema=False
        )
        raw_store = ChronosGraphStore.from_connection(
            branch_conn, ensure_schema=False
        )
        branch_errors: list[Exception] = []

        def create_raw_branch() -> None:
            try:
                raw_store.context.create_branch_from_checkpoint(
                    "raw-race-child", checkpoints[0]
                )
            except Exception as exc:  # noqa: BLE001 - asserted below.
                branch_errors.append(exc)

        worker = threading.Thread(target=create_raw_branch)
        with _history_protection_lock(apply_conn):
            worker.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                waiting = probe_conn.execute(
                    "SELECT wait_event_type = 'Lock' AND wait_event = 'advisory' "
                    "FROM pg_stat_activity "
                    "WHERE pid = %s",
                    (branch_conn.info.backend_pid,),
                ).fetchone()
                if waiting is not None and bool(waiting[0]):
                    break
                time.sleep(0.01)
            else:
                pytest.fail("raw branch creation never waited on the protection lock")
            with pytest.raises(RuntimeError, match="after tombstones"):
                apply_history_compaction_plan(
                    apply_conn,
                    store=apply_store,
                    plan_id=plan["plan_id"],
                    confirm=plan["plan_id"],
                    apply=True,
                    policy_raw=policy,
                    crash_after_batch="tombstones",
                )
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert len(branch_errors) == 1
        assert "checkpoint" in str(branch_errors[0])
        assert "raw-race-child" not in {
            branch.branch_id for branch in raw_store.context.list_branches()
        }


def test_generation_metadata_postcheck_converts_delete_race_to_tombstone(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = "history-compaction-metadata-race"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        catalog = seed_catalog(conn, subtypes=["claim"])
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkpoints: list[str] = []
        for revision in range(3):
            store.apply_facts(
                [NodeFact("n:metadata-race", "claim", {"revision": revision}, {}, {})]
            )
            checkpoints.append(
                store.checkpoint_generation(
                    f"metadata-race-{revision}",
                    deployment_name=deployment,
                    catalog_version_id=catalog,
                )
            )
        policy = {
            "default": "latest-only",
            "audit_tail_generations": 1,
            "nodes": {"claim": "latest-only"},
            "edges": {},
        }
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        original_get = store.context.get_checkpoint
        raced = False

        def compact_between_check_and_use(checkpoint: str):
            nonlocal raced
            if not raced:
                raced = True
                with psycopg.connect(clean_runtime) as apply_conn:
                    apply_store = ChronosGraphStore.from_connection(
                        apply_conn, ensure_schema=False
                    )
                    with pytest.raises(RuntimeError, match="after tombstones"):
                        apply_history_compaction_plan(
                            apply_conn,
                            store=apply_store,
                            plan_id=plan["plan_id"],
                            confirm=plan["plan_id"],
                            apply=True,
                            policy_raw=policy,
                            crash_after_batch="tombstones",
                        )
            return original_get(checkpoint)

        monkeypatch.setattr(
            store.context,
            "get_checkpoint",
            compact_between_check_and_use,
        )
        with pytest.raises(GenerationCompacted):
            store.get_generation(checkpoints[0])


def test_generation_ordinal_postcheck_converts_success_race_to_tombstone() -> None:
    class Result:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class InterleavingDb:
        tombstoned = False

        def execute(self, query: str, _params=None):
            if "to_regclass('okg.compacted_generations')" in query:
                return Result({"relation": "okg.compacted_generations"})
            if "to_regprocedure" in query:
                return Result({"procedure": "okg.compacted_generation_record"})
            if "compacted_generation_record" in query:
                return Result(
                    {
                        "record": (
                            {
                                "checkpoint_id": "generation:ordinal-race",
                                "deployment_name": "race",
                                "branch": "default",
                                "plan_id": "race-plan",
                            }
                            if self.tombstoned
                            else None
                        )
                    }
                )
            if "to_regclass('okg.generation_ordinals')" in query:
                return Result({"t": "okg.generation_ordinals"})
            if "SELECT ordinal FROM okg.generation_ordinals" in query:
                self.tombstoned = True
                return Result({"ordinal": 7})
            raise AssertionError(query)

    with pytest.raises(GenerationCompacted):
        resolve_generation_ordinal(InterleavingDb(), "generation:ordinal-race")


def test_apply_refuses_an_active_publisher_before_tombstoning(clean_runtime: str) -> None:
    deployment = "history-compaction-publisher-race"
    with psycopg.connect(clean_runtime) as conn:
        catalog = seed_catalog(conn, subtypes=["claim"])
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for revision in range(3):
            store.apply_facts(
                [NodeFact("n:publisher-race", "claim", {"revision": revision}, {}, {})]
            )
            store.checkpoint_generation(
                f"publisher-race-{revision}",
                deployment_name=deployment,
                catalog_version_id=catalog,
            )
        policy = {
            "default": "latest-only",
            "audit_tail_generations": 1,
            "nodes": {"claim": "latest-only"},
            "edges": {},
        }
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )

    with (
        psycopg.connect(clean_runtime) as publisher_conn,
        psycopg.connect(clean_runtime) as apply_conn,
    ):
        classid, objid = publisher_publish_advisory_keys(deployment)
        publisher_conn.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
        apply_store = ChronosGraphStore.from_connection(apply_conn, ensure_schema=False)
        try:
            with pytest.raises(SubstrateError, match="publisher lock is held"):
                apply_history_compaction_plan(
                    apply_conn,
                    store=apply_store,
                    plan_id=plan["plan_id"],
                    confirm=plan["plan_id"],
                    apply=True,
                    policy_raw=policy,
                    publisher_lock_wait_seconds=0,
                )
            assert apply_conn.execute(
                "SELECT count(*) FROM okg.compacted_generations WHERE plan_id = %s",
                (plan["plan_id"],),
            ).fetchone()[0] == 0
        finally:
            publisher_conn.execute(
                "SELECT pg_advisory_unlock(%s, %s)", (classid, objid)
            )


def test_compaction_ignores_abandoned_branches_but_not_live_ones(
    clean_runtime: str,
) -> None:
    """An abandoned branch must not block compaction; a live one still must (#1306).

    `okg branch abandon` only stamps `status: abandoned` into the branch
    metadata -- the row survives in `_chronos_branch_interval_branches`. The
    protection guard used to reject every branch other than main/target
    regardless of status, so one `okg deployment fork-worktree` disabled
    history compaction for the deployment permanently, with the error text
    telling the operator to do the thing that does not work.
    """
    deployment = "history-compaction-abandoned-branch"
    fork = "experiment:abandoned-fork"
    checkpoint_ids: list[str] = []
    with psycopg.connect(clean_runtime) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for index in range(6):
            seed.apply_facts(
                [NodeFact("n:abandoned", "claim", {"revision": index}, {}, {})]
            )
            checkpoint_ids.append(
                seed.checkpoint_generation(
                    f"abandoned-{index}",
                    deployment_name=deployment,
                    catalog_version_id=active_catalog,
                    metadata={"target_generation_id": 300 + index},
                )
            )
    finally:
        seed.close()

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {"claim": "latest-only"},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.create_branch(
            fork,
            metadata={
                "kind": "okg_branch",
                "deployment_name": deployment,
                "branch_name": fork,
                "role": "experiment",
                "status": "active",
                "base_generation_id": checkpoint_ids[0],
            },
        )
        conn.commit()

        # A LIVE fork still reads the parent's history, so it still refuses.
        with pytest.raises(SubstrateError, match="child/fork branch exists"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )

        # Abandon it the way the CLI does: metadata only, row untouched.
        metadata = dict(store.context.get_branch(fork).metadata or {})
        metadata["status"] = "abandoned"
        metadata["closed_at"] = "2026-08-01T00:00:00+00:00"
        store.context.update_branch_metadata(fork, metadata)
        conn.commit()
        assert conn.execute(
            "SELECT 1 FROM _chronos_branch_interval_branches WHERE branch_id = %s",
            (fork,),
        ).fetchone() is not None, "abandon is metadata-only by design"

        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        # The plan says out loud which tombstoned branches it walked past.
        assert plan["ignored_abandoned_branches"] == [fork]
        assert plan["retired_checkpoints"], (
            "compaction must actually plan retirements, not pass vacuously"
        )
        # Diagnostics only: apply must not re-derive this as binding drift.
        assert "ignored_abandoned_branches" not in _binding_drift(
            plan, dict(plan), progress={}
        )


def test_node_deletes_are_scoped_to_the_compacted_branch(
    clean_runtime: str,
) -> None:
    """Node history deletes must remove this branch's rows and only this branch's.

    The physical node/edge interval tables are shared by every Chronos branch
    and carry no branch column, so `live_hi <= point` is not on its own a
    branch-local predicate: `point` comes from the compacted branch's
    checkpoints, but rows written by another branch can satisfy it. Branch
    `main` is the live case -- `_protection_set` permits it to coexist and
    `_checkpoints` never enumerates its checkpoints, so they are neither
    protected nor tombstoned and a read there would silently come back short.

    Both directions are asserted. Deleting nothing is a safe failure but still
    a broken feature, and no other test asserts a node row is ever removed.
    """
    deployment = "history-compaction-branch-scope"
    checkpoint_ids: list[str] = []
    with psycopg.connect(clean_runtime) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for index in range(6):
            seed.apply_facts(
                [NodeFact("n:scoped", "claim", {"revision": index}, {}, {})]
            )
            checkpoint_ids.append(
                seed.checkpoint_generation(
                    f"scope-{index}",
                    deployment_name=deployment,
                    catalog_version_id=active_catalog,
                    metadata={"target_generation_id": 200 + index},
                )
            )
    finally:
        seed.close()

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {"claim": "latest-only"},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()[0]
        physical_ident = sql.Identifier(*str(physical).split("."))

        # Plant a row owned by `main`, positioned so the retirement predicate
        # (`live_hi <= point`) matches it. Only the segment scope keeps it.
        main_segment = conn.execute(
            "SELECT current_segment_id FROM _chronos_branch_interval_branches "
            "WHERE branch_id = 'main'"
        ).fetchone()[0]
        conn.execute(
            sql.SQL(
                "INSERT INTO {} (node_id, subtype, attrs, live_lo, live_hi, "
                "writer_segment_id, deleted) "
                "VALUES ('n:main-owned', 'claim', '{{}}'::jsonb, 0, 1, %s, false)"
            ).format(physical_ident),
            (main_segment,),
        )
        conn.commit()

        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        # The plan must SHOW the operator what branch scoping is holding back.
        # A deployment upgraded in place has all its pre-existing rows stamped
        # with the bootstrap segment, which `main` owns, so compacting
        # `default` reclaims none of it. Under-delivering on a delete is safe;
        # under-delivering silently is not.
        node_estimates = plan["logical_estimates"]["nodes"]
        assert int(node_estimates["rows_eligible_ignoring_branch_scope"]) >= 1
        assert int(node_estimates["rows_branch_total"]) <= int(
            node_estimates["rows_total"]
        )

        result = apply_history_compaction_plan(
            conn,
            store=store,
            plan_id=plan["plan_id"],
            confirm=plan["plan_id"],
            apply=True,
            policy_raw=policy,
            expected_deployment_name=deployment,
        )
        assert result["progress"]["nodes"]["complete"] is True

        # The feature actually reclaims: this guards the segment predicate
        # against silently matching nothing.
        assert int(result["progress"]["nodes"]["deleted"]) > 0

        # ...and it stopped at the branch boundary.
        assert conn.execute(
            sql.SQL(
                "SELECT count(*) FROM {} WHERE node_id = 'n:main-owned'"
            ).format(physical_ident)
        ).fetchone()[0] == 1


def _statement_text(query: object, conn: psycopg.Connection) -> str:
    if isinstance(query, sql.Composable):
        return query.as_string(conn)
    if isinstance(query, bytes):
        return query.decode("utf-8", "replace")
    return str(query)


def _recording_cursor_factory(log: list[str]) -> type:
    """A cursor that records every statement text it is asked to run.

    `Connection.execute` builds its cursor through `cursor_factory`, so this
    captures the planner's whole statement inventory without threading a probe
    through production code.
    """

    class _RecordingCursor(psycopg.Cursor):
        def execute(self, query, params=None, **kwargs):  # type: ignore[override]
            log.append(_statement_text(query, self.connection))
            return super().execute(query, params, **kwargs)

    return _RecordingCursor


# The planner's exact count(*) inventory after #1308. It is a CONSTANT: two
# classifier-inventory NULL probes and ONE four-population pass per graph
# table. Nothing here scales with the number of subtypes or edge types, which
# is the whole point -- the old planner ran six more count(*) per classifier
# value and never finished on a 61 GB / 29.1M-row edge table. (okg#3112 S4
# dropped the effective-interval table, and with it the planner's five
# effective-table statements.)
_EXPECTED_PLANNER_COUNT_STATEMENTS = 4


@pytest.mark.parametrize("classifier_count", [4, 32])
def test_planner_count_statements_do_not_scale_with_classifier_cardinality(
    clean_runtime: str, classifier_count: int
) -> None:
    """One grouped pass per table, not one count(*) per classifier (#1308).

    On okg-workspace the dry-run planner held the publisher lease for 69
    minutes without producing a plan, sequential-scanning the edge interval
    table once per edge type with 41 connections queued behind it. The fix is
    a single grouped pass per table; this pins the statement inventory to a
    constant and pins the grouped result to the per-classifier result on the
    same data, so the speedup cannot come from counting something else.
    """
    deployment = f"history-compaction-grouped-{classifier_count}"
    subtypes = [f"claim{index:02d}" for index in range(classifier_count)]
    edge_types = [f"rel{index:02d}" for index in range(classifier_count)]
    with psycopg.connect(clean_runtime) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=subtypes)

    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for revision in range(6):
            facts: list[object] = []
            for subtype in subtypes:
                facts.append(
                    NodeFact(f"n:{subtype}", subtype, {"revision": revision}, {}, {})
                )
                facts.append(
                    NodeFact(
                        f"n:{subtype}:dst", subtype, {"revision": revision}, {}, {}
                    )
                )
            for index, edge_type in enumerate(edge_types):
                facts.append(
                    EdgeFact(
                        f"n:{subtypes[index]}",
                        f"n:{subtypes[index]}:dst",
                        edge_type,
                        {"revision": revision},
                    )
                )
            seed.apply_facts(facts)
            seed.checkpoint_generation(
                f"grouped-{revision}",
                deployment_name=deployment,
                catalog_version_id=active_catalog,
                metadata={"target_generation_id": 700 + revision},
            )
    finally:
        seed.close()

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {},
        "edges": {},
    }
    statements: list[str] = []
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        conn.cursor_factory = _recording_cursor_factory(statements)
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        conn.cursor_factory = psycopg.Cursor

        counts = [text for text in statements if "count(*)" in text.lower()]
        assert len(counts) == _EXPECTED_PLANNER_COUNT_STATEMENTS, (
            "planner count(*) inventory must not scale with classifier "
            f"cardinality ({classifier_count} classifiers): "
            + "\n".join(counts)
        )
        # Not vacuous: the plan really did classify this history as retirable.
        assert plan["logical_estimates"]["nodes"]["rows_eligible"] > 0
        assert plan["logical_estimates"]["edges"]["rows_eligible"] > 0

        # The grouped pass must agree with the per-classifier arithmetic it
        # replaced, on exactly this data.
        physical = plan["physical_tables"]
        for logical_name, table_key, classifier in (
            ("nodes", NODE_TABLE, "subtype"),
            ("edges", EDGE_TABLE, "edge_type"),
        ):
            reference = 0
            reference_unscoped = 0
            for value, floor in plan["floors"][logical_name].items():
                if floor["point"] is None:
                    continue
                reference += int(
                    conn.execute(
                        sql.SQL(
                            "SELECT count(*) FROM {} WHERE {} = %s AND live_hi <= %s "
                            "AND writer_segment_id IN ("
                            " SELECT segment_id FROM _chronos_branch_interval_segments"
                            "  WHERE owner_branch_id = %s)"
                        ).format(
                            sql.Identifier(physical[table_key]),
                            sql.Identifier(classifier),
                        ),
                        (value, int(floor["point"]), "default"),
                    ).fetchone()[0]
                )
                reference_unscoped += int(
                    conn.execute(
                        sql.SQL(
                            "SELECT count(*) FROM {} WHERE {} = %s AND live_hi <= %s"
                        ).format(
                            sql.Identifier(physical[table_key]),
                            sql.Identifier(classifier),
                        ),
                        (value, int(floor["point"])),
                    ).fetchone()[0]
                )
            estimate = plan["logical_estimates"][logical_name]
            assert estimate["rows_eligible"] == reference
            assert (
                estimate["rows_eligible_ignoring_branch_scope"]
                == reference_unscoped - reference
            )

    # Progress is durable: an operator can tell slow from stuck afterwards.
    phases = {entry["phase"] for entry in plan["planner_progress"]}
    assert {"estimate_nodes", "estimate_edges"} <= phases
    assert "estimate_effective" not in phases
    assert all(
        isinstance(entry["elapsed_seconds"], float)
        and entry["elapsed_seconds"] >= 0.0
        for entry in plan["planner_progress"]
    )


def test_planner_logs_per_phase_progress_with_elapsed_seconds(
    clean_runtime: str, caplog: pytest.LogCaptureFixture
) -> None:
    """69 minutes of silence is what made #1308 look like a hang.

    The planner now logs each phase start and finish with elapsed seconds, so
    `okg generation compact --progress` shows movement while the lease is held.
    """
    deployment = "history-compaction-progress-logs"
    with psycopg.connect(clean_runtime) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for revision in range(3):
            seed.apply_facts(
                [NodeFact("n:progress", "claim", {"revision": revision}, {}, {})]
            )
            seed.checkpoint_generation(
                f"progress-{revision}",
                deployment_name=deployment,
                catalog_version_id=active_catalog,
                metadata={"target_generation_id": 800 + revision},
            )
    finally:
        seed.close()

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with caplog.at_level(
            logging.INFO, logger="okg.substrate.history_compaction"
        ):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=policy,
            )
    messages = [record.getMessage() for record in caplog.records]
    started = [text for text in messages if "estimate_edges started" in text]
    finished = [text for text in messages if "estimate_edges finished" in text]
    assert started and finished, messages
    assert "elapsed=" in started[0]
    assert re.search(r"finished in \d+\.\ds", finished[0]), finished[0]


def test_grouped_counts_survive_interval_points_beyond_bigint(
    clean_runtime: str,
) -> None:
    """Chronos interval coordinates are NUMERIC(32,0), not bigint.

    `_chronos_branch_interval_segments.branch_point` is `numeric(32,0)`, and
    the `main` branch's continuation segments carry points around 5e30. Casting
    the floor array to `bigint[]` would raise `NumericValueOutOfRange` there,
    turning a slow plan into a crashing one on exactly the branch whose rows
    the plan's own output tells operators to look at. The per-classifier form
    this replaced passed each point as a bare parameter and had no such limit.
    """
    from okg.substrate.history_compaction import _grouped_table_counts

    deployment = "history-compaction-numeric-point"
    with psycopg.connect(clean_runtime) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(clean_runtime)
    try:
        for revision in range(3):
            seed.apply_facts(
                [NodeFact("n:numeric", "claim", {"revision": revision}, {}, {})]
            )
            seed.checkpoint_generation(
                f"numeric-{revision}",
                deployment_name=deployment,
                catalog_version_id=active_catalog,
                metadata={"target_generation_id": 1000 + revision},
            )
    finally:
        seed.close()

    huge = 5_000_000_000_000_001_581_138_830_084_190  # a real `main` point
    assert huge > 2**63 - 1
    with psycopg.connect(clean_runtime) as conn:
        physical = conn.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE backend = 'interval' AND table_name = 'okg.graph_nodes'"
        ).fetchone()[0]
        counts = _grouped_table_counts(
            conn,
            table=str(physical),
            classifier="subtype",
            floors={"claim": huge},
            branch="default",
        )
        reference = int(
            conn.execute(
                sql.SQL("SELECT count(*) FROM {} WHERE {} = %s AND live_hi <= %s").format(
                    sql.Identifier(str(physical)), sql.Identifier("subtype")
                ),
                ("claim", huge),
            ).fetchone()[0]
        )
    # A floor above every point retires the whole classifier, so this is not a
    # vacuous zero-equals-zero comparison.
    assert reference > 0
    assert counts["rows_eligible_any_branch"] == reference
    assert counts["rows_total"] >= reference
def _seed_retention_history(
    dsn: str, deployment: str, *, revisions: int = 6
) -> int:
    with psycopg.connect(dsn) as catalog_conn:
        active_catalog = seed_catalog(catalog_conn, subtypes=["claim"])
    seed = ChronosGraphStore.connect(dsn)
    try:
        for revision in range(revisions):
            seed.apply_facts(
                [NodeFact("n:retention", "claim", {"revision": revision}, {}, {})]
            )
            seed.checkpoint_generation(
                f"retention-{revision}",
                deployment_name=deployment,
                catalog_version_id=active_catalog,
                metadata={"target_generation_id": 900 + revision},
            )
    finally:
        seed.close()
    return active_catalog


def test_compaction_plans_with_a_printed_default_when_retention_is_unset(
    clean_runtime: str,
) -> None:
    """Undeclared retention must not disable the only reclaim lever (#1309).

    `okg generation compact` used to refuse with "chronos.history_retention
    must be a mapping" whenever the deployment declared nothing -- which is
    every deployment by default, so the lever was unavailable exactly when
    growth became a problem. It now plans against a documented built-in
    default and names it in the plan, which is the opposite of a silent
    fallback: the operator sees the rules before confirming a single delete.
    """
    deployment = "history-retention-unset"
    _seed_retention_history(clean_runtime, deployment)
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=None,
        )
    assert plan["policy_source"] == "built_in_default"
    assert plan["policy"]["audit_tail_generations"] == 50
    assert plan["policy"]["default"] == {"horizon_seconds": "1209600"}
    notice = plan["policy_notice"]
    assert "chronos.history_retention is not declared" in notice
    # The notice names the exact keys to add, not just the fact of a default.
    assert "history_retention:" in notice
    assert "audit_tail_generations: 50" in notice
    assert "horizon: 14d" in notice


def test_declared_retention_still_wins_and_a_broken_one_still_refuses(
    clean_runtime: str,
) -> None:
    """The default fills an ABSENCE only; a broken declaration is still fatal."""
    deployment = "history-retention-declared"
    _seed_retention_history(clean_runtime, deployment)
    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {"claim": "latest-only"},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        plan = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=policy,
        )
        assert plan["policy_source"] == "deployment"
        assert plan["policy_notice"] is None
        assert plan["policy"]["default"] == "latest-only"

        with pytest.raises(SubstrateError, match="must be a mapping"):
            create_history_compaction_plan(
                conn,
                store=store,
                deployment_name=deployment,
                policy_raw=["not", "a", "mapping"],  # type: ignore[arg-type]
            )


def test_doctor_probe_reports_growth_and_whether_retention_is_declared(
    clean_runtime: str,
) -> None:
    """`okg doctor` must say retention is unset before an incident says it.

    The refusal was silent in normal operation: nothing warned that history
    was unbounded until someone tried to compact. The probe reports the
    interval-table size and the superseded-row ratio in both states, and warns
    when nothing is declared.
    """
    deployment = "history-retention-doctor"
    _seed_retention_history(clean_runtime, deployment)
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        unset = check_history_retention(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw=None,
        )
        declared = check_history_retention(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw={"default": "latest-only", "audit_tail_generations": 2},
        )

    assert unset["status"] == "warn"
    assert unset["code"] == "history_retention_unset"
    assert unset["configured"] is False
    # Not vacuous: the seeded history really is mostly superseded.
    assert unset["tables"]["nodes"]["rows_total"] > 0
    assert unset["tables"]["nodes"]["rows_superseded"] > 0
    assert 0.0 < unset["superseded_row_ratio"] <= 1.0
    assert unset["interval_relation_bytes"] > 0
    assert "history_retention:" in unset["message"]

    assert declared["configured"] is True
    assert declared["deletion_authority"] == "confirmed_compaction_plan"
    assert declared["completed_compactions"] == 0
    # Declared, mostly-dead, and never compacted: that combination is itself a
    # warning, because nothing else deletes history.
    assert declared["status"] == "warn"
    assert declared["code"] == "history_retention_declared_but_never_applied"
    assert "only thing that deletes history" in declared["message"]
    assert "okg generation compact" in declared["message"]


def test_scaffold_ships_a_history_retention_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A new deployment must not default to unbounded history (#1309).

    Without `chronos.history_retention` every superseded node and edge version
    is kept forever; okg-workspace reached 88% dead history and 136 GB that
    way. The scaffold now ships the same shape that deployment runs, and says
    in the file that declaring it makes the confirmed compaction plan the sole
    deletion authority.

    This lives here rather than beside the other `okg deployment new` tests so
    the change does not have to enrol
    `tests/substrate/cli/test_deployment_new.py::test_generated_scaffold_can_publish_fixture_source`
    into a blocking lane: that test fails on current `dev` in a fresh database
    with `source privacy policy for 'bootstrap-fixture' failed Nomos audit`,
    and enrolling a failing test is not this change's business.
    """
    import yaml

    from okg.substrate.deployment_scaffold import create_deployment_scaffold
    from okg.substrate.history_compaction import (
        DEFAULT_HISTORY_RETENTION,
        parse_history_retention,
    )

    monkeypatch.delenv("OKG_DSN", raising=False)
    root = tmp_path / "deployments"
    result = create_deployment_scaffold("retention-scaffold", deployments_root=root)
    body = (result.deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    manifest = yaml.safe_load(body)
    retention = manifest["chronos"]["history_retention"]
    assert retention == dict(DEFAULT_HISTORY_RETENTION)
    # It is not just well-shaped text: the real parser accepts it.
    policy = parse_history_retention(
        retention, catalog_node_subtypes=set(), observed_edge_types=set()
    )
    assert policy.audit_tail_generations == 50
    assert policy.default.mode == "horizon"
    assert policy.default.horizon_seconds == 14 * 86400
    # The ON/OFF consequence is stated where the operator will read it.
    assert "turns OFF the publisher per-cycle" in body
    assert "sole deletion authority" in body


def test_unattended_apply_refuses_a_built_in_default(clean_runtime: str) -> None:
    """A schedule confirms its own plan, so nobody reads the notice (#1309).

    The CLI may apply a plan built from the built-in retention default: an
    operator read the dry run and typed the plan id back. The okg-workspace
    compaction schedule has neither of those, so for it an undeclared policy
    is a refusal, not a default.
    """
    from okg.substrate.history_compaction import unattended_apply_refusal

    deployment = "history-retention-unattended"
    _seed_retention_history(clean_runtime, deployment)
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        defaulted = create_history_compaction_plan(
            conn, store=store, deployment_name=deployment, policy_raw=None
        )
        declared = create_history_compaction_plan(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw={"default": "latest-only", "audit_tail_generations": 2},
        )
    refusal = unattended_apply_refusal(defaulted)
    assert refusal is not None
    assert "is not " in refusal and "declared" in refusal
    assert "built-in default" in refusal
    assert unattended_apply_refusal(declared) is None

    # The schedule that must honour it actually calls it.
    workflow = (
        ROOT / "deployments/okg-workspace/workflows/history_compaction.py"
    ).read_text(encoding="utf-8")
    assert "unattended_apply_refusal(plan)" in workflow
    assert '"applied": False' in workflow


def test_compact_cli_prints_the_default_notice_to_stderr(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The notice must reach the operator, not just the JSON payload.

    A plan is long; `policy_source` buried in it is easy to skim past. The
    command says out loud that it used rules the deployment never declared.
    """
    import argparse

    notice = "chronos.history_retention is not declared for this deployment; ..."
    plan = {
        "plan_id": "plan-1",
        "policy_source": "built_in_default",
        "policy_notice": notice,
    }

    def _fake_plan(*args: object, **kwargs: object) -> dict[str, object]:
        return plan

    monkeypatch.setattr(
        "okg.substrate.history_compaction.create_history_compaction_plan",
        _fake_plan,
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda name: SimpleNamespace(chronos={}),
    )
    monkeypatch.setattr(
        "okg.substrate.db.connection.connect",
        lambda dsn: _NullConnection(),
    )
    monkeypatch.setattr(
        "okg.substrate.graph_store.ChronosGraphStore.from_connection",
        classmethod(lambda cls, conn, **kwargs: object()),
    )
    monkeypatch.setattr(
        "okg.substrate.cli.main._resolve_dsn", lambda args: "postgres:///unused"
    )

    exit_code = _cmd_generation_compact(
        argparse.Namespace(
            deployment="demo",
            branch="default",
            apply=False,
            confirm=None,
            offline_reclaim=False,
            workers_stopped=False,
            progress=False,
        ),
        as_json=True,
    )
    captured = capsys.readouterr()
    assert exit_code == 0
    assert notice in captured.err
    assert "built_in_default" in captured.out


class _NullConnection:
    """The compact CLI only passes the connection through in this test."""

    def __enter__(self) -> _NullConnection:
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


def test_doctor_reports_history_retention_for_a_deployment(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg doctor` is where an operator learns history is growing (#1309).

    Checking the probe function alone would leave the wiring untested, and the
    wiring is the whole point: the refusal used to be silent in normal
    operation until someone tried to compact during an incident.
    """
    import argparse
    import json as _json

    from okg.substrate.cli.doctor import _cmd_doctor
    from okg.substrate.deployment_scaffold import create_deployment_scaffold

    root = tmp_path / "deployments"
    result = create_deployment_scaffold("doctor-retention", deployments_root=root)
    deployment = result.deployment_name
    _seed_retention_history(clean_runtime, deployment)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_DSN", clean_runtime)

    _cmd_doctor(
        argparse.Namespace(
            deployment=deployment, dsn=clean_runtime, check=None, json=True
        ),
        as_json=True,
    )
    report = _json.loads(capsys.readouterr().out)
    retention = report["history_retention"]
    # The scaffold declares a policy, so a confirmed compaction plan is the
    # only deletion authority and the doctor must say so.
    assert retention["configured"] is True
    assert retention["tables"]["nodes"]["rows_total"] > 0
    assert retention["status"] == "warn"
    assert retention["code"] == "history_retention_declared_but_never_applied"
    assert any(
        warning["code"] == "history_retention_declared_but_never_applied"
        for warning in report.get("warnings", [])
    )


def test_doctor_probe_is_quiet_while_no_history_is_dead(
    clean_runtime: str,
) -> None:
    """Issue #3578: a fresh install warned about retention with 0 of 4 rows
    dead. With nothing to retire there is nothing to warn about; the warning
    starts with the first superseded version (the test above)."""
    deployment = "history-retention-fresh"
    _seed_retention_history(clean_runtime, deployment, revisions=1)
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        unset = check_history_retention(
            conn, store=store, deployment_name=deployment, policy_raw=None,
        )
        declared = check_history_retention(
            conn,
            store=store,
            deployment_name=deployment,
            policy_raw={"default": "latest-only", "audit_tail_generations": 2},
        )

    for report in (unset, declared):
        assert report["tables"]["nodes"]["rows_total"] > 0
        assert report["tables"]["nodes"]["rows_branch_superseded"] == 0
        assert report["dead_history_rows"] == 0
        assert (report["status"], report["code"]) == ("ok", "history_retention_ok")
        assert "nothing needs retiring" in report["message"]
    # What the probe reports stays true: declaring still turns the prune off.
    assert declared["completed_compactions"] == 0
    assert "okg generation compact" in declared["message"]


def test_doctor_on_a_fresh_deployment_shows_none_of_the_3578_warnings(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #3578: on a deployment just scaffolded and published once,
    doctor called its own disk check misconfigured and warned about
    retention and an unacked worker on a deployment that runs no worker."""
    import argparse
    import json as _json

    from okg.substrate.cli.doctor import _cmd_doctor
    from okg.substrate.deployment_scaffold import create_deployment_scaffold

    root = tmp_path / "deployments"
    result = create_deployment_scaffold("doctor-fresh", deployments_root=root)
    deployment = result.deployment_name
    _seed_retention_history(clean_runtime, deployment, revisions=1)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_DSN", clean_runtime)

    _cmd_doctor(
        argparse.Namespace(
            deployment=deployment, dsn=clean_runtime, check=None, json=True
        ),
        as_json=True,
    )
    report = _json.loads(capsys.readouterr().out)
    codes = {warning["code"] for warning in report.get("warnings", [])}
    assert not codes & {
        "host_headroom_thresholds_undeclared",
        "history_retention_declared_but_never_applied",
        "history_retention_unset",
        "deployment_apply_pending_worker_ack",
    }, codes
    assert report["host_disk_headroom"]["status"] != "misconfigured"
    assert report["history_retention"]["status"] == "ok"


def test_doctor_reports_unacked_stages_without_a_warning_when_runtime_is_off(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #3578: `deployment_apply_health` lists an unacked worker stage
    as a warning only with the runtime off, where no worker ever runs.
    Doctor keeps the stages in its report and adds no warning; a problem
    (runtime on) still fails it."""
    import argparse
    import json as _json

    import okg.substrate.deployment_apply as deployment_apply
    from okg.substrate.cli.doctor import _cmd_doctor
    from okg.substrate.deployment_scaffold import create_deployment_scaffold

    root = tmp_path / "deployments"
    result = create_deployment_scaffold("doctor-unacked", deployments_root=root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_DSN", clean_runtime)

    def _run(health: dict) -> dict:
        monkeypatch.setattr(
            deployment_apply, "deployment_apply_health", lambda **_: health,
        )
        _cmd_doctor(
            argparse.Namespace(
                deployment=result.deployment_name, dsn=clean_runtime,
                check=None, json=True,
            ),
            as_json=True,
        )
        return _json.loads(capsys.readouterr().out)

    pending = _run({
        "ok": True, "status": "pending_worker_ack", "problems": [],
        "warnings": ["stage_needed:worker_reload"],
    })
    assert pending["deployment_apply"]["warnings"] == ["stage_needed:worker_reload"]
    assert "deployment_apply_pending_worker_ack" not in {
        w["code"] for w in pending.get("warnings", [])
    }
    assert "deployment_apply_not_converged" not in {
        e["code"] for e in pending["errors"]
    }

    blocked = _run({
        "ok": False, "status": "blocked", "warnings": [],
        "problems": ["stage_needed:worker_reload"],
        "next_command": "okg deployment apply x --json",
    })
    assert blocked["ok"] is False
    assert "deployment_apply_not_converged" in {e["code"] for e in blocked["errors"]}
