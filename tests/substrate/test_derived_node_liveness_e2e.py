"""Focused disposable publish proof for derived-node fixed-point cleanup."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
import pytest

from okg.substrate.consistency import TransformConsistencyGate
from okg.substrate.derived_node_liveness import (
    frontier_digest,
    read_frontier_summary,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.publisher import Publisher
from okg.substrate.publish_proof import dlq_count
from okg.substrate.runtime_dlq import current_runtime_dlq
from okg.substrate.source_retirement import (
    plan_source_retirement,
    remediate_source_retirement,
)
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres


def _derived(
    node_id: str,
    members: list[str],
    *,
    policy: str = "any_member",
    producer: str = "_enricher:fixture",
) -> NodeFact:
    canonical = sorted(set(members))
    return NodeFact(
        node_id,
        "thing",
        {},
        {
            "derived_from": canonical,
            "derived_support": {
                "schema": "okg.derived_support.v1",
                "producer": producer,
                "members": canonical,
                "policy": policy,
            },
        },
        {"v": 1},
    )


def _publisher(
    dsn: str,
    deployment: str,
    *,
    branch_name: str = "default",
) -> Publisher:
    return Publisher(
        dsn,
        deployment,
        branch_name=branch_name,
        consistency_gates=[TransformConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(dsn, deployment),
    )


def test_cleanup_publishes_fixed_point_and_orphan_blocks(
    clean_runtime: str,
) -> None:
    deployment = f"derived-node-liveness-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["thing"],
            narrowings=[("thing", "related_to", "thing")],
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact("input:a", "thing", {}, {"id": "a"}, {"v": 1}),
                NodeFact("input:b", "thing", {}, {"id": "b"}, {"v": 1}),
            ],
            branch="default",
            source="source:fixture",
        )
        store.apply_facts(
            [
                _derived("derived:any", ["input:a", "input:b"]),
                _derived("derived:a-only", ["input:a"]),
                _derived(
                    "derived:all",
                    ["input:a", "input:b"],
                    policy="all_members",
                ),
                _derived("derived:downstream", ["derived:a-only"]),
                _derived("derived:cycle-a", ["input:a", "derived:cycle-b"]),
                _derived("derived:cycle-b", ["derived:cycle-a"]),
                EdgeFact(
                    "derived:all",
                    "input:b",
                    "related_to",
                    provenance="derived_deterministic",
                    source_record_id={"id": "derived-edge"},
                    source_revision={"v": 1},
                ),
            ],
            branch="default",
            source="_enricher:fixture",
        )
        store.apply_facts(
            [NodeFact("curated:1", "thing", {}, {"id": "curated"}, {"v": 1})],
            branch="default",
            source="curated:operator",
        )
        store.apply_facts(
            [NodeFact("extractor:1", "thing", {}, {"id": "extractor"}, {"v": 1})],
            branch="default",
            source="_extractor:fixture",
        )

    baseline = _publisher(clean_runtime, deployment).cycle()
    assert baseline["status"] == "published", baseline
    baseline_generation = str(baseline["generation_id"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.apply_facts(
            [
                NodeFact(
                    "input:a",
                    "thing",
                    {},
                    {"id": "a"},
                    {"v": 2},
                    op="R",
                )
            ],
            branch="default",
            source="source:fixture",
        )

    cleaned = _publisher(clean_runtime, deployment).cycle()
    assert cleaned["status"] == "published", cleaned
    cleanup_details = cleaned["consistency_gates"][0]["details"][
        "derived_node_cleanup"
    ]
    assert cleanup_details["retracted_node_ids"] == [
        "derived:a-only",
        "derived:all",
        "derived:cycle-a",
        "derived:cycle-b",
        "derived:downstream",
    ]
    assert cleanup_details["retracted_incident_edge_count"] == 1
    assert cleanup_details["no_unsupported_derived_nodes"]["result"] == "clean"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        cleanup_generation = store.get_generation(str(cleaned["generation_id"]))
        cleanup_metadata = dict(cleanup_generation.metadata or {})
        mutation_receipt = cleanup_metadata["projection_deltas"][
            "consistency_gate_mutations"
        ]
        assert mutation_receipt == {
            "dangling_edges_pruned": 0,
            "orphaned_derived_edges_retracted": 0,
            "unsupported_derived_nodes_retracted": 5,
            "unsupported_derived_incident_edges_retracted": 1,
            "nodes_removed": 5,
            "edges_removed": 1,
        }
        current = store.checkout("default")
        assert {
            str(row["node_id"])
            for row in current.query("SELECT node_id FROM okg.graph_nodes")
        } == {"input:b", "derived:any", "curated:1", "extractor:1"}
        historical = store.checkout_generation(baseline_generation)
        historical_node_ids = {
            str(row["node_id"])
            for row in historical.query("SELECT node_id FROM okg.graph_nodes")
        }
        assert historical_node_ids == {
            "input:a",
            "input:b",
            "derived:any",
            "derived:a-only",
            "derived:all",
            "derived:cycle-a",
            "derived:cycle-b",
            "derived:downstream",
            "curated:1",
            "extractor:1",
        }

        with pytest.raises(SubstrateError, match="different producer"):
            store.apply_facts(
                [
                    _derived(
                        "derived:any",
                        ["input:b"],
                        producer="_enricher:other",
                    )
                ],
                branch="default",
                source="_enricher:other",
            )

        store.apply_facts(
            [_derived("derived:orphan-seed", ["input:b"])],
            branch="default",
            source="_enricher:fixture",
        )

    orphan_seed = _publisher(clean_runtime, deployment).cycle()
    assert orphan_seed["status"] == "published", orphan_seed
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest_before_block = str(
            store.latest_generation(
                deployment_name=deployment,
                branch="default",
                status="published",
            ).checkpoint_id
        )
        store.apply_facts(
            [
                NodeFact(
                    "input:b",
                    "thing",
                    {},
                    {"id": "b"},
                    {"v": 2},
                    op="R",
                )
            ],
            branch="default",
            source="source:fixture",
        )
        store.apply_facts(
            [_derived("derived:unknown", ["input:never-existed"])],
            branch="default",
            source="_enricher:fixture",
        )

    blocked = _publisher(clean_runtime, deployment).cycle()
    assert blocked["status"] == "blocked", blocked
    block_details = blocked["consistency_gates"][0]["details"][
        "derived_node_cleanup"
    ]
    assert block_details["retracted_node_ids"] == []
    assert block_details["no_unsupported_derived_nodes"] == {
        "check_id": "no_unsupported_derived_nodes",
        "result": "blocked",
        "blocks_publish": True,
        "blocking_node_ids": [
            "derived:any",
            "derived:orphan-seed",
            "derived:unknown",
        ],
        "blocking_declarations": [
            {
                "node_id": "derived:any",
                "status": "unresolved_member",
                "cause": "member_absent_from_candidate_and_prior",
                "member_ids": ["input:a"],
            },
            {
                "node_id": "derived:unknown",
                "status": "unresolved_member",
                "cause": "member_absent_from_candidate_and_prior",
                "member_ids": ["input:never-existed"],
            },
        ],
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        latest_after_block = str(
            store.latest_generation(
                deployment_name=deployment,
                branch="default",
                status="published",
            ).checkpoint_id
        )
        assert latest_after_block == latest_before_block
        candidate_node_ids = {
            str(row["node_id"])
            for row in store.checkout("default").query(
                "SELECT node_id FROM okg.graph_nodes"
            )
        }
        assert {
            "derived:any",
            "derived:orphan-seed",
            "derived:unknown",
        }.issubset(candidate_node_ids)
        with conn.cursor() as cur:
            runtime_dlq = current_runtime_dlq(cur)

    current_dlq_count, queue_depth = dlq_count(clean_runtime)
    assert current_dlq_count == 0
    assert int(queue_depth["current_dlq"]) == 0
    assert int(queue_depth["terminal_failure_history"]) == 0
    doctor_command = [
        str(Path(sys.executable).with_name("okg")),
        "doctor",
        "--check",
        "database",
        "--dsn",
        clean_runtime,
        "--json",
    ]
    doctor_proc = subprocess.run(
        doctor_command,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert doctor_proc.returncode == 0, doctor_proc.stderr or doctor_proc.stdout
    doctor_report = json.loads(doctor_proc.stdout)
    assert doctor_report["ok"] is True
    assert doctor_report["status"] == "OK"
    assert doctor_report["summary"]["error_count"] == 0

    report_path = os.environ.get("OKG_DERIVED_LIVENESS_REPORT")
    if report_path:
        report = {
            "schema": "okg.derived_node_liveness_proof.v1",
            "deployment_name": deployment,
            "baseline_generation": baseline_generation,
            "cleanup_generation": str(cleaned["generation_id"]),
            "cleanup": {
                "details": cleanup_details,
                "consistency_gate_mutations": mutation_receipt,
                "published_node_count": int(cleaned["node_count"]),
                "published_edge_count": int(cleaned["edge_count"]),
                "historical_node_ids": sorted(historical_node_ids),
            },
            "blocked_publish": {
                "attempted_generation": str(blocked["generation_id"]),
                "latest_before": latest_before_block,
                "latest_after": latest_after_block,
                "check": block_details["no_unsupported_derived_nodes"],
                "candidate_node_ids": sorted(candidate_node_ids),
            },
            "operations": {
                "queue_depth": queue_depth,
                "current_runtime_dlq": runtime_dlq,
                "current_dlq_count": current_dlq_count,
                "failed_job_count": len(runtime_dlq["current_failures"]),
                "doctor_command": doctor_command,
                "doctor": doctor_report,
            },
        }
        Path(report_path).write_text(
            json.dumps(report, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )


def test_first_fork_publish_uses_base_support_as_prior_evidence(
    clean_runtime: str,
) -> None:
    """A member removed on a new fork is known-dead, never unknown."""
    deployment = f"derived-node-liveness-fork-{uuid.uuid4().hex[:8]}"
    fork = f"experiment:derived-liveness-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["thing"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("input:a", "thing", {}, {"id": "a"}, {"v": 1})],
            branch="default",
            source="source:fixture",
        )
        store.apply_facts(
            [_derived("derived:fork", ["input:a"], policy="all_members")],
            branch="default",
            source="_enricher:fixture",
        )

    baseline = _publisher(clean_runtime, deployment).cycle()
    assert baseline["status"] == "published", baseline
    base_generation = str(baseline["generation_id"])

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "base_branch": "default",
                "base_generation_id": base_generation,
            },
        )
        store.apply_facts(
            [
                NodeFact(
                    "input:a",
                    "thing",
                    {},
                    {"id": "a"},
                    {"v": 2},
                    op="R",
                )
            ],
            branch=fork,
            source="source:fixture",
        )

    published = _publisher(
        clean_runtime,
        deployment,
        branch_name=fork,
    ).cycle()
    assert published["status"] == "published", published
    cleanup = published["consistency_gates"][0]["details"][
        "derived_node_cleanup"
    ]
    assert cleanup["retracted_node_ids"] == ["derived:fork"]
    assert cleanup["blocking_node_ids"] == []


def test_source_retirement_frontier_drives_derived_cleanup(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The guarded source-retirement plan reaches its derived dependents."""
    deployment = f"derived-node-retirement-{uuid.uuid4().hex[:8]}"

    def registry_identity(*_args: object, **_kwargs: object):
        return (
            "alpha",
            {"ownership_id": "dep.alpha"},
            ("alpha", "dep.alpha"),
            "registry-digest",
        )

    monkeypatch.setattr(
        "okg.substrate.source_retirement._registry_identity",
        registry_identity,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["thing"], narrowings=[])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("alpha:1", "thing", {}, {"id": "a"}, {"v": 1})],
            source="dep.alpha",
        )
        store.apply_facts(
            [NodeFact("beta:1", "thing", {}, {"id": "b"}, {"v": 1})],
            source="dep.beta",
        )
        store.apply_facts(
            [_derived("derived:alpha", ["alpha:1"])],
            source="_enricher:fixture",
        )

    baseline = _publisher(clean_runtime, deployment).cycle()
    assert baseline["status"] == "published", baseline
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        plan = plan_source_retirement(
            conn,
            source="alpha",
            deployment_name=deployment,
        )
        assert [item["node_id"] for item in plan.derived_support_frontier] == [
            "derived:alpha"
        ]
        applied = remediate_source_retirement(
            conn,
            source_name="alpha",
            deployment_name=deployment,
            expected_plan_digest=plan.plan_digest,
            confirm_source="alpha",
            apply=True,
        )
        assert applied.applied is True

    published = _publisher(clean_runtime, deployment).cycle()
    assert published["status"] == "published", published
    cleanup = published["consistency_gates"][0]["details"][
        "derived_node_cleanup"
    ]
    # The record carries the frontier as a count and digest (#2718); the
    # digest pins its content to exactly the retired node.
    assert "frontier_node_ids" not in cleanup
    assert cleanup["frontier_node_count"] == 1
    assert cleanup["frontier_node_digest_sha256"] == frontier_digest(["alpha:1"])
    assert cleanup["evaluated_node_ids"] == ["derived:alpha"]
    assert cleanup["retracted_node_ids"] == ["derived:alpha"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        live_ids = {
            str(row["node_id"])
            for row in ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).checkout("default").query("SELECT node_id FROM okg.graph_nodes")
        }
    assert live_ids == {"beta:1"}


def _thing(node_id: str, revision: int = 1) -> NodeFact:
    # The attribute changes with the revision, so a re-applied node is a real
    # content change and the next publish is not a no-op.
    return NodeFact(
        node_id, "thing", {"rev": revision}, {"id": node_id}, {"v": revision}
    )


def _cleanup_record(result: dict) -> dict:
    gates = result["consistency_gates"]
    assert len(gates) == 1, gates
    return gates[0]["details"]["derived_node_cleanup"]


def _metadata_text(conn: psycopg.Connection, generation_id: str) -> str:
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoint_id = str(store.get_generation(generation_id).checkpoint_id)
    row = conn.execute(
        "SELECT metadata::text FROM _chronos_branch_interval_checkpoints"
        " WHERE checkpoint_id = %s",
        (checkpoint_id,),
    ).fetchone()
    assert row is not None, checkpoint_id
    return str(row[0])


def _publish_new_nodes(dsn: str, deployment: str, node_ids: list[str]) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [_thing(node_id) for node_id in node_ids],
            branch="default",
            source="source:fixture",
        )
    published = _publisher(dsn, deployment).cycle()
    assert published["status"] == "published", published
    return published


def test_checkpoint_metadata_stays_bounded_whatever_the_frontier_size(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 1,200-node frontier costs the checkpoint what a 300-node one does.

    The gate's own scope sample keeps at most ``observations.SCOPE_ID_LIMIT``
    ids per list (10,000 in production). The test lowers that cap to 100 so
    both frontiers exceed it with a few hundred nodes instead of tens of
    thousands; every id list left in the metadata is then at its cap. Before
    issue #2718 the cleanup record added every frontier id on top: about
    14 KB more for the larger publish here, 62 MB on okg-workspace.
    """
    from okg.substrate.consistency import observations

    monkeypatch.setattr(observations, "SCOPE_ID_LIMIT", 100)
    deployment = f"derived-frontier-size-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["thing"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_thing("seed:0")], branch="default", source="source:fixture"
        )
    baseline = _publisher(clean_runtime, deployment).cycle()
    assert baseline["status"] == "published", baseline

    mid_ids = [f"mid:{index:08d}" for index in range(300)]
    big_ids = [f"big:{index:08d}" for index in range(1200)]
    mid = _publish_new_nodes(clean_runtime, deployment, mid_ids)
    big = _publish_new_nodes(clean_runtime, deployment, big_ids)

    sizes = []
    for published, ids in ((mid, mid_ids), (big, big_ids)):
        cleanup = _cleanup_record(published)
        assert "frontier_node_ids" not in cleanup
        assert cleanup["frontier_node_count"] == len(ids)
        assert cleanup["frontier_node_digest_sha256"] == frontier_digest(ids)
        # The cleanup record itself is a few hundred bytes at any size.
        assert len(json.dumps(cleanup)) < 2048, cleanup
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            text = _metadata_text(conn, str(published["generation_id"]))
        assert "frontier_node_ids" not in text
        sizes.append(len(text.encode("utf-8")))
    print(
        f"frontier {len(mid_ids)} -> metadata {sizes[0]} bytes; "
        f"frontier {len(big_ids)} -> metadata {sizes[1]} bytes"
    )
    # 900 more frontier ids would add about 14 KB as a list; counts and
    # timings may move a few bytes.
    assert abs(sizes[1] - sizes[0]) < 2048, sizes


def test_consistency_gate_digest_detects_a_changed_frontier(
    clean_runtime: str,
) -> None:
    """Same size, different nodes: the gate's recorded digest tells them apart."""
    deployment = f"derived-frontier-change-{uuid.uuid4().hex[:8]}"
    ids = [f"n:{index}" for index in range(6)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["thing"], narrowings=[])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_thing(node_id) for node_id in ids],
            branch="default",
            source="source:fixture",
        )
    baseline = _publisher(clean_runtime, deployment).cycle()
    assert baseline["status"] == "published", baseline

    digests = []
    for revision, changed in ((2, ids[:3]), (3, ids[:3]), (4, ids[3:])):
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
                [_thing(node_id, revision) for node_id in changed],
                branch="default",
                source="source:fixture",
            )
        published = _publisher(clean_runtime, deployment).cycle()
        assert published["status"] == "published", published
        details = published["consistency_gates"][0]["details"]
        # The scoped pass is the one that hands the gate a dirty frontier.
        assert details["scope_mode"] == "dirty_edges+incident_dirty_nodes", (
            details["scope_mode"]
        )
        cleanup = details["derived_node_cleanup"]
        assert cleanup["schema"] == "okg.derived_node_cleanup.v2"
        summary = read_frontier_summary(cleanup)
        assert summary is not None
        assert summary["basis"] == "recorded"
        assert summary["frontier_node_count"] == len(changed)
        assert summary["frontier_node_digest_sha256"] == frontier_digest(changed)
        # Same algorithm as the gate's own scope identity over the dirty nodes.
        assert summary["frontier_node_digest_sha256"] == (
            details["scope_identity"]["dirty_node_digest_sha256"]
        )
        digests.append(summary["frontier_node_digest_sha256"])

    same_nodes_again, different_nodes = digests[1], digests[2]
    assert digests[0] == same_nodes_again
    assert different_nodes != same_nodes_again

    # An old checkpoint that recorded the list reads on the same terms.
    legacy = read_frontier_summary({"frontier_node_ids": list(reversed(ids[3:]))})
    assert legacy is not None
    assert legacy["basis"] == "legacy_id_list"
    assert legacy["frontier_node_digest_sha256"] == different_nodes
