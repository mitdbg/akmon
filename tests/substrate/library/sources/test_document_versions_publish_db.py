"""Real publish of the `document_versions` module (PACT document-versions).

Composes the module into a deployment on the test database, runs the helper's
facts and record set through the source runner, and publishes with the
consistency gate the deployment stack always includes. It shows what the unit
tests cannot: the five narrowings accept the helper's edges, a version that
stops being latest is rewritten with its contents, a `supersedes` edge a late
version makes untrue is retracted, and a graph that breaks a rule does not
publish.

Set DOCVER_RECORD to a path to also write what was published as JSON.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import psycopg
import pytest
import yaml

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    RecordEmission,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.document_versions import (
    BlockAnnotation,
    DocumentBlock,
    DocumentValue,
    DocumentVersion,
    DocumentVersionFacts,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

pytestmark = pytest.mark.postgres

DEPLOYMENT = "docver"
SOURCE = "system:document_versions_proof"
SUBTYPES = ("document_version", "document_block", "document_value", "block_annotation")


def _doc(version, issued_at, *, entity="acme", family="report", blocks=2, **extra):
    return DocumentVersion(
        entity=entity,
        family=family,
        version=version,
        issued_at=issued_at,
        covers="2023",
        doc_kind="annual",
        title=f"{family} {version}",
        blocks=[
            DocumentBlock(f"{version}-b{i}", "paragraph", f"{version} paragraph {i}", {"i": i})
            for i in range(blocks)
        ],
        values=[DocumentValue(f"{version}-total", "total", "usd", "2023", {"amount": 10})],
        annotations=[
            BlockAnnotation("layout", "main", {"cols": 1}),
            BlockAnnotation("history", "main", {"was": "earlier"}),
        ],
        **extra,
    )


def _refused(version, entity):
    return DocumentVersion(
        entity=entity,
        family="report",
        version=version,
        issued_at=None,
        admitted=False,
        refusal="no_issue_time",
    )


FIRST = [
    _doc("v1", "2024-01-01T00:00:00Z"),
    _doc("v2", "2024-03-01T00:00:00Z"),
    _doc("n1", "2024-02-01T00:00:00Z", family="notes", blocks=1),
    _refused("z1", "zed"),
]
# v3 arrives late but was issued between v1 and v2; v4 is the newest.
SECOND = FIRST + [
    _doc("v3", "2024-02-01T00:00:00Z", amends="v1"),
    _doc("v4", "2024-05-01T00:00:00Z"),
]


class _Adapter:
    """A source that hands the runner the helper's facts and a record set."""

    name = "document-versions-proof"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self) -> None:
        self.next_run: Optional[tuple[list, Optional[RecordSet]]] = None

    def run(self, run_id, *, mode="cursor", cursor=None, since_progress=None):
        del run_id, cursor, since_progress
        facts, record_set = self.next_run
        return SourceRun(
            facts=facts,
            completed_scope=True,
            next_cursor={},
            run_mode=mode,
            record_set=record_set,
        )


class _Deployment:
    """The module composed onto the test database, with a runner and publisher."""

    def __init__(self, tmp_path: Path, dsn: str) -> None:
        from okg.substrate.catalog.modules_apply import apply_composed_catalog
        from okg.substrate.catalog.modules_compose import compose_catalog

        self.dsn = dsn
        root = tmp_path / DEPLOYMENT
        (root / "schemas").mkdir(parents=True)
        (root / "deployment.yaml").write_text(
            yaml.safe_dump(
                {
                    "name": DEPLOYMENT,
                    "description": "document_versions publish proof",
                    "modules": ["document_versions"],
                }
            )
        )
        self.composed = compose_catalog(root)
        with psycopg.connect(dsn) as conn:
            apply_composed_catalog(conn, self.composed)
            conn.commit()
        self.identity = bind_test_catalog_writer(dsn, DEPLOYMENT)
        self.adapter = _Adapter()
        self.runner = SourceRunner(dsn, source_name=SOURCE, write_fact_log=True)
        self.helper: Optional[DocumentVersionFacts] = None

    def run(
        self,
        versions,
        *,
        record_set: bool = True,
        extra_edge: Optional[EdgeFact] = None,
    ) -> dict[str, Any]:
        helper = DocumentVersionFacts(DEPLOYMENT)
        for version in versions:
            helper.add(version)
        self.helper = helper
        facts = list(helper.facts())
        declared: Optional[RecordSet] = helper.record_set() if record_set else None
        if extra_edge is not None:
            facts.append(extra_edge)
            # The edge is declared by its source's record, as a real source
            # would, so the runner's own record-set check passes and the
            # consistency gate is what judges it.
            owner_key = extra_edge.src.replace("docblock:", "docver:").rsplit(":", 1)[0]
            owner = declared.records[owner_key]
            declared = RecordSet(
                records={
                    **declared.records,
                    owner_key: RecordEmission(
                        primary=owner.primary,
                        descendants=owner.descendants,
                        edges=[
                            *owner.edges,
                            EdgeKey(
                                src=extra_edge.src,
                                dst=extra_edge.dst,
                                edge_type=extra_edge.edge_type,
                                source_record_id=extra_edge.source_record_id,
                            ),
                        ],
                    ),
                }
            )
        self.adapter.next_run = (facts, declared)
        result = self.runner.run(self.adapter, mode="scope_complete").as_dict()
        return {
            key: result[key]
            for key in (
                "mode",
                "nodes_appended",
                "edges_appended",
                "retractions_node",
                "retractions_edge",
                "completed_scope",
            )
        }

    def publish(self) -> dict[str, Any]:
        from okg.substrate.consistency import TransformConsistencyGate
        from okg.substrate.publisher import Publisher

        return Publisher(
            self.dsn,
            DEPLOYMENT,
            catalog_identity=self.identity,
            consistency_gates=[TransformConsistencyGate()],
        ).cycle()

    def counts(self, generation_id) -> dict[str, Any]:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.connect(self.dsn, ensure_schema=False)
        try:
            generation = store.checkout_generation(generation_id)
            nodes = {
                row["subtype"]: row["count"]
                for row in generation.query(
                    "SELECT subtype, COUNT(*) AS count FROM okg.graph_nodes "
                    "WHERE subtype = ANY(%(subtypes)s) GROUP BY subtype",
                    {"subtypes": list(SUBTYPES)},
                )
            }
            edges = {
                row["edge_type"]: row["count"]
                for row in generation.query(
                    "SELECT edge_type, COUNT(*) AS count FROM okg.graph_edges "
                    "WHERE src LIKE 'doc%:docver:%' OR src LIKE 'blockann:docver:%' "
                    "GROUP BY edge_type"
                )
            }
            latest = [
                {"subtype": r["subtype"], "is_latest": r["is_latest"], "count": r["count"]}
                for r in generation.query(
                    "SELECT subtype, attrs->>'is_latest' AS is_latest, COUNT(*) AS count "
                    "FROM okg.graph_nodes WHERE subtype = ANY(%(subtypes)s) "
                    "GROUP BY subtype, attrs->>'is_latest' ORDER BY 1, 2",
                    {"subtypes": list(SUBTYPES)},
                )
            ]
            return {"nodes": nodes, "edges": edges, "is_latest": latest}
        finally:
            store.close()

    def query(self, generation_id, sql: str) -> list[dict[str, Any]]:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.connect(self.dsn, ensure_schema=False)
        try:
            return [dict(r) for r in store.checkout_generation(generation_id).query(sql)]
        finally:
            store.close()


def _latest_versions(snapshot: dict[str, Any]) -> list[int]:
    return [
        row["count"]
        for row in snapshot["is_latest"]
        if row["subtype"] == "document_version" and row["is_latest"] == "true"
    ]


def test_two_generations_publish_flip_the_old_latest_and_retract_the_stale_edge(
    tmp_path: Path, clean_runtime: str
) -> None:
    from okg.substrate.cli import doctor as doctor_cli
    from okg.substrate.db.management import database_health_report
    from okg.substrate.runtime_queue_health import summarize_runtime_queue_health

    deployment = _Deployment(tmp_path, clean_runtime)
    record: dict[str, Any] = {
        "deployment": DEPLOYMENT,
        "module": "document_versions",
        "catalog": {
            "subtypes": sorted(deployment.composed.subtypes & set(SUBTYPES)),
            "narrowings": len(deployment.composed.narrowings),
        },
        "before": "fresh database, nothing published yet",
    }

    run1 = deployment.run(FIRST)
    out1 = deployment.publish()
    assert out1["status"] == "published", out1
    snap1 = deployment.counts(out1["generation_id"])
    record["generation_1"] = {"id": str(out1["generation_id"]), "status": out1["status"], **snap1}

    run2 = deployment.run(SECOND)
    out2 = deployment.publish()
    assert out2["status"] == "published", out2
    snap2 = deployment.counts(out2["generation_id"])
    record["generation_2"] = {"id": str(out2["generation_id"]), "status": out2["status"], **snap2}
    record["runner"] = [run1, run2]

    assert snap1["nodes"] == {
        "document_version": 4, "document_block": 5, "document_value": 3, "block_annotation": 6,
    }
    assert snap1["edges"] == {"contains": 8, "annotates": 6, "supersedes": 1}
    assert snap2["nodes"] == {
        "document_version": 6, "document_block": 9, "document_value": 5, "block_annotation": 10,
    }
    assert snap2["edges"] == {"contains": 14, "annotates": 10, "supersedes": 3, "amends": 1}
    assert _latest_versions(snap1) == [2]
    assert _latest_versions(snap2) == [2]
    # The late version made `v2 supersedes v1` untrue; the runner took it away.
    assert run1["retractions_edge"] == 0
    assert run2["retractions_edge"] == 1

    supersedes = deployment.query(
        out2["generation_id"],
        "SELECT src, dst FROM okg.graph_edges WHERE edge_type = 'supersedes' ORDER BY src",
    )
    assert [[r["src"], r["dst"]] for r in supersedes] == [
        ["docver:docver:acme:v2", "docver:docver:acme:v3"],
        ["docver:docver:acme:v3", "docver:docver:acme:v1"],
        ["docver:docver:acme:v4", "docver:docver:acme:v2"],
    ]
    amends = deployment.query(
        out2["generation_id"],
        "SELECT src, dst FROM okg.graph_edges WHERE edge_type = 'amends' ORDER BY src",
    )
    assert [[r["src"], r["dst"]] for r in amends] == [
        ["docver:docver:acme:v3", "docver:docver:acme:v1"]
    ]
    record["supersedes"] = [[r["src"], r["dst"]] for r in supersedes]
    record["amends"] = [[r["src"], r["dst"]] for r in amends]

    # The old latest and everything it contains was rewritten with "false".
    flipped = deployment.query(
        out2["generation_id"],
        "SELECT attrs->>'is_latest' AS is_latest, COUNT(*) AS count FROM okg.graph_nodes "
        "WHERE subtype = 'document_block' AND attrs->>'version' = 'v2' GROUP BY 1",
    )
    assert flipped == [{"is_latest": "false", "count": 2}]
    record["blocks_of_v2_after_newer_version"] = flipped

    # One read: the latest blocks of one document, in position order.
    blocks = deployment.query(
        out2["generation_id"],
        "SELECT attrs->>'version' AS version, (attrs->>'position')::int AS position, "
        "attrs->>'text' AS text FROM okg.graph_nodes "
        "WHERE subtype = 'document_block' AND attrs->>'is_latest' = 'true' "
        "AND attrs->>'entity' = 'acme' AND attrs->>'family' = 'report' "
        "ORDER BY (attrs->>'position')::int",
    )
    assert [r["text"] for r in blocks] == ["v4 paragraph 0", "v4 paragraph 1"]
    record["read_latest_blocks_of_acme_report"] = blocks

    # Checks beside the publish. Queue health needs the runtime's workflow
    # table; without it the summary says "unavailable" and its failure count
    # of 0 is an absence, not a measurement.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            health = summarize_runtime_queue_health(cur)
            cur.execute("SELECT to_regclass('dbos.workflow_status')::text")
            workflow_table = cur.fetchone()[0]
    record["queue"] = {
        "workflow_table": workflow_table,
        "measured": workflow_table is not None,
        "status": health.get("status"),
        "dlq_count": health.get("dlq_count"),
    }
    db_report = database_health_report(clean_runtime, deployment_name=None)
    record["database_check"] = {
        "ok": db_report.get("ok"),
        "errors": [e if isinstance(e, str) else e.get("code") for e in db_report.get("errors", [])],
    }
    parents = doctor_cli._default_doctor_passage_parents(
        clean_runtime, DEPLOYMENT, {"ok": True, "errors": []}
    )
    record["passage_parent_check"] = parents
    assert parents["ok"] is True and parents["undeclared"] == []

    path = os.environ.get("DOCVER_RECORD")
    if path:
        Path(path).write_text(json.dumps(record, indent=2, sort_keys=True, default=str) + "\n")


def test_a_source_that_forgets_the_record_set_is_blocked_at_publish(
    tmp_path: Path, clean_runtime: str
) -> None:
    """Failure case: a stale `supersedes` edge gives one version two targets."""
    deployment = _Deployment(tmp_path, clean_runtime)
    deployment.run(FIRST)
    out1 = deployment.publish()
    assert out1["status"] == "published", out1

    # The same second run, but the source hands over no record set.
    run2 = deployment.run(SECOND, record_set=False)
    out2 = deployment.publish()

    assert run2["retractions_edge"] == 0
    assert out2["status"] == "blocked", out2
    assert out2["predecessor_generation_id"] == out1["generation_id"]
    assert "consistency_publish_gate_blocked" in str(out2)


def test_an_edge_without_a_narrowing_blocks_the_publish(
    tmp_path: Path, clean_runtime: str
) -> None:
    """Failure case: a block cannot contain a version."""
    deployment = _Deployment(tmp_path, clean_runtime)
    deployment.run(SECOND)
    out1 = deployment.publish()
    assert out1["status"] == "published", out1

    wrong_way = EdgeFact(
        src="docblock:docver:acme:v4:0",
        dst="docver:docver:acme:v4",
        edge_type="contains",
        source_record_id={"proof": "wrong-way"},
    )
    deployment.run(SECOND, extra_edge=wrong_way)
    out2 = deployment.publish()

    assert out2["status"] == "blocked", out2
    assert "consistency_publish_gate_blocked" in str(out2)
