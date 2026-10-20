#!/usr/bin/env python3
"""Publish an old scholarly shape, reconcile it, and prove the live graph."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg.conninfo import conninfo_to_dict

from okg.substrate.exchange.harness import database_doctor, describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.linkers.declarative import DeclarativeLinker
from okg.substrate.library.linkers.rules import CrossLinkRule, RuleEndpoint
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.scholarly.bib_references import (
    BibReferencesSource,
)
from okg.substrate.publisher import Publisher
from scripts.proof_catalog_authority import bind_proof_catalog_writer


DEPLOYMENT = "scholarly-source-shapes-proof"
SOURCE = "proof.bib-references"
DOI = "10.1146/annurev-nucl-110320-051823"
PAPER = f"paper:doi:{DOI}"
CANONICAL = "Annual Review of Nuclear and Particle Science"
FULL_VENUE = "venue:annual-review-of-nuclear-and-particle-science"
ABBREVIATED_VENUE = "venue:ann-rev-nucl-part-sci"
FULL_CONFERENCE = "conf:annual-review-of-nuclear-and-particle-science"
ABBREVIATED_CONFERENCE = "conf:ann-rev-nucl-part-sci"
FULL_EDITION = "confed:annual-review-of-nuclear-and-particle-science-2021"
ABBREVIATED_EDITION = "confed:ann-rev-nucl-part-sci-2021"


def _published_generation_id(result: dict[str, Any], *, stage: str) -> str:
    """Return a successfully published generation without a stale fallback."""
    if result.get("status") != "published":
        raise RuntimeError(f"{stage} publish blocked: {result}")
    generation_id = result.get("generation_id")
    if not generation_id:
        raise RuntimeError(f"{stage} publish missing generation_id: {result}")
    return str(generation_id)


def _assert_isolated(dsn: str, allow_non_test_dsn: bool) -> None:
    database = str(conninfo_to_dict(dsn).get("dbname") or "")
    if not allow_non_test_dsn and "test" not in database and "isolated" not in database:
        raise RuntimeError(
            f"refusing scholarly proof against non-test database {database!r}"
        )


def _record_set(facts: Iterable[NodeFact | EdgeFact]) -> RecordSet:
    materialized = list(facts)
    return RecordSet(records={
        "bibliography-scope": RecordEmission(
            primary=None,
            descendants=[
                RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
                for fact in materialized
                if isinstance(fact, NodeFact)
            ],
            edges=[
                EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=fact.attrs,
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=fact.source_record_id,
                    source_revision=fact.source_revision,
                )
                for fact in materialized
                if isinstance(fact, EdgeFact)
            ],
        ),
    })


class _StaticSource:
    name = "scholarly-shape-proof-static"
    profile = "discovery_crawl"

    def __init__(self, facts: Iterable[NodeFact | EdgeFact]) -> None:
        self.facts = tuple(facts)

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        sync_scope: dict[str, Any] | None = None,
    ) -> SourceRun:
        del cursor, sync_scope
        return SourceRun(
            facts=self.facts,
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"shape": "legacy"},
            run_mode=mode,
            record_set=(
                _record_set(self.facts)
                if mode in {"scope_complete", "reconcile"}
                else None
            ),
        )


def _legacy_facts() -> list[NodeFact | EdgeFact]:
    revision = {"fixture": "old-shape"}
    record = {"bib_path": "cooper.bib", "doi": DOI}
    facts: list[NodeFact | EdgeFact] = [
        NodeFact(PAPER, "paper", {"paper_id": PAPER, "title": "Review A", "doi": DOI}, record, revision),
    ]
    for raw, venue, conference, edition in (
        (CANONICAL, FULL_VENUE, FULL_CONFERENCE, FULL_EDITION),
        ("Ann. Rev. Nucl. Part. Sci.", ABBREVIATED_VENUE, ABBREVIATED_CONFERENCE, ABBREVIATED_EDITION),
    ):
        venue_record = {"bib_path": "cooper.bib", "venue": raw}
        edition_record = {"venue": raw, "year": "2021"}
        facts.extend([
            NodeFact(venue, "venue", {"name": raw}, venue_record, revision),
            NodeFact(conference, "conference", {"name": raw}, {"venue": raw}, revision),
            NodeFact(edition, "conference_edition", {"name": f"{raw} 2021", "year": "2021"}, edition_record, revision),
            EdgeFact(edition, conference, "member_of", source_record_id=edition_record, source_revision=revision),
            EdgeFact(PAPER, edition, "published_in", source_record_id={**record, "venue": raw}, source_revision=revision),
        ])
    return facts


def _citation_facts() -> list[NodeFact]:
    return [
        NodeFact(
            "citation:underscore",
            "citation",
            {"citation_id": "citation:underscore", "cited_paper_ref": "paper:Graham_2021"},
            {"citation": "underscore"},
            {"fixture": "citation"},
        ),
        NodeFact(
            "citation:colon",
            "citation",
            {"citation_id": "citation:colon", "cited_paper_ref": "paper:Graham:2021ggy"},
            {"citation": "colon"},
            {"fixture": "citation"},
        ),
    ]


def _seed_catalog(dsn: str, facts: Iterable[NodeFact | EdgeFact]) -> int:
    materialized = list(facts)
    node_subtypes = {
        fact.node_id: fact.subtype
        for fact in materialized
        if isinstance(fact, NodeFact)
    }
    narrowings = {
        (node_subtypes[fact.src], fact.edge_type, node_subtypes[fact.dst])
        for fact in materialized
        if isinstance(fact, EdgeFact)
    }
    narrowings.add(("citation", "cites", "paper"))
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version = int(cur.fetchone()[0])
        for subtype in sorted(set(node_subtypes.values())):
            cur.execute(
                "INSERT INTO okg.subtypes (name, catalog_version_id) VALUES (%s, %s)",
                (subtype, catalog_version),
            )
            cur.execute(
                "INSERT INTO okg.subtype_ancestors "
                "(catalog_version_id, subtype_name, ancestor_name, depth) "
                "VALUES (%s, %s, %s, 0) ON CONFLICT DO NOTHING",
                (catalog_version, subtype, subtype),
            )
        for parent, edge_type, child in sorted(narrowings):
            cur.execute(
                "INSERT INTO okg.narrowings "
                "(parent_subtype, edge_type, child_subtype, catalog_version_id) "
                "VALUES (%s, %s, %s, %s)",
                (parent, edge_type, child, catalog_version),
            )
    return catalog_version


def _provision_dbos_runtime(dsn: str) -> None:
    from dbos import DBOS, DBOSConfig

    try:
        DBOS.destroy()
    except Exception:  # noqa: BLE001 - no singleton is the common case.
        pass
    DBOS(config=DBOSConfig(
        name="scholarly-source-shapes-proof",
        system_database_url=dsn,
        database_url=dsn,
        run_admin_server=False,
    ))
    try:
        DBOS.launch()
    finally:
        DBOS.destroy()


def _snapshot(dsn: str, generation_id: str) -> dict[str, Any]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        graph = store.checkout_generation(generation_id)
        nodes = graph.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes "
            "ORDER BY subtype, node_id"
        )
        edges = graph.query(
            "SELECT src, edge_type, dst FROM okg.graph_edges "
            "ORDER BY edge_type, src, dst"
        )
    finally:
        store.close()
    return {"nodes": nodes, "edges": edges}


def _queue_health(dsn: str) -> dict[str, Any]:
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        available = cur.fetchone()[0] is not None
        if not available:
            return {"available": False, "active": None, "dlq": None}
        cur.execute(
            "SELECT count(*) FILTER (WHERE status IN "
            "('PENDING','ENQUEUED','DELAYED')), "
            "count(*) FILTER (WHERE status IN "
            "('ERROR','FAILED','MAX_RECOVERY_ATTEMPTS',"
            "'MAX_RECOVERY_ATTEMPTS_EXCEEDED')) FROM dbos.workflow_status"
        )
        active, dlq = cur.fetchone()
        return {"available": True, "active": int(active), "dlq": int(dlq)}


def _isolated_doctor(dsn: str) -> dict[str, Any]:
    from okg.substrate.db import migrate as migrate_module

    original = dict(migrate_module.REQUIRED_EXTENSIONS)
    migrate_module.REQUIRED_EXTENSIONS.pop("pg_cron", None)
    try:
        return database_doctor(dsn)
    finally:
        migrate_module.REQUIRED_EXTENSIONS.clear()
        migrate_module.REQUIRED_EXTENSIONS.update(original)


def run(*, dsn: str, allow_non_test_dsn: bool = False) -> dict[str, Any]:
    _assert_isolated(dsn, allow_non_test_dsn)
    _provision_dbos_runtime(dsn)
    legacy = _legacy_facts()
    citations = _citation_facts()
    with tempfile.TemporaryDirectory(prefix="okg-scholarly-proof-") as temp_dir:
        bib = Path(temp_dir) / "cooper.bib"
        bib.write_text(
            "@article{Graham_2021,title={Review A},"
            f"journal={{{CANONICAL}}},year={{2021}},doi={{{DOI}}}}}\n"
            "@article{Graham:2021ggy,title={Review A},"
            "journal={Ann. Rev. Nucl. Part. Sci.},year={2021},"
            f"doi={{{DOI}}}}}\n",
            encoding="utf-8",
        )
        corrected = BibReferencesSource(
            bib_paths=[str(bib)],
            venue_aliases={"Ann. Rev. Nucl. Part. Sci.": CANONICAL},
        )
        preview = list(corrected.run("preview", mode="scope_complete").facts)
        catalog_version = _seed_catalog(dsn, [*legacy, *citations, *preview])
        catalog_identity = bind_proof_catalog_writer(
            dsn,
            deployment_name=DEPLOYMENT,
            catalog_version_id=catalog_version,
            proof_name="scholarly-source-shapes",
        )

        SourceRunner(dsn, source_name="proof.citations").run(
            _StaticSource(citations), mode="scope_complete"
        )
        runner = SourceRunner(dsn, source_name=SOURCE)
        runner.run(_StaticSource(legacy), mode="scope_complete")
        before_publish = Publisher(
            dsn,
            DEPLOYMENT,
            enrichers=[],
            catalog_identity=catalog_identity,
        ).cycle()
        before_generation = _published_generation_id(
            before_publish, stage="old-shape"
        )
        before = _snapshot(dsn, before_generation)

        reconcile = runner.run(corrected, mode="scope_complete")
        linker = DeclarativeLinker(rules=[CrossLinkRule(
            id="citation_cites_paper_by_bib_key",
            src=RuleEndpoint(subtypes=("citation",), field="cited_paper_ref"),
            dst=RuleEndpoint(subtypes=("paper",), field="bib_key_refs"),
            matcher="exact",
            edge="cites",
            bidirectional=False,
        )])
        corrected_publish = Publisher(
            dsn,
            DEPLOYMENT,
            enrichers=[linker],
            catalog_identity=catalog_identity,
        ).cycle(force_enrichers=True)
        corrected_generation = _published_generation_id(
            corrected_publish, stage="corrected"
        )
        corrected_description = describe_generation(dsn, corrected_generation)
        corrected_graph = _snapshot(dsn, corrected_generation)

        contributing_source_record_ids = sorted(
            [
                dict(fact.source_record_id)
                for fact in preview
                if isinstance(fact, NodeFact) and fact.node_id == PAPER
            ],
            key=lambda record_id: str(record_id.get("cite_key") or ""),
        )

        replay_run = runner.run(corrected, mode="scope_complete")
        replay_publish = Publisher(
            dsn,
            DEPLOYMENT,
            enrichers=[linker],
            catalog_identity=catalog_identity,
        ).cycle(force_enrichers=True)
        replay_generation = _published_generation_id(
            replay_publish, stage="replay"
        )
        replay_graph = _snapshot(dsn, replay_generation)

    node_by_id = {row["node_id"]: row for row in corrected_graph["nodes"]}
    corrected_edges = {
        (row["src"], row["edge_type"], row["dst"])
        for row in corrected_graph["edges"]
    }
    stale_nodes = {
        FULL_CONFERENCE,
        ABBREVIATED_CONFERENCE,
        FULL_EDITION,
        ABBREVIATED_EDITION,
        ABBREVIATED_VENUE,
    }
    paper_attrs = dict(node_by_id[PAPER]["attrs"])
    queue = _queue_health(dsn)
    doctor = _isolated_doctor(dsn)
    before_description = describe_generation(dsn, before_generation)
    graph_replay_stable = corrected_graph == replay_graph
    ok = bool(
        corrected_description.get("status") == "published"
        and stale_nodes.isdisjoint(node_by_id)
        and FULL_VENUE in node_by_id
        and node_by_id[FULL_VENUE]["attrs"].get("venue_aliases")
        == ["Ann. Rev. Nucl. Part. Sci.", CANONICAL]
        and paper_attrs.get("bib_keys") == ["Graham:2021ggy", "Graham_2021"]
        and [
            record_id.get("cite_key")
            for record_id in contributing_source_record_ids
        ] == ["Graham:2021ggy", "Graham_2021"]
        and (PAPER, "published_in", FULL_VENUE) in corrected_edges
        and ("citation:underscore", "cites", PAPER) in corrected_edges
        and ("citation:colon", "cites", PAPER) in corrected_edges
        and reconcile.retractions_node >= 5
        and reconcile.retractions_edge >= 4
        and replay_run.nodes_appended == 0
        and replay_run.edges_appended == 0
        and replay_run.retractions_node == 0
        and replay_run.retractions_edge == 0
        and graph_replay_stable
        and queue == {"available": True, "active": 0, "dlq": 0}
        and doctor.get("ok") is True
    )
    return {
        "schema": "okg.scholarly_source_shapes_proof/v1",
        "ok": ok,
        "deployment": DEPLOYMENT,
        "old_generation": before_description,
        "corrected_generation": corrected_description,
        "before_counts": {
            "nodes_live": len(before["nodes"]),
            "edges_live": len(before["edges"]),
        },
        "after_counts": {
            "nodes_live": len(corrected_graph["nodes"]),
            "edges_live": len(corrected_graph["edges"]),
        },
        "retractions": {
            "nodes": reconcile.retractions_node,
            "edges": reconcile.retractions_edge,
        },
        "stale_nodes_absent": sorted(stale_nodes.isdisjoint(node_by_id) and stale_nodes or []),
        "paper": {"node_id": PAPER, "attrs": paper_attrs},
        "contributing_source_record_ids": contributing_source_record_ids,
        "venue": node_by_id.get(FULL_VENUE),
        "citation_edges": sorted(
            list(edge) for edge in corrected_edges if edge[1] == "cites"
        ),
        "replay": {
            "nodes_appended": replay_run.nodes_appended,
            "edges_appended": replay_run.edges_appended,
            "nodes_retracted": replay_run.retractions_node,
            "edges_retracted": replay_run.retractions_edge,
            "graph_stable": graph_replay_stable,
            "publish_status": replay_publish.get("status"),
            "generation_id": replay_publish.get("generation_id"),
        },
        "queue_dlq": queue,
        "doctor_profile": "isolated-test-without-pg-cron",
        "doctor": doctor,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--allow-non-test-dsn", action="store_true")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    result = run(dsn=args.dsn, allow_non_test_dsn=args.allow_non_test_dsn)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2, default=str) + "\n")
    if args.json or not args.out:
        print(json.dumps(result, indent=2, default=str))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
