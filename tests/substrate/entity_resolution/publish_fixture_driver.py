#!/usr/bin/env python3
"""Publish and verify the issue-1084 entity-resolution/merge fixture.

The proof is intentionally deployment-scoped and refuses to reuse a deployment
name.  It is safe to run against an existing OKG database without truncating or
rewriting unrelated runtime state.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import ComposedCatalog, NarrowingRow
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    claim_catalog_ownership,
    diagnose_catalog_ownership,
    ensure_catalog_write_allowed,
)
from okg.substrate.entity_resolution import EntityResolutionStage
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.projection.remediation import (
    mark_entity_merge_published,
    remediate_entity_merge,
)
from okg.substrate.publish_proof import dlq_count
from okg.substrate.publisher import Publisher


SURVIVOR_ASSERTION_SOURCE = "_enricher:cooper_mention_grounding"
RETIREE_ASSERTION_SOURCE = "_enricher:cooper_physics_extraction"
EDGE_EQUIVALENCE_POLICY = {
    "schema_version": "okg.entity_merge.edge_equivalence.v1",
    "survivor": {
        "producer": SURVIVOR_ASSERTION_SOURCE,
        "provenance": "derived_deterministic",
    },
    "retiree": {
        "producer": RETIREE_ASSERTION_SOURCE,
        "provenance": "explicit",
    },
    "audit_attrs": ["evidence_basis", "provenance_kind", "src_kind"],
    "confidence_rule": "survivor_gte",
}


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    if not slug:
        raise ValueError("deployment must contain an ASCII letter or digit")
    return slug[:48]


def _policy(deployment: str, branch: str) -> dict[str, Any]:
    return {
        "deployment_name": deployment,
        "branch_name": branch,
        "algorithm_version": "unicode-escape-v1",
        "policies": {
            "region": {
                "name_field": "name",
                "id_prefix": "region",
                "identity_fields": ["region_id"],
                "alias_first": True,
                "live_exact": True,
                "post_filter": {"deny_exact": ["other", "selection"]},
            }
        },
    }


def _fixture_catalog(deployment: str, ids: dict[str, str]) -> ComposedCatalog:
    origin = "proof:issue-1084-entity-resolution"
    return ComposedCatalog(
        deployment_name=deployment,
        modules=[origin],
        subtypes={"paper", "region"},
        subtype_origin={"paper": origin, "region": origin},
        narrowings=[
            NarrowingRow(
                parent_subtype="paper",
                edge_archetype="grounds",
                child_subtype="region",
                origin=origin,
                narrowing_name="paper_grounds_region",
                entity_merge_equivalence=EDGE_EQUIVALENCE_POLICY,
            )
        ],
        aliases=[
            {
                "alias": "Signal Region",
                "canonical": ids["signal"],
                "scope": "region",
            },
            {
                "alias": "extended-ABCD",
                "canonical": ids["extended"],
                "scope": "region",
            },
        ],
    )


def _apply_fixture_catalog(
    conn: psycopg.Connection,
    *,
    deployment: str,
    ids: dict[str, str],
) -> tuple[Any, int]:
    composed = _fixture_catalog(deployment, ids)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=Path(__file__).resolve().parent,
        command="issue-1084 entity-resolution fixture proof",
        writer_scope="deployment_root",
    )
    report = diagnose_catalog_ownership(conn, identity=identity)
    if report.get("owner") is None:
        # The proof requires a fresh deployment name. Force is therefore
        # bounded to an absent deployment-specific owner and cannot replace a
        # foreign writer; an existing owner always takes the fail-closed path.
        claim_catalog_ownership(conn, identity=identity, force=True)
    else:
        ensure_catalog_write_allowed(conn, identity=identity)
    catalog_version_id = apply_composed_catalog(
        conn,
        composed,
        identity=identity,
    )
    return identity, catalog_version_id


def _publish(
    dsn: str,
    deployment: str,
    branch: str,
    *,
    catalog_identity: Any,
) -> dict[str, Any]:
    result = Publisher(
        dsn,
        deployment,
        branch_name=branch,
        catalog_identity=catalog_identity,
    ).cycle()
    if result.get("status") != "published":
        raise AssertionError(f"fixture publish did not complete: {result}")
    return result


def prove(dsn: str, deployment: str, branch: str) -> dict[str, Any]:
    token = _slug(deployment)
    ids = {
        "signal": f"region:signal-{token}",
        "extended": f"region:extended-abcd-{token}",
        "historical_duplicate": f"region:signal-legacy-{token}",
        "paper": f"paper:cooper-{token}",
    }
    novel_name = f"γ Transition Region {token}"
    generations: list[str] = []
    publishes: list[dict[str, Any]] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        if store.list_generations(
            deployment_name=deployment, branch=branch, status=None
        ):
            raise RuntimeError(
                f"deployment {deployment!r} already has generations; choose a fresh name"
            )
        catalog_identity, catalog_version_id = _apply_fixture_catalog(
            conn,
            deployment=deployment,
            ids=ids,
        )
        store.apply_facts(
            [
                NodeFact(
                    ids["signal"],
                    "region",
                    {"name": "Canonical Signal Region", "curated": True},
                    {"id": ids["signal"]},
                    {"fixture": 1},
                ),
                NodeFact(
                    ids["extended"],
                    "region",
                    {"name": "extended-ABCD", "curated": True},
                    {"id": ids["extended"]},
                    {"fixture": 1},
                ),
                NodeFact(
                    ids["paper"],
                    "paper",
                    {"title": "Cooper operational fixture"},
                    {"id": ids["paper"]},
                    {"fixture": 1},
                ),
            ],
            branch=branch,
            source="curated-fixture",
        )
        publishes.append(
            _publish(
                dsn,
                deployment,
                branch,
                catalog_identity=catalog_identity,
            )
        )
        generations.append(str(publishes[-1]["generation_id"]))

        stage = EntityResolutionStage(_policy(deployment, branch))
        signal = stage.resolve(
            conn,
            proposed_node_id=f"region:random-signal-{token}",
            subtype="region",
            attrs={"name": "Signal Region"},
        )
        extended = stage.resolve(
            conn,
            proposed_node_id=f"region:random-extended-{token}",
            subtype="region",
            attrs={"name": "extended abcd"},
        )
        novel = stage.resolve(
            conn,
            proposed_node_id=f"region:random-gamma-{token}",
            subtype="region",
            attrs={"name": novel_name},
        )
        if signal.canonical_node_id != ids["signal"]:
            raise AssertionError("curated alias did not win")
        if extended.canonical_node_id != ids["extended"]:
            raise AssertionError("normalized curated alias did not win")
        if not novel.materialize or "u03b3" not in str(novel.canonical_node_id):
            raise AssertionError("Unicode-safe novel mint was not materialized")
        store.apply_facts(
            [
                NodeFact(
                    str(novel.canonical_node_id),
                    "region",
                    {"name": novel_name, "extracted": True},
                    {"paper": ids["paper"]},
                    {"fixture": 2},
                )
            ],
            branch=branch,
            source="extracted-fixture",
        )
        publishes.append(
            _publish(
                dsn,
                deployment,
                branch,
                catalog_identity=catalog_identity,
            )
        )
        generations.append(str(publishes[-1]["generation_id"]))
        replay = stage.resolve(
            conn,
            proposed_node_id=f"region:random-gamma-replay-{token}",
            subtype="region",
            attrs={"name": novel_name.replace(" ", "  ").replace("Region", "—Region")},
        )
        if replay.canonical_node_id != novel.canonical_node_id or replay.materialize:
            raise AssertionError("novel identity replay was not stable")

        store.apply_facts(
            [
                NodeFact(
                    ids["historical_duplicate"],
                    "region",
                    {"name": "Signal Region", "legacy_batch": token},
                    {"paper": ids["paper"]},
                    {"fixture": "legacy"},
                )
            ],
            branch=branch,
            source="legacy-extractor",
        )
        # Redacted persisted-row shape from issue #1084: two software producers
        # asserted the same relationship from the same upstream record, but
        # their audit attrs/provenance differ. Direct row insertion is
        # intentional: source-scoped apply_facts would restamp each producer as
        # the lineage source, while these producer outputs preserve the shared
        # upstream paper lineage observed in the deployment.
        from okg.substrate.graph_store import _edge_id
        from okg.substrate.nomos.source_lineage import (
            LINEAGE_ATTR,
            stamp_attrs_with_source_lineage,
        )

        source_record_id = {"paper": ids["paper"], "section": 2}
        shared_lineage = stamp_attrs_with_source_lineage(
            {},
            source_id="cooper-paper-fixture",
            source_record_id=source_record_id,
            source_revision={"content_hash": f"redacted:{token}"},
            policy_fingerprint="sha256:redacted-cooper-policy",
        )[LINEAGE_ATTR]
        observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        session = store.checkout(branch)
        with session.transaction():
            session.upsert_rows(
                "okg.graph_edges",
                [
                    {
                        "edge_id": _edge_id(
                            ids["paper"], "grounds", ids["signal"]
                        ),
                        "src": ids["paper"],
                        "dst": ids["signal"],
                        "edge_type": "grounds",
                        "attrs": {
                            "section": 2,
                            "provenance_kind": "name_mention",
                            "evidence_basis": "verbatim_chunk_mention",
                            LINEAGE_ATTR: shared_lineage,
                        },
                        "provenance": "derived_deterministic",
                        "confidence": 0.95,
                        "source": SURVIVOR_ASSERTION_SOURCE,
                        "source_record_id": source_record_id,
                        "source_revision": {"run_id": "mention-grounding-7"},
                        "run_id": "mention-grounding-7",
                        "observed_at": observed_at,
                    },
                    {
                        "edge_id": _edge_id(
                            ids["paper"],
                            "grounds",
                            ids["historical_duplicate"],
                        ),
                        "src": ids["paper"],
                        "dst": ids["historical_duplicate"],
                        "edge_type": "grounds",
                        "attrs": {
                            "section": 2,
                            "provenance_kind": "model_extraction",
                            "src_kind": "document_chunk",
                            LINEAGE_ATTR: shared_lineage,
                        },
                        "provenance": "explicit",
                        "confidence": 0.93,
                        "source": RETIREE_ASSERTION_SOURCE,
                        "source_record_id": source_record_id,
                        "source_revision": {"run_id": "physics-extraction-4"},
                        "run_id": "physics-extraction-4",
                        "observed_at": observed_at,
                    },
                ],
            )
        publishes.append(
            _publish(
                dsn,
                deployment,
                branch,
                catalog_identity=catalog_identity,
            )
        )
        generations.append(str(publishes[-1]["generation_id"]))
        debt_graph = store.checkout_generation(generations[-1], branch=branch)
        survivor_edge_before = dict(
            debt_graph.query(
                "SELECT edge_id, src, dst, edge_type, attrs, provenance, "
                "confidence, source, source_record_id, source_revision, "
                "run_id, observed_at FROM okg.graph_edges "
                "WHERE src = :paper AND dst = :survivor "
                "AND edge_type = 'grounds'",
                {"paper": ids["paper"], "survivor": ids["signal"]},
            )[0]
        )
        before_counts = {
            "target_nodes_live": len(
                debt_graph.query(
                    "SELECT node_id FROM okg.graph_nodes "
                    "WHERE node_id = ANY(:ids)",
                    {"ids": [ids["signal"], ids["historical_duplicate"]]},
                )
            ),
            "target_edges_live": len(
                debt_graph.query(
                    "SELECT edge_id FROM okg.graph_edges "
                    "WHERE src = :paper AND edge_type = 'grounds'",
                    {"paper": ids["paper"]},
                )
            ),
        }

        dry_run = remediate_entity_merge(
            conn,
            survivor_node_id=ids["signal"],
            retired_node_id=ids["historical_duplicate"],
            deployment_name=deployment,
            branch_name=branch,
            accept_survivor_attributes=True,
            apply=False,
        )
        dry_plan = dry_run.details["plan"]
        if dry_plan["blocking_conflicts"] or len(
            dry_plan["deduplicated_edges"]
        ) != 1:
            raise AssertionError(
                f"declared cross-producer assertion did not deduplicate: {dry_plan}"
            )
        digest = str(dry_plan["plan_digest"])
        applied = remediate_entity_merge(
            conn,
            survivor_node_id=ids["signal"],
            retired_node_id=ids["historical_duplicate"],
            deployment_name=deployment,
            branch_name=branch,
            expected_plan_digest=digest,
            accept_survivor_attributes=True,
            actor="issue-796-proof",
            apply=True,
        )
        publishes.append(
            _publish(
                dsn,
                deployment,
                branch,
                catalog_identity=catalog_identity,
            )
        )
        generations.append(str(publishes[-1]["generation_id"]))
        mark_entity_merge_published(
            conn,
            merge_id=str(applied.details["receipt"]["merge_id"]),
            repair_generation_id=generations[-1],
        )

        graph = store.checkout_generation(generations[-1], branch=branch)
        regions = graph.query(
            "SELECT node_id FROM okg.graph_nodes "
            "WHERE subtype = 'region' AND node_id = ANY(:ids) ORDER BY node_id",
            {"ids": [*ids.values(), str(novel.canonical_node_id)]},
        )
        region_ids = [str(row["node_id"]) for row in regions]
        expected_region_ids = sorted(
            [ids["signal"], ids["extended"], str(novel.canonical_node_id)]
        )
        if region_ids != expected_region_ids:
            raise AssertionError(
                f"repaired region set differs: {region_ids} != {expected_region_ids}"
            )
        edges = graph.query(
            "SELECT edge_id, src, dst, edge_type, attrs, provenance, "
            "confidence, source, source_record_id, source_revision, "
            "run_id, observed_at "
            "FROM okg.graph_edges WHERE src = :paper AND edge_type = 'grounds'",
            {"paper": ids["paper"]},
        )
        if len(edges) != 1 or str(edges[0]["dst"]) != ids["signal"]:
            raise AssertionError("merge did not retire the duplicate assertion")
        survivor_edge_after = dict(edges[0])
        if survivor_edge_after != survivor_edge_before:
            raise AssertionError("merge changed the surviving assertion")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, repair_generation_id, plan_digest, plan "
                "FROM okg.entity_merge_receipts "
                "WHERE merge_id = %s",
                (str(applied.details["receipt"]["merge_id"]),),
            )
            (
                receipt_status,
                receipt_generation,
                receipt_plan_digest,
                receipt_plan,
            ) = cur.fetchone()
            cur.execute(
                "SELECT count(*) FROM okg.extractor_retry_queue "
                "WHERE deployment_name = %s",
                (deployment,),
            )
            retry_depth = int(cur.fetchone()[0])
        dead_letters, queue_depth = dlq_count(dsn)
        generation_rows = [
            {
                key: result.get(key)
                for key in (
                    "generation_id",
                    "compat_generation_id",
                    "deployment_name",
                    "status",
                    "catalog_version_id",
                    "published_at",
                    "nodes_added",
                    "nodes_updated",
                    "nodes_retracted",
                    "edges_added",
                    "edges_updated",
                    "edges_retracted",
                    "node_count",
                    "edge_count",
                    "noop",
                )
            }
            for result in publishes
        ]
        if any(str(row["status"]) != "published" for row in generation_rows):
            raise AssertionError("fixture produced a non-published generation")
        if receipt_status != "published" or str(receipt_generation) != generations[-1]:
            raise AssertionError("merge receipt was not coupled to repair publication")
        if str(receipt_plan_digest) != digest:
            raise AssertionError("merge receipt plan digest differs from dry-run")
        if isinstance(receipt_plan, str):
            receipt_plan = json.loads(receipt_plan)
        dry_deduplicated_json = json.loads(
            json.dumps(dry_plan["deduplicated_edges"], default=str)
        )
        if receipt_plan["deduplicated_edges"] != dry_deduplicated_json:
            raise AssertionError("merge receipt lost assertion equivalence evidence")
        if retry_depth or dead_letters:
            raise AssertionError(
                f"runtime queues not clean: retry={retry_depth}, dlq={dead_letters}"
            )

    return {
        "ok": True,
        "deployment_name": deployment,
        "branch_name": branch,
        "catalog_version_id": catalog_version_id,
        "generations": generation_rows,
        "final_generation_id": generations[-1],
        "published_generation_count": len(generations),
        "identity": {
            "alias_canonical": signal.canonical_node_id,
            "normalized_alias_canonical": extended.canonical_node_id,
            "novel_canonical": novel.canonical_node_id,
            "novel_replay_canonical": replay.canonical_node_id,
            "region_ids": region_ids,
        },
        "merge": {
            "plan_digest": digest,
            "merge_id": applied.details["receipt"]["merge_id"],
            "receipt_status": receipt_status,
            "repair_generation_id": str(receipt_generation),
            "deduplicated_edge_count": len(dry_plan["deduplicated_edges"]),
            "assertion_variance": dry_plan["deduplicated_edges"][0][
                "assertion_variance"
            ],
            "equivalence_evidence": dry_plan["deduplicated_edges"][0][
                "equivalence_evidence"
            ],
            "survivor_edge_unchanged": True,
            "survivor_edge": survivor_edge_after,
            "receipt_plan_digest": str(receipt_plan_digest),
        },
        "row_counts": {
            "before": before_counts,
            "after": {
                "target_nodes_live": len(
                    {
                        node_id
                        for node_id in region_ids
                        if node_id in {ids["signal"], ids["historical_duplicate"]}
                    }
                ),
                "target_edges_live": len(edges),
            },
        },
        "queues": {
            "extractor_retry_depth": retry_depth,
            "dlq_count": dead_letters,
            "runtime_status_counts": queue_depth,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--branch", default="default")
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    report = prove(args.dsn, args.deployment, args.branch)
    payload = json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
