"""Materialized output dependency index tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    ComponentLineage,
    DependencyLookupKey,
    GraphLineage,
    MaterializedLineageEnvelope,
    RelationArtifactDefinition,
    RelationRow,
    SourceRecordLineage,
    lookup_dirty_outputs,
    publish_lineage_envelopes,
    publish_relation_artifact,
    stable_hash,
    summarize_lineage_index,
)


pytestmark = pytest.mark.postgres


def _hash(label: str) -> str:
    return f"sha256:{label}"


def _definition() -> RelationArtifactDefinition:
    suffix = uuid4().hex
    return RelationArtifactDefinition(
        definition_id=f"test.lineage.{suffix}",
        deployment_name="test",
        logical_relation_id=f"lineage_summary_{suffix}",
        compute_hash=stable_hash({"sql": "select lineage"}),
        output_schema_hash=stable_hash({"columns": {"id": "text"}}),
        renderer_hash=stable_hash({"renderer": "relation-json"}),
        dirty_rule_hash=stable_hash({"dependencies": ["source_record"]}),
        output_contract={"columns": {"id": "text"}},
        dirty_rule={"dependencies": ["source_record"]},
    )


def _generation_id() -> int:
    return 90_000 + (uuid4().int % 1_000_000)


def _insert_generation(conn: psycopg.Connection, generation_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, 'test', 'published', 1, now(),
              %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "correlation_id": f"test:lineage-index:{generation_id}",
            },
        )
    conn.commit()


def _publish_manifest(
    conn: psycopg.Connection,
    *,
    generation_id: int,
) -> tuple[RelationArtifactDefinition, object]:
    definition = _definition()
    published = publish_relation_artifact(
        conn,
        definition,
        target_generation_id=generation_id,
        rows=[
            RelationRow(
                scope_key="repo:akmon",
                row_key="summary",
                row_data={"id": "summary"},
                dependency_hash="dep:commit:1",
            )
        ],
    )
    return definition, published


def _envelope(
    *,
    output_id: str = "page:repo-summary",
    output_kind: str = "generated_page",
    record_id: str = "commit:1",
    graph_generation_id: int = 1,
    graph_nodes: tuple[str, ...] = ("node:repo",),
    graph_edges: tuple[str, ...] = ("edge:repo:commit",),
    component_hash: str = "component:v1",
    decision_ids: tuple[str, ...] = (),
) -> MaterializedLineageEnvelope:
    return MaterializedLineageEnvelope(
        output_id=output_id,
        output_kind=output_kind,
        source_records=[
            SourceRecordLineage(
                source_id="repo-sync",
                record_id=record_id,
                content_hash=_hash(record_id),
                source_revision={"commit": record_id},
            )
        ],
        graph=GraphLineage(
            graph_generation_id=graph_generation_id,
            graph_branch="main",
            node_ids=graph_nodes,
            edge_ids=graph_edges,
        ),
        catalog_version_id=1,
        component=ComponentLineage(
            component_id="repo.summary.renderer",
            component_hash=_hash(component_hash),
            component_kind="renderer",
            version="v1",
        ),
        output_digest=_hash(f"{output_id}:{record_id}"),
        previous_output_digest=None,
        invalidation_scope="per-output",
        proposal_ids=("proposal:lineage",),
        evidence_ids=("evidence:lineage",),
        decision_ids=decision_ids,
    )


def test_migrate_creates_materialized_dependency_index_tables(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('okg.derived_output_lineage_envelopes'),
                   to_regclass('okg.derived_output_dependency_index')
            """
        )
        assert cur.fetchone() == (
            "okg.derived_output_lineage_envelopes",
            "okg.derived_output_dependency_index",
        )


def test_dependency_index_finds_dirty_outputs_by_source_graph_and_component(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _, published = _publish_manifest(conn, generation_id=generation_id)
        result = publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[_envelope(graph_generation_id=generation_id)],
        )

        assert result.envelope_count == 1
        assert result.dependency_key_count > 0
        assert {"source_record", "graph_node", "component_hash"} <= set(
            result.dependency_kinds
        )

        source_lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("source_record", "repo-sync:commit:1")
            ],
        )
        graph_lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("graph_node", "node:repo"),
                DependencyLookupKey("graph_edge", "edge:repo:commit"),
            ],
        )
        component_lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey(
                    "component_hash",
                    _hash("component:v1"),
                )
            ],
        )

    assert source_lookup.available
    assert [output.output_id for output in source_lookup.dirty_outputs] == [
        "page:repo-summary"
    ]
    assert graph_lookup.available
    assert [output.output_id for output in graph_lookup.dirty_outputs] == [
        "page:repo-summary"
    ]
    assert component_lookup.available
    assert [output.invalidation_scope for output in component_lookup.dirty_outputs] == [
        "per-output"
    ]


def test_missing_ref_for_covered_kind_is_available_empty(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _, published = _publish_manifest(conn, generation_id=generation_id)
        publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[_envelope(graph_generation_id=generation_id)],
        )

        lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("source_record", "repo-sync:commit:missing")
            ],
        )

    assert lookup.available
    assert lookup.dirty_outputs == ()
    assert lookup.missing_dependency_kinds == ()


def test_uncovered_dependency_kind_reports_unavailable_not_empty(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _, published = _publish_manifest(conn, generation_id=generation_id)
        publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[_envelope(graph_generation_id=generation_id)],
        )

        lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("decision", "decision:not-covered")
            ],
        )

    assert not lookup.available
    assert lookup.status == "unavailable"
    assert lookup.dirty_outputs == ()
    assert lookup.missing_dependency_kinds == ("decision",)


def test_republishing_manifest_replaces_stale_dependency_keys(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _, published = _publish_manifest(conn, generation_id=generation_id)
        publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[_envelope(graph_generation_id=generation_id)],
        )
        publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[
                _envelope(
                    record_id="commit:2",
                    graph_generation_id=generation_id,
                    component_hash="component:v2",
                )
            ],
        )

        old_lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("source_record", "repo-sync:commit:1")
            ],
        )
        new_lookup = lookup_dirty_outputs(
            conn,
            dependency_keys=[
                DependencyLookupKey("source_record", "repo-sync:commit:2")
            ],
        )

    assert old_lookup.available
    assert old_lookup.dirty_outputs == ()
    assert new_lookup.available
    assert [output.output_digest for output in new_lookup.dirty_outputs] == [
        _hash("page:repo-summary:commit:2")
    ]


def test_lineage_index_summary_reports_coverage_counts(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _, published = _publish_manifest(conn, generation_id=generation_id)
        publish_result = publish_lineage_envelopes(
            conn,
            manifest_id=published.manifest_id,
            envelopes=[
                _envelope(
                    output_id="page:repo-summary",
                    output_kind="generated_page",
                    record_id="commit:1",
                    graph_generation_id=generation_id,
                    decision_ids=("decision:lineage",),
                ),
                _envelope(
                    output_id="native-view:repo-summary",
                    output_kind="native_view",
                    record_id="commit:2",
                    graph_generation_id=generation_id,
                    graph_nodes=("node:view",),
                    graph_edges=(),
                    component_hash="component:view:v1",
                ),
            ],
        )

        summary = summarize_lineage_index(
            conn,
            target_generation_id=generation_id,
        )

    assert summary.envelope_count == 2
    assert summary.dependency_key_count == publish_result.dependency_key_count
    assert summary.output_kind_counts == {
        "generated_page": 1,
        "native_view": 1,
    }
    assert summary.dependency_kind_counts["source_record"] == 2
    assert summary.dependency_kind_counts["component_hash"] == 2
    assert "source_record" not in summary.unavailable_dependency_kinds
    assert "decision" not in summary.unavailable_dependency_kinds
