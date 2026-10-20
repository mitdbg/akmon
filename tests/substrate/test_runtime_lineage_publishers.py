"""Runtime lineage publication tests for materialized artifact publishers."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    LINEAGE_STATUS_AVAILABLE,
    LINEAGE_STATUS_UNAVAILABLE,
    ComponentLineage,
    GraphLineage,
    InlineViewOutput,
    LineageIndexError,
    MaterializedLineageEnvelope,
    RelationArtifactDefinition,
    RelationRow,
    SourceRecordLineage,
    ViewArtifactDefinition,
    publish_relation_artifact,
    publish_view_artifact,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _hash(label: str) -> str:
    return f"sha256:{label}"


def _generation_id() -> int:
    return 910_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:runtime-lineage:{generation_id}",
            },
        )
    conn.commit()


def _relation_definition(label: str) -> RelationArtifactDefinition:
    suffix = uuid4().hex
    return RelationArtifactDefinition(
        definition_id=f"test.runtime.lineage.relation.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"runtime_lineage_{label}_{suffix}",
        compute_hash=stable_hash({"relation": label, "compute": "v1"}),
        output_schema_hash=stable_hash({"columns": {"id": "text"}}),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash({"dependencies": ["source_record"]}),
        output_contract={"columns": {"id": "text"}},
        dirty_rule={"dependencies": ["source_record"]},
    )


def _view_definition(label: str) -> ViewArtifactDefinition:
    suffix = uuid4().hex
    return ViewArtifactDefinition(
        definition_id=f"test.runtime.lineage.view.{label}.{suffix}",
        deployment_name="test",
        view_id=f"runtime-lineage-{label}-{suffix}",
        view_name=f"Runtime lineage {label}",
        compute_hash=stable_hash({"view": label, "compute": "v1"}),
        output_schema_hash=stable_hash({"slots": ["markdown", "json"]}),
        renderer_hash=stable_hash({"renderer": "markdown"}),
        dirty_rule_hash=stable_hash({"dependencies": ["source_record"]}),
        output_contract={"slots": ["markdown", "json"]},
        dirty_rule={"dependencies": ["source_record"]},
    )


def _envelope(
    *,
    output_id: str,
    output_kind: str,
    record_id: str,
    generation_id: int,
    component_id: str,
    component_kind: str,
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
            graph_generation_id=generation_id,
            graph_branch="main",
            node_ids=(f"node:{record_id}",),
            edge_ids=(f"edge:{record_id}:touches",),
        ),
        catalog_version_id=1,
        component=ComponentLineage(
            component_id=component_id,
            component_hash=_hash(f"{component_id}:v1"),
            component_kind=component_kind,
            version="v1",
        ),
        output_digest=_hash(output_id),
        previous_output_digest=None,
        invalidation_scope="per-output",
    )


def test_publishers_persist_lineage_envelopes_and_reverse_keys(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    relation = _relation_definition("indexed")
    view = _view_definition("indexed")

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        relation_envelope = _envelope(
            output_id=f"relation:{relation.logical_relation_id}",
            output_kind="derived_relation",
            record_id="commit:relation",
            generation_id=generation_id,
            component_id=relation.definition_id,
            component_kind="transform",
        )
        view_envelope = _envelope(
            output_id=f"view:{view.view_id}:markdown",
            output_kind="native_view",
            record_id="commit:view",
            generation_id=generation_id,
            component_id=view.definition_id,
            component_kind="view_component",
        )

        published_relation = publish_relation_artifact(
            conn,
            relation,
            target_generation_id=generation_id,
            rows=[
                RelationRow(
                    scope_key="repo:okg",
                    row_key="summary",
                    row_data={"id": "summary"},
                    dependency_hash=_hash("commit:relation"),
                )
            ],
            lineage_envelopes=[relation_envelope],
        )
        published_view = publish_view_artifact(
            conn,
            view,
            target_generation_id=generation_id,
            outputs=[
                InlineViewOutput.markdown("# Runtime lineage"),
                InlineViewOutput.json({"runtime_lineage": True}),
            ],
            dependency_manifest_ids=[published_relation.manifest_id],
            lineage_envelopes=[view_envelope],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.manifest_id, e.output_kind, count(i.dependency_index_id)
                  FROM okg.derived_output_lineage_envelopes e
                  JOIN okg.derived_output_dependency_index i
                    ON i.lineage_envelope_id = e.lineage_envelope_id
                 WHERE e.manifest_id = ANY(%(manifest_ids)s)
                 GROUP BY e.manifest_id, e.output_kind
                 ORDER BY e.output_kind
                """,
                {
                    "manifest_ids": [
                        published_relation.manifest_id,
                        published_view.manifest_id,
                    ]
                },
            )
            indexed_rows = cur.fetchall()
            cur.execute(
                """
                SELECT run_id, metrics->'lineage'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = ANY(%(run_ids)s)
                 ORDER BY run_id
                """,
                {
                    "run_ids": [
                        published_relation.run_id,
                        published_view.run_id,
                    ]
                },
            )
            lineage_metrics = {row[0]: row[1] for row in cur.fetchall()}

    assert published_relation.lineage_status == LINEAGE_STATUS_AVAILABLE
    assert published_relation.lineage_envelope_count == 1
    assert published_relation.lineage_dependency_key_count > 0
    assert published_view.lineage_status == LINEAGE_STATUS_AVAILABLE
    assert published_view.lineage_envelope_count == 1
    assert published_view.lineage_dependency_key_count > 0
    assert indexed_rows == [
        (published_relation.manifest_id, "derived_relation", 6),
        (published_view.manifest_id, "native_view", 6),
    ]
    assert lineage_metrics[published_relation.run_id]["status"] == (
        LINEAGE_STATUS_AVAILABLE
    )
    assert lineage_metrics[published_view.run_id]["status"] == (
        LINEAGE_STATUS_AVAILABLE
    )


def test_publishers_mark_missing_lineage_unavailable(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    relation = _relation_definition("missing")
    view = _view_definition("missing")

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        published_relation = publish_relation_artifact(
            conn,
            relation,
            target_generation_id=generation_id,
            rows=[
                RelationRow(
                    scope_key="repo:okg",
                    row_key="summary",
                    row_data={"id": "summary"},
                )
            ],
        )
        published_view = publish_view_artifact(
            conn,
            view,
            target_generation_id=generation_id,
            outputs=[InlineViewOutput.markdown("# Missing lineage")],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.derived_output_lineage_envelopes
                 WHERE manifest_id = ANY(%(manifest_ids)s)
                """,
                {
                    "manifest_ids": [
                        published_relation.manifest_id,
                        published_view.manifest_id,
                    ]
                },
            )
            envelope_count = cur.fetchone()[0]
            cur.execute(
                """
                SELECT run_id, metrics->'lineage'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = ANY(%(run_ids)s)
                 ORDER BY run_id
                """,
                {
                    "run_ids": [
                        published_relation.run_id,
                        published_view.run_id,
                    ]
                },
            )
            lineage_metrics = {row[0]: row[1] for row in cur.fetchall()}

    assert envelope_count == 0
    assert published_relation.lineage_status == LINEAGE_STATUS_UNAVAILABLE
    assert published_view.lineage_status == LINEAGE_STATUS_UNAVAILABLE
    assert lineage_metrics[published_relation.run_id] == {
        "status": LINEAGE_STATUS_UNAVAILABLE,
        "reason": "lineage_envelopes_not_provided",
        "envelope_count": 0,
        "dependency_key_count": 0,
        "dependency_kinds": [],
    }
    assert lineage_metrics[published_view.run_id]["status"] == (
        LINEAGE_STATUS_UNAVAILABLE
    )


def test_empty_lineage_input_fails_before_manifest_publication(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    relation = _relation_definition("empty")

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        with pytest.raises(LineageIndexError, match="at least one envelope"):
            publish_relation_artifact(
                conn,
                relation,
                target_generation_id=generation_id,
                rows=[
                    RelationRow(
                        scope_key="repo:okg",
                        row_key="summary",
                        row_data={"id": "summary"},
                    )
                ],
                lineage_envelopes=[],
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.derived_output_manifests
                 WHERE definition_id = %(definition_id)s
                """,
                {"definition_id": relation.definition_id},
            )
            manifest_count = cur.fetchone()[0]

    assert manifest_count == 0
