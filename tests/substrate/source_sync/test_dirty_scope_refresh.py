"""Dirty-scope refresh refinement for DeltaProjection formal bindings."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    ComponentLineage,
    GraphLineage,
    LineageInvalidation,
    MaterializedLineageEnvelope,
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    SourceRecordLineage,
    publish_relation_artifact,
    relation_visible_rows_for_manifest,
    schedule_relation_artifacts,
    stable_hash,
)
from okg.substrate.derived_artifacts.change_frontier import build_change_frontier


pytestmark = pytest.mark.postgres


def _hash(label: str) -> str:
    return f"sha256:{label}"


def _generation_id() -> int:
    return 980_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:dirty-scope-refresh:{generation_id}",
            },
        )
    conn.commit()


def _definition(label: str) -> RelationArtifactDefinition:
    suffix = uuid4().hex
    return RelationArtifactDefinition(
        definition_id=f"test.dirty_scope_refresh.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"dirty_scope_refresh_{label}_{suffix}",
        compute_hash=stable_hash({"relation": label, "compute": "v1"}),
        output_schema_hash=stable_hash({"columns": {"label": "text"}}),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash({"dependencies": ["source_record", "graph_node"]}),
        output_contract={"columns": {"label": "text"}},
        dirty_rule={"dependencies": ["source_record", "graph_node"]},
    )


def _envelope(
    definition: RelationArtifactDefinition,
    *,
    output_id: str,
    record_id: str,
    generation_id: int,
) -> MaterializedLineageEnvelope:
    return MaterializedLineageEnvelope(
        output_id=output_id,
        output_kind="derived_relation",
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
            edge_ids=(),
        ),
        catalog_version_id=1,
        component=ComponentLineage(
            component_id=definition.definition_id,
            component_hash=_hash(f"{definition.definition_id}:v1"),
            component_kind="transform",
            version="v1",
        ),
        output_digest=_hash(f"{definition.definition_id}:{output_id}"),
        previous_output_digest=None,
        invalidation_scope="per-scope",
    )


def _publish_previous(
    conn: psycopg.Connection,
    definition: RelationArtifactDefinition,
    *,
    generation_id: int,
    output_id: str,
    record_id: str,
    rows: list[RelationRow],
):
    return publish_relation_artifact(
        conn,
        definition,
        target_generation_id=generation_id,
        rows=rows,
        lineage_envelopes=[
            _envelope(
                definition,
                output_id=output_id,
                record_id=record_id,
                generation_id=generation_id,
            )
        ],
    )


def test_dirty_scopes_cover_changed_dependents(clean_runtime: str) -> None:
    """A changed source/graph key refreshes only its dependent scope."""
    dirty = _definition("dirty")
    clean = _definition("clean")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _publish_previous(
            conn,
            dirty,
            generation_id=gen1,
            output_id="scope:dirty",
            record_id="commit:dirty",
            rows=[
                RelationRow(
                    scope_key="scope:dirty",
                    row_key="row:dirty",
                    row_data={"label": "dirty-v1"},
                ),
                RelationRow(
                    scope_key="scope:stable",
                    row_key="row:stable",
                    row_data={"label": "stable-v1"},
                ),
            ],
        )
        clean_previous = _publish_previous(
            conn,
            clean,
            generation_id=gen1,
            output_id="scope:clean",
            record_id="commit:clean",
            rows=[
                RelationRow(
                    scope_key="scope:clean",
                    row_key="row:clean",
                    row_data={"label": "clean-v1"},
                )
            ],
        )
        frontier = build_change_frontier(
            source_delta_batches=[
                {
                    "source_id": "repo-sync",
                    "contract": "valid",
                    "changed_record_ids": ["commit:dirty"],
                }
            ],
            graph_node_ids=["node:commit:dirty"],
        )

        result = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    dirty,
                    rows=[
                        RelationRow(
                            scope_key="scope:dirty",
                            row_key="row:dirty",
                            row_data={"label": "dirty-v2"},
                        ),
                        RelationRow(
                            scope_key="scope:stable",
                            row_key="row:stable",
                            row_data={"label": "stable-ignored"},
                        ),
                    ],
                ),
                RelationScheduleTask(
                    clean,
                    rows=[
                        RelationRow(
                            scope_key="scope:clean",
                            row_key="row:clean",
                            row_data={"label": "clean-v2-ignored"},
                        )
                    ],
                ),
            ],
            lineage_invalidations=frontier.invalidations,
        )
        nodes = {node.definition_id: node for node in result.nodes}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind, metrics->'lineage_planning'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": nodes[dirty.definition_id].run_id},
            )
            dirty_run = cur.fetchone()
            cur.execute(
                """
                SELECT reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": nodes[clean.definition_id].manifest_id},
            )
            clean_reuse = cur.fetchone()[0]

        visible = relation_visible_rows_for_manifest(
            conn,
            dirty,
            manifest_id=nodes[dirty.definition_id].manifest_id,
        )
        refreshed_rows = [
            (row.scope_key, dict(row.row_data))
            for row in sorted(visible, key=lambda r: r.scope_key)
        ]

    assert frontier.key_counts_by_kind == {
        "source_record": 1,
        "graph_node": 1,
    }
    assert result.lineage_plan is not None
    assert result.lineage_plan.available
    assert result.lineage_plan.dirty_output_count == 1
    assert result.lineage_plan.reusable_output_count == 1
    assert result.lineage_plan.dirty_scope_keys_by_definition == {
        dirty.definition_id: ("scope:dirty",)
    }
    assert result.lineage_plan.reusable_definition_ids == (clean.definition_id,)
    assert nodes[dirty.definition_id].status == "completed"
    assert nodes[dirty.definition_id].dirty_scope_count == 1
    assert nodes[clean.definition_id].status == "carried_forward"
    assert clean_reuse == clean_previous.manifest_id
    assert dirty_run[0] == "dirty"
    assert dirty_run[1]["dirty_scope_keys"] == ["scope:dirty"]
    assert refreshed_rows == [
        ("scope:dirty", {"label": "dirty-v2"}),
        ("scope:stable", {"label": "stable-v1"}),
    ]


def test_unavailable_lookup_falls_back_full_and_preserves_dirty_scope(
    clean_runtime: str,
) -> None:
    """Mixed covered and unavailable keys force full rebuild but keep dirty metrics."""
    dirty = _definition("mixed-dirty")
    clean = _definition("mixed-clean")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _publish_previous(
            conn,
            dirty,
            generation_id=gen1,
            output_id="scope:dirty",
            record_id="commit:dirty",
            rows=[
                RelationRow(
                    scope_key="scope:dirty",
                    row_key="row:dirty",
                    row_data={"label": "dirty-v1"},
                )
            ],
        )
        _publish_previous(
            conn,
            clean,
            generation_id=gen1,
            output_id="scope:clean",
            record_id="commit:clean",
            rows=[
                RelationRow(
                    scope_key="scope:clean",
                    row_key="row:clean",
                    row_data={"label": "clean-v1"},
                )
            ],
        )
        frontier = build_change_frontier(
            source_delta_batches=[
                {
                    "source_id": "repo-sync",
                    "contract": "valid",
                    "changed_record_ids": ["commit:dirty"],
                }
            ]
        )

        result = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    dirty,
                    rows=[
                        RelationRow(
                            scope_key="scope:dirty",
                            row_key="row:dirty",
                            row_data={"label": "dirty-v2"},
                        )
                    ],
                ),
                RelationScheduleTask(
                    clean,
                    rows=[
                        RelationRow(
                            scope_key="scope:clean",
                            row_key="row:clean",
                            row_data={"label": "clean-v2"},
                        )
                    ],
                ),
            ],
            lineage_invalidations=[
                *frontier.invalidations,
                # `decision` is an indexed kind, but this test fixture has no
                # decision lineage envelopes. The runtime must widen to full
                # while preserving the covered dirty source scope in metrics.
                LineageInvalidation("decision", "decision:missing"),
            ],
        )
        nodes = {node.definition_id: node for node in result.nodes}

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind, metrics->'lineage_planning'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": nodes[dirty.definition_id].run_id},
            )
            dirty_run = cur.fetchone()
            cur.execute(
                """
                SELECT trigger_kind, metrics->'lineage_planning'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": nodes[clean.definition_id].run_id},
            )
            clean_run = cur.fetchone()

    assert result.lineage_plan is not None
    assert not result.lineage_plan.available
    assert result.lineage_plan.unavailable_dependency_kinds == ("decision",)
    assert result.lineage_plan.dirty_output_count == 1
    assert result.lineage_plan.dirty_scope_keys_by_definition == {
        dirty.definition_id: ("scope:dirty",)
    }
    assert set(result.lineage_plan.fallback_definition_ids) == {
        dirty.definition_id,
        clean.definition_id,
    }
    assert result.lineage_plan.reusable_output_count == 0
    assert nodes[dirty.definition_id].status == "completed"
    assert nodes[dirty.definition_id].dirty_scope_count == 1
    assert nodes[clean.definition_id].status == "completed"
    assert nodes[clean.definition_id].dirty_scope_count == 0
    assert dirty_run[0] == "full"
    assert dirty_run[1]["force_full"] is True
    assert dirty_run[1]["dirty_scope_keys"] == ["scope:dirty"]
    assert clean_run[0] == "full"
    assert clean_run[1]["force_full"] is True
    assert clean_run[1]["dirty_scope_keys"] == []
