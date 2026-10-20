"""Lineage-driven scheduler invalidation tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.derived_artifacts.change_frontier import (
    lens_overlay_invalidations,
)
from okg.substrate.derived_artifacts.lineage_report import build_lineage_report
from okg.substrate.derived_artifacts import (
    ComponentLineage,
    GraphLineage,
    LineageInvalidation,
    MaterializedLineageEnvelope,
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    SourceRecordLineage,
    plan_lineage_invalidations,
    publish_relation_artifact,
    relation_visible_rows_for_manifest,
    schedule_relation_artifacts,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _hash(label: str) -> str:
    return f"sha256:{label}"


def _generation_id() -> int:
    return 930_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:lineage-scheduler:{generation_id}",
            },
        )
    conn.commit()


def _definition(label: str) -> RelationArtifactDefinition:
    suffix = uuid4().hex
    return RelationArtifactDefinition(
        definition_id=f"test.lineage.scheduler.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"lineage_scheduler_{label}_{suffix}",
        compute_hash=stable_hash({"relation": label, "compute": "v1"}),
        output_schema_hash=stable_hash({"columns": {"label": "text"}}),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash({"dependencies": ["source_record"]}),
        output_contract={"columns": {"label": "text"}},
        dirty_rule={"dependencies": ["source_record"]},
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
            edge_ids=(f"edge:{record_id}:touches",),
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


def test_lineage_planner_reports_dirty_reusable_and_unavailable_counts(
    clean_runtime: str,
) -> None:
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
        tasks = [
            RelationScheduleTask(dirty, rows=[]),
            RelationScheduleTask(clean, rows=[]),
        ]

        for invalidation in (
            LineageInvalidation("source_record", "repo-sync:commit:dirty"),
            LineageInvalidation("graph_node", "node:commit:dirty"),
            LineageInvalidation(
                "component_hash",
                _hash(f"{dirty.definition_id}:v1"),
            ),
        ):
            plan = plan_lineage_invalidations(
                conn,
                tasks=tasks,
                target_generation_id=gen2,
                invalidations=[invalidation],
            )

            assert plan.available
            assert plan.dirty_output_count == 1
            assert plan.reusable_output_count == 1
            assert plan.dirty_scope_keys_by_definition == {
                dirty.definition_id: ("scope:dirty",)
            }
            assert plan.reusable_definition_ids == (clean.definition_id,)

        unavailable = plan_lineage_invalidations(
            conn,
            tasks=tasks,
            target_generation_id=gen2,
            invalidations=[LineageInvalidation("decision", "decision:missing")],
        )

    assert not unavailable.available
    assert unavailable.status == "unavailable"
    assert unavailable.unavailable_dependency_kinds == ("decision",)
    assert set(unavailable.fallback_definition_ids) == {
        dirty.definition_id,
        clean.definition_id,
    }
    assert unavailable.reusable_output_count == 0


def test_scheduler_refreshes_lineage_dirty_scope_and_carries_reusable_output(
    clean_runtime: str,
) -> None:
    dirty = _definition("refresh")
    clean = _definition("carry")
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
            lineage_invalidations=[
                LineageInvalidation("source_record", "repo-sync:commit:dirty")
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

    assert result.lineage_plan is not None
    assert result.lineage_plan.available
    assert result.lineage_plan.dirty_output_count == 1
    assert result.lineage_plan.reusable_output_count == 1
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


def test_lens_overlay_frontier_refreshes_downstream_relation(
    clean_runtime: str,
) -> None:
    dirty = _definition("lens-overlay-dirty")
    clean = _definition("lens-overlay-clean")
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
                            row_data={"label": "clean-v2-ignored"},
                        )
                    ],
                ),
            ],
            lineage_invalidations=lens_overlay_invalidations([
                {"source_record_id": {"node_id": "node:commit:dirty"}}
            ]),
        )
        nodes = {node.definition_id: node for node in result.nodes}

        with conn.cursor() as cur:
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

    assert result.lineage_plan is not None
    assert result.lineage_plan.available
    assert result.lineage_plan.dirty_scope_keys_by_definition == {
        dirty.definition_id: ("scope:dirty",)
    }
    assert nodes[dirty.definition_id].dirty_scope_count == 1
    assert nodes[clean.definition_id].status == "carried_forward"
    assert clean_reuse == clean_previous.manifest_id
    assert [(row.scope_key, row.row_data["label"]) for row in visible] == [
        ("scope:dirty", "dirty-v2")
    ]


def test_scheduler_falls_back_to_full_recompute_for_unavailable_lookup(
    clean_runtime: str,
) -> None:
    definition = _definition("fallback")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _publish_previous(
            conn,
            definition,
            generation_id=gen1,
            output_id="scope:all",
            record_id="commit:all",
            rows=[
                RelationRow(
                    scope_key="scope:all",
                    row_key="row:all",
                    row_data={"label": "v1"},
                )
            ],
        )

        result = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    definition,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="row:all",
                            row_data={"label": "v2"},
                        )
                    ],
                )
            ],
            lineage_invalidations=[
                LineageInvalidation("decision", "decision:not-indexed")
            ],
        )
        node = result.nodes[0]
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind, metrics->'lineage_planning'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": node.run_id},
            )
            run = cur.fetchone()

    assert result.lineage_plan is not None
    assert not result.lineage_plan.available
    assert result.lineage_plan.unavailable_dependency_kinds == ("decision",)
    assert result.lineage_plan.fallback_definition_ids == (
        definition.definition_id,
    )
    assert node.status == "completed"
    assert node.dirty_scope_count == 0
    assert run[0] == "full"
    assert run[1]["force_full"] is True


def test_z_lineage_report_includes_planning_summary(
    clean_runtime: str,
) -> None:
    dirty = _definition("report-dirty")
    clean = _definition("report-clean")
    gen1 = _generation_id()
    gen2 = gen1 + 1
    gen3 = gen1 + 2

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _insert_generation(conn, gen3)
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
        tasks = [
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
        ]
        schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=tasks,
            lineage_invalidations=[
                LineageInvalidation("source_record", "repo-sync:commit:dirty")
            ],
        )
        schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen3,
            tasks=tasks,
            lineage_invalidations=[
                LineageInvalidation("decision", "decision:not-indexed")
            ],
        )

        report = build_lineage_report(conn, deployment="test")

    assert report["status"] == "available"
    assert report["summary"]["envelope_count"] >= 2
    assert report["planning_summary"]["max_dirty_output_count"] == 1
    assert report["planning_summary"]["max_reusable_output_count"] == 1
    assert report["planning_summary"]["unavailable_run_count"] == 2
    assert report["planning_summary"]["force_full_run_count"] == 2
    assert report["planning_summary"]["unavailable_dependency_kinds"] == [
        "decision"
    ]
