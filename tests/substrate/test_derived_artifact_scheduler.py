"""Derived artifact scheduler/DAG proof-slice tests."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.derived_artifacts import (
    DerivedArtifactDependency,
    DerivedArtifactScheduleError,
    DirtyScope,
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    ViewArtifactDefinition,
    ViewScheduleTask,
    enqueue_view_dirty_scope,
    get_view_slot,
    pending_dirty_scope_keys,
    register_dependency,
    register_jsonb_relation,
    register_view_artifact_definition,
    relation_visible_rows_for_manifest,
    schedule_derived_artifacts,
    schedule_relation_artifacts,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _definition(label: str) -> RelationArtifactDefinition:
    suffix = uuid4().hex[:12]
    output_contract = {
        "columns": {
            "label": "text",
            "value": "text",
        }
    }
    dirty_rule = {
        "scope_key": "scope_key",
        "dependencies": ["test.fixture"],
    }
    return RelationArtifactDefinition(
        definition_id=f"test.scheduler.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"scheduler_{label}_{suffix}",
        compute_hash=stable_hash({"label": label, "compute": suffix}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _view_definition(label: str) -> ViewArtifactDefinition:
    suffix = uuid4().hex[:12]
    output_contract = {
        "slots": ["markdown", "json"],
        "template": label,
    }
    dirty_rule = {
        "scope_key": "view",
        "dependencies": ["derived_artifact_manifest"],
    }
    return ViewArtifactDefinition(
        definition_id=f"test.scheduler.view.{label}.{suffix}",
        deployment_name="test",
        view_id=f"test.scheduler.view.{label}.{suffix}",
        view_name=f"Scheduled {label}",
        compute_hash=stable_hash({"view": label, "compute": suffix}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "markdown+json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 200_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:scheduler:{generation_id}",
            },
        )
    conn.commit()


def test_scheduler_runs_dependencies_in_topological_order_and_binds_manifest(
    clean_runtime: str,
) -> None:
    upstream = _definition("upstream")
    downstream = _definition("downstream")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        register_jsonb_relation(conn, upstream)
        register_jsonb_relation(conn, downstream)
        register_dependency(
            conn,
            DerivedArtifactDependency(
                downstream_definition_id=downstream.definition_id,
                upstream_definition_id=upstream.definition_id,
                required_output_schema_hash=upstream.output_schema_hash,
                metadata={"reason": "downstream renders upstream rows"},
            ),
        )

        result = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=generation_id,
            tasks=[
                RelationScheduleTask(
                    downstream,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="downstream:1",
                            row_data={"label": "downstream", "value": "ok"},
                        )
                    ],
                    dirty_scope_keys=[],
                ),
                RelationScheduleTask(
                    upstream,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="upstream:1",
                            row_data={"label": "upstream", "value": "ok"},
                        )
                    ],
                    dirty_scope_keys=[],
                ),
            ],
        )

        assert [node.definition_id for node in result.nodes] == [
            upstream.definition_id,
            downstream.definition_id,
        ]
        downstream_node = result.nodes[1]
        upstream_node = result.nodes[0]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status
                  FROM okg.derived_schedule_plans
                 WHERE plan_id = %(plan_id)s
                """,
                {"plan_id": result.plan_id},
            )
            assert cur.fetchone()[0] == "completed"

            cur.execute(
                """
                SELECT definition_id, topo_index, status, run_id
                  FROM okg.derived_schedule_nodes
                 WHERE plan_id = %(plan_id)s
                 ORDER BY topo_index
                """,
                {"plan_id": result.plan_id},
            )
            assert cur.fetchall() == [
                (
                    upstream.definition_id,
                    0,
                    "completed",
                    upstream_node.run_id,
                ),
                (
                    downstream.definition_id,
                    1,
                    "completed",
                    downstream_node.run_id,
                ),
            ]

            cur.execute(
                """
                SELECT upstream_manifest_id, upstream_target_generation_id,
                       required_output_schema_hash,
                       bound_output_schema_hash, metadata
                  FROM okg.derived_run_dependency_bindings
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": downstream_node.run_id},
            )
            binding = cur.fetchone()

    assert binding == (
        upstream_node.manifest_id,
        generation_id,
        upstream.output_schema_hash,
        upstream.output_schema_hash,
        {"reason": "downstream renders upstream rows"},
    )


def test_scheduler_rejects_dependency_cycle(clean_runtime: str) -> None:
    left = _definition("left")
    right = _definition("right")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        register_jsonb_relation(conn, left)
        register_jsonb_relation(conn, right)
        register_dependency(
            conn,
            DerivedArtifactDependency(
                downstream_definition_id=left.definition_id,
                upstream_definition_id=right.definition_id,
            ),
        )
        register_dependency(
            conn,
            DerivedArtifactDependency(
                downstream_definition_id=right.definition_id,
                upstream_definition_id=left.definition_id,
            ),
        )

        with pytest.raises(DerivedArtifactScheduleError, match="cycle"):
            schedule_relation_artifacts(
                conn,
                deployment_name="test",
                target_generation_id=generation_id,
                tasks=[
                    RelationScheduleTask(left, rows=[], dirty_scope_keys=[]),
                    RelationScheduleTask(right, rows=[], dirty_scope_keys=[]),
                ],
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.derived_schedule_plans
                 WHERE target_generation_id = %(generation_id)s
                """,
                {"generation_id": generation_id},
            )
            assert cur.fetchone()[0] == 0


def test_scheduler_carries_forward_and_refreshes_dirty_scope(
    clean_runtime: str,
) -> None:
    definition = _definition("refresh")
    gen1 = _generation_id()
    gen2 = gen1 + 1
    gen3 = gen1 + 2
    initial_rows = [
        RelationRow(
            scope_key="scope:a",
            row_key="row:a",
            row_data={"label": "a", "value": "one"},
        ),
        RelationRow(
            scope_key="scope:b",
            row_key="row:b",
            row_data={"label": "b", "value": "one"},
        ),
    ]
    refreshed_rows = [
        RelationRow(
            scope_key="scope:a",
            row_key="row:a",
            row_data={"label": "a", "value": "one"},
        ),
        RelationRow(
            scope_key="scope:b",
            row_key="row:b2",
            row_data={"label": "b", "value": "two"},
        ),
    ]

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        _insert_generation(conn, gen3)
        register_jsonb_relation(conn, definition)

        first = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                RelationScheduleTask(
                    definition,
                    rows=initial_rows,
                    dirty_scope_keys=[],
                )
            ],
        )
        carried = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    definition,
                    rows=initial_rows,
                    dirty_scope_keys=[],
                )
            ],
        )
        refreshed = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen3,
            tasks=[
                RelationScheduleTask(
                    definition,
                    rows=refreshed_rows,
                    dirty_scope_keys=["scope:b"],
                )
            ],
        )

        assert first.nodes[0].status == "completed"
        assert carried.nodes[0].status == "carried_forward"
        assert refreshed.nodes[0].dirty_scope_count == 1

        visible = relation_visible_rows_for_manifest(
            conn,
            definition,
            manifest_id=refreshed.nodes[0].manifest_id,
        )
        rows = [
            (row.scope_key, row.row_key, dict(row.row_data))
            for row in sorted(visible, key=lambda r: (r.scope_key, r.row_key))
        ]

    assert rows == [
        ("scope:a", "row:a", {"label": "a", "value": "one"}),
        ("scope:b", "row:b2", {"label": "b", "value": "two"}),
    ]


def test_scheduler_recomputes_relation_on_definition_hash_change(
    clean_runtime: str,
) -> None:
    definition = _definition("relation-hash-change")
    changed = replace(
        definition,
        compute_hash=stable_hash(
            {
                "definition_id": definition.definition_id,
                "compute": "changed",
            }
        ),
    )
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        register_jsonb_relation(conn, definition)
        first = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                RelationScheduleTask(
                    definition,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="row:1",
                            row_data={"label": "v1", "value": "one"},
                        )
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        second = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    changed,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="row:1",
                            row_data={"label": "v2", "value": "two"},
                        )
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": second.nodes[0].run_id},
            )
            trigger_kind = cur.fetchone()[0]

            cur.execute(
                """
                SELECT reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": second.nodes[0].manifest_id},
            )
            reused_from_manifest_id = cur.fetchone()[0]

            cur.execute(
                """
                SELECT storage_ref
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'relation'
                """,
                {"manifest_id": second.nodes[0].manifest_id},
            )
            storage_ref = cur.fetchone()[0]

            cur.execute(
                sql.SQL(
                    """
                    SELECT row_data
                      FROM {}.{}
                     WHERE definition_id = %(definition_id)s
                       AND target_generation_id = %(generation_id)s
                       AND row_key = 'row:1'
                    """
                ).format(
                    sql.Identifier(storage_ref["schema"]),
                    sql.Identifier(storage_ref["relation"]),
                ),
                {
                    "definition_id": changed.definition_id,
                    "generation_id": gen2,
                },
            )
            row_data = cur.fetchone()[0]

    assert first.nodes[0].status == "completed"
    assert second.nodes[0].status == "completed"
    assert trigger_kind == "full"
    assert reused_from_manifest_id is None
    assert row_data == {"label": "v2", "value": "two"}


def test_scheduler_runs_relation_then_native_view_without_graph_projection(
    clean_runtime: str,
) -> None:
    upstream = _definition("view-upstream")
    view = _view_definition("brief")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        register_jsonb_relation(conn, upstream)
        register_view_artifact_definition(conn, view)
        register_dependency(
            conn,
            DerivedArtifactDependency(
                downstream_definition_id=view.definition_id,
                upstream_definition_id=upstream.definition_id,
                required_output_schema_hash=upstream.output_schema_hash,
                metadata={"reason": "view renders relation summary"},
            ),
        )

        result = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=generation_id,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Brief\n\nUses relation."),
                        InlineViewOutput.json({"uses_relation": True}),
                    ],
                    dirty_scope_keys=[],
                ),
                RelationScheduleTask(
                    upstream,
                    rows=[
                        RelationRow(
                            scope_key="scope:all",
                            row_key="row:1",
                            row_data={"label": "upstream", "value": "ok"},
                        )
                    ],
                    dirty_scope_keys=[],
                ),
            ],
        )

        assert [node.definition_id for node in result.nodes] == [
            upstream.definition_id,
            view.definition_id,
        ]
        upstream_node = result.nodes[0]
        view_node = result.nodes[1]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT upstream_manifest_id, upstream_target_generation_id,
                       required_output_schema_hash,
                       bound_output_schema_hash, metadata
                  FROM okg.derived_run_dependency_bindings
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": view_node.run_id},
            )
            binding = cur.fetchone()

            cur.execute(
                """
                SELECT slot_name, media_type, inline_payload
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                 ORDER BY slot_name
                """,
                {"manifest_id": view_node.manifest_id},
            )
            slots = cur.fetchall()
            cur.execute(
                """
                SELECT run_id, metrics->'registry_row'
                  FROM okg.derived_artifact_runs
                 WHERE run_id = ANY(%(run_ids)s)
                """,
                {
                    "run_ids": [
                        upstream_node.run_id,
                        view_node.run_id,
                    ]
                },
            )
            registry_rows = {row[0]: row[1] for row in cur.fetchall()}

        # The scheduler must not project a `view` compatibility node into
        # the graph. Post-cutover the graph lives in Chronos; assert via a
        # checkout that no `view`-subtype node exists.
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        projected_node_count = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND subtype = 'view'
            """,
            {"node_id": f"view:{view.view_id}"},
        )[0]["n"]

    assert binding == (
        upstream_node.manifest_id,
        generation_id,
        upstream.output_schema_hash,
        upstream.output_schema_hash,
        {"reason": "view renders relation summary"},
    )
    assert slots == [
        (
            "json",
            "application/json",
            {"kind": "json", "value": {"uses_relation": True}},
        ),
        (
            "markdown",
            "text/markdown",
            {"kind": "text", "text": "# Brief\n\nUses relation."},
        ),
    ]
    relation_row = registry_rows[upstream_node.run_id]
    view_row = registry_rows[view_node.run_id]
    assert relation_row["scheduler_kind"] == "relation"
    assert relation_row["determinism"] == "deterministic"
    assert relation_row["oracle"] == "CorrectnessOracle"
    assert relation_row["materializations"][0]["kind"] == "side_table"
    assert relation_row["raw_dirty_rule_deps"] == ["test.fixture"]
    assert relation_row["unsupported_dirty_rule_deps"] == ["test.fixture"]
    assert relation_row["dependency_fallback_used"] is True
    assert view_row["scheduler_kind"] == "view"
    assert view_row["determinism"] == "judgment"
    assert view_row["oracle"] == "ProjectionParityOracle"
    assert view_row["override"] is True
    assert "decision" in view_row["declared_deps"]
    assert view_row["raw_dirty_rule_deps"] == ["derived_artifact_manifest"]
    assert view_row["unsupported_dirty_rule_deps"] == ["derived_artifact_manifest"]
    assert view_row["dependency_fallback_used"] is True
    assert projected_node_count == 0


def test_scheduler_carries_forward_native_view(
    clean_runtime: str,
) -> None:
    view = _view_definition("carry-forward")
    gen1 = _generation_id()
    gen2 = gen1 + 1
    outputs = [
        InlineViewOutput.markdown("# Stable View\n\nNo changes."),
        InlineViewOutput.json({"stable": True}),
    ]

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        first = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=outputs,
                    dirty_scope_keys=[],
                )
            ],
        )
        carried = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=outputs,
                    dirty_scope_keys=[],
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": carried.nodes[0].manifest_id},
            )
            manifest = cur.fetchone()

        storage_ref = get_view_slot(
            conn,
            manifest_id=carried.nodes[0].manifest_id,
            slot_name="markdown",
        ).storage_ref

    assert first.nodes[0].status == "completed"
    assert carried.nodes[0].status == "carried_forward"
    assert manifest == ("completed", first.nodes[0].manifest_id)
    assert storage_ref["source_generation_id"] == gen1
    assert storage_ref["target_generation_id"] == gen2
    assert storage_ref["reused_from_manifest_id"] == (
        str(first.nodes[0].manifest_id)
    )


def test_scheduler_recomputes_native_view_on_definition_hash_change(
    clean_runtime: str,
) -> None:
    view = _view_definition("view-hash-change")
    changed = replace(
        view,
        compute_hash=stable_hash(
            {"definition_id": view.definition_id, "compute": "changed"}
        ),
    )
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        first = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Hash View\n\nVersion 1."),
                        InlineViewOutput.json({"version": 1}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        second = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    changed,
                    outputs=[
                        InlineViewOutput.markdown("# Hash View\n\nVersion 2."),
                        InlineViewOutput.json({"version": 2}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": second.nodes[0].run_id},
            )
            trigger_kind = cur.fetchone()[0]

            cur.execute(
                """
                SELECT reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": second.nodes[0].manifest_id},
            )
            reused_from_manifest_id = cur.fetchone()[0]

            cur.execute(
                """
                SELECT inline_payload
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'markdown'
                """,
                {"manifest_id": second.nodes[0].manifest_id},
            )
            markdown_payload = cur.fetchone()[0]

    assert first.nodes[0].status == "completed"
    assert second.nodes[0].status == "completed"
    assert trigger_kind == "full"
    assert reused_from_manifest_id is None
    assert markdown_payload == {
        "kind": "text",
        "text": "# Hash View\n\nVersion 2.",
    }


def test_scheduler_refreshes_dirty_native_view_and_consumes_scope(
    clean_runtime: str,
) -> None:
    view = _view_definition("dirty")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Dirty View\n\nVersion 1."),
                        InlineViewOutput.json({"version": 1}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        enqueue_view_dirty_scope(
            conn,
            view,
            target_generation_id=gen2,
            dirty_scope=DirtyScope(
                scope_key="view",
                dirty_reason="source_changed",
                dependency_kind="node_text_hash",
                dependency_ref="node:a",
                dependency_hash="h2",
            ),
        )
        assert pending_dirty_scope_keys(
            conn,
            view,
            target_generation_id=gen2,
        ) == ["view"]

        refreshed = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown("# Dirty View\n\nVersion 2."),
                        InlineViewOutput.json({"version": 2}),
                    ],
                    dirty_scope_keys=None,
                )
            ],
        )

        assert pending_dirty_scope_keys(
            conn,
            view,
            target_generation_id=gen2,
        ) == []

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind, dirty_scopes, dirty_rows
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": refreshed.nodes[0].run_id},
            )
            run = cur.fetchone()

            cur.execute(
                """
                SELECT inline_payload
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'markdown'
                """,
                {"manifest_id": refreshed.nodes[0].manifest_id},
            )
            markdown_payload = cur.fetchone()[0]

        # No `view` compatibility node should be projected into the graph.
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        projected_node_count = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND subtype = 'view'
            """,
            {"node_id": f"view:{view.view_id}"},
        )[0]["n"]

    assert refreshed.nodes[0].status == "completed"
    assert refreshed.nodes[0].dirty_scope_count == 1
    assert run == ("dirty", 1, 2)
    assert markdown_payload == {
        "kind": "text",
        "text": "# Dirty View\n\nVersion 2.",
    }
    assert projected_node_count == 0


def test_scheduler_later_refresh_consumes_older_dirty_native_view_scope(
    clean_runtime: str,
) -> None:
    view = _view_definition("older-dirty")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown(
                            "# Dirty View\n\nVersion 1."
                        ),
                        InlineViewOutput.json({"version": 1}),
                    ],
                    dirty_scope_keys=[],
                )
            ],
        )
        enqueue_view_dirty_scope(
            conn,
            view,
            target_generation_id=gen1,
            dirty_scope=DirtyScope(
                scope_key="view",
                dirty_reason="external_progress_changed",
                dependency_kind="source_progress",
                dependency_ref="source_progress:test",
                dependency_hash="h2",
            ),
        )

        refreshed = schedule_derived_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                ViewScheduleTask(
                    view,
                    outputs=[
                        InlineViewOutput.markdown(
                            "# Dirty View\n\nVersion 2."
                        ),
                        InlineViewOutput.json({"version": 2}),
                    ],
                    dirty_scope_keys=["view"],
                )
            ],
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT consumed_run_id
                  FROM okg.derived_dirty_scopes
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(target_generation_id)s
                   AND scope_key = 'view'
                """,
                {
                    "definition_id": view.definition_id,
                    "target_generation_id": gen1,
                },
            )
            consumed_run_id = cur.fetchone()[0]

    assert consumed_run_id == refreshed.nodes[0].run_id
