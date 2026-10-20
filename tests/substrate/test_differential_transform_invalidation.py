"""Differential transform invalidation proof tests."""
from __future__ import annotations

from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from okg.substrate.derived_artifacts import (
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    register_jsonb_relation,
    schedule_relation_artifacts,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def test_definition_hash_change_recomputes_only_dependent_relation(
    clean_runtime: str,
) -> None:
    changed_definition = _definition("changed")
    unchanged_definition = _definition("unchanged")
    changed_v2 = replace(
        changed_definition,
        compute_hash=stable_hash(
            {
                "definition_id": changed_definition.definition_id,
                "compute": "v2",
            }
        ),
    )
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        register_jsonb_relation(conn, changed_definition)
        register_jsonb_relation(conn, unchanged_definition)

        first = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen1,
            tasks=[
                RelationScheduleTask(
                    changed_definition,
                    rows=[
                        RelationRow(
                            scope_key="scope:a",
                            row_key="changed:a",
                            row_data={"label": "changed", "value": "v1"},
                        ),
                        RelationRow(
                            scope_key="scope:b",
                            row_key="changed:b",
                            row_data={"label": "changed", "value": "v1"},
                        ),
                    ],
                    dirty_scope_keys=[],
                ),
                RelationScheduleTask(
                    unchanged_definition,
                    rows=[
                        RelationRow(
                            scope_key="scope:a",
                            row_key="unchanged:a",
                            row_data={
                                "label": "unchanged",
                                "value": "stable",
                            },
                        )
                    ],
                    dirty_scope_keys=[],
                ),
            ],
        )
        second = schedule_relation_artifacts(
            conn,
            deployment_name="test",
            target_generation_id=gen2,
            tasks=[
                RelationScheduleTask(
                    changed_v2,
                    rows=[
                        RelationRow(
                            scope_key="scope:a",
                            row_key="changed:a",
                            row_data={"label": "changed", "value": "v2"},
                        ),
                        RelationRow(
                            scope_key="scope:b",
                            row_key="changed:b",
                            row_data={"label": "changed", "value": "v2"},
                        ),
                    ],
                    dirty_scope_keys=[],
                ),
                RelationScheduleTask(
                    unchanged_definition,
                    rows=[
                        RelationRow(
                            scope_key="scope:a",
                            row_key="unchanged:a",
                            row_data={
                                "label": "unchanged",
                                "value": "stable",
                            },
                        )
                    ],
                    dirty_scope_keys=[],
                ),
            ],
        )

        first_by_definition = {
            node.definition_id: node for node in first.nodes
        }
        second_by_definition = {
            node.definition_id: node for node in second.nodes
        }
        changed_node = second_by_definition[changed_definition.definition_id]
        unchanged_node = second_by_definition[
            unchanged_definition.definition_id
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trigger_kind, dirty_scopes, reused_scopes
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": changed_node.run_id},
            )
            changed_run = cur.fetchone()

            cur.execute(
                """
                SELECT trigger_kind, reused_from_run_id, reused_scopes
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": unchanged_node.run_id},
            )
            unchanged_run = cur.fetchone()

            cur.execute(
                """
                SELECT reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": changed_node.manifest_id},
            )
            changed_reused_manifest = cur.fetchone()[0]

            cur.execute(
                """
                SELECT reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": unchanged_node.manifest_id},
            )
            unchanged_reused_manifest = cur.fetchone()[0]

            changed_rows = _relation_rows(
                cur,
                definition=changed_v2,
                manifest_id=changed_node.manifest_id,
                target_generation_id=gen2,
            )
            unchanged_storage = _storage_ref(
                cur,
                manifest_id=unchanged_node.manifest_id,
            )

    assert first_by_definition[changed_definition.definition_id].status == (
        "completed"
    )
    assert first_by_definition[unchanged_definition.definition_id].status == (
        "completed"
    )
    assert changed_node.status == "completed"
    assert changed_run == ("full", 2, 0)
    assert changed_reused_manifest is None
    assert changed_rows == [
        ("scope:a", {"label": "changed", "value": "v2"}),
        ("scope:b", {"label": "changed", "value": "v2"}),
    ]

    assert unchanged_node.status == "carried_forward"
    assert unchanged_run == (
        "carry_forward",
        first_by_definition[unchanged_definition.definition_id].run_id,
        1,
    )
    assert unchanged_reused_manifest == first_by_definition[
        unchanged_definition.definition_id
    ].manifest_id
    assert unchanged_storage["reused_from_manifest_id"] == str(
        first_by_definition[unchanged_definition.definition_id].manifest_id
    )
    assert unchanged_storage["reused_for_generation_id"] == gen2


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
        "dependencies": ["differential-transform-test"],
    }
    return RelationArtifactDefinition(
        definition_id=f"test.differential.transform.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"differential_transform_{label}_{suffix}",
        compute_hash=stable_hash({"label": label, "compute": "v1"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 400_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": (
                    f"test:differential-transform:{generation_id}"
                ),
            },
        )
    conn.commit()


def _storage_ref(
    cur: psycopg.Cursor,
    *,
    manifest_id,
):
    cur.execute(
        """
        SELECT storage_ref
          FROM okg.derived_output_slots
         WHERE manifest_id = %(manifest_id)s
           AND slot_name = 'relation'
        """,
        {"manifest_id": manifest_id},
    )
    return cur.fetchone()[0]


def _relation_rows(
    cur: psycopg.Cursor,
    *,
    definition: RelationArtifactDefinition,
    manifest_id,
    target_generation_id: int,
) -> list[tuple[str, dict]]:
    storage_ref = _storage_ref(cur, manifest_id=manifest_id)
    cur.execute(
        sql.SQL(
            """
            SELECT scope_key, row_data
              FROM {}.{}
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
             ORDER BY scope_key
            """
        ).format(
            sql.Identifier(storage_ref["schema"]),
            sql.Identifier(storage_ref["relation"]),
        ),
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
        },
    )
    return cur.fetchall()
