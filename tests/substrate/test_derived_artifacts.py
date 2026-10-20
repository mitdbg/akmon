"""Derived artifact proof-slice tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from okg.substrate.derived_artifacts import (
    DirtyScope,
    RelationArtifactDefinition,
    RelationRow,
    carry_forward_relation_artifact,
    enqueue_dirty_scope,
    pending_dirty_scope_keys,
    publish_relation_artifact,
    refresh_relation_artifact,
    register_jsonb_relation,
    relation_visible_rows_for_manifest,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _definition() -> RelationArtifactDefinition:
    suffix = uuid4().hex
    output_contract = {
        "columns": {
            "claim_id": "text",
            "severity": "text",
            "score": "float",
        }
    }
    dirty_rule = {
        "scope_key": "document_id",
        "dependencies": ["nodes_live.attrs.text_hash"],
    }
    return RelationArtifactDefinition(
        definition_id=f"test.derived.{suffix}",
        deployment_name="test",
        logical_relation_id=f"claim_summary_{suffix}",
        compute_hash=stable_hash({"sql": "select claims"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 10_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:derived:{generation_id}",
            },
        )
    conn.commit()


def test_migrate_creates_derived_artifact_tables(
    okg_migrated: str,
) -> None:
    with psycopg.connect(okg_migrated) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regnamespace('okg_derived')")
        assert cur.fetchone()[0] == "okg_derived"

        cur.execute(
            """
            SELECT tablename FROM pg_tables
             WHERE schemaname = 'okg'
               AND tablename LIKE 'derived_%'
            """
        )
        tables = {row[0] for row in cur.fetchall()}

    assert {
        "derived_artifact_definitions",
        "derived_artifact_runs",
        "derived_output_manifests",
        "derived_output_slots",
        "derived_relation_registry",
        "derived_dirty_scopes",
        "derived_dirty_rule_runs",
        "derived_dirty_rule_audits",
        "derived_artifact_dependencies",
        "derived_schedule_plans",
        "derived_schedule_nodes",
        "derived_run_dependency_bindings",
    } <= tables


def test_publish_relation_artifact_promotes_complete_manifest(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()
    rows = [
        RelationRow(
            scope_key="doc:a",
            row_key="claim:1",
            row_data={"claim_id": "claim:1", "severity": "high"},
            dependency_hash="dep:a",
        ),
        RelationRow(
            scope_key="doc:b",
            row_key="claim:2",
            row_data={"claim_id": "claim:2", "severity": "low"},
            dependency_hash="dep:b",
        ),
    ]

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)

        published = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=generation_id,
            rows=rows,
            trigger_kind="manual",
            metrics={"scheduler_ms": 1, "materialization_ms": 2},
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, output_manifest_id, compute_hash,
                       output_schema_hash, renderer_hash, dirty_rule_hash
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": published.run_id},
            )
            run = cur.fetchone()
            assert run == (
                "completed",
                published.manifest_id,
                definition.compute_hash,
                definition.output_schema_hash,
                definition.renderer_hash,
                definition.dirty_rule_hash,
            )

            cur.execute(
                """
                SELECT status, required_slots, manifest_hash, promoted_at
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": published.manifest_id},
            )
            manifest = cur.fetchone()
            assert manifest[0] == "completed"
            assert manifest[1] == ["relation"]
            assert manifest[2] == published.manifest_hash
            assert manifest[3] is not None

            cur.execute(
                """
                SELECT storage_kind, payload_hash, storage_ref, row_count
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'relation'
                """,
                {"manifest_id": published.manifest_id},
            )
            slot = cur.fetchone()
            assert slot[0] == "relation"
            assert len(slot[1]) == 64
            assert slot[2]["relation"] == published.relation_name
            assert slot[2]["target_generation_id"] == generation_id
            assert slot[3] == 2

            cur.execute(
                sql.SQL(
                    """
                    SELECT row_key, row_data
                      FROM {}.{}
                     WHERE definition_id = %(definition_id)s
                       AND target_generation_id = %(generation_id)s
                     ORDER BY row_key
                    """
                ).format(
                    sql.Identifier(published.schema_name),
                    sql.Identifier(published.relation_name),
                ),
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            relation_rows = cur.fetchall()

    assert relation_rows == [
        ("claim:1", {"claim_id": "claim:1", "severity": "high"}),
        ("claim:2", {"claim_id": "claim:2", "severity": "low"}),
    ]


def test_carry_forward_relation_artifact_is_generation_visible(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        source = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=source_generation_id,
            rows=[
                RelationRow(
                    scope_key="doc:a",
                    row_key="claim:1",
                    row_data={"claim_id": "claim:1", "score": 0.7},
                )
            ],
        )

        carried = carry_forward_relation_artifact(
            conn,
            definition,
            source_manifest_id=source.manifest_id,
            target_generation_id=target_generation_id,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, output_manifest_id, reused_from_run_id
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": carried.run_id},
            )
            run = cur.fetchone()
            assert run == (
                "carried_forward",
                carried.manifest_id,
                source.run_id,
            )

            cur.execute(
                """
                SELECT status, reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                   AND target_generation_id = %(target_generation_id)s
                """,
                {
                    "manifest_id": carried.manifest_id,
                    "target_generation_id": target_generation_id,
                },
            )
            assert cur.fetchone() == ("completed", source.manifest_id)

            cur.execute(
                """
                SELECT storage_ref, row_count
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'relation'
                """,
                {"manifest_id": carried.manifest_id},
            )
            storage_ref, row_count = cur.fetchone()

    assert carried.carried_forward is True
    assert carried.reused_from_manifest_id == source.manifest_id
    assert carried.relation_name == source.relation_name
    assert storage_ref["relation"] == source.relation_name
    assert storage_ref["target_generation_id"] == source_generation_id
    assert storage_ref["reused_from_manifest_id"] == str(source.manifest_id)
    assert storage_ref["reused_for_generation_id"] == target_generation_id
    assert row_count == 1


def test_refresh_relation_artifact_reuses_clean_scopes(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        source = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=source_generation_id,
            rows=[
                RelationRow(
                    scope_key="doc:a",
                    row_key="claim:1",
                    row_data={"claim_id": "claim:1", "severity": "low"},
                    dependency_hash="dep:a:v1",
                ),
                RelationRow(
                    scope_key="doc:b",
                    row_key="claim:2",
                    row_data={"claim_id": "claim:2", "severity": "medium"},
                    dependency_hash="dep:b:v1",
                ),
                RelationRow(
                    scope_key="doc:c",
                    row_key="claim:3",
                    row_data={"claim_id": "claim:3", "severity": "high"},
                    dependency_hash="dep:c:v1",
                ),
            ],
        )
        enqueue_dirty_scope(
            conn,
            definition,
            target_generation_id=target_generation_id,
            dirty_scope=DirtyScope(
                scope_key="doc:b",
                dirty_reason="source_text_changed",
                dependency_kind="node_text_hash",
                dependency_ref="doc:b",
                dependency_hash="dep:b:v2",
            ),
        )
        assert pending_dirty_scope_keys(
            conn,
            definition,
            target_generation_id=target_generation_id,
        ) == ["doc:b"]

        refreshed = refresh_relation_artifact(
            conn,
            definition,
            source_manifest_id=source.manifest_id,
            target_generation_id=target_generation_id,
            dirty_scope_keys=["doc:b"],
            dirty_rows=[
                RelationRow(
                    scope_key="doc:b",
                    row_key="claim:2",
                    row_data={"claim_id": "claim:2", "severity": "critical"},
                    dependency_hash="dep:b:v2",
                )
            ],
            metrics={"dirty_fraction": 1 / 3},
        )

        assert pending_dirty_scope_keys(
            conn,
            definition,
            target_generation_id=target_generation_id,
        ) == []

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, trigger_kind, output_manifest_id,
                       dirty_scopes, dirty_rows, reused_scopes
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": refreshed.run_id},
            )
            assert cur.fetchone() == (
                "completed",
                "dirty",
                refreshed.manifest_id,
                1,
                1,
                2,
            )

            cur.execute(
                """
                SELECT status, reused_from_manifest_id
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": refreshed.manifest_id},
            )
            assert cur.fetchone() == ("completed", source.manifest_id)

            cur.execute(
                """
                SELECT storage_ref, row_count
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                   AND slot_name = 'relation'
                """,
                {"manifest_id": refreshed.manifest_id},
            )
            storage_ref, row_count = cur.fetchone()
            assert storage_ref["source_generation_id"] == source_generation_id
            assert storage_ref["target_generation_id"] == target_generation_id
            assert storage_ref["dirty_scope_keys"] == ["doc:b"]
            assert storage_ref["dirty_scope_count"] == 1
            assert storage_ref["copied_scope_count"] == 2
            assert storage_ref["copied_row_count"] == 2
            assert storage_ref["total_scope_count"] == 3
            assert row_count == 3

            visible = relation_visible_rows_for_manifest(
                conn,
                definition,
                manifest_id=refreshed.manifest_id,
            )
            target_rows = [
                (row.scope_key, dict(row.row_data), row.dependency_hash)
                for row in sorted(visible, key=lambda r: r.scope_key)
            ]

            cur.execute(
                """
                SELECT consumed_run_id
                  FROM okg.derived_dirty_scopes
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(target_generation_id)s
                   AND scope_key = 'doc:b'
                """,
                {
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                },
            )
            assert cur.fetchone()[0] == refreshed.run_id

    assert refreshed.row_count == 3
    assert refreshed.reused_from_manifest_id == source.manifest_id
    assert refreshed.dirty_scope_count == 1
    assert refreshed.reused_scope_count == 2
    assert target_rows == [
        ("doc:a", {"claim_id": "claim:1", "severity": "low"}, "dep:a:v1"),
        ("doc:b", {"claim_id": "claim:2", "severity": "critical"}, "dep:b:v2"),
        ("doc:c", {"claim_id": "claim:3", "severity": "high"}, "dep:c:v1"),
    ]


def test_refresh_relation_artifact_removes_disappeared_dirty_rows(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        source = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=source_generation_id,
            rows=[
                RelationRow(
                    scope_key="doc:a",
                    row_key="claim:1",
                    row_data={"claim_id": "claim:1"},
                ),
                RelationRow(
                    scope_key="doc:b",
                    row_key="claim:2",
                    row_data={"claim_id": "claim:2"},
                ),
                RelationRow(
                    scope_key="doc:c",
                    row_key="claim:3",
                    row_data={"claim_id": "claim:3"},
                ),
            ],
        )
        enqueue_dirty_scope(
            conn,
            definition,
            target_generation_id=target_generation_id,
            dirty_scope=DirtyScope(
                scope_key="doc:b",
                dirty_reason="claim_removed",
                dependency_kind="node_text_hash",
                dependency_ref="doc:b",
            ),
        )

        refreshed = refresh_relation_artifact(
            conn,
            definition,
            source_manifest_id=source.manifest_id,
            target_generation_id=target_generation_id,
            dirty_scope_keys=["doc:b"],
            dirty_rows=[],
        )

        with conn.cursor() as cur:
            visible = relation_visible_rows_for_manifest(
                conn,
                definition,
                manifest_id=refreshed.manifest_id,
            )
            target_rows = [
                (row.scope_key, row.row_key)
                for row in sorted(visible, key=lambda r: r.scope_key)
            ]

            cur.execute(
                """
                SELECT total_scopes, dirty_scopes, dirty_rows, reused_scopes
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": refreshed.run_id},
            )
            run_counts = cur.fetchone()

    assert target_rows == [("doc:a", "claim:1"), ("doc:c", "claim:3")]
    assert run_counts == (3, 1, 0, 2)
    assert refreshed.row_count == 2
    assert refreshed.dirty_scope_count == 1
    assert refreshed.reused_scope_count == 2


def test_failed_relation_publish_rolls_back_manifest_and_rows(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        relation_name = register_jsonb_relation(conn, definition)
        duplicate_rows = [
            RelationRow(
                scope_key="doc:a",
                row_key="claim:1",
                row_data={"claim_id": "claim:1"},
            ),
            RelationRow(
                scope_key="doc:a",
                row_key="claim:1",
                row_data={"claim_id": "claim:1", "duplicate": True},
            ),
        ]

        with pytest.raises(psycopg.errors.UniqueViolation):
            publish_relation_artifact(
                conn,
                definition,
                target_generation_id=generation_id,
                rows=duplicate_rows,
            )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.derived_artifact_runs
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                """,
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            assert cur.fetchone()[0] == 0

            cur.execute(
                """
                SELECT count(*)
                  FROM okg.derived_output_manifests
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                """,
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            assert cur.fetchone()[0] == 0

            cur.execute(
                sql.SQL(
                    """
                    SELECT count(*)
                      FROM {}.{}
                     WHERE definition_id = %(definition_id)s
                       AND target_generation_id = %(generation_id)s
                    """
                ).format(
                    sql.Identifier("okg_derived"),
                    sql.Identifier(relation_name),
                ),
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            assert cur.fetchone()[0] == 0
