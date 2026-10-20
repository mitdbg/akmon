"""Dirty-rule executor proof-slice tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.derived_artifacts import (
    DirtyRuleLoadError,
    RelationArtifactDefinition,
    RelationRow,
    RelationScheduleTask,
    SqlDirtyRuleSpec,
    execute_sql_dirty_rule,
    pending_dirty_scope_keys,
    publish_relation_artifact,
    relation_visible_rows_for_manifest,
    schedule_relation_artifacts,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _definition(label: str = "dirty") -> RelationArtifactDefinition:
    suffix = uuid4().hex[:12]
    output_contract = {
        "columns": {
            "label": "text",
            "value": "text",
        }
    }
    dirty_rule = {
        "scope_universe_sql": "declared",
        "dirty_scope_sql": "declared",
        "missed_dirty_audit_sql": "optional",
    }
    return RelationArtifactDefinition(
        definition_id=f"test.dirty.{label}.{suffix}",
        deployment_name="test",
        logical_relation_id=f"dirty_{label}_{suffix}",
        compute_hash=stable_hash({"label": label, "compute": suffix}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 300_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:dirty:{generation_id}",
            },
        )
    conn.commit()


def _insert_document_fact(
    conn: psycopg.Connection,
    *,
    document_id: str,
    text_hash: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts (
              node_id, subtype, attrs, source, source_record_id,
              source_revision, observed_at, dedupe_key, run_id, op
            ) VALUES (
              %(node_id)s, 'document_chunk', %(attrs)s::jsonb,
              'docs', %(source_record_id)s::jsonb,
              %(source_revision)s::jsonb, now(), %(dedupe_key)s,
              %(run_id)s, 'I'
            )
            """,
            {
                "node_id": f"chunk:{document_id}",
                "attrs": Jsonb(
                    {
                        "document_id": document_id,
                        "text_hash": text_hash,
                    }
                ),
                "source_record_id": Jsonb({"document_id": document_id}),
                "source_revision": Jsonb({"text_hash": text_hash}),
                "dedupe_key": f"dirty:{document_id}:{uuid4()}",
                "run_id": uuid4(),
            },
        )
    conn.commit()


SCOPE_UNIVERSE_SQL = """
SELECT scope_key
  FROM (VALUES ('doc:a'), ('doc:b'), ('doc:c')) AS u(scope_key)
 ORDER BY scope_key
"""


DIRTY_FROM_NODE_FACTS_SQL = """
SELECT attrs->>'document_id' AS scope_key,
       'source_fact_changed' AS dirty_reason,
       'node_facts' AS dependency_kind,
       node_id AS dependency_ref,
       attrs->>'text_hash' AS dependency_hash
  FROM okg.node_facts
 WHERE source = 'docs'
   AND attrs->>'document_id' IS NOT NULL
 ORDER BY scope_key
"""


def test_sql_dirty_rule_derives_replayable_scopes_from_source_facts(
    clean_runtime: str,
) -> None:
    definition = _definition("facts")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _insert_document_fact(conn, document_id="doc:b", text_hash="h2")

        first = execute_sql_dirty_rule(
            conn,
            SqlDirtyRuleSpec(
                definition=definition,
                scope_universe_sql=SCOPE_UNIVERSE_SQL,
                dirty_scope_sql=DIRTY_FROM_NODE_FACTS_SQL,
            ),
            target_generation_id=generation_id,
        )
        second = execute_sql_dirty_rule(
            conn,
            SqlDirtyRuleSpec(
                definition=definition,
                scope_universe_sql=SCOPE_UNIVERSE_SQL,
                dirty_scope_sql=DIRTY_FROM_NODE_FACTS_SQL,
            ),
            target_generation_id=generation_id,
        )

        assert first.dirty_scope_keys == ["doc:b"]
        assert second.dirty_scope_keys == ["doc:b"]
        assert pending_dirty_scope_keys(
            conn,
            definition,
            target_generation_id=generation_id,
        ) == ["doc:b"]

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT dirty_rule_run_id
                  FROM okg.derived_dirty_scopes
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                """,
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            dirty_scope_rows = cur.fetchall()
            cur.execute(
                """
                SELECT status, total_scope_count, dirty_scope_count,
                       fallback_kind
                  FROM okg.derived_dirty_rule_runs
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {"dirty_rule_run_id": second.dirty_rule_run_id},
            )
            run = cur.fetchone()
            cur.execute(
                """
                SELECT status, missed_scope_count
                  FROM okg.derived_dirty_rule_audits
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {"dirty_rule_run_id": second.dirty_rule_run_id},
            )
            audit = cur.fetchone()

    assert dirty_scope_rows == [(second.dirty_rule_run_id,)]
    assert run == ("completed", 3, 1, "none")
    assert audit == ("skipped", 0)


def test_sql_dirty_rule_falls_back_to_full_universe_when_incomplete(
    clean_runtime: str,
) -> None:
    definition = _definition("fallback")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        result = execute_sql_dirty_rule(
            conn,
            SqlDirtyRuleSpec(
                definition=definition,
                scope_universe_sql=SCOPE_UNIVERSE_SQL,
                dirty_scope_sql="""
                SELECT 'doc:a' AS scope_key,
                       false AS dirty_complete,
                       'missing dependency coverage' AS incomplete_reason
                """,
            ),
            target_generation_id=generation_id,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT scope_key, dirty_reason, dependency_kind
                  FROM okg.derived_dirty_scopes
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                 ORDER BY scope_key
                """,
                {
                    "definition_id": definition.definition_id,
                    "generation_id": generation_id,
                },
            )
            dirty_rows = cur.fetchall()

    assert result.status == "completed"
    assert result.fallback_kind == "full_scope_universe"
    assert result.dirty_scope_keys == ["doc:a", "doc:b", "doc:c"]
    assert result.fallback_scope_count == 3
    assert dirty_rows == [
        ("doc:a", "dirty_rule_fallback", "dirty_rule_fallback"),
        ("doc:b", "dirty_rule_fallback", "dirty_rule_fallback"),
        ("doc:c", "dirty_rule_fallback", "dirty_rule_fallback"),
    ]


def test_missed_dirty_audit_enqueues_missed_scope_and_marks_run(
    clean_runtime: str,
) -> None:
    definition = _definition("audit")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        result = execute_sql_dirty_rule(
            conn,
            SqlDirtyRuleSpec(
                definition=definition,
                scope_universe_sql=SCOPE_UNIVERSE_SQL,
                dirty_scope_sql="SELECT 'doc:a' AS scope_key",
                missed_dirty_audit_sql="SELECT 'doc:c' AS scope_key",
            ),
            target_generation_id=generation_id,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, missed_scope_count, fallback_kind
                  FROM okg.derived_dirty_rule_runs
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {"dirty_rule_run_id": result.dirty_rule_run_id},
            )
            run = cur.fetchone()
            cur.execute(
                """
                SELECT status, missed_scope_count, missed_scope_keys
                  FROM okg.derived_dirty_rule_audits
                 WHERE dirty_rule_run_id = %(dirty_rule_run_id)s
                """,
                {"dirty_rule_run_id": result.dirty_rule_run_id},
            )
            audit = cur.fetchone()

    assert result.status == "audit_failed"
    assert result.dirty_scope_keys == ["doc:a", "doc:c"]
    assert run == ("audit_failed", 1, "none")
    assert audit == ("failed", 1, ["doc:c"])


def test_sql_dirty_rule_rejects_write_shaped_sql(clean_runtime: str) -> None:
    definition = _definition("reject")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        with pytest.raises(DirtyRuleLoadError, match="write-shaped SQL"):
            execute_sql_dirty_rule(
                conn,
                SqlDirtyRuleSpec(
                    definition=definition,
                    scope_universe_sql=SCOPE_UNIVERSE_SQL,
                    dirty_scope_sql="DELETE FROM okg.node_facts",
                ),
                target_generation_id=generation_id,
            )


def test_scheduler_consumes_dirty_rule_output(clean_runtime: str) -> None:
    definition = _definition("scheduler")
    gen1 = _generation_id()
    gen2 = gen1 + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, gen1)
        _insert_generation(conn, gen2)
        source = publish_relation_artifact(
            conn,
            definition,
            target_generation_id=gen1,
            rows=[
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
            ],
        )

        dirty = execute_sql_dirty_rule(
            conn,
            SqlDirtyRuleSpec(
                definition=definition,
                scope_universe_sql="""
                SELECT scope_key
                  FROM (VALUES ('scope:a'), ('scope:b')) AS u(scope_key)
                """,
                dirty_scope_sql="SELECT 'scope:b' AS scope_key",
            ),
            target_generation_id=gen2,
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
                            scope_key="scope:a",
                            row_key="row:a",
                            row_data={"label": "a", "value": "one"},
                        ),
                        RelationRow(
                            scope_key="scope:b",
                            row_key="row:b2",
                            row_data={"label": "b", "value": "two"},
                        ),
                    ],
                    dirty_scope_keys=None,
                )
            ],
        )

        assert source.manifest_id is not None
        assert dirty.dirty_scope_keys == ["scope:b"]
        assert result.nodes[0].dirty_scope_count == 1
        assert pending_dirty_scope_keys(
            conn,
            definition,
            target_generation_id=gen2,
        ) == []

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT consumed_run_id, dirty_rule_run_id
                  FROM okg.derived_dirty_scopes
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(generation_id)s
                   AND scope_key = 'scope:b'
                """,
                {
                    "definition_id": definition.definition_id,
                    "generation_id": gen2,
                },
            )
            dirty_scope_row = cur.fetchone()

        visible = relation_visible_rows_for_manifest(
            conn,
            definition,
            manifest_id=result.nodes[0].manifest_id,
        )
        rows = [
            (row.scope_key, row.row_key, dict(row.row_data))
            for row in sorted(visible, key=lambda r: (r.scope_key, r.row_key))
        ]

    assert dirty_scope_row == (
        result.nodes[0].run_id,
        dirty.dirty_rule_run_id,
    )
    assert rows == [
        ("scope:a", "row:a", {"label": "a", "value": "one"}),
        ("scope:b", "row:b2", {"label": "b", "value": "two"}),
    ]
