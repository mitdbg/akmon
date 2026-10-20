"""Native V1 view artifact proof-slice tests."""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    ViewArtifactDefinition,
    carry_forward_view_artifact,
    get_view_slot,
    publish_view_artifact,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _definition() -> ViewArtifactDefinition:
    suffix = uuid4().hex
    output_contract = {
        "slots": ["markdown", "json"],
        "template": "deterministic-test-template",
    }
    dirty_rule = {
        "scope_key": "view",
        "dependencies": ["nodes_live", "observation_current"],
    }
    return ViewArtifactDefinition(
        definition_id=f"test.view.{suffix}",
        deployment_name="test",
        view_id=f"test.view.{suffix}",
        view_name="Test View",
        compute_hash=stable_hash(
            {
                "planner": "tests.native_view",
                "template": "deterministic-test-template",
            }
        ),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "markdown+json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 20_000 + (uuid4().int % 1_000_000)


def _insert_generation(conn: psycopg.Connection, generation_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, 'test', 'published', 1,
              now(), %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "correlation_id": f"test:derived-view:{generation_id}",
            },
        )
    conn.commit()


def test_publish_view_artifact_promotes_inline_manifest_slots(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)

        published = publish_view_artifact(
            conn,
            definition,
            target_generation_id=generation_id,
            outputs=[
                InlineViewOutput.markdown("# Test View\n\nHello."),
                InlineViewOutput.html("<h1>Test View</h1><p>Hello.</p>"),
                InlineViewOutput.json({"title": "Test View", "count": 1}),
            ],
            metrics={"render_ms": 3},
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT artifact_kind, logical_key, compute_hash,
                       output_schema_hash, renderer_hash, dirty_rule_hash
                  FROM okg.derived_artifact_definitions
                 WHERE definition_id = %(definition_id)s
                """,
                {"definition_id": definition.definition_id},
            )
            assert cur.fetchone() == (
                "view",
                definition.view_id,
                definition.compute_hash,
                definition.output_schema_hash,
                definition.renderer_hash,
                definition.dirty_rule_hash,
            )

            cur.execute(
                """
                SELECT status, output_manifest_id, dirty_rows
                  FROM okg.derived_artifact_runs
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": published.run_id},
            )
            assert cur.fetchone() == (
                "completed",
                published.manifest_id,
                3,
            )

            cur.execute(
                """
                SELECT status, required_slots, manifest_hash
                  FROM okg.derived_output_manifests
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": published.manifest_id},
            )
            assert cur.fetchone() == (
                "completed",
                ["html", "json", "markdown"],
                published.manifest_hash,
            )

            cur.execute(
                """
                SELECT slot_name, storage_kind, media_type, renderer_ref,
                       inline_payload, storage_ref, byte_count
                  FROM okg.derived_output_slots
                 WHERE manifest_id = %(manifest_id)s
                 ORDER BY slot_name
                """,
                {"manifest_id": published.manifest_id},
            )
            rows = cur.fetchall()

        # The view publish path must not project a `view` compatibility
        # node into the graph. Post-cutover the graph lives in Chronos, so
        # assert via a checkout that no `view`-subtype node exists.
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        view_node_count = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE subtype = 'view'"
        )[0]["n"]

    assert [row[0] for row in rows] == ["html", "json", "markdown"]
    assert {row[1] for row in rows} == {"inline"}
    assert rows[0][2] == "text/html"
    assert rows[1][2] == "application/json"
    assert rows[2][2] == "text/markdown"
    assert rows[1][4] == {
        "kind": "json",
        "value": {"title": "Test View", "count": 1},
    }
    assert rows[2][4] == {"kind": "text", "text": "# Test View\n\nHello."}
    assert rows[2][5]["target_generation_id"] == generation_id
    assert rows[2][6] == len("# Test View\n\nHello.".encode("utf-8"))
    assert view_node_count == 0


def test_carry_forward_view_artifact_is_generation_visible(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        source = publish_view_artifact(
            conn,
            definition,
            target_generation_id=source_generation_id,
            outputs=[
                InlineViewOutput.markdown("# Stable\n\nNo source changed."),
                InlineViewOutput.json({"stable": True}),
            ],
        )

        carried = carry_forward_view_artifact(
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
            manifest = cur.fetchone()

        markdown_slot = get_view_slot(
            conn,
            manifest_id=carried.manifest_id,
            slot_name="markdown",
        )
        inline_payload = markdown_slot.inline_payload
        storage_ref = markdown_slot.storage_ref

    assert run == ("carried_forward", carried.manifest_id, source.run_id)
    assert manifest == ("completed", source.manifest_id)
    assert carried.carried_forward is True
    assert carried.reused_from_manifest_id == source.manifest_id
    assert inline_payload == {
        "kind": "text",
        "text": "# Stable\n\nNo source changed.",
    }
    assert storage_ref["target_generation_id"] == target_generation_id
    assert storage_ref["source_generation_id"] == source_generation_id
    assert storage_ref["reused_from_manifest_id"] == str(source.manifest_id)


def test_failed_view_publish_rolls_back_manifest_and_run(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)

        with pytest.raises(TypeError):
            publish_view_artifact(
                conn,
                definition,
                target_generation_id=generation_id,
                outputs=[
                    InlineViewOutput(
                        slot_name="json",
                        media_type="application/json",
                        artifact_type="okg/view-json-v1",
                        renderer_ref="okg.renderer.json.v1",
                        payload={"not_json": object()},
                    )
                ],
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
            runs = cur.fetchone()[0]

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
            manifests = cur.fetchone()[0]

    assert runs == 0
    assert manifests == 0
