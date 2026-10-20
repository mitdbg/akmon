"""Viz read-side tests for native V1 view artifacts."""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

pytest.importorskip("fastapi")
msgpack = pytest.importorskip("msgpack")

from fastapi.testclient import TestClient
from psycopg_pool import ConnectionPool

from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    ViewArtifactDefinition,
    publish_relation_artifact,
    publish_view_artifact,
    stable_hash,
)
from okg.substrate.lenses import LensDefinition, register_lens_definition
from okg.substrate.mcp.session import open_session
from okg.substrate.graph_store import ChronosGraphStore
from okg.viz.app import build_app
from okg.viz.config import load_default_config
from okg.viz.context import VizContext


pytestmark = pytest.mark.postgres


def _definition(
    *,
    view_id: str = "test.portal.native",
    view_name: str = "Native Portal View",
    slots: list[str] | None = None,
) -> ViewArtifactDefinition:
    output_contract = {"slots": slots or ["markdown", "json"]}
    return ViewArtifactDefinition(
        definition_id=f"view:{view_id}",
        deployment_name="default",
        view_id=view_id,
        view_name=view_name,
        compute_hash=stable_hash({"view_id": view_id, "compute": "test"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "markdown+json"}),
        dirty_rule_hash=stable_hash({"scope_key": "test"}),
        output_contract={**output_contract, "view_name": view_name},
        dirty_rule={"scope_key": "test"},
    )


def _relation_definition(view_id: str) -> RelationArtifactDefinition:
    output_contract = {
        "logical_relation_id": f"{view_id}.summary_relation",
        "columns": ["field", "value"],
    }
    return RelationArtifactDefinition(
        definition_id=f"view-relation:{view_id}:relation",
        deployment_name="default",
        logical_relation_id=f"{view_id}.summary_relation",
        compute_hash=stable_hash({"view_id": view_id, "sql": "test"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash({"scope_key": "test"}),
        output_contract=output_contract,
        dirty_rule={"scope_key": "test"},
    )


def _relation_lens(view_id: str) -> LensDefinition:
    definition = _definition(
        view_id=view_id,
        slots=["markdown", "json", "table", "relation"],
    )
    lens_contract = {
        "editable_slots": {
            "relation": {
                "row_identity": "row_key",
                "editable_fields": ["value"],
            }
        }
    }
    putback_policy = {
        "mode": "overlay",
        "validator": "expected_value_compare_v1",
    }
    return LensDefinition(
        definition_id=f"lens:{view_id}:relation-editor",
        deployment_name="default",
        lens_id=f"{view_id}.relation_editor",
        view_definition_id=definition.definition_id,
        view_id=definition.view_id,
        compute_hash=stable_hash({"lens": view_id, "compute": "test"}),
        output_schema_hash=stable_hash(lens_contract),
        renderer_hash=stable_hash({"renderer": "lens-overlay"}),
        dirty_rule_hash=stable_hash(putback_policy),
        lens_contract=lens_contract,
        putback_policy=putback_policy,
        source_authority_policy={
            "allowed_authorities": ["test.summary"],
            "commit_modes": ["overlay"],
        },
    )


def _seed_native_view(
    dsn: str,
    *,
    include_json: bool = True,
    view_id: str = "test.portal.native",
    relation_rows: list[RelationRow] | None = None,
    governed: bool = False,
) -> int:
    generation_id = 101
    definition = _definition(
        view_id=view_id,
        slots=(
            ["markdown", "json", "table", "relation"]
            if include_json else ["markdown"]
        ),
    )
    outputs = [
        InlineViewOutput.markdown(
            "# Native Portal View\n\nRead from a V1 slot."
        )
    ]
    if include_json:
        outputs.append(
            InlineViewOutput.json(
                {
                    "markered": (
                        "<!--frag:f1 start--># Native Portal View\n"
                        "\nRead from a V1 slot."
                        "<!--frag:f1 end-->\n"
                    ),
                    "fragment_offsets": {
                        "f1": {"start": 0, "end": 42}
                    },
                    "view_manifest": {
                        "view_id": definition.view_id,
                        "fragments": [
                            {
                                "id": "f1",
                                "sources": [],
                                "content": "Read from a V1 slot.",
                            }
                        ],
                    },
                }
            )
        )
        outputs.append(
            InlineViewOutput.table(
                {
                    "version": 1,
                    "view_id": definition.view_id,
                    "tables": [
                        {
                            "table_id": "summary",
                            "title": "Summary",
                            "columns": ["field", "value"],
                            "rows": [
                                {"field": "count", "value": 1},
                            ],
                        }
                    ],
                }
            )
        )
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(generation_id)s, 'default', 'published',
                  1, now(), 'test:native-view-viz:101'
                )
                """,
                {"generation_id": generation_id},
            )
        if include_json:
            relation_definition = _relation_definition(view_id)
            relation = publish_relation_artifact(
                conn,
                relation_definition,
                target_generation_id=generation_id,
                rows=relation_rows or [
                    RelationRow(
                        scope_key="summary",
                        row_key="count",
                        row_data={"field": "count", "value": 1},
                    )
                ],
            )
            outputs.append(
                InlineViewOutput.relation_ref(
                    slot_name="relation",
                    upstream_definition_id=relation_definition.definition_id,
                    logical_relation_id=(
                        relation_definition.logical_relation_id
                    ),
                    storage_ref={
                        "schema": relation.schema_name,
                        "relation": relation.relation_name,
                        "target_generation_id": generation_id,
                        "upstream_manifest_id": str(relation.manifest_id),
                        "title": "Summary relation",
                        "columns": ["field", "value"],
                    },
                    row_count=relation.row_count,
                )
            )
        published_view = publish_view_artifact(
            conn,
            definition,
            target_generation_id=generation_id,
            outputs=outputs,
        )
        if governed:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE okg.derived_artifact_runs
                       SET metrics = metrics || %s::jsonb
                     WHERE run_id = %s
                    """,
                    (
                        '{"read_asset_authority":{"asset_id":"test:governed"}}',
                        published_view.run_id,
                    ),
                )
        store = ChronosGraphStore.from_connection(conn)
        store.checkpoint_generation(
            generation_id,
            branch="default",
            deployment_name="default",
            catalog_version_id=1,
        )
        # Native V1 views publish a derived-artifact manifest, NOT a
        # compatibility graph node. Assert no `view:` node landed in the
        # Chronos graph at the published checkpoint.
        graph = store.checkout_generation(generation_id)
        view_node_rows = graph.query(
            "SELECT count(*) AS n FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": f"view:{definition.view_id}"},
        )
        assert view_node_rows[0]["n"] == 0
    return generation_id


def _register_relation_lens(
    dsn: str,
    *,
    view_id: str = "test.portal.native",
) -> LensDefinition:
    lens = _relation_lens(view_id)
    with psycopg.connect(dsn) as conn:
        register_lens_definition(conn, lens)
    return lens


def _client_for_native_view(
    dsn: str,
    *,
    generation_id: int,
    tmp_path: Path,
):
    pool = ConnectionPool(
        conninfo=dsn,
        min_size=1,
        max_size=2,
        kwargs={"autocommit": True},
        open=True,
        name="okg-viz-native-view-test",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn,
            deployment_name="default",
            requested_generation=generation_id,
        )
    ctx = VizContext(
        dsn=dsn,
        deployment_name="default",
        manifest={"name": "default"},
        config=load_default_config(),
        pool=pool,
        session=session,
        deployment_dir=tmp_path,
    )
    app = build_app(ctx_provider=lambda: ctx, is_ready=lambda: True)
    return TestClient(app), ctx


def test_portal_reads_native_view_without_compatibility_node(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    generation_id = _seed_native_view(clean_runtime)
    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=generation_id,
        tmp_path=tmp_path,
    )
    try:
        index = client.get("/viz/api/portal/views")
        assert index.status_code == 200, index.text
        items = index.json()["items"]
        assert [item["view_id"] for item in items] == [
            "test.portal.native"
        ]
        assert items[0]["node_id"] == "view:test.portal.native"
        assert items[0]["manifest_uri"].startswith(
            "derived-artifact://manifest/"
        )

        node = client.get("/viz/api/node/view:test.portal.native")
        assert node.status_code == 200, node.text
        node_payload = node.json()
        assert node_payload["node_type"] == "view"
        assert node_payload["node_label"] == "Native Portal View"
        assert node_payload["attrs"]["markdown_body"].startswith(
            "# Native Portal View"
        )
        assert node_payload["attrs"]["manifest_uri"].startswith(
            "derived-artifact://manifest/"
        )

        markered = client.get(
            "/viz/api/portal/views/test.portal.native/markered"
        )
        assert markered.status_code == 200, markered.text
        markered_payload = markered.json()
        assert markered_payload["meta"]["available"] is True
        assert "Native Portal View" in markered_payload["markered"]
        assert markered_payload["fragment_offsets"] == {
            "f1": {"start": 0, "end": 42}
        }

        table = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=table"
        )
        assert table.status_code == 200, table.text
        table_payload = table.json()
        assert table_payload["slot"]["slot_name"] == "table"
        assert table_payload["slot"]["payload"]["tables"][0]["rows"] == [
            {"field": "count", "value": 1}
        ]

        relation = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=relation"
        )
        assert relation.status_code == 200, relation.text
        relation_payload = relation.json()
        assert relation_payload["slot"]["storage_kind"] == "relation"
        assert relation_payload["relation_preview"]["rows"] == [
            {
                "scope_key": "summary",
                "row_key": "count",
                "row_data": {"field": "count", "value": 1},
            }
        ]

        cited = client.get(
            "/viz/api/portal/views/test.portal.native/cited"
        )
        assert cited.status_code == 200, cited.text
        assert cited.json()["citations"] == []

        expand = client.get("/viz/api/expand/view:test.portal.native")
        assert expand.status_code == 200, expand.text
        expand_payload = msgpack.unpackb(expand.content, raw=False)
        assert expand_payload["meta"]["source"] == (
            "view:test.portal.native"
        )

        khop = client.get("/viz/api/khop/view:test.portal.native")
        assert khop.status_code == 200, khop.text
        khop_payload = msgpack.unpackb(khop.content, raw=False)
        assert khop_payload["nodes"] == [
            {
                "id": "view:test.portal.native",
                "type": "view",
                "label": "Native Portal View",
            }
        ]

    finally:
        ctx.close()


def test_portal_refuses_governed_native_payload_without_principal(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    generation_id = _seed_native_view(clean_runtime, governed=True)
    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=generation_id,
        tmp_path=tmp_path,
    )
    try:
        response = client.get("/viz/api/node/view:test.portal.native")
        assert response.status_code == 403
        assert response.json()["detail"]["reason"] == (
            "governed_native_view_principal_required"
        )
    finally:
        ctx.close()


def test_portal_view_slot_filters_relation_preview(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    generation_id = _seed_native_view(
        clean_runtime,
        relation_rows=[
            RelationRow(
                scope_key="patient:a",
                row_key="condition",
                row_data={"field": "condition", "value": "diabetes"},
            ),
            RelationRow(
                scope_key="patient:a",
                row_key="medication",
                row_data={"field": "medication", "value": "metformin"},
            ),
            RelationRow(
                scope_key="patient:b",
                row_key="condition",
                row_data={"field": "condition", "value": "hypertension"},
            ),
        ],
    )
    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=generation_id,
        tmp_path=tmp_path,
    )
    try:
        scoped = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=relation"
            "&scope_key=patient%3Aa&limit=1"
        )
        assert scoped.status_code == 200, scoped.text
        scoped_preview = scoped.json()["relation_preview"]
        assert scoped_preview["row_count"] == 2
        assert scoped_preview["slot_row_count"] == 3
        assert scoped_preview["scope_key"] == "patient:a"
        assert scoped_preview["truncated"] is True
        assert scoped_preview["rows"] == [
            {
                "scope_key": "patient:a",
                "row_key": "condition",
                "row_data": {
                    "field": "condition",
                    "value": "diabetes",
                },
            }
        ]

        row = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=relation"
            "&scope_key=patient%3Aa&row_key=medication&limit=5"
        )
        assert row.status_code == 200, row.text
        row_preview = row.json()["relation_preview"]
        assert row_preview["row_count"] == 1
        assert row_preview["truncated"] is False
        assert row_preview["rows"] == [
            {
                "scope_key": "patient:a",
                "row_key": "medication",
                "row_data": {
                    "field": "medication",
                    "value": "metformin",
                },
            }
        ]
    finally:
        ctx.close()


def test_viz_lens_putback_endpoint_commits_overlay(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Lens overlay actor is server-derived (body.actor_id is ignored, per
    # secure-viz-operator-routes). Pin the operator identity so the recorded
    # actor is deterministic ("operator:test") regardless of the body value.
    monkeypatch.setenv("OKG_OPERATOR_EMAIL", "test")
    generation_id = _seed_native_view(clean_runtime)
    lens = _register_relation_lens(clean_runtime)
    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=generation_id,
        tmp_path=tmp_path,
    )
    try:
        lenses = client.get(
            "/viz/api/portal/lenses?view_id=test.portal.native"
        )
        assert lenses.status_code == 200, lenses.text
        lens_payload = lenses.json()
        assert lens_payload["lenses"][0]["lens_id"] == lens.lens_id
        assert (
            lens_payload["lenses"][0]["lens_contract"]["editable_slots"]
            ["relation"]["editable_fields"]
            == ["value"]
        )

        response = client.post(
            "/viz/api/portal/lens-putback",
            json={
                "view_id": "test.portal.native",
                "lens_id": lens.lens_id,
                "actor_id": "operator:test",
                "idempotency_key": "edit-summary-count",
                "deltas": [
                    {
                        "slot_name": "relation",
                        "scope_key": "summary",
                        "row_key": "count",
                        "field_path": ["value"],
                        "expected_value": 1,
                        "new_value": 2,
                        "source_authority": "test.summary",
                        "source_record_id": {"row_key": "count"},
                    }
                ],
            },
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["request"]["status"] == "committed_overlay"
        assert payload["meta"]["committed"] is True
        preview_rows = payload["relation_previews"]["relation"]["rows"]
        assert preview_rows[0]["row_data"] == {"field": "count", "value": 1}
        assert preview_rows[0]["lens_overlays"][0]["new_value"] == 2

        relation = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=relation"
        )
        assert relation.status_code == 200, relation.text
        rows = relation.json()["relation_preview"]["rows"]
        assert rows[0]["row_data"]["value"] == 1
        assert rows[0]["lens_overlays"][0]["new_value"] == 2
        overlay_id = rows[0]["lens_overlays"][0]["overlay_id"]

        overlays = client.get(
            "/viz/api/portal/lens-overlays"
            "?view_id=test.portal.native&status=active"
        )
        assert overlays.status_code == 200, overlays.text
        overlay_items = overlays.json()["overlays"]
        assert [item["overlay_id"] for item in overlay_items] == [
            overlay_id
        ]
        assert overlay_items[0]["actor_id"] == "operator:test"
        assert overlay_items[0]["row_key"] == "count"

        revoke = client.post(
            f"/viz/api/portal/lens-overlays/{overlay_id}/revoke",
            json={
                "actor_id": "operator:test",
                "reason": "test_cleanup",
            },
        )
        assert revoke.status_code == 200, revoke.text
        revoked = revoke.json()["overlay"]
        assert revoked["overlay_id"] == overlay_id
        assert revoked["status"] == "superseded"
        assert revoked["metadata"]["revoked_by"] == "operator:test"
        assert revoked["metadata"]["revoked_reason"] == "test_cleanup"

        relation_after_revoke = client.get(
            "/viz/api/portal/view-slot"
            "?view_id=test.portal.native&slot_name=relation"
        )
        assert relation_after_revoke.status_code == 200
        rows_after_revoke = (
            relation_after_revoke.json()["relation_preview"]["rows"]
        )
        assert "lens_overlays" not in rows_after_revoke[0]

        audit = client.get(
            "/viz/api/portal/lens-overlays"
            "?view_id=test.portal.native&status=all"
        )
        assert audit.status_code == 200, audit.text
        assert audit.json()["overlays"][0]["status"] == "superseded"

        conflict = client.post(
            "/viz/api/portal/lens-putback",
            json={
                "view_id": "test.portal.native",
                "lens_id": lens.lens_id,
                "actor_id": "operator:test",
                "idempotency_key": "edit-summary-count-conflict",
                "deltas": [
                    {
                        "slot_name": "relation",
                        "scope_key": "summary",
                        "row_key": "count",
                        "field_path": ["value"],
                        "expected_value": 99,
                        "new_value": 3,
                        "source_authority": "test.summary",
                        "source_record_id": {"row_key": "count"},
                    }
                ],
            },
        )
        assert conflict.status_code == 409, conflict.text
        assert conflict.json()["detail"]["reason"] == "lens_putback_conflict"
    finally:
        ctx.close()


def test_markered_endpoint_treats_json_slot_as_optional(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    generation_id = _seed_native_view(
        clean_runtime,
        include_json=False,
        view_id="test.portal.markdown_only",
    )
    client, ctx = _client_for_native_view(
        clean_runtime,
        generation_id=generation_id,
        tmp_path=tmp_path,
    )
    try:
        markered = client.get(
            "/viz/api/portal/views/test.portal.markdown_only/markered"
        )
        assert markered.status_code == 200, markered.text
        payload = markered.json()
        assert payload["meta"]["available"] is False
        assert payload["fragment_offsets"] == {}
        assert payload["markered"] is None
    finally:
        ctx.close()
