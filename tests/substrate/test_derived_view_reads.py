"""Canonical read-path tests for native V1 view artifacts.

Behavioral home for the `okg view show` read surface.
"""
from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    ViewArtifactAmbiguous,
    ViewArtifactDefinition,
    ViewArtifactNotFound,
    carry_forward_view_artifact,
    get_view_artifact_at_generation,
    inspect_view_slot,
    list_view_artifacts,
    list_view_slots,
    publish_relation_artifact,
    publish_view_artifact,
    read_view_slot,
    register_view_artifact_definition,
    stable_hash,
)


pytestmark = pytest.mark.postgres


def _definition(
    *,
    deployment_name: str = "test",
    view_id: str | None = None,
    definition_id: str | None = None,
) -> ViewArtifactDefinition:
    suffix = uuid4().hex
    resolved_view_id = view_id or f"test.view.read.{suffix}"
    resolved_definition_id = definition_id or f"view:{resolved_view_id}"
    output_contract = {"slots": ["markdown", "json"]}
    dirty_rule = {"scope_key": "view", "dependencies": ["nodes_live"]}
    return ViewArtifactDefinition(
        definition_id=resolved_definition_id,
        deployment_name=deployment_name,
        view_id=resolved_view_id,
        view_name="Read Path View",
        compute_hash=stable_hash(
            {
                "definition_id": resolved_definition_id,
                "view_id": resolved_view_id,
                "template": "read-path-test",
            }
        ),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "markdown+json"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _relation_definition(
    *,
    deployment_name: str = "test",
    logical_relation_id: str,
) -> RelationArtifactDefinition:
    output_contract = {
        "logical_relation_id": logical_relation_id,
        "columns": ["field", "value"],
    }
    dirty_rule = {"scope_key": "summary", "dependencies": ["nodes_live"]}
    return RelationArtifactDefinition(
        definition_id=f"relation:{logical_relation_id}",
        deployment_name=deployment_name,
        logical_relation_id=logical_relation_id,
        compute_hash=stable_hash({
            "logical_relation_id": logical_relation_id,
            "compute": "test",
        }),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _generation_id() -> int:
    return 30_000 + (uuid4().int % 1_000_000)


def _insert_generation(
    conn: psycopg.Connection,
    generation_id: int,
    *,
    deployment_name: str = "test",
    status: str = "published",
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.graph_generations (
              generation_id, deployment_name, status, catalog_version_id,
              published_at, correlation_id
            ) VALUES (
              %(generation_id)s, %(deployment_name)s, %(status)s, 1,
              now(), %(correlation_id)s
            )
            """,
            {
                "generation_id": generation_id,
                "deployment_name": deployment_name,
                "status": status,
                "correlation_id": (
                    f"test:derived-view-read:{generation_id}"
                ),
            },
        )
    conn.commit()


def _publish(
    conn: psycopg.Connection,
    definition: ViewArtifactDefinition,
    generation_id: int,
    *,
    title: str,
    count: int,
):
    return publish_view_artifact(
        conn,
        definition,
        target_generation_id=generation_id,
        outputs=[
            InlineViewOutput.markdown(f"# {title}\n\nCount: {count}"),
            InlineViewOutput.json({"title": title, "count": count}),
        ],
    )


def test_read_latest_view_slot_uses_completed_manifest_slots(
    clean_runtime: str,
) -> None:
    definition = _definition()
    first_generation_id = _generation_id()
    second_generation_id = first_generation_id + 1

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, first_generation_id)
        _insert_generation(conn, second_generation_id)
        _publish(conn, definition, first_generation_id, title="First", count=1)
        second = _publish(
            conn,
            definition,
            second_generation_id,
            title="Second",
            count=2,
        )

        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="markdown",
        )
        slots = list_view_slots(conn, manifest_id=second.manifest_id)

    assert read.artifact.target_generation_id == second_generation_id
    assert read.artifact.manifest_id == second.manifest_id
    assert read.slot.payload_value() == "# Second\n\nCount: 2"
    assert [slot.slot_name for slot in slots] == ["json", "markdown"]
    assert slots[0].payload_value() == {"title": "Second", "count": 2}


def test_exact_generation_read_never_falls_back(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    empty_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, empty_generation_id)
        published = _publish(
            conn,
            definition,
            source_generation_id,
            title="Exact",
            count=1,
        )

        artifact = get_view_artifact_at_generation(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            generation_id=source_generation_id,
        )
        with pytest.raises(ViewArtifactNotFound):
            get_view_artifact_at_generation(
                conn,
                deployment_name="test",
                view_id=definition.view_id,
                generation_id=empty_generation_id,
            )

    assert artifact.manifest_id == published.manifest_id
    assert artifact.target_generation_id == source_generation_id


def test_carry_forward_view_read_is_generation_visible(
    clean_runtime: str,
) -> None:
    definition = _definition()
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        source = _publish(
            conn,
            definition,
            source_generation_id,
            title="Stable",
            count=7,
        )
        carried = carry_forward_view_artifact(
            conn,
            definition,
            source_manifest_id=source.manifest_id,
            target_generation_id=target_generation_id,
        )

        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="json",
            generation_id=target_generation_id,
        )

    assert read.artifact.target_generation_id == target_generation_id
    assert read.artifact.manifest_id == carried.manifest_id
    assert read.artifact.run_status == "carried_forward"
    assert read.artifact.reused_from_manifest_id == source.manifest_id
    assert read.slot.payload_value() == {"title": "Stable", "count": 7}


def test_view_read_does_not_require_compatibility_node(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _publish(conn, definition, generation_id, title="No Node", count=3)

        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="markdown",
        )
        # The read path must not project a `view` compatibility node into
        # the graph. Post-cutover the graph is the Chronos store, so check
        # via a checkout that no such node exists.
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        compatibility_nodes = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_nodes
             WHERE node_id = :node_id
               AND subtype = 'view'
            """,
            {"node_id": f"view:{definition.view_id}"},
        )[0]["n"]

    assert read.slot.payload_value().startswith("# No Node")
    assert compatibility_nodes == 0


def test_list_view_artifacts_reports_latest_and_exact_generation(
    clean_runtime: str,
) -> None:
    first_generation_id = _generation_id()
    second_generation_id = first_generation_id + 1
    stale_definition = _definition(
        view_id=f"test.view.list.{uuid4().hex}.a",
    )
    fresh_definition = _definition(
        view_id=f"test.view.list.{uuid4().hex}.b",
    )

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, first_generation_id)
        _insert_generation(conn, second_generation_id)
        _publish(
            conn,
            stale_definition,
            first_generation_id,
            title="Stale",
            count=1,
        )
        _publish(
            conn,
            fresh_definition,
            second_generation_id,
            title="Fresh",
            count=2,
        )

        latest = list_view_artifacts(conn, deployment_name="test")
        exact_first = list_view_artifacts(
            conn,
            deployment_name="test",
            generation_id=first_generation_id,
        )

    latest_by_view = {item.view_id: item for item in latest}
    assert latest_by_view[stale_definition.view_id].freshness == "stale"
    assert latest_by_view[fresh_definition.view_id].freshness == "fresh"
    assert latest_by_view[fresh_definition.view_id].slot_names == (
        "json", "markdown",
    )
    assert latest_by_view[fresh_definition.view_id].required_slots == (
        "json", "markdown",
    )
    assert latest_by_view[fresh_definition.view_id].to_json()["view_id"] == (
        fresh_definition.view_id
    )

    assert [item.view_id for item in exact_first] == [
        stale_definition.view_id,
    ]
    assert exact_first[0].freshness == "fresh"


def test_list_view_artifacts_supports_chronos_only_generation(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    definition = _definition()

    with psycopg.connect(clean_runtime) as conn:
        ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkpoint_generation(
            generation_id,
            deployment_name="test",
            status="published",
            catalog_version_id=1,
        )
        published = _publish(
            conn,
            definition,
            generation_id,
            title="Chronos",
            count=7,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.graph_generations
                 WHERE deployment_name = 'test'
                   AND generation_id = %s
                """,
                (generation_id,),
            )
            legacy_rows = cur.fetchone()[0]

        entries = list_view_artifacts(conn, deployment_name="test")
        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="markdown",
        )

    assert legacy_rows == 0
    assert [item.view_id for item in entries] == [definition.view_id]
    assert entries[0].target_generation_id == generation_id
    assert entries[0].manifest_id == published.manifest_id
    assert read.slot.payload_value() == "# Chronos\n\nCount: 7"


def test_ambiguous_view_definition_fails_without_definition_id(
    clean_runtime: str,
) -> None:
    view_id = f"test.view.ambiguous.{uuid4().hex}"
    first = _definition(view_id=view_id, definition_id=f"view:{view_id}:a")
    second = _definition(view_id=view_id, definition_id=f"view:{view_id}:b")
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _publish(conn, first, generation_id, title="First", count=1)
        register_view_artifact_definition(conn, second)

        with pytest.raises(ViewArtifactAmbiguous):
            read_view_slot(
                conn,
                deployment_name="test",
                view_id=view_id,
                slot_name="markdown",
            )

        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=view_id,
            slot_name="markdown",
            definition_id=first.definition_id,
        )

    assert read.artifact.definition_id == first.definition_id
    assert read.slot.payload_value() == "# First\n\nCount: 1"


def test_native_view_read_returns_markdown_json_and_list_entries(
    clean_runtime: str,
) -> None:
    definition = _definition()
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _publish(conn, definition, generation_id, title="CLI", count=5)

    with psycopg.connect(clean_runtime) as conn:
        markdown = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="markdown",
        )
        json_slot = read_view_slot(
            conn,
            deployment_name="test",
            view_id=definition.view_id,
            slot_name="json",
        )
        entries = list_view_artifacts(conn, deployment_name="test")

    assert markdown.slot.payload_value() == "# CLI\n\nCount: 5"
    assert json_slot.artifact.view_id == definition.view_id
    assert json_slot.artifact.target_generation_id == generation_id
    assert json_slot.slot.slot_name == "json"
    assert json_slot.slot.payload_value() == {"title": "CLI", "count": 5}
    assert len(entries) == 1
    assert entries[0].view_id == definition.view_id
    assert list(entries[0].slot_names) == ["json", "markdown"]


def test_view_inspect_reports_relation_slot_evidence(
    clean_runtime: str,
) -> None:
    generation_id = _generation_id()
    view_id = f"test.view.inspect.{uuid4().hex}"
    definition = ViewArtifactDefinition(
        definition_id=f"view:{view_id}",
        deployment_name="test",
        view_id=view_id,
        view_name="Inspectable Relation View",
        compute_hash=stable_hash({"view_id": view_id, "compute": "test"}),
        output_schema_hash=stable_hash({"slots": ["relation"]}),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash({"scope_key": "summary"}),
        output_contract={"slots": ["relation"]},
        dirty_rule={"scope_key": "summary"},
    )
    relation_definition = _relation_definition(
        logical_relation_id=f"{view_id}.summary_relation",
    )

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        relation = publish_relation_artifact(
            conn,
            relation_definition,
            target_generation_id=generation_id,
            rows=[
                RelationRow(
                    scope_key="summary",
                    row_key="count",
                    row_data={"field": "count", "value": 1},
                )
            ],
        )
        publish_view_artifact(
            conn,
            definition,
            target_generation_id=generation_id,
            outputs=[
                InlineViewOutput.relation_ref(
                    slot_name="relation",
                    upstream_definition_id=(
                        relation_definition.definition_id
                    ),
                    logical_relation_id=(
                        relation_definition.logical_relation_id
                    ),
                    storage_ref={
                        "schema": relation.schema_name,
                        "relation": relation.relation_name,
                        "target_generation_id": generation_id,
                        "upstream_manifest_id": str(relation.manifest_id),
                        "upstream_target_generation_id": generation_id,
                        "bound_compute_hash": (
                            relation_definition.compute_hash
                        ),
                        "bound_output_schema_hash": (
                            relation_definition.output_schema_hash
                        ),
                        "bound_renderer_hash": (
                            relation_definition.renderer_hash
                        ),
                        "bound_dirty_rule_hash": (
                            relation_definition.dirty_rule_hash
                        ),
                        "columns": [
                            {
                                "name": "field",
                                "label": "Field",
                                "data_type": "text",
                                "required": True,
                            },
                            {
                                "name": "value",
                                "label": "Value",
                                "data_type": "integer",
                                "required": True,
                            },
                        ],
                        "column_names": ["field", "value"],
                        "scope": {"policy": "constant"},
                        "row_identity": {"policy": "column"},
                    },
                    row_count=relation.row_count,
                )
            ],
        )

        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=view_id,
            slot_name="relation",
            preview_limit=1,
        )

    assert inspection["relation"]["available"] is True
    assert inspection["relation"]["row_count"] == 1
    assert inspection["relation"]["scope_count"] == 1
    assert inspection["relation"]["physical_relation"] == (
        f"{relation.schema_name}.{relation.relation_name}"
    )
    assert inspection["relation"]["bound_hashes"]["compute_hash"] == (
        relation_definition.compute_hash
    )
    assert inspection["relation"]["registry"]["status"] == "active"
    assert inspection["relation"]["preview"]["rows"] == [
        {
            "scope_key": "summary",
            "row_key": "count",
            "row_data": {"field": "count", "value": 1},
            "dependency_hash": None,
        }
    ]
    assert inspection["relation"]["declared"]["column_names"] == [
        "field",
        "value",
    ]


def test_inspect_relation_slot_after_carry_forward_reports_true_count(
    clean_runtime: str,
) -> None:
    # Regression for a silent P0 in inspect_view_slot / the AKMON `inspect`
    # operator: on a CARRY-FORWARD cycle, carry_forward_relation_artifact reuses
    # the prior relation manifest verbatim, so the rows physically stay under the
    # SOURCE generation (storage_ref.target_generation_id = gen N) while the view
    # slot binds upstream_target_generation_id = the new gen N+1 (where NO
    # relation rows were written). _inspect_relation_slot must count rows at the
    # generation where they physically live (target_generation_id), not the
    # binding gen — otherwise it silently reports row_count 0 / empty preview for
    # EVERY carried-forward relation view (the common case). This storage_ref
    # shape is exactly what the publisher's carry-forward path produces
    # (empirically: target_generation_id = gen1, upstream_target_generation_id =
    # gen2). With the pre-fix upstream-first precedence the assertions below saw
    # row_count 0.
    source_generation_id = _generation_id()
    target_generation_id = source_generation_id + 1
    view_id = f"test.view.inspect.carryfwd.{uuid4().hex}"
    definition = ViewArtifactDefinition(
        definition_id=f"view:{view_id}",
        deployment_name="test",
        view_id=view_id,
        view_name="Carried Relation View",
        compute_hash=stable_hash({"view_id": view_id, "compute": "test"}),
        output_schema_hash=stable_hash({"slots": ["relation"]}),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash({"scope_key": "summary"}),
        output_contract={"slots": ["relation"]},
        dirty_rule={"scope_key": "summary"},
    )
    relation_definition = _relation_definition(
        logical_relation_id=f"{view_id}.summary_relation",
    )

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        # Rows physically written ONLY under the source generation.
        relation = publish_relation_artifact(
            conn,
            relation_definition,
            target_generation_id=source_generation_id,
            rows=[
                RelationRow(
                    scope_key="summary",
                    row_key="count",
                    row_data={"field": "count", "value": 1},
                ),
                RelationRow(
                    scope_key="summary",
                    row_key="other",
                    row_data={"field": "other", "value": 2},
                ),
            ],
        )
        # The view manifest is bound at the NEW (carry-forward) generation, but
        # the relation slot's storage_ref keeps target_generation_id at the
        # source gen where the rows live.
        publish_view_artifact(
            conn,
            definition,
            target_generation_id=target_generation_id,
            outputs=[
                InlineViewOutput.relation_ref(
                    slot_name="relation",
                    upstream_definition_id=(
                        relation_definition.definition_id
                    ),
                    logical_relation_id=(
                        relation_definition.logical_relation_id
                    ),
                    storage_ref={
                        "schema": relation.schema_name,
                        "relation": relation.relation_name,
                        # rows live under the source generation:
                        "target_generation_id": source_generation_id,
                        "upstream_manifest_id": str(relation.manifest_id),
                        # binding gen (carry-forward), row-less:
                        "upstream_target_generation_id": target_generation_id,
                        "bound_compute_hash": (
                            relation_definition.compute_hash
                        ),
                        "bound_output_schema_hash": (
                            relation_definition.output_schema_hash
                        ),
                        "bound_renderer_hash": (
                            relation_definition.renderer_hash
                        ),
                        "bound_dirty_rule_hash": (
                            relation_definition.dirty_rule_hash
                        ),
                        "columns": [
                            {
                                "name": "field",
                                "label": "Field",
                                "data_type": "text",
                                "required": True,
                            },
                            {
                                "name": "value",
                                "label": "Value",
                                "data_type": "integer",
                                "required": True,
                            },
                        ],
                        "column_names": ["field", "value"],
                        "scope": {"policy": "constant"},
                        "row_identity": {"policy": "column"},
                    },
                    row_count=relation.row_count,
                )
            ],
        )

        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=view_id,
            slot_name="relation",
            preview_limit=5,
        )

    rel = inspection["relation"]
    assert rel["available"] is True, rel
    # The P0: with the pre-fix upstream-first precedence this was 0.
    assert rel["row_count"] == 2, rel
    assert rel["scope_count"] == 1, rel
    # The relation query resolves at the generation where rows physically live.
    assert rel["relation_target_generation_id"] == source_generation_id, rel
    # The view's OWN bound generation is reported separately and is unchanged.
    assert rel["view_target_generation_id"] == target_generation_id, rel
    preview_values = sorted(
        row["row_data"]["value"] for row in rel["preview"]["rows"]
    )
    assert preview_values == [1, 2], rel
