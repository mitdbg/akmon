"""Lens putback proof-slice tests."""
from __future__ import annotations

import json
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    ViewArtifactDefinition,
    carry_forward_view_artifact,
    inspect_view_slot,
    publish_relation_artifact,
    publish_view_artifact,
    read_view_slot,
    stable_hash,
)
from okg.substrate.lenses import (
    LensDefinition,
    LensPutbackDelta,
    LensPutbackError,
    LensPutbackStateError,
    commit_lens_overlay,
    prepare_lens_putback,
    register_lens_definition,
)


pytestmark = pytest.mark.postgres


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
                "correlation_id": f"test:lens:{generation_id}",
            },
        )
    conn.commit()


def _view_definition(suffix: str) -> ViewArtifactDefinition:
    output_contract = {"slots": ["editable_publications"]}
    dirty_rule = {"scope_key": "publication", "dependencies": ["nodes_live"]}
    return ViewArtifactDefinition(
        definition_id=f"view:test.lens.publications.{suffix}",
        deployment_name="test",
        view_id=f"test.lens.publications.{suffix}",
        view_name="Editable publications",
        compute_hash=stable_hash({"view": suffix, "compute": "v1"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _relation_definition(suffix: str) -> RelationArtifactDefinition:
    output_contract = {
        "logical_relation_id": f"test.lens.publications.{suffix}.rows",
        "columns": ["title", "status"],
    }
    dirty_rule = {"scope_key": "publication", "dependencies": ["nodes_live"]}
    return RelationArtifactDefinition(
        definition_id=f"relation:test.lens.publications.{suffix}.rows",
        deployment_name="test",
        logical_relation_id=f"test.lens.publications.{suffix}.rows",
        compute_hash=stable_hash({"relation": suffix, "compute": "v1"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "jsonb-relation"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _lens_definition(
    suffix: str,
    view: ViewArtifactDefinition,
) -> LensDefinition:
    lens_contract = {
        "editable_slots": {
            "editable_publications": {
                "row_identity": "row_key",
                "editable_fields": ["title"],
            }
        }
    }
    putback_policy = {
        "mode": "overlay",
        "validator": "expected_value_compare_v1",
    }
    return LensDefinition(
        definition_id=f"lens:test.lens.publications.{suffix}",
        deployment_name="test",
        lens_id=f"test.lens.publications.{suffix}",
        view_definition_id=view.definition_id,
        view_id=view.view_id,
        compute_hash=stable_hash({"lens": suffix, "compute": "v1"}),
        output_schema_hash=stable_hash(lens_contract),
        renderer_hash=stable_hash({"renderer": "lens-overlay"}),
        dirty_rule_hash=stable_hash(putback_policy),
        lens_contract=lens_contract,
        putback_policy=putback_policy,
        source_authority_policy={
            "allowed_authorities": ["test.publications"],
            "commit_modes": ["overlay"],
        },
    )


def _publish_editable_view(
    conn: psycopg.Connection,
    *,
    suffix: str,
    generation_id: int,
    title: str = "Old title",
) -> tuple[ViewArtifactDefinition, LensDefinition, object]:
    view = _view_definition(suffix)
    relation_definition = _relation_definition(suffix)
    relation = publish_relation_artifact(
        conn,
        relation_definition,
        target_generation_id=generation_id,
        rows=[
            RelationRow(
                scope_key="publication",
                row_key="publication:afi-1",
                row_data={
                    "title": title,
                    "status": "current",
                    "source_revision": "rev-1",
                },
            )
        ],
    )
    published_view = publish_view_artifact(
        conn,
        view,
        target_generation_id=generation_id,
        outputs=[
            InlineViewOutput.relation_ref(
                slot_name="editable_publications",
                upstream_definition_id=relation_definition.definition_id,
                logical_relation_id=relation_definition.logical_relation_id,
                storage_ref={
                    "schema": relation.schema_name,
                    "relation": relation.relation_name,
                    "target_generation_id": generation_id,
                    "upstream_manifest_id": str(relation.manifest_id),
                    "upstream_target_generation_id": generation_id,
                    "bound_compute_hash": relation_definition.compute_hash,
                    "bound_output_schema_hash": (
                        relation_definition.output_schema_hash
                    ),
                    "bound_renderer_hash": relation_definition.renderer_hash,
                    "bound_dirty_rule_hash": (
                        relation_definition.dirty_rule_hash
                    ),
                    "columns": [
                        {
                            "name": "title",
                            "label": "Title",
                            "data_type": "text",
                            "required": True,
                        },
                        {
                            "name": "status",
                            "label": "Status",
                            "data_type": "text",
                            "required": True,
                        },
                    ],
                    "column_names": ["title", "status"],
                    "scope": {"policy": "column", "column": "scope_key"},
                    "row_identity": {"policy": "column", "column": "row_key"},
                },
                row_count=relation.row_count,
            )
        ],
    )
    return view, _lens_definition(suffix, view), published_view


def test_lens_putback_prepares_and_commits_overlay(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)

        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published_view.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            idempotency_key="edit-title-1",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Old title",
                    new_value="New title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                    expected_source_revision={"revision": "rev-1"},
                )
            ],
        )
        idempotent = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published_view.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            idempotency_key="edit-title-1",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Old title",
                    new_value="New title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                )
            ],
        )
        committed = commit_lens_overlay(
            conn,
            request_id=prepared.request_id,
        )
        committed_again = commit_lens_overlay(
            conn,
            request_id=prepared.request_id,
        )
        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=lens.view_id,
            generation_id=generation_id,
            slot_name="editable_publications",
            preview_limit=10,
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT old_value, new_value, source_authority,
                       source_record_id, expected_source_revision,
                       source_delta, metadata
                  FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": prepared.request_id},
            )
            overlay = cur.fetchone()
        from okg.substrate.publisher.publisher import Publisher

        publisher_frontier = Publisher(
            clean_runtime,
            "test",
        )._view_change_frontier(
            conn,
            generation_id=generation_id,
            chronos_dirty_scope=None,
            overlay_completed_watermark_at=None,
        )

    assert prepared.status == "prepared"
    assert prepared.delta_count == 1
    assert prepared.conflict_count == 0
    assert idempotent.request_id == prepared.request_id
    assert committed.status == "committed_overlay"
    assert committed_again.request_id == committed.request_id
    assert committed_again.status == "committed_overlay"
    assert overlay[0] == "Old title"
    assert overlay[1] == "New title"
    assert overlay[2] == "test.publications"
    assert overlay[3] == {"node_id": "publication:afi-1"}
    assert overlay[4] == {"revision": "rev-1"}
    assert overlay[5]["operation"] == "replace_value"
    assert overlay[5]["field_path"] == ["title"]
    assert overlay[6]["projection_deltas"]["source"] == "lens_overlay"
    assert overlay[6]["projection_deltas"]["lineage_invalidations"] == [
        {
            "dependency_kind": "graph_node",
            "dependency_ref": "publication:afi-1",
            "dirty_reason": "lens_overlay_committed",
        }
    ]
    assert [
        (
            invalidation.dependency_kind,
            invalidation.dependency_ref,
            invalidation.dirty_reason,
        )
        for invalidation in publisher_frontier
    ] == [
        ("graph_node", "publication:afi-1", "lens_overlay_committed")
    ]
    preview_rows = inspection["relation"]["preview"]["rows"]
    assert preview_rows[0]["lens_overlays"][0]["new_value"] == "New title"
    assert preview_rows[0]["lens_overlays"][0]["field_path"] == ["title"]
    assert preview_rows[0]["lens_overlays"][0]["status"] == "active"


def test_lens_overlay_surfaces_on_carried_forward_view_generation(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()
    while target_generation_id <= source_generation_id:
        target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        view, lens, source_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=source_generation_id,
        )
        register_lens_definition(conn, lens)
        carried_view = carry_forward_view_artifact(
            conn,
            view,
            source_manifest_id=source_view.manifest_id,
            target_generation_id=target_generation_id,
        )

        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=carried_view.manifest_id,
            target_generation_id=target_generation_id,
            actor_id="operator:test",
            idempotency_key="edit-carried-title-1",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Old title",
                    new_value="Carried generation title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                    expected_source_revision={"revision": "rev-1"},
                )
            ],
        )
        committed = commit_lens_overlay(
            conn,
            request_id=prepared.request_id,
        )
        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=lens.view_id,
            generation_id=target_generation_id,
            slot_name="editable_publications",
            preview_limit=10,
        )
        read = read_view_slot(
            conn,
            deployment_name="test",
            view_id=lens.view_id,
            generation_id=target_generation_id,
            slot_name="editable_publications",
        )
        from okg.viz.endpoints import _relation_preview_for_slot

        viz_preview = _relation_preview_for_slot(
            conn,
            read.slot,
            view_definition_id=read.artifact.definition_id,
            view_target_generation_id=read.artifact.target_generation_id,
        )

    relation = inspection["relation"]
    preview_rows = relation["preview"]["rows"]
    overlays = preview_rows[0]["lens_overlays"]
    assert committed.status == "committed_overlay"
    assert relation["view_target_generation_id"] == target_generation_id
    assert relation["relation_target_generation_id"] == source_generation_id
    assert preview_rows[0]["row_data"]["title"] == "Old title"
    assert overlays[0]["old_value"] == "Old title"
    assert overlays[0]["new_value"] == "Carried generation title"
    assert overlays[0]["status"] == "active"
    assert viz_preview is not None
    assert viz_preview["view_target_generation_id"] == target_generation_id
    assert viz_preview["relation_target_generation_id"] == source_generation_id
    assert viz_preview["rows"][0]["lens_overlays"][0]["new_value"] == (
        "Carried generation title"
    )


def test_lens_overlay_carries_forward_while_exact_base_matches(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()
    while target_generation_id <= source_generation_id:
        target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        view, lens, source_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=source_generation_id,
        )
        register_lens_definition(conn, lens)

        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=source_view.manifest_id,
            target_generation_id=source_generation_id,
            actor_id="operator:test",
            idempotency_key="edit-source-title-1",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Old title",
                    new_value="Forward title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                    expected_source_revision={"revision": "rev-1"},
                )
            ],
        )
        commit_lens_overlay(conn, request_id=prepared.request_id)
        carry_forward_view_artifact(
            conn,
            view,
            source_manifest_id=source_view.manifest_id,
            target_generation_id=target_generation_id,
        )
        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=lens.view_id,
            generation_id=target_generation_id,
            slot_name="editable_publications",
            preview_limit=10,
        )

    overlays = inspection["relation"]["preview"]["rows"][0]["lens_overlays"]
    assert overlays[0]["target_generation_id"] == source_generation_id
    assert overlays[0]["old_value"] == "Old title"
    assert overlays[0]["new_value"] == "Forward title"


def test_lens_overlay_does_not_carry_when_exact_base_changes(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    source_generation_id = _generation_id()
    target_generation_id = _generation_id()
    while target_generation_id == source_generation_id:
        target_generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, source_generation_id)
        _insert_generation(conn, target_generation_id)
        _view, lens, source_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=source_generation_id,
        )
        register_lens_definition(conn, lens)

        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=source_view.manifest_id,
            target_generation_id=source_generation_id,
            actor_id="operator:test",
            idempotency_key="edit-source-title-2",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Old title",
                    new_value="Forward title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                    expected_source_revision={"revision": "rev-1"},
                )
            ],
        )
        commit_lens_overlay(conn, request_id=prepared.request_id)
        _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=target_generation_id,
            title="Changed source title",
        )
        inspection = inspect_view_slot(
            conn,
            deployment_name="test",
            view_id=lens.view_id,
            generation_id=target_generation_id,
            slot_name="editable_publications",
            preview_limit=10,
        )

    assert "lens_overlays" not in inspection["relation"]["preview"]["rows"][0]


def test_lens_putback_conflict_does_not_commit_overlay(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)

        conflict = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published_view.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_publications",
                    scope_key="publication",
                    row_key="publication:afi-1",
                    field_path=("title",),
                    expected_value="Stale title",
                    new_value="New title",
                    source_authority="test.publications",
                    source_record_id={"node_id": "publication:afi-1"},
                )
            ],
        )
        with pytest.raises(LensPutbackStateError):
            commit_lens_overlay(conn, request_id=conflict.request_id)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT validation_status, validation_error
                  FROM okg.lens_putback_deltas
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": conflict.request_id},
            )
            delta = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": conflict.request_id},
            )
            overlay_count = cur.fetchone()[0]

    assert conflict.status == "conflict"
    assert conflict.conflict_count == 1
    assert delta[0] == "conflict"
    assert delta[1]["type"] == "expected_value_mismatch"
    assert delta[1]["actual_value"] == "Old title"
    assert overlay_count == 0


def test_lens_putback_rejects_noneditable_field(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published_view = _publish_editable_view(
            conn,
            suffix=suffix,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)

        with pytest.raises(LensPutbackError):
            prepare_lens_putback(
                conn,
                lens,
                base_view_manifest_id=published_view.manifest_id,
                target_generation_id=generation_id,
                actor_id="operator:test",
                deltas=[
                    LensPutbackDelta(
                        slot_name="editable_publications",
                        scope_key="publication",
                        row_key="publication:afi-1",
                        field_path=("status",),
                        expected_value="current",
                        new_value="draft",
                        source_authority="test.publications",
                        source_record_id={"node_id": "publication:afi-1"},
                    )
                ],
            )


def _insert_chronos_checkpoint(
    conn: psycopg.Connection,
    *,
    token: int,
    status: str,
    deployment_name: str,
    kind: str = "okg_generation",
) -> None:
    """A Chronos-native generation: a checkpoint carrying the view token,
    and no okg.graph_generations row (the native publisher writes none)."""
    checkpoint_id = f"okg:{deployment_name}:branch:default:gen:test:{uuid4().hex}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public._chronos_branch_interval_checkpoints
              (checkpoint_id, branch_id, segment_id, created_at, metadata)
            VALUES (%(checkpoint_id)s, 'default', 0, now()::text, %(metadata)s)
            """,
            {
                "checkpoint_id": checkpoint_id,
                "metadata": json.dumps({
                    "kind": kind,
                    "status": status,
                    "deployment_name": deployment_name,
                    "target_generation_id": token,
                }),
            },
        )
    conn.commit()


@pytest.mark.parametrize(
    ("status", "deployment_name", "kind", "accepted"),
    [
        ("published", "test", "okg_generation", True),
        ("blocked", "test", "okg_generation", False),
        ("published", "another-deployment", "okg_generation", False),
        # Same token, same deployment, published -- but not a generation.
        ("published", "test", "merge_back_gate_probe", False),
    ],
)
def test_lens_putback_accepts_a_published_chronos_checkpoint_base(
    clean_runtime: str,
    status: str,
    deployment_name: str,
    kind: str,
    accepted: bool,
) -> None:
    suffix = uuid4().hex
    token = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_chronos_checkpoint(
            conn, token=token, status=status, deployment_name=deployment_name,
            kind=kind,
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.graph_generations WHERE generation_id = %s",
                (token,),
            )
            assert cur.fetchone()[0] == 0
        _view, lens, published_view = _publish_editable_view(
            conn, suffix=suffix, generation_id=token,
        )
        register_lens_definition(conn, lens)

        def prepare():
            return prepare_lens_putback(
                conn,
                lens,
                base_view_manifest_id=published_view.manifest_id,
                target_generation_id=token,
                actor_id="operator:test",
                deltas=[
                    LensPutbackDelta(
                        slot_name="editable_publications",
                        scope_key="publication",
                        row_key="publication:afi-1",
                        field_path=("title",),
                        expected_value="Old title",
                        new_value="New title",
                        source_authority="test.publications",
                        source_record_id={"node_id": "publication:afi-1"},
                    )
                ],
            )

        if accepted:
            prepared = prepare()
            assert prepared.status == "prepared"
            committed = commit_lens_overlay(conn, request_id=prepared.request_id)
            assert committed.status == "committed_overlay"
        else:
            with pytest.raises(LensPutbackError, match="not a completed published view"):
                prepare()


def test_lens_registration_requires_existing_view_definition(
    clean_runtime: str,
) -> None:
    view = _view_definition(uuid4().hex)
    lens = _lens_definition(uuid4().hex, view)

    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(LensPutbackError):
            register_lens_definition(conn, lens)
