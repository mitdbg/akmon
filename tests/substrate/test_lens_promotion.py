"""Lens-promotion proof-slice tests (AKMON Phase 5)."""
from __future__ import annotations

from dataclasses import replace
import json
from uuid import uuid4

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.consistency import ConsistencyPublishGateEvaluation
from okg.substrate.derived_artifacts import (
    InlineViewOutput,
    RelationArtifactDefinition,
    RelationRow,
    ViewArtifactDefinition,
    publish_relation_artifact,
    publish_view_artifact,
    stable_hash,
)
from okg.substrate.lenses import (
    LensDefinition,
    LensPromotionMappingError,
    LensPromotionNotSupported,
    LensPromotionStateError,
    LensPutbackDelta,
    apply_promoted_overlay,
    commit_lens_overlay,
    lens_producer_id,
    lens_promotion_mapping_checksum,
    list_pending_lens_envelopes,
    prepare_lens_putback,
    promote_lens_overlay,
    register_lens_definition,
)
from okg.substrate.producer_authority import materialize_producer_profile
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _generation_id() -> int:
    return 600_000 + (uuid4().int % 1_000_000)


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
                "correlation_id": f"test:lens-promote:{generation_id}",
            },
        )
    conn.commit()


def _view_definition(
    suffix: str, deployment_name: str = "test"
) -> ViewArtifactDefinition:
    output_contract = {"slots": ["editable_people"]}
    dirty_rule = {"scope_key": "person", "dependencies": ["nodes_live"]}
    return ViewArtifactDefinition(
        definition_id=f"view:test.lens.promote.{suffix}",
        deployment_name=deployment_name,
        view_id=f"test.lens.promote.{suffix}",
        view_name="Editable people",
        compute_hash=stable_hash({"view": suffix, "compute": "v1"}),
        output_schema_hash=stable_hash(output_contract),
        renderer_hash=stable_hash({"renderer": "relation-table"}),
        dirty_rule_hash=stable_hash(dirty_rule),
        output_contract=output_contract,
        dirty_rule=dirty_rule,
    )


def _relation_definition(
    suffix: str, deployment_name: str = "test"
) -> RelationArtifactDefinition:
    output_contract = {
        "logical_relation_id": f"test.lens.promote.{suffix}.rows",
        "columns": ["display_name", "status"],
    }
    dirty_rule = {"scope_key": "person", "dependencies": ["nodes_live"]}
    return RelationArtifactDefinition(
        definition_id=f"relation:test.lens.promote.{suffix}.rows",
        deployment_name=deployment_name,
        logical_relation_id=f"test.lens.promote.{suffix}.rows",
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
    deployment_name: str = "test",
) -> LensDefinition:
    lens_contract = {
        "editable_slots": {
            "editable_people": {
                "row_identity": "row_key",
                "editable_fields": ["display_name"],
            }
        }
    }
    putback_policy = {
        "mode": "overlay",
        "validator": "expected_value_compare_v1",
    }
    return LensDefinition(
        definition_id=f"lens:test.lens.promote.{suffix}",
        deployment_name=deployment_name,
        lens_id=f"test.lens.promote.{suffix}",
        view_definition_id=view.definition_id,
        view_id=view.view_id,
        compute_hash=stable_hash({"lens": suffix, "compute": "v1"}),
        output_schema_hash=stable_hash(lens_contract),
        renderer_hash=stable_hash({"renderer": "lens-overlay"}),
        dirty_rule_hash=stable_hash(putback_policy),
        lens_contract=lens_contract,
        putback_policy=putback_policy,
        source_authority_policy={
            "allowed_authorities": ["test.people"],
            "commit_modes": ["overlay"],
        },
    )


def _inline_view_output(
    relation_definition: RelationArtifactDefinition,
    generation_id: int,
):
    return InlineViewOutput.relation_ref(
        slot_name="editable_people",
        upstream_definition_id=relation_definition.definition_id,
        logical_relation_id=relation_definition.logical_relation_id,
        storage_ref={
            "schema": "okg_derived",
            "relation": (
                relation_definition.logical_relation_id.replace(".", "_")
            ),
            "target_generation_id": generation_id,
            "upstream_manifest_id": None,
            "upstream_target_generation_id": generation_id,
            "bound_compute_hash": relation_definition.compute_hash,
            "bound_output_schema_hash": relation_definition.output_schema_hash,
            "bound_renderer_hash": relation_definition.renderer_hash,
            "bound_dirty_rule_hash": relation_definition.dirty_rule_hash,
            "columns": [
                {
                    "name": "display_name", "label": "Name",
                    "data_type": "text", "required": True,
                },
                {
                    "name": "status", "label": "Status",
                    "data_type": "text", "required": True,
                },
            ],
            "column_names": ["display_name", "status"],
            "scope": {"policy": "column", "column": "scope_key"},
            "row_identity": {"policy": "column", "column": "row_key"},
        },
        row_count=1,
    )


def _publish_view_with_row(
    conn,
    *,
    view: ViewArtifactDefinition,
    relation_definition: RelationArtifactDefinition,
    generation_id: int,
    display_name: str = "Old name",
):
    """Publish a relation + view in one shot; returns the relation +
    view artifacts so the lens registration can reference them."""

    relation = publish_relation_artifact(
        conn,
        relation_definition,
        target_generation_id=generation_id,
        rows=[
            RelationRow(
                scope_key="person",
                row_key="person:p-1",
                row_data={
                    "display_name": display_name,
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
                slot_name="editable_people",
                upstream_definition_id=relation_definition.definition_id,
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
                            "name": "display_name", "label": "Name",
                            "data_type": "text", "required": True,
                        },
                        {
                            "name": "status", "label": "Status",
                            "data_type": "text", "required": True,
                        },
                    ],
                    "column_names": ["display_name", "status"],
                    "scope": {"policy": "column", "column": "scope_key"},
                    "row_identity": {"policy": "column", "column": "row_key"},
                },
                row_count=relation.row_count,
            )
        ],
    )
    return relation, published_view


def _publish_editable_view(
    conn: psycopg.Connection,
    *,
    suffix: str,
    generation_id: int,
    display_name: str = "Old name",
) -> tuple[ViewArtifactDefinition, LensDefinition, object]:
    view = _view_definition(suffix)
    relation_definition = _relation_definition(suffix)
    relation = publish_relation_artifact(
        conn,
        relation_definition,
        target_generation_id=generation_id,
        rows=[
            RelationRow(
                scope_key="person",
                row_key="person:p-1",
                row_data={
                    "display_name": display_name,
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
                slot_name="editable_people",
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
                            "name": "display_name",
                            "label": "Name",
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
                    "column_names": ["display_name", "status"],
                    "scope": {"policy": "column", "column": "scope_key"},
                    "row_identity": {"policy": "column", "column": "row_key"},
                },
                row_count=relation.row_count,
            )
        ],
    )
    return view, _lens_definition(suffix, view), published_view


def _commit_overlay(conn: psycopg.Connection, lens: LensDefinition, *, manifest_id, generation_id: int):
    prepared = prepare_lens_putback(
        conn,
        lens,
        base_view_manifest_id=manifest_id,
        target_generation_id=generation_id,
        actor_id="operator:test",
        idempotency_key=f"promote-test-{uuid4().hex}",
        deltas=[
            LensPutbackDelta(
                slot_name="editable_people",
                scope_key="person",
                row_key="person:p-1",
                field_path=("display_name",),
                expected_value="Old name",
                new_value="New name",
                source_authority="test.people",
                source_record_id={"node_id": "person:p-1"},
                expected_source_revision={"revision": "rev-1"},
            )
        ],
    )
    return commit_lens_overlay(conn, request_id=prepared.request_id)


def _set_promotion_mapping(
    conn: psycopg.Connection,
    definition_id: str,
    mapping: dict | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.lens_definitions
               SET lens_promotion_mapping = %(mapping)s
             WHERE definition_id = %(definition_id)s
            """,
            {
                "definition_id": definition_id,
                "mapping": None if mapping is None else Jsonb(mapping),
            },
        )
    conn.commit()


def _person_attr_mapping() -> dict:
    return {
        "version": 1,
        "rules": [
            {
                "id": "person.display_name",
                "slot_name": "editable_people",
                "field_path": ["display_name"],
                "mutation": {
                    "kind": "node_attr",
                    "subtype": "person",
                    "attr_field": "display_name",
                    "node_id_template": "{row_key}",
                },
            }
        ],
    }


def _person_attr_and_status_mapping() -> dict:
    mapping = _person_attr_mapping()
    mapping["rules"] = [
        *mapping["rules"],
        {
            "id": "person.status",
            "slot_name": "editable_people",
            "field_path": ["status"],
            "mutation": {
                "kind": "node_attr",
                "subtype": "person",
                "attr_field": "status",
                "node_id_template": "{row_key}",
            },
        },
    ]
    return mapping


def _lens_with_status_editable(base_lens: LensDefinition) -> LensDefinition:
    lens_contract = {
        **dict(base_lens.lens_contract),
        "editable_slots": {
            "editable_people": {
                "row_identity": "row_key",
                "editable_fields": ["display_name", "status"],
            }
        },
    }
    return replace(
        base_lens,
        lens_contract=lens_contract,
        output_schema_hash=stable_hash(lens_contract),
    )


def test_promote_without_mapping_raises_not_supported(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        assert committed.status == "committed_overlay"
        # explicit NULL — promotion must be refused
        _set_promotion_mapping(conn, lens.definition_id, None)

        with pytest.raises(LensPromotionNotSupported) as excinfo:
            promote_lens_overlay(
                conn,
                request_id=committed.request_id,
                actor_id="operator:test",
            )
        assert "lens_promotion_mapping" in excinfo.value.fix_hint


def test_promote_opens_lens_admission_envelope(clean_runtime: str) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )

        mapping = _person_attr_mapping()
        _set_promotion_mapping(conn, lens.definition_id, mapping)

        result = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert result.inserted is True
        assert result.status == "pending"
        assert result.producer_id == lens_producer_id(lens.definition_id)
        assert len(result.planned_mutations) == 1
        mutation = result.planned_mutations[0]
        assert mutation.subtype == "person"
        assert mutation.node_id == "person:p-1"
        assert mutation.attrs == {"display_name": "New name"}
        assert mutation.kind == "node_attr"

        # Envelope written with producer_kind='lens'
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT producer_kind, producer_id, intent, status, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": result.admission_id},
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == "lens"
        assert row[1] == result.producer_id
        assert row[2] == "promote_lens_overlay"
        assert row[3] == "pending"
        verdict = row[4]
        assert verdict["kind"] == "lens_promotion_plan"
        assert verdict["lens_request_id"] == str(committed.request_id)
        assert verdict["mapping_checksum"] == (
            lens_promotion_mapping_checksum(mapping)
        )


def test_promote_idempotent_for_same_request(clean_runtime: str) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )

        first = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        second = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )

        assert first.inserted is True
        # Second call coalesces — same envelope id, inserted=False.
        assert second.inserted is False
        assert second.admission_id == first.admission_id


def test_promote_refuses_non_committed_overlay(clean_runtime: str) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        # Prepare but do not commit so status stays 'prepared'.
        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            idempotency_key=f"prep-only-{uuid4().hex}",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_people",
                    scope_key="person",
                    row_key="person:p-1",
                    field_path=("display_name",),
                    expected_value="Old name",
                    new_value="New name",
                    source_authority="test.people",
                    source_record_id={"node_id": "person:p-1"},
                    expected_source_revision={"revision": "rev-1"},
                )
            ],
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )

        with pytest.raises(LensPromotionStateError):
            promote_lens_overlay(
                conn,
                request_id=prepared.request_id,
                actor_id="operator:test",
            )


def test_promote_with_no_matching_rule_raises_mapping_error(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )

        # Mapping targets a different slot — no overlay row will match.
        mapping = {
            "version": 1,
            "rules": [
                {
                    "id": "no-match",
                    "slot_name": "some_other_slot",
                    "field_path": ["display_name"],
                    "mutation": {
                        "kind": "node_attr",
                        "subtype": "person",
                        "attr_field": "display_name",
                        "node_id_template": "{row_key}",
                    },
                }
            ],
        }
        _set_promotion_mapping(conn, lens.definition_id, mapping)

        with pytest.raises(LensPromotionMappingError):
            promote_lens_overlay(
                conn,
                request_id=committed.request_id,
                actor_id="operator:test",
            )


def test_promote_with_partially_unmapped_overlay_fails_closed(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, base_lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        lens = _lens_with_status_editable(base_lens)
        register_lens_definition(conn, lens)
        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            idempotency_key=f"partial-unmapped-{uuid4().hex}",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_people",
                    scope_key="person",
                    row_key="person:p-1",
                    field_path=("display_name",),
                    expected_value="Old name",
                    new_value="New name",
                    source_authority="test.people",
                    source_record_id={"node_id": "person:p-1"},
                    expected_source_revision={"revision": "rev-1"},
                ),
                LensPutbackDelta(
                    slot_name="editable_people",
                    scope_key="person",
                    row_key="person:p-1",
                    field_path=("status",),
                    expected_value="current",
                    new_value="inactive",
                    source_authority="test.people",
                    source_record_id={"node_id": "person:p-1"},
                    expected_source_revision={"revision": "rev-1"},
                ),
            ],
        )
        committed = commit_lens_overlay(conn, request_id=prepared.request_id)
        assert committed.status == "committed_overlay"
        _set_promotion_mapping(conn, lens.definition_id, _person_attr_mapping())

        with pytest.raises(LensPromotionMappingError, match="unmapped overlay"):
            promote_lens_overlay(
                conn,
                request_id=committed.request_id,
                actor_id="operator:test",
            )


def test_apply_promoted_overlay_fails_closed_when_plan_omits_overlay(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, base_lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        lens = _lens_with_status_editable(base_lens)
        register_lens_definition(conn, lens)
        prepared = prepare_lens_putback(
            conn,
            lens,
            base_view_manifest_id=published.manifest_id,
            target_generation_id=generation_id,
            actor_id="operator:test",
            idempotency_key=f"apply-missing-overlay-{uuid4().hex}",
            deltas=[
                LensPutbackDelta(
                    slot_name="editable_people",
                    scope_key="person",
                    row_key="person:p-1",
                    field_path=("display_name",),
                    expected_value="Old name",
                    new_value="New name",
                    source_authority="test.people",
                    source_record_id={"node_id": "person:p-1"},
                    expected_source_revision={"revision": "rev-1"},
                ),
                LensPutbackDelta(
                    slot_name="editable_people",
                    scope_key="person",
                    row_key="person:p-1",
                    field_path=("status",),
                    expected_value="current",
                    new_value="inactive",
                    source_authority="test.people",
                    source_record_id={"node_id": "person:p-1"},
                    expected_source_revision={"revision": "rev-1"},
                ),
            ],
        )
        committed = commit_lens_overlay(conn, request_id=prepared.request_id)
        assert committed.status == "committed_overlay"
        _set_promotion_mapping(
            conn,
            lens.definition_id,
            _person_attr_and_status_mapping(),
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert promotion.status == "pending"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            verdict = dict(cur.fetchone()[0])
            assert len(verdict["planned_mutations"]) == 2
            verdict["planned_mutations"] = verdict["planned_mutations"][:1]
            cur.execute(
                """
                UPDATE okg.admission_envelopes
                   SET verdict = %(verdict)s
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {
                    "admission_id": promotion.admission_id,
                    "verdict": Jsonb(verdict),
                },
            )
        conn.commit()

        applied = apply_promoted_overlay(
            conn,
            admission_id=promotion.admission_id,
        )
        assert applied.status == "failed"
        assert applied.nodes_added == 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope_status, failed_verdict = cur.fetchone()
            cur.execute(
                """
                SELECT array_agg(status ORDER BY delta_id)
                  FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
            overlay_statuses = cur.fetchone()[0]
            cur.execute(
                """
                SELECT status FROM okg.lens_putback_requests
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
            request_status = cur.fetchone()[0]
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact_count = cur.fetchone()[0]
        assert envelope_status == "failed"
        assert failed_verdict["failure"]["code"] == (
            "lens_promotion_overlay_plan_mismatch"
        )
        assert len(failed_verdict["failure"]["missing_overlay_ids"]) == 1
        assert overlay_statuses == ["active", "active"]
        assert request_status == "committed_overlay"
        assert fact_count == 0


def test_lens_producer_kind_accepted_by_producer_authority() -> None:
    """The producer_authority module accepts 'lens' as a kind."""

    from okg.substrate.producer_authority import PRODUCER_KINDS

    assert "lens" in PRODUCER_KINDS


def _activate_lens_profile(
    conn: psycopg.Connection,
    *,
    lens: LensDefinition,
    subtypes: tuple[str, ...] = ("person",),
) -> None:
    """Materialize an active `_lens:<def_id>` producer profile."""

    materialize_producer_profile(
        conn,
        deployment_name=lens.deployment_name,
        producer_id=lens_producer_id(lens.definition_id),
        producer_kind="lens",
        trust_label="derived-deterministic-trusted",
        admission_mode="review",
        authority_scope={
            "lens_definition_id": lens.definition_id,
            "mapping_checksum": "sha256:test",
        },
        allowed_record_kinds=("node",),
        output_signature={
            "nodes": [
                {
                    "subtype": s,
                    "clause_id": f"lens.{s}.attr",
                }
                for s in subtypes
            ],
        },
        policy_clauses=[
            {
                "id": f"lens.{s}.attr",
                "effect": "allow",
                "record_kind": "node",
                "subtype": s,
            }
            for s in subtypes
        ],
        output_scope_summary={},
        source_ref={},
        catalog_version_id=None,
    )
    conn.commit()


def test_apply_promoted_overlay_writes_node_fact_and_flips_status(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )

        applied = apply_promoted_overlay(
            conn,
            admission_id=promotion.admission_id,
        )
        assert applied.status == "published"
        assert applied.nodes_added == 1
        assert applied.target_generation_ref is None

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT node_id, subtype, attrs, source, op
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact = cur.fetchone()
        assert fact is not None
        assert fact[0] == "person:p-1"
        assert fact[1] == "person"
        assert fact[2] == {"display_name": "New name"}
        assert fact[3] == lens_producer_id(lens.definition_id)
        assert fact[4] == "I"

        # Overlay row + request reflect promoted status
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, promoted_admission_id IS NOT NULL,
                       promoted_at IS NOT NULL
                  FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
            overlay = cur.fetchone()
            cur.execute(
                """
                SELECT status FROM okg.lens_putback_requests
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
            request_status = cur.fetchone()
            cur.execute(
                """
                SELECT status, target_generation_ref,
                       verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
        assert overlay == ("promoted", True, True)
        assert request_status == ("promoted",)
        assert envelope is not None
        assert envelope[0] == "published"
        assert envelope[1] is None
        assert envelope[2]["applied"]["target_generation_ref"] is None


def test_apply_promoted_overlay_rejects_when_profile_missing(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        # NOTE: no producer profile is materialized

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        applied = apply_promoted_overlay(
            conn,
            admission_id=promotion.admission_id,
        )

        assert applied.status == "rejected"
        assert applied.nodes_added == 0
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
        assert envelope is not None
        assert envelope[0] == "rejected"
        assert envelope[1]["rejection"]["code"] == "producer_profile_missing"


def test_apply_promoted_overlay_idempotent(clean_runtime: str) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        first = apply_promoted_overlay(
            conn,
            admission_id=promotion.admission_id,
        )
        # A second apply is a no-op: status stays published, no
        # duplicate facts get written.
        second = apply_promoted_overlay(
            conn,
            admission_id=promotion.admission_id,
        )
        assert first.status == "published"
        assert second.status == "published"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*) FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            count = cur.fetchone()[0]
        assert count == 1


def test_list_pending_lens_envelopes_filters_by_status(
    clean_runtime: str,
) -> None:
    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        _view, lens, published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )

        before = list_pending_lens_envelopes(
            conn, deployment_name=lens.deployment_name
        )
        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        pending = list_pending_lens_envelopes(
            conn, deployment_name=lens.deployment_name
        )
        assert str(promotion.admission_id) not in (str(x) for x in before)
        assert str(promotion.admission_id) in (str(x) for x in pending)


def test_publisher_infers_lens_kind_from_producer_id_prefix() -> None:
    """An authority block missing producer_kind falls back to the
    prefix-derived kind."""

    from okg.substrate.publisher.publisher import _infer_derived_producer_kind

    assert _infer_derived_producer_kind("_lens:my_lens") == "lens"
    assert _infer_derived_producer_kind("_extractor:x") == "extractor"
    assert _infer_derived_producer_kind("_enricher:y") == "enricher"


class _BlockingConsistencyGate:
    name = "lens_test_blocking_gate"

    def evaluate(
        self,
        conn,
        *,
        target_generation_id: int,
        catalog_version_id: int,
        deployment_name: str,
        branch_name: str = "default",
    ) -> ConsistencyPublishGateEvaluation:
        return ConsistencyPublishGateEvaluation(
            run_id=uuid4(),
            check_id=self.name,
            target_generation_id=target_generation_id,
            gate_name=self.name,
            result="blocked_error",
            blocks_publish=True,
            reason="test_lens_blocked_publish",
            open_observation_count=0,
            coverage_complete=False,
            coverage_basis="test",
            budget_ms=None,
            elapsed_ms=1,
            preflight={
                "catalog_version_id": catalog_version_id,
                "deployment_name": deployment_name,
                "branch_name": branch_name,
            },
            details={},
        )


def test_publisher_cycle_drains_accepted_lens_envelope(
    clean_runtime: str,
) -> None:
    """End-to-end Phase B: publisher cycle's lens-promotion phase
    finds the accepted envelope, applies the mutation, the projector
    picks up the derived node_fact, and the overlay row + envelope
    transition to terminal states."""

    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-promote-{suffix[:8]}"
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        # The publisher creates its own generation; we only need a
        # pre-existing committed overlay attached to an EARLIER
        # generation so the request row resolves to a real
        # graph_generation FK.
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-promote-e2e:{generation_id}",
                },
            )
        conn.commit()

        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        relation_definition = _relation_definition(
            suffix, deployment_name=deployment
        )
        _relation, published_view = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=relation_definition,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published_view.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert promotion.status == "pending"

    # Now run a publisher cycle in a fresh connection so the cycle
    # acquires its own lock + transaction context.
    pub = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    out = pub.cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref,
                       verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
            cur.execute(
                """
                SELECT status, promoted_admission_id IS NOT NULL
                  FROM okg.lens_overlay_log
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
            overlay = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact_count = cur.fetchone()[0]
    assert envelope is not None
    assert envelope[0] == "published"
    assert out["generation_id"].startswith("okg:")
    assert envelope[1] == out["generation_id"]
    assert envelope[2]["applied"]["target_generation_ref"] == (
        out["generation_id"]
    )
    assert envelope[2]["applied"]["target_generation_status"] == "published"
    assert overlay == ("promoted", True)
    assert fact_count == 1

    # Re-running the publisher cycle is idempotent for the lens
    # phase: no new node_fact gets written, no duplicate envelope.
    pub2 = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    out2 = pub2.cycle()
    assert out2["status"] in {"published", "no_op"}
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            count_after_second = cur.fetchone()[0]
    assert count_after_second == 1


def test_pending_lens_promotion_disables_noop_generation_reuse(
    clean_runtime: str,
) -> None:
    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-promote-noop-{suffix[:8]}"
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-promote-noop:{generation_id}",
                },
            )
        conn.commit()

        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        relation_definition = _relation_definition(
            suffix, deployment_name=deployment
        )
        _relation, published_view = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=relation_definition,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

    baseline = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert baseline["status"] == "published", baseline

    with psycopg.connect(clean_runtime) as conn:
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published_view.manifest_id,
            generation_id=generation_id,
        )
        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert promotion.status == "pending"

    out = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment)).cycle()
    assert out["status"] == "published", out
    assert out.get("reused_generation") is not True
    assert out["generation_id"] != baseline["generation_id"]

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact_count = cur.fetchone()[0]
    assert envelope == (
        "published",
        out["generation_id"],
    )
    assert fact_count == 1


def test_blocked_publisher_cycle_links_lens_envelope_to_blocked_generation(
    clean_runtime: str,
) -> None:
    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-promote-blocked-{suffix[:8]}"
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-promote-blocked:{generation_id}",
                },
            )
        conn.commit()

        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        relation_definition = _relation_definition(
            suffix, deployment_name=deployment
        )
        _relation, published_view = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=relation_definition,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published_view.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert promotion.status == "pending"

    pub = Publisher(
        clean_runtime,
        deployment,
        consistency_gates=[_BlockingConsistencyGate()],
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    out = pub.cycle()
    assert out["status"] == "blocked", out

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref,
                       verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
            cur.execute(
                """
                SELECT event_type, reason, payload
                  FROM okg.admission_events
                 WHERE admission_id = %(admission_id)s::uuid
                   AND reason = 'lens_promotion_generation_ref_finalized'
                """,
                {"admission_id": promotion.admission_id},
            )
            event = cur.fetchone()

    assert envelope is not None
    assert envelope[0] == "blocked"
    assert envelope[1] == out["generation_id"]
    assert envelope[2]["applied"]["target_generation_status"] == "blocked"
    assert event is not None
    assert event[0] == "blocked"
    assert event[1] == "lens_promotion_generation_ref_finalized"
    assert event[2]["target_generation_ref"] == out["generation_id"]
    assert event[2]["status"] == "blocked"
    assert event[2]["generation_status"] == "blocked"


def test_publisher_cycle_rejects_lens_envelope_when_profile_missing(
    clean_runtime: str,
) -> None:
    """The publisher cycle's lens-promotion phase rejects an envelope
    whose deployment has not declared the matching producer profile.
    """

    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-noprofile-{suffix[:8]}"
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-promote-noprofile:{generation_id}",
                },
            )
        conn.commit()
        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        relation_definition = _relation_definition(
            suffix, deployment_name=deployment
        )
        _relation, published_view = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=relation_definition,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published_view.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        # NOTE: no producer profile materialized.

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
        )
        assert promotion.status == "pending"

    pub = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    out = pub.cycle()
    # Profile missing → envelope rejected → no derived facts.
    assert out["status"] in {"published", "no_op"}

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, verdict
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
            cur.execute(
                """
                SELECT count(*)
                  FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact_count = cur.fetchone()[0]
    assert envelope is not None
    assert envelope[0] == "rejected"
    assert envelope[1]["rejection"]["code"] == "producer_profile_missing"
    assert fact_count == 0


def test_doctor_lens_promotions_surfaces_stale_overlay(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`okg doctor --check lens-promotions` warns on a committed
    overlay that has aged past `--stale-after-days`."""

    suffix = uuid4().hex
    deployment = f"test-doctor-{suffix[:8]}"
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-doctor:{generation_id}",
                },
            )
        conn.commit()
        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        relation_definition = _relation_definition(
            suffix, deployment_name=deployment
        )
        _relation, published_view = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=relation_definition,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn,
            lens,
            manifest_id=published_view.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        # Backdate the committed_at to make it stale.
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.lens_putback_requests
                   SET committed_at = now() - interval '40 days'
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": str(committed.request_id)},
            )
        conn.commit()

    from okg.substrate.cli.doctor import _doctor_lens_promotions
    import argparse

    args = argparse.Namespace(
        dsn=clean_runtime,
        deployment=deployment,
        stale_after_days=30,
    )
    rc = _doctor_lens_promotions(args, as_json=True)
    captured = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert captured["ok"] is True
    warning_codes = [w["code"] for w in captured["warnings"]]
    assert "committed_overlay_stale" in warning_codes
    assert "producer_profile_missing" in warning_codes


def test_list_pending_lens_envelopes_filters_by_target_branch(
    clean_runtime: str,
) -> None:
    """AKMON Phase B.2: an envelope pinned to a non-default branch is
    invisible to a publisher cycle running on the default branch, and
    vice versa."""

    suffix = uuid4().hex
    deployment = f"test-branch-route-{suffix[:8]}"
    generation_id = _generation_id()
    quarantine_branch = f"quarantine:lens-{suffix[:6]}"

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:lens-branch-route:{generation_id}",
                },
            )
        conn.commit()
        view = _view_definition(suffix, deployment_name=deployment)
        lens_default = _lens_definition(
            suffix, view, deployment_name=deployment
        )
        suffix_q = f"{suffix}-q"
        view_q = _view_definition(suffix_q, deployment_name=deployment)
        lens_quarantine = _lens_definition(
            suffix_q, view_q, deployment_name=deployment
        )
        rel_default = _relation_definition(
            suffix, deployment_name=deployment
        )
        rel_quarantine = _relation_definition(
            suffix_q, deployment_name=deployment
        )
        _r1, pv_default = _publish_view_with_row(
            conn,
            view=view,
            relation_definition=rel_default,
            generation_id=generation_id,
        )
        _r2, pv_quarantine = _publish_view_with_row(
            conn,
            view=view_q,
            relation_definition=rel_quarantine,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens_default)
        register_lens_definition(conn, lens_quarantine)
        req_default = _commit_overlay(
            conn,
            lens_default,
            manifest_id=pv_default.manifest_id,
            generation_id=generation_id,
        )
        req_quarantine = _commit_overlay(
            conn,
            lens_quarantine,
            manifest_id=pv_quarantine.manifest_id,
            generation_id=generation_id,
        )
        for lens in (lens_default, lens_quarantine):
            _set_promotion_mapping(
                conn, lens.definition_id, _person_attr_mapping()
            )

        promo_default = promote_lens_overlay(
            conn,
            request_id=req_default.request_id,
            actor_id="operator:test",
            target_branch_name=None,
        )
        promo_quarantine = promote_lens_overlay(
            conn,
            request_id=req_quarantine.request_id,
            actor_id="operator:test",
            target_branch_name=quarantine_branch,
        )

        default_pending = list_pending_lens_envelopes(
            conn, deployment_name=deployment, branch_name="default"
        )
        quarantine_pending = list_pending_lens_envelopes(
            conn,
            deployment_name=deployment,
            branch_name=quarantine_branch,
        )
        all_pending = list_pending_lens_envelopes(
            conn, deployment_name=deployment
        )

    default_ids = {str(x) for x in default_pending}
    quarantine_ids = {str(x) for x in quarantine_pending}
    all_ids = {str(x) for x in all_pending}
    assert promo_default.admission_id in default_ids
    assert promo_quarantine.admission_id not in default_ids
    assert promo_quarantine.admission_id in quarantine_ids
    assert promo_default.admission_id not in quarantine_ids
    assert {
        promo_default.admission_id, promo_quarantine.admission_id
    }.issubset(all_ids)


def test_publisher_cycles_route_envelopes_to_their_branches(
    clean_runtime: str,
) -> None:
    """End-to-end Phase B.2: two open lens envelopes in the same
    deployment — one default, one quarantine — yield two generations,
    each carrying only its matching envelope's derived node_fact.
    """

    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-route-{suffix[:8]}"
    generation_id = _generation_id()
    quarantine_branch = f"quarantine:lens-{suffix[:6]}"

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:branch-route-e2e:{generation_id}",
                },
            )
            # Per fix-akmon-design-mistakes task 1: non-default
            # branches must be created explicitly before the
            # publisher will accept them. Auto-create-on-publish
            # was removed for `quarantine:*` to surface operator
            # typos at branch-creation time.
            cur.execute(
                """
                INSERT INTO okg.branches
                  (deployment_name, name, role, status)
                VALUES (%s, %s, 'quarantine', 'active')
                ON CONFLICT DO NOTHING
                """,
                (deployment, quarantine_branch),
            )
        conn.commit()

        view_d = _view_definition(suffix, deployment_name=deployment)
        lens_d = _lens_definition(suffix, view_d, deployment_name=deployment)
        rel_d = _relation_definition(suffix, deployment_name=deployment)
        suffix_q = f"{suffix}-q"
        view_q = _view_definition(suffix_q, deployment_name=deployment)
        lens_q = _lens_definition(
            suffix_q, view_q, deployment_name=deployment
        )
        rel_q = _relation_definition(suffix_q, deployment_name=deployment)
        _r1, pv_d = _publish_view_with_row(
            conn, view=view_d, relation_definition=rel_d,
            generation_id=generation_id,
        )
        _r2, pv_q = _publish_view_with_row(
            conn, view=view_q, relation_definition=rel_q,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens_d)
        register_lens_definition(conn, lens_q)
        req_d = _commit_overlay(
            conn, lens_d, manifest_id=pv_d.manifest_id,
            generation_id=generation_id,
        )
        req_q = _commit_overlay(
            conn, lens_q, manifest_id=pv_q.manifest_id,
            generation_id=generation_id,
        )
        for lens in (lens_d, lens_q):
            _set_promotion_mapping(
                conn, lens.definition_id, _person_attr_mapping()
            )
            _activate_lens_profile(conn, lens=lens)

        promo_d = promote_lens_overlay(
            conn,
            request_id=req_d.request_id,
            actor_id="operator:test",
            target_branch_name=None,
        )
        promo_q = promote_lens_overlay(
            conn,
            request_id=req_q.request_id,
            actor_id="operator:test",
            target_branch_name=quarantine_branch,
        )

    pub_default = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    out_default = pub_default.cycle()
    assert out_default["status"] == "published"

    # fork-publish-soundness (fce6eb7aa): a fork branch's FIRST publish must
    # resolve its base generation, else the consistency gate blocks loudly
    # rather than silently full-validating the inherited base. The quarantine
    # branch was registered above with no lineage metadata; now that the
    # default generation is published, wire the quarantine Chronos branch to
    # it so `resolve_fork_base_checkpoint` resolves and the cycle publishes.
    from okg.substrate.graph_store import ChronosGraphStore

    fork_store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        fork_store.create_branch(
            quarantine_branch,
            from_branch="default",
            metadata={
                "base_branch": "default",
                "base_generation_id": out_default["generation_id"],
            },
        )
    finally:
        fork_store.close()

    pub_quarantine = Publisher(
        clean_runtime, deployment, branch_name=quarantine_branch,
        catalog_identity=bind_test_catalog_writer(clean_runtime, deployment),
    )
    out_quarantine = pub_quarantine.cycle()
    assert out_quarantine["status"] == "published"
    assert out_default["generation_id"] != out_quarantine["generation_id"]

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promo_d.admission_id},
            )
            envelope_d = cur.fetchone()
            cur.execute(
                """
                SELECT status, target_generation_ref
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promo_q.admission_id},
            )
            envelope_q = cur.fetchone()
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        gen_branches = {
            checkpoint.checkpoint_id: checkpoint.metadata.get("branch")
            for checkpoint in store.list_generations(
                deployment_name=deployment,
                status="published",
            )
            if checkpoint.checkpoint_id in {
                out_default["generation_id"],
                out_quarantine["generation_id"],
            }
        }
    finally:
        store.close()

    assert envelope_d[0] == "published"
    assert envelope_q[0] == "published"
    assert gen_branches[out_default["generation_id"]] == "default"
    assert gen_branches[out_quarantine["generation_id"]] == quarantine_branch
    # Each envelope must carry ITS OWN branch's generation ref. This test
    # already selected the refs and then asserted nothing about them, so the
    # one case positioned to catch cross-branch mis-linking was checking only
    # that both rows reached 'published' — which they would even if the
    # finalize pass had stamped both with the same generation.
    assert envelope_d[1] == out_default["generation_id"]
    assert envelope_q[1] == out_quarantine["generation_id"]
    assert envelope_d[1] != envelope_q[1]


def test_default_publisher_does_not_drain_quarantine_envelope(
    clean_runtime: str,
) -> None:
    """A publisher on the default branch leaves an envelope pinned to
    a quarantine branch in `pending` state — it is invisible to the
    default cycle's lens-promotion phase."""

    from okg.substrate.publisher.publisher import Publisher

    suffix = uuid4().hex
    deployment = f"test-route-skip-{suffix[:8]}"
    generation_id = _generation_id()
    quarantine_branch = f"quarantine:lens-{suffix[:6]}"

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.graph_generations (
                  generation_id, deployment_name, status,
                  catalog_version_id, published_at, correlation_id
                ) VALUES (
                  %(g)s, %(deployment)s, 'published', 1, now(),
                  %(corr)s
                )
                """,
                {
                    "g": generation_id,
                    "deployment": deployment,
                    "corr": f"test:branch-route-skip:{generation_id}",
                },
            )
        conn.commit()
        view = _view_definition(suffix, deployment_name=deployment)
        lens = _lens_definition(suffix, view, deployment_name=deployment)
        rel = _relation_definition(suffix, deployment_name=deployment)
        _r, pv = _publish_view_with_row(
            conn, view=view, relation_definition=rel,
            generation_id=generation_id,
        )
        register_lens_definition(conn, lens)
        committed = _commit_overlay(
            conn, lens, manifest_id=pv.manifest_id,
            generation_id=generation_id,
        )
        _set_promotion_mapping(
            conn, lens.definition_id, _person_attr_mapping()
        )
        _activate_lens_profile(conn, lens=lens)

        promotion = promote_lens_overlay(
            conn,
            request_id=committed.request_id,
            actor_id="operator:test",
            target_branch_name=quarantine_branch,
        )

    pub_default = Publisher(clean_runtime, deployment, catalog_identity=bind_test_catalog_writer(clean_runtime, deployment))
    pub_default.cycle()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT status, target_branch_name
                  FROM okg.admission_envelopes
                 WHERE admission_id = %(admission_id)s::uuid
                """,
                {"admission_id": promotion.admission_id},
            )
            envelope = cur.fetchone()
            cur.execute(
                """
                SELECT count(*) FROM okg.node_facts
                 WHERE source = %(producer_id)s
                """,
                {"producer_id": lens_producer_id(lens.definition_id)},
            )
            fact_count = cur.fetchone()[0]
    assert envelope == ("pending", quarantine_branch)
    assert fact_count == 0


def test_register_lens_definition_rejects_malformed_mapping(
    clean_runtime: str,
) -> None:
    """Validation runs at register time, not promote time.

    A malformed `lens_promotion_mapping` (missing required mutation
    field) raises `LensPromotionMappingError` at
    `register_lens_definition()` rather than waiting until a
    `promote_lens_overlay()` call surfaces it.
    """

    suffix = uuid4().hex
    generation_id = _generation_id()

    with psycopg.connect(clean_runtime) as conn:
        _insert_generation(conn, generation_id)
        view, lens, _published = _publish_editable_view(
            conn, suffix=suffix, generation_id=generation_id
        )
        # Construct a malformed mapping (mutation.kind missing).
        malformed_mapping = {
            "version": 1,
            "rules": [
                {
                    "id": "person.display_name",
                    "slot_name": "editable_people",
                    "mutation": {
                        # missing required "kind"
                        "subtype": "person",
                        "attr_field": "display_name",
                    },
                }
            ],
        }
        from dataclasses import replace

        bad_lens = replace(lens, lens_promotion_mapping=malformed_mapping)

        with pytest.raises(LensPromotionMappingError):
            register_lens_definition(conn, bad_lens)
