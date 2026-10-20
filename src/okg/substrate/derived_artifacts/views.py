"""Native V1 view artifacts over derived-artifact manifests.

Views publish typed inline output slots through the canonical Postgres
manifest model. The completed output-set manifest is the only durable
read authority; legacy disk manifests and ``view`` graph nodes are not
published by this module.
"""
from __future__ import annotations

import json
import time
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from .relations import DirtyScope
from .relations import LINEAGE_STATUS_UNAVAILABLE
from .relations import RELATION_MEDIA_TYPE
from .relations import RELATION_SLOT_NAME
from .relations import _lineage_unavailable_summary
from .relations import _metrics_with_lineage
from .relations import _prepare_lineage_envelopes
from .relations import _publish_lineage_for_manifest
from .relations import _set_run_lineage_metrics
from .relations import stable_hash
from okg.substrate.derived_artifacts.lineage import MaterializedLineageEnvelope

VIEW_MARKDOWN_SLOT_NAME = "markdown"
VIEW_MARKDOWN_MEDIA_TYPE = "text/markdown"
VIEW_MARKDOWN_ARTIFACT_TYPE = "okg/view-markdown-v1"
VIEW_MARKDOWN_RENDERER_REF = "okg.renderer.markdown.v1"

VIEW_HTML_SLOT_NAME = "html"
VIEW_HTML_MEDIA_TYPE = "text/html"
VIEW_HTML_ARTIFACT_TYPE = "okg/view-html-v1"
VIEW_HTML_RENDERER_REF = "okg.renderer.html.sandboxed.v1"

VIEW_JSON_SLOT_NAME = "json"
VIEW_JSON_MEDIA_TYPE = "application/json"
VIEW_JSON_ARTIFACT_TYPE = "okg/view-json-v1"
VIEW_JSON_RENDERER_REF = "okg.renderer.json.v1"

VIEW_TABLE_SLOT_NAME = "table"
VIEW_TABLE_MEDIA_TYPE = "application/vnd.okg.table+json"
VIEW_TABLE_ARTIFACT_TYPE = "okg/view-table-json-v1"
VIEW_TABLE_RENDERER_REF = "okg.renderer.table.json.v1"


@dataclass(frozen=True)
class ViewArtifactDefinition:
    """Definition metadata for a representational view artifact."""

    definition_id: str
    deployment_name: str
    view_id: str
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    view_name: str = ""
    renderer_ref: str = VIEW_MARKDOWN_RENDERER_REF
    output_contract: Mapping[str, Any] = field(default_factory=dict)
    dirty_rule: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InlineViewOutput:
    """One inline output slot for a view manifest."""

    slot_name: str
    media_type: str
    artifact_type: str
    renderer_ref: str
    payload: str | Mapping[str, Any] | Sequence[Any] | int | float | bool | None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    storage_kind: str = "inline"
    storage_ref: Mapping[str, Any] | None = None
    row_count: int | None = None
    byte_count: int | None = None

    @classmethod
    def markdown(
        cls,
        text: str,
        *,
        slot_name: str = VIEW_MARKDOWN_SLOT_NAME,
        renderer_ref: str = VIEW_MARKDOWN_RENDERER_REF,
        metadata: Mapping[str, Any] | None = None,
    ) -> "InlineViewOutput":
        return cls(
            slot_name=slot_name,
            media_type=VIEW_MARKDOWN_MEDIA_TYPE,
            artifact_type=VIEW_MARKDOWN_ARTIFACT_TYPE,
            renderer_ref=renderer_ref,
            payload=text,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def html(
        cls,
        html: str,
        *,
        slot_name: str = VIEW_HTML_SLOT_NAME,
        renderer_ref: str = VIEW_HTML_RENDERER_REF,
        metadata: Mapping[str, Any] | None = None,
    ) -> "InlineViewOutput":
        return cls(
            slot_name=slot_name,
            media_type=VIEW_HTML_MEDIA_TYPE,
            artifact_type=VIEW_HTML_ARTIFACT_TYPE,
            renderer_ref=renderer_ref,
            payload=html,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def json(
        cls,
        value: Mapping[str, Any] | Sequence[Any],
        *,
        slot_name: str = VIEW_JSON_SLOT_NAME,
        renderer_ref: str = VIEW_JSON_RENDERER_REF,
        metadata: Mapping[str, Any] | None = None,
    ) -> "InlineViewOutput":
        return cls(
            slot_name=slot_name,
            media_type=VIEW_JSON_MEDIA_TYPE,
            artifact_type=VIEW_JSON_ARTIFACT_TYPE,
            renderer_ref=renderer_ref,
            payload=value,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def table(
        cls,
        value: Mapping[str, Any],
        *,
        slot_name: str = VIEW_TABLE_SLOT_NAME,
        renderer_ref: str = VIEW_TABLE_RENDERER_REF,
        metadata: Mapping[str, Any] | None = None,
    ) -> "InlineViewOutput":
        return cls(
            slot_name=slot_name,
            media_type=VIEW_TABLE_MEDIA_TYPE,
            artifact_type=VIEW_TABLE_ARTIFACT_TYPE,
            renderer_ref=renderer_ref,
            payload=value,
            metadata=dict(metadata or {}),
        )

    @classmethod
    def relation_ref(
        cls,
        *,
        slot_name: str,
        upstream_definition_id: str,
        logical_relation_id: str,
        renderer_ref: str = "okg.renderer.sql_relation.table.v1",
        media_type: str = RELATION_MEDIA_TYPE,
        artifact_type: str = "okg/view-relation-reference-v1",
        upstream_slot_name: str = RELATION_SLOT_NAME,
        metadata: Mapping[str, Any] | None = None,
        storage_ref: Mapping[str, Any] | None = None,
        row_count: int | None = None,
    ) -> "InlineViewOutput":
        ref = {
            "kind": "derived_relation",
            "upstream_definition_id": upstream_definition_id,
            "upstream_slot_name": upstream_slot_name,
            "logical_relation_id": logical_relation_id,
            **dict(storage_ref or {}),
        }
        return cls(
            slot_name=slot_name,
            media_type=media_type,
            artifact_type=artifact_type,
            renderer_ref=renderer_ref,
            payload=None,
            metadata=dict(metadata or {}),
            storage_kind="relation",
            storage_ref=ref,
            row_count=row_count,
        )


@dataclass(frozen=True)
class PublishedViewArtifact:
    """Identity and slot summary for a published view manifest."""

    run_id: UUID
    manifest_id: UUID
    view_id: str
    target_generation_id: int
    manifest_hash: str
    slot_payload_hashes: Mapping[str, str]
    carried_forward: bool = False
    reused_from_manifest_id: UUID | None = None
    lineage_status: str = LINEAGE_STATUS_UNAVAILABLE
    lineage_envelope_count: int = 0
    lineage_dependency_key_count: int = 0
    lineage_dependency_kinds: tuple[str, ...] = ()
    timings_ms: Mapping[str, float] = field(default_factory=dict)


def _required_hashes(definition: ViewArtifactDefinition) -> dict[str, str]:
    return {
        "compute_hash": definition.compute_hash,
        "output_schema_hash": definition.output_schema_hash,
        "renderer_hash": definition.renderer_hash,
        "dirty_rule_hash": definition.dirty_rule_hash,
    }


def _upsert_view_definition(
    cur: psycopg.Cursor[Any],
    definition: ViewArtifactDefinition,
) -> None:
    output_contract = {
        "view_id": definition.view_id,
        "view_name": definition.view_name,
        **dict(definition.output_contract),
    }
    cur.execute(
        """
        INSERT INTO okg.derived_artifact_definitions (
          definition_id, artifact_kind, deployment_name, logical_key,
          compute_hash, output_schema_hash, renderer_hash,
          dirty_rule_hash, renderer_ref, output_contract, dirty_rule
        ) VALUES (
          %(definition_id)s, 'view', %(deployment_name)s, %(logical_key)s,
          %(compute_hash)s, %(output_schema_hash)s, %(renderer_hash)s,
          %(dirty_rule_hash)s, %(renderer_ref)s,
          %(output_contract)s, %(dirty_rule)s
        )
        ON CONFLICT (definition_id) DO UPDATE SET
          artifact_kind = 'view',
          deployment_name = EXCLUDED.deployment_name,
          logical_key = EXCLUDED.logical_key,
          compute_hash = EXCLUDED.compute_hash,
          output_schema_hash = EXCLUDED.output_schema_hash,
          renderer_hash = EXCLUDED.renderer_hash,
          dirty_rule_hash = EXCLUDED.dirty_rule_hash,
          renderer_ref = EXCLUDED.renderer_ref,
          output_contract = EXCLUDED.output_contract,
          dirty_rule = EXCLUDED.dirty_rule,
          updated_at = now()
        WHERE (
          okg.derived_artifact_definitions.artifact_kind,
          okg.derived_artifact_definitions.deployment_name,
          okg.derived_artifact_definitions.logical_key,
          okg.derived_artifact_definitions.compute_hash,
          okg.derived_artifact_definitions.output_schema_hash,
          okg.derived_artifact_definitions.renderer_hash,
          okg.derived_artifact_definitions.dirty_rule_hash,
          okg.derived_artifact_definitions.renderer_ref,
          okg.derived_artifact_definitions.output_contract,
          okg.derived_artifact_definitions.dirty_rule
        ) IS DISTINCT FROM (
          'view',
          EXCLUDED.deployment_name,
          EXCLUDED.logical_key,
          EXCLUDED.compute_hash,
          EXCLUDED.output_schema_hash,
          EXCLUDED.renderer_hash,
          EXCLUDED.dirty_rule_hash,
          EXCLUDED.renderer_ref,
          EXCLUDED.output_contract,
          EXCLUDED.dirty_rule
        )
        """,
        {
            "definition_id": definition.definition_id,
            "deployment_name": definition.deployment_name,
            "logical_key": definition.view_id,
            "compute_hash": definition.compute_hash,
            "output_schema_hash": definition.output_schema_hash,
            "renderer_hash": definition.renderer_hash,
            "dirty_rule_hash": definition.dirty_rule_hash,
            "renderer_ref": definition.renderer_ref,
            "output_contract": Jsonb(output_contract),
            "dirty_rule": Jsonb(dict(definition.dirty_rule)),
        },
    )


def register_view_artifact_definition(
    conn: psycopg.Connection[Any],
    definition: ViewArtifactDefinition,
) -> None:
    """Ensure a native view artifact definition exists."""

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_view_definition(cur, definition)


def enqueue_view_dirty_scope(
    conn: psycopg.Connection[Any],
    definition: ViewArtifactDefinition,
    *,
    target_generation_id: int,
    dirty_scope: DirtyScope,
) -> None:
    """Persist a dirty scope for a native view artifact."""

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_view_definition(cur, definition)
            cur.execute(
                """
                INSERT INTO okg.derived_dirty_scopes (
                  dirty_rule_run_id, definition_id,
                  target_generation_id, scope_key,
                  dirty_reason, dependency_kind, dependency_ref,
                  dependency_hash
                ) VALUES (
                  %(dirty_rule_run_id)s, %(definition_id)s,
                  %(target_generation_id)s, %(scope_key)s,
                  %(dirty_reason)s, %(dependency_kind)s,
                  %(dependency_ref)s, %(dependency_hash)s
                )
                ON CONFLICT (
                  definition_id, target_generation_id, scope_key,
                  dependency_kind, dependency_ref
                ) DO UPDATE SET
                  dirty_rule_run_id = EXCLUDED.dirty_rule_run_id,
                  dirty_reason = EXCLUDED.dirty_reason,
                  dependency_hash = EXCLUDED.dependency_hash,
                  queued_at = now(),
                  consumed_run_id = NULL,
                  consumed_at = NULL
                """,
                {
                    "dirty_rule_run_id": dirty_scope.dirty_rule_run_id,
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "scope_key": dirty_scope.scope_key,
                    "dirty_reason": dirty_scope.dirty_reason,
                    "dependency_kind": dirty_scope.dependency_kind,
                    "dependency_ref": dirty_scope.dependency_ref,
                    "dependency_hash": dirty_scope.dependency_hash,
                },
            )


def _inline_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return {"kind": "text", "text": value}
    return {"kind": "json", "value": value}


def _payload_bytes(inline_payload: Mapping[str, Any]) -> bytes:
    if inline_payload.get("kind") == "text":
        return str(inline_payload.get("text", "")).encode("utf-8")
    return json.dumps(
        inline_payload.get("value"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")


def _slot_records(
    *,
    definition: ViewArtifactDefinition,
    target_generation_id: int,
    outputs: Sequence[InlineViewOutput],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    records: list[dict[str, Any]] = []
    for output in outputs:
        if output.slot_name in seen:
            raise ValueError(f"duplicate view output slot {output.slot_name!r}")
        seen.add(output.slot_name)
        if output.storage_kind == "inline":
            inline_payload = _inline_payload(output.payload)
            storage_ref = {
                "kind": "inline",
                "definition_id": definition.definition_id,
                "view_id": definition.view_id,
                "target_generation_id": target_generation_id,
                "slot_name": output.slot_name,
            }
            payload_hash = stable_hash(
                {
                    "media_type": output.media_type,
                    "artifact_type": output.artifact_type,
                    "renderer_ref": output.renderer_ref,
                    "inline_payload": inline_payload,
                }
            )
            byte_count = len(_payload_bytes(inline_payload))
        else:
            if output.storage_ref is None:
                raise ValueError(
                    f"non-inline view output slot {output.slot_name!r} "
                    "requires storage_ref"
                )
            inline_payload = None
            storage_ref = dict(output.storage_ref)
            payload_hash = stable_hash(
                {
                    "media_type": output.media_type,
                    "artifact_type": output.artifact_type,
                    "renderer_ref": output.renderer_ref,
                    "storage_kind": output.storage_kind,
                    "storage_ref": storage_ref,
                }
            )
            byte_count = (
                output.byte_count
                if output.byte_count is not None
                else len(_payload_bytes({"kind": "json", "value": storage_ref}))
            )
        records.append(
            {
                "slot_name": output.slot_name,
                "storage_kind": output.storage_kind,
                "media_type": output.media_type,
                "artifact_type": output.artifact_type,
                "renderer_ref": output.renderer_ref,
                "payload_hash": payload_hash,
                "storage_ref": storage_ref,
                "inline_payload": inline_payload,
                "row_count": output.row_count,
                "byte_count": byte_count,
                "metadata": dict(output.metadata),
            }
        )
    if not records:
        raise ValueError("publish_view_artifact requires at least one output slot")
    return sorted(records, key=lambda record: record["slot_name"])


def _manifest_hash(
    *,
    definition: ViewArtifactDefinition,
    target_generation_id: int,
    slot_records: Sequence[Mapping[str, Any]],
    reused_from_manifest_id: UUID | None = None,
) -> str:
    return stable_hash(
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "required_slots": [record["slot_name"] for record in slot_records],
            "reused_from_manifest_id": (
                str(reused_from_manifest_id)
                if reused_from_manifest_id is not None
                else None
            ),
            "slots": [
                {
                    "slot_name": record["slot_name"],
                    "storage_kind": record["storage_kind"],
                    "media_type": record["media_type"],
                    "artifact_type": record["artifact_type"],
                    "renderer_ref": record["renderer_ref"],
                    "payload_hash": record["payload_hash"],
                    "storage_ref": record["storage_ref"],
                }
                for record in slot_records
            ],
        }
    )


def _carry_forward_manifest_hash(
    *,
    definition: ViewArtifactDefinition,
    target_generation_id: int,
    required_slots: Sequence[str],
    source_manifest_id: UUID,
    source_manifest_hash: str,
) -> str:
    return stable_hash(
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "required_slots": list(required_slots),
            "reused_from_manifest_id": str(source_manifest_id),
            "source_manifest_hash": source_manifest_hash,
        }
    )


def _insert_view_slot_records(
    cur: psycopg.Cursor[Any],
    *,
    manifest_id: UUID,
    slot_records: Sequence[Mapping[str, Any]],
) -> None:
    cur.executemany(
        """
        INSERT INTO okg.derived_output_slots (
          manifest_id, slot_name, storage_kind, media_type,
          artifact_type, renderer_ref, payload_hash, storage_ref,
          inline_payload, row_count, byte_count, metadata
        ) VALUES (
          %(manifest_id)s, %(slot_name)s, %(storage_kind)s,
          %(media_type)s, %(artifact_type)s, %(renderer_ref)s,
          %(payload_hash)s, %(storage_ref)s, %(inline_payload)s,
          %(row_count)s, %(byte_count)s, %(metadata)s
        )
        """,
        [
            {
                "manifest_id": manifest_id,
                "slot_name": record["slot_name"],
                "storage_kind": record["storage_kind"],
                "media_type": record["media_type"],
                "artifact_type": record["artifact_type"],
                "renderer_ref": record["renderer_ref"],
                "payload_hash": record["payload_hash"],
                "storage_ref": Jsonb(record["storage_ref"]),
                "inline_payload": Jsonb(record["inline_payload"]),
                "row_count": record["row_count"],
                "byte_count": record["byte_count"],
                "metadata": Jsonb(record["metadata"]),
            }
            for record in slot_records
        ],
    )


def publish_view_artifact(
    conn: psycopg.Connection[Any],
    definition: ViewArtifactDefinition,
    *,
    target_generation_id: int,
    outputs: Sequence[InlineViewOutput],
    trigger_kind: str = "manual",
    dependency_manifest_ids: Sequence[UUID] = (),
    metrics: Mapping[str, Any] | None = None,
    lineage_envelopes: (
        Sequence[MaterializedLineageEnvelope] | None
    ) = None,
    use_transaction: bool = True,
    definition_registered: bool = False,
) -> PublishedViewArtifact:
    """Publish inline view outputs as one completed V1 manifest."""

    timings_ms: dict[str, float] = {}
    total_started = time.perf_counter()

    def add_timing(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    slot_records = _slot_records(
        definition=definition,
        target_generation_id=target_generation_id,
        outputs=outputs,
    )
    required_slots = [record["slot_name"] for record in slot_records]
    manifest_hash = _manifest_hash(
        definition=definition,
        target_generation_id=target_generation_id,
        slot_records=slot_records,
    )
    add_timing("publish.prepare_slots", step_started)
    step_started = time.perf_counter()
    prepared_lineage = _prepare_lineage_envelopes(lineage_envelopes)
    lineage_summary = _lineage_unavailable_summary()
    run_metrics = (
        _metrics_with_lineage(metrics, lineage_summary)
        if prepared_lineage is None
        else dict(metrics or {})
    )
    add_timing("publish.prepare_lineage", step_started)

    transaction_started = time.perf_counter()
    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            transaction_body_started = time.perf_counter()
            step_started = time.perf_counter()
            if definition_registered:
                timings_ms["publish.upsert_definition.skipped"] = 1.0
            else:
                _upsert_view_definition(cur, definition)
            add_timing("publish.upsert_definition", step_started)

            run_id = uuid4()
            manifest_id = uuid4()
            step_started = time.perf_counter()
            cur.execute(
                """
                WITH ins_run AS (
                  INSERT INTO okg.derived_artifact_runs (
                    run_id, definition_id, target_generation_id, status,
                    trigger_kind, compute_hash, output_schema_hash,
                    renderer_hash, dirty_rule_hash,
                    dependency_manifest_ids, total_scopes, dirty_scopes,
                    dirty_rows, reused_scopes, metrics, output_manifest_id,
                    completed_at
                  ) VALUES (
                    %(run_id)s, %(definition_id)s, %(target_generation_id)s,
                    'completed',
                    %(trigger_kind)s, %(compute_hash)s,
                    %(output_schema_hash)s, %(renderer_hash)s,
                    %(dirty_rule_hash)s, %(dependency_manifest_ids)s,
                    1, 1, %(dirty_rows)s, 0, %(metrics)s, %(manifest_id)s,
                    now()
                  )
                  RETURNING run_id
                ),
                ins_manifest AS (
                  INSERT INTO okg.derived_output_manifests (
                    manifest_id, run_id, definition_id, target_generation_id,
                    status, required_slots, manifest_hash, promoted_at
                  )
                  SELECT %(manifest_id)s, ins_run.run_id, %(definition_id)s,
                         %(target_generation_id)s, 'completed',
                         %(required_slots)s, %(manifest_hash)s, now()
                    FROM ins_run
                  RETURNING manifest_id
                ),
                slot_input AS (
                  SELECT *
                    FROM jsonb_to_recordset(%(slot_records)s::jsonb) AS s(
                      slot_name text,
                      storage_kind text,
                      media_type text,
                      artifact_type text,
                      renderer_ref text,
                      payload_hash text,
                      storage_ref jsonb,
                      inline_payload jsonb,
                      row_count bigint,
                      byte_count bigint,
                      metadata jsonb
                    )
                )
                INSERT INTO okg.derived_output_slots (
                  manifest_id, slot_name, storage_kind, media_type,
                  artifact_type, renderer_ref, payload_hash, storage_ref,
                  inline_payload, row_count, byte_count, metadata
                )
                SELECT ins_manifest.manifest_id, slot_input.slot_name,
                       slot_input.storage_kind, slot_input.media_type,
                       slot_input.artifact_type, slot_input.renderer_ref,
                       slot_input.payload_hash, slot_input.storage_ref,
                       slot_input.inline_payload, slot_input.row_count,
                       slot_input.byte_count, slot_input.metadata
                  FROM ins_manifest
                 CROSS JOIN slot_input
                """,
                {
                    **_required_hashes(definition),
                    "run_id": run_id,
                    "manifest_id": manifest_id,
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "trigger_kind": trigger_kind,
                    "dependency_manifest_ids": list(dependency_manifest_ids),
                    "dirty_rows": len(slot_records),
                    "metrics": Jsonb(run_metrics),
                    "required_slots": required_slots,
                    "manifest_hash": manifest_hash,
                    "slot_records": Jsonb([
                        {
                            "slot_name": record["slot_name"],
                            "storage_kind": record["storage_kind"],
                            "media_type": record["media_type"],
                            "artifact_type": record["artifact_type"],
                            "renderer_ref": record["renderer_ref"],
                            "payload_hash": record["payload_hash"],
                            "storage_ref": record["storage_ref"],
                            "inline_payload": record["inline_payload"],
                            "row_count": record["row_count"],
                            "byte_count": record["byte_count"],
                            "metadata": record["metadata"],
                        }
                        for record in slot_records
                    ]),
                },
            )
            add_timing("publish.insert_metadata", step_started)
            timings_ms["publish.insert_metadata.coalesced_run_manifest_slots"] = 1.0

            step_started = time.perf_counter()
            lineage_summary = _publish_lineage_for_manifest(
                conn,
                manifest_id=manifest_id,
                envelopes=prepared_lineage,
            )
            add_timing("publish.publish_lineage", step_started)
            if prepared_lineage is not None:
                step_started = time.perf_counter()
                _set_run_lineage_metrics(
                    cur,
                    run_id=run_id,
                    lineage=lineage_summary,
                )
                add_timing("publish.set_lineage_metrics", step_started)
            add_timing("publish.transaction.body", transaction_body_started)
    if not use_transaction:
        timings_ms["publish.transaction.external"] = 1.0
    add_timing("publish.transaction", transaction_started)
    add_timing("publish.total", total_started)

    return PublishedViewArtifact(
        run_id=run_id,
        manifest_id=manifest_id,
        view_id=definition.view_id,
        target_generation_id=target_generation_id,
        manifest_hash=manifest_hash,
        slot_payload_hashes={
            record["slot_name"]: record["payload_hash"]
            for record in slot_records
        },
        lineage_status=lineage_summary.status,
        lineage_envelope_count=lineage_summary.envelope_count,
        lineage_dependency_key_count=lineage_summary.dependency_key_count,
        lineage_dependency_kinds=lineage_summary.dependency_kinds,
        timings_ms=timings_ms,
    )


def carry_forward_view_artifact(
    conn: psycopg.Connection[Any],
    definition: ViewArtifactDefinition,
    *,
    source_manifest_id: UUID,
    target_generation_id: int,
    metrics: Mapping[str, Any] | None = None,
    lineage_envelopes: (
        Sequence[MaterializedLineageEnvelope] | None
    ) = None,
) -> PublishedViewArtifact:
    """Reuse a completed view manifest for a later generation."""

    timings_ms: dict[str, float] = {}
    total_started = time.perf_counter()

    def add_timing(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    transaction_started = time.perf_counter()
    with conn.transaction():
        with conn.cursor() as cur:
            transaction_body_started = time.perf_counter()
            published = _carry_forward_view_artifact_in_cursor(
                conn,
                cur,
                definition,
                source_manifest_id=source_manifest_id,
                target_generation_id=target_generation_id,
                metrics=metrics,
                lineage_envelopes=lineage_envelopes,
                timings_ms=timings_ms,
            )
            add_timing("carry_forward.transaction.body", transaction_body_started)
    add_timing("carry_forward.transaction", transaction_started)
    add_timing("carry_forward.total", total_started)
    return replace(published, timings_ms=dict(timings_ms))


def _carry_forward_view_artifact_in_cursor(
    conn: psycopg.Connection[Any],
    cur: psycopg.Cursor[Any],
    definition: ViewArtifactDefinition,
    *,
    source_manifest_id: UUID,
    target_generation_id: int,
    metrics: Mapping[str, Any] | None = None,
    definition_registered: bool = False,
    lineage_envelopes: (
        Sequence["MaterializedLineageEnvelope"] | None
    ) = None,
    timings_ms: dict[str, float] | None = None,
) -> PublishedViewArtifact:
    timings = timings_ms if timings_ms is not None else {}

    def add_timing(label: str, started: float) -> None:
        timings[label] = round(
            timings.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    prepared_lineage = _prepare_lineage_envelopes(lineage_envelopes)
    lineage_summary = _lineage_unavailable_summary()
    run_metrics = (
        _metrics_with_lineage(metrics, lineage_summary)
        if prepared_lineage is None
        else dict(metrics or {})
    )
    add_timing("carry_forward.prepare_lineage", step_started)

    step_started = time.perf_counter()
    if not definition_registered:
        _upsert_view_definition(cur, definition)
    source = _load_completed_view_manifest_header(
        cur,
        definition,
        source_manifest_id,
    )
    _ensure_definition_hashes_match(definition, source)
    add_timing("carry_forward.load_source_manifest", step_started)

    step_started = time.perf_counter()
    required_slots = list(source["required_slots"] or ())
    manifest_hash = _carry_forward_manifest_hash(
        definition=definition,
        target_generation_id=target_generation_id,
        required_slots=required_slots,
        source_manifest_id=source_manifest_id,
        source_manifest_hash=source["manifest_hash"],
    )
    add_timing("carry_forward.prepare_manifest_pointer", step_started)

    run_id = uuid4()
    manifest_id = uuid4()
    step_started = time.perf_counter()
    cur.execute(
        """
        WITH ins_run AS (
          INSERT INTO okg.derived_artifact_runs (
            run_id, definition_id, target_generation_id, status,
            trigger_kind, compute_hash, output_schema_hash,
            renderer_hash, dirty_rule_hash,
            dependency_manifest_ids, reused_from_run_id,
            total_scopes, dirty_scopes, dirty_rows,
            reused_scopes, metrics, output_manifest_id, completed_at
          ) VALUES (
            %(run_id)s, %(definition_id)s, %(target_generation_id)s,
            'carried_forward',
            'carry_forward', %(compute_hash)s,
            %(output_schema_hash)s, %(renderer_hash)s,
            %(dirty_rule_hash)s, %(dependency_manifest_ids)s,
            %(reused_from_run_id)s, 1, 0, 0, 1, %(metrics)s,
            %(manifest_id)s, now()
          )
          RETURNING run_id
        )
        INSERT INTO okg.derived_output_manifests (
          manifest_id, run_id, definition_id, target_generation_id,
          status, required_slots, manifest_hash,
          reused_from_manifest_id, promoted_at
        )
        SELECT %(manifest_id)s, ins_run.run_id, %(definition_id)s,
               %(target_generation_id)s, 'completed',
               %(required_slots)s, %(manifest_hash)s,
               %(source_manifest_id)s, now()
          FROM ins_run
        """,
        {
            **_required_hashes(definition),
            "run_id": run_id,
            "manifest_id": manifest_id,
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "dependency_manifest_ids": [source_manifest_id],
            "reused_from_run_id": source["run_id"],
            "metrics": Jsonb(run_metrics),
            "required_slots": required_slots,
            "manifest_hash": manifest_hash,
            "source_manifest_id": source_manifest_id,
        },
    )
    add_timing("carry_forward.insert_metadata", step_started)
    timings["carry_forward.insert_metadata.coalesced_run_manifest"] = 1.0
    timings["carry_forward.insert_slots.skipped_pointer_reuse"] = 1.0

    step_started = time.perf_counter()
    lineage_summary = _publish_lineage_for_manifest(
        conn,
        manifest_id=manifest_id,
        envelopes=prepared_lineage,
    )
    add_timing("carry_forward.publish_lineage", step_started)
    if prepared_lineage is not None:
        step_started = time.perf_counter()
        _set_run_lineage_metrics(
            cur,
            run_id=run_id,
            lineage=lineage_summary,
        )
        add_timing("carry_forward.set_lineage_metrics", step_started)
    return PublishedViewArtifact(
        run_id=run_id,
        manifest_id=manifest_id,
        view_id=definition.view_id,
        target_generation_id=target_generation_id,
        manifest_hash=manifest_hash,
        slot_payload_hashes={},
        carried_forward=True,
        reused_from_manifest_id=source_manifest_id,
        lineage_status=lineage_summary.status,
        lineage_envelope_count=lineage_summary.envelope_count,
        lineage_dependency_key_count=lineage_summary.dependency_key_count,
        lineage_dependency_kinds=lineage_summary.dependency_kinds,
        timings_ms=dict(timings),
    )


def _load_completed_view_manifest(
    cur: psycopg.Cursor[Any],
    definition: ViewArtifactDefinition,
    manifest_id: UUID,
) -> dict[str, Any]:
    header = _load_completed_view_manifest_header(
        cur,
        definition,
        manifest_id,
    )
    cur.execute(
        """
        SELECT slot_name, storage_kind, media_type, artifact_type,
               renderer_ref, payload_hash, storage_ref, inline_payload,
               row_count, byte_count, metadata
          FROM okg.derived_output_slots
         WHERE manifest_id = %(manifest_id)s
         ORDER BY slot_name
        """,
        {"manifest_id": manifest_id},
    )
    slots = [
        {
            "slot_name": slot[0],
            "storage_kind": slot[1],
            "media_type": slot[2],
            "artifact_type": slot[3],
            "renderer_ref": slot[4],
            "payload_hash": slot[5],
            "storage_ref": slot[6],
            "inline_payload": slot[7],
            "row_count": slot[8],
            "byte_count": slot[9],
            "metadata": slot[10],
        }
        for slot in cur.fetchall()
    ]
    return {**header, "slots": slots}


def _load_completed_view_manifest_header(
    cur: psycopg.Cursor[Any],
    definition: ViewArtifactDefinition,
    manifest_id: UUID,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT m.manifest_id, m.run_id, m.definition_id,
               m.target_generation_id, m.required_slots, m.manifest_hash,
               m.reused_from_manifest_id,
               r.compute_hash, r.output_schema_hash, r.renderer_hash,
               r.dirty_rule_hash
          FROM okg.derived_output_manifests m
          JOIN okg.derived_artifact_runs r
            ON r.run_id = m.run_id
         WHERE m.manifest_id = %(manifest_id)s
           AND m.definition_id = %(definition_id)s
           AND m.status = 'completed'
           AND r.status IN ('completed', 'carried_forward')
        """,
        {
            "manifest_id": manifest_id,
            "definition_id": definition.definition_id,
        },
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            "completed view manifest not found for "
            f"{definition.definition_id!r}: {manifest_id}"
        )
    return {
        "manifest_id": row[0],
        "run_id": row[1],
        "definition_id": row[2],
        "target_generation_id": row[3],
        "required_slots": row[4],
        "manifest_hash": row[5],
        "reused_from_manifest_id": row[6],
        "compute_hash": row[7],
        "output_schema_hash": row[8],
        "renderer_hash": row[9],
        "dirty_rule_hash": row[10],
    }


def _ensure_definition_hashes_match(
    definition: ViewArtifactDefinition,
    source: Mapping[str, Any],
) -> None:
    for field_name, expected in _required_hashes(definition).items():
        if source[field_name] != expected:
            raise ValueError(
                f"cannot reuse manifest with mismatched {field_name}: "
                f"source={source[field_name]!r} definition={expected!r}"
            )
