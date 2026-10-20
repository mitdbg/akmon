"""Relation-backed derived artifact publication.

The implementation here is deliberately small. It exercises the V1
contract's load-bearing pieces before the scheduler, consistency
evaluators, and view/lens adapters depend on them:

* definitions record independent compute/schema/renderer/dirty hashes;
* relation DDL is minted and owned by the substrate;
* outputs publish through a complete manifest in one transaction;
* carry-forward reuse creates generation-visible run/manifest state.
"""
from __future__ import annotations

import json
import time
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Mapping, Sequence
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

# Owned by the effect-free `hashing` module so planning code can reach
# them without importing psycopg. Imported back here so every existing
# `from ...relations import stable_hash, RELATION_SLOT_NAME` still
# resolves to the same objects.
from okg.substrate.derived_artifacts.hashing import (
    RELATION_SLOT_NAME,
    stable_hash,
)


DERIVED_RELATION_SCHEMA = "okg_derived"
RELATION_MEDIA_TYPE = "application/vnd.okg.relation+json"
RELATION_ARTIFACT_TYPE = "okg/sql-jsonb-relation-v1"
RELATION_RENDERER_REF = "okg.renderer.sql_relation.jsonb.v1"
LINEAGE_STATUS_AVAILABLE = "lineage_available"
LINEAGE_STATUS_UNAVAILABLE = "lineage_unavailable"

if TYPE_CHECKING:
    from okg.substrate.derived_artifacts.lineage import (
        MaterializedLineageEnvelope,
    )


@dataclass(frozen=True)
class RelationArtifactDefinition:
    """Definition metadata for a JSONB relation artifact."""

    definition_id: str
    deployment_name: str
    logical_relation_id: str
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    artifact_kind: str = "derived_artifact"
    renderer_ref: str = RELATION_RENDERER_REF
    output_contract: Mapping[str, Any] = field(default_factory=dict)
    dirty_rule: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RelationRow:
    """One logical row in a derived relation output."""

    scope_key: str
    row_key: str
    row_data: Mapping[str, Any]
    dependency_hash: str | None = None


@dataclass(frozen=True)
class DirtyScope:
    """Durable dirty-scope event for a derived artifact."""

    scope_key: str
    dirty_reason: str
    dependency_kind: str
    dependency_ref: str
    dependency_hash: str | None = None
    dirty_rule_run_id: UUID | None = None


@dataclass(frozen=True)
class PublishedArtifact:
    """Identity of a published output-set manifest."""

    run_id: UUID
    manifest_id: UUID
    schema_name: str
    relation_name: str
    row_count: int
    manifest_hash: str
    storage_ref: Mapping[str, Any] = field(default_factory=dict)
    payload_hash: str = ""
    byte_count: int = 0
    carried_forward: bool = False
    reused_from_manifest_id: UUID | None = None
    dirty_scope_count: int = 0
    reused_scope_count: int = 0
    lineage_status: str = LINEAGE_STATUS_UNAVAILABLE
    lineage_envelope_count: int = 0
    lineage_dependency_key_count: int = 0
    lineage_dependency_kinds: tuple[str, ...] = ()
    timings_ms: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class VisibleRelationRow:
    """One row visible through a relation manifest's reuse chain."""

    scope_key: str
    row_key: str
    row_data: Mapping[str, Any]
    row_hash: str
    dependency_hash: str | None
    byte_count: int
    target_generation_id: int


@dataclass(frozen=True)
class RelationDirtyRowsMatchResult:
    matches: bool
    source_manifest: Mapping[str, Any] | None = None
    previous_dirty_stats: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class LineagePublicationSummary:
    """Durable lineage publication status for one output manifest."""

    status: str
    envelope_count: int = 0
    dependency_key_count: int = 0
    dependency_kinds: tuple[str, ...] = ()
    reason: str | None = None

    def to_metrics(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": self.status,
            "envelope_count": self.envelope_count,
            "dependency_key_count": self.dependency_key_count,
            "dependency_kinds": list(self.dependency_kinds),
        }
        if self.reason:
            payload["reason"] = self.reason
        return payload


def _prepare_lineage_envelopes(
    lineage_envelopes: Sequence["MaterializedLineageEnvelope"] | None,
) -> tuple["MaterializedLineageEnvelope", ...] | None:
    if lineage_envelopes is None:
        return None
    envelopes = tuple(lineage_envelopes)
    if not envelopes:
        from .dependency_index import LineageIndexError

        raise LineageIndexError(
            "lineage_envelopes must contain at least one envelope when "
            "provided; omit the argument to return lineage_unavailable"
        )
    return envelopes


def _lineage_unavailable_summary() -> LineagePublicationSummary:
    return LineagePublicationSummary(
        status=LINEAGE_STATUS_UNAVAILABLE,
        reason="lineage_envelopes_not_provided",
    )


def _metrics_with_lineage(
    metrics: Mapping[str, Any] | None,
    lineage: LineagePublicationSummary,
) -> dict[str, Any]:
    payload = dict(metrics or {})
    payload["lineage"] = lineage.to_metrics()
    return payload


def _publish_lineage_for_manifest(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: UUID,
    envelopes: Sequence["MaterializedLineageEnvelope"] | None,
) -> LineagePublicationSummary:
    if envelopes is None:
        return _lineage_unavailable_summary()

    from .dependency_index import publish_lineage_envelopes

    result = publish_lineage_envelopes(
        conn,
        manifest_id=manifest_id,
        envelopes=envelopes,
    )
    return LineagePublicationSummary(
        status=LINEAGE_STATUS_AVAILABLE,
        envelope_count=result.envelope_count,
        dependency_key_count=result.dependency_key_count,
        dependency_kinds=result.dependency_kinds,
    )


def _set_run_lineage_metrics(
    cur: psycopg.Cursor[Any],
    *,
    run_id: UUID,
    lineage: LineagePublicationSummary,
) -> None:
    cur.execute(
        """
        UPDATE okg.derived_artifact_runs
           SET metrics = metrics || %(lineage_metrics)s
         WHERE run_id = %(run_id)s
        """,
        {
            "run_id": run_id,
            "lineage_metrics": Jsonb({"lineage": lineage.to_metrics()}),
        },
    )


def _physical_relation_name(definition: RelationArtifactDefinition) -> str:
    digest = stable_hash(
        {
            "definition_id": definition.definition_id,
            "deployment_name": definition.deployment_name,
            "logical_relation_id": definition.logical_relation_id,
            "output_schema_hash": definition.output_schema_hash,
        }
    )[:32]
    return f"da_{digest}"


def _insert_relation_rows(
    cur: psycopg.Cursor[Any],
    *,
    physical_name: str,
    definition_id: str,
    target_generation_id: int,
    rows: Sequence[RelationRow],
    row_hashes: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    if not rows:
        return {
            "row_count": 0,
            "scope_count": 0,
            "byte_count": 0,
        }
    cur.execute(
        sql.SQL(
            """
            WITH inserted AS (
              INSERT INTO {}.{} (
                definition_id, target_generation_id, scope_key,
                row_key, row_data, row_hash, dependency_hash
              )
              SELECT
                %(definition_id)s,
                %(target_generation_id)s,
                payload.scope_key,
                payload.row_key,
                payload.row_data,
                payload.row_hash,
                payload.dependency_hash
              FROM jsonb_to_recordset(%(rows)s::jsonb) AS payload(
                scope_key text,
                row_key text,
                row_data jsonb,
                row_hash text,
                dependency_hash text
              )
              RETURNING scope_key, pg_column_size(row_data) AS byte_count
            )
            SELECT count(*)::bigint,
                   count(DISTINCT scope_key)::bigint,
                   COALESCE(sum(byte_count), 0)::bigint
              FROM inserted
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "definition_id": definition_id,
            "target_generation_id": target_generation_id,
            "rows": Jsonb(
                [
                    {
                        "scope_key": row.scope_key,
                        "row_key": row.row_key,
                        "row_data": dict(row.row_data),
                        "row_hash": row_hash["row_hash"],
                        "dependency_hash": row.dependency_hash,
                    }
                    for row, row_hash in zip(rows, row_hashes, strict=True)
                ]
            ),
        },
    )
    row = cur.fetchone()
    return {
        "row_count": int(row[0] or 0) if row is not None else 0,
        "scope_count": int(row[1] or 0) if row is not None else 0,
        "byte_count": int(row[2] or 0) if row is not None else 0,
    }


def _ddl_hash(physical_name: str, definition: RelationArtifactDefinition) -> str:
    return stable_hash(
        {
            "storage": "sql-jsonb-relation-v1",
            "schema": DERIVED_RELATION_SCHEMA,
            "relation": physical_name,
            "output_schema_hash": definition.output_schema_hash,
            "columns": [
                "definition_id",
                "target_generation_id",
                "scope_key",
                "row_key",
                "row_data",
                "row_hash",
                "dependency_hash",
                "created_at",
            ],
        }
    )


def _row_hash(row: RelationRow) -> str:
    return stable_hash(
        {
            "scope_key": row.scope_key,
            "row_key": row.row_key,
            "row_data": row.row_data,
            "dependency_hash": row.dependency_hash,
        }
    )


def _required_hashes(definition: RelationArtifactDefinition) -> dict[str, str]:
    return {
        "compute_hash": definition.compute_hash,
        "output_schema_hash": definition.output_schema_hash,
        "renderer_hash": definition.renderer_hash,
        "dirty_rule_hash": definition.dirty_rule_hash,
    }


def _upsert_definition(
    cur: psycopg.Cursor[Any],
    definition: RelationArtifactDefinition,
) -> None:
    cur.execute(
        """
        INSERT INTO okg.derived_artifact_definitions (
          definition_id, artifact_kind, deployment_name, logical_key,
          compute_hash, output_schema_hash, renderer_hash,
          dirty_rule_hash, renderer_ref, output_contract, dirty_rule
        ) VALUES (
          %(definition_id)s, %(artifact_kind)s, %(deployment_name)s,
          %(logical_key)s, %(compute_hash)s, %(output_schema_hash)s,
          %(renderer_hash)s, %(dirty_rule_hash)s, %(renderer_ref)s,
          %(output_contract)s, %(dirty_rule)s
        )
        ON CONFLICT (definition_id) DO UPDATE SET
          artifact_kind = EXCLUDED.artifact_kind,
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
          EXCLUDED.artifact_kind,
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
            "artifact_kind": definition.artifact_kind,
            "deployment_name": definition.deployment_name,
            "logical_key": definition.logical_relation_id,
            "compute_hash": definition.compute_hash,
            "output_schema_hash": definition.output_schema_hash,
            "renderer_hash": definition.renderer_hash,
            "dirty_rule_hash": definition.dirty_rule_hash,
            "renderer_ref": definition.renderer_ref,
            "output_contract": Jsonb(dict(definition.output_contract)),
            "dirty_rule": Jsonb(dict(definition.dirty_rule)),
        },
    )


def register_jsonb_relation(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
) -> str:
    """Ensure the substrate-owned physical table exists.

    The table name is minted from definition identity and output schema
    hash. Callers get the physical relation name, but never provide one.
    """

    physical_name = _physical_relation_name(definition)

    with conn.transaction():
        with conn.cursor() as cur:
            _ensure_relation_registered(cur, definition, physical_name)

    return physical_name


def enqueue_dirty_scope(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    target_generation_id: int,
    dirty_scope: DirtyScope,
) -> None:
    """Persist a dirty scope for replayable incremental scheduling."""

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_definition(cur, definition)
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


def pending_dirty_scope_keys(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    target_generation_id: int,
) -> list[str]:
    """Return unconsumed dirty scopes for a definition/generation."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT scope_key
              FROM okg.derived_dirty_scopes
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
               AND consumed_run_id IS NULL
             GROUP BY scope_key
             ORDER BY scope_key
            """,
            {
                "definition_id": definition.definition_id,
                "target_generation_id": target_generation_id,
            },
        )
        return [row[0] for row in cur.fetchall()]


def publish_relation_artifact(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    target_generation_id: int,
    rows: Sequence[RelationRow],
    trigger_kind: str = "manual",
    dependency_manifest_ids: Sequence[UUID] = (),
    metrics: Mapping[str, Any] | None = None,
    lineage_envelopes: (
        Sequence["MaterializedLineageEnvelope"] | None
    ) = None,
) -> PublishedArtifact:
    """Publish rows as a completed output-set manifest.

    The row writes, output slot insert, manifest promotion, and run
    completion happen in one transaction. A reader never needs to infer
    completeness from physical table state.
    """

    physical_name = _physical_relation_name(definition)
    sorted_rows = sorted(rows, key=lambda r: (r.scope_key, r.row_key))
    row_hashes = [
        {
            "scope_key": row.scope_key,
            "row_key": row.row_key,
            "row_hash": _row_hash(row),
            "dependency_hash": row.dependency_hash,
        }
        for row in sorted_rows
    ]
    payload_hash = stable_hash(
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "relation": physical_name,
            "row_hashes": row_hashes,
        }
    )
    storage_ref = {
        "schema": DERIVED_RELATION_SCHEMA,
        "relation": physical_name,
        "logical_relation_id": definition.logical_relation_id,
        "target_generation_id": target_generation_id,
        "scope_count": len({row.scope_key for row in sorted_rows}),
        "row_count": len(sorted_rows),
    }
    manifest_hash = stable_hash(
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "required_slots": [RELATION_SLOT_NAME],
            "slots": [
                {
                    "slot_name": RELATION_SLOT_NAME,
                    "storage_kind": "relation",
                    "payload_hash": payload_hash,
                    "storage_ref": storage_ref,
                }
            ],
        }
    )
    prepared_lineage = _prepare_lineage_envelopes(lineage_envelopes)
    lineage_summary = _lineage_unavailable_summary()
    run_metrics = (
        _metrics_with_lineage(metrics, lineage_summary)
        if prepared_lineage is None
        else dict(metrics or {})
    )

    with conn.transaction():
        with conn.cursor() as cur:
            _upsert_definition(cur, definition)
            _ensure_relation_registered(cur, definition, physical_name)

            cur.execute(
                """
                INSERT INTO okg.derived_artifact_runs (
                  definition_id, target_generation_id, status,
                  trigger_kind, compute_hash, output_schema_hash,
                  renderer_hash, dirty_rule_hash,
                  dependency_manifest_ids, total_scopes, dirty_scopes,
                  dirty_rows, reused_scopes, metrics
                ) VALUES (
                  %(definition_id)s, %(target_generation_id)s, 'running',
                  %(trigger_kind)s, %(compute_hash)s,
                  %(output_schema_hash)s, %(renderer_hash)s,
                  %(dirty_rule_hash)s, %(dependency_manifest_ids)s,
                  %(total_scopes)s, %(dirty_scopes)s, %(dirty_rows)s,
                  %(reused_scopes)s, %(metrics)s
                )
                RETURNING run_id
                """,
                {
                    **_required_hashes(definition),
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "trigger_kind": trigger_kind,
                    "dependency_manifest_ids": list(dependency_manifest_ids),
                    "total_scopes": len({row.scope_key for row in sorted_rows}),
                    "dirty_scopes": len({row.scope_key for row in sorted_rows}),
                    "dirty_rows": len(sorted_rows),
                    "reused_scopes": 0,
                    "metrics": Jsonb(run_metrics),
                },
            )
            (run_id,) = cur.fetchone()

            cur.execute(
                """
                INSERT INTO okg.derived_output_manifests (
                  run_id, definition_id, target_generation_id, status,
                  required_slots, manifest_hash
                ) VALUES (
                  %(run_id)s, %(definition_id)s,
                  %(target_generation_id)s, 'staged',
                  %(required_slots)s, %(manifest_hash)s
                )
                RETURNING manifest_id
                """,
                {
                    "run_id": run_id,
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "required_slots": [RELATION_SLOT_NAME],
                    "manifest_hash": f"staged:{payload_hash}",
                },
            )
            (manifest_id,) = cur.fetchone()

            _insert_relation_rows(
                cur,
                physical_name=physical_name,
                definition_id=definition.definition_id,
                target_generation_id=target_generation_id,
                rows=sorted_rows,
                row_hashes=row_hashes,
            )

            byte_count = len(
                json.dumps(
                    [dict(row.row_data) for row in sorted_rows],
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    default=str,
                ).encode("utf-8")
            )
            cur.execute(
                """
                INSERT INTO okg.derived_output_slots (
                  manifest_id, slot_name, storage_kind, media_type,
                  artifact_type, renderer_ref, payload_hash, storage_ref,
                  row_count, byte_count
                ) VALUES (
                  %(manifest_id)s, %(slot_name)s, 'relation',
                  %(media_type)s, %(artifact_type)s, %(renderer_ref)s,
                  %(payload_hash)s, %(storage_ref)s,
                  %(row_count)s, %(byte_count)s
                )
                """,
                {
                    "manifest_id": manifest_id,
                    "slot_name": RELATION_SLOT_NAME,
                    "media_type": RELATION_MEDIA_TYPE,
                    "artifact_type": RELATION_ARTIFACT_TYPE,
                    "renderer_ref": definition.renderer_ref,
                    "payload_hash": payload_hash,
                    "storage_ref": Jsonb(storage_ref),
                    "row_count": len(sorted_rows),
                    "byte_count": byte_count,
                },
            )
            cur.execute(
                """
                UPDATE okg.derived_output_manifests
                   SET status = 'completed',
                       manifest_hash = %(manifest_hash)s,
                       promoted_at = now()
                 WHERE manifest_id = %(manifest_id)s
                """,
                {
                    "manifest_id": manifest_id,
                    "manifest_hash": manifest_hash,
                },
            )
            cur.execute(
                """
                UPDATE okg.derived_artifact_runs
                   SET status = 'completed',
                       output_manifest_id = %(manifest_id)s,
                       completed_at = now()
                 WHERE run_id = %(run_id)s
                """,
                {"run_id": run_id, "manifest_id": manifest_id},
            )
            lineage_summary = _publish_lineage_for_manifest(
                conn,
                manifest_id=manifest_id,
                envelopes=prepared_lineage,
            )
            if prepared_lineage is not None:
                _set_run_lineage_metrics(
                    cur,
                    run_id=run_id,
                    lineage=lineage_summary,
                )

    return PublishedArtifact(
        run_id=run_id,
        manifest_id=manifest_id,
        schema_name=DERIVED_RELATION_SCHEMA,
        relation_name=physical_name,
        row_count=len(sorted_rows),
        manifest_hash=manifest_hash,
        storage_ref=storage_ref,
        payload_hash=payload_hash,
        byte_count=byte_count,
        lineage_status=lineage_summary.status,
        lineage_envelope_count=lineage_summary.envelope_count,
        lineage_dependency_key_count=lineage_summary.dependency_key_count,
        lineage_dependency_kinds=lineage_summary.dependency_kinds,
    )


def carry_forward_relation_artifact(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    source_manifest_id: UUID,
    target_generation_id: int,
    metrics: Mapping[str, Any] | None = None,
    definition_registered: bool = False,
    lineage_envelopes: (
        Sequence["MaterializedLineageEnvelope"] | None
    ) = None,
) -> PublishedArtifact:
    """Reuse a completed manifest for a later generation.

    This creates a new run and completed manifest for the target
    generation. The slot points at the prior physical payload and
    records the reuse explicitly, so pinned readers have
    generation-visible evidence even when bytes are reused.
    """

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
            published = _carry_forward_relation_artifact_in_cursor(
                conn,
                cur,
                definition,
                source_manifest_id=source_manifest_id,
                target_generation_id=target_generation_id,
                metrics=metrics,
                definition_registered=definition_registered,
                lineage_envelopes=lineage_envelopes,
                timings_ms=timings_ms,
            )
            add_timing("carry_forward.transaction.body", transaction_body_started)
    add_timing("carry_forward.transaction", transaction_started)
    add_timing("carry_forward.total", total_started)
    return replace(published, timings_ms=dict(timings_ms))


def _carry_forward_relation_artifact_in_cursor(
    conn: psycopg.Connection[Any],
    cur: psycopg.Cursor[Any],
    definition: RelationArtifactDefinition,
    *,
    source_manifest_id: UUID,
    target_generation_id: int,
    metrics: Mapping[str, Any] | None = None,
    definition_registered: bool = False,
    lineage_envelopes: (
        Sequence["MaterializedLineageEnvelope"] | None
    ) = None,
    timings_ms: dict[str, float] | None = None,
) -> PublishedArtifact:
    timings = timings_ms if timings_ms is not None else {}

    def add_timing(label: str, started: float) -> None:
        timings[label] = round(
            timings.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    prepared_lineage = _prepare_lineage_envelopes(lineage_envelopes)
    lineage_summary = _lineage_unavailable_summary()
    run_metrics = (
        _metrics_with_lineage(metrics, lineage_summary)
        if prepared_lineage is None
        else dict(metrics or {})
    )
    step_started = time.perf_counter()
    if not definition_registered:
        _upsert_definition(cur, definition)
    add_timing("carry_forward.upsert_definition", step_started)
    step_started = time.perf_counter()
    source = _load_source_manifest(cur, definition, source_manifest_id)
    physical_name = source["relation_name"]
    _ensure_definition_hashes_match(definition, source)
    add_timing("carry_forward.load_source_manifest", step_started)

    step_started = time.perf_counter()
    run_id = uuid4()
    manifest_id = uuid4()
    storage_ref = {
        **source["storage_ref"],
        "reused_from_manifest_id": str(source_manifest_id),
        "reused_for_generation_id": target_generation_id,
    }
    manifest_hash = stable_hash(
        {
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "required_slots": [RELATION_SLOT_NAME],
            "reused_from_manifest_id": str(source_manifest_id),
            "slots": [
                {
                    "slot_name": RELATION_SLOT_NAME,
                    "storage_kind": "relation",
                    "payload_hash": source["payload_hash"],
                    "storage_ref": storage_ref,
                }
            ],
        }
    )
    add_timing("carry_forward.prepare_payload", step_started)
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
            %(reused_from_run_id)s, %(total_scopes)s, 0, 0,
            %(reused_scopes)s, %(metrics)s, %(manifest_id)s, now()
          )
          RETURNING run_id
        ),
        ins_manifest AS (
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
          RETURNING manifest_id
        )
        INSERT INTO okg.derived_output_slots (
          manifest_id, slot_name, storage_kind, media_type,
          artifact_type, renderer_ref, payload_hash, storage_ref,
          row_count, byte_count
        )
        SELECT ins_manifest.manifest_id, %(slot_name)s, 'relation',
               %(media_type)s, %(artifact_type)s, %(renderer_ref)s,
               %(payload_hash)s, %(storage_ref)s,
               %(row_count)s, %(byte_count)s
          FROM ins_manifest
        """,
        {
            **_required_hashes(definition),
            "run_id": run_id,
            "manifest_id": manifest_id,
            "definition_id": definition.definition_id,
            "target_generation_id": target_generation_id,
            "dependency_manifest_ids": [source_manifest_id],
            "reused_from_run_id": source["run_id"],
            "total_scopes": source["scope_count"],
            "reused_scopes": source["scope_count"],
            "metrics": Jsonb(run_metrics),
            "required_slots": [RELATION_SLOT_NAME],
            "manifest_hash": manifest_hash,
            "source_manifest_id": source_manifest_id,
            "slot_name": RELATION_SLOT_NAME,
            "media_type": source["media_type"],
            "artifact_type": source["artifact_type"],
            "renderer_ref": source["renderer_ref"],
            "payload_hash": source["payload_hash"],
            "storage_ref": Jsonb(storage_ref),
            "row_count": source["row_count"],
            "byte_count": source["byte_count"],
        },
    )
    add_timing("carry_forward.insert_metadata", step_started)
    timings["carry_forward.insert_metadata.coalesced_run_manifest_slot"] = 1.0

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
    return PublishedArtifact(
        run_id=run_id,
        manifest_id=manifest_id,
        schema_name=DERIVED_RELATION_SCHEMA,
        relation_name=physical_name,
        row_count=int(source["row_count"] or 0),
        manifest_hash=manifest_hash,
        storage_ref=storage_ref,
        payload_hash=source["payload_hash"],
        byte_count=int(source["byte_count"] or 0),
        carried_forward=True,
        reused_from_manifest_id=source_manifest_id,
        reused_scope_count=int(source["scope_count"] or 0),
        lineage_status=lineage_summary.status,
        lineage_envelope_count=lineage_summary.envelope_count,
        lineage_dependency_key_count=lineage_summary.dependency_key_count,
        lineage_dependency_kinds=lineage_summary.dependency_kinds,
        timings_ms=dict(timings),
    )


def relation_visible_rows_for_manifest(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> list[VisibleRelationRow]:
    """Return rows visible through a relation manifest reuse chain.

    Incremental relation manifests may physically store only dirty-scope rows.
    Clean scopes remain visible through ``reused_from_manifest_id``. A scope
    named in a newer manifest's ``dirty_scope_keys`` shadows older rows for the
    same scope, including the zero-row case where the scope disappeared.
    """

    with conn.cursor() as cur:
        return _relation_visible_rows_for_manifest_in_cursor(
            cur,
            definition_id=definition.definition_id,
            manifest_id=manifest_id,
            scope_keys=scope_keys,
        )


def relation_visible_rows_for_manifest_id(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> list[VisibleRelationRow]:
    with conn.cursor() as cur:
        return _relation_visible_rows_for_manifest_in_cursor(
            cur,
            definition_id=definition_id,
            manifest_id=manifest_id,
            scope_keys=scope_keys,
        )


def relation_visible_hash_rows_for_manifest(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> list[tuple[str, str, str, str | None]]:
    rows = relation_visible_rows_for_manifest(
        conn,
        definition,
        manifest_id=manifest_id,
        scope_keys=scope_keys,
    )
    return [
        (row.scope_key, row.row_key, row.row_hash, row.dependency_hash)
        for row in rows
    ]


def relation_visible_stats_for_manifest(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        return _relation_visible_stats_for_manifest_in_cursor(
            cur,
            definition_id=definition.definition_id,
            manifest_id=manifest_id,
            scope_keys=scope_keys,
        )


def relation_visible_stats_for_manifest_id(
    conn: psycopg.Connection[Any],
    *,
    definition_id: str,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    with conn.cursor() as cur:
        return _relation_visible_stats_for_manifest_in_cursor(
            cur,
            definition_id=definition_id,
            manifest_id=manifest_id,
            scope_keys=scope_keys,
        )


def _relation_visible_rows_for_manifest_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    definition_id: str,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> list[VisibleRelationRow]:
    source = _load_source_manifest_by_definition_id(
        cur,
        definition_id=definition_id,
        source_manifest_id=manifest_id,
    )
    physical_name = source["relation_name"]
    scope_filter = (
        None if scope_keys is None else sorted({str(scope) for scope in scope_keys})
    )
    cur.execute(
        sql.SQL(
            """
            WITH RECURSIVE lineage AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     0 AS depth
                FROM okg.derived_output_manifests m
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     lineage.depth + 1 AS depth
                FROM lineage
                JOIN okg.derived_output_manifests m
                  ON m.manifest_id = lineage.reused_from_manifest_id
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE lineage.depth < 64
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
            )
            SELECT r.scope_key, r.row_key, r.row_data, r.row_hash,
                   r.dependency_hash, pg_column_size(r.row_data),
                   r.target_generation_id
              FROM lineage l
              JOIN {}.{} r
                ON r.definition_id = %(definition_id)s
               AND r.target_generation_id = l.target_generation_id
             WHERE (%(scope_keys)s::text[] IS NULL
                    OR r.scope_key = ANY(%(scope_keys)s::text[]))
               AND NOT EXISTS (
                 SELECT 1
                   FROM lineage newer
                  WHERE newer.depth < l.depth
                    AND newer.dirty_scope_keys ? r.scope_key
               )
             ORDER BY r.scope_key, r.row_key
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "slot_name": RELATION_SLOT_NAME,
            "manifest_id": manifest_id,
            "definition_id": definition_id,
            "scope_keys": scope_filter,
        },
    )
    return [
        VisibleRelationRow(
            scope_key=str(row[0]),
            row_key=str(row[1]),
            row_data=dict(row[2] or {}),
            row_hash=str(row[3]),
            dependency_hash=None if row[4] is None else str(row[4]),
            byte_count=int(row[5] or 0),
            target_generation_id=int(row[6]),
        )
        for row in cur.fetchall()
    ]


def _relation_visible_stats_for_manifest_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    definition_id: str,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    source = _load_source_manifest_by_definition_id(
        cur,
        definition_id=definition_id,
        source_manifest_id=manifest_id,
    )
    physical_name = source["relation_name"]
    scope_filter = (
        None if scope_keys is None else sorted({str(scope) for scope in scope_keys})
    )
    cur.execute(
        sql.SQL(
            """
            WITH RECURSIVE lineage AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     0 AS depth
                FROM okg.derived_output_manifests m
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     lineage.depth + 1 AS depth
                FROM lineage
                JOIN okg.derived_output_manifests m
                  ON m.manifest_id = lineage.reused_from_manifest_id
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE lineage.depth < 64
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
            )
            SELECT r.scope_key, r.row_key, r.row_hash, r.dependency_hash,
                   pg_column_size(r.row_data)
              FROM lineage l
              JOIN {}.{} r
                ON r.definition_id = %(definition_id)s
               AND r.target_generation_id = l.target_generation_id
             WHERE (%(scope_keys)s::text[] IS NULL
                    OR r.scope_key = ANY(%(scope_keys)s::text[]))
               AND NOT EXISTS (
                 SELECT 1
                   FROM lineage newer
                  WHERE newer.depth < l.depth
                    AND newer.dirty_scope_keys ? r.scope_key
               )
             ORDER BY r.scope_key, r.row_key
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "slot_name": RELATION_SLOT_NAME,
            "manifest_id": manifest_id,
            "definition_id": definition_id,
            "scope_keys": scope_filter,
        },
    )
    rows = cur.fetchall()
    return {
        "row_hashes": [
            {
                "scope_key": str(row[0]),
                "row_key": str(row[1]),
                "row_hash": str(row[2]),
                "dependency_hash": None if row[3] is None else str(row[3]),
            }
            for row in rows
        ],
        "row_count": len(rows),
        "scope_count": len({str(row[0]) for row in rows}),
        "byte_count": sum(int(row[4] or 0) for row in rows),
    }


def _relation_visible_count_stats_for_manifest_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    definition_id: str,
    manifest_id: UUID,
    scope_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    scope_filter = (
        None if scope_keys is None else sorted({str(scope) for scope in scope_keys})
    )
    source = _load_source_manifest_by_definition_id(
        cur,
        definition_id=definition_id,
        source_manifest_id=manifest_id,
    )
    metadata_stats = _relation_count_stats_from_overlay_metadata(
        source,
        scope_keys=scope_filter,
    )
    if metadata_stats is not None:
        return metadata_stats
    physical_name = source["relation_name"]
    storage_ref = source.get("storage_ref")
    if not isinstance(storage_ref, Mapping):
        storage_ref = {}
    if scope_filter is not None:
        dirty_scope_keys = {
            str(scope)
            for scope in storage_ref.get("dirty_scope_keys", [])
            if isinstance(scope, str)
        }
        head_overlay_owns_requested_scopes = bool(dirty_scope_keys) and set(
            scope_filter
        ).issubset(dirty_scope_keys)
    else:
        head_overlay_owns_requested_scopes = False
    if "reused_from_manifest_id" not in storage_ref or head_overlay_owns_requested_scopes:
        stats = _relation_count_stats_for_generation_in_cursor(
            cur,
            physical_name=physical_name,
            definition_id=definition_id,
            target_generation_id=int(source["target_generation_id"]),
            scope_keys=scope_filter,
        )
        stats["fast_path"] = (
            "head_overlay_direct"
            if head_overlay_owns_requested_scopes
            else "full_manifest_direct"
        )
        return stats
    if scope_filter is not None:
        cur.execute(
            sql.SQL(
                """
                WITH RECURSIVE lineage AS (
                  SELECT m.manifest_id, m.reused_from_manifest_id,
                         m.target_generation_id,
                         COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                           AS dirty_scope_keys,
                         0 AS depth
                    FROM okg.derived_output_manifests m
                    JOIN okg.derived_output_slots s
                      ON s.manifest_id = m.manifest_id
                     AND s.slot_name = %(slot_name)s
                     AND s.storage_kind = 'relation'
                   WHERE m.manifest_id = %(manifest_id)s
                     AND m.definition_id = %(definition_id)s
                     AND m.status = 'completed'
                  UNION ALL
                  SELECT m.manifest_id, m.reused_from_manifest_id,
                         m.target_generation_id,
                         COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                           AS dirty_scope_keys,
                         lineage.depth + 1 AS depth
                    FROM lineage
                    JOIN okg.derived_output_manifests m
                      ON m.manifest_id = lineage.reused_from_manifest_id
                    JOIN okg.derived_output_slots s
                      ON s.manifest_id = m.manifest_id
                     AND s.slot_name = %(slot_name)s
                     AND s.storage_kind = 'relation'
                   WHERE lineage.depth < 64
                     AND m.definition_id = %(definition_id)s
                     AND m.status = 'completed'
                ),
                requested AS (
                  SELECT unnest(%(scope_keys)s::text[]) AS scope_key
                ),
                scope_owner AS (
                  SELECT DISTINCT ON (requested.scope_key)
                         requested.scope_key,
                         lineage.target_generation_id
                    FROM requested
                    JOIN lineage
                      ON lineage.dirty_scope_keys ? requested.scope_key
                      OR lineage.reused_from_manifest_id IS NULL
                   ORDER BY requested.scope_key, lineage.depth
                ),
                visible_rows AS (
                  SELECT r.scope_key, pg_column_size(r.row_data) AS row_bytes
                    FROM scope_owner owner
                    JOIN {}.{} r
                      ON r.definition_id = %(definition_id)s
                     AND r.target_generation_id = owner.target_generation_id
                     AND r.scope_key = owner.scope_key
                )
                SELECT count(*)::bigint,
                       count(DISTINCT scope_key)::bigint,
                       COALESCE(sum(row_bytes), 0)::bigint
                  FROM visible_rows
                """
            ).format(
                sql.Identifier(DERIVED_RELATION_SCHEMA),
                sql.Identifier(physical_name),
            ),
            {
                "slot_name": RELATION_SLOT_NAME,
                "manifest_id": manifest_id,
                "definition_id": definition_id,
                "scope_keys": scope_filter,
            },
        )
        row = cur.fetchone()
        return {
            "row_hashes": [],
            "row_count": int(row[0] or 0) if row is not None else 0,
            "scope_count": int(row[1] or 0) if row is not None else 0,
            "byte_count": int(row[2] or 0) if row is not None else 0,
        }
    cur.execute(
        sql.SQL(
            """
            WITH RECURSIVE lineage AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     0 AS depth
                FROM okg.derived_output_manifests m
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id,
                     COALESCE(s.storage_ref->'dirty_scope_keys', '[]'::jsonb)
                       AS dirty_scope_keys,
                     lineage.depth + 1 AS depth
                FROM lineage
                JOIN okg.derived_output_manifests m
                  ON m.manifest_id = lineage.reused_from_manifest_id
                JOIN okg.derived_output_slots s
                  ON s.manifest_id = m.manifest_id
                 AND s.slot_name = %(slot_name)s
                 AND s.storage_kind = 'relation'
               WHERE lineage.depth < 64
                 AND m.definition_id = %(definition_id)s
                 AND m.status = 'completed'
            ),
            visible_rows AS (
              SELECT r.scope_key, pg_column_size(r.row_data) AS row_bytes
                FROM lineage l
                JOIN {}.{} r
                  ON r.definition_id = %(definition_id)s
                 AND r.target_generation_id = l.target_generation_id
               WHERE (%(scope_keys)s::text[] IS NULL
                      OR r.scope_key = ANY(%(scope_keys)s::text[]))
                 AND NOT EXISTS (
                   SELECT 1
                     FROM lineage newer
                    WHERE newer.depth < l.depth
                      AND newer.dirty_scope_keys ? r.scope_key
                 )
            )
            SELECT count(*)::bigint,
                   count(DISTINCT scope_key)::bigint,
                   COALESCE(sum(row_bytes), 0)::bigint
              FROM visible_rows
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "slot_name": RELATION_SLOT_NAME,
            "manifest_id": manifest_id,
            "definition_id": definition_id,
            "scope_keys": scope_filter,
        },
    )
    row = cur.fetchone()
    return {
        "row_hashes": [],
        "row_count": int(row[0] or 0) if row is not None else 0,
        "scope_count": int(row[1] or 0) if row is not None else 0,
        "byte_count": int(row[2] or 0) if row is not None else 0,
    }


def _relation_count_stats_from_overlay_metadata(
    source: Mapping[str, Any],
    *,
    scope_keys: Sequence[str] | None,
) -> dict[str, Any] | None:
    if scope_keys is None:
        return None
    storage_ref = source.get("storage_ref")
    if not isinstance(storage_ref, Mapping):
        return None
    if "reused_from_manifest_id" not in storage_ref:
        return None
    dirty_scope_keys = sorted(
        {
            str(scope)
            for scope in storage_ref.get("dirty_scope_keys", [])
            if isinstance(scope, str)
        }
    )
    if not dirty_scope_keys or dirty_scope_keys != sorted(set(scope_keys)):
        return None
    required_keys = (
        "physical_dirty_row_count",
        "physical_dirty_scope_count",
        "physical_dirty_byte_count",
    )
    if any(key not in storage_ref for key in required_keys):
        return None
    return {
        "row_hashes": [],
        "row_count": int(storage_ref["physical_dirty_row_count"] or 0),
        "scope_count": int(storage_ref["physical_dirty_scope_count"] or 0),
        "byte_count": int(storage_ref["physical_dirty_byte_count"] or 0),
        "fast_path": "head_overlay_metadata",
    }


def _relation_count_stats_for_generation_in_cursor(
    cur: psycopg.Cursor[Any],
    *,
    physical_name: str,
    definition_id: str,
    target_generation_id: int,
    scope_keys: Sequence[str] | None = None,
) -> dict[str, Any]:
    scope_filter = (
        None if scope_keys is None else sorted({str(scope) for scope in scope_keys})
    )
    cur.execute(
        sql.SQL(
            """
            SELECT count(*)::bigint,
                   count(DISTINCT scope_key)::bigint,
                   COALESCE(sum(pg_column_size(row_data)), 0)::bigint
              FROM {}.{}
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
               AND (%(scope_keys)s::text[] IS NULL
                    OR scope_key = ANY(%(scope_keys)s::text[]))
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "definition_id": definition_id,
            "target_generation_id": target_generation_id,
            "scope_keys": scope_filter,
        },
    )
    row = cur.fetchone()
    return {
        "row_hashes": [],
        "row_count": int(row[0] or 0) if row is not None else 0,
        "scope_count": int(row[1] or 0) if row is not None else 0,
        "byte_count": int(row[2] or 0) if row is not None else 0,
    }


def refresh_relation_artifact(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    source_manifest_id: UUID,
    target_generation_id: int,
    dirty_scope_keys: Sequence[str],
    dirty_rows: Sequence[RelationRow],
    metrics: Mapping[str, Any] | None = None,
    consume_dirty_scopes: bool = True,
    lineage_envelopes: (
        Sequence["MaterializedLineageEnvelope"] | None
    ) = None,
    use_transaction: bool = True,
    definition_registered: bool = False,
    source_manifest: Mapping[str, Any] | None = None,
    previous_dirty_stats: Mapping[str, Any] | None = None,
) -> PublishedArtifact:
    """Publish an incremental refresh from a prior relation manifest.

    Dirty scopes are written into the target generation and clean scopes are
    inherited from ``source_manifest_id`` through manifest reuse. The result is
    a complete generation-visible relation, while physical writes stay
    proportional to the dirty rows.
    """

    timings_ms: dict[str, float] = {}
    total_started = time.perf_counter()

    def add_timing(label: str, started: float) -> None:
        timings_ms[label] = round(
            timings_ms.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    dirty_scopes = sorted(set(dirty_scope_keys))
    if not dirty_scopes:
        raise ValueError(
            "refresh_relation_artifact requires at least one dirty scope; "
            "use carry_forward_relation_artifact when nothing is dirty"
        )
    row_scopes = {row.scope_key for row in dirty_rows}
    unknown_scopes = row_scopes - set(dirty_scopes)
    if unknown_scopes:
        raise ValueError(
            "dirty_rows contains scope keys not listed in dirty_scope_keys: "
            f"{sorted(unknown_scopes)}"
        )

    physical_name = _physical_relation_name(definition)
    sorted_dirty_rows = sorted(
        dirty_rows,
        key=lambda row: (row.scope_key, row.row_key),
    )
    dirty_row_hashes = [
        {
            "scope_key": row.scope_key,
            "row_key": row.row_key,
            "row_hash": _row_hash(row),
            "dependency_hash": row.dependency_hash,
        }
        for row in sorted_dirty_rows
    ]
    add_timing("refresh.prepare_dirty_rows", step_started)
    step_started = time.perf_counter()
    prepared_lineage = _prepare_lineage_envelopes(lineage_envelopes)
    lineage_summary = _lineage_unavailable_summary()
    run_metrics = (
        _metrics_with_lineage(metrics, lineage_summary)
        if prepared_lineage is None
        else dict(metrics or {})
    )
    run_id = uuid4()
    manifest_id = uuid4()
    add_timing("refresh.prepare_lineage", step_started)

    transaction_started = time.perf_counter()
    transaction_context = conn.transaction() if use_transaction else nullcontext()
    with transaction_context:
        with conn.cursor() as cur:
            transaction_body_started = time.perf_counter()
            step_started = time.perf_counter()
            if definition_registered:
                timings_ms["refresh.ensure_definition_relation.skipped"] = 1.0
            else:
                _upsert_definition(cur, definition)
                _ensure_relation_registered(cur, definition, physical_name)
            add_timing("refresh.ensure_definition_relation", step_started)
            step_started = time.perf_counter()
            if source_manifest is None:
                source = _load_source_manifest(cur, definition, source_manifest_id)
            else:
                source = dict(source_manifest)
                timings_ms["refresh.load_source_manifest.reused"] = 1.0
            _ensure_definition_hashes_match(definition, source)
            source_generation_id = int(source["target_generation_id"])
            physical_name = source["relation_name"]
            add_timing("refresh.load_source_manifest", step_started)

            step_started = time.perf_counter()
            if previous_dirty_stats is None:
                previous_dirty_stats = (
                    _relation_visible_count_stats_for_manifest_in_cursor(
                        cur,
                        definition_id=definition.definition_id,
                        manifest_id=source_manifest_id,
                        scope_keys=dirty_scopes,
                    )
                )
                stats_fast_path = previous_dirty_stats.get("fast_path")
                if isinstance(stats_fast_path, str) and stats_fast_path:
                    timings_ms[f"refresh.measure_reused_clean_rows.{stats_fast_path}"] = (
                        1.0
                    )
            else:
                previous_dirty_stats = dict(previous_dirty_stats)
                timings_ms["refresh.measure_reused_clean_rows.reused"] = 1.0
            source_row_count = int(source["row_count"] or 0)
            source_scope_count = int(source["scope_count"] or 0)
            source_byte_count = int(source["byte_count"] or 0)
            copied_rows = max(
                0,
                source_row_count - int(previous_dirty_stats["row_count"] or 0),
            )
            copied_scope_count = max(
                0,
                source_scope_count - int(previous_dirty_stats["scope_count"] or 0),
            )
            copied_byte_count = max(
                0,
                source_byte_count - int(previous_dirty_stats["byte_count"] or 0),
            )
            add_timing("refresh.measure_reused_clean_rows", step_started)

            step_started = time.perf_counter()
            dirty_insert_stats = _insert_relation_rows(
                cur,
                physical_name=physical_name,
                definition_id=definition.definition_id,
                target_generation_id=target_generation_id,
                rows=sorted_dirty_rows,
                row_hashes=dirty_row_hashes,
            )
            add_timing("refresh.insert_dirty_rows", step_started)

            step_started = time.perf_counter()
            dirty_row_count = int(dirty_insert_stats["row_count"])
            dirty_scope_count_actual = int(dirty_insert_stats["scope_count"])
            total_scope_count = copied_scope_count + len(dirty_scopes)
            relation_stats = {
                "row_hashes": [dict(row_hash) for row_hash in dirty_row_hashes],
                "row_count": copied_rows + dirty_row_count,
                "scope_count": copied_scope_count + dirty_scope_count_actual,
                "byte_count": copied_byte_count + int(dirty_insert_stats["byte_count"]),
            }
            add_timing("refresh.incremental_relation_stats", step_started)
            step_started = time.perf_counter()
            payload_hash = stable_hash(
                {
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "relation": physical_name,
                    "source_manifest_id": str(source_manifest_id),
                    "source_payload_hash": source["payload_hash"],
                    "dirty_scope_keys": dirty_scopes,
                    "dirty_row_hashes": relation_stats["row_hashes"],
                }
            )
            storage_ref = {
                "schema": DERIVED_RELATION_SCHEMA,
                "relation": physical_name,
                "logical_relation_id": definition.logical_relation_id,
                "target_generation_id": target_generation_id,
                "source_generation_id": source_generation_id,
                "reused_from_manifest_id": str(source_manifest_id),
                "dirty_scope_keys": dirty_scopes,
                "dirty_scope_count": len(dirty_scopes),
                "copied_scope_count": copied_scope_count,
                "copied_row_count": copied_rows,
                "copied_byte_count": copied_byte_count,
                "physical_dirty_row_count": dirty_row_count,
                "physical_dirty_scope_count": dirty_scope_count_actual,
                "physical_dirty_byte_count": int(dirty_insert_stats["byte_count"]),
                "physical_row_storage": "dirty_overlay",
                "total_scope_count": total_scope_count,
                "scope_count": relation_stats["scope_count"],
                "row_count": relation_stats["row_count"],
            }
            manifest_hash = stable_hash(
                {
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "required_slots": [RELATION_SLOT_NAME],
                    "reused_from_manifest_id": str(source_manifest_id),
                    "slots": [
                        {
                            "slot_name": RELATION_SLOT_NAME,
                            "storage_kind": "relation",
                            "payload_hash": payload_hash,
                            "storage_ref": storage_ref,
                        }
                    ],
                }
            )
            add_timing("refresh.hash_manifest_payload", step_started)
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
                    'completed',
                    'dirty', %(compute_hash)s,
                    %(output_schema_hash)s, %(renderer_hash)s,
                    %(dirty_rule_hash)s, %(dependency_manifest_ids)s,
                    %(reused_from_run_id)s, %(total_scopes)s,
                    %(dirty_scopes)s, %(dirty_rows)s, %(reused_scopes)s,
                    %(metrics)s, %(manifest_id)s, now()
                  )
                  RETURNING run_id
                ),
                ins_manifest AS (
                  INSERT INTO okg.derived_output_manifests (
                    manifest_id, run_id, definition_id,
                    target_generation_id, status, required_slots,
                    manifest_hash, reused_from_manifest_id, promoted_at
                  )
                  SELECT %(manifest_id)s, ins_run.run_id, %(definition_id)s,
                         %(target_generation_id)s, 'completed',
                         %(required_slots)s, %(manifest_hash)s,
                         %(source_manifest_id)s, now()
                    FROM ins_run
                  RETURNING manifest_id
                )
                INSERT INTO okg.derived_output_slots (
                  manifest_id, slot_name, storage_kind, media_type,
                  artifact_type, renderer_ref, payload_hash, storage_ref,
                  row_count, byte_count
                )
                SELECT ins_manifest.manifest_id, %(slot_name)s, 'relation',
                       %(media_type)s, %(artifact_type)s, %(renderer_ref)s,
                       %(payload_hash)s, %(storage_ref)s,
                       %(row_count)s, %(byte_count)s
                  FROM ins_manifest
                """,
                {
                    **_required_hashes(definition),
                    "run_id": run_id,
                    "manifest_id": manifest_id,
                    "definition_id": definition.definition_id,
                    "target_generation_id": target_generation_id,
                    "dependency_manifest_ids": [source_manifest_id],
                    "reused_from_run_id": source["run_id"],
                    "total_scopes": total_scope_count,
                    "dirty_scopes": len(dirty_scopes),
                    "dirty_rows": len(sorted_dirty_rows),
                    "reused_scopes": copied_scope_count,
                    "metrics": Jsonb(run_metrics),
                    "required_slots": [RELATION_SLOT_NAME],
                    "manifest_hash": manifest_hash,
                    "source_manifest_id": source_manifest_id,
                    "slot_name": RELATION_SLOT_NAME,
                    "media_type": RELATION_MEDIA_TYPE,
                    "artifact_type": RELATION_ARTIFACT_TYPE,
                    "renderer_ref": definition.renderer_ref,
                    "payload_hash": payload_hash,
                    "storage_ref": Jsonb(storage_ref),
                    "row_count": relation_stats["row_count"],
                    "byte_count": relation_stats["byte_count"],
                },
            )
            add_timing("refresh.insert_metadata", step_started)
            timings_ms["refresh.insert_metadata.coalesced_run_manifest_slot"] = 1.0

            step_started = time.perf_counter()
            lineage_summary = _publish_lineage_for_manifest(
                conn,
                manifest_id=manifest_id,
                envelopes=prepared_lineage,
            )
            add_timing("refresh.publish_lineage", step_started)
            if prepared_lineage is not None:
                step_started = time.perf_counter()
                _set_run_lineage_metrics(
                    cur,
                    run_id=run_id,
                    lineage=lineage_summary,
                )
                add_timing("refresh.set_lineage_metrics", step_started)
            if consume_dirty_scopes:
                step_started = time.perf_counter()
                cur.execute(
                    """
                    UPDATE okg.derived_dirty_scopes
                       SET consumed_run_id = %(run_id)s,
                           consumed_at = now()
                     WHERE definition_id = %(definition_id)s
                       AND target_generation_id <= %(target_generation_id)s
                       AND scope_key = ANY(%(dirty_scopes)s)
                       AND consumed_run_id IS NULL
                    """,
                    {
                        "run_id": run_id,
                        "definition_id": definition.definition_id,
                        "target_generation_id": target_generation_id,
                        "dirty_scopes": dirty_scopes,
                    },
                )
                add_timing("refresh.consume_dirty_scopes", step_started)
            else:
                timings_ms[
                    "refresh.consume_dirty_scopes.skipped_direct_delta"
                ] = 1.0
            add_timing("refresh.transaction.body", transaction_body_started)
    if not use_transaction:
        timings_ms["refresh.transaction.external"] = 1.0
    add_timing("refresh.transaction", transaction_started)
    add_timing("refresh.total", total_started)

    return PublishedArtifact(
        run_id=run_id,
        manifest_id=manifest_id,
        schema_name=DERIVED_RELATION_SCHEMA,
        relation_name=physical_name,
        row_count=int(relation_stats["row_count"]),
        manifest_hash=manifest_hash,
        storage_ref=storage_ref,
        payload_hash=payload_hash,
        byte_count=int(relation_stats["byte_count"]),
        reused_from_manifest_id=source_manifest_id,
        dirty_scope_count=len(dirty_scopes),
        reused_scope_count=copied_scope_count,
        lineage_status=lineage_summary.status,
        lineage_envelope_count=lineage_summary.envelope_count,
        lineage_dependency_key_count=lineage_summary.dependency_key_count,
        lineage_dependency_kinds=lineage_summary.dependency_kinds,
        timings_ms=timings_ms,
    )


def relation_dirty_rows_match_manifest(
    conn: psycopg.Connection[Any],
    definition: RelationArtifactDefinition,
    *,
    source_manifest_id: UUID,
    dirty_scope_keys: Sequence[str],
    dirty_rows: Sequence[RelationRow],
    timing_sink: dict[str, float] | None = None,
) -> RelationDirtyRowsMatchResult:
    """Return true when recomputed dirty-scope rows match a source manifest."""

    def add_timing(label: str, started: float) -> None:
        if timing_sink is None:
            return
        timing_sink[label] = round(
            timing_sink.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    dirty_scopes = sorted(set(dirty_scope_keys))
    if not dirty_scopes:
        add_timing("dirty_rows_match.prepare_current_rows", step_started)
        return RelationDirtyRowsMatchResult(matches=True)
    row_scopes = {row.scope_key for row in dirty_rows}
    unknown_scopes = row_scopes - set(dirty_scopes)
    if unknown_scopes:
        raise ValueError(
            "dirty_rows contains scope keys not listed in dirty_scope_keys: "
            f"{sorted(unknown_scopes)}"
        )

    current_rows = [
        (
            row.scope_key,
            row.row_key,
            _row_hash(row),
            row.dependency_hash,
        )
        for row in sorted(dirty_rows, key=lambda item: (item.scope_key, item.row_key))
    ]
    if len(set(current_rows)) != len(current_rows):
        add_timing("dirty_rows_match.prepare_current_rows", step_started)
        return RelationDirtyRowsMatchResult(matches=False)
    add_timing("dirty_rows_match.prepare_current_rows", step_started)

    with conn.cursor() as cur:
        step_started = time.perf_counter()
        source = _load_source_manifest(cur, definition, source_manifest_id)
        _ensure_definition_hashes_match(definition, source)
        add_timing("dirty_rows_match.load_source_manifest", step_started)
        step_started = time.perf_counter()
        previous_stats = _relation_visible_stats_for_manifest_in_cursor(
            cur,
            definition_id=definition.definition_id,
            manifest_id=source_manifest_id,
            scope_keys=dirty_scopes,
        )
        previous_rows = [
            (
                row["scope_key"],
                row["row_key"],
                row["row_hash"],
                row["dependency_hash"],
            )
            for row in previous_stats["row_hashes"]
        ]
        add_timing("dirty_rows_match.query_previous_rows", step_started)

    step_started = time.perf_counter()
    matches = set(current_rows) == set(previous_rows)
    add_timing("dirty_rows_match.compare_rows", step_started)
    return RelationDirtyRowsMatchResult(
        matches=matches,
        source_manifest=source,
        previous_dirty_stats=previous_stats,
    )


def _ensure_relation_registered(
    cur: psycopg.Cursor[Any],
    definition: RelationArtifactDefinition,
    physical_name: str,
) -> None:
    ddl_hash = _ddl_hash(physical_name, definition)
    if _relation_registration_current(
        cur,
        definition=definition,
        physical_name=physical_name,
        ddl_hash=ddl_hash,
    ):
        return

    _upsert_definition(cur, definition)
    cur.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
            sql.Identifier(DERIVED_RELATION_SCHEMA)
        )
    )
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
              definition_id        TEXT NOT NULL,
              target_generation_id BIGINT NOT NULL,
              scope_key            TEXT NOT NULL,
              row_key              TEXT NOT NULL,
              row_data             JSONB NOT NULL,
              row_hash             TEXT NOT NULL,
              dependency_hash      TEXT,
              created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
              PRIMARY KEY (
                definition_id, target_generation_id, scope_key, row_key
              )
            )
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        )
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS {}
              ON {}.{} (definition_id, target_generation_id, scope_key)
            """
        ).format(
            sql.Identifier(f"{physical_name}_scope_idx"),
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        )
    )
    cur.execute(
        sql.SQL("GRANT SELECT, INSERT, UPDATE ON {}.{} TO app_rw").format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        )
    )
    cur.execute(
        """
        INSERT INTO okg.derived_relation_registry (
          definition_id, logical_relation_id, schema_name, physical_name,
          output_schema_hash, ddl_hash, status
        ) VALUES (
          %(definition_id)s, %(logical_relation_id)s, %(schema_name)s,
          %(physical_name)s, %(output_schema_hash)s, %(ddl_hash)s,
          'active'
        )
        ON CONFLICT (
          definition_id, logical_relation_id, output_schema_hash
        ) DO UPDATE SET
          schema_name = EXCLUDED.schema_name,
          physical_name = EXCLUDED.physical_name,
          ddl_hash = EXCLUDED.ddl_hash,
          status = 'active',
          retired_at = NULL
        WHERE (
          okg.derived_relation_registry.schema_name,
          okg.derived_relation_registry.physical_name,
          okg.derived_relation_registry.ddl_hash,
          okg.derived_relation_registry.status,
          okg.derived_relation_registry.retired_at
        ) IS DISTINCT FROM (
          EXCLUDED.schema_name,
          EXCLUDED.physical_name,
          EXCLUDED.ddl_hash,
          'active',
          NULL
        )
        """,
        {
            "definition_id": definition.definition_id,
            "logical_relation_id": definition.logical_relation_id,
            "schema_name": DERIVED_RELATION_SCHEMA,
            "physical_name": physical_name,
            "output_schema_hash": definition.output_schema_hash,
            "ddl_hash": ddl_hash,
        },
    )


def _relation_registration_current(
    cur: psycopg.Cursor[Any],
    *,
    definition: RelationArtifactDefinition,
    physical_name: str,
    ddl_hash: str,
) -> bool:
    cur.execute(
        """
        SELECT 1
          FROM okg.derived_artifact_definitions d
          JOIN okg.derived_relation_registry r
            ON r.definition_id = d.definition_id
         WHERE d.definition_id = %(definition_id)s
           AND d.artifact_kind = %(artifact_kind)s
           AND d.deployment_name = %(deployment_name)s
           AND d.logical_key = %(logical_key)s
           AND d.compute_hash = %(compute_hash)s
           AND d.output_schema_hash = %(output_schema_hash)s
           AND d.renderer_hash = %(renderer_hash)s
           AND d.dirty_rule_hash = %(dirty_rule_hash)s
           AND d.renderer_ref = %(renderer_ref)s
           AND d.output_contract = %(output_contract)s
           AND d.dirty_rule = %(dirty_rule)s
           AND r.logical_relation_id = %(logical_relation_id)s
           AND r.schema_name = %(schema_name)s
           AND r.physical_name = %(physical_name)s
           AND r.output_schema_hash = %(output_schema_hash)s
           AND r.ddl_hash = %(ddl_hash)s
           AND r.status = 'active'
           AND r.retired_at IS NULL
           AND to_regclass(%(qualified_relation_name)s) IS NOT NULL
         LIMIT 1
        """,
        {
            "definition_id": definition.definition_id,
            "artifact_kind": definition.artifact_kind,
            "deployment_name": definition.deployment_name,
            "logical_key": definition.logical_relation_id,
            "compute_hash": definition.compute_hash,
            "output_schema_hash": definition.output_schema_hash,
            "renderer_hash": definition.renderer_hash,
            "dirty_rule_hash": definition.dirty_rule_hash,
            "renderer_ref": definition.renderer_ref,
            "output_contract": Jsonb(dict(definition.output_contract)),
            "dirty_rule": Jsonb(dict(definition.dirty_rule)),
            "logical_relation_id": definition.logical_relation_id,
            "schema_name": DERIVED_RELATION_SCHEMA,
            "physical_name": physical_name,
            "ddl_hash": ddl_hash,
            "qualified_relation_name": (
                f"{DERIVED_RELATION_SCHEMA}.{physical_name}"
            ),
        },
    )
    return cur.fetchone() is not None


def _relation_stats(
    cur: psycopg.Cursor[Any],
    physical_name: str,
    definition_id: str,
    target_generation_id: int,
    *,
    timing_sink: dict[str, float] | None = None,
) -> dict[str, Any]:
    def add_timing(label: str, started: float) -> None:
        if timing_sink is None:
            return
        timing_sink[label] = round(
            timing_sink.get(label, 0.0)
            + ((time.perf_counter() - started) * 1000.0),
            3,
        )

    step_started = time.perf_counter()
    cur.execute(
        sql.SQL(
            """
            SELECT scope_key, row_key, row_hash, dependency_hash
              FROM {}.{}
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
             ORDER BY scope_key, row_key
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "definition_id": definition_id,
            "target_generation_id": target_generation_id,
        },
    )
    row_hashes = [
        {
            "scope_key": row[0],
            "row_key": row[1],
            "row_hash": row[2],
            "dependency_hash": row[3],
        }
        for row in cur.fetchall()
    ]
    add_timing("relation_stats.fetch_row_hashes", step_started)
    step_started = time.perf_counter()
    cur.execute(
        sql.SQL(
            """
            SELECT count(*), count(DISTINCT scope_key),
                   COALESCE(sum(pg_column_size(row_data)), 0)
              FROM {}.{}
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
            """
        ).format(
            sql.Identifier(DERIVED_RELATION_SCHEMA),
            sql.Identifier(physical_name),
        ),
        {
            "definition_id": definition_id,
            "target_generation_id": target_generation_id,
        },
    )
    row_count, scope_count, byte_count = cur.fetchone()
    add_timing("relation_stats.aggregate", step_started)
    return {
        "row_hashes": row_hashes,
        "row_count": int(row_count or 0),
        "scope_count": int(scope_count or 0),
        "byte_count": int(byte_count or 0),
    }


def _load_source_manifest(
    cur: psycopg.Cursor[Any],
    definition: RelationArtifactDefinition,
    source_manifest_id: UUID,
) -> dict[str, Any]:
    return _load_source_manifest_by_definition_id(
        cur,
        definition_id=definition.definition_id,
        source_manifest_id=source_manifest_id,
    )


def _load_source_manifest_by_definition_id(
    cur: psycopg.Cursor[Any],
    *,
    definition_id: str,
    source_manifest_id: UUID,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT
          m.run_id,
          m.target_generation_id,
          r.compute_hash,
          r.output_schema_hash,
          r.renderer_hash,
          r.dirty_rule_hash,
          s.media_type,
          s.artifact_type,
          s.renderer_ref,
          s.payload_hash,
          s.storage_ref,
          s.row_count,
          s.byte_count
        FROM okg.derived_output_manifests m
        JOIN okg.derived_artifact_runs r
          ON r.run_id = m.run_id
        JOIN okg.derived_output_slots s
          ON s.manifest_id = m.manifest_id
         AND s.slot_name = %(slot_name)s
         AND s.storage_kind = 'relation'
        WHERE m.manifest_id = %(manifest_id)s
          AND m.definition_id = %(definition_id)s
          AND m.status = 'completed'
        """,
        {
            "slot_name": RELATION_SLOT_NAME,
            "manifest_id": source_manifest_id,
            "definition_id": definition_id,
        },
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            "source_manifest_id must reference a completed relation "
            "manifest for the same definition"
        )

    (
        run_id,
        target_generation_id,
        compute_hash,
        output_schema_hash,
        renderer_hash,
        dirty_rule_hash,
        media_type,
        artifact_type,
        renderer_ref,
        payload_hash,
        storage_ref,
        row_count,
        byte_count,
    ) = row
    if not isinstance(storage_ref, dict):
        raise ValueError("source relation slot has non-object storage_ref")
    relation_name = storage_ref.get("relation")
    if not isinstance(relation_name, str) or not relation_name:
        raise ValueError("source relation slot is missing storage_ref.relation")

    return {
        "run_id": run_id,
        "target_generation_id": target_generation_id,
        "compute_hash": compute_hash,
        "output_schema_hash": output_schema_hash,
        "renderer_hash": renderer_hash,
        "dirty_rule_hash": dirty_rule_hash,
        "media_type": media_type,
        "artifact_type": artifact_type,
        "renderer_ref": renderer_ref,
        "payload_hash": payload_hash,
        "storage_ref": storage_ref,
        "relation_name": relation_name,
        "row_count": row_count,
        "byte_count": byte_count,
        "scope_count": storage_ref.get("scope_count", 0),
    }


def _ensure_definition_hashes_match(
    definition: RelationArtifactDefinition,
    source: Mapping[str, Any],
) -> None:
    for key, expected in _required_hashes(definition).items():
        if source.get(key) != expected:
            raise ValueError(
                "cannot carry forward manifest across definition hash "
                f"change: {key} differs"
            )
