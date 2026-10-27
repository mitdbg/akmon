"""Canonical read path for native V1 view artifacts.

View publication writes the durable state to derived-artifact
definitions, completed output manifests, and typed output slots. This
module is the narrow read-side companion: callers resolve view output
from those tables rather than from compatibility ``view`` nodes.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Mapping
from uuid import UUID

import psycopg
from psycopg import sql as pg_sql

from .relations import (
    relation_visible_rows_for_manifest_id,
    relation_visible_stats_for_manifest_id,
)


class ViewArtifactReadError(Exception):
    """Base class for canonical view read failures."""


class ViewArtifactNotFound(ViewArtifactReadError):
    """Raised when no completed view manifest satisfies the read."""


class ViewArtifactAmbiguous(ViewArtifactReadError):
    """Raised when a logical view id maps to multiple definitions."""

    def __init__(
        self,
        *,
        deployment_name: str,
        view_id: str,
        definition_ids: list[str],
    ) -> None:
        self.deployment_name = deployment_name
        self.view_id = view_id
        self.definition_ids = definition_ids
        joined = ", ".join(definition_ids)
        super().__init__(
            "view id resolves to multiple definitions: "
            f"deployment={deployment_name!r} view_id={view_id!r} "
            f"definition_ids=[{joined}]"
        )


class ViewSlotNotFound(ViewArtifactReadError):
    """Raised when a completed manifest has no requested slot."""


@dataclass(frozen=True)
class ViewArtifactRead:
    """A completed native view output-set manifest."""

    definition_id: str
    deployment_name: str
    view_id: str
    manifest_id: UUID
    run_id: UUID
    target_generation_id: int
    manifest_hash: str
    required_slots: tuple[str, ...]
    reused_from_manifest_id: UUID | None
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    renderer_ref: str
    output_contract: Mapping[str, Any]
    manifest_metadata: Mapping[str, Any]
    run_status: str
    promoted_at: datetime | None
    published_at: datetime | None

    def to_json(self) -> dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "deployment_name": self.deployment_name,
            "view_id": self.view_id,
            "manifest_id": str(self.manifest_id),
            "run_id": str(self.run_id),
            "target_generation_id": self.target_generation_id,
            "manifest_hash": self.manifest_hash,
            "required_slots": list(self.required_slots),
            "reused_from_manifest_id": (
                str(self.reused_from_manifest_id)
                if self.reused_from_manifest_id is not None
                else None
            ),
            "compute_hash": self.compute_hash,
            "output_schema_hash": self.output_schema_hash,
            "renderer_hash": self.renderer_hash,
            "dirty_rule_hash": self.dirty_rule_hash,
            "renderer_ref": self.renderer_ref,
            "output_contract": dict(self.output_contract),
            "manifest_metadata": dict(self.manifest_metadata),
            "run_status": self.run_status,
            "promoted_at": (
                self.promoted_at.isoformat()
                if self.promoted_at is not None
                else None
            ),
            "published_at": (
                self.published_at.isoformat()
                if self.published_at is not None
                else None
            ),
        }


@dataclass(frozen=True)
class ViewArtifactSummary:
    """List entry for one completed native view manifest."""

    definition_id: str
    deployment_name: str
    view_id: str
    view_name: str
    manifest_id: UUID
    target_generation_id: int
    manifest_hash: str
    required_slots: tuple[str, ...]
    slot_names: tuple[str, ...]
    run_status: str
    freshness: str
    reused_from_manifest_id: UUID | None
    promoted_at: datetime | None
    published_at: datetime | None
    latest_published_generation_id: int
    # True when the deployment's current views_registry.yaml no longer lists
    # this view; False when it does; None when the registry cannot be read
    # (no deployment directory, no registry file, or one that does not parse).
    retired: bool | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "definition_id": self.definition_id,
            "deployment_name": self.deployment_name,
            "view_id": self.view_id,
            "view_name": self.view_name,
            "retired": self.retired,
            "manifest_id": str(self.manifest_id),
            "target_generation_id": self.target_generation_id,
            "manifest_hash": self.manifest_hash,
            "required_slots": list(self.required_slots),
            "slot_names": list(self.slot_names),
            "run_status": self.run_status,
            "freshness": self.freshness,
            "reused_from_manifest_id": (
                str(self.reused_from_manifest_id)
                if self.reused_from_manifest_id is not None
                else None
            ),
            "promoted_at": (
                self.promoted_at.isoformat()
                if self.promoted_at is not None
                else None
            ),
            "published_at": (
                self.published_at.isoformat()
                if self.published_at is not None
                else None
            ),
            "latest_published_generation_id": (
                self.latest_published_generation_id
            ),
        }


@dataclass(frozen=True)
class ViewOutputSlotRead:
    """One typed output slot in a completed view manifest."""

    slot_id: UUID
    manifest_id: UUID
    slot_name: str
    storage_kind: str
    media_type: str
    artifact_type: str
    renderer_ref: str
    payload_hash: str
    storage_ref: Mapping[str, Any]
    inline_payload: Mapping[str, Any] | None
    row_count: int | None
    byte_count: int | None
    metadata: Mapping[str, Any]

    @property
    def inline_kind(self) -> str | None:
        if self.inline_payload is None:
            return None
        kind = self.inline_payload.get("kind")
        return str(kind) if kind is not None else None

    def payload_value(self) -> Any:
        """Return the inline payload value, or the storage reference.

        Inline text slots return a string. Inline JSON slots return the
        decoded JSON value. Non-inline slots return their storage
        reference because the payload lives outside the slot row.
        """

        if self.inline_payload is None:
            return dict(self.storage_ref)
        if self.inline_payload.get("kind") == "text":
            return self.inline_payload.get("text", "")
        if self.inline_payload.get("kind") == "json":
            return self.inline_payload.get("value")
        return dict(self.inline_payload)

    def to_json(self) -> dict[str, Any]:
        return {
            "slot_id": str(self.slot_id),
            "manifest_id": str(self.manifest_id),
            "slot_name": self.slot_name,
            "storage_kind": self.storage_kind,
            "media_type": self.media_type,
            "artifact_type": self.artifact_type,
            "renderer_ref": self.renderer_ref,
            "payload_hash": self.payload_hash,
            "storage_ref": dict(self.storage_ref),
            "inline_payload": (
                dict(self.inline_payload)
                if self.inline_payload is not None
                else None
            ),
            "payload": self.payload_value(),
            "row_count": self.row_count,
            "byte_count": self.byte_count,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ViewSlotRead:
    """A canonical view read: completed manifest plus requested slot."""

    artifact: ViewArtifactRead
    slot: ViewOutputSlotRead

    def to_json(self) -> dict[str, Any]:
        return {
            "artifact": self.artifact.to_json(),
            "slot": self.slot.to_json(),
        }


def inspect_view_slot(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
    slot_name: str,
    generation_id: int | None = None,
    definition_id: str | None = None,
    preview_limit: int = 0,
) -> dict[str, Any]:
    """Return manifest, slot, and backing-storage evidence for a view slot."""

    read = read_view_slot(
        conn,
        deployment_name=deployment_name,
        view_id=view_id,
        slot_name=slot_name,
        generation_id=generation_id,
        definition_id=definition_id,
    )
    relation = None
    if read.slot.storage_kind == "relation":
        relation = _inspect_relation_slot(
            conn,
            slot=read.slot,
            view_definition_id=read.artifact.definition_id,
            view_target_generation_id=read.artifact.target_generation_id,
            preview_limit=preview_limit,
        )
    return {
        **read.to_json(),
        "relation": relation,
    }


def get_latest_view_artifact(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
    definition_id: str | None = None,
) -> ViewArtifactRead:
    """Return the latest completed manifest on a published generation."""

    with conn.cursor() as cur:
        resolved_definition_id = _resolve_definition_id(
            cur,
            deployment_name=deployment_name,
            view_id=view_id,
            definition_id=definition_id,
        )
        artifact = _fetch_view_artifact(
            cur,
            deployment_name=deployment_name,
            definition_id=resolved_definition_id,
            generation_id=None,
        )
    if artifact is None:
        raise ViewArtifactNotFound(
            "no completed published view artifact found for "
            f"deployment={deployment_name!r} view_id={view_id!r}"
        )
    return artifact


def get_view_artifact_at_generation(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
    generation_id: int,
    definition_id: str | None = None,
) -> ViewArtifactRead:
    """Return the completed manifest exactly at ``generation_id``."""

    with conn.cursor() as cur:
        resolved_definition_id = _resolve_definition_id(
            cur,
            deployment_name=deployment_name,
            view_id=view_id,
            definition_id=definition_id,
        )
        artifact = _fetch_view_artifact(
            cur,
            deployment_name=deployment_name,
            definition_id=resolved_definition_id,
            generation_id=generation_id,
        )
    if artifact is None:
        raise ViewArtifactNotFound(
            "no completed published view artifact found for "
            f"deployment={deployment_name!r} view_id={view_id!r} "
            f"generation_id={generation_id}"
        )
    return artifact


def list_view_slots(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: UUID,
) -> list[ViewOutputSlotRead]:
    """Return all slots for one completed output-set manifest."""

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH RECURSIVE manifest_chain AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id, 0 AS depth
                FROM okg.derived_output_manifests m
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT parent.manifest_id, parent.reused_from_manifest_id,
                     parent.target_generation_id, manifest_chain.depth + 1
                FROM okg.derived_output_manifests parent
                JOIN manifest_chain
                  ON parent.manifest_id =
                     manifest_chain.reused_from_manifest_id
               WHERE parent.status = 'completed'
                 AND manifest_chain.depth < 64
            ),
            requested AS (
              SELECT manifest_id, reused_from_manifest_id, target_generation_id
                FROM manifest_chain
               WHERE depth = 0
            ),
            immediate_source AS (
              SELECT chain.manifest_id, chain.target_generation_id
                FROM manifest_chain chain
                JOIN requested
                  ON requested.reused_from_manifest_id = chain.manifest_id
               LIMIT 1
            ),
            slot_source AS (
              SELECT chain.manifest_id, chain.target_generation_id
                FROM manifest_chain chain
               WHERE EXISTS (
                       SELECT 1
                         FROM okg.derived_output_slots probe
                        WHERE probe.manifest_id = chain.manifest_id
                     )
               ORDER BY chain.depth
               LIMIT 1
            )
            SELECT s.slot_id, requested.manifest_id, s.slot_name,
                   s.storage_kind, s.media_type, s.artifact_type,
                   s.renderer_ref, s.payload_hash,
                   CASE
                     WHEN slot_source.manifest_id = requested.manifest_id
                     THEN s.storage_ref
                     ELSE s.storage_ref || jsonb_build_object(
                       'target_generation_id',
                       requested.target_generation_id,
                       'source_generation_id',
                       COALESCE(
                         immediate_source.target_generation_id,
                         slot_source.target_generation_id
                       ),
                       'reused_from_manifest_id',
                       requested.reused_from_manifest_id::text,
                       'source_manifest_id',
                       slot_source.manifest_id::text
                     )
                   END AS storage_ref,
                   s.inline_payload, s.row_count, s.byte_count,
                   s.metadata
              FROM slot_source
              JOIN requested ON true
              LEFT JOIN immediate_source ON true
              JOIN okg.derived_output_slots s
                ON s.manifest_id = slot_source.manifest_id
             ORDER BY s.slot_name
            """,
            {"manifest_id": manifest_id},
        )
        return [_row_to_slot(row) for row in cur.fetchall()]


_REGISTRY_FROM_DEPLOYMENT: Any = object()
_REGISTRY_IDS_CACHE: dict[tuple[str, int, int], frozenset[str]] = {}


def current_registry_view_ids(deployment_name: str) -> frozenset[str] | None:
    """View ids the deployment's current views_registry.yaml lists.

    None when that cannot be known: the deployment directory is not found,
    it has no views_registry.yaml, or the file does not parse. A caller must
    not read None as "no views" -- it labels nothing retired.
    """

    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.errors import SubstrateError
    from okg.substrate.views.registry import load_views_registry

    deployment_dir = find_deployment_dir(deployment_name)
    if deployment_dir is None:
        return None
    registry_path = deployment_dir / "views_registry.yaml"
    try:
        stat = registry_path.stat()
    except OSError:
        return None
    # The parse is cached per file version, so a list call does not re-read
    # and re-parse the YAML every time.
    key = (str(registry_path), stat.st_mtime_ns, stat.st_size)
    if key in _REGISTRY_IDS_CACHE:
        return _REGISTRY_IDS_CACHE[key]
    try:
        entries = load_views_registry(deployment_dir)
    except SubstrateError:
        return None
    ids = frozenset(entry.view_def_id for entry in entries)
    if len(_REGISTRY_IDS_CACHE) >= 64:
        _REGISTRY_IDS_CACHE.clear()
    _REGISTRY_IDS_CACHE[key] = ids
    return ids


def list_view_artifacts(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    generation_id: int | None = None,
    registry_view_ids: Any = _REGISTRY_FROM_DEPLOYMENT,
) -> list[ViewArtifactSummary]:
    """List completed native view manifests for a deployment.

    Each entry says whether the view is ``retired``: absent from the current
    views_registry.yaml (read from the deployment unless ``registry_view_ids``
    is passed; None there means "unknown"). No manifest is removed.

    ``generation_id=None`` returns the latest completed published
    manifest per definition and marks entries stale when the manifest
    targets an older generation than the deployment's latest published
    generation. A supplied generation id is exact and returns only
    manifests completed for that generation.
    """

    generation_predicate = (
        "AND m.target_generation_id = %(generation_id)s"
        if generation_id is not None
        else ""
    )
    generation_cte, generation_params = _published_generation_cte(
        conn,
        deployment_name=deployment_name,
    )
    params = {
        "deployment_name": deployment_name,
        "generation_id": generation_id,
        **generation_params,
    }
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH {generation_cte},
            latest_generation AS (
              SELECT generation_id
                FROM published_generations
               ORDER BY published_at DESC NULLS LAST, generation_id DESC
               LIMIT 1
            ),
            slot_names AS (
              SELECT manifest_id,
                     array_agg(slot_name ORDER BY slot_name) AS names
                FROM okg.derived_output_slots
               GROUP BY manifest_id
            ),
            ranked AS (
              SELECT d.definition_id,
                     d.deployment_name,
                     d.logical_key AS view_id,
                     COALESCE(
                       NULLIF(d.output_contract->>'view_name', ''),
                       d.logical_key
                     ) AS view_name,
                     m.manifest_id,
                     m.target_generation_id,
                     m.manifest_hash,
                     m.required_slots,
                     COALESCE(
                       sn.names,
                       m.required_slots,
                       ARRAY[]::text[]
                     ) AS slot_names,
                     r.status AS run_status,
                     m.reused_from_manifest_id,
                     m.promoted_at,
                     g.published_at,
                     lg.generation_id AS latest_published_generation_id,
                     row_number() OVER (
                       PARTITION BY d.definition_id
                       ORDER BY m.target_generation_id DESC,
                                m.promoted_at DESC
                     ) AS rn
                FROM okg.derived_artifact_definitions d
                JOIN okg.derived_output_manifests m
                  ON m.definition_id = d.definition_id
                JOIN okg.derived_artifact_runs r
                  ON r.run_id = m.run_id
                JOIN published_generations g
                  ON g.generation_id = m.target_generation_id
                CROSS JOIN latest_generation lg
                LEFT JOIN slot_names sn
                  ON sn.manifest_id = m.manifest_id
               WHERE d.deployment_name = %(deployment_name)s
                 AND d.artifact_kind = 'view'
                 AND m.status = 'completed'
                 AND r.status IN ('completed', 'carried_forward')
                 {generation_predicate}
            )
            SELECT definition_id, deployment_name, view_id, view_name,
                   manifest_id, target_generation_id, manifest_hash,
                   required_slots, slot_names, run_status,
                   reused_from_manifest_id, promoted_at, published_at,
                   latest_published_generation_id
              FROM ranked
             WHERE rn = 1
             ORDER BY view_id, definition_id
            """,
            params,
        )
        summaries = [
            _row_to_summary(row, reference_generation_id=generation_id)
            for row in cur.fetchall()
        ]
    if registry_view_ids is _REGISTRY_FROM_DEPLOYMENT:
        registry_view_ids = current_registry_view_ids(deployment_name)
    if registry_view_ids is None:
        return summaries
    current = frozenset(registry_view_ids)
    return [
        replace(summary, retired=summary.view_id not in current)
        for summary in summaries
    ]


def get_view_slot(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: UUID,
    slot_name: str,
) -> ViewOutputSlotRead:
    """Return one slot from a completed output-set manifest."""

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH RECURSIVE manifest_chain AS (
              SELECT m.manifest_id, m.reused_from_manifest_id,
                     m.target_generation_id, 0 AS depth
                FROM okg.derived_output_manifests m
               WHERE m.manifest_id = %(manifest_id)s
                 AND m.status = 'completed'
              UNION ALL
              SELECT parent.manifest_id, parent.reused_from_manifest_id,
                     parent.target_generation_id, manifest_chain.depth + 1
                FROM okg.derived_output_manifests parent
                JOIN manifest_chain
                  ON parent.manifest_id =
                     manifest_chain.reused_from_manifest_id
               WHERE parent.status = 'completed'
                 AND manifest_chain.depth < 64
            ),
            requested AS (
              SELECT manifest_id, reused_from_manifest_id, target_generation_id
                FROM manifest_chain
               WHERE depth = 0
            ),
            immediate_source AS (
              SELECT chain.manifest_id, chain.target_generation_id
                FROM manifest_chain chain
                JOIN requested
                  ON requested.reused_from_manifest_id = chain.manifest_id
               LIMIT 1
            ),
            slot_source AS (
              SELECT chain.manifest_id, chain.target_generation_id
                FROM manifest_chain chain
               WHERE EXISTS (
                       SELECT 1
                         FROM okg.derived_output_slots probe
                        WHERE probe.manifest_id = chain.manifest_id
                          AND probe.slot_name = %(slot_name)s
                     )
               ORDER BY chain.depth
               LIMIT 1
            )
            SELECT s.slot_id, requested.manifest_id, s.slot_name,
                   s.storage_kind, s.media_type, s.artifact_type,
                   s.renderer_ref, s.payload_hash,
                   CASE
                     WHEN slot_source.manifest_id = requested.manifest_id
                     THEN s.storage_ref
                     ELSE s.storage_ref || jsonb_build_object(
                       'target_generation_id',
                       requested.target_generation_id,
                       'source_generation_id',
                       COALESCE(
                         immediate_source.target_generation_id,
                         slot_source.target_generation_id
                       ),
                       'reused_from_manifest_id',
                       requested.reused_from_manifest_id::text,
                       'source_manifest_id',
                       slot_source.manifest_id::text
                     )
                   END AS storage_ref,
                   s.inline_payload, s.row_count, s.byte_count,
                   s.metadata
              FROM slot_source
              JOIN requested ON true
              LEFT JOIN immediate_source ON true
              JOIN okg.derived_output_slots s
                ON s.manifest_id = slot_source.manifest_id
               AND s.slot_name = %(slot_name)s
            """,
            {"manifest_id": manifest_id, "slot_name": slot_name},
        )
        row = cur.fetchone()
    if row is None:
        raise ViewSlotNotFound(
            f"slot {slot_name!r} not found on completed manifest "
            f"{manifest_id}"
        )
    return _row_to_slot(row)


def read_view_slot(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
    slot_name: str,
    generation_id: int | None = None,
    definition_id: str | None = None,
) -> ViewSlotRead:
    """Resolve a view and return one typed output slot.

    ``generation_id=None`` returns the latest completed manifest on a
    published generation. A supplied generation id is exact and never
    falls back to an earlier manifest.
    """

    if generation_id is None:
        artifact = get_latest_view_artifact(
            conn,
            deployment_name=deployment_name,
            view_id=view_id,
            definition_id=definition_id,
        )
    else:
        artifact = get_view_artifact_at_generation(
            conn,
            deployment_name=deployment_name,
            view_id=view_id,
            generation_id=generation_id,
            definition_id=definition_id,
        )
    slot = get_view_slot(
        conn,
        manifest_id=artifact.manifest_id,
        slot_name=slot_name,
    )
    return ViewSlotRead(artifact=artifact, slot=slot)


def _inspect_relation_slot(
    conn: psycopg.Connection[Any],
    *,
    slot: ViewOutputSlotRead,
    view_definition_id: str | None = None,
    view_target_generation_id: int | None = None,
    preview_limit: int,
) -> dict[str, Any]:
    storage_ref = dict(slot.payload_value() or {})
    schema_name = str(storage_ref.get("schema") or "")
    relation_name = str(storage_ref.get("relation") or "")
    upstream_definition_id = str(
        storage_ref.get("upstream_definition_id") or ""
    )
    view_generation_id = (
        view_target_generation_id
        if view_target_generation_id is not None
        else storage_ref.get("target_generation_id")
    )
    # Resolve the generation under which the relation ROWS physically live.
    #
    # The two carry-forward paths use OPPOSITE conventions for where the
    # slot's ``target_generation_id`` points, so no fixed precedence is
    # correct for both:
    #   * carry_forward_relation_artifact reuses the prior manifest verbatim
    #     — the rows stay under the prior ``target_generation_id`` and the
    #     slot binds ``upstream_target_generation_id`` to the (row-less) new
    #     generation.
    #   * carry_forward_view_artifact rebinds the slot's
    #     ``target_generation_id`` to the (row-less) NEW view generation and
    #     records the row-bearing generation in ``source_generation_id`` /
    #     leaves ``upstream_target_generation_id`` pointing at it.
    # A `target_generation_id`-first precedence fixes the relation path but
    # silently empties the view-carry-forward path (and vice versa). Instead
    # of guessing, probe the candidate generations and use whichever one
    # actually contains rows for this definition; fall back to the first
    # candidate so a genuinely empty relation still reports an honest zero.
    relation_generation_candidates: list[int] = []
    for _candidate in (
        storage_ref.get("target_generation_id"),
        storage_ref.get("upstream_target_generation_id"),
        storage_ref.get("source_generation_id"),
    ):
        if _candidate is None:
            continue
        _candidate_int = int(_candidate)
        if _candidate_int not in relation_generation_candidates:
            relation_generation_candidates.append(_candidate_int)
    relation_generation_id = (
        relation_generation_candidates[0]
        if relation_generation_candidates
        else None
    )
    out: dict[str, Any] = {
        "available": False,
        "schema": schema_name or None,
        "relation": relation_name or None,
        "physical_relation": (
            f"{schema_name}.{relation_name}"
            if schema_name and relation_name else None
        ),
        "logical_relation_id": storage_ref.get("logical_relation_id"),
        "upstream_definition_id": upstream_definition_id or None,
        "upstream_manifest_id": storage_ref.get("upstream_manifest_id"),
        "upstream_target_generation_id": storage_ref.get(
            "upstream_target_generation_id"
        ),
        "target_generation_id": view_generation_id,
        "view_target_generation_id": view_generation_id,
        "relation_target_generation_id": relation_generation_id,
        "source_generation_id": storage_ref.get("source_generation_id"),
        "bound_hashes": {
            "compute_hash": storage_ref.get("bound_compute_hash"),
            "output_schema_hash": storage_ref.get(
                "bound_output_schema_hash"
            ),
            "renderer_hash": storage_ref.get("bound_renderer_hash"),
            "dirty_rule_hash": storage_ref.get("bound_dirty_rule_hash"),
        },
        "declared": {
            "columns": storage_ref.get("columns") or [],
            "column_names": storage_ref.get("column_names") or [],
            "scope": storage_ref.get("scope") or {},
            "row_identity": storage_ref.get("row_identity") or {},
        },
        "recorded": {
            "slot_row_count": slot.row_count,
            "slot_byte_count": slot.byte_count,
            "storage_row_count": storage_ref.get("row_count"),
            "storage_scope_count": storage_ref.get("scope_count"),
        },
    }
    if (
        not schema_name
        or not relation_name
        or not upstream_definition_id
        or view_generation_id is None
        or relation_generation_id is None
    ):
        out["reason"] = "incomplete_relation_storage_ref"
        return out

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.oid,
                   pg_total_relation_size(c.oid),
                   pg_relation_size(c.oid),
                   pg_indexes_size(c.oid)
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = %(schema_name)s
               AND c.relname = %(relation_name)s
            """,
            {
                "schema_name": schema_name,
                "relation_name": relation_name,
            },
        )
        size_row = cur.fetchone()
        if size_row is None:
            out["reason"] = "physical_relation_not_found"
            return out

        # Pick the candidate generation that actually holds rows (see the
        # comment at the candidate list above). Probe in priority order and
        # stop at the first non-empty one; keep the first candidate if none
        # has rows so a genuinely empty relation reports an honest zero.
        for _candidate_int in relation_generation_candidates:
            cur.execute(
                pg_sql.SQL(
                    """
                    SELECT EXISTS (
                      SELECT 1
                        FROM {}.{}
                       WHERE definition_id = %(definition_id)s
                         AND target_generation_id = %(target_generation_id)s
                    )
                    """
                ).format(
                    pg_sql.Identifier(schema_name),
                    pg_sql.Identifier(relation_name),
                ),
                {
                    "definition_id": upstream_definition_id,
                    "target_generation_id": _candidate_int,
                },
            )
            if cur.fetchone()[0]:
                relation_generation_id = _candidate_int
                break
        out["relation_target_generation_id"] = relation_generation_id

        cur.execute(
            pg_sql.SQL(
                """
                SELECT count(*)::bigint,
                       count(DISTINCT scope_key)::bigint
                  FROM {}.{}
                 WHERE definition_id = %(definition_id)s
                   AND target_generation_id = %(target_generation_id)s
                """
            ).format(
                pg_sql.Identifier(schema_name),
                pg_sql.Identifier(relation_name),
            ),
            {
                "definition_id": upstream_definition_id,
                "target_generation_id": int(relation_generation_id),
            },
        )
        count_row = cur.fetchone()
        visible_stats: dict[str, Any] | None = None
        upstream_manifest_id = storage_ref.get("upstream_manifest_id")
        if upstream_manifest_id:
            visible_stats = relation_visible_stats_for_manifest_id(
                conn,
                definition_id=upstream_definition_id,
                manifest_id=UUID(str(upstream_manifest_id)),
            )
        cur.execute(
            """
            SELECT relation_id, logical_relation_id, output_schema_hash,
                   ddl_hash, partition_policy, index_policy,
                   retention_policy, status, created_at, retired_at
              FROM okg.derived_relation_registry
             WHERE schema_name = %(schema_name)s
               AND physical_name = %(relation_name)s
               AND definition_id = %(definition_id)s
             ORDER BY (status = 'active') DESC, created_at DESC
             LIMIT 1
            """,
            {
                "schema_name": schema_name,
                "relation_name": relation_name,
                "definition_id": upstream_definition_id,
            },
        )
        registry_row = cur.fetchone()
        preview_rows: list[dict[str, Any]] = []
        if preview_limit > 0:
            if upstream_manifest_id:
                visible_rows = relation_visible_rows_for_manifest_id(
                    conn,
                    definition_id=upstream_definition_id,
                    manifest_id=UUID(str(upstream_manifest_id)),
                )
                preview_rows = [
                    {
                        "scope_key": row.scope_key,
                        "row_key": row.row_key,
                        "row_data": dict(row.row_data),
                        "dependency_hash": row.dependency_hash,
                    }
                    for row in visible_rows[: int(preview_limit)]
                ]
            else:
                cur.execute(
                    pg_sql.SQL(
                        """
                        SELECT scope_key, row_key, row_data, dependency_hash
                          FROM {}.{}
                         WHERE definition_id = %(definition_id)s
                           AND target_generation_id = %(target_generation_id)s
                         ORDER BY scope_key, row_key
                         LIMIT %(limit)s
                        """
                    ).format(
                        pg_sql.Identifier(schema_name),
                        pg_sql.Identifier(relation_name),
                    ),
                    {
                        "definition_id": upstream_definition_id,
                        "target_generation_id": int(relation_generation_id),
                        "limit": int(preview_limit),
                    },
                )
                preview_rows = [
                    {
                        "scope_key": row[0],
                        "row_key": row[1],
                        "row_data": row[2],
                        "dependency_hash": row[3],
                    }
                    for row in cur.fetchall()
                ]
            if preview_rows:
                _attach_lens_overlays(
                    cur,
                    preview_rows,
                    view_definition_id=view_definition_id,
                    target_generation_id=int(view_generation_id),
                    slot_name=slot.slot_name,
                )

    actual_row_count = (
        int(visible_stats["row_count"])
        if visible_stats is not None
        else int(count_row[0] or 0)
    )
    actual_scope_count = (
        int(visible_stats["scope_count"])
        if visible_stats is not None
        else int(count_row[1] or 0)
    )
    out.update({
        "available": True,
        "row_count": actual_row_count,
        "scope_count": actual_scope_count,
        "physical_row_count": int(count_row[0] or 0),
        "bytes": {
            "total": int(size_row[1] or 0),
            "table": int(size_row[2] or 0),
            "indexes": int(size_row[3] or 0),
        },
        "preview": {
            "limit": int(preview_limit),
            "rows": preview_rows,
            "truncated": actual_row_count > len(preview_rows),
        },
    })
    if registry_row is not None:
        out["registry"] = {
            "relation_id": str(registry_row[0]),
            "logical_relation_id": registry_row[1],
            "output_schema_hash": registry_row[2],
            "ddl_hash": registry_row[3],
            "partition_policy": registry_row[4],
            "index_policy": registry_row[5],
            "retention_policy": registry_row[6],
            "status": registry_row[7],
            "created_at": (
                registry_row[8].isoformat()
                if registry_row[8] is not None else None
            ),
            "retired_at": (
                registry_row[9].isoformat()
                if registry_row[9] is not None else None
            ),
        }
    else:
        out["registry"] = None
    return out


def _attach_lens_overlays(
    cur: psycopg.Cursor[Any],
    rows: list[dict[str, Any]],
    *,
    view_definition_id: str | None,
    target_generation_id: int,
    slot_name: str,
) -> None:
    from okg.substrate.lenses import applicable_lens_overlays_for_row
    from okg.substrate.lenses import load_lens_overlays_for_rows

    row_identities = [
        (str(row["scope_key"]), str(row["row_key"]))
        for row in rows
    ]
    overlays_by_row = load_lens_overlays_for_rows(
        cur,
        view_definition_id=view_definition_id,
        target_generation_id=target_generation_id,
        slot_name=slot_name,
        row_identities=row_identities,
    )
    for row in rows:
        overlays = overlays_by_row.get(
            (str(row["scope_key"]), str(row["row_key"])),
            [],
        )
        applicable = applicable_lens_overlays_for_row(
            row.get("row_data") if isinstance(row.get("row_data"), Mapping)
            else {},
            overlays,
        )
        if applicable:
            row["lens_overlays"] = applicable


def _resolve_definition_id(
    cur: psycopg.Cursor[Any],
    *,
    deployment_name: str,
    view_id: str,
    definition_id: str | None,
) -> str:
    if definition_id is not None:
        cur.execute(
            """
            SELECT definition_id
              FROM okg.derived_artifact_definitions
             WHERE definition_id = %(definition_id)s
               AND deployment_name = %(deployment_name)s
               AND artifact_kind = 'view'
               AND logical_key = %(view_id)s
            """,
            {
                "definition_id": definition_id,
                "deployment_name": deployment_name,
                "view_id": view_id,
            },
        )
        row = cur.fetchone()
        if row is None:
            raise ViewArtifactNotFound(
                "view definition not found for "
                f"deployment={deployment_name!r} view_id={view_id!r} "
                f"definition_id={definition_id!r}"
            )
        return str(row[0])

    cur.execute(
        """
        SELECT definition_id
          FROM okg.derived_artifact_definitions
         WHERE deployment_name = %(deployment_name)s
           AND artifact_kind = 'view'
           AND logical_key = %(view_id)s
         ORDER BY definition_id
        """,
        {
            "deployment_name": deployment_name,
            "view_id": view_id,
        },
    )
    definition_ids = [str(row[0]) for row in cur.fetchall()]
    if not definition_ids:
        raise ViewArtifactNotFound(
            "view definition not found for "
            f"deployment={deployment_name!r} view_id={view_id!r}"
        )
    if len(definition_ids) > 1:
        raise ViewArtifactAmbiguous(
            deployment_name=deployment_name,
            view_id=view_id,
            definition_ids=definition_ids,
        )
    return definition_ids[0]


def _fetch_view_artifact(
    cur: psycopg.Cursor[Any],
    *,
    deployment_name: str,
    definition_id: str,
    generation_id: int | None,
) -> ViewArtifactRead | None:
    generation_predicate = (
        "AND m.target_generation_id = %(generation_id)s"
        if generation_id is not None
        else ""
    )
    generation_cte, generation_params = _published_generation_cte(
        cur.connection,
        deployment_name=deployment_name,
    )
    params = {
        "deployment_name": deployment_name,
        "definition_id": definition_id,
        "generation_id": generation_id,
        **generation_params,
    }
    cur.execute(
        f"""
        WITH {generation_cte}
        SELECT d.definition_id, d.deployment_name, d.logical_key,
               m.manifest_id, m.run_id, m.target_generation_id,
               m.manifest_hash, m.required_slots,
               m.reused_from_manifest_id, m.metadata, m.promoted_at,
               r.status, r.compute_hash, r.output_schema_hash,
               r.renderer_hash, r.dirty_rule_hash,
               d.renderer_ref, d.output_contract, g.published_at
          FROM okg.derived_artifact_definitions d
          JOIN okg.derived_output_manifests m
            ON m.definition_id = d.definition_id
          JOIN okg.derived_artifact_runs r
            ON r.run_id = m.run_id
          JOIN published_generations g
            ON g.generation_id = m.target_generation_id
         WHERE d.definition_id = %(definition_id)s
           AND d.artifact_kind = 'view'
           AND m.status = 'completed'
           AND r.status IN ('completed', 'carried_forward')
           {generation_predicate}
         ORDER BY m.target_generation_id DESC, m.promoted_at DESC
         LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    return _row_to_artifact(row) if row is not None else None


def _published_generation_cte(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
) -> tuple[str, dict[str, Any]]:
    records = _chronos_published_generation_records(
        conn,
        deployment_name=deployment_name,
    )
    if records:
        return (
            """
            published_generations AS (
              SELECT generation_id, published_at
                FROM jsonb_to_recordset(%(published_generations)s::jsonb)
                     AS g(generation_id bigint, published_at timestamptz)
            )
            """,
            {"published_generations": json.dumps(records)},
        )
    return (
        """
        published_generations AS (
          SELECT generation_id, published_at
            FROM okg.graph_generations
           WHERE deployment_name = %(deployment_name)s
             AND status = 'published'
        )
        """,
        {},
    )


def _chronos_published_generation_records(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
) -> list[dict[str, Any]]:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    try:
        checkpoints = store.list_generations(
            deployment_name=deployment_name,
            branch="default",
            status="published",
        )
    except psycopg.errors.UndefinedTable:
        return []
    records: list[dict[str, Any]] = []
    for checkpoint in checkpoints:
        metadata = dict(checkpoint.metadata or {})
        target_generation_id = metadata.get("target_generation_id")
        if target_generation_id is None:
            checkpoint_id = str(checkpoint.checkpoint_id)
            if checkpoint_id.isdigit():
                target_generation_id = int(checkpoint_id)
        if target_generation_id is None:
            continue
        published_at = (
            metadata.get("published_at")
            or metadata.get("created_at")
            or getattr(checkpoint, "created_at", None)
        )
        if isinstance(published_at, datetime):
            published_at = published_at.isoformat()
        records.append({
            "generation_id": int(target_generation_id),
            "published_at": published_at,
        })
    return records


def _row_to_artifact(row: tuple[Any, ...]) -> ViewArtifactRead:
    return ViewArtifactRead(
        definition_id=str(row[0]),
        deployment_name=str(row[1]),
        view_id=str(row[2]),
        manifest_id=row[3],
        run_id=row[4],
        target_generation_id=int(row[5]),
        manifest_hash=str(row[6]),
        required_slots=tuple(row[7] or ()),
        reused_from_manifest_id=row[8],
        manifest_metadata=row[9] or {},
        promoted_at=row[10],
        run_status=str(row[11]),
        compute_hash=str(row[12]),
        output_schema_hash=str(row[13]),
        renderer_hash=str(row[14]),
        dirty_rule_hash=str(row[15]),
        renderer_ref=str(row[16]),
        output_contract=row[17] or {},
        published_at=row[18],
    )


def _row_to_summary(
    row: tuple[Any, ...],
    *,
    reference_generation_id: int | None,
) -> ViewArtifactSummary:
    target_generation_id = int(row[5])
    latest_generation_id = int(row[13])
    freshness_generation_id = (
        int(reference_generation_id)
        if reference_generation_id is not None
        else latest_generation_id
    )
    run_status = str(row[9])
    freshness = (
        "fresh"
        if target_generation_id == freshness_generation_id
        and run_status in ("completed", "carried_forward")
        else "stale"
    )
    return ViewArtifactSummary(
        definition_id=str(row[0]),
        deployment_name=str(row[1]),
        view_id=str(row[2]),
        view_name=str(row[3]),
        manifest_id=row[4],
        target_generation_id=target_generation_id,
        manifest_hash=str(row[6]),
        required_slots=tuple(row[7] or ()),
        slot_names=tuple(row[8] or ()),
        run_status=run_status,
        freshness=freshness,
        reused_from_manifest_id=row[10],
        promoted_at=row[11],
        published_at=row[12],
        latest_published_generation_id=latest_generation_id,
    )


def _row_to_slot(row: tuple[Any, ...]) -> ViewOutputSlotRead:
    return ViewOutputSlotRead(
        slot_id=row[0],
        manifest_id=row[1],
        slot_name=str(row[2]),
        storage_kind=str(row[3]),
        media_type=str(row[4]),
        artifact_type=str(row[5]),
        renderer_ref=str(row[6]),
        payload_hash=str(row[7]),
        storage_ref=row[8] or {},
        inline_payload=row[9],
        row_count=row[10],
        byte_count=row[11],
        metadata=row[12] or {},
    )
