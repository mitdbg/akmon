"""Lens putback proof slice.

This module implements the first conservative lens boundary: an
editable relation slot can produce validated putback deltas against an
exact completed view manifest, and those deltas can be committed to an
overlay log. It deliberately does not write external source truth.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


LensPutbackStatus = Literal[
    "preparing",
    "prepared",
    "conflict",
    "committed_overlay",
    "rejected",
    "failed",
]


class LensPutbackError(Exception):
    """Base class for lens putback failures."""


class LensPutbackConflict(LensPutbackError):
    """Raised when an edit no longer matches the base manifest row."""


class LensPutbackStateError(LensPutbackError):
    """Raised when a request cannot transition from its current state."""


class LensDefinitionNotFound(LensPutbackError):
    """Raised when no lens definition matches a lookup."""


class LensDefinitionAmbiguous(LensPutbackError):
    """Raised when a view has multiple lenses and none was selected."""

    def __init__(
        self,
        *,
        deployment_name: str,
        view_id: str,
        lens_ids: Sequence[str],
    ) -> None:
        self.deployment_name = deployment_name
        self.view_id = view_id
        self.lens_ids = tuple(lens_ids)
        super().__init__(
            "view resolves to multiple lens definitions: "
            f"deployment={deployment_name!r} view_id={view_id!r} "
            f"lens_ids={list(self.lens_ids)!r}"
        )


class LensOverlayNotFound(LensPutbackError):
    """Raised when an overlay audit/revoke lookup has no match."""


@dataclass(frozen=True)
class LensDefinition:
    """Definition metadata for an editable derived lens."""

    definition_id: str
    deployment_name: str
    lens_id: str
    view_definition_id: str
    view_id: str
    compute_hash: str
    output_schema_hash: str
    renderer_hash: str
    dirty_rule_hash: str
    renderer_ref: str = "okg.renderer.lens.overlay.v1"
    lens_contract: Mapping[str, Any] = field(default_factory=dict)
    putback_policy: Mapping[str, Any] = field(default_factory=dict)
    source_authority_policy: Mapping[str, Any] = field(default_factory=dict)
    lens_promotion_mapping: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class LensPutbackDelta:
    """One deterministic edit emitted from a lens row."""

    slot_name: str
    scope_key: str
    row_key: str
    field_path: Sequence[str]
    expected_value: Any
    new_value: Any
    source_authority: str
    source_record_id: Mapping[str, Any]
    expected_source_revision: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LensPutbackRequest:
    """Persisted lens putback request summary."""

    request_id: UUID
    lens_definition_id: str
    deployment_name: str
    lens_id: str
    base_view_definition_id: str
    base_view_manifest_id: UUID
    target_generation_id: int
    actor_id: str
    status: LensPutbackStatus
    delta_count: int
    conflict_count: int
    idempotency_key: str | None = None
    error: Mapping[str, Any] | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "request_id": str(self.request_id),
            "lens_definition_id": self.lens_definition_id,
            "deployment_name": self.deployment_name,
            "lens_id": self.lens_id,
            "base_view_definition_id": self.base_view_definition_id,
            "base_view_manifest_id": str(self.base_view_manifest_id),
            "target_generation_id": self.target_generation_id,
            "actor_id": self.actor_id,
            "status": self.status,
            "delta_count": self.delta_count,
            "conflict_count": self.conflict_count,
            "idempotency_key": self.idempotency_key,
            "error": None if self.error is None else dict(self.error),
            "metadata": dict(self.metadata),
        }


def register_lens_definition(
    conn: psycopg.Connection[Any],
    definition: LensDefinition,
) -> None:
    """Register or update a lens definition.

    The shared derived-artifact table records split definition hashes;
    ``okg.lens_definitions`` records lens-specific authority and
    putback policy metadata.

    When ``definition.lens_promotion_mapping`` is non-None, it is
    validated up front via ``LensPromotionMapping.from_dict()`` so
    malformed mappings surface a ``LensPromotionMappingError`` at
    register time rather than at promotion time.
    """

    if definition.lens_promotion_mapping is not None:
        # Late import: promotion_mapping → putback → promotion_mapping
        # if pulled at module load. Imported here on the register path
        # to keep that cycle broken.
        from .promotion_mapping import LensPromotionMapping
        LensPromotionMapping.from_dict(
            definition.lens_promotion_mapping,
            definition_id=definition.definition_id,
        )

    with conn.transaction():
        with conn.cursor() as cur:
            _assert_view_definition(cur, definition)
            cur.execute(
                """
                INSERT INTO okg.derived_artifact_definitions (
                  definition_id, artifact_kind, deployment_name,
                  logical_key, compute_hash, output_schema_hash,
                  renderer_hash, dirty_rule_hash, renderer_ref,
                  output_contract, dirty_rule
                ) VALUES (
                  %(definition_id)s, 'lens', %(deployment_name)s,
                  %(lens_id)s, %(compute_hash)s, %(output_schema_hash)s,
                  %(renderer_hash)s, %(dirty_rule_hash)s, %(renderer_ref)s,
                  %(output_contract)s, %(dirty_rule)s
                )
                ON CONFLICT (definition_id) DO UPDATE SET
                  artifact_kind = 'lens',
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
                """,
                {
                    "definition_id": definition.definition_id,
                    "deployment_name": definition.deployment_name,
                    "lens_id": definition.lens_id,
                    "compute_hash": definition.compute_hash,
                    "output_schema_hash": definition.output_schema_hash,
                    "renderer_hash": definition.renderer_hash,
                    "dirty_rule_hash": definition.dirty_rule_hash,
                    "renderer_ref": definition.renderer_ref,
                    "output_contract": Jsonb(dict(definition.lens_contract)),
                    "dirty_rule": Jsonb(dict(definition.putback_policy)),
                },
            )
            cur.execute(
                """
                INSERT INTO okg.lens_definitions (
                  definition_id, deployment_name, lens_id,
                  view_definition_id, view_id, lens_contract,
                  putback_policy, source_authority_policy,
                  lens_promotion_mapping
                ) VALUES (
                  %(definition_id)s, %(deployment_name)s, %(lens_id)s,
                  %(view_definition_id)s, %(view_id)s, %(lens_contract)s,
                  %(putback_policy)s, %(source_authority_policy)s,
                  %(lens_promotion_mapping)s
                )
                ON CONFLICT (definition_id) DO UPDATE SET
                  deployment_name = EXCLUDED.deployment_name,
                  lens_id = EXCLUDED.lens_id,
                  view_definition_id = EXCLUDED.view_definition_id,
                  view_id = EXCLUDED.view_id,
                  lens_contract = EXCLUDED.lens_contract,
                  putback_policy = EXCLUDED.putback_policy,
                  source_authority_policy =
                    EXCLUDED.source_authority_policy,
                  lens_promotion_mapping = COALESCE(
                    EXCLUDED.lens_promotion_mapping,
                    okg.lens_definitions.lens_promotion_mapping
                  ),
                  updated_at = now()
                """,
                {
                    "definition_id": definition.definition_id,
                    "deployment_name": definition.deployment_name,
                    "lens_id": definition.lens_id,
                    "view_definition_id": definition.view_definition_id,
                    "view_id": definition.view_id,
                    "lens_contract": Jsonb(dict(definition.lens_contract)),
                    "putback_policy": Jsonb(dict(definition.putback_policy)),
                    "source_authority_policy": Jsonb(
                        dict(definition.source_authority_policy)
                    ),
                    "lens_promotion_mapping": (
                        None
                        if definition.lens_promotion_mapping is None
                        else Jsonb(dict(definition.lens_promotion_mapping))
                    ),
                },
            )


def load_lens_definition_for_view(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
    lens_id: str | None = None,
) -> LensDefinition:
    """Load a registered lens for a view.

    ``lens_id`` is optional only when the view has exactly one lens.
    Callers that expose an operator surface should pass it once there
    are multiple edit policies for the same representational view.
    """

    lens_predicate = (
        "AND ld.lens_id = %(lens_id)s"
        if lens_id is not None
        else ""
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT ld.definition_id, ld.deployment_name, ld.lens_id,
                   ld.view_definition_id, ld.view_id,
                   d.compute_hash, d.output_schema_hash, d.renderer_hash,
                   d.dirty_rule_hash, d.renderer_ref,
                   ld.lens_contract, ld.putback_policy,
                   ld.source_authority_policy
              FROM okg.lens_definitions ld
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = ld.definition_id
             WHERE ld.deployment_name = %(deployment_name)s
               AND ld.view_id = %(view_id)s
               AND d.artifact_kind = 'lens'
               {lens_predicate}
             ORDER BY ld.lens_id, ld.definition_id
            """,
            {
                "deployment_name": deployment_name,
                "view_id": view_id,
                "lens_id": lens_id,
            },
        )
        rows = cur.fetchall()
    if not rows:
        selector = f" lens_id={lens_id!r}" if lens_id is not None else ""
        raise LensDefinitionNotFound(
            "lens definition not found: "
            f"deployment={deployment_name!r} view_id={view_id!r}{selector}"
        )
    if lens_id is None and len(rows) > 1:
        raise LensDefinitionAmbiguous(
            deployment_name=deployment_name,
            view_id=view_id,
            lens_ids=[str(row[2]) for row in rows],
        )
    return _row_to_lens_definition(rows[0])


def list_lens_definitions_for_view(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str,
) -> list[LensDefinition]:
    """List registered lenses for a completed view surface."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ld.definition_id, ld.deployment_name, ld.lens_id,
                   ld.view_definition_id, ld.view_id,
                   d.compute_hash, d.output_schema_hash, d.renderer_hash,
                   d.dirty_rule_hash, d.renderer_ref,
                   ld.lens_contract, ld.putback_policy,
                   ld.source_authority_policy
              FROM okg.lens_definitions ld
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = ld.definition_id
             WHERE ld.deployment_name = %(deployment_name)s
               AND ld.view_id = %(view_id)s
               AND d.artifact_kind = 'lens'
             ORDER BY ld.lens_id, ld.definition_id
            """,
            {
                "deployment_name": deployment_name,
                "view_id": view_id,
            },
        )
        return [_row_to_lens_definition(row) for row in cur.fetchall()]


def _row_to_lens_definition(row: Sequence[Any]) -> LensDefinition:
    return LensDefinition(
        definition_id=str(row[0]),
        deployment_name=str(row[1]),
        lens_id=str(row[2]),
        view_definition_id=str(row[3]),
        view_id=str(row[4]),
        compute_hash=str(row[5]),
        output_schema_hash=str(row[6]),
        renderer_hash=str(row[7]),
        dirty_rule_hash=str(row[8]),
        renderer_ref=str(row[9]),
        lens_contract=dict(row[10] or {}),
        putback_policy=dict(row[11] or {}),
        source_authority_policy=dict(row[12] or {}),
    )


def _assert_view_definition(
    cur: psycopg.Cursor[Any],
    definition: LensDefinition,
) -> None:
    cur.execute(
        """
        SELECT 1
          FROM okg.derived_artifact_definitions
         WHERE definition_id = %(view_definition_id)s
           AND deployment_name = %(deployment_name)s
           AND logical_key = %(view_id)s
           AND artifact_kind = 'view'
        """,
        {
            "view_definition_id": definition.view_definition_id,
            "deployment_name": definition.deployment_name,
            "view_id": definition.view_id,
        },
    )
    if cur.fetchone() is None:
        raise LensPutbackError(
            "lens definition must reference an existing view definition: "
            f"view_definition_id={definition.view_definition_id!r} "
            f"deployment={definition.deployment_name!r} "
            f"view_id={definition.view_id!r}"
        )


def prepare_lens_putback(
    conn: psycopg.Connection[Any],
    definition: LensDefinition,
    *,
    base_view_manifest_id: UUID,
    target_generation_id: int,
    actor_id: str,
    deltas: Sequence[LensPutbackDelta],
    idempotency_key: str | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> LensPutbackRequest:
    """Validate a set of lens edits against an exact view manifest.

    Each delta compares the caller's expected value with the row value
    in the completed relation slot. Conflicts are persisted and the
    request stops in ``conflict`` instead of writing an overlay.
    """

    if not deltas:
        raise ValueError("prepare_lens_putback requires at least one delta")
    _validate_delta_shapes(deltas)
    _validate_delta_policy(definition, deltas)

    with conn.transaction():
        with conn.cursor() as cur:
            existing = _load_idempotent_request(
                cur,
                definition=definition,
                actor_id=actor_id,
                idempotency_key=idempotency_key,
            )
            if existing is not None:
                return existing

            _assert_base_view_manifest(
                cur,
                definition=definition,
                manifest_id=base_view_manifest_id,
                target_generation_id=target_generation_id,
            )
            cur.execute(
                """
                INSERT INTO okg.lens_putback_requests (
                  lens_definition_id, deployment_name, lens_id,
                  base_view_definition_id, base_view_manifest_id,
                  target_generation_id, actor_id, idempotency_key,
                  status, delta_count, metadata
                ) VALUES (
                  %(lens_definition_id)s, %(deployment_name)s, %(lens_id)s,
                  %(base_view_definition_id)s, %(base_view_manifest_id)s,
                  %(target_generation_id)s, %(actor_id)s,
                  %(idempotency_key)s, 'preparing', %(delta_count)s,
                  %(metadata)s
                )
                RETURNING request_id
                """,
                {
                    "lens_definition_id": definition.definition_id,
                    "deployment_name": definition.deployment_name,
                    "lens_id": definition.lens_id,
                    "base_view_definition_id": definition.view_definition_id,
                    "base_view_manifest_id": base_view_manifest_id,
                    "target_generation_id": target_generation_id,
                    "actor_id": actor_id,
                    "idempotency_key": idempotency_key,
                    "delta_count": len(deltas),
                    "metadata": Jsonb(dict(metadata or {})),
                },
            )
            (request_id,) = cur.fetchone()

            conflict_count = 0
            for delta_index, delta in enumerate(deltas):
                validation = _validate_delta_against_slot(
                    cur,
                    base_view_manifest_id=base_view_manifest_id,
                    target_generation_id=target_generation_id,
                    delta=delta,
                )
                if validation["status"] == "conflict":
                    conflict_count += 1
                _insert_delta(
                    cur,
                    request_id=request_id,
                    delta_index=delta_index,
                    delta=delta,
                    validation=validation,
                )

            status: LensPutbackStatus = (
                "conflict" if conflict_count else "prepared"
            )
            cur.execute(
                """
                UPDATE okg.lens_putback_requests
                   SET status = %(status)s,
                       conflict_count = %(conflict_count)s,
                       prepared_at = CASE
                         WHEN %(status)s = 'prepared' THEN now()
                         ELSE prepared_at
                       END,
                       error = CASE
                         WHEN %(status)s = 'conflict'
                         THEN %(error)s
                         ELSE error
                       END
                 WHERE request_id = %(request_id)s
                """,
                {
                    "request_id": request_id,
                    "status": status,
                    "conflict_count": conflict_count,
                    "error": Jsonb(
                        {
                            "type": "lens_putback_conflict",
                            "conflict_count": conflict_count,
                        }
                    ),
                },
            )
            return _load_request_by_id(cur, request_id)


def commit_lens_overlay(
    conn: psycopg.Connection[Any],
    *,
    request_id: UUID,
) -> LensPutbackRequest:
    """Commit a prepared request to the lens overlay log."""

    with conn.transaction():
        with conn.cursor() as cur:
            request = _load_request_by_id(cur, request_id, for_update=True)
            if request.status == "committed_overlay":
                return request
            if request.status != "prepared":
                raise LensPutbackStateError(
                    "lens putback request must be prepared before overlay "
                    f"commit: request_id={request_id} status={request.status}"
                )

            cur.execute(
                """
                INSERT INTO okg.lens_overlay_log (
                  request_id, delta_id, lens_definition_id,
                  target_generation_id, source_authority,
                  source_record_id, expected_source_revision, field_path,
                  old_value, new_value, source_delta, status, metadata
                )
                SELECT
                  r.request_id, d.delta_id, r.lens_definition_id,
                  r.target_generation_id, d.source_authority,
                  d.source_record_id, d.expected_source_revision,
                  d.field_path, d.expected_value, d.new_value,
                  d.source_delta, 'active', d.metadata
                FROM okg.lens_putback_requests r
                JOIN okg.lens_putback_deltas d
                  ON d.request_id = r.request_id
                WHERE r.request_id = %(request_id)s
                  AND d.validation_status = 'prepared'
                ON CONFLICT (delta_id) DO NOTHING
                """,
                {"request_id": request_id},
            )
            _record_overlay_frontier_metadata(cur, request_id=request_id)
            cur.execute(
                """
                UPDATE okg.lens_putback_requests
                   SET status = 'committed_overlay',
                       committed_at = now()
                 WHERE request_id = %(request_id)s
                """,
                {"request_id": request_id},
            )
            return _load_request_by_id(cur, request_id)


def _lineage_invalidation_payload(invalidation: Any) -> dict[str, Any]:
    payload = {
        "dependency_kind": invalidation.dependency_kind,
        "dependency_ref": invalidation.dependency_ref,
        "dirty_reason": invalidation.dirty_reason,
    }
    if invalidation.dependency_hash is not None:
        payload["dependency_hash"] = invalidation.dependency_hash
    return payload


def _record_overlay_frontier_metadata(
    cur: psycopg.Cursor[Any],
    *,
    request_id: UUID,
) -> None:
    """Persist projection-delta keys for committed overlay rows."""

    from okg.substrate.derived_artifacts.change_frontier import (
        lens_overlay_invalidations,
    )

    cur.execute(
        """
        SELECT overlay_id, source_record_id, source_delta
          FROM okg.lens_overlay_log
         WHERE request_id = %(request_id)s
           AND status = 'active'
        """,
        {"request_id": request_id},
    )
    rows = cur.fetchall()
    for overlay_id, source_record_id, source_delta in rows:
        invalidations = lens_overlay_invalidations([
            {
                "source_record_id": source_record_id,
                "source_delta": source_delta,
            }
        ])
        if not invalidations:
            continue
        cur.execute(
            """
            UPDATE okg.lens_overlay_log
               SET metadata = metadata || %(metadata)s
             WHERE overlay_id = %(overlay_id)s
            """,
            {
                "overlay_id": overlay_id,
                "metadata": Jsonb(
                    {
                        "projection_deltas": {
                            "source": "lens_overlay",
                            "request_id": str(request_id),
                            "lineage_invalidations": [
                                _lineage_invalidation_payload(invalidation)
                                for invalidation in invalidations
                            ],
                        }
                    }
                ),
            },
        )


SweepPolicy = Literal["sweep_to_failed", "auto_commit"]


def sweep_stale_putback_requests(
    conn: psycopg.Connection[Any],
    *,
    deployment: str,
    now: datetime,
    stale_after: timedelta = timedelta(hours=1),
    policy: SweepPolicy = "sweep_to_failed",
) -> dict[str, int]:
    """Sweep prepared putback requests older than ``stale_after``.

    Two policies:

      * ``sweep_to_failed`` (default): transitions stale prepared rows to
        ``status='failed'`` with a structured ``error`` describing the
        stale-prepared condition. The reader must call
        :func:`prepare_lens_putback` again to retry.
      * ``auto_commit``: applies stale prepared rows via
        :func:`commit_lens_overlay`. Used when the substrate is the
        intended committer (e.g. batch-prepare-then-commit flows).

    The ``now`` parameter is required (rather than reading ``now()`` from
    the database) so a wrapping DBOS workflow can capture ``now`` once in
    a ``@DBOS.step`` for replay-determinism: a recovered workflow sees the
    same timestamp the original attempt did and produces the same sweep
    set, not an expanded one drawn from the wall clock during downtime.

    Returns ``{"considered": N, "swept": M, "committed": K, "skipped": S}``.
    Always idempotent: terminal-status rows are filtered out by the WHERE
    clause; a second invocation on the same DB state returns
    ``{"considered": 0, ...}`` (modulo new stale rows that arrived in the
    meantime).
    """
    if policy not in ("sweep_to_failed", "auto_commit"):
        raise ValueError(
            f"unknown sweep policy: {policy!r} "
            "(expected 'sweep_to_failed' or 'auto_commit')"
        )
    cutoff = now - stale_after
    result = {"considered": 0, "swept": 0, "committed": 0, "skipped": 0}

    if policy == "sweep_to_failed":
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE okg.lens_putback_requests
                       SET status = 'failed',
                           rejected_at = %(now)s,
                           error = %(error)s
                     WHERE deployment_name = %(deployment)s
                       AND status = 'prepared'
                       AND prepared_at IS NOT NULL
                       AND prepared_at < %(cutoff)s
                    RETURNING request_id
                    """,
                    {
                        "now": now,
                        "deployment": deployment,
                        "cutoff": cutoff,
                        "error": Jsonb(
                            {
                                "type": "stale_prepared_request",
                                "stale_after_seconds": int(
                                    stale_after.total_seconds()
                                ),
                                "swept_at": now.isoformat(),
                            }
                        ),
                    },
                )
                swept_ids = [row[0] for row in cur.fetchall()]
        result["considered"] = len(swept_ids)
        result["swept"] = len(swept_ids)
        return result

    if policy == "auto_commit":
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT request_id
                  FROM okg.lens_putback_requests
                 WHERE deployment_name = %(deployment)s
                   AND status = 'prepared'
                   AND prepared_at IS NOT NULL
                   AND prepared_at < %(cutoff)s
                 ORDER BY prepared_at
                """,
                {"deployment": deployment, "cutoff": cutoff},
            )
            candidate_ids = [row[0] for row in cur.fetchall()]
        result["considered"] = len(candidate_ids)
        for request_id in candidate_ids:
            try:
                commit_lens_overlay(conn, request_id=request_id)
                result["committed"] += 1
            except LensPutbackStateError:
                # Another committer beat us; the row is no longer in
                # 'prepared' state. Skip rather than fail the sweep.
                result["skipped"] += 1
        return result

    # Unreachable: the policy-check at the top of the function raises
    # for any other value. Keeping this line silences the type checker
    # on the inferred-return-path.
    raise AssertionError(f"unhandled policy branch: {policy!r}")


def load_lens_putback_request(
    conn: psycopg.Connection[Any],
    request_id: UUID,
) -> LensPutbackRequest:
    """Load one persisted putback request."""

    with conn.cursor() as cur:
        return _load_request_by_id(cur, request_id)


def list_lens_overlays(
    conn: psycopg.Connection[Any],
    *,
    deployment_name: str,
    view_id: str | None = None,
    lens_id: str | None = None,
    status: str | None = "active",
    target_generation_id: int | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List overlay log rows for operator audit surfaces."""

    filters = ["ld.deployment_name = %(deployment_name)s"]
    params: dict[str, Any] = {
        "deployment_name": deployment_name,
        "limit": max(1, min(int(limit), 500)),
    }
    if view_id:
        filters.append("ld.view_id = %(view_id)s")
        params["view_id"] = view_id
    if lens_id:
        filters.append("ld.lens_id = %(lens_id)s")
        params["lens_id"] = lens_id
    if status:
        filters.append("l.status = %(status)s")
        params["status"] = status
    if target_generation_id is not None:
        filters.append("l.target_generation_id <= %(target_generation_id)s")
        params["target_generation_id"] = int(target_generation_id)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              l.overlay_id, l.request_id, l.delta_id,
              l.lens_definition_id, ld.lens_id, ld.view_definition_id,
              ld.view_id, ld.deployment_name, l.target_generation_id,
              l.source_authority, l.source_record_id,
              l.expected_source_revision, l.field_path, l.old_value,
              l.new_value, l.source_delta, l.status, l.metadata,
              l.created_at, l.superseded_at, r.actor_id,
              l.source_delta->>'slot_name' AS slot_name,
              l.source_delta->>'scope_key' AS scope_key,
              l.source_delta->>'row_key' AS row_key
              FROM okg.lens_overlay_log l
              JOIN okg.lens_definitions ld
                ON ld.definition_id = l.lens_definition_id
              JOIN okg.lens_putback_requests r
                ON r.request_id = l.request_id
             WHERE {" AND ".join(filters)}
             ORDER BY l.created_at DESC, l.overlay_id DESC
             LIMIT %(limit)s
            """,
            params,
        )
        return [_overlay_row_to_json(row) for row in cur.fetchall()]


def revoke_lens_overlay(
    conn: psycopg.Connection[Any],
    *,
    overlay_id: UUID,
    deployment_name: str,
    actor_id: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Mark an active overlay superseded while preserving audit evidence."""

    revocation = {
        "revoked_by": actor_id,
        "revoked_at": datetime.now(UTC).isoformat(),
    }
    if reason:
        revocation["revoked_reason"] = reason

    with conn.transaction():
        with conn.cursor() as cur:
            row = _load_overlay_by_id(
                cur,
                overlay_id=overlay_id,
                deployment_name=deployment_name,
                for_update=True,
            )
            if row is None:
                raise LensOverlayNotFound(
                    "lens overlay not found: "
                    f"overlay_id={overlay_id} deployment={deployment_name!r}"
                )
            if row["status"] != "active":
                return row
            cur.execute(
                """
                UPDATE okg.lens_overlay_log
                   SET status = 'superseded',
                       superseded_at = now(),
                       metadata = metadata || %(revocation)s
                 WHERE overlay_id = %(overlay_id)s
                """,
                {
                    "overlay_id": overlay_id,
                    "revocation": Jsonb(revocation),
                },
            )
            updated = _load_overlay_by_id(
                cur,
                overlay_id=overlay_id,
                deployment_name=deployment_name,
            )
            if updated is None:
                raise RuntimeError(f"overlay disappeared after update: {overlay_id}")
            return updated


def load_lens_overlays_for_rows(
    cur: psycopg.Cursor[Any],
    *,
    view_definition_id: str | None,
    target_generation_id: int,
    slot_name: str,
    row_identities: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Load active lens overlays keyed by ``(scope_key, row_key)``."""

    normalized = [
        (str(scope_key), str(row_key))
        for scope_key, row_key in row_identities
    ]
    if not normalized:
        return {}
    scope_keys = [scope_key for scope_key, _row_key in normalized]
    row_keys = [row_key for _scope_key, row_key in normalized]
    view_predicate = (
        "AND ld.view_definition_id = %(view_definition_id)s"
        if view_definition_id
        else ""
    )
    cur.execute(
        f"""
        WITH requested(scope_key, row_key) AS (
          SELECT *
            FROM unnest(%(scope_keys)s::text[], %(row_keys)s::text[])
        )
        SELECT DISTINCT ON (
               requested.scope_key, requested.row_key,
               l.lens_definition_id, l.field_path
               )
               requested.scope_key, requested.row_key,
               l.overlay_id, l.request_id, l.delta_id,
               l.lens_definition_id, ld.lens_id, l.source_authority,
               l.source_record_id, l.expected_source_revision,
               l.field_path, l.old_value, l.new_value, l.status,
               l.source_delta, l.target_generation_id
          FROM requested
          JOIN okg.lens_overlay_log l
            ON l.source_delta->>'scope_key' = requested.scope_key
           AND l.source_delta->>'row_key' = requested.row_key
          JOIN okg.lens_definitions ld
            ON ld.definition_id = l.lens_definition_id
         WHERE l.target_generation_id <= %(target_generation_id)s
           AND l.status = 'active'
           AND l.source_delta->>'slot_name' = %(slot_name)s
           {view_predicate}
         ORDER BY requested.scope_key, requested.row_key,
                  l.lens_definition_id, l.field_path,
                  l.target_generation_id DESC,
                  l.created_at DESC, l.overlay_id DESC
        """,
        {
            "target_generation_id": int(target_generation_id),
            "slot_name": slot_name,
            "scope_keys": scope_keys,
            "row_keys": row_keys,
            "view_definition_id": view_definition_id,
        },
    )
    overlays_by_row: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for overlay in cur.fetchall():
        key = (str(overlay[0]), str(overlay[1]))
        overlays_by_row.setdefault(key, []).append(
            {
                "overlay_id": str(overlay[2]),
                "request_id": str(overlay[3]),
                "delta_id": str(overlay[4]),
                "lens_definition_id": overlay[5],
                "lens_id": overlay[6],
                "source_authority": overlay[7],
                "source_record_id": overlay[8],
                "expected_source_revision": overlay[9],
                "field_path": list(overlay[10] or []),
                "old_value": overlay[11],
                "new_value": overlay[12],
                "status": overlay[13],
                "source_delta": overlay[14],
                "target_generation_id": int(overlay[15]),
            }
        )
    return overlays_by_row


def applicable_lens_overlays_for_row(
    row_data: Mapping[str, Any],
    overlays: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return active overlays whose exact base still matches ``row_data``.

    Active overlay evidence can carry forward to later generations only
    while the row still contains the value the operator validated
    against. If the source relation changes underneath the row, the old
    overlay remains auditable in ``lens_overlay_log`` but no longer
    decorates the current read.
    """

    applicable: list[dict[str, Any]] = []
    for overlay in overlays:
        field_path = overlay.get("field_path")
        if (
            not isinstance(field_path, Sequence)
            or isinstance(field_path, str)
        ):
            continue
        found, actual_value = _extract_path(
            row_data,
            [str(part) for part in field_path],
        )
        if found and actual_value == overlay.get("old_value"):
            applicable.append(dict(overlay))
    return applicable


def _load_overlay_by_id(
    cur: psycopg.Cursor[Any],
    *,
    overlay_id: UUID,
    deployment_name: str,
    for_update: bool = False,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT
          l.overlay_id, l.request_id, l.delta_id,
          l.lens_definition_id, ld.lens_id, ld.view_definition_id,
          ld.view_id, ld.deployment_name, l.target_generation_id,
          l.source_authority, l.source_record_id,
          l.expected_source_revision, l.field_path, l.old_value,
          l.new_value, l.source_delta, l.status, l.metadata,
          l.created_at, l.superseded_at, r.actor_id,
          l.source_delta->>'slot_name' AS slot_name,
          l.source_delta->>'scope_key' AS scope_key,
          l.source_delta->>'row_key' AS row_key
          FROM okg.lens_overlay_log l
          JOIN okg.lens_definitions ld
            ON ld.definition_id = l.lens_definition_id
          JOIN okg.lens_putback_requests r
            ON r.request_id = l.request_id
         WHERE l.overlay_id = %(overlay_id)s
           AND ld.deployment_name = %(deployment_name)s
         {"FOR UPDATE OF l" if for_update else ""}
        """,
        {
            "overlay_id": overlay_id,
            "deployment_name": deployment_name,
        },
    )
    row = cur.fetchone()
    return None if row is None else _overlay_row_to_json(row)


def _overlay_row_to_json(row: Sequence[Any]) -> dict[str, Any]:
    return {
        "overlay_id": str(row[0]),
        "request_id": str(row[1]),
        "delta_id": str(row[2]),
        "lens_definition_id": row[3],
        "lens_id": row[4],
        "view_definition_id": row[5],
        "view_id": row[6],
        "deployment_name": row[7],
        "target_generation_id": int(row[8]),
        "source_authority": row[9],
        "source_record_id": row[10],
        "expected_source_revision": row[11],
        "field_path": list(row[12] or []),
        "old_value": row[13],
        "new_value": row[14],
        "source_delta": row[15],
        "status": row[16],
        "metadata": row[17] or {},
        "created_at": row[18].isoformat() if row[18] is not None else None,
        "superseded_at": (
            row[19].isoformat() if row[19] is not None else None
        ),
        "actor_id": row[20],
        "slot_name": row[21],
        "scope_key": row[22],
        "row_key": row[23],
    }


def _validate_delta_shapes(deltas: Sequence[LensPutbackDelta]) -> None:
    for index, delta in enumerate(deltas):
        if not delta.field_path:
            raise ValueError(f"delta {index} must declare a field_path")
        if any(not part for part in delta.field_path):
            raise ValueError(f"delta {index} has an empty field_path segment")
        if not delta.source_authority:
            raise ValueError(f"delta {index} must declare source_authority")
        if not delta.source_record_id:
            raise ValueError(f"delta {index} must declare source_record_id")


def _validate_delta_policy(
    definition: LensDefinition,
    deltas: Sequence[LensPutbackDelta],
) -> None:
    editable_slots = definition.lens_contract.get("editable_slots")
    if not isinstance(editable_slots, Mapping) or not editable_slots:
        raise LensPutbackError(
            "lens contract must declare editable_slots before putback"
        )
    allowed_authorities = definition.source_authority_policy.get(
        "allowed_authorities"
    )
    if not isinstance(allowed_authorities, Sequence) or isinstance(
        allowed_authorities,
        str,
    ):
        raise LensPutbackError(
            "lens source_authority_policy must declare allowed_authorities"
        )
    allowed_authority_set = {str(value) for value in allowed_authorities}
    commit_modes = definition.source_authority_policy.get("commit_modes")
    if commit_modes is not None:
        if not isinstance(commit_modes, Sequence) or isinstance(
            commit_modes,
            str,
        ):
            raise LensPutbackError(
                "lens source_authority_policy commit_modes must be a list"
            )
        if "overlay" not in {str(value) for value in commit_modes}:
            raise LensPutbackError(
                "V1 lens putback requires overlay commit mode"
            )

    for index, delta in enumerate(deltas):
        slot_policy = editable_slots.get(delta.slot_name)
        if not isinstance(slot_policy, Mapping):
            raise LensPutbackError(
                f"delta {index} targets non-editable slot "
                f"{delta.slot_name!r}"
            )
        editable_fields = slot_policy.get("editable_fields")
        if not isinstance(editable_fields, Sequence) or isinstance(
            editable_fields,
            str,
        ):
            raise LensPutbackError(
                f"slot {delta.slot_name!r} must declare editable_fields"
            )
        field_name = str(delta.field_path[0])
        if field_name not in {str(value) for value in editable_fields}:
            raise LensPutbackError(
                f"delta {index} targets non-editable field {field_name!r} "
                f"on slot {delta.slot_name!r}"
            )
        if delta.source_authority not in allowed_authority_set:
            raise LensPutbackError(
                f"delta {index} source authority "
                f"{delta.source_authority!r} is not allowed by lens policy"
            )


def _assert_base_view_manifest(
    cur: psycopg.Cursor[Any],
    *,
    definition: LensDefinition,
    manifest_id: UUID,
    target_generation_id: int,
) -> None:
    # The view's generation must be published. A legacy publisher records
    # that as an okg.graph_generations row. The Chronos-native publisher
    # writes no such row: it stamps the integer token it gives view
    # manifests into the published checkpoint's metadata
    # (`target_generation_id`), so accept a published checkpoint of the
    # same deployment carrying this token. `kind = 'okg_generation'` is the
    # filter okg.v_generations and generation_resolver use, so a checkpoint
    # of any other kind never counts. A lens putback carries no branch, so
    # there is no branch to match; the token names one generation.
    cur.execute(
        """
        SELECT m.manifest_id
          FROM okg.derived_output_manifests m
          JOIN okg.derived_artifact_definitions d
            ON d.definition_id = m.definition_id
         WHERE m.manifest_id = %(manifest_id)s
           AND m.definition_id = %(view_definition_id)s
           AND m.target_generation_id = %(target_generation_id)s
           AND m.status = 'completed'
           AND d.artifact_kind = 'view'
           AND d.deployment_name = %(deployment_name)s
           AND (
             EXISTS (
               SELECT 1
                 FROM okg.graph_generations g
                WHERE g.generation_id = m.target_generation_id
                  AND g.status = 'published'
             )
             OR EXISTS (
               SELECT 1
                 FROM public._chronos_branch_interval_checkpoints c
                WHERE c.metadata::jsonb ->> 'kind' = 'okg_generation'
                  AND c.metadata::jsonb ->> 'status' = 'published'
                  AND c.metadata::jsonb ->> 'deployment_name'
                      = %(deployment_name)s
                  AND c.metadata::jsonb ->> 'target_generation_id'
                      = %(target_generation_id)s::text
             )
           )
        """,
        {
            "manifest_id": manifest_id,
            "view_definition_id": definition.view_definition_id,
            "target_generation_id": target_generation_id,
            "deployment_name": definition.deployment_name,
        },
    )
    if cur.fetchone() is None:
        raise LensPutbackError(
            "base view manifest is not a completed published view "
            f"manifest_id={manifest_id} "
            f"view_definition_id={definition.view_definition_id!r} "
            f"target_generation_id={target_generation_id}"
        )


def _validate_delta_against_slot(
    cur: psycopg.Cursor[Any],
    *,
    base_view_manifest_id: UUID,
    target_generation_id: int,
    delta: LensPutbackDelta,
) -> dict[str, Any]:
    slot_ref = _load_relation_slot_ref(
        cur,
        manifest_id=base_view_manifest_id,
        slot_name=delta.slot_name,
    )
    row = _load_relation_row(
        cur,
        slot_ref=slot_ref,
        target_generation_id=target_generation_id,
        scope_key=delta.scope_key,
        row_key=delta.row_key,
    )
    if row is None:
        return {
            "status": "conflict",
            "error": {
                "type": "row_not_found",
                "slot_name": delta.slot_name,
                "scope_key": delta.scope_key,
                "row_key": delta.row_key,
            },
            "actual_value": None,
        }
    found, actual_value = _extract_path(row, delta.field_path)
    if not found or actual_value != delta.expected_value:
        return {
            "status": "conflict",
            "error": {
                "type": "expected_value_mismatch",
                "slot_name": delta.slot_name,
                "scope_key": delta.scope_key,
                "row_key": delta.row_key,
                "field_path": list(delta.field_path),
                "expected_value": delta.expected_value,
                "actual_value": actual_value,
                "path_found": found,
            },
            "actual_value": actual_value,
        }
    return {
        "status": "prepared",
        "error": None,
        "actual_value": actual_value,
    }


def _load_relation_slot_ref(
    cur: psycopg.Cursor[Any],
    *,
    manifest_id: UUID,
    slot_name: str,
) -> Mapping[str, Any]:
    # perf-reimpl's manifest-reuse view storage means a carried-forward manifest
    # physically stores ZERO slot rows -- clean slots are inherited through
    # reused_from_manifest_id. Resolve the slot through the reuse lineage (the same
    # recursive walk every other view reader uses) rather than a direct slot lookup,
    # which misses an inherited slot and wrongly reports the base manifest slotless.
    from okg.substrate.derived_artifacts.read_views import (
        ViewSlotNotFound,
        get_view_slot,
    )

    try:
        slot = get_view_slot(
            cur.connection, manifest_id=manifest_id, slot_name=slot_name
        )
    except ViewSlotNotFound as exc:
        raise LensPutbackError(
            f"base view manifest has no slot {slot_name!r}: {manifest_id}"
        ) from exc
    if slot.storage_kind != "relation":
        raise LensPutbackError(
            f"lens putback requires a relation slot: slot={slot_name!r}"
        )
    storage_ref = dict(slot.storage_ref or {})
    required = {"schema", "relation"}
    missing = sorted(required - set(storage_ref))
    if missing:
        raise LensPutbackError(
            f"relation slot {slot_name!r} is missing storage_ref keys: "
            f"{missing}"
        )
    return storage_ref


def _load_relation_row(
    cur: psycopg.Cursor[Any],
    *,
    slot_ref: Mapping[str, Any],
    target_generation_id: int,
    scope_key: str,
    row_key: str,
) -> Mapping[str, Any] | None:
    relation_generation = int(
        slot_ref.get("upstream_target_generation_id")
        or slot_ref.get("target_generation_id")
        or target_generation_id
    )
    definition_id = str(
        slot_ref.get("upstream_definition_id")
        or slot_ref.get("definition_id")
        or ""
    )
    if not definition_id:
        raise LensPutbackError("relation slot has no upstream definition id")
    cur.execute(
        sql.SQL(
            """
            SELECT row_data
              FROM {}.{}
             WHERE definition_id = %(definition_id)s
               AND target_generation_id = %(target_generation_id)s
               AND scope_key = %(scope_key)s
               AND row_key = %(row_key)s
            """
        ).format(
            sql.Identifier(str(slot_ref["schema"])),
            sql.Identifier(str(slot_ref["relation"])),
        ),
        {
            "definition_id": definition_id,
            "target_generation_id": relation_generation,
            "scope_key": scope_key,
            "row_key": row_key,
        },
    )
    row = cur.fetchone()
    return None if row is None else dict(row[0] or {})


def _insert_delta(
    cur: psycopg.Cursor[Any],
    *,
    request_id: UUID,
    delta_index: int,
    delta: LensPutbackDelta,
    validation: Mapping[str, Any],
) -> None:
    source_delta = {
        "operation": "replace_value",
        "slot_name": delta.slot_name,
        "scope_key": delta.scope_key,
        "row_key": delta.row_key,
        "field_path": list(delta.field_path),
        "old_value": validation.get("actual_value"),
        "new_value": delta.new_value,
        "source_authority": delta.source_authority,
        "source_record_id": dict(delta.source_record_id),
    }
    cur.execute(
        """
        INSERT INTO okg.lens_putback_deltas (
          request_id, delta_index, slot_name, scope_key, row_key,
          field_path, expected_value, new_value, source_authority,
          source_record_id, expected_source_revision, source_delta,
          validation_status, validation_error, metadata
        ) VALUES (
          %(request_id)s, %(delta_index)s, %(slot_name)s, %(scope_key)s,
          %(row_key)s, %(field_path)s, %(expected_value)s, %(new_value)s,
          %(source_authority)s, %(source_record_id)s,
          %(expected_source_revision)s, %(source_delta)s,
          %(validation_status)s, %(validation_error)s, %(metadata)s
        )
        """,
        {
            "request_id": request_id,
            "delta_index": delta_index,
            "slot_name": delta.slot_name,
            "scope_key": delta.scope_key,
            "row_key": delta.row_key,
            "field_path": list(delta.field_path),
            "expected_value": Jsonb(delta.expected_value),
            "new_value": Jsonb(delta.new_value),
            "source_authority": delta.source_authority,
            "source_record_id": Jsonb(dict(delta.source_record_id)),
            "expected_source_revision": (
                Jsonb(dict(delta.expected_source_revision))
                if delta.expected_source_revision is not None
                else None
            ),
            "source_delta": Jsonb(source_delta),
            "validation_status": validation["status"],
            "validation_error": (
                Jsonb(validation["error"])
                if validation.get("error") is not None
                else None
            ),
            "metadata": Jsonb(dict(delta.metadata)),
        },
    )


def _load_idempotent_request(
    cur: psycopg.Cursor[Any],
    *,
    definition: LensDefinition,
    actor_id: str,
    idempotency_key: str | None,
) -> LensPutbackRequest | None:
    if idempotency_key is None:
        return None
    cur.execute(
        """
        SELECT request_id
          FROM okg.lens_putback_requests
         WHERE lens_definition_id = %(lens_definition_id)s
           AND actor_id = %(actor_id)s
           AND idempotency_key = %(idempotency_key)s
        """,
        {
            "lens_definition_id": definition.definition_id,
            "actor_id": actor_id,
            "idempotency_key": idempotency_key,
        },
    )
    row = cur.fetchone()
    return None if row is None else _load_request_by_id(cur, row[0])


def _load_request_by_id(
    cur: psycopg.Cursor[Any],
    request_id: UUID,
    *,
    for_update: bool = False,
) -> LensPutbackRequest:
    query = """
        SELECT request_id, lens_definition_id, deployment_name, lens_id,
               base_view_definition_id, base_view_manifest_id,
               target_generation_id, actor_id, status, delta_count,
               conflict_count, idempotency_key, error, metadata
          FROM okg.lens_putback_requests
         WHERE request_id = %(request_id)s
    """
    if for_update:
        query += " FOR UPDATE"
    cur.execute(query, {"request_id": request_id})
    row = cur.fetchone()
    if row is None:
        raise LensPutbackError(f"lens putback request not found: {request_id}")
    return LensPutbackRequest(
        request_id=row[0],
        lens_definition_id=row[1],
        deployment_name=row[2],
        lens_id=row[3],
        base_view_definition_id=row[4],
        base_view_manifest_id=row[5],
        target_generation_id=int(row[6]),
        actor_id=row[7],
        status=row[8],
        delta_count=int(row[9]),
        conflict_count=int(row[10]),
        idempotency_key=row[11],
        error=None if row[12] is None else dict(row[12]),
        metadata=dict(row[13] or {}),
    )


def _extract_path(
    row_data: Mapping[str, Any],
    field_path: Sequence[str],
) -> tuple[bool, Any]:
    current: Any = row_data
    for part in field_path:
        if not isinstance(current, Mapping) or part not in current:
            return False, None
        current = current[part]
    return True, current
