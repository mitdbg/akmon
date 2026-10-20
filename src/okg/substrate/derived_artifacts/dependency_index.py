"""Reverse dependency indexes for materialized output lineage."""
from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Mapping, Sequence
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.derived_artifacts.lineage import (
    MaterializedLineageEnvelope,
)
from okg.substrate.derived_artifacts.relations import stable_hash


INDEXED_DEPENDENCY_KINDS = frozenset({
    "component",
    "component_hash",
    "decision",
    "evidence",
    "graph_edge",
    "graph_node",
    "proposal",
    "source_content_hash",
    "source_record",
})


class LineageIndexError(ValueError):
    """Raised when lineage index publication or lookup is invalid."""


@dataclass(frozen=True)
class DependencyLookupKey:
    dependency_kind: str
    dependency_ref: str

    def __post_init__(self) -> None:
        _require_non_empty(self.dependency_kind, "dependency_kind")
        _require_non_empty(self.dependency_ref, "dependency_ref")

    def to_dict(self) -> dict[str, str]:
        return {
            "dependency_kind": self.dependency_kind,
            "dependency_ref": self.dependency_ref,
        }


@dataclass(frozen=True)
class DirtyMaterializedOutput:
    manifest_id: UUID
    definition_id: str
    target_generation_id: int
    output_id: str
    output_kind: str
    invalidation_scope: str
    envelope_hash: str
    output_digest: str


@dataclass(frozen=True)
class DependencyLookupResult:
    status: str
    dirty_outputs: tuple[DirtyMaterializedOutput, ...]
    missing_dependency_kinds: tuple[str, ...]
    queried_keys: tuple[DependencyLookupKey, ...]

    @property
    def available(self) -> bool:
        return self.status == "available"


@dataclass(frozen=True)
class LineageIndexPublishResult:
    manifest_id: UUID
    envelope_count: int
    dependency_key_count: int
    dependency_kinds: tuple[str, ...]
    timings_ms: dict[str, float] | None = None


@dataclass(frozen=True)
class LineageIndexSummary:
    envelope_count: int
    dependency_key_count: int
    output_kind_counts: Mapping[str, int]
    dependency_kind_counts: Mapping[str, int]
    unavailable_dependency_kinds: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "envelope_count": self.envelope_count,
            "dependency_key_count": self.dependency_key_count,
            "output_kind_counts": dict(self.output_kind_counts),
            "dependency_kind_counts": dict(self.dependency_kind_counts),
            "unavailable_dependency_kinds": list(
                self.unavailable_dependency_kinds
            ),
        }


def publish_lineage_envelopes(
    conn: psycopg.Connection[Any],
    *,
    manifest_id: UUID,
    envelopes: Sequence[MaterializedLineageEnvelope],
) -> LineageIndexPublishResult:
    """Persist lineage envelopes and rebuild their reverse index rows."""

    if not envelopes:
        raise LineageIndexError("envelopes must contain at least one envelope")

    timings_ms: dict[str, float] = {}
    total_started = time.perf_counter()

    def add_timing(label: str, started: float) -> None:
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        timings_ms[label] = round(timings_ms.get(label, 0.0) + elapsed, 3)

    with conn.transaction():
        with conn.cursor() as cur:
            step_started = time.perf_counter()
            manifest = _load_completed_manifest(cur, manifest_id)
            add_timing("load_manifest", step_started)
            step_started = time.perf_counter()
            cur.execute(
                """
                DELETE FROM okg.derived_output_lineage_envelopes
                 WHERE manifest_id = %(manifest_id)s
                """,
                {"manifest_id": manifest_id},
            )
            add_timing("delete_existing", step_started)

            dependency_key_count = 0
            dependency_kinds: set[str] = set()
            # `unify-dirty-set-authority` / perf: accumulate every reverse-index
            # row and write them in ONE batched INSERT (jsonb_to_recordset)
            # rather than one round-trip per dependency key. The index is now
            # read on every publish cycle (view carry-forward) and written on
            # every rebuild, so the prior per-key cur.execute loop (measured
            # ~2s / 6004 rows for a view citing 1000 nodes) dominated the
            # rebuild path. (lineage_envelope_id, dependency_kind,
            # dependency_ref) is unique within the batch: envelope ids are
            # freshly inserted for this manifest and dependency_keys() dedups
            # refs per kind.
            index_rows: list[dict[str, Any]] = []
            for envelope in sorted(envelopes, key=lambda item: item.output_id):
                step_started = time.perf_counter()
                envelope_id = _insert_envelope(
                    cur,
                    manifest=manifest,
                    manifest_id=manifest_id,
                    envelope=envelope,
                )
                add_timing("insert_envelope", step_started)
                step_started = time.perf_counter()
                keys = envelope.dependency_keys()
                for dependency_kind, dependency_refs in sorted(keys.items()):
                    _require_known_dependency_kind(dependency_kind)
                    dependency_kinds.add(dependency_kind)
                    for dependency_ref in dependency_refs:
                        dependency_key_count += 1
                        index_rows.append({
                            "lineage_envelope_id": str(envelope_id),
                            "manifest_id": str(manifest_id),
                            "output_id": envelope.output_id,
                            "dependency_kind": dependency_kind,
                            "dependency_ref": dependency_ref,
                            "dependency_key_hash": stable_hash({
                                "dependency_kind": dependency_kind,
                                "dependency_ref": dependency_ref,
                            }),
                        })
                add_timing("build_index_rows", step_started)
            if index_rows:
                step_started = time.perf_counter()
                cur.execute(
                    """
                    INSERT INTO okg.derived_output_dependency_index (
                      lineage_envelope_id, manifest_id, output_id,
                      dependency_kind, dependency_ref, dependency_key_hash
                    )
                    SELECT lineage_envelope_id, manifest_id, output_id,
                           dependency_kind, dependency_ref, dependency_key_hash
                      FROM jsonb_to_recordset(%(rows)s) AS x(
                        lineage_envelope_id uuid,
                        manifest_id uuid,
                        output_id text,
                        dependency_kind text,
                        dependency_ref text,
                        dependency_key_hash text
                      )
                    """,
                    {"rows": Jsonb(index_rows)},
                )
                add_timing("insert_index_rows", step_started)
    add_timing("total", total_started)

    return LineageIndexPublishResult(
        manifest_id=manifest_id,
        envelope_count=len(envelopes),
        dependency_key_count=dependency_key_count,
        dependency_kinds=tuple(sorted(dependency_kinds)),
        timings_ms=timings_ms,
    )


def lookup_dirty_outputs(
    conn: psycopg.Connection[Any],
    *,
    dependency_keys: Sequence[DependencyLookupKey],
    target_generation_id: int | None = None,
    output_kind: str | None = None,
) -> DependencyLookupResult:
    """Return materialized outputs reachable from dependency keys.

    A missing dependency kind is reported as ``unavailable`` instead of
    quietly returning an empty dirty set. Missing refs for a covered kind
    are an available empty result.
    """

    keys = tuple(dependency_keys)
    if not keys:
        return DependencyLookupResult(
            status="available",
            dirty_outputs=(),
            missing_dependency_kinds=(),
            queried_keys=(),
        )
    requested_kinds = {key.dependency_kind for key in keys}
    unknown_kinds = requested_kinds - INDEXED_DEPENDENCY_KINDS

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dependency_kind
              FROM okg.derived_output_dependency_index
             WHERE dependency_kind = ANY(%(dependency_kinds)s)
             GROUP BY dependency_kind
            """,
            {"dependency_kinds": list(requested_kinds)},
        )
        covered_kinds = {row[0] for row in cur.fetchall()}
        missing_kinds = tuple(sorted(
            unknown_kinds | (requested_kinds - covered_kinds)
        ))

        cur.execute(
            """
            WITH requested AS (
              SELECT dependency_kind, dependency_ref
                FROM jsonb_to_recordset(%(dependency_keys)s) AS item(
                  dependency_kind TEXT,
                  dependency_ref TEXT
                )
            )
            SELECT DISTINCT
                   e.manifest_id,
                   e.definition_id,
                   e.target_generation_id,
                   e.output_id,
                   e.output_kind,
                   e.invalidation_scope,
                   e.envelope_hash,
                   e.output_digest
              FROM requested r
              JOIN okg.derived_output_dependency_index i
                ON i.dependency_kind = r.dependency_kind
               AND i.dependency_ref = r.dependency_ref
              JOIN okg.derived_output_lineage_envelopes e
                ON e.lineage_envelope_id = i.lineage_envelope_id
             WHERE (%(target_generation_id)s::bigint IS NULL
                    OR e.target_generation_id =
                       %(target_generation_id)s::bigint)
               AND (%(output_kind)s::text IS NULL
                    OR e.output_kind = %(output_kind)s::text)
             ORDER BY e.definition_id, e.output_id
            """,
            {
                "dependency_keys": Jsonb([key.to_dict() for key in keys]),
                "target_generation_id": target_generation_id,
                "output_kind": output_kind,
            },
        )
        dirty_outputs = tuple(
            DirtyMaterializedOutput(
                manifest_id=row[0],
                definition_id=row[1],
                target_generation_id=row[2],
                output_id=row[3],
                output_kind=row[4],
                invalidation_scope=row[5],
                envelope_hash=row[6],
                output_digest=row[7],
            )
            for row in cur.fetchall()
        )

    return DependencyLookupResult(
        status="unavailable" if missing_kinds else "available",
        dirty_outputs=dirty_outputs,
        missing_dependency_kinds=missing_kinds,
        queried_keys=keys,
    )


def summarize_lineage_index(
    conn: psycopg.Connection[Any],
    *,
    target_generation_id: int | None = None,
    deployment_name: str | None = None,
) -> LineageIndexSummary:
    """Return aggregate coverage for lineage envelopes and reverse keys."""

    params = {
        "deployment_name": deployment_name,
        "target_generation_id": target_generation_id,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
              FROM okg.derived_output_lineage_envelopes e
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = e.definition_id
             WHERE (%(deployment_name)s::text IS NULL
                    OR d.deployment_name = %(deployment_name)s::text)
               AND (%(target_generation_id)s::bigint IS NULL
                    OR e.target_generation_id =
                       %(target_generation_id)s::bigint)
            """,
            params,
        )
        envelope_count = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT output_kind, count(*)
              FROM okg.derived_output_lineage_envelopes e
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = e.definition_id
             WHERE (%(deployment_name)s::text IS NULL
                    OR d.deployment_name = %(deployment_name)s::text)
               AND (%(target_generation_id)s::bigint IS NULL
                    OR e.target_generation_id =
                       %(target_generation_id)s::bigint)
             GROUP BY output_kind
             ORDER BY output_kind
            """,
            params,
        )
        output_kind_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

        cur.execute(
            """
            SELECT i.dependency_kind, count(*)
              FROM okg.derived_output_dependency_index i
              JOIN okg.derived_output_lineage_envelopes e
                ON e.lineage_envelope_id = i.lineage_envelope_id
              JOIN okg.derived_artifact_definitions d
                ON d.definition_id = e.definition_id
             WHERE (%(deployment_name)s::text IS NULL
                    OR d.deployment_name = %(deployment_name)s::text)
               AND (%(target_generation_id)s::bigint IS NULL
                    OR e.target_generation_id =
                       %(target_generation_id)s::bigint)
             GROUP BY i.dependency_kind
             ORDER BY i.dependency_kind
            """,
            params,
        )
        dependency_kind_counts = {
            row[0]: int(row[1]) for row in cur.fetchall()
        }

    return LineageIndexSummary(
        envelope_count=envelope_count,
        dependency_key_count=sum(dependency_kind_counts.values()),
        output_kind_counts=output_kind_counts,
        dependency_kind_counts=dependency_kind_counts,
        unavailable_dependency_kinds=tuple(sorted(
            INDEXED_DEPENDENCY_KINDS - set(dependency_kind_counts)
        )),
    )


def _load_completed_manifest(
    cur: psycopg.Cursor[Any],
    manifest_id: UUID,
) -> dict[str, Any]:
    cur.execute(
        """
        SELECT manifest_id, definition_id, target_generation_id, status
          FROM okg.derived_output_manifests
         WHERE manifest_id = %(manifest_id)s
        """,
        {"manifest_id": manifest_id},
    )
    row = cur.fetchone()
    if row is None:
        raise LineageIndexError(
            "manifest_id must reference an existing derived output manifest"
        )
    if row[3] != "completed":
        raise LineageIndexError(
            "manifest_id must reference a completed derived output manifest"
        )
    return {
        "manifest_id": row[0],
        "definition_id": row[1],
        "target_generation_id": int(row[2]),
    }


def _insert_envelope(
    cur: psycopg.Cursor[Any],
    *,
    manifest: Mapping[str, Any],
    manifest_id: UUID,
    envelope: MaterializedLineageEnvelope,
) -> UUID:
    encoded = envelope.to_dict()
    cur.execute(
        """
        INSERT INTO okg.derived_output_lineage_envelopes (
          manifest_id, definition_id, target_generation_id, output_id,
          output_kind, lineage_schema, envelope_hash, output_digest,
          previous_output_digest, component_id, component_hash,
          invalidation_scope, envelope
        ) VALUES (
          %(manifest_id)s, %(definition_id)s, %(target_generation_id)s,
          %(output_id)s, %(output_kind)s, %(lineage_schema)s,
          %(envelope_hash)s, %(output_digest)s,
          %(previous_output_digest)s, %(component_id)s,
          %(component_hash)s, %(invalidation_scope)s, %(envelope)s
        )
        ON CONFLICT (manifest_id, output_id) DO UPDATE SET
          definition_id = EXCLUDED.definition_id,
          target_generation_id = EXCLUDED.target_generation_id,
          output_kind = EXCLUDED.output_kind,
          lineage_schema = EXCLUDED.lineage_schema,
          envelope_hash = EXCLUDED.envelope_hash,
          output_digest = EXCLUDED.output_digest,
          previous_output_digest = EXCLUDED.previous_output_digest,
          component_id = EXCLUDED.component_id,
          component_hash = EXCLUDED.component_hash,
          invalidation_scope = EXCLUDED.invalidation_scope,
          envelope = EXCLUDED.envelope
        RETURNING lineage_envelope_id
        """,
        {
            "manifest_id": manifest_id,
            "definition_id": manifest["definition_id"],
            "target_generation_id": manifest["target_generation_id"],
            "output_id": envelope.output_id,
            "output_kind": envelope.output_kind,
            "lineage_schema": encoded["schema"],
            "envelope_hash": envelope.envelope_hash,
            "output_digest": envelope.output_digest,
            "previous_output_digest": envelope.previous_output_digest,
            "component_id": envelope.component.component_id,
            "component_hash": envelope.component.component_hash,
            "invalidation_scope": envelope.invalidation_scope,
            "envelope": Jsonb(encoded),
        },
    )
    return cur.fetchone()[0]


def _require_known_dependency_kind(dependency_kind: str) -> None:
    if dependency_kind not in INDEXED_DEPENDENCY_KINDS:
        raise LineageIndexError(
            "dependency_kind must be one of "
            f"{sorted(INDEXED_DEPENDENCY_KINDS)}; got {dependency_kind!r}"
        )


def _require_non_empty(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise LineageIndexError(f"{field_name} is required")


__all__ = [
    "INDEXED_DEPENDENCY_KINDS",
    "DependencyLookupKey",
    "DependencyLookupResult",
    "DirtyMaterializedOutput",
    "LineageIndexError",
    "LineageIndexPublishResult",
    "LineageIndexSummary",
    "lookup_dirty_outputs",
    "publish_lineage_envelopes",
    "summarize_lineage_index",
]
