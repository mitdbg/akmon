"""Internal attribute observation state for AKMON effective attrs.

Raw graph attrs remain source-owned in ``okg.graph_nodes`` /
``okg.graph_edges``. This module stores derived/reviewer/proposal
observations separately and materializes accepted observations into a
per-generation effective-attrs snapshot. Default reads overlay only
the immutable snapshot for the caller's pin; mutable observation state
is reserved for provenance, supersession, and future materialization.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.images.values import check_incoming
from okg.substrate.nomos.read_masking import ReadRedactedAttrs
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR


logger = logging.getLogger(__name__)

TargetKind = Literal["node", "edge"]


def _first_column(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


ATTRIBUTE_OBSERVATION_TABLE = "okg.attribute_observations"
ATTRIBUTE_EFFECTIVE_TABLE = "okg.attribute_effective_attrs"
DEFAULT_NAMESPACE = "default"
CURRENT_STATUSES = frozenset({"accepted"})
TERMINAL_STATUSES = frozenset({"rejected", "superseded", "stale"})
ALL_STATUSES = frozenset({"candidate", *CURRENT_STATUSES, *TERMINAL_STATUSES})
RESERVED_EFFECTIVE_ATTR_KEYS = frozenset({LINEAGE_ATTR})
# PACT lane-s-storage-redesign-s2 (design section 5): one marker row per
# generation and namespace whose overlays were materialized, written even when
# the generation has none, so a pinned read can tell "no overlays" from "never
# materialized"; and the same overlay rows as validity ranges over branch
# ordinals, so their size grows with overlay changes, not with publishes. S2
# writes both and still copies the per-generation table forward (Nomos reads
# it); S3 reads the ranges.
OVERLAY_GENERATION_MARKER_TABLE = "okg.attribute_overlay_generations"
OVERLAY_RANGES_TABLE = "okg.attribute_overlay_ranges"
OVERLAY_RANGE_OPEN_ORD = (1 << 63) - 1


class AttributeConflictError(SubstrateError):
    """Raised when more than one current accepted overlay would win."""


@dataclass(frozen=True)
class AttributeObservation:
    observation_id: str
    target_kind: TargetKind
    target_id: str
    generation_id: str
    namespace: str
    attr_key: str
    value: Any
    value_hash: str
    status: str
    producer: str
    provenance: dict[str, Any]
    dependencies: dict[str, Any]
    dependency_fingerprint: str | None
    supersedes: str | None
    superseded_by: str | None
    decision: dict[str, Any]
    confidence: float | None
    observed_at: Any
    updated_at: Any

    def as_dict(self) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id,
            "target_kind": self.target_kind,
            "target_id": self.target_id,
            "generation_id": self.generation_id,
            "namespace": self.namespace,
            "attr_key": self.attr_key,
            "value": self.value,
            "value_hash": self.value_hash,
            "status": self.status,
            "producer": self.producer,
            "provenance": self.provenance,
            "dependencies": self.dependencies,
            "dependency_fingerprint": self.dependency_fingerprint,
            "supersedes": self.supersedes,
            "superseded_by": self.superseded_by,
            "decision": self.decision,
            "confidence": self.confidence,
            "observed_at": self.observed_at,
            "updated_at": self.updated_at,
        }


# --------------------------------------------------------------------------- #
# schema-interval-narrow-split: the ONE in-database hash site (req.sigma-pinned)
#
# ``okg.jsonb_canonical_text`` renders a stored jsonb value deterministically:
# object keys sorted under COLLATE "C" (jsonb storage already dedupes keys),
# numbers normalized via trim_scale so jsonb-EQUAL numerics that differ only in
# scale ("1.0" vs "1.00") render identically, strings/booleans/null via the
# stored value's canonical jsonb text. ``okg.attribute_content_hash`` is the
# single content-hash site: sha256 over canonical(attrs) + a presence marker +
# canonical(raw_attrs). raw_attrs NULL (presence-means-overlay: "raw == attrs")
# hashes differently from any non-NULL raw_attrs, so overlay identity is part
# of the content tuple. Defined as plain strings (NOT f-strings) because the
# bodies contain literal braces; the SAME text is replayed by migration
# 0020_interval_narrow_split.sql so a migrated deployment converges
# byte-identically with a fresh install (the convergence lane compares prosrc).
# --------------------------------------------------------------------------- #
_JSONB_CANONICAL_TEXT_FN_SQL = """
CREATE OR REPLACE FUNCTION okg.jsonb_canonical_text(j jsonb)
RETURNS text
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $fn$
DECLARE
  result text;
BEGIN
  -- Scalar leaves are rendered inline (jsonb's own text output is canonical
  -- for strings/booleans/null; numbers are trim_scale-normalized so
  -- jsonb-EQUAL numerics of different scale render identically); recursion
  -- happens only for nested containers. Keys sort under COLLATE "C" for a
  -- collation-independent rendering.
  CASE jsonb_typeof(j)
    WHEN 'object' THEN
      SELECT '{' || COALESCE(string_agg(
               to_jsonb(e.key)::text || ':' ||
               CASE jsonb_typeof(e.value)
                 WHEN 'object' THEN okg.jsonb_canonical_text(e.value)
                 WHEN 'array' THEN okg.jsonb_canonical_text(e.value)
                 WHEN 'number' THEN
                   trim_scale((e.value #>> '{}')::numeric)::text
                 ELSE e.value::text
               END,
               ',' ORDER BY e.key COLLATE "C"), '') || '}'
        INTO result
        FROM jsonb_each(j) AS e(key, value);
    WHEN 'array' THEN
      SELECT '[' || COALESCE(string_agg(
               CASE jsonb_typeof(a.value)
                 WHEN 'object' THEN okg.jsonb_canonical_text(a.value)
                 WHEN 'array' THEN okg.jsonb_canonical_text(a.value)
                 WHEN 'number' THEN
                   trim_scale((a.value #>> '{}')::numeric)::text
                 ELSE a.value::text
               END, ',' ORDER BY a.ord), '') || ']'
        INTO result
        FROM jsonb_array_elements(j) WITH ORDINALITY AS a(value, ord);
    WHEN 'number' THEN
      result := trim_scale((j #>> '{}')::numeric)::text;
    ELSE
      result := j::text;
  END CASE;
  RETURN result;
END
$fn$
"""

_ATTRIBUTE_CONTENT_HASH_FN_SQL = """
CREATE OR REPLACE FUNCTION okg.attribute_content_hash(attrs jsonb, raw_attrs jsonb)
RETURNS bytea
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $fn$
  SELECT sha256(convert_to(
    okg.jsonb_canonical_text(attrs)
    || E'\\x1f'
    || CASE WHEN raw_attrs IS NULL THEN E'\\x1e'
            ELSE okg.jsonb_canonical_text(raw_attrs) END,
    'UTF8'))
$fn$
"""


_SCHEMA_STATEMENTS: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS okg",
    _JSONB_CANONICAL_TEXT_FN_SQL,
    _ATTRIBUTE_CONTENT_HASH_FN_SQL,
    f"""
    CREATE TABLE IF NOT EXISTS {ATTRIBUTE_OBSERVATION_TABLE} (
      observation_id TEXT PRIMARY KEY,
      target_kind TEXT NOT NULL
        CHECK (target_kind IN ('node', 'edge')),
      target_id TEXT NOT NULL,
      generation_id TEXT NOT NULL,
      namespace TEXT NOT NULL DEFAULT 'default',
      attr_key TEXT NOT NULL,
      value JSONB NOT NULL,
      value_hash TEXT NOT NULL,
      status TEXT NOT NULL
        CHECK (status IN ('candidate', 'accepted', 'rejected',
                          'superseded', 'stale')),
      producer TEXT NOT NULL,
      provenance JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      dependencies JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      dependency_fingerprint TEXT,
      supersedes TEXT,
      superseded_by TEXT,
      decision JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      confidence DOUBLE PRECISION,
      observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    f"""
    CREATE UNIQUE INDEX IF NOT EXISTS
      idx_attribute_observations_current_unique
      ON {ATTRIBUTE_OBSERVATION_TABLE}
      (target_kind, target_id, generation_id, namespace, attr_key)
      WHERE status = 'accepted' AND superseded_by IS NULL
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_attribute_observations_target
      ON {ATTRIBUTE_OBSERVATION_TABLE}
      (target_kind, target_id, generation_id, namespace, attr_key, status)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_attribute_observations_dependency
      ON {ATTRIBUTE_OBSERVATION_TABLE}
      (dependency_fingerprint)
      WHERE dependency_fingerprint IS NOT NULL
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {ATTRIBUTE_EFFECTIVE_TABLE} (
      target_kind TEXT NOT NULL
        CHECK (target_kind IN ('node', 'edge')),
      target_id TEXT NOT NULL,
      generation_id TEXT NOT NULL,
      namespace TEXT NOT NULL DEFAULT 'default',
      attr_key TEXT NOT NULL,
      observation_id TEXT NOT NULL,
      value JSONB NOT NULL,
      value_hash TEXT NOT NULL,
      producer TEXT NOT NULL,
      provenance JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      dependencies JSONB NOT NULL DEFAULT '{{}}'::jsonb,
      dependency_fingerprint TEXT,
      confidence DOUBLE PRECISION,
      materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (
        target_kind, target_id, generation_id, namespace, attr_key
      )
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_attribute_effective_attrs_target
      ON {ATTRIBUTE_EFFECTIVE_TABLE}
      (target_kind, target_id, generation_id, namespace)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_attribute_effective_attrs_observation
      ON {ATTRIBUTE_EFFECTIVE_TABLE}
      (observation_id)
    """,
    # PACT lane-s-storage-redesign-s2: the overlay marker and ranges. The same
    # DDL is in db/schema.sql and migration 0044.
    """
    CREATE TABLE IF NOT EXISTS okg.attribute_overlay_generations (
  generation_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  previous_generation_id TEXT,
  overlay_rows BIGINT NOT NULL,
  chronos_branch_id TEXT,
  ordinal BIGINT,
  ranges_synced BOOLEAN NOT NULL DEFAULT FALSE,
  source TEXT NOT NULL CHECK (source IN ('materializer', 'backfill')),
  materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (generation_id, namespace)
)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_attribute_overlay_generations_branch
  ON okg.attribute_overlay_generations (chronos_branch_id, ordinal)
    """,
    """
    CREATE TABLE IF NOT EXISTS okg.attribute_overlay_ranges (
  chronos_branch_id TEXT NOT NULL,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  attr_key TEXT NOT NULL,
  from_ord BIGINT NOT NULL,
  to_ord BIGINT NOT NULL,
  observation_id TEXT NOT NULL,
  value JSONB NOT NULL,
  value_hash TEXT NOT NULL,
  PRIMARY KEY (chronos_branch_id, target_kind, target_id, namespace, attr_key, from_ord),
  CHECK (from_ord < to_ord)
)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_attribute_overlay_ranges_key
  ON okg.attribute_overlay_ranges
  (chronos_branch_id, target_kind, namespace, attr_key, from_ord, target_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_attribute_overlay_ranges_open
  ON okg.attribute_overlay_ranges (chronos_branch_id, namespace)
  WHERE to_ord = 9223372036854775807
    """,
)


# The observation, overlay, marker and range tables are granted SELECT to the
# constrained okg_mcp read pool by the migration allow-list (db/views.sql).
# Re-apply the grant whenever the schema is (re)created, so a recreated table
# never leaves the MCP readers on "permission denied". Role-guarded, per-table
# best-effort (a deployment with no okg_mcp role, or a creator lacking GRANT
# authority, is a no-op, never an error). ensure_attribute_schema runs only on
# provision and migrate (never per publish), so this adds no grant churn.
_OKG_MCP_ATTRIBUTE_GRANTS = f"""
DO $okg_mcp_attribute_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    RETURN;
  END IF;
  BEGIN
    GRANT SELECT ON TABLE {ATTRIBUTE_OBSERVATION_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE {ATTRIBUTE_EFFECTIVE_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE {OVERLAY_GENERATION_MARKER_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE {OVERLAY_RANGES_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_attribute_grants$
"""


def ensure_attribute_schema(conn: psycopg.Connection) -> None:
    """Create the internal attribute observation and overlay tables."""
    with conn.cursor() as cur:
        for statement in _SCHEMA_STATEMENTS:
            cur.execute(statement)
        cur.execute(_OKG_MCP_ATTRIBUTE_GRANTS)


def ensure_attribute_schema_db(db: Any) -> None:
    """Create the internal tables through the Chronos DB adapter."""
    for statement in _SCHEMA_STATEMENTS:
        db.execute(statement)
    db.execute(_OKG_MCP_ATTRIBUTE_GRANTS)


def stable_json_dumps(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def attribute_value_hash(value: Any) -> str:
    payload = stable_json_dumps(value).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def edge_target_id(edge_id: int | str) -> str:
    return str(edge_id)


def _normalize_target_kind(target_kind: str) -> TargetKind:
    if target_kind not in {"node", "edge"}:
        raise ValueError("target_kind must be 'node' or 'edge'")
    return target_kind  # type: ignore[return-value]


def _row_to_observation(row: Any) -> AttributeObservation:
    return AttributeObservation(
        observation_id=row["observation_id"],
        target_kind=_normalize_target_kind(row["target_kind"]),
        target_id=row["target_id"],
        generation_id=row["generation_id"],
        namespace=row["namespace"],
        attr_key=row["attr_key"],
        value=row["value"],
        value_hash=row["value_hash"],
        status=row["status"],
        producer=row["producer"],
        provenance=dict(row["provenance"] or {}),
        dependencies=dict(row["dependencies"] or {}),
        dependency_fingerprint=row["dependency_fingerprint"],
        supersedes=row["supersedes"],
        superseded_by=row["superseded_by"],
        decision=dict(row["decision"] or {}),
        confidence=row["confidence"],
        observed_at=row["observed_at"],
        updated_at=row["updated_at"],
    )


# Process-level cache of tables KNOWN to exist, keyed by (db, host, table). A
# cached table is assumed never dropped mid-process, so a positive result is
# stable and the to_regclass round-trip can be skipped on repeat checks. Negative
# results are NOT cached — a table may be created later (e.g. on first publish).
_TABLE_EXISTS_CACHE: set[tuple[str, str, str]] = set()


def _table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    info = conn.info
    key = (info.dbname or "", str(info.host or ""), table_name)
    if key in _TABLE_EXISTS_CACHE:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        exists = cur.fetchone()[0] is not None
    if exists:
        _TABLE_EXISTS_CACHE.add(key)
    else:
        # An authoritative miss self-corrects any stale positive: a table cached
        # as existing was dropped out-of-band, so evict the entry.
        _TABLE_EXISTS_CACHE.discard(key)
    return exists


def _schema_installed(conn: psycopg.Connection) -> bool:
    return _table_exists(conn, ATTRIBUTE_OBSERVATION_TABLE) and _table_exists(
        conn, ATTRIBUTE_EFFECTIVE_TABLE,
    )


def _require_attribute_schema(conn: psycopg.Connection) -> None:
    if not _schema_installed(conn):
        raise SubstrateError(
            "attribute schema is not installed",
            fix_hint=(
                "run the substrate schema migration or ChronosGraphStore."
                "ensure_schema before writing attribute observations"
            ),
        )


def _attribute_materialization_state_empty(conn: psycopg.Connection) -> bool:
    """Return True when no attribute snapshot work is possible."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT NOT EXISTS (
                     SELECT 1 FROM {ATTRIBUTE_OBSERVATION_TABLE} LIMIT 1
                   )
               AND NOT EXISTS (
                     SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE} LIMIT 1
                   )
            """,
        )
        row = cur.fetchone()
    return bool(row and row[0])



def _current_observation(
    conn: psycopg.Connection,
    *,
    target_kind: TargetKind,
    target_id: str,
    generation_id: str,
    namespace: str,
    attr_key: str,
) -> AttributeObservation | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT *
              FROM {ATTRIBUTE_OBSERVATION_TABLE}
             WHERE target_kind = %s
               AND target_id = %s
               AND generation_id = %s
               AND namespace = %s
               AND attr_key = %s
               AND status = 'accepted'
               AND superseded_by IS NULL
             ORDER BY observed_at DESC, observation_id DESC
            """,
            (target_kind, target_id, generation_id, namespace, attr_key),
        )
        rows = cur.fetchall()
    if len(rows) > 1:
        raise AttributeConflictError(
            "multiple current accepted attribute observations",
            data={
                "target_kind": target_kind,
                "target_id": target_id,
                "generation_id": generation_id,
                "namespace": namespace,
                "attr_key": attr_key,
                "observation_ids": [r["observation_id"] for r in rows],
            },
            fix_hint="supersede or reject all but one accepted observation",
        )
    return _row_to_observation(rows[0]) if rows else None


def _observation_by_id(
    conn: psycopg.Connection,
    observation_id: str,
) -> AttributeObservation | None:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT *
              FROM {ATTRIBUTE_OBSERVATION_TABLE}
             WHERE observation_id = %s
            """,
            (observation_id,),
        )
        row = cur.fetchone()
    return _row_to_observation(row) if row else None


def record_attribute_observation(
    conn: psycopg.Connection,
    *,
    target_kind: str,
    target_id: int | str,
    generation_id: int | str,
    attr_key: str,
    value: Any,
    namespace: str = DEFAULT_NAMESPACE,
    status: str = "accepted",
    producer: str,
    provenance: dict[str, Any] | None = None,
    dependencies: dict[str, Any] | None = None,
    dependency_fingerprint: str | None = None,
    supersedes: str | None = None,
    decision: dict[str, Any] | None = None,
    confidence: float | None = None,
    observation_id: str | None = None,
) -> AttributeObservation:
    """Append an attribute observation.

    Accepted writes supersede the prior accepted row for the same
    target/generation/namespace/key. The raw graph row is never mutated,
    and existing generation snapshots are not updated until an explicit
    materialization step for a new generation.
    """
    _require_attribute_schema(conn)
    kind = _normalize_target_kind(str(target_kind))
    target = str(target_id)
    generation = str(generation_id)
    if not attr_key:
        raise ValueError("attr_key must be non-empty")
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if status not in ALL_STATUSES:
        raise ValueError(f"unsupported attribute status: {status!r}")
    if not producer:
        raise ValueError("producer must be non-empty")
    # Image values (#3046): an accepted observation is merged over the
    # node's attrs on read, so its value is checked as that attr would be.
    # Only okg's ingest image step adds `png` or `error`.
    check_incoming({attr_key: value})

    obs_id = observation_id or f"attr:{uuid.uuid4().hex}"
    value_hash = attribute_value_hash(value)
    with conn.transaction():
        old: AttributeObservation | None = None
        if status == "accepted":
            old = _current_observation(
                conn,
                target_kind=kind,
                target_id=target,
                generation_id=generation,
                namespace=namespace,
                attr_key=attr_key,
            )
            if old is not None and supersedes is None:
                supersedes = old.observation_id
        superseded_observation = (
            old
            if old is not None and old.observation_id == supersedes
            else (
                _observation_by_id(conn, supersedes)
                if supersedes is not None
                else None
            )
        )
        supersede_ids: list[str] = []
        if old is not None:
            supersede_ids.append(old.observation_id)
        if (
            superseded_observation is not None
            and superseded_observation.status == "accepted"
            and superseded_observation.observation_id not in supersede_ids
        ):
            supersede_ids.append(superseded_observation.observation_id)
        with conn.cursor(row_factory=dict_row) as cur:
            for supersede_id in supersede_ids:
                cur.execute(
                    f"""
                    UPDATE {ATTRIBUTE_OBSERVATION_TABLE}
                       SET status = 'superseded',
                           superseded_by = %s,
                           decision = decision || %s::jsonb,
                           updated_at = now()
                     WHERE observation_id = %s
                    """,
                    (
                        obs_id,
                        Jsonb({
                            "superseded_by": obs_id,
                            "reason": "accepted_replacement",
                        }),
                        supersede_id,
                    ),
                )
            insert_decision = dict(decision or {})
            if superseded_observation is not None:
                insert_decision.setdefault(
                    "diff",
                    {
                        "old": superseded_observation.value,
                        "new": value,
                        "old_value_hash": superseded_observation.value_hash,
                        "new_value_hash": value_hash,
                    },
                )
            cur.execute(
                f"""
                INSERT INTO {ATTRIBUTE_OBSERVATION_TABLE}
                  (observation_id, target_kind, target_id, generation_id,
                   namespace, attr_key, value, value_hash, status, producer,
                   provenance, dependencies, dependency_fingerprint,
                   supersedes, decision, confidence)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                   %s::jsonb, %s::jsonb, %s, %s, %s::jsonb, %s)
                RETURNING *
                """,
                (
                    obs_id,
                    kind,
                    target,
                    generation,
                    namespace,
                    attr_key,
                    Jsonb(value),
                    value_hash,
                    status,
                    producer,
                    Jsonb(provenance or {}),
                    Jsonb(dependencies or {}),
                    dependency_fingerprint,
                    supersedes,
                    Jsonb(insert_decision),
                    confidence,
                ),
            )
            return _row_to_observation(cur.fetchone())


def mark_attribute_stale(
    conn: psycopg.Connection,
    observation_id: str,
    *,
    reason: str,
    decision: dict[str, Any] | None = None,
) -> AttributeObservation:
    _require_attribute_schema(conn)
    payload = {"reason": reason, **(decision or {})}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            UPDATE {ATTRIBUTE_OBSERVATION_TABLE}
               SET status = 'stale',
                   decision = decision || %s::jsonb,
                   updated_at = now()
             WHERE observation_id = %s
             RETURNING *
            """,
            (Jsonb(payload), observation_id),
        )
        row = cur.fetchone()
    if row is None:
        raise KeyError(f"attribute observation {observation_id!r} not found")
    return _row_to_observation(row)


def reject_attribute_observation(
    conn: psycopg.Connection,
    observation_id: str,
    *,
    reason: str,
    decision: dict[str, Any] | None = None,
) -> AttributeObservation:
    _require_attribute_schema(conn)
    payload = {"reason": reason, **(decision or {})}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            UPDATE {ATTRIBUTE_OBSERVATION_TABLE}
               SET status = 'rejected',
                   decision = decision || %s::jsonb,
                   updated_at = now()
             WHERE observation_id = %s
             RETURNING *
            """,
            (Jsonb(payload), observation_id),
        )
        row = cur.fetchone()
    if row is None:
        raise KeyError(f"attribute observation {observation_id!r} not found")
    return _row_to_observation(row)


def effective_attrs_for_target(
    conn: psycopg.Connection,
    *,
    target_kind: str,
    target_id: int | str,
    generation_id: int | str,
    raw_attrs: dict[str, Any] | None,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """Return flat source attrs plus materialized effective attrs for the pin."""
    read_redacted = isinstance(raw_attrs, ReadRedactedAttrs)
    attrs = dict(raw_attrs or {})
    # Only an in-memory read-filter result is terminal. The persisted
    # nomos_redacted attribute also marks at-rest sanitization and must not
    # suppress overlays before a later policy decision.
    if read_redacted:
        return attrs
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        return attrs
    rows = _current_overlays(
        conn,
        target_kind=_normalize_target_kind(str(target_kind)),
        target_ids=[str(target_id)],
        generation_id=str(generation_id),
        namespace=namespace,
    )
    for row in rows.get(str(target_id), []):
        key = row["attr_key"]
        if key in RESERVED_EFFECTIVE_ATTR_KEYS:
            continue
        if key in attrs and attrs[key] != row["value"]:
            # Accepted overlays intentionally take precedence over raw
            # values, but the provenance path keeps the observation audit.
            pass
        attrs[key] = row["value"]
    return attrs


def apply_effective_attrs_to_rows(
    conn: psycopg.Connection,
    rows: Iterable[dict[str, Any]],
    *,
    target_kind: str,
    target_id_field: str,
    generation_id: int | str,
    namespace: str = DEFAULT_NAMESPACE,
) -> list[dict[str, Any]]:
    """Batch-project effective attrs onto rows that carry ``attrs``."""
    materialized = [dict(row) for row in rows]
    if not materialized or not _table_exists(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        return materialized
    ids = [str(row[target_id_field]) for row in materialized]
    overlays = _current_overlays(
        conn,
        target_kind=_normalize_target_kind(str(target_kind)),
        target_ids=ids,
        generation_id=str(generation_id),
        namespace=namespace,
    )
    for row in materialized:
        raw_attrs = row.get("attrs")
        attrs = dict(raw_attrs or {})
        # Preserve only a trusted in-memory terminal result when a downstream
        # MCP path performs its conventional overlay pass after Nomos.
        if isinstance(raw_attrs, ReadRedactedAttrs):
            row["attrs"] = attrs
            continue
        for overlay in overlays.get(str(row[target_id_field]), []):
            if overlay["attr_key"] in RESERVED_EFFECTIVE_ATTR_KEYS:
                continue
            attrs[overlay["attr_key"]] = overlay["value"]
        row["attrs"] = attrs
    return materialized


def _current_overlays(
    conn: psycopg.Connection,
    *,
    target_kind: TargetKind,
    target_ids: list[str],
    generation_id: str,
    namespace: str,
) -> dict[str, list[dict[str, Any]]]:
    if not target_ids:
        return {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT target_id, namespace, attr_key, value, observation_id
              FROM {ATTRIBUTE_EFFECTIVE_TABLE}
             WHERE target_kind = %s
               AND target_id = ANY(%s)
               AND generation_id = %s
               AND namespace = %s
             ORDER BY target_id, namespace, attr_key, observation_id DESC
            """,
            (target_kind, target_ids, generation_id, namespace),
        )
        rows = [dict(row) for row in cur.fetchall()]
    by_target_key: dict[tuple[str, str, str], list[str]] = {}
    for row in rows:
        key = (row["target_id"], row["namespace"], row["attr_key"])
        by_target_key.setdefault(key, []).append(row["observation_id"])
    conflicts = {
        key: observation_ids
        for key, observation_ids in by_target_key.items()
        if len(observation_ids) > 1
    }
    if conflicts:
        raise AttributeConflictError(
            "multiple current accepted attribute observations",
            data={
                "generation_id": generation_id,
                "target_kind": target_kind,
                "conflicts": {
                    "|".join(key): ids for key, ids in conflicts.items()
                },
            },
            fix_hint="supersede or reject all but one accepted observation",
        )
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(row["target_id"], []).append(row)
    return out


def attribute_history_for_target(
    conn: psycopg.Connection,
    *,
    target_kind: str,
    target_id: int | str,
    generation_id: int | str,
    attr_key: str | None = None,
    namespace: str = DEFAULT_NAMESPACE,
) -> list[dict[str, Any]]:
    """Return opt-in history/provenance rows for a target attribute."""
    if not _table_exists(conn, ATTRIBUTE_OBSERVATION_TABLE):
        return []
    kind = _normalize_target_kind(str(target_kind))
    target = str(target_id)
    generation = str(generation_id)
    attr_clause = ""
    snapshot_attr_clause = ""
    params: list[Any]
    if _table_exists(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        params = [
            kind,
            target,
            namespace,
            generation,
            kind,
            target,
            generation,
            namespace,
        ]
        if attr_key is not None:
            attr_clause = "AND obs.attr_key = %s"
            snapshot_attr_clause = "AND snap.attr_key = %s"
            params.extend([attr_key, attr_key])
        sql = f"""
            SELECT DISTINCT obs.*
              FROM {ATTRIBUTE_OBSERVATION_TABLE} AS obs
             WHERE obs.target_kind = %s
               AND obs.target_id = %s
               AND obs.namespace = %s
               AND (
                 obs.generation_id = %s
                 OR obs.observation_id IN (
                   SELECT snap.observation_id
                     FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS snap
                    WHERE snap.target_kind = %s
                      AND snap.target_id = %s
                      AND snap.generation_id = %s
                      AND snap.namespace = %s
                      {snapshot_attr_clause}
                 )
               )
               {attr_clause}
             ORDER BY obs.attr_key, obs.observed_at, obs.observation_id
            """
    else:
        params = [kind, target, generation, namespace]
        if attr_key is not None:
            attr_clause = "AND attr_key = %s"
            params.append(attr_key)
        sql = f"""
            SELECT *
              FROM {ATTRIBUTE_OBSERVATION_TABLE}
             WHERE target_kind = %s
               AND target_id = %s
               AND generation_id = %s
               AND namespace = %s
               {attr_clause}
             ORDER BY attr_key, observed_at, observation_id
            """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        observations = [_row_to_observation(row).as_dict() for row in cur.fetchall()]
    by_id = {row["observation_id"]: row for row in observations}
    for row in observations:
        if row.get("supersedes") and row["supersedes"] in by_id:
            old = by_id[row["supersedes"]]
            row.setdefault(
                "diff",
                {
                    "old": old["value"],
                    "new": row["value"],
                    "old_value_hash": old["value_hash"],
                    "new_value_hash": row["value_hash"],
                },
            )
    return observations


def materialize_effective_attributes_for_generation(
    conn: psycopg.Connection,
    *,
    generation_id: int | str,
    previous_generation_id: int | str | None = None,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """Create the immutable effective-attrs snapshot for one generation.

    Existing snapshots are left untouched. Stale, rejected, and superseded
    observations never carry forward; accepted observations recorded for the
    new generation override any carried-forward snapshot row for the same
    target/key during this first materialization.
    """
    _require_attribute_schema(conn)
    generation = str(generation_id)
    previous = str(previous_generation_id) if previous_generation_id else None
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if _attribute_materialization_state_empty(conn):
        with conn.transaction(), conn.cursor() as cur:
            _write_overlay_generation_marker(
                cur, generation=generation, previous=previous, namespace=namespace,
            )
        return {
            "generation_id": generation,
            "previous_generation_id": previous,
            "namespace": namespace,
            "materialized": True,
            "reason": "empty_attribute_state",
            "carried_forward": 0,
            "overlaid": 0,
        }

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 1
                  FROM {ATTRIBUTE_EFFECTIVE_TABLE}
                 WHERE generation_id = %s
                   AND namespace = %s
                 LIMIT 1
                """,
                (generation, namespace),
            )
            if cur.fetchone() is not None:
                _write_overlay_generation_marker(
                    cur, generation=generation, previous=previous,
                    namespace=namespace,
                )
                return {
                    "generation_id": generation,
                    "previous_generation_id": previous,
                    "namespace": namespace,
                    "materialized": False,
                    "reason": "already_materialized",
                    "carried_forward": 0,
                    "overlaid": 0,
                }

            carried_forward = 0
            if previous is not None:
                cur.execute(
                    f"""
                    INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE}
                      (target_kind, target_id, generation_id, namespace,
                       attr_key, observation_id, value, value_hash, producer,
                       provenance, dependencies, dependency_fingerprint,
                       confidence)
                    SELECT snap.target_kind, snap.target_id, %s,
                           snap.namespace, snap.attr_key, snap.observation_id,
                           snap.value, snap.value_hash, snap.producer,
                           snap.provenance, snap.dependencies,
                           snap.dependency_fingerprint, snap.confidence
                      FROM {ATTRIBUTE_EFFECTIVE_TABLE} AS snap
                      JOIN {ATTRIBUTE_OBSERVATION_TABLE} AS obs
                        ON obs.observation_id = snap.observation_id
                     WHERE snap.generation_id = %s
                       AND snap.namespace = %s
                       AND snap.attr_key <> ALL(%s::text[])
                       AND obs.status = 'accepted'
                       AND obs.superseded_by IS NULL
                       AND NOT EXISTS (
                         SELECT 1
                           FROM {ATTRIBUTE_OBSERVATION_TABLE} AS current_obs
                          WHERE current_obs.target_kind = snap.target_kind
                            AND current_obs.target_id = snap.target_id
                            AND current_obs.generation_id = %s
                            AND current_obs.namespace = snap.namespace
                            AND current_obs.attr_key = snap.attr_key
                            AND current_obs.status = 'accepted'
                            AND current_obs.superseded_by IS NULL
                       )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        generation,
                        previous,
                        namespace,
                        sorted(RESERVED_EFFECTIVE_ATTR_KEYS),
                        generation,
                    ),
                )
                carried_forward = cur.rowcount
                cur.execute(
                    f"""
                    INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE}
                      (target_kind, target_id, generation_id, namespace,
                       attr_key, observation_id, value, value_hash, producer,
                       provenance, dependencies, dependency_fingerprint,
                       confidence)
                    SELECT obs.target_kind, obs.target_id, %s,
                           obs.namespace, obs.attr_key, obs.observation_id,
                           obs.value, obs.value_hash, obs.producer,
                           obs.provenance, obs.dependencies,
                           obs.dependency_fingerprint, obs.confidence
                      FROM {ATTRIBUTE_OBSERVATION_TABLE} AS obs
                     WHERE obs.generation_id = %s
                       AND obs.namespace = %s
                       AND obs.attr_key <> ALL(%s::text[])
                       AND obs.status = 'accepted'
                       AND obs.superseded_by IS NULL
                       AND NOT EXISTS (
                         SELECT 1
                           FROM {ATTRIBUTE_OBSERVATION_TABLE} AS current_obs
                          WHERE current_obs.target_kind = obs.target_kind
                            AND current_obs.target_id = obs.target_id
                            AND current_obs.generation_id = %s
                            AND current_obs.namespace = obs.namespace
                            AND current_obs.attr_key = obs.attr_key
                            AND current_obs.status = 'accepted'
                            AND current_obs.superseded_by IS NULL
                       )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        generation,
                        previous,
                        namespace,
                        sorted(RESERVED_EFFECTIVE_ATTR_KEYS),
                        generation,
                    ),
                )
                carried_forward += cur.rowcount

            cur.execute(
                f"""
                INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE}
                  (target_kind, target_id, generation_id, namespace,
                   attr_key, observation_id, value, value_hash, producer,
                   provenance, dependencies, dependency_fingerprint,
                   confidence)
                SELECT target_kind, target_id, generation_id, namespace,
                       attr_key, observation_id, value, value_hash, producer,
                       provenance, dependencies, dependency_fingerprint,
                       confidence
                  FROM {ATTRIBUTE_OBSERVATION_TABLE}
                 WHERE generation_id = %s
                   AND namespace = %s
                   AND attr_key <> ALL(%s::text[])
                   AND status = 'accepted'
                   AND superseded_by IS NULL
                ON CONFLICT (
                  target_kind, target_id, generation_id, namespace, attr_key
                ) DO NOTHING
                """,
                (generation, namespace, sorted(RESERVED_EFFECTIVE_ATTR_KEYS)),
            )
            overlaid = cur.rowcount
            _write_overlay_generation_marker(
                cur, generation=generation, previous=previous, namespace=namespace,
            )

    return {
        "generation_id": generation,
        "previous_generation_id": previous,
        "namespace": namespace,
        "materialized": True,
        "reason": (
            "empty_attribute_state"
            if carried_forward == 0 and overlaid == 0
            else "materialized"
        ),
        "carried_forward": carried_forward,
        "overlaid": overlaid,
    }


# --------------------------------------------------------------------------- #
# PACT lane-s-storage-redesign-s2 (design section 5): the overlay marker and
# the overlay validity ranges. The per-generation table above stays the
# authority S2 reads (and Nomos reads); these two are written beside it.
# --------------------------------------------------------------------------- #


def _write_overlay_generation_marker(
    cur: Any,
    *,
    generation: str,
    previous: str | None,
    namespace: str,
) -> None:
    """Record that ``generation``'s overlays for ``namespace`` were
    materialized, with the number of overlay rows it holds now (zero is a
    real answer). Every exit of the materializer calls this; a re-run updates
    the count and keeps the rest."""
    cur.execute("SELECT to_regclass(%s)", (OVERLAY_GENERATION_MARKER_TABLE,))
    if cur.fetchone()[0] is None:
        raise SubstrateError(
            "the overlay generation marker table is missing",
            data={"table": OVERLAY_GENERATION_MARKER_TABLE, "generation_id": generation},
            fix_hint="run `okg migrate --apply` (migration 0044) before publishing",
        )
    cur.execute(
        f"""
        INSERT INTO {OVERLAY_GENERATION_MARKER_TABLE}
          (generation_id, namespace, previous_generation_id, overlay_rows, source)
        SELECT %(gen)s, %(ns)s, %(prev)s, count(*), 'materializer'
          FROM {ATTRIBUTE_EFFECTIVE_TABLE}
         WHERE generation_id = %(gen)s AND namespace = %(ns)s
        ON CONFLICT (generation_id, namespace)
        DO UPDATE SET overlay_rows = EXCLUDED.overlay_rows
        """,
        {"gen": generation, "ns": namespace, "prev": previous},
    )


def require_overlay_generation_marker(
    conn: psycopg.Connection,
    generation_id: int | str,
    *,
    namespace: str = DEFAULT_NAMESPACE,
) -> None:
    """Refuse, by name, a read of a generation whose overlays were never
    materialized. Such a read is never answered from graph values alone."""
    generation = str(generation_id)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (OVERLAY_GENERATION_MARKER_TABLE,))
        present = cur.fetchone()[0] is not None
        found = False
        if present:
            cur.execute(
                f"SELECT 1 FROM {OVERLAY_GENERATION_MARKER_TABLE} "
                "WHERE generation_id = %s AND namespace = %s",
                (generation, namespace),
            )
            found = cur.fetchone() is not None
    if found:
        return
    # Worded as mcp/graph_read.unmarked_pin_refusal restates it (issue #4049).
    if not present:
        message = (
            "this database has no overlay marker table (migration 0044 is not "
            "applied), so no generation's effective attributes can be read"
        )
        hint = "an operator runs `okg migrate --apply`, which also writes the markers"
    else:
        message = (
            "the pinned generation has no overlay marker, so its effective "
            "attributes cannot be read (the graph rows plus overlays are the "
            "only read path): its overlay step never ran. Every publish, "
            "fast-forward, history compaction and rollover now writes one; a "
            "generation minted by an older release, or a bare checkpoint "
            "outside them (a bench branch), has none"
        )
        hint = (
            "read a generation published after it: the next publish on this "
            "branch writes its marker, and a read with no generation_id then "
            "pins that generation"
        )
    err = SubstrateError(
        message,
        data={
            "generation_id": generation,
            "namespace": namespace,
            "marker_table_present": present,
        },
        fix_hint=hint,
    )
    err.code = "overlay_generation_unmaterialized"
    raise err


def _overlay_ranges_rebuild_sql(*, upto: bool) -> str:
    """The validity ranges of one branch, from its per-generation overlay
    rows in ordinal order (gaps and islands): a run of consecutive branch
    ordinals over which one (target, key) keeps the same observation and value
    is one range, closed at the next ordinal of the branch, or open when the
    run reaches the branch's last ordinal (``upto`` limits the replay)."""
    limit = "AND o.ordinal <= %(upto)s" if upto else ""
    return f"""
        WITH ords AS (
          SELECT o.checkpoint_id AS generation_id, o.ordinal,
                 row_number() OVER (ORDER BY o.ordinal) AS rn,
                 lead(o.ordinal) OVER (ORDER BY o.ordinal) AS next_ord
            FROM okg.generation_ordinals o
           WHERE o.branch_id = %(branch)s {limit}
        ),
        overlay_rows AS (
          SELECT p.target_kind, p.target_id, p.namespace, p.attr_key,
                 p.observation_id, p.value_hash, p.value,
                 ords.ordinal, ords.rn, ords.next_ord
            FROM {ATTRIBUTE_EFFECTIVE_TABLE} p
            JOIN ords ON ords.generation_id = p.generation_id
        ),
        islands AS (
          SELECT *,
                 rn - row_number() OVER (
                   PARTITION BY target_kind, target_id, namespace, attr_key,
                                observation_id, value_hash
                   ORDER BY rn
                 ) AS island
            FROM overlay_rows
        )
        INSERT INTO {OVERLAY_RANGES_TABLE}
          (chronos_branch_id, target_kind, target_id, namespace, attr_key, from_ord,
           to_ord, observation_id, value, value_hash)
        SELECT %(branch)s, target_kind, target_id, namespace, attr_key,
               min(ordinal),
               COALESCE((array_agg(next_ord ORDER BY rn DESC))[1], %(open)s),
               observation_id, (array_agg(value ORDER BY rn))[1], value_hash
          FROM islands
         GROUP BY target_kind, target_id, namespace, attr_key, observation_id,
                  value_hash, island
    """


def rebuild_overlay_ranges_for_branch(
    conn: psycopg.Connection,
    branch_id: str,
    *,
    upto_ordinal: int | None = None,
) -> dict[str, Any]:
    """Replace one branch's overlay ranges with a replay of its per-generation
    rows, and mark the replayed generations' markers synced. Used by the
    one-time backfill and when a publish finds the branch's ranges behind."""
    params: dict[str, Any] = {
        "branch": branch_id, "open": OVERLAY_RANGE_OPEN_ORD, "upto": upto_ordinal,
    }
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"DELETE FROM {OVERLAY_RANGES_TABLE} WHERE chronos_branch_id = %s", (branch_id,)
        )
        cur.execute(_overlay_ranges_rebuild_sql(upto=upto_ordinal is not None), params)
        written = cur.rowcount
        cur.execute(
            f"""
            UPDATE {OVERLAY_GENERATION_MARKER_TABLE} m
               SET chronos_branch_id = o.branch_id, ordinal = o.ordinal, ranges_synced = TRUE
              FROM okg.generation_ordinals o
             WHERE o.checkpoint_id = m.generation_id
               AND o.branch_id = %(branch)s
               AND (%(upto)s::bigint IS NULL OR o.ordinal <= %(upto)s::bigint)
            """,
            params,
        )
    return {"branch_id": branch_id, "mode": "rebuild", "ranges_written": written}


def sync_overlay_ranges_for_generation(
    conn: psycopg.Connection,
    generation_id: int | str,
) -> dict[str, Any]:
    """Fold one generation's per-generation overlay rows into its branch's
    ranges at its ordinal: close every open range whose (observation, value)
    the generation no longer holds, open one for each row no open range holds.

    Runs right after the generation's ordinal is assigned. Idempotent for the
    branch's latest synced generation (a post-publish re-materialize re-runs
    it). When the branch's ranges are behind (a generation published without a
    sync, or a branch never backfilled), the branch is rebuilt by replay
    instead. A generation older than the branch's latest synced one is refused
    by name: its later ranges are already closed. A generation with no ordinal
    (the interval arm off) is skipped and says so.
    """
    generation = str(generation_id)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT branch_id, ordinal FROM okg.generation_ordinals WHERE checkpoint_id = %s",
            (generation,),
        )
        row = cur.fetchone()
        if row is None:
            return {"generation_id": generation, "mode": "skipped", "reason": "no_ordinal"}
        branch_id, ordinal = str(row[0]), int(row[1])
        cur.execute(
            f"SELECT max(ordinal) FROM {OVERLAY_GENERATION_MARKER_TABLE} "
            "WHERE chronos_branch_id = %s AND ranges_synced",
            (branch_id,),
        )
        last_synced = cur.fetchone()[0]
        cur.execute(
            "SELECT max(ordinal) FROM okg.generation_ordinals "
            "WHERE branch_id = %s AND ordinal < %s",
            (branch_id, ordinal),
        )
        previous_ordinal = cur.fetchone()[0]
    if last_synced is not None and int(last_synced) > ordinal:
        err = SubstrateError(
            "overlay ranges are already synced past this generation",
            data={
                "generation_id": generation, "branch_id": branch_id,
                "ordinal": ordinal, "last_synced_ordinal": int(last_synced),
            },
            fix_hint="re-materialize only the branch's latest generation",
        )
        err.code = "overlay_ranges_out_of_order"
        raise err
    in_step = (
        last_synced is not None and int(last_synced) == ordinal
    ) or (
        previous_ordinal is None
        if last_synced is None
        else int(last_synced) == int(previous_ordinal)
    )
    if not in_step:
        return {
            **rebuild_overlay_ranges_for_branch(conn, branch_id, upto_ordinal=ordinal),
            "generation_id": generation,
            "ordinal": ordinal,
        }
    params = {
        "branch": branch_id, "ord": ordinal, "gen": generation,
        "open": OVERLAY_RANGE_OPEN_ORD,
    }
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {OVERLAY_RANGES_TABLE} r
               SET to_ord = %(ord)s
             WHERE r.chronos_branch_id = %(branch)s
               AND r.to_ord = %(open)s
               AND r.from_ord < %(ord)s
               AND NOT EXISTS (
                 SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE} p
                  WHERE p.generation_id = %(gen)s
                    AND p.target_kind = r.target_kind
                    AND p.target_id = r.target_id
                    AND p.namespace = r.namespace
                    AND p.attr_key = r.attr_key
                    AND p.observation_id = r.observation_id
                    AND p.value_hash = r.value_hash)
            """,
            params,
        )
        closed = cur.rowcount
        cur.execute(
            f"""
            INSERT INTO {OVERLAY_RANGES_TABLE}
              (chronos_branch_id, target_kind, target_id, namespace, attr_key, from_ord,
               to_ord, observation_id, value, value_hash)
            SELECT %(branch)s, p.target_kind, p.target_id, p.namespace, p.attr_key,
                   %(ord)s, %(open)s, p.observation_id, p.value, p.value_hash
              FROM {ATTRIBUTE_EFFECTIVE_TABLE} p
             WHERE p.generation_id = %(gen)s
               AND NOT EXISTS (
                 SELECT 1 FROM {OVERLAY_RANGES_TABLE} r
                  WHERE r.chronos_branch_id = %(branch)s
                    AND r.to_ord = %(open)s
                    AND r.target_kind = p.target_kind
                    AND r.target_id = p.target_id
                    AND r.namespace = p.namespace
                    AND r.attr_key = p.attr_key
                    AND r.observation_id = p.observation_id
                    AND r.value_hash = p.value_hash)
            """,
            params,
        )
        opened = cur.rowcount
        cur.execute(
            f"""
            UPDATE {OVERLAY_GENERATION_MARKER_TABLE}
               SET chronos_branch_id = %(branch)s, ordinal = %(ord)s, ranges_synced = TRUE
             WHERE generation_id = %(gen)s
            """,
            params,
        )
        if cur.rowcount == 0:
            raise SubstrateError(
                "overlay ranges synced for a generation with no marker",
                data={"generation_id": generation, "branch_id": branch_id},
                fix_hint="materialize the generation's overlays before its ordinal step",
            )
    return {
        "generation_id": generation, "branch_id": branch_id, "ordinal": ordinal,
        "mode": "step", "closed": closed, "opened": opened,
    }


def overlay_range_parity(
    conn: psycopg.Connection,
    *,
    branch_id: str | None = None,
    latest_only: bool = False,
) -> list[dict[str, Any]]:
    """Every synced generation (or each branch's latest one) whose range stab
    at its ordinal differs from its per-generation overlay rows on (target,
    key, observation, value hash), with the counts each way. Empty means the
    ranges equal the per-generation table at every generation checked."""
    latest = (
        "AND m.ordinal = (SELECT max(m2.ordinal) FROM "
        f"{OVERLAY_GENERATION_MARKER_TABLE} m2 WHERE m2.chronos_branch_id = m.chronos_branch_id "
        "AND m2.ranges_synced)"
        if latest_only else ""
    )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            WITH checked AS (
              SELECT DISTINCT m.generation_id, m.chronos_branch_id AS branch_id, m.ordinal
                FROM {OVERLAY_GENERATION_MARKER_TABLE} m
               WHERE m.ranges_synced
                 AND (%(branch)s::text IS NULL OR m.chronos_branch_id = %(branch)s::text)
                 {latest}
            )
            SELECT c.generation_id, c.branch_id, c.ordinal,
                   (SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_TABLE} p
                     WHERE p.generation_id = c.generation_id
                       AND NOT EXISTS (
                         SELECT 1 FROM {OVERLAY_RANGES_TABLE} r
                          WHERE r.chronos_branch_id = c.branch_id
                            AND r.from_ord <= c.ordinal AND c.ordinal < r.to_ord
                            AND r.target_kind = p.target_kind
                            AND r.target_id = p.target_id
                            AND r.namespace = p.namespace
                            AND r.attr_key = p.attr_key
                            AND r.observation_id = p.observation_id
                            AND r.value_hash = p.value_hash)
                   ) AS missing_from_ranges,
                   (SELECT count(*) FROM {OVERLAY_RANGES_TABLE} r
                     WHERE r.chronos_branch_id = c.branch_id
                       AND r.from_ord <= c.ordinal AND c.ordinal < r.to_ord
                       AND NOT EXISTS (
                         SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_TABLE} p
                          WHERE p.generation_id = c.generation_id
                            AND p.target_kind = r.target_kind
                            AND p.target_id = r.target_id
                            AND p.namespace = r.namespace
                            AND p.attr_key = r.attr_key
                            AND p.observation_id = r.observation_id
                            AND p.value_hash = r.value_hash)
                   ) AS extra_in_ranges
              FROM checked c
             ORDER BY c.branch_id, c.ordinal
            """,
            {"branch": branch_id},
        )
        rows = [dict(r) for r in cur.fetchall()]
    return [
        r for r in rows
        if int(r["missing_from_ranges"]) or int(r["extra_in_ranges"])
    ]
