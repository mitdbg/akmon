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
import os
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Literal, NamedTuple

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.derived_artifacts.maintained_derivation import (
    EXPLICIT_INTERVAL_OPEN_ORD,
    ExplicitIntervalTable,
    backfill_explicit_interval_rows_for_generation,
    close_dirty_explicit_intervals,
    explicit_interval_payload_unchanged_sql,
    explicit_interval_sync_temp_name,
    open_missing_explicit_intervals_from_staged,
    stage_explicit_snapshot_slice,
    sync_explicit_interval_rows_for_generation,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.images.values import check_incoming
from okg.substrate.nomos.read_masking import ReadRedactedAttrs
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.support.user_hints import okg_prefix


logger = logging.getLogger(__name__)

TargetKind = Literal["node", "edge"]


def _first_column(row: Any) -> Any:
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


class DirtyTargets(NamedTuple):
    """ivm-effective-attribute-view / task.plumb-dirty-scope: the publisher's
    already-computed per-cycle raw changeset, threaded into the view materializer
    so it stages/merges/writes only changed targets instead of re-deriving the
    delta from a full chronos-visible scan.

    ``node_ids`` / ``edge_ids`` are the RAW graph targets that changed this cycle
    (from ``chronos_dirty_scope()`` — added/updated/retracted nodes and edges,
    edge ids as strings). They do NOT include OVERLAY-only changes (enricher
    attribute observations written out-of-band), which is correct: the
    materializer unions these with the per-generation overlay gen-diff itself, so
    an overlay-only change is never silently carried forward stale. ``None`` for
    the whole struct means "delta unavailable" -> the materializer falls back to
    the full-scan path (backfill / migrate / idempotent re-run)."""

    node_ids: frozenset[str]
    edge_ids: frozenset[str]

ATTRIBUTE_OBSERVATION_TABLE = "okg.attribute_observations"
ATTRIBUTE_EFFECTIVE_TABLE = "okg.attribute_effective_attrs"
# scale-operator-attr-reads / req.effective-attrs-view (Layer 2): a per-generation
# MATERIALIZED snapshot of EFFECTIVE node/edge attrs (raw graph attrs merged with
# the accepted overlay) carrying exploded stable columns (subtype/edge_type/src/dst)
# so the MCP attr operators can filter/group/aggregate over hundreds of thousands of
# rows IN SQL (index scan) instead of the bounded Python overlay. This is a DERIVED
# read-optimization downstream of ATTRIBUTE_EFFECTIVE_TABLE, never a substitute for it.
ATTRIBUTE_EFFECTIVE_VIEW_TABLE = "okg.attribute_effective_view"
# ivm-effective-attribute-view / task.interval-row-representation: the SHARED-row
# replacement for the per-generation snapshot. Each row carries a validity interval
# [from_ord, to_ord) over branch-scoped generation ORDINALS (see generation_ordinal):
# an unchanged target is ONE row spanning many generations, so per-publish heap
# growth is O(changed), not O(live). A read pinned to generation G resolves G ->
# (branch, ordinal) and STABS the interval (from_ord <= G_ord < to_ord). Behind the
# OKG_IVM_INTERVAL_VIEW defaults on now that interval rows are proven
# byte-identical to the snapshot and avoid per-generation full-copy publishes.
ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE = "okg.attribute_effective_view_intervals"
# schema-interval-narrow-split / req.split-shape: the insert-once
# content-addressed version store. Interval and snapshot rows carry a bytea(32)
# attrs_hash instead of inline jsonb payloads; the payload (attrs +
# presence-means-overlay raw_attrs) is written ONCE here and referenced through
# a composite FK (target_kind, target_id, attrs_hash) ON DELETE RESTRICT.
ATTRIBUTE_VERSIONS_TABLE = "okg.attribute_versions"
# req.sigma-pinned: THE hash site. Exactly one IMMUTABLE in-database function
# computes the content hash over the deterministic rendering of the STORED
# jsonb value (never a Python-side serialization -- jsonb key order, spacing,
# unicode escapes, and numeric rendering all diverge between Python json.dumps
# and jsonb::text). Every writer INSERT...SELECT calls this function; the
# keep-open-vs-close comparison compares stored hashes, so jsonb-equal payloads
# (key order, 1.0 vs 1.00) can never close+reopen an interval.
ATTRIBUTE_CONTENT_HASH_FN = "okg.attribute_content_hash"
# Open (still-live) interval upper bound: max int8 sentinel, so the stab predicate
# is a clean half-open range (from_ord <= G_ord < to_ord) with no NULL handling.
EFFECTIVE_INTERVAL_OPEN_ORD = EXPLICIT_INTERVAL_OPEN_ORD
DEFAULT_NAMESPACE = "default"
CURRENT_STATUSES = frozenset({"accepted"})
TERMINAL_STATUSES = frozenset({"rejected", "superseded", "stale"})
ALL_STATUSES = frozenset({"candidate", *CURRENT_STATUSES, *TERMINAL_STATUSES})
RESERVED_EFFECTIVE_ATTR_KEYS = frozenset({LINEAGE_ATTR})


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

_ATTRIBUTE_VERSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS okg.attribute_versions (
  target_kind TEXT NOT NULL
    CHECK (target_kind IN ('node', 'edge')),
  target_id TEXT NOT NULL,
  attrs_hash BYTEA NOT NULL,
  -- The effective payload. NOT NULL and never defaulted: an absent payload is
  -- a bug, not an empty object.
  attrs JSONB NOT NULL,
  -- presence-means-overlay: raw_attrs is stored ONLY when it byte-differs from
  -- ``attrs`` (an overlay actually applied); NULL means "raw == attrs" and
  -- readers fall back via COALESCE(raw_attrs, attrs). Never DEFAULT '{}': an
  -- absent value is the identity signal, not an empty object.
  raw_attrs JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (target_kind, target_id, attrs_hash)
)
"""



_SCHEMA_STATEMENTS: tuple[str, ...] = (
    "CREATE SCHEMA IF NOT EXISTS okg",
    _JSONB_CANONICAL_TEXT_FN_SQL,
    _ATTRIBUTE_CONTENT_HASH_FN_SQL,
    _ATTRIBUTE_VERSIONS_TABLE_SQL,
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
    # --- scale-operator-attr-reads / req.effective-attrs-view (Layer 2) --- #
    # One row per live (target, generation): exploded stable columns the attr
    # operators push subtype/edge_type into, plus the content-addressed
    # ``attrs_hash`` reference into okg.attribute_versions (the payload itself
    # lives ONCE in the version store -- schema-interval-narrow-split; the
    # snapshot measured 100% payload overlap with the interval front, so it
    # stops carrying a second full copy). generation_id is TEXT (the opaque
    # ``okg:...:gen:...`` string), never bigint. ``attrs_hash`` is deliberately
    # the LAST column so a migrated deployment (ALTER TABLE ADD COLUMN appends)
    # converges to the same column order as a fresh install.
    f"""
    CREATE TABLE IF NOT EXISTS {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} (
      target_kind TEXT NOT NULL
        CHECK (target_kind IN ('node', 'edge')),
      target_id TEXT NOT NULL,
      generation_id TEXT NOT NULL,
      namespace TEXT NOT NULL DEFAULT 'default',
      subtype TEXT,
      src TEXT,
      dst TEXT,
      edge_type TEXT,
      overlay_count INT NOT NULL DEFAULT 0,
      raw_text_hash TEXT,
      materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      attrs_hash BYTEA NOT NULL,
      PRIMARY KEY (target_kind, target_id, generation_id, namespace),
      CONSTRAINT attribute_effective_view_version_fkey
        FOREIGN KEY (target_kind, target_id, attrs_hash)
        REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
        ON DELETE RESTRICT
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_view_generation_namespace_idx
      ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
      (generation_id, namespace)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_view_node_subtype_idx
      ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
      (generation_id, namespace, subtype, target_id)
      WHERE target_kind = 'node'
    """,
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_view_edge_type_idx
      ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
      (generation_id, namespace, edge_type, target_id)
      WHERE target_kind = 'edge'
    """,
    # --- ivm-effective-attribute-view / task.interval-row-representation --- #
    # Shared validity-interval rows over branch-scoped generation ordinals. One row
    # per (target, branch, from_ord); to_ord = EFFECTIVE_INTERVAL_OPEN_ORD while live.
    f"""
    CREATE TABLE IF NOT EXISTS {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} (
      target_kind TEXT NOT NULL
        CHECK (target_kind IN ('node', 'edge')),
      target_id TEXT NOT NULL,
      namespace TEXT NOT NULL DEFAULT 'default',
      branch_id TEXT NOT NULL,
      from_ord BIGINT NOT NULL,
      to_ord BIGINT NOT NULL DEFAULT {EFFECTIVE_INTERVAL_OPEN_ORD},
      subtype TEXT,
      src TEXT,
      dst TEXT,
      edge_type TEXT,
      overlay_count INT NOT NULL DEFAULT 0,
      raw_text_hash TEXT,
      materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      -- schema-interval-narrow-split: the row carries a bytea(32) reference
      -- into okg.attribute_versions instead of inline jsonb payloads, so an
      -- interval CLOSE rewrites ~150-300 B instead of ~1.3 KB (the
      -- write-amplification claim; the measured interval dedup ratio is 1.0,
      -- so this is NOT a storage-dedup play). Deliberately the LAST column so
      -- migrated deployments converge to the fresh column order.
      attrs_hash BYTEA NOT NULL,
      CHECK (from_ord < to_ord),
      PRIMARY KEY (target_kind, target_id, namespace, branch_id, from_ord),
      CONSTRAINT attribute_effective_view_intervals_version_fkey
        FOREIGN KEY (target_kind, target_id, attrs_hash)
        REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
        ON DELETE RESTRICT
    )
    """,
    # Write path: find/close the OPEN interval for a target (close + open's
    # NOT-EXISTS anti-join both seek here). Partial = only the live front.
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_open_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (branch_id, namespace, target_kind, target_id)
      WHERE to_ord = {EFFECTIVE_INTERVAL_OPEN_ORD}
    """,
    # Read path (stab live-at-G): range-scan from_ord, filter to_ord, by subtype.
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_node_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (branch_id, namespace, subtype, from_ord, to_ord, target_id)
      WHERE target_kind = 'node'
    """,
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_edge_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (branch_id, namespace, edge_type, from_ord, to_ord, target_id)
      WHERE target_kind = 'edge'
    """,
    # Edge pages in numeric id order (#3408): the read compiler orders edges
    # by `(target_id)::bigint`, as the Chronos path does, and the text-order
    # keys above cannot serve that, so a page of one edge type read and
    # sorted every interval of the type (213 ms for 75k on Krama). target_kind
    # is NOT a key column: the partial predicate fixes it, and PostgreSQL 17
    # has no skip scan, so a leading target_kind key made the scan walk every
    # alphabetically earlier edge type. Edge ids are numeric by construction.
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_edge_num_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (namespace, branch_id, edge_type, ((target_id)::bigint))
      WHERE target_kind = 'edge'
    """,
    # Read path by edge endpoint (#3235, PACT lane-h-edge-interval-src-dst-idx):
    # a filtered expand and an edges filter or map with a src/dst predicate
    # stab live-at-G for one endpoint. Without these the planner seq-scans the
    # whole interval table (measured 94 ms for 241 of 800k rows on lane B's CMS
    # copy). _src serves direction "out", _dst serves "in". Like their
    # siblings they are created here, so provision and `okg migrate` (inside
    # its transaction) both build them: non-concurrent, holding a SHARE lock
    # on the table until migrate commits. Publishes wait; a plain read of
    # this table does not, but migrate's transaction also holds locks on the
    # views it rebuilds (okg.v_generations), so reads through those wait for
    # the whole migrate, now longer by the build. The snapshot table read
    # only under OKG_IVM_INTERVAL_VIEW=0 gets
    # no endpoint index.
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_edge_src_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (branch_id, namespace, src, from_ord, to_ord, target_id)
      WHERE target_kind = 'edge'
    """,
    f"""
    CREATE INDEX IF NOT EXISTS attribute_effective_intervals_edge_dst_idx
      ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
      (branch_id, namespace, dst, from_ord, to_ord, target_id)
      WHERE target_kind = 'edge'
    """,
    # Per-table autovacuum tuning (PACT bound-okg-workspace-disk-growth, task 4).
    # Both tables churn every publish (overlay re-merge + retention-prune deletes),
    # so the cluster-default 20% scale factor lets dead tuples accumulate into GBs
    # of bloat before autovacuum fires. Tighten so vacuum keeps pace with the prune.
    # Idempotent: re-setting the same reloptions is a no-op. The CREATE TABLE IF NOT
    # EXISTS statements above guarantee the tables exist before these ALTERs run.
    f"""
    ALTER TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} SET (
      autovacuum_vacuum_scale_factor = 0.05,
      autovacuum_analyze_scale_factor = 0.05,
      autovacuum_vacuum_cost_limit = 2000
    )
    """,
    f"""
    ALTER TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} SET (
      autovacuum_vacuum_scale_factor = 0.05,
      autovacuum_analyze_scale_factor = 0.05,
      autovacuum_vacuum_cost_limit = 2000
    )
    """,
)


# The effective-attribute view + observation tables are granted SELECT to the
# constrained okg_mcp read pool by the migration allow-list (db/views.sql). But the
# self-heal path (okg#400) DROPs + recreates okg.attribute_effective_view to reclaim
# disk, and a recreated table loses its grants -- after which the MCP attr operators
# (filter/map/aggregate on the okg_mcp pool) fail "permission denied" until a
# re-migration. So re-apply the grant whenever the schema is (re)created. Mirrors
# _grant_chronos_public_tables_to_mcp_if_present: role-guarded, per-table best-effort
# (a deployment with no okg_mcp role, or a creator lacking GRANT authority, is a
# no-op, never an error). ensure_attribute_schema runs only on provision + self-heal
# (never per-publish), so this adds no steady-state grant churn.
_OKG_MCP_EFFECTIVE_VIEW_GRANTS = f"""
DO $okg_mcp_effective_view_grants$
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
    GRANT SELECT ON TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
  BEGIN
    GRANT SELECT ON TABLE {ATTRIBUTE_VERSIONS_TABLE} TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_effective_view_grants$
"""


# schema-interval-narrow-split, rephased by
# phase-interval-narrow-split-migration (#1143): converge a pre-split WIDE
# deployment (inline attrs/raw_attrs jsonb columns) onto the narrow shape
# ONLY when the wide tables are EMPTY (zero data motion). A populated wide
# database is REFUSED with a typed error (SQLSTATE OKG20): the inline
# convergence used to rewrite every row of both tables, copy the full jsonb
# payload, and validate two composite FKs inside whatever transaction the
# caller happened to hold — measured at 1.35 TB of reads on okg-workspace
# without completing. That data motion now has exactly ONE owner: the
# checkpointed multi-phase driver running the registered
# 0020_interval_narrow_split spec (okg.substrate.db.multiphase). migrate()
# is the single caller allowed to OBSERVE the wide shape without refusing
# (tolerate_pre_narrow_split=True) because its orchestration loop runs that
# driver immediately after its transaction commits.
# Idempotent and guarded per step; a converged database only pays the
# information_schema/pg_constraint probes.
_NARROW_SPLIT_POPULATED_WIDE_REFUSAL = """
      RAISE EXCEPTION USING
        ERRCODE = 'OKG20',
        MESSAGE = 'okg.attribute_effective_view/_intervals are in the '
          'pre-0020 wide shape with rows; refusing inline convergence',
        DETAIL = 'migration 0020_interval_narrow_split is pending and its '
          'data motion runs only through the checkpointed multi-phase '
          'driver, never inside a schema ensure',
        HINT = 'run okg migrate --apply (or okg up) for this deployment; '
          'progress is durable in okg.schema_migration_phases and a killed '
          'run resumes from its checkpoint';
"""

_NARROW_SPLIT_POPULATED_WIDE_TOLERATE = """
      -- migrate() orchestration: the multi-phase driver converges this
      -- database right after the migrate transaction commits.
      RETURN;
"""


def _narrow_split_upgrade_sql(*, tolerate_pre_narrow_split: bool) -> str:
    populated_wide_arm = (
        _NARROW_SPLIT_POPULATED_WIDE_TOLERATE
        if tolerate_pre_narrow_split
        else _NARROW_SPLIT_POPULATED_WIDE_REFUSAL
    )
    # Fail loud if the template ever loses the marker: a silent .replace()
    # no-op would ship a DO block with NO populated-wide arm, whose
    # fall-through convergence is exactly the unbounded inline rewrite this
    # refusal exists to kill (#1143).
    if "__POPULATED_WIDE_ARM__" not in _NARROW_SPLIT_UPGRADE_SQL_TEMPLATE:
        raise AssertionError(
            "_NARROW_SPLIT_UPGRADE_SQL_TEMPLATE lost its "
            "__POPULATED_WIDE_ARM__ marker"
        )
    return _NARROW_SPLIT_UPGRADE_SQL_TEMPLATE.replace(
        "__POPULATED_WIDE_ARM__", populated_wide_arm
    )


_NARROW_SPLIT_UPGRADE_SQL_TEMPLATE = """
DO $narrow_split$
DECLARE
  view_wide boolean;
  intervals_wide boolean;
  populated boolean := false;
BEGIN
  -- Static per-table blocks (no format()/EXECUTE): psycopg scans query text
  -- for percent placeholders whenever bind params ride along, so a dynamic
  -- format() form would be rejected client-side. plpgsql resolves statements
  -- lazily, so the wide-shape statements inside an untaken IF never touch
  -- dropped columns. (This comment must stay percent-free for the same reason.)
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg' AND table_name = 'attribute_effective_view'
       AND column_name = 'attrs'
  ) INTO view_wide;
  SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'attribute_effective_view_intervals'
       AND column_name = 'attrs'
  ) INTO intervals_wide;
  IF view_wide THEN
    SELECT EXISTS (
      SELECT 1 FROM okg.attribute_effective_view LIMIT 1
    ) INTO populated;
  END IF;
  IF intervals_wide AND NOT populated THEN
    SELECT EXISTS (
      SELECT 1 FROM okg.attribute_effective_view_intervals LIMIT 1
    ) INTO populated;
  END IF;
  IF (view_wide OR intervals_wide) AND populated THEN
__POPULATED_WIDE_ARM__
  END IF;

  -- From here down both wide tables are EMPTY (or already narrow): the
  -- "convergence" below is pure DDL — the UPDATE/INSERT statements match
  -- zero rows by construction.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg' AND table_name = 'attribute_effective_view'
       AND column_name = 'attrs'
  ) THEN
    ALTER TABLE okg.attribute_effective_view
      ADD COLUMN IF NOT EXISTS attrs_hash bytea;
    UPDATE okg.attribute_effective_view
       SET raw_attrs = NULL WHERE raw_attrs = attrs;
    UPDATE okg.attribute_effective_view
       SET attrs_hash = okg.attribute_content_hash(attrs, raw_attrs)
     WHERE attrs_hash IS NULL;
    INSERT INTO okg.attribute_versions
      (target_kind, target_id, attrs_hash, attrs, raw_attrs)
    SELECT target_kind, target_id, attrs_hash, attrs, raw_attrs
      FROM okg.attribute_effective_view
    ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING;
    ALTER TABLE okg.attribute_effective_view
      ALTER COLUMN attrs_hash SET NOT NULL;
    ALTER TABLE okg.attribute_effective_view
      DROP COLUMN attrs, DROP COLUMN raw_attrs;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name = 'attribute_effective_view_intervals'
       AND column_name = 'attrs'
  ) THEN
    ALTER TABLE okg.attribute_effective_view_intervals
      ADD COLUMN IF NOT EXISTS attrs_hash bytea;
    UPDATE okg.attribute_effective_view_intervals
       SET raw_attrs = NULL WHERE raw_attrs = attrs;
    UPDATE okg.attribute_effective_view_intervals
       SET attrs_hash = okg.attribute_content_hash(attrs, raw_attrs)
     WHERE attrs_hash IS NULL;
    INSERT INTO okg.attribute_versions
      (target_kind, target_id, attrs_hash, attrs, raw_attrs)
    SELECT target_kind, target_id, attrs_hash, attrs, raw_attrs
      FROM okg.attribute_effective_view_intervals
    ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING;
    ALTER TABLE okg.attribute_effective_view_intervals
      ALTER COLUMN attrs_hash SET NOT NULL;
    ALTER TABLE okg.attribute_effective_view_intervals
      DROP COLUMN attrs, DROP COLUMN raw_attrs;
  END IF;

  IF to_regclass('okg.attribute_effective_view') IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'attribute_effective_view_version_fkey'
       AND conrelid = 'okg.attribute_effective_view'::regclass
  ) THEN
    ALTER TABLE okg.attribute_effective_view
      ADD CONSTRAINT attribute_effective_view_version_fkey
      FOREIGN KEY (target_kind, target_id, attrs_hash)
      REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
      ON DELETE RESTRICT;
  END IF;
  IF to_regclass('okg.attribute_effective_view_intervals') IS NOT NULL
     AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conname = 'attribute_effective_view_intervals_version_fkey'
       AND conrelid = 'okg.attribute_effective_view_intervals'::regclass
  ) THEN
    ALTER TABLE okg.attribute_effective_view_intervals
      ADD CONSTRAINT attribute_effective_view_intervals_version_fkey
      FOREIGN KEY (target_kind, target_id, attrs_hash)
      REFERENCES okg.attribute_versions (target_kind, target_id, attrs_hash)
      ON DELETE RESTRICT;
  END IF;
END
$narrow_split$
"""


def ensure_attribute_schema(
    conn: psycopg.Connection,
    *,
    tolerate_pre_narrow_split: bool = False,
) -> None:
    """Create the internal attribute observation table for write paths.

    ``tolerate_pre_narrow_split`` is for migrate()'s internal ensures ONLY:
    it observes a populated pre-0020 wide database without refusing (the
    orchestration loop converges it through the multi-phase driver right
    after migrate's transaction commits). Every other caller refuses the
    populated wide shape loudly — see ``_narrow_split_upgrade_sql``.
    """
    with conn.cursor() as cur:
        for statement in _SCHEMA_STATEMENTS:
            cur.execute(statement)
        # schema-interval-narrow-split: converge an EMPTY pre-split wide
        # deployment; refuse (or, under migrate, tolerate) a populated one.
        cur.execute(
            _narrow_split_upgrade_sql(
                tolerate_pre_narrow_split=tolerate_pre_narrow_split
            )
        )
        cur.execute(_OKG_MCP_EFFECTIVE_VIEW_GRANTS)
    from okg.substrate.attribute_integrity import (
        ensure_attribute_integrity_schema,
    )

    ensure_attribute_integrity_schema(conn)


def ensure_attribute_schema_db(
    db: Any,
    *,
    tolerate_pre_narrow_split: bool = False,
) -> None:
    """Create the internal table through the Chronos DB adapter."""
    for statement in _SCHEMA_STATEMENTS:
        db.execute(statement)
    db.execute(
        _narrow_split_upgrade_sql(
            tolerate_pre_narrow_split=tolerate_pre_narrow_split
        )
    )
    db.execute(_OKG_MCP_EFFECTIVE_VIEW_GRANTS)


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

# Tables that VIOLATE the never-dropped-mid-process invariant and must never be
# cached. The effective VIEW is a derived read cache that okg#400 deliberately
# DROPs to reclaim disk (and test/operator resets drop it out-of-band); a stale
# positive would skip the publish-time self-heal that recreates it, leaving reads
# permanently on the slow fallback. Every check for it stays authoritative.
_TABLE_EXISTS_NEVER_CACHE: frozenset[str] = frozenset({ATTRIBUTE_EFFECTIVE_VIEW_TABLE})


def _table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    info = conn.info
    key = (info.dbname or "", str(info.host or ""), table_name)
    cacheable = table_name not in _TABLE_EXISTS_NEVER_CACHE
    if cacheable and key in _TABLE_EXISTS_CACHE:
        return True
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table_name,))
        exists = cur.fetchone()[0] is not None
    if exists:
        if cacheable:
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


def _empty_overlay_view_fast_path_enabled() -> bool:
    return os.environ.get(
        "OKG_ATTRIBUTE_EFFECTIVE_VIEW_EMPTY_OVERLAY_FAST_PATH",
    ) in {"1", "true", "TRUE", "yes", "YES"}


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

    # scale-operator-attr-reads / req.effective-attrs-view (Layer 2): keep the
    # indexed effective-attrs snapshot in parity with the overlay we just wrote.
    # At publish time this is a no-op (the view rows for this generation don't
    # exist yet -- the view materialize runs after this in the publish txn). For
    # a POST-publish overlay write + re-materialize (the standard reviewer/derived
    # write path), this propagates the new effective value into the snapshot so
    # the SQL fast path agrees with the Python read path.
    refresh_effective_attribute_view_overlay_for_generation(
        conn, generation_id=generation, namespace=namespace
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
# scale-operator-attr-reads / req.effective-attrs-view (Layer 2):
# the per-generation MATERIALIZED effective-attrs snapshot the MCP attr
# operators filter/group/aggregate over in SQL.
# --------------------------------------------------------------------------- #


def _chronos_visible_read_sql(session: Any, table: str) -> tuple[str, Any] | None:
    """Return (visible_subquery_sql, branch_point) for ``table`` on a live-branch
    Chronos session, or None if the backend doesn't expose the prepared rewrite.

    The session's prepared ref caches, per checkout, the visibility subquery for
    each registered table (``okg.graph_nodes`` -> ``SELECT ... FROM <physical>
    WHERE live_lo <= :_chronos_branch_point AND :_chronos_branch_point < live_hi
    AND deleted = FALSE``) and the segment branch_point. This is the IDENTICAL
    predicate ``session.query("... FROM okg.graph_nodes ...")`` runs (the parity
    anchor with ``_lower_program_over_chronos`` / the operator read path), so
    embedding it as a sub-select inside an INSERT...SELECT on the same connection
    materializes exactly the generation's live rows -- set-based, no per-row
    Python round-trip. We read it through the documented prepared-ref shape and
    fall back to a session.query() + staging path if the shape is absent.
    """
    ref = getattr(session, "_ref", None)
    if ref is None:
        return None
    metadata = getattr(ref, "metadata", None) or {}
    replacements = metadata.get("replacements")
    segment = metadata.get("segment")
    if not isinstance(replacements, dict) or segment is None:
        return None
    sql = replacements.get(table)
    branch_point = getattr(segment, "branch_point", None)
    if not isinstance(sql, str) or branch_point is None:
        return None
    return sql, branch_point


def _chronos_interval_physical_table(
    conn: psycopg.Connection,
    *,
    table: str,
    required_columns: Iterable[str],
) -> str | None:
    """Return the interval physical table for a logical graph table when usable."""

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT physical_table, columns
              FROM _chronos_branch_tables
             WHERE table_name = %s
               AND backend = 'interval'
            """,
            (table,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    physical_table = str(row[0])
    columns_raw = row[1]
    if isinstance(columns_raw, str):
        columns = set(json.loads(columns_raw))
    elif isinstance(columns_raw, Iterable):
        columns = set(str(col) for col in columns_raw)
    else:
        return None
    if not set(required_columns).issubset(columns):
        return None
    return physical_table


def _stage_dirty_raw_via_interval_keys(
    conn: psycopg.Connection,
    *,
    graph_table: str,
    target_kind: TargetKind,
    temp_table: str,
    merge_ids: set[str],
    branch_point: Any,
    prelock_cache_table: str | None = None,
) -> int | None:
    """Stage dirty raw rows by target id against Chronos interval storage.

    The full visible-read subquery is still the parity fallback, but for a known
    dirty set we can use the physical interval table's target-id visibility index
    directly: ``id = ANY(dirty_ids) AND live_lo <= branch_point < live_hi``.
    That preserves delete semantics (deleted rows stage zero rows) and also
    handles overlay-only changes, whose raw rows are unchanged but still visible
    at the branch point.

    With ``prelock_cache_table`` (issue 2941 fix 5) each staged row also
    carries the pre-lock hash and text digest recorded for its target, in the
    ``cached_hash`` and ``cached_digest`` columns the caller added to
    ``temp_table``; the merge decides whether the hash may be reused (see
    PrelockHashCache). The cache's primary key keeps the LEFT JOIN at one row
    per staged row.
    """

    is_node = target_kind == "node"
    if not merge_ids:
        return 0
    cache_cols = sql.SQL("")
    cache_select = sql.SQL("")
    cache_join = sql.SQL("")
    if prelock_cache_table is not None:
        cache_cols = sql.SQL(", cached_hash, cached_digest")
        cache_select = sql.SQL(", pc.attrs_hash, pc.attrs_digest")
        cache_join = sql.SQL(
            "LEFT JOIN {cache} pc ON pc.target_id = {target}"
        ).format(
            cache=_staging_identifier(prelock_cache_table),
            target=sql.SQL("n.node_id" if is_node else "e.edge_id::text"),
        )
    required = (
        ("node_id", "subtype", "attrs", "text_hash")
        if is_node
        else ("edge_id", "src", "dst", "edge_type", "attrs")
    )
    physical_table = _chronos_interval_physical_table(
        conn, table=graph_table, required_columns=required
    )
    if physical_table is None:
        return None

    with conn.cursor() as cur:
        if is_node:
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {temp} (node_id, subtype, attrs, text_hash{cache_cols})
                    SELECT DISTINCT ON (n.node_id)
                           n.node_id, n.subtype, n.attrs, n.text_hash{cache_select}
                      FROM {physical} n
                      {cache_join}
                     WHERE n.node_id = ANY(%s::text[])
                       AND n.live_lo <= %s
                       AND %s < n.live_hi
                       AND COALESCE(n.deleted, FALSE) = FALSE
                     ORDER BY n.node_id, n.live_lo DESC
                    """
                ).format(
                    temp=_staging_identifier(temp_table),
                    physical=sql.Identifier(physical_table),
                    cache_cols=cache_cols,
                    cache_select=cache_select,
                    cache_join=cache_join,
                ),
                (sorted(merge_ids), branch_point, branch_point),
            )
        else:
            try:
                edge_ids: list[int] | list[str] = sorted(
                    {int(edge_id) for edge_id in merge_ids}
                )
                edge_predicate = sql.SQL("e.edge_id = ANY(%s::bigint[])")
            except ValueError:
                edge_ids = sorted(merge_ids)
                edge_predicate = sql.SQL("e.edge_id::text = ANY(%s::text[])")
            cur.execute(
                sql.SQL(
                    """
                    INSERT INTO {temp}
                      (edge_id, src, dst, edge_type, attrs, text_hash{cache_cols})
                    SELECT DISTINCT ON (e.edge_id)
                           e.edge_id::text, e.src, e.dst, e.edge_type,
                           e.attrs, NULL::text{cache_select}
                      FROM {physical} e
                      {cache_join}
                     WHERE {edge_predicate}
                       AND e.live_lo <= %s
                       AND %s < e.live_hi
                       AND COALESCE(e.deleted, FALSE) = FALSE
                     ORDER BY e.edge_id, e.live_lo DESC
                    """
                ).format(
                    temp=_staging_identifier(temp_table),
                    physical=sql.Identifier(physical_table),
                    edge_predicate=edge_predicate,
                    cache_cols=cache_cols,
                    cache_select=cache_select,
                    cache_join=cache_join,
                ),
                (edge_ids, branch_point, branch_point),
            )
        return int(cur.rowcount or 0)


def _stage_raw_nodes_via_session(
    conn: psycopg.Connection,
    session: Any,
    *,
    temp_table: str,
) -> None:
    """Fallback raw-node staging: pull the generation's live nodes through the
    session (parity-identical visibility) and COPY them into a temp table. One
    COPY, not a per-row INSERT, so the cost is a single bulk load even at 470K."""
    rows = session.query(
        "SELECT node_id, subtype, attrs, text_hash FROM okg.graph_nodes", {}
    )
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {temp_table} "
            "(node_id TEXT, subtype TEXT, attrs JSONB, text_hash TEXT) "
            "ON COMMIT DROP"
        )
        with cur.copy(
            f"COPY {temp_table} (node_id, subtype, attrs, text_hash) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(
                    (
                        row["node_id"],
                        row["subtype"],
                        Jsonb(row["attrs"] or {}),
                        row.get("text_hash"),
                    )
                )


def _stage_raw_edges_via_session(
    conn: psycopg.Connection,
    session: Any,
    *,
    temp_table: str,
) -> None:
    rows = session.query(
        "SELECT edge_id, src, dst, edge_type, attrs FROM okg.graph_edges", {}
    )
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE TEMP TABLE {temp_table} "
            "(edge_id TEXT, src TEXT, dst TEXT, edge_type TEXT, attrs JSONB) "
            "ON COMMIT DROP"
        )
        with cur.copy(
            f"COPY {temp_table} (edge_id, src, dst, edge_type, attrs) FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(
                    (
                        str(row["edge_id"]),
                        row["src"],
                        row["dst"],
                        row["edge_type"],
                        Jsonb(row["attrs"] or {}),
                    )
                )


def _assert_no_overlay_conflict(
    conn: psycopg.Connection,
    *,
    generation_id: str,
    namespace: str,
) -> None:
    """Surface a multiple-current-accepted overlay collision LOUDLY rather than
    letting the merge silently last-wins (the no-default-fallback contract).

    Reads ``attribute_observations`` -- the source of truth where a conflict can
    actually exist (two ``accepted, superseded_by IS NULL`` rows for the same
    target/key, if the partial unique index were dodged). The overlay
    snapshot's INSERT...SELECT uses ``ON CONFLICT DO NOTHING`` against a PK that
    would otherwise collapse such a pair to one row silently; this guard catches
    it first. ``_current_overlays`` raises ``AttributeConflictError`` for the
    same condition, so both the SQL materialize path and the Python read path
    fail loudly on a conflict -- result parity on the failure case too."""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT target_kind, target_id, attr_key, count(*) AS n
              FROM {ATTRIBUTE_OBSERVATION_TABLE}
             WHERE generation_id = %s
               AND namespace = %s
               AND status = 'accepted'
               AND superseded_by IS NULL
             GROUP BY target_kind, target_id, attr_key
            HAVING count(*) > 1
             LIMIT 1
            """,
            (generation_id, namespace),
        )
        row = cur.fetchone()
    if row is not None:
        raise AttributeConflictError(
            "multiple current accepted attribute observations",
            data={
                "generation_id": generation_id,
                "namespace": namespace,
                "target_kind": row[0],
                "target_id": row[1],
                "attr_key": row[2],
            },
            fix_hint="supersede or reject all but one accepted observation",
        )


def materialize_effective_attribute_view_for_generation(
    conn: psycopg.Connection,
    *,
    generation_id: int | str,
    branch: str = "default",
    previous_generation_id: int | str | None = None,
    namespace: str = DEFAULT_NAMESPACE,
    from_checkpoint: bool = False,
    empty_overlay_fast_path: bool | None = None,
    dirty_targets: DirtyTargets | None = None,
    graph_store: Any | None = None,
    prelock_hash_cache: "PrelockHashCache | None" = None,
) -> dict[str, Any]:
    """Build the indexed effective-attrs SNAPSHOT for one generation.

    Effective attrs = the raw graph attrs (sourced from the SAME Chronos
    visibility the operators read) merged with the accepted overlay snapshot
    (``ATTRIBUTE_EFFECTIVE_TABLE``) for this generation. Merge semantics match
    ``apply_effective_attrs_to_rows`` exactly:

      * accepted overlay OVERRIDES raw  -> ``raw || overlay`` (jsonb || is
        right-biased == Python ``attrs[key] = value``);
      * reserved effective keys are excluded from the OVERLAY (not from raw --
        Python never strips raw), via ``attr_key <> ALL(reserved)`` on the
        overlay CTE;
      * a multiple-current-accepted conflict is surfaced (AttributeConflictError),
        never silently last-wins.

    Generation-pinned: every row carries the opaque ``generation_id`` string.
    Idempotent: a re-run for an already-materialized generation is a no-op.

    At PUBLISH (``from_checkpoint=False``) this runs inside the publish txn after
    the overlay snapshot is written and BEFORE ``checkpoint_generation`` -- the new
    generation's checkpoint does not exist yet, so the raw base is read from the
    live-branch checkout (the rows about to BE this generation).

    At BACKFILL / migrate (``from_checkpoint=True``) the generation is ALREADY a
    published checkpoint, so the raw base is read from
    ``checkout_generation(generation_id)`` (the pinned past generation), NOT the
    live branch head (which may have moved on).

    INCREMENTAL: when ``previous_generation_id`` has a snapshot, unchanged targets
    (same raw_text_hash + overlay) are carried forward verbatim from the prior
    snapshot and only changed/new targets are re-merged (steady state O(changed)),
    byte-identical to a full re-materialize.

    ``prelock_hash_cache`` (issue 2941 fixes 2 and 5) is the publisher's
    pre-lock hash pass. A full scan or a dirty-driven merge reuses a cached
    hash only for a row whose hash inputs match the cached ones (see
    PrelockHashCache); the result's ``prelock_hash`` says per kind how many
    rows did, or why none could.
    """
    _require_attribute_schema(conn)
    generation = str(generation_id)
    if not namespace:
        raise ValueError("namespace must be non-empty")
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE):
        # Self-heal (okg#400): the view is a DERIVED read cache. If it was
        # dropped -- e.g. to reclaim disk on a constrained deployment -- recreate
        # it here so THIS publish rebuilds it, instead of silently no-op'ing and
        # leaving operator reads permanently on the slow fallback path until a
        # re-migrate. ensure_attribute_schema is CREATE TABLE IF NOT EXISTS, so
        # it is a no-op when the table is already present.
        ensure_attribute_schema(conn)
    use_empty_overlay_fast_path = (
        _empty_overlay_view_fast_path_enabled()
        if empty_overlay_fast_path is None
        else bool(empty_overlay_fast_path)
    )
    if use_empty_overlay_fast_path and _attribute_materialization_state_empty(conn):
        # Defense in depth (issue #647, fix-ivm-fresh-db-publish): the
        # interval sync derives its open intervals FROM this snapshot, so
        # skipping the snapshot under the interval arm starves it — zero
        # intervals vs the full graph count fails the first-publish
        # reconcile on every fresh database. The publisher call site
        # already excludes the combination; mirror it here for direct
        # callers of the materialize.
        from okg.substrate.graph_store import (
            ChronosGraphStore as _FastPathStore,
        )
        from okg.substrate.graph_store_internal.generation_ordinal import (
            interval_representation_flag_enabled as _fast_path_flag,
        )
        from okg.substrate.graph_store_internal.generation_ordinal import (
            is_interval_tracked_branch as _fast_path_itb,
        )

        # Flag half first so the branch probe (and the store it needs) is not
        # built at all when configuration already rules the interval arm out.
        fast_path_store = graph_store or _FastPathStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=bool(getattr(conn, "autocommit", True)),
        )
        _fast_path_interval_arm = _fast_path_flag() and _fast_path_itb(
            fast_path_store.context.db,
            branch,
        )
        if not _fast_path_interval_arm:
            return {
                "generation_id": generation,
                "previous_generation_id": (
                    str(previous_generation_id)
                    if previous_generation_id else None
                ),
                "namespace": namespace,
                "materialized": True,
                "reason": "empty_overlay_identity",
                "node_rows": 0,
                "edge_rows": 0,
                "prelock_hash": _prelock_unused_report(
                    prelock_hash_cache, "empty-overlay fast path wrote no rows"
                ),
            }

    from okg.substrate.graph_store import ChronosGraphStore

    store = graph_store or ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
        autocommit=bool(getattr(conn, "autocommit", True)),
    )
    if from_checkpoint:
        session = store.checkout_generation(generation_id, branch=branch)
    else:
        session = store.checkout(branch)
    db = store.context.db

    reserved = sorted(RESERVED_EFFECTIVE_ATTR_KEYS)
    # Issue 2941: how each kind's content hash ran (parallel, serial, or
    # unavailable with its reason); surfaced in the result below.
    parallel_hash: dict[str, str] = {}
    prelock_report: dict[str, Any] = {}
    prelock_tables: dict[str, str] = {}
    if prelock_hash_cache is not None:
        unusable = _prelock_cache_unusable_reason(conn, prelock_hash_cache)
        if unusable is None:
            prelock_tables = dict(prelock_hash_cache.tables)
        else:
            prelock_report = {"node": f"unavailable: {unusable}",
                              "edge": f"unavailable: {unusable}"}
            logger.warning(
                "effective attribute view: pre-lock hash cache not used (%s); "
                "hashing every row under the lock",
                unusable,
            )
    previous = (
        str(previous_generation_id) if previous_generation_id else None
    )

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
                 WHERE generation_id = %s AND namespace = %s LIMIT 1
                """,
                (generation, namespace),
            )
            if cur.fetchone() is not None:
                return {
                    "generation_id": generation,
                    "namespace": namespace,
                    "materialized": False,
                    "reason": "already_materialized",
                    "node_rows": 0,
                    "edge_rows": 0,
                    "prelock_hash": _prelock_unused_report(
                        prelock_hash_cache, "generation already materialized"
                    ),
                }

        _assert_no_overlay_conflict(
            conn, generation_id=generation, namespace=namespace
        )

        # The carry-forward is dropped (snapshot -> changed-only) only when the
        # interval path OWNS this branch. A STRUCTURAL fork (no recorded base
        # ordinal) keeps the full snapshot and reads fall back to it -- correct,
        # just not interval-accelerated (task.branch-fork-correctness).
        # This MUST be the same predicate the publisher uses to decide whether
        # to run the interval sync: dropping the carry-forward on the strength
        # of a sync that never ran leaves the unchanged majority of rows in
        # neither the snapshot nor the interval table (issue #1514 -- the
        # ordinal kill switch stopped the publisher's sync while this call site
        # still read only OKG_IVM_INTERVAL_VIEW).
        from okg.substrate.graph_store_internal.generation_ordinal import (
            interval_representation_enabled,
        )

        _interval_view = interval_representation_enabled(db, branch)

        node_rows = _materialize_view_kind(
            conn,
            session,
            db,
            generation=generation,
            namespace=namespace,
            reserved=reserved,
            target_kind="node",
            previous_generation=previous,
            dirty_ids=(dirty_targets.node_ids if dirty_targets is not None else None),
            interval_view=_interval_view,
            hash_report=parallel_hash,
            prelock_cache_table=prelock_tables.get("node"),
            prelock_report=prelock_report,
        )
        edge_rows = _materialize_view_kind(
            conn,
            session,
            db,
            generation=generation,
            namespace=namespace,
            reserved=reserved,
            target_kind="edge",
            previous_generation=previous,
            dirty_ids=(dirty_targets.edge_ids if dirty_targets is not None else None),
            interval_view=_interval_view,
            hash_report=parallel_hash,
            prelock_cache_table=prelock_tables.get("edge"),
            prelock_report=prelock_report,
        )
        # Materialization happens before the publish gate records its checkpoint.
        # Protect the candidate explicitly and reserve the rest of the retention
        # budget for already-published snapshots. Otherwise two failed candidates
        # can evict the last readable published snapshot.
        pruned = prune_effective_attribute_view_to_recent(
            conn,
            protected_generation_id=generation,
        )

    return {
        "generation_id": generation,
        "previous_generation_id": previous,
        "namespace": namespace,
        "materialized": True,
        "node_rows": node_rows,
        "edge_rows": edge_rows,
        "pruned_old_generation_rows": pruned,
        "parallel_hash": parallel_hash,
        "prelock_hash": prelock_report or None,
    }


# ivm-effective-attribute-view / task.interval-row-representation: the payload
# columns an interval row mirrors from the snapshot. The "unchanged" decision (keep
# an interval OPEN vs close+reopen) compares ALL of these so a kept-open interval is
# byte-identical to what a full per-generation snapshot would have stored.
# schema-interval-narrow-split (req.sigma-pinned): the jsonb payloads are replaced
# by ``attrs_hash`` -- the content hash computed by okg.attribute_content_hash at
# write time -- so the keep-open-vs-close comparison IS the hash comparison and a
# jsonb-equal payload (key order, numeric scale) can never churn an interval.
_INTERVAL_PAYLOAD_COLS = (
    "subtype",
    "src",
    "dst",
    "edge_type",
    "overlay_count",
    "raw_text_hash",
    "attrs_hash",
)
# The columns the effective-view READERS return; the interval stab returns exactly
# these so a stab at ordinal(G) is byte-identical to the snapshot at generation G.
# ``attrs`` / ``raw_attrs`` are MATERIALIZED through the version join
# (okg.attribute_versions) -- see ``effective_interval_stab_rows`` /
# ``ATTRIBUTE_VERSION_JOIN_SQL``; the read contract is unchanged by the split.
_INTERVAL_READ_COLS = (
    "target_id",
    "subtype",
    "src",
    "dst",
    "edge_type",
    "attrs",
    "raw_attrs",
    "raw_text_hash",
)

_EFFECTIVE_INTERVAL_SPEC = ExplicitIntervalTable(
    table=ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    snapshot_table=ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    payload_columns=_INTERVAL_PAYLOAD_COLS,
    read_columns=_INTERVAL_READ_COLS,
    target_kinds=("node", "edge"),
)


def _interval_sync_temp_name(prefix: str, target_kind: TargetKind) -> str:
    return explicit_interval_sync_temp_name(prefix, target_kind)


def _close_dirty_effective_intervals(
    cur: Any,
    *,
    iv: str,
    snap: str,
    target_kind: TargetKind,
    params: dict[str, Any],
    close_ids: set[str],
) -> int:
    """Compatibility wrapper around the shared explicit-interval close helper."""
    spec = (
        _EFFECTIVE_INTERVAL_SPEC
        if iv == ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
        and snap == ATTRIBUTE_EFFECTIVE_VIEW_TABLE
        else ExplicitIntervalTable(
            table=iv,
            snapshot_table=snap,
            payload_columns=_INTERVAL_PAYLOAD_COLS,
            read_columns=_INTERVAL_READ_COLS,
            target_kinds=("node", "edge"),
        )
    )
    return close_dirty_explicit_intervals(
        cur,
        spec=spec,
        target_kind=target_kind,
        params=params,
        close_ids=close_ids,
    )


def sync_effective_interval_view_for_generation(
    conn: psycopg.Connection,
    db: Any,
    *,
    generation_id: int | str,
    namespace: str = DEFAULT_NAMESPACE,
    dirty_targets: "DirtyTargets | None" = None,
) -> dict[str, Any]:
    """Fold the freshly-materialized snapshot for ``generation_id`` into the shared
    interval table (``ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE``).

    Writes O(changed) rows against the interval table:

      * CLOSE (``to_ord = ordinal(G)``) every OPEN interval the snapshot does NOT
        confirm unchanged -- an updated target (payload differs) or a retracted
        target (no snapshot row at G);
      * OPEN a new ``[ordinal(G), OPEN)`` interval for every snapshot target without
        a matching OPEN interval -- a new target, or one just closed by an update;
      * leave UNCHANGED targets' open intervals untouched (a no-op writes ZERO rows).

    Two modes, by ``dirty_targets``:

      * **Full mode** (``dirty_targets is None``): the snapshot is the FULL live set,
        so a retract is detected by ABSENCE (an open interval whose target has no
        snapshot row). Requires a full snapshot (the flag-off / from-scratch path).
      * **Delta mode** (``dirty_targets`` given): the snapshot is CHANGED-ONLY (the
        interval cutover drops the O(total) carry-forward), so absence no longer
        means retraction -- it usually means UNCHANGED. The close is therefore
        restricted to the dirty set (``target_id = ANY(dirty)``): a dirty target not
        confirmed-unchanged by the snapshot is retracted (in dirty, absent from
        snapshot) or updated (in dirty, snapshot differs); a NON-dirty target is
        never touched. ``retracted == dirty - snapshot`` -- the dirty scope includes
        retracts (a retract is a graph write), proven by the retract oracle.

    Requires ``generation_id`` to already carry a generation ordinal (minted at
    ``checkpoint_generation``), so this runs AFTER the checkpoint. Idempotent.
    """
    gen = str(generation_id)
    dirty_by_kind = (
        None
        if dirty_targets is None
        else {
            "node": dirty_targets.node_ids,
            "edge": dirty_targets.edge_ids,
        }
    )
    return sync_explicit_interval_rows_for_generation(
        conn,
        db,
        spec=_EFFECTIVE_INTERVAL_SPEC,
        generation_id=gen,
        namespace=namespace,
        dirty_target_ids_by_kind=dirty_by_kind,
        grant_fn=_grant_interval_view_to_mcp,
    )


def reanchor_effective_interval_view_for_generation(
    conn: psycopg.Connection,
    db: Any,
    *,
    generation_id: int | str,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """Re-derive the whole open interval front from a full snapshot.

    The publisher calls this at a full-proof boundary (first publish, a
    blocked generation in between, a dirty integrity proof, or the count-stamp
    cadence). The generation's effective view is complete, so every open
    interval is checked against it:

      * an open interval whose payload matches its target's snapshot row stays
        OPEN and is not rewritten (okg#2314: closing and reopening unchanged
        targets added one row per target per reanchor);
      * an open interval whose payload differs, whose target is absent from the
        snapshot, or which is one of several open intervals for the same target
        is CLOSED at this ordinal -- this is the repair: a stored payload that
        drifted from the recomputed view never survives a reanchor;
      * every snapshot target left without an open interval gets a new one at
        this ordinal.

    "Matches" is ``explicit_interval_payload_unchanged_sql`` -- the same rule
    the incremental and full syncs use. The payload columns are narrow (the
    jsonb payload is represented by ``attrs_hash``), so the comparison is a
    join on small columns, not on JSON documents.

    The transaction is idempotent for a fresh generation ordinal: current rows
    at that ordinal are deleted before the front is re-derived.

    Both steps read a staged, analyzed copy of this generation's snapshot
    rows (``stage_explicit_snapshot_slice``), never the shared snapshot table
    directly. The generation was written moments ago, so the shared table's
    statistics do not know it; probing it once per open interval read the
    whole generation per probe and ran past the statement timeout at 300k
    targets (okg#1718).
    """
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    generation = str(generation_id)
    branch_id, ordinal = resolve_generation_ordinal_row(db, generation)
    unchanged = explicit_interval_payload_unchanged_sql(_EFFECTIVE_INTERVAL_SPEC)
    counts: dict[str, dict[str, int]] = {}
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"LOCK TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
        for target_kind in ("node", "edge"):
            params = {
                "br": branch_id,
                "ns": namespace,
                "tk": target_kind,
                "new": ordinal,
                "open": EFFECTIVE_INTERVAL_OPEN_ORD,
                "gen": generation,
            }
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %(br)s AND namespace = %(ns)s "
                "AND target_kind = %(tk)s AND from_ord = %(new)s",
                params,
            )
            deleted_current = int(cur.rowcount or 0)
            snapshot = stage_explicit_snapshot_slice(
                cur,
                spec=_EFFECTIVE_INTERVAL_SPEC,
                target_kind=target_kind,
                params=params,
            )
            # Keep an open interval only when it is its target's single open
            # interval AND the snapshot confirms its payload unchanged.
            cur.execute(
                f"""
                UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} AS i
                   SET to_ord = %(new)s, materialized_at = now()
                 WHERE i.branch_id = %(br)s AND i.namespace = %(ns)s
                   AND i.target_kind = %(tk)s
                   AND i.to_ord = %(open)s AND i.from_ord < %(new)s
                   AND (
                     NOT EXISTS (
                       SELECT 1 FROM {snapshot} AS s
                        WHERE s.target_id = i.target_id
                          AND {unchanged}
                     )
                     OR EXISTS (
                       SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} AS d
                        WHERE d.branch_id = i.branch_id
                          AND d.namespace = i.namespace
                          AND d.target_kind = i.target_kind
                          AND d.target_id = i.target_id
                          AND d.to_ord = %(open)s
                          AND d.from_ord < %(new)s
                          AND d.from_ord <> i.from_ord
                     )
                   )
                """,
                params,
            )
            closed_older = int(cur.rowcount or 0)
            opened = open_missing_explicit_intervals_from_staged(
                cur,
                spec=_EFFECTIVE_INTERVAL_SPEC,
                params=params,
                staged_snapshot=snapshot,
            )
            cur.execute(f"DROP TABLE IF EXISTS {snapshot}")
            counts[target_kind] = {
                "deleted_current": deleted_current,
                "closed": closed_older,
                "opened": opened,
            }
    _grant_interval_view_to_mcp(conn)
    return {
        "generation_id": generation,
        "branch_id": branch_id,
        "ordinal": ordinal,
        "mode": "full_reanchor",
        "counts": counts,
        "rows_written": sum(
            kind_counts["deleted_current"]
            + kind_counts["closed"]
            + kind_counts["opened"]
            for kind_counts in counts.values()
        ),
    }


def _effective_interval_stab_counts_for_generation(
    conn: psycopg.Connection,
    *,
    branch_id: str,
    ordinal: int,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, int]:
    """Count interval rows visible at one published ordinal.

    This is the exact fallback used only when the constant-size integrity receipt
    cannot prove the predecessor. It deliberately stabs the published predecessor,
    not the mutable OPEN front: a failed candidate may have committed interval
    writes at a later ordinal without becoming published.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT target_kind, count(*) "
            f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE branch_id = %s AND namespace = %s "
            "AND from_ord <= %s AND %s < to_ord GROUP BY target_kind",
            (branch_id, namespace, int(ordinal), int(ordinal)),
        )
        counts = {str(kind): int(count) for kind, count in cur.fetchall()}
    return {"node": counts.get("node", 0), "edge": counts.get("edge", 0)}


#: Marker reason stamped into a checkpoint's ``effective_interval_view_sync``
#: metadata when the publisher deliberately skipped this generation's interval
#: sync. An intermediate publish (``cycle(native_view_refresh_final=False)``)
#: defers its effective-attribute snapshot, so there is nothing to fold and the
#: interval table intentionally stays at the last generation that DID sync.
DEFERRED_INTERVAL_SYNC_REASON = "deferred_intermediate_publish"


def resolve_interval_precondition_anchor(
    checkpoint: Any,
    *,
    load_checkpoint: Callable[[str], Any],
    max_steps: int = 64,
) -> dict[str, Any]:
    """Pick the generation whose interval rows the backfill precondition checks.

    ``require_effective_interval_backfill_precondition`` compares a published
    generation's checkpoint node/edge counts against the intervals open at that
    generation's ordinal. That equality only holds for a generation that folded
    itself into the interval table. A DELIBERATELY deferred generation (an
    intermediate publish) folds nothing, so its counts legitimately run ahead of
    the interval rows and the comparison refuses a healthy deployment with a
    repair instruction the operator cannot act on (issue #1927; the precondition
    from issue #774 predates the deferral contract).

    Step back over each generation whose checkpoint EXPLICITLY records a
    deferred interval sync and return the first one that does not. Nothing else
    moves the anchor: a checkpoint with no marker (published before the marker
    existed, or built by compaction, which deliberately does not inherit one)
    stops the walk where it stands, and a chain longer than ``max_steps`` returns
    the CALLER'S OWN ``checkpoint`` so the caller's original comparison is what
    runs.

    Two cases deliberately leave the caller refusing:

    * A deferred generation whose predecessor cannot be read. Nothing here can
      tell a compacted-away predecessor from a corrupted chain.
    * A deferred generation with NO predecessor -- the branch's first publish was
      itself intermediate, so no generation on it has ever folded into the
      interval table. That is indistinguishable from the unbackfilled deployment
      issue #774's refusal exists to catch, and the refusal's own fix is the
      right remedy for both:
      ``okg db repair-effective-attribute-intervals`` rebuilds a full snapshot
      from the published graph and reconciles the intervals from it, so the
      operator has an action that works.

    Pure with respect to the database: ``load_checkpoint`` supplies each
    predecessor, so this is unit-testable without a publisher or a server.
    """
    current = checkpoint
    steps = 0
    while steps < max_steps:
        metadata = getattr(current, "metadata", None) or {}
        marker = metadata.get("effective_interval_view_sync")
        if not isinstance(marker, dict):
            return {
                "checkpoint": current,
                "steps": steps,
                "reason": "sync_state_unrecorded",
            }
        if marker.get("reason") != DEFERRED_INTERVAL_SYNC_REASON:
            return {
                "checkpoint": current,
                "steps": steps,
                "reason": (
                    "interval_sync_ran"
                    if marker.get("ran") is True
                    else str(marker.get("reason"))
                ),
            }
        predecessor_id = metadata.get("predecessor_generation_id")
        if not predecessor_id:
            return {
                "checkpoint": current,
                "steps": steps,
                "reason": "deferred_without_recorded_predecessor",
            }
        try:
            predecessor = load_checkpoint(str(predecessor_id))
        except Exception:  # noqa: BLE001 -- an unreadable predecessor keeps the
            # caller's fail-closed comparison rather than inventing an anchor.
            predecessor = None
        if predecessor is None:
            return {
                "checkpoint": current,
                "steps": steps,
                "reason": "deferred_predecessor_unavailable",
            }
        current = predecessor
        steps += 1
    # Hand back the CALLER'S checkpoint, not the one the walk stopped on: an
    # unbounded deferred chain is an unproven state, and anchoring on a
    # generation 64 back would check counts nothing in between has proven.
    return {
        "checkpoint": checkpoint,
        "steps": steps,
        "reason": "deferred_chain_longer_than_max_steps",
    }


def require_effective_interval_backfill_precondition(
    conn: psycopg.Connection,
    db: Any,
    *,
    deployment_name: str,
    branch: str,
    generation_id: int | str,
    checkpoint_counts: dict[str, int] | None,
    namespace: str = DEFAULT_NAMESPACE,
    anchor_note: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Refuse an unbackfilled existing deployment before interval fast-path work.

    A clean attribute-integrity receipt is the O(1) steady-state proof. Legacy or
    crash-recovery state falls back to an exact count at the published predecessor's
    ordinal. No branch-head read and no repair occurs here.

    ``generation_id`` is the ANCHOR generation the caller resolved with
    ``resolve_interval_precondition_anchor`` -- normally the published
    predecessor, but the last generation that actually folded itself into the
    interval table when the predecessor deliberately deferred its sync.
    ``anchor_note`` is set ONLY when the anchor moved, and carries that
    resolution into a refusal so an operator can see the checked generation is
    not the immediate predecessor.
    """
    from okg.substrate.attribute_integrity import (
        ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,
        ATTRIBUTE_INTEGRITY_STATE_TABLE,
        _load_receipt,
        _receipt_is_well_formed,
        check_attribute_integrity_fast_path,
    )
    from okg.substrate.errors import EffectiveIntervalBackfillRequired
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    fix_hint = (
        f"run `{okg_prefix()} db repair-effective-attribute-intervals "
        f"--deployment {deployment_name} --apply` before enabling "
        "OKG_IVM_INTERVAL_VIEW"
    )

    def refuse(
        reason: str,
        *,
        expected: dict[str, int] | None,
        observed: dict[str, int] | None,
        ordinal: int | None = None,
        receipt_reason: str | None = None,
    ) -> None:
        raise EffectiveIntervalBackfillRequired(
            "effective-attribute interval fast path requires an explicit backfill",
            data={
                "reason": reason,
                "deployment_name": deployment_name,
                "branch": branch,
                "predecessor_generation_id": str(generation_id),
                "predecessor_ordinal": ordinal,
                "expected_counts": expected,
                "observed_counts": observed,
                "receipt_reason": receipt_reason,
                # Present only when the checked generation is NOT the immediate
                # predecessor, so the payload's shape itself says whether
                # `predecessor_generation_id` needs a second reading.
                **({"anchor": dict(anchor_note)} if anchor_note else {}),
            },
            fix_hint=fix_hint,
        )

    try:
        expected = {
            "node": int(checkpoint_counts["node"]),
            "edge": int(checkpoint_counts["edge"]),
        }
    except (KeyError, TypeError, ValueError):
        refuse(
            "predecessor_checkpoint_counts_unavailable",
            expected=None,
            observed=None,
        )
        raise AssertionError("unreachable")

    try:
        branch_id, ordinal = resolve_generation_ordinal_row(db, str(generation_id))
    except Exception as exc:  # noqa: BLE001 -- normalize every unavailable ordinal
        refuse(
            "predecessor_ordinal_unavailable",
            expected=expected,
            observed=None,
            receipt_reason=type(exc).__name__,
        )
        raise AssertionError("unreachable")
    if branch_id != branch:
        refuse(
            "predecessor_branch_mismatch",
            expected=expected,
            observed=None,
            ordinal=ordinal,
        )

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,))
        interval_table = cur.fetchone()[0]
    if interval_table is None:
        refuse(
            "interval_table_missing",
            expected=expected,
            observed=None,
            ordinal=ordinal,
        )

    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass(%s), to_regclass(%s)",
            (ATTRIBUTE_INTEGRITY_STATE_TABLE, ATTRIBUTE_INTEGRITY_RECEIPT_TABLE),
        )
        integrity_state_table, integrity_receipt_table = cur.fetchone()
    receipt_authority_available = (
        integrity_state_table is not None and integrity_receipt_table is not None
    )
    receipt: dict[str, Any] = {
        "ok": False,
        "reason": "receipt_authority_unavailable",
    }
    if receipt_authority_available:
        try:
            # A legacy or partially migrated receipt schema can raise while it is
            # being read. Isolate the probe so its SQL error cannot poison the
            # publisher transaction before the named cutover refusal is formed.
            with conn.transaction():
                receipt = check_attribute_integrity_fast_path(
                    conn,
                    deployment_name=deployment_name,
                    branch=branch,
                    namespace=namespace,
                    generation_id=str(generation_id),
                    ordinal=ordinal,
                    checkpoint_counts=expected,
                    register_scope=False,
                )
        except Exception as exc:  # noqa: BLE001 -- exact fallback handles legacy
            receipt = {
                "ok": False,
                "reason": "receipt_check_unavailable",
                "error_type": type(exc).__name__,
            }
    if receipt.get("ok") is True:
        return {
            "ok": True,
            "mode": "integrity_receipt",
            "generation_id": str(generation_id),
            "branch_id": branch_id,
            "ordinal": ordinal,
            "counts": expected,
            "receipt_digest": receipt.get("receipt_digest"),
        }

    # Preserve the existing receipt-backed recovery contract (issue #1114 owns
    # that path): a protected mutation after a proven generation dirties the
    # receipt and deliberately forces the publisher's full reanchor. Issue #774
    # is the distinct legacy-cutover state with no prior proof at all.
    has_verified_prior_receipt = False
    if receipt_authority_available:
        try:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT latest_receipt_digest "
                        f"FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                        "WHERE deployment_name = %s AND branch_id = %s "
                        "AND namespace = %s",
                        (deployment_name, branch, namespace),
                    )
                    state_row = cur.fetchone()
                prior_digest = state_row[0] if state_row else None
                prior_receipt = _load_receipt(conn, prior_digest)
                has_verified_prior_receipt = bool(
                    _receipt_is_well_formed(prior_receipt)
                    and prior_receipt.get("deployment_name") == deployment_name
                    and prior_receipt.get("branch_id") == branch
                    and prior_receipt.get("namespace") == namespace
                    and str(prior_receipt.get("generation_id"))
                    == str(generation_id)
                    and int(prior_receipt.get("ordinal")) == int(ordinal)
                )
        except Exception:  # noqa: BLE001 -- missing/malformed authority is unverified
            has_verified_prior_receipt = False

    observed = _effective_interval_stab_counts_for_generation(
        conn,
        branch_id=branch_id,
        ordinal=ordinal,
        namespace=namespace,
    )
    if observed != expected and not has_verified_prior_receipt:
        refuse(
            "predecessor_interval_count_mismatch",
            expected=expected,
            observed=observed,
            ordinal=ordinal,
            receipt_reason=str(receipt.get("reason") or "unknown"),
        )
    return {
        "ok": True,
        "mode": (
            "receipt_backed_recovery"
            if observed != expected
            else "exact_predecessor_count"
        ),
        "generation_id": str(generation_id),
        "branch_id": branch_id,
        "ordinal": ordinal,
        "counts": observed,
        "expected_counts": expected,
        "receipt_reason": receipt.get("reason"),
    }


def backfill_effective_interval_view_for_generation(
    conn: psycopg.Connection,
    db: Any,
    *,
    generation_id: int | str,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """One-time backfill so OKG_IVM_INTERVAL_VIEW can be enabled on an EXISTING
    deployment.

    Opens an interval per LIVE target at ordinal(generation_id) from the generation's
    FULL snapshot. A flag-OFF deployment keeps full per-generation snapshots and mints
    the generation ordinal on every publish, so the inputs exist; run this for the
    LATEST published generation just before flipping the flag. Until the interval
    table is backfilled, the per-publish reconcile FAILS the first flag-on publish
    (open-count < live count) -- this turns that into a deliberate one-time step.

    Uses sync in FULL mode (``dirty_targets=None`` -> the snapshot is the full live
    set), then verifies the resulting OPEN-interval count equals the snapshot row
    count -- failing loud if it does not (e.g. the snapshot was already changed-only,
    which means the flag was partially on; backfill requires a FULL snapshot).
    Idempotent: a re-run reconciles the interval table to the snapshot.
    """
    with conn.transaction():
        # Serialize with the publisher's interval sync and the repairs, which
        # take the same lock (okg#3112 step 4b removed the attr_index branch
        # lock this used to take first).
        with conn.cursor() as cur:
            cur.execute(
                f"LOCK TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "IN SHARE ROW EXCLUSIVE MODE"
            )
        result = backfill_explicit_interval_rows_for_generation(
            conn,
            db,
            spec=_EFFECTIVE_INTERVAL_SPEC,
            generation_id=generation_id,
            namespace=namespace,
            grant_fn=_grant_interval_view_to_mcp,
        )
    return result


def _grant_interval_view_to_mcp(conn: psycopg.Connection) -> None:
    """Best-effort: let the constrained okg_mcp read pool SELECT the interval
    table + the ordinal registry the interval reader resolves from. Mirrors the
    chronos public-table grant: only matters when OKG_IVM_INTERVAL_VIEW is on
    (exactly when sync runs), and a missing okg_mcp role or insufficient grant
    authority is non-fatal -- the reader falls back to the snapshot. Runs in a
    SAVEPOINT so a failed GRANT can never poison the caller's transaction."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp'")
            if cur.fetchone() is None:
                return
            # Cheap idempotence: the grant only needs to land ONCE, so skip the
            # savepoint+GRANT on every later sync (a catalog lookup, not per-publish
            # DDL). Without this the grant DO-work runs on every flag-on publish.
            cur.execute(
                "SELECT has_table_privilege('okg_mcp', %s, 'SELECT') "
                "AND has_table_privilege('okg_mcp', %s, 'SELECT')",
                (
                    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
                    ATTRIBUTE_VERSIONS_TABLE,
                ),
            )
            if cur.fetchone()[0]:
                return
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"GRANT SELECT ON {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}, "
                    f"{ATTRIBUTE_VERSIONS_TABLE}, "
                    "okg.generation_ordinals TO okg_mcp"
                )
    except Exception:  # noqa: BLE001 -- provisioning aid, never load-bearing
        pass


# The SQL join fragment every effective-attr reader materializes payloads
# through (schema-interval-narrow-split): base-table alias ``{alias}`` joined to
# the version store on the composite FK key. Readers select ``av.attrs`` /
# ``av.raw_attrs`` -- presence-means-overlay NULL semantics carried verbatim.
def attribute_version_join_sql(alias: str, *, version_alias: str = "av") -> str:
    return (
        f"JOIN {ATTRIBUTE_VERSIONS_TABLE} {version_alias} "
        f"ON {version_alias}.target_kind = {alias}.target_kind "
        f"AND {version_alias}.target_id = {alias}.target_id "
        f"AND {version_alias}.attrs_hash = {alias}.attrs_hash"
    )


def effective_snapshot_read_rows(
    conn: psycopg.Connection,
    *,
    generation_id: int | str,
    target_kind: TargetKind,
    namespace: str = DEFAULT_NAMESPACE,
) -> list[tuple[Any, ...]]:
    """Read one generation's snapshot rows in ``_INTERVAL_READ_COLS`` order,
    payloads materialized through the version-store join. The snapshot-side
    ground truth the interval-stab oracle compares against."""
    cols = ", ".join(
        f"av.{column}" if column in ("attrs", "raw_attrs") else f"s.{column}"
        for column in _INTERVAL_READ_COLS
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {cols}
              FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} s
              {attribute_version_join_sql("s")}
             WHERE s.generation_id = %(gen)s
               AND s.namespace = %(ns)s
               AND s.target_kind = %(tk)s
            """,
            {"gen": str(generation_id), "ns": namespace, "tk": target_kind},
        )
        return [tuple(row) for row in cur.fetchall()]


def effective_interval_stab_rows(
    conn: psycopg.Connection,
    db: Any,
    *,
    generation_id: int | str,
    target_kind: TargetKind,
    namespace: str = DEFAULT_NAMESPACE,
) -> list[tuple[Any, ...]]:
    """Stab the interval table at ``ordinal(generation_id)`` and return the rows
    live at that generation, in ``_INTERVAL_READ_COLS`` order. This is the
    interval-model equivalent of selecting the snapshot for one generation.
    The jsonb payloads are materialized through the version-store join, so the
    read contract (columns and byte content) is unchanged by the narrow split.
    """
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    branch_id, g_ord = resolve_generation_ordinal_row(db, str(generation_id))
    cols = ", ".join(
        f"av.{column}" if column in ("attrs", "raw_attrs") else f"i.{column}"
        for column in _INTERVAL_READ_COLS
    )
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {cols}
              FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
              {attribute_version_join_sql("i")}
             WHERE i.branch_id = %(br)s
               AND i.namespace = %(ns)s
               AND i.target_kind = %(tk)s
               AND i.from_ord <= %(g)s
               AND %(g)s < i.to_ord
            """,
            {"br": branch_id, "ns": namespace, "tk": target_kind, "g": g_ord},
        )
        return [tuple(row) for row in cur.fetchall()]


# scale-operator-attr-reads / req.effective-attrs-view (Layer 2): instrumentation
# the incremental-materialize tests read to PROVE steady-state carry-forward does
# O(changed) MERGE work (not O(all)). ``merged_rows`` = rows that paid the full
# ``raw || overlay`` jsonb merge + overlay-aggregate join; ``carried_rows`` = rows
# copied verbatim from the prior snapshot's already-merged ``attrs`` (no recompute,
# no overlay join). NOTE: carry-forward still INSERTS a row per (target,
# generation) -- the snapshot is generation-keyed -- so the GIN/insert cost is not
# eliminated; what carry-forward removes is the per-row jsonb-merge CPU + overlay
# join for the unchanged majority. Best-effort, never load-bearing for correctness.
_MATERIALIZE_INSTRUMENTATION: dict[str, int] = {
    "merged_rows": 0,
    "carried_rows": 0,
    "kinds": 0,
    # ivm-effective-attribute-view / task.plumb-dirty-scope: rows STAGED into the
    # temp raw set. In the full-scan path this is the whole chronos-visible set
    # (O(total)); in the dirty-driven path it is only the changed/new + overlay-
    # changed targets (O(changed)), and 0 for a no-op publish. The carry-forward
    # INSERT count stays O(total) until task.interval-row-representation.
    "staged_rows": 0,
    # Issue 2941: kinds whose raw rows were staged in an UNLOGGED table (so the
    # content-hash statement may run on parallel workers), and kinds whose hash
    # statement ran with the parallel planner settings applied.
    "unlogged_stages": 0,
    "parallel_hash_stages": 0,
    # Kinds that would have staged unlogged but could not (no CREATE on the
    # okg schema, or the operator disabled parallel query). They ran the old
    # serial TEMP path; the reason is in the materialize result's
    # ``parallel_hash`` field and in one warning log line.
    "parallel_hash_unavailable": 0,
    # Issue 2941 fix 2: merged rows whose hash came from the pre-lock cache.
    "prelock_hash_reused": 0,
}


def _reset_materialize_instrumentation() -> None:
    _MATERIALIZE_INSTRUMENTATION.update(
        merged_rows=0,
        carried_rows=0,
        kinds=0,
        staged_rows=0,
        unlogged_stages=0,
        parallel_hash_stages=0,
        parallel_hash_unavailable=0,
        prelock_hash_reused=0,
    )


# --------------------------------------------------------------------------- #
# Issue 2941: parallel content hash in the effective-attribute-view stage.
#
# The merge statement computes okg.attribute_content_hash (recursive plpgsql
# canonical text + sha256) once per merged row. On a full materialize that is
# O(total) rows and was ~73% of the stage at 400k. Parallel workers cannot read
# a TEMP table (its pages live in the leader's local buffers), so a statement
# over the old temp staging table always ran on one core. Staging the raw rows
# in an UNLOGGED table instead puts them in shared buffers, where workers can
# scan them; both hash functions are IMMUTABLE PARALLEL SAFE already.
#
# Safety of the unlogged staging table:
#   * It is created and dropped inside the materialize transaction
#     (``with conn.transaction()`` in the caller, asserted below). It is never
#     committed, so no other session -- no other branch, publish, or deployment
#     -- can see it, and an error or a crash rolls its creation back.
#   * Its name carries the backend pid and a random token, so concurrent
#     publishes never contend on one catalog name.
#   * Unlogged means no WAL. The rows are scratch data rebuilt on every run,
#     so crash safety is unchanged.
#
# Small dirty-driven cycles keep the old TEMP path: they stage O(changed) rows,
# and a worker launch would cost more than it saves. A publishing role without
# CREATE on the okg schema (app_rw), or a server with parallel query turned
# off, also keeps the TEMP path, and the materialize result says so (see
# _parallel_hash_workers). A crash in the middle of the transaction can leave
# the uncommitted table's data file on disk (PostgreSQL removes orphaned TEMP
# files at startup but not orphaned regular or unlogged ones); its catalog row
# never committed, so nothing can read it.
_PARALLEL_HASH_MIN_ROWS = 20_000
_PARALLEL_HASH_MAX_WORKERS = 8
_PARALLEL_HASH_MIN_WORKERS = 2
_PARALLEL_HASH_SETTINGS = (
    "max_parallel_workers_per_gather",
    "parallel_setup_cost",
    "parallel_tuple_cost",
    "min_parallel_table_scan_size",
)


def _raw_stage_is_unlogged(*, dirty_driven: bool, merge_count: int) -> bool:
    """Stage in an unlogged table when the merge set may be large.

    A full scan stages every visible row (O(total)); a dirty-driven cycle
    stages only its merge set, whose size is known before staging.
    """
    return (not dirty_driven) or merge_count >= _PARALLEL_HASH_MIN_ROWS


def _parallel_hash_workers(conn: psycopg.Connection) -> tuple[int, str | None]:
    """How many workers one hash statement may ask for, or why it may not.

    Returns ``(workers, None)`` when the parallel path can run, else
    ``(0, reason)``. The path needs CREATE on the okg schema for the unlogged
    staging table: a restricted publisher role such as ``app_rw`` has it
    revoked (schema.sql), and such a deployment keeps the serial TEMP path.
    An operator who set ``max_parallel_workers_per_gather`` or
    ``max_parallel_workers`` to 0 has turned parallel query off, and that is
    honored. Otherwise the request is ``min(8, max(2, max_parallel_workers //
    2))``, so one publish never asks for more than half the server's parallel
    worker pool (readers keep the rest) beyond the floor of 2.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT has_schema_privilege(current_user, 'okg', 'CREATE'), "
            "current_setting('max_parallel_workers_per_gather')::int, "
            "current_setting('max_parallel_workers')::int"
        )
        can_create, per_gather, pool = cur.fetchone()
    if not can_create:
        return 0, "no CREATE on schema okg for the publishing role"
    if per_gather <= 0:
        return 0, "max_parallel_workers_per_gather is 0 (parallel query disabled)"
    if pool <= 0:
        return 0, "max_parallel_workers is 0 (parallel query disabled)"
    return (
        min(_PARALLEL_HASH_MAX_WORKERS, max(_PARALLEL_HASH_MIN_WORKERS, pool // 2)),
        None,
    )


def _unlogged_stage_table_name(conn: psycopg.Connection, target_kind: str) -> str:
    """A session-unique name in the okg schema for one unlogged staging table.

    Refuses outside an open transaction: the table is safe only because its
    creation is rolled back on every error path and it is dropped before commit.
    """
    status = conn.info.transaction_status
    if status != psycopg.pq.TransactionStatus.INTRANS:
        raise SubstrateError(
            "unlogged attribute staging requires an open transaction "
            f"(transaction status {status.name}); refusing to create a "
            "table that an error could leave behind"
        )
    return (
        f"okg._eff_stage_{target_kind}_{conn.info.backend_pid}_"
        f"{uuid.uuid4().hex[:12]}"
    )


def _staging_identifier(table: str) -> sql.Identifier:
    """Quote a possibly schema-qualified staging table name."""
    return sql.Identifier(*table.split("."))


def _execute_hash_stage(
    conn: psycopg.Connection,
    db: Any,
    stage_sql: str,
    params: dict[str, Any],
    *,
    rows: int,
    workers: int,
) -> str:
    """Run the merge-and-hash statement, on parallel workers when it is large.

    The planner does not know the plpgsql hash is expensive, so on its own it
    keeps a few-hundred-thousand-row scan serial. For this one statement the
    parallel setup and tuple costs are zeroed and the per-gather worker cap is
    set to ``workers`` (see _parallel_hash_workers). Each worker gets its own
    ``work_mem``. The settings are transaction-local (``set_config(...,
    true)``) and are put back right after the statement; if the statement
    fails, the enclosing transaction's rollback reverts them. The server still
    decides how many workers actually launch; with none free the statement
    runs serially and the result is the same. Returns the status reported in
    the materialize result.
    """
    if rows < _PARALLEL_HASH_MIN_ROWS:
        db.execute(stage_sql, params)
        return f"serial: {rows} staged rows, below {_PARALLEL_HASH_MIN_ROWS}"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            + ", ".join(f"current_setting('{name}')" for name in _PARALLEL_HASH_SETTINGS)
        )
        prior = cur.fetchone()
        cur.execute(
            "SELECT set_config('max_parallel_workers_per_gather', %s, true), "
            "set_config('parallel_setup_cost', '0', true), "
            "set_config('parallel_tuple_cost', '0', true), "
            "set_config('min_parallel_table_scan_size', '0', true)",
            (str(workers),),
        )
    db.execute(stage_sql, params)
    with conn.cursor() as cur:
        for name, value in zip(_PARALLEL_HASH_SETTINGS, prior):
            cur.execute("SELECT set_config(%s, %s, true)", (name, value))
    _MATERIALIZE_INSTRUMENTATION["parallel_hash_stages"] += 1
    return f"parallel: up to {workers} workers requested"


# --------------------------------------------------------------------------- #
# Issue 2941 fix 2: content hashes computed before the publish lock.
#
# A full materialize (first publish, the count-stamp cadence) hashes every row.
# The publisher can compute those hashes BEFORE it takes the publish lock, from
# the branch head as it stands then, into a session TEMP table. Under the lock
# the materialize still stages every row from the branch exactly as before and
# still merges every row with this generation's overlay; it reuses a cached
# hash only for a row whose hash inputs are provably the same as the ones the
# cache hashed, and hashes every other row as it always did.
#
# Why a reused hash equals the hash the old path computes:
#   * The cache holds, per target, SHA-256 of the raw ``attrs`` exact jsonb
#     text and H(attrs || '{}', NULLIF(attrs, attrs || '{}')), which is exactly
#     the merge-and-hash expression of _materialize_view_kind for a target with
#     no overlay row (COALESCE(NULL, '{}') is '{}').
#   * A row reuses it only when the target has no overlay row in this
#     generation AND the SHA-256 of its staged raw ``attrs`` text equals the
#     cached one. Equal text is the same jsonb value (jsonb prints a value one
#     way and reads that text back to the same value), so both sides evaluate
#     the same IMMUTABLE expression over the same input and get the same hash.
#     The only assumption is that SHA-256 has no collision between two
#     versions of one target's attrs; the version store already assumes that
#     of attrs_hash (it keys versions by it and ON CONFLICT DO NOTHING treats
#     an equal hash as an equal payload).
#   * The check is on exact text, stricter than jsonb equality: a rewrite to a
#     jsonb-equal value with another numeric scale misses and is re-hashed.
#   * The written ``attrs``/``raw_attrs`` always come from the rows staged
#     under the lock, never from the cache; only the hash value is reused.
#   * The two hash functions are fingerprinted when the cache is built and
#     again before it is used; a replaced function discards the cache.
#   * The cache is a TEMP table: only this session can read it. It lives in
#     this session between the pre-lock pass and the materialize, and the
#     publisher drops it when the cycle ends. A missing table (a pooled
#     connection reset, DISCARD TEMP) is detected and the materialize hashes
#     every row, and says so.
# A row the branch changed, added or deleted after the pre-lock pass, and a
# row with an overlay, simply misses the cache. The cache can only save work;
# it never decides what is written.
#
# Fix 5: an incremental cycle (and a full cycle over a full previous snapshot)
# hashes only the rows changed since the last checkpoint, which can still be
# most of the graph when a new source lands. For those cycles the pass hashes
# only the rows the branch wrote since its last checkpoint (scope "changed",
# ``writer_segment_ids``), and the dirty-driven merge joins the cache exactly
# as the full scan does. A changed row the pass did not see is a cache miss and
# is hashed under the lock; the reuse check above is unchanged.
_PRELOCK_HASH_TABLE_PREFIX = "_eff_prelock_hash_"


@dataclass
class PrelockHashCache:
    """Hashes computed before the publish lock, one TEMP table per kind."""

    tables: dict[str, str]
    function_fingerprint: str
    report: dict[str, Any]


def _hash_function_fingerprint(conn: psycopg.Connection) -> str | None:
    """Identity of the two hash functions as installed right now.

    None when either is missing, so a cache can never be built or used then.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   md5(string_agg(p.oid::text || ':'
                                  || md5(pg_get_functiondef(p.oid)),
                                  ',' ORDER BY p.proname))
              FROM pg_proc p
              JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'okg'
               AND p.proname IN ('attribute_content_hash', 'jsonb_canonical_text')
            """
        )
        count, digest = cur.fetchone()
    if int(count) != 2:
        return None
    return str(digest)


def _attrs_digest_expression(alias: str) -> str:
    """SHA-256 of the raw attrs' exact jsonb text: the cache's identity check."""
    return f"sha256(convert_to({alias}.attrs::text, 'UTF8'))"


def _prelock_hash_expression(alias: str) -> str:
    """The no-overlay merge-and-hash expression of _materialize_view_kind."""
    merged = f"{alias}.attrs || '{{}}'::jsonb"
    return (
        f"{ATTRIBUTE_CONTENT_HASH_FN}({merged}, "
        f"NULLIF({alias}.attrs, {merged}))"
    )


def prepare_prelock_attribute_hash_cache(
    conn: psycopg.Connection,
    *,
    branch: str = "default",
    graph_store: Any | None = None,
    statement_timeout_ms: int | None = None,
    writer_segment_ids: Iterable[int] | None = None,
) -> PrelockHashCache:
    """Hash the visible rows of ``branch`` into this session's TEMP tables.

    With ``writer_segment_ids`` None it hashes every visible row (scope
    "full"). With a list of Chronos writer segments (the segments since the
    branch's last checkpoint) it hashes only the visible rows written in them
    (scope "changed", issue 2941 fix 5), read from the interval physical table
    by the same visibility test as the dirty-driven staging.

    Runs before the publisher takes its lock. It reads the branch and writes
    only TEMP tables (plus, for the parallel hash, an unlogged staging table
    that is created and dropped inside one transaction, as in fix 1), so it
    changes nothing any other session can see. Raises on any failure; the
    publisher reports the reason and publishes without the cache.
    """
    if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
        raise SubstrateError(
            "the pre-lock attribute hash needs an idle connection",
            data={"transaction_status": conn.info.transaction_status.name},
        )
    fingerprint = _hash_function_fingerprint(conn)
    if fingerprint is None:
        raise SubstrateError("the attribute content hash functions are not installed")
    from okg.substrate.graph_store import ChronosGraphStore

    store = graph_store or ChronosGraphStore.from_connection(
        conn, ensure_schema=False, autocommit=True
    )
    session = store.checkout(branch)
    db = store.context.db
    workers, unavailable = _parallel_hash_workers(conn)
    segments = (
        None if writer_segment_ids is None
        else sorted({int(segment) for segment in writer_segment_ids})
    )
    if segments is not None and not segments:
        raise SubstrateError(
            "a changed-rows pre-lock hash needs at least one writer segment"
        )
    report: dict[str, Any] = {
        "parallel": (
            f"up to {workers} workers" if unavailable is None
            else f"unavailable: {unavailable}"
        ),
        "scope": "full" if segments is None else "changed",
    }
    tables: dict[str, str] = {}
    for target_kind in ("node", "edge"):
        is_node = target_kind == "node"
        graph_table = "okg.graph_nodes" if is_node else "okg.graph_edges"
        visible = _chronos_visible_read_sql(session, graph_table)
        if visible is None:
            raise SubstrateError(
                "the branch does not expose its prepared visible-row read",
                data={"table": graph_table},
            )
        visible_sql, branch_point = visible
        id_col = "node_id" if is_node else "edge_id"
        params: dict[str, Any] = {"_chronos_branch_point": branch_point}
        raw_select = (
            f"SELECT raw.{id_col}::text AS target_id, raw.attrs "
            f"FROM ({visible_sql}) raw WHERE raw.attrs IS NOT NULL"
        )
        if segments is not None:
            physical = _chronos_interval_physical_table(
                conn, table=graph_table, required_columns=(id_col, "attrs")
            )
            if physical is None:
                raise SubstrateError(
                    "the branch has no interval physical table to read its "
                    "changed rows from",
                    data={"table": graph_table},
                )
            params["_prelock_segments"] = segments
            physical_sql = sql.Identifier(physical).as_string(conn)
            # The visibility test of _stage_dirty_raw_via_interval_keys, limited
            # to the rows written since the last checkpoint.
            raw_select = (
                f"SELECT DISTINCT ON (p.{id_col}) p.{id_col}::text AS target_id, "
                f"p.attrs FROM {physical_sql} p "
                "WHERE p.writer_segment_id = ANY(:_prelock_segments) "
                "AND p.live_lo <= :_chronos_branch_point "
                "AND :_chronos_branch_point < p.live_hi "
                "AND COALESCE(p.deleted, FALSE) = FALSE "
                f"AND p.attrs IS NOT NULL ORDER BY p.{id_col}, p.live_lo DESC"
            )
        table = f"{_PRELOCK_HASH_TABLE_PREFIX}{target_kind}"
        qualified = f"pg_temp.{table}"
        with conn.transaction():
            if statement_timeout_ms:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT set_config('statement_timeout', %s, true)",
                        (str(int(statement_timeout_ms)),),
                    )
            db.execute(f"DROP TABLE IF EXISTS {qualified}", {})
            if unavailable is None:
                stage = _unlogged_stage_table_name(conn, f"prelock_{target_kind}")
                stage_name = stage.split(".", 1)[1]
                db.execute(
                    f"CREATE UNLOGGED TABLE okg.{stage_name} "
                    "(target_id TEXT, attrs JSONB)",
                    {},
                )
                scur = db.execute(f"INSERT INTO {stage} {raw_select}", params)
                rows = int(getattr(scur, "rowcount", 0) or 0)
                _execute_hash_stage(
                    conn,
                    db,
                    f"CREATE TEMP TABLE {table} AS "
                    f"SELECT s.target_id, {_attrs_digest_expression('s')} "
                    f"AS attrs_digest, {_prelock_hash_expression('s')} "
                    f"AS attrs_hash FROM {stage} s",
                    {},
                    rows=rows,
                    workers=workers,
                )
                db.execute(f"DROP TABLE IF EXISTS {stage}", {})
            else:
                db.execute(
                    f"CREATE TEMP TABLE {table} AS "
                    f"SELECT r.target_id, {_attrs_digest_expression('r')} "
                    f"AS attrs_digest, {_prelock_hash_expression('r')} "
                    f"AS attrs_hash FROM ({raw_select}) r",
                    params,
                )
            # One row per target, or the cache is refused: the lookup under the
            # lock must never multiply a staged row.
            db.execute(f"ALTER TABLE {qualified} ADD PRIMARY KEY (target_id)", {})
            db.execute(f"ANALYZE {qualified}", {})
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {qualified}")
                report[target_kind] = int(cur.fetchone()[0])
        tables[target_kind] = qualified
    return PrelockHashCache(
        tables=tables, function_fingerprint=fingerprint, report=report
    )


def drop_prelock_attribute_hash_cache(
    conn: psycopg.Connection, cache: PrelockHashCache | None
) -> None:
    """Drop the cache's TEMP tables when the connection can run a statement."""
    if cache is None or conn.closed:
        return
    status = conn.info.transaction_status
    if status == psycopg.pq.TransactionStatus.IDLE:
        with conn.transaction(), conn.cursor() as cur:
            for table in cache.tables.values():
                cur.execute(f"DROP TABLE IF EXISTS {table}")
    elif status == psycopg.pq.TransactionStatus.INTRANS:
        with conn.cursor() as cur:
            for table in cache.tables.values():
                cur.execute(f"DROP TABLE IF EXISTS {table}")
    else:
        # A failed transaction cannot run the DROP. The tables are TEMP: they
        # end with the session, and the next pre-lock pass on this session
        # drops and rebuilds them under the same names.
        logger.warning(
            "pre-lock attribute hash cache left in place (transaction status %s); "
            "it is session-local and is replaced by the next pass",
            status.name,
        )


def _prelock_unused_report(
    cache: PrelockHashCache | None, reason: str
) -> dict[str, str] | None:
    """The ``prelock_hash`` result field when the materialize hashed nothing."""
    if cache is None:
        return None
    return {"node": f"unused: {reason}", "edge": f"unused: {reason}"}


def _prelock_cache_unusable_reason(
    conn: psycopg.Connection, cache: PrelockHashCache
) -> str | None:
    """Why the cache must not be used right now, or None when it may be."""
    with conn.cursor() as cur:
        for kind, table in cache.tables.items():
            cur.execute("SELECT to_regclass(%s)", (table,))
            if cur.fetchone()[0] is None:
                return f"{kind} cache table {table} no longer exists"
    if _hash_function_fingerprint(conn) != cache.function_fingerprint:
        return "the attribute content hash functions changed after the pre-lock pass"
    return None


def _materialize_view_kind(
    conn: psycopg.Connection,
    session: Any,
    db: Any,
    *,
    generation: str,
    namespace: str,
    reserved: list[str],
    target_kind: TargetKind,
    previous_generation: str | None = None,
    dirty_ids: frozenset[str] | None = None,
    interval_view: bool = False,
    hash_report: dict[str, str] | None = None,
    prelock_cache_table: str | None = None,
    prelock_report: dict[str, Any] | None = None,
) -> int:
    """Merge one kind (node|edge) into the effective-attrs view via set-based
    INSERT...SELECT. Prefers the embedded chronos visibility sub-select run
    through the chronos DB adapter (``db``), which translates the
    ``:_chronos_branch_point`` bind; falls back to session staging on ``conn`` if
    the prepared rewrite isn't exposed.

    INCREMENTAL CARRY-FORWARD (the chosen design to avoid the publish-latency
    regression): when ``previous_generation`` has a snapshot, this carries forward
    the prior generation's snapshot rows VERBATIM (one INSERT...SELECT, no
    ``raw || overlay`` recompute, no GIN-heavy re-merge) for every target whose raw
    is UNCHANGED (``raw_attrs`` JSONB-equal to the prior snapshot) AND whose overlay
    is UNCHANGED, and re-merges (the expensive path) ONLY the changed + new targets.
    Steady state (no attr changes) is therefore O(changed) merge work,
    byte-identical to a full re-materialize because ``attrs = raw_attrs || overlay``
    is a pure function of (raw_attrs, overlay) and we carry forward only when BOTH
    are identical.

    All binds are chronos ``:name`` style so the whole statement translates
    uniformly through the adapter.
    """
    is_node = target_kind == "node"
    graph_table = "okg.graph_nodes" if is_node else "okg.graph_edges"
    visible = _chronos_visible_read_sql(session, graph_table)

    overlay_cte = f"""
      overlay AS (
        SELECT target_id,
               jsonb_object_agg(attr_key, value) AS overlay_attrs,
               count(*) AS n
          FROM {ATTRIBUTE_EFFECTIVE_TABLE}
         WHERE target_kind = :tk
           AND namespace = :ns
           AND generation_id = :gen
           AND attr_key <> ALL(:reserved)
         GROUP BY target_id
      )
    """

    # ``raw_text_hash`` is the USER-FACING raw text hash surfaced as the node's
    # ``text_hash`` field on the read fast path -- it carries chronos's
    # ``text_hash`` verbatim (nodes), NULL for edges, EXACTLY as the slow path's
    # projection returns. It is NOT the carry-forward key (chronos text_hash is
    # optional / often NULL). The carry-forward change-detection keys on
    # ``raw_attrs`` jsonb EQUALITY (the true content key: ``attrs`` is a pure
    # function of (raw_attrs, overlay), so raw_attrs-equal + overlay-equal ⇒
    # byte-identical attrs).
    if is_node:
        insert_cols = (
            "target_kind, target_id, generation_id, namespace, subtype, "
            "overlay_count, raw_text_hash, attrs_hash"
        )
        # presence-means-overlay: store raw_attrs ONLY when it byte-differs from
        # the merged attrs (an overlay actually changed something); else NULL and
        # readers fall back to attrs. NULLIF is a pure function of the two blobs,
        # so the version-store content hash and the parity fingerprint stay sound
        # (raw_attrs's NULL co-varies with attrs).
        merged_select = (
            "SELECT r.node_id AS target_id, r.subtype, "
            "NULLIF(r.attrs, r.attrs || COALESCE(o.overlay_attrs, '{}'::jsonb)) "
            "AS raw_attrs, "
            "r.attrs || COALESCE(o.overlay_attrs, '{}'::jsonb) AS attrs, "
            "COALESCE(o.n, 0) AS overlay_count, r.text_hash AS raw_text_hash "
        )
        snapshot_select = (
            "SELECT :tk, m.target_id, :gen, :ns, m.subtype, "
            "m.overlay_count, m.raw_text_hash, m.attrs_hash"
        )
        raw_cols = "node_id, subtype, attrs, text_hash"
        join_key = "r.node_id"
    else:
        insert_cols = (
            "target_kind, target_id, generation_id, namespace, src, dst, "
            "edge_type, overlay_count, raw_text_hash, attrs_hash"
        )
        # presence-means-overlay (see the node branch above): raw_attrs = NULL
        # unless the merged attrs byte-differs from the raw base.
        merged_select = (
            "SELECT r.edge_id AS target_id, r.src, r.dst, r.edge_type, "
            "NULLIF(r.attrs, r.attrs || COALESCE(o.overlay_attrs, '{}'::jsonb)) "
            "AS raw_attrs, "
            "r.attrs || COALESCE(o.overlay_attrs, '{}'::jsonb) AS attrs, "
            "COALESCE(o.n, 0) AS overlay_count, NULL::text AS raw_text_hash "
        )
        snapshot_select = (
            "SELECT :tk, m.target_id, :gen, :ns, m.src, m.dst, m.edge_type, "
            "m.overlay_count, m.raw_text_hash, m.attrs_hash"
        )
        raw_cols = "edge_id::text AS edge_id, src, dst, edge_type, attrs, NULL AS text_hash"
        join_key = "r.edge_id"

    params: dict[str, Any] = {
        "tk": target_kind,
        "ns": namespace,
        "gen": generation,
        "reserved": reserved,
    }

    vis_id = "node_id" if is_node else "edge_id"
    prior_exists = previous_generation is not None and _prior_snapshot_exists(
        conn, previous_generation, namespace, target_kind
    )
    # ivm-effective-attribute-view / task.plumb-dirty-scope: when the publisher
    # hands us this cycle's RAW changeset (dirty_ids) AND a prior snapshot exists,
    # drive carry-forward from the KNOWN delta instead of staging + raw_attrs-JOIN
    # over the full chronos-visible set. The merge set = raw-dirty UNION the
    # per-generation overlay gen-diff (overlay-only changes are invisible to the
    # chronos dirty scope; unioning them is mandatory or an enricher-changed target
    # would be silently carried forward stale -- a P0). carry = prior snapshot rows
    # NOT IN that merge set, copied verbatim (disjoint from the merge set, so no
    # raw/overlay re-check is needed). Falls back to the full-scan path when the
    # delta is unavailable (dirty_ids is None: backfill / migrate / idempotent) or
    # when the prepared chronos read isn't exposed (session-staging fallback).
    # In interval-view mode the prior per-generation snapshot is no longer the
    # carry-forward authority; the interval table is. A prior changed-only
    # generation may have zero rows for this kind, and that must not force the
    # next publish back to a full snapshot.
    prior_snapshot_complete = True
    if previous_generation is not None and not interval_view:
        prior_snapshot_complete = _prior_snapshot_covers_checkpoint(
            conn,
            previous_generation=previous_generation,
            namespace=namespace,
            target_kind=target_kind,
        )
    dirty_driven = (
        dirty_ids is not None
        and visible is not None
        and (
            (prior_exists and prior_snapshot_complete)
            or (interval_view and previous_generation is not None)
        )
    )
    merge_ids: set[str] | None = None
    if dirty_driven:
        merge_ids = set(dirty_ids) | _overlay_changed_ids(
            conn,
            generation=generation,
            previous_generation=previous_generation,
            namespace=namespace,
            target_kind=target_kind,
            reserved=reserved,
        )

    # Stage the raw rows to merge into a temp table. Full-scan: every chronos-visible
    # row of this generation (O(total)). Dirty-driven: only the merge set
    # (O(changed)), and ZERO rows for a no-op cycle.
    temp = f"_eff_raw_{target_kind}s_{abs(hash(generation)) % 100000}"
    unlogged_stage = False
    parallel_workers = 0
    hash_status = "serial: session staging (no prepared chronos read)"
    staged = 0
    use_prelock = False
    if prelock_cache_table is not None and prelock_report is not None:
        prelock_report[target_kind] = "unused: session staging (no prepared chronos read)"
    if visible is not None:
        sql, branch_point = visible
        params["_chronos_branch_point"] = branch_point
        if is_node:
            stage_cols = "node_id TEXT, subtype TEXT, attrs JSONB, text_hash TEXT"
        else:
            stage_cols = (
                "edge_id TEXT, src TEXT, dst TEXT, edge_type TEXT, "
                "attrs JSONB, text_hash TEXT"
            )
        stage_filter = ""
        if dirty_driven:
            params["_merge_ids"] = list(merge_ids)
            stage_filter = f" WHERE raw.{vis_id}::text = ANY(:_merge_ids)"
        stage_sel = f"SELECT {raw_cols} FROM ({sql}) raw{stage_filter}"
        # Issue 2941 fix 2: each staged row carries the hash and text digest
        # the pre-lock pass recorded for the same target; the merge statement
        # keeps the hash only when the digest still matches (see
        # PrelockHashCache). The primary key on the cache keeps this LEFT JOIN
        # at one row per row, and the join reads only narrow columns. Fix 5:
        # a dirty-driven cycle stages its merge set the same way, so a cache
        # of the rows changed since the last checkpoint serves it too.
        use_prelock = prelock_cache_table is not None
        if prelock_cache_table is not None and prelock_report is not None:
            prelock_report[target_kind] = "used"
        if use_prelock:
            stage_cols += ", cached_hash BYTEA, cached_digest BYTEA"
            qualified_raw = (
                "raw.node_id, raw.subtype, raw.attrs, raw.text_hash"
                if is_node
                else "raw.edge_id::text AS edge_id, raw.src, raw.dst, "
                "raw.edge_type, raw.attrs, NULL AS text_hash"
            )
            stage_sel = (
                f"SELECT {qualified_raw}, pc.attrs_hash AS cached_hash, "
                "pc.attrs_digest AS cached_digest "
                f"FROM ({sql}) raw "
                f"LEFT JOIN {prelock_cache_table} pc "
                f"ON pc.target_id = raw.{vis_id}::text{stage_filter}"
            )
        unlogged_stage = _raw_stage_is_unlogged(
            dirty_driven=dirty_driven,
            merge_count=len(merge_ids) if merge_ids is not None else 0,
        )
        hash_status = f"serial: small merge set (below {_PARALLEL_HASH_MIN_ROWS})"
        if unlogged_stage:
            parallel_workers, unavailable = _parallel_hash_workers(conn)
            if unavailable is not None:
                # Announced, not silent: the result carries the reason and
                # the operator sees one warning per kind.
                unlogged_stage = False
                hash_status = f"unavailable: {unavailable}"
                _MATERIALIZE_INSTRUMENTATION["parallel_hash_unavailable"] += 1
                logger.warning(
                    "effective attribute view: parallel content hash unavailable "
                    "for %s (%s); hashing serially",
                    target_kind,
                    unavailable,
                )
        if unlogged_stage:
            # Issue 2941: shared-buffer staging so the hash can run on parallel
            # workers. Uncommitted and dropped below; see _PARALLEL_HASH_MIN_ROWS.
            temp = _unlogged_stage_table_name(conn, target_kind)
            db.execute(f"CREATE UNLOGGED TABLE {temp} ({stage_cols})", {})
            _MATERIALIZE_INSTRUMENTATION["unlogged_stages"] += 1
        else:
            db.execute(
                f"CREATE TEMP TABLE {temp} ({stage_cols}) ON COMMIT DROP", {}
            )
        if dirty_driven:
            keyed_staged = _stage_dirty_raw_via_interval_keys(
                conn,
                graph_table=graph_table,
                target_kind=target_kind,
                temp_table=temp,
                merge_ids=merge_ids,
                branch_point=branch_point,
                prelock_cache_table=prelock_cache_table,
            )
        else:
            keyed_staged = None
        if keyed_staged is None:
            scur = db.execute(f"INSERT INTO {temp} {stage_sel}", params)
            staged = int(getattr(scur, "rowcount", 0) or 0)
        else:
            staged = keyed_staged
    else:
        if is_node:
            _stage_raw_nodes_via_session(conn, session, temp_table=temp)
        else:
            _stage_raw_edges_via_session(conn, session, temp_table=temp)
            # session staging gives (edge_id, src, dst, edge_type, attrs); add the
            # NULL text_hash column the edge merge SELECT references (edges carry
            # NULL raw_text_hash; the carry-forward keys on raw_attrs, not hash).
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {temp} ADD COLUMN text_hash TEXT")
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {temp}")
            staged = int(cur.fetchone()[0])
    _MATERIALIZE_INSTRUMENTATION["staged_rows"] += staged

    # CARRY-FORWARD: copy unchanged prior-snapshot rows verbatim.
    not_carried = ""
    carried = 0
    if dirty_driven and interval_view:
        # ivm-effective-attribute-view interval cutover: the unchanged majority
        # lives in the interval table (sync delta mode carries it across
        # generations), so DON'T carry it forward into the snapshot. The snapshot
        # becomes CHANGED-ONLY and the O(total) carry-forward INSERT that dominated
        # publish latency (~410ms on the bench) is eliminated. Flag-on operator
        # reads use the interval table, and cardinality comes from the Chronos count
        # -- neither needs the full per-generation snapshot. sync MUST run in delta
        # mode (dirty_targets) for this generation so absence is not read as retract.
        # interval_view is False for a STRUCTURAL FORK (no recorded ordinal): the
        # fork keeps the full snapshot so its snapshot-fallback reads stay correct.
        carried = 0
    elif dirty_driven:
        carried = _carry_forward_by_exclusion(
            conn,
            generation=generation,
            previous_generation=previous_generation,
            namespace=namespace,
            target_kind=target_kind,
            exclude_ids=merge_ids,
        )
        _MATERIALIZE_INSTRUMENTATION["carried_rows"] += carried
    elif prior_exists:
        # Full-scan path: detect unchanged targets by raw_attrs-equality JOIN to the
        # staged set AND overlay fingerprint; merge only the complement (not_carried).
        carried = _carry_forward_unchanged_rows(
            conn,
            generation=generation,
            previous_generation=previous_generation,
            namespace=namespace,
            target_kind=target_kind,
            reserved=reserved,
            new_raw_temp=temp,
            id_col=("node_id" if is_node else "edge_id"),
        )
        _MATERIALIZE_INSTRUMENTATION["carried_rows"] += carried
        not_carried = (
            f" AND NOT EXISTS (SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} cf "
            "WHERE cf.target_kind = :tk AND cf.namespace = :ns "
            f"AND cf.generation_id = :gen AND cf.target_id = {join_key})"
        )
    _MATERIALIZE_INSTRUMENTATION["kinds"] += 1

    if is_node:
        merge_raw_cols = "node_id, subtype, attrs, text_hash"
    else:
        merge_raw_cols = "edge_id, src, dst, edge_type, attrs, text_hash"
    # schema-interval-narrow-split: stage the merged payloads ONCE (with the
    # content hash computed in-database by okg.attribute_content_hash -- the
    # single hash site, req.sigma-pinned), write the payload into the
    # insert-once version store, then insert the NARROW snapshot rows carrying
    # only the hash. All three statements run inside the caller's transaction,
    # so the composite FK (snapshot -> versions) is never observed unsatisfied.
    merged_temp = f"_eff_merged_{target_kind}s_{abs(hash(generation)) % 100000}"
    hash_expr = f"{ATTRIBUTE_CONTENT_HASH_FN}(m.attrs, m.raw_attrs)"
    if use_prelock:
        # Reuse the pre-lock hash only for a target with no overlay row whose
        # raw attrs are byte for byte the text the cache hashed: its merged
        # attrs and raw_attrs are then exactly the cached expression's inputs.
        # The digest runs here, on the parallel workers. Every other row is
        # hashed as before.
        merge_raw_cols += ", cached_hash, cached_digest"
        merged_select += (
            ", r.cached_hash AS prelock_hash, "
            "(r.cached_digest IS NOT NULL AND o.target_id IS NULL "
            f"AND {_attrs_digest_expression('r')} = r.cached_digest) "
            "AS prelock_match "
        )
        hash_expr = (
            "CASE WHEN m.prelock_match "
            f"THEN m.prelock_hash ELSE {hash_expr} END"
        )
    stage_sql = (
        f"CREATE TEMP TABLE {merged_temp} ON COMMIT DROP AS "
        f"WITH r0 AS (SELECT {merge_raw_cols} FROM {temp}), {overlay_cte} "
        f"SELECT m.*, {hash_expr} "
        "AS attrs_hash FROM ("
        f"{merged_select}"
        f"FROM r0 r "
        f"LEFT JOIN overlay o ON o.target_id = {join_key} "
        f"WHERE TRUE{not_carried}"
        ") m"
    )
    if unlogged_stage:
        hash_status = _execute_hash_stage(
            conn, db, stage_sql, params, rows=staged, workers=parallel_workers
        )
    else:
        db.execute(stage_sql, params)
    if hash_report is not None:
        hash_report[target_kind] = hash_status
    if use_prelock:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), count(*) FILTER "
                "(WHERE prelock_match) "
                f"FROM {merged_temp}"
            )
            merged_count, reused = cur.fetchone()
        _MATERIALIZE_INSTRUMENTATION["prelock_hash_reused"] += int(reused)
        if prelock_report is not None:
            prelock_report[target_kind] = (
                f"used: {int(reused)} of {int(merged_count)} merged rows "
                "reused a pre-lock hash"
            )
    db.execute(
        f"INSERT INTO {ATTRIBUTE_VERSIONS_TABLE} "
        "(target_kind, target_id, attrs_hash, attrs, raw_attrs) "
        f"SELECT :tk, m.target_id, m.attrs_hash, m.attrs, m.raw_attrs "
        f"FROM {merged_temp} m "
        "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
        params,
    )
    cur = db.execute(
        f"INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} ({insert_cols}) "
        f"{snapshot_select} "
        f"FROM {merged_temp} m "
        "ON CONFLICT (target_kind, target_id, generation_id, namespace) "
        "DO NOTHING",
        params,
    )
    merged = cur.rowcount
    _MATERIALIZE_INSTRUMENTATION["merged_rows"] += merged
    # Drop the staged temps eagerly (ON COMMIT DROP also covers the txn boundary).
    # The unlogged staging table has no ON COMMIT DROP: this DROP, inside the
    # same transaction, is what keeps it from ever being committed.
    db.execute(f"DROP TABLE IF EXISTS {merged_temp}", {})
    db.execute(f"DROP TABLE IF EXISTS {temp}", {})
    # Return the TOTAL rows materialized for this kind/generation = carried-forward
    # (verbatim copies of unchanged targets) + merged (freshly re-merged changed /
    # new targets). This equals count(*) of the generation's snapshot for this
    # kind — i.e. the true published node/edge cardinality, which the publisher
    # stamps into the checkpoint (no separate count(*) scan). The merged/carried
    # split stays observable via _MATERIALIZE_INSTRUMENTATION for the
    # carry-forward tests.
    return merged + carried


def _prior_snapshot_exists(
    conn: psycopg.Connection,
    previous_generation: str,
    namespace: str,
    target_kind: TargetKind,
) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND namespace = %s "
            "AND target_kind = %s LIMIT 1",
            (previous_generation, namespace, target_kind),
        )
        return cur.fetchone() is not None


def _prior_snapshot_covers_checkpoint(
    conn: psycopg.Connection,
    *,
    previous_generation: str,
    namespace: str,
    target_kind: TargetKind,
) -> bool:
    """Return whether a prior snapshot is full enough for snapshot carry-forward.

    Interval-view publishes deliberately write changed-only snapshots. If a later
    process runs with interval mode off, carrying forward from that changed-only
    snapshot would keep producing incomplete snapshot-fallback reads. Metadata may
    be absent on older checkpoints; in that case keep the historical path rather
    than forcing a full rebuild for every legacy deployment.
    """

    expected = _checkpoint_expected_kind_count(
        conn,
        previous_generation=previous_generation,
        target_kind=target_kind,
    )
    if expected is None:
        return True
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND namespace = %s AND target_kind = %s",
            (previous_generation, namespace, target_kind),
        )
        observed = int(cur.fetchone()[0])
    return observed >= expected


def _checkpoint_expected_kind_count(
    conn: psycopg.Connection,
    *,
    previous_generation: str,
    target_kind: TargetKind,
) -> int | None:
    column = "node_count" if target_kind == "node" else "edge_count"
    if not _table_exists(conn, "_chronos_branch_interval_checkpoints"):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT metadata
              FROM _chronos_branch_interval_checkpoints
             WHERE checkpoint_id = %s
             LIMIT 1
            """,
            (previous_generation,),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    try:
        metadata = json.loads(str(row[0]))
    except (TypeError, ValueError):
        return None
    value = metadata.get(column)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_SNAPSHOT_KIND_COUNT_SQL = {
    # One statement per kind, with the kind as a literal, so the planner can
    # use that kind's partial index (target_kind = 'node' / 'edge'), whose
    # leading column is generation_id.
    kind: (
        f"SELECT count(*) FROM (SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
        f"WHERE target_kind = '{kind}' AND generation_id = %s "
        "AND namespace = %s LIMIT %s) s"
    )
    for kind in ("node", "edge")
}


def prior_snapshot_coverage(
    conn: psycopg.Connection,
    previous_generation: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, tuple[int, int | None]]:
    """Per kind, how many snapshot rows ``previous_generation`` has.

    Returns ``{kind: (rows, expected)}``; ``expected`` is the checkpoint's
    count for that kind, or None when the checkpoint does not record it (then
    ``rows`` is 0 and nothing is counted). Each count stops at ``expected``:
    the pre-lock planner only asks whether the snapshot is full.
    """
    coverage: dict[str, tuple[int, int | None]] = {}
    present = _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE)
    for kind in ("node", "edge"):
        expected = _checkpoint_expected_kind_count(
            conn, previous_generation=previous_generation, target_kind=kind
        )
        if expected is None or not present:
            coverage[kind] = (0, expected)
            continue
        with conn.cursor() as cur:
            cur.execute(
                _SNAPSHOT_KIND_COUNT_SQL[kind],
                (previous_generation, namespace, int(expected)),
            )
            coverage[kind] = (int(cur.fetchone()[0]), int(expected))
    return coverage


def _carry_forward_unchanged_rows(
    conn: psycopg.Connection,
    *,
    generation: str,
    previous_generation: str,
    namespace: str,
    target_kind: TargetKind,
    reserved: list[str],
    new_raw_temp: str,
    id_col: str,
) -> int:
    """Copy prior-snapshot rows VERBATIM into ``generation`` for every target
    whose raw is UNCHANGED (the NEW raw's ``attrs`` for the target equals the prior
    snapshot's ``raw_attrs``) AND whose overlay is UNCHANGED (same fingerprint of
    (attr_key, value_hash) over the current vs prior overlay).

    No chronos merge, no ``raw || overlay`` recompute -- the carried
    ``attrs``/``raw_attrs`` are byte-identical to what a full re-materialize would
    produce, because ``attrs`` is a pure function of (raw_attrs, overlay) and we
    copy ONLY when BOTH are identical. The raw_attrs JSONB-equality JOIN against the
    staged new raw (``new_raw_temp``) is what makes a target whose raw CHANGED fall
    out of the carry set (it is then merged fresh). Keying on raw_attrs (not the
    chronos text_hash, which is optional / often NULL) keeps carry-forward correct
    for every node/edge type. Returns rows carried forward.

    The overlay check excludes the ids ``_overlay_changed_ids`` returns (one
    set-based fingerprint diff) instead of joining per-target fingerprints into
    this statement; see the comment below for the measured plan it replaces.
    """
    # Lane SG stall fix: the overlay comparison is computed ONCE, set-based, as
    # the ids whose fingerprint differs between the two generations (the same
    # FULL OUTER JOIN the dirty-driven path uses), then excluded with a hashed
    # ``<> ALL`` filter. The previous form LEFT JOINed two GROUP BY fingerprint
    # subqueries to the prior snapshot inside this INSERT. The planner sees a
    # just-published generation as about one row (it is absent from the
    # column statistics until the next ANALYZE), so it chose nested loops that
    # re-ran each whole fingerprint aggregate once per prior-snapshot row: on
    # the Krama copy, 35,291 prior rows x a sort of about 59,000 overlay rows,
    # which exceeded 3,600 s. Excluding "fingerprint differs" is exactly the old
    # "fingerprints NOT DISTINCT" predicate: a target with no overlay in either
    # generation (NULL = NULL) is carried, one that gained or lost an overlay is
    # not.
    overlay_changed = _overlay_changed_ids(
        conn,
        generation=generation,
        previous_generation=previous_generation,
        namespace=namespace,
        target_kind=target_kind,
        reserved=reserved,
    )
    # The version-store check is a scalar subquery (one primary-key lookup per
    # candidate row) wrapped in IS TRUE, not a join. The staged raw table was
    # created inside this transaction and has no statistics: the planner
    # guesses 5.4 million narrow rows for 445,769 wide ones on the Krama copy.
    # As a join, that guess alone made it hash the stage against every version
    # row of the kind, slower than before on a full carry with no overlays
    # (nodes 50-55 s against 36-42 s); a bare "=" let it hash the JSONB values
    # (44-46 s). IS TRUE keeps the comparison out of the hash keys, so the
    # only join is the id equality and the lookup runs once per joined row
    # (37-39 s). ANALYZE of the stage was rejected: it cut the parallel merge
    # that follows from 8 workers to 1 (13 s to 115 s). The FK from the
    # snapshot to the version store guarantees the version row; a missing one
    # yields NULL, IS TRUE is false, and the row is not carried, as the join
    # did.
    sql = f"""
      INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
        (target_kind, target_id, generation_id, namespace, subtype, src, dst,
         edge_type, overlay_count, raw_text_hash, attrs_hash)
      SELECT p.target_kind, p.target_id, %(gen)s, p.namespace, p.subtype,
             p.src, p.dst, p.edge_type, p.overlay_count,
             p.raw_text_hash, p.attrs_hash
        FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} p
        JOIN {new_raw_temp} nr
          ON nr.{id_col}::text = p.target_id
       WHERE p.generation_id = %(pgen)s
         AND p.namespace = %(ns)s
         AND p.target_kind = %(tk)s
         AND p.target_id <> ALL(%(overlay_changed)s::text[])
         -- schema-interval-narrow-split: the prior snapshot row's payload
         -- lives in the version store (composite FK). presence-means-overlay:
         -- its raw base is COALESCE(raw_attrs, attrs) (NULL raw_attrs means
         -- raw == attrs), so the unchanged-raw carry key compares the NEW full
         -- raw against that base. Without the COALESCE every no-overlay target
         -- would miss and be re-merged -- a carry-forward perf regression.
         AND (nr.attrs = (
           SELECT COALESCE(av.raw_attrs, av.attrs)
             FROM {ATTRIBUTE_VERSIONS_TABLE} av
            WHERE av.target_kind = p.target_kind
              AND av.target_id = p.target_id
              AND av.attrs_hash = p.attrs_hash
         )) IS TRUE
      ON CONFLICT (target_kind, target_id, generation_id, namespace) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "gen": generation,
                "pgen": previous_generation,
                "ns": namespace,
                "tk": target_kind,
                "overlay_changed": sorted(overlay_changed),
            },
        )
        return cur.rowcount


def _overlay_changed_ids(
    conn: psycopg.Connection,
    *,
    generation: str,
    previous_generation: str,
    namespace: str,
    target_kind: TargetKind,
    reserved: list[str],
) -> set[str]:
    """ivm-effective-attribute-view / task.plumb-dirty-scope: the target_ids whose
    OVERLAY fingerprint differs between ``generation`` and ``previous_generation``.

    Overlay observations (``record_attribute_observation``) are written out-of-band
    and never touch the raw graph, so an overlay-only change is INVISIBLE to the
    publisher's chronos dirty scope. The dirty-driven materialize MUST union this
    set into its merge set, or an enricher-changed target would be carried forward
    with stale ``attrs`` (a silent-correctness P0). The full-scan carry-forward
    (``_carry_forward_unchanged_rows``) excludes the same set; a FULL OUTER JOIN
    catches a target whose overlay APPEARED or DISAPPEARED (fp NULL on one side).
    Reads the generation-stamped ``ATTRIBUTE_EFFECTIVE_TABLE`` (indexed on
    (target_kind, target_id, generation_id, namespace)) -- O(overlay), not O(graph).
    """
    overlay_fp = (
        "SELECT target_id, "
        "md5(string_agg(attr_key || '=' || value_hash, ',' ORDER BY attr_key)) AS fp "
        f"FROM {ATTRIBUTE_EFFECTIVE_TABLE} "
        "WHERE target_kind = %(tk)s AND namespace = %(ns)s "
        "AND generation_id = %(gen)s AND attr_key <> ALL(%(reserved)s::text[]) "
        "GROUP BY target_id"
    )
    overlay_fp_prev = overlay_fp.replace("%(gen)s", "%(pgen)s")
    sql = f"""
      WITH cur_fp AS ({overlay_fp}), prev_fp AS ({overlay_fp_prev})
      SELECT COALESCE(c.target_id, p.target_id) AS tid
        FROM cur_fp c
        FULL OUTER JOIN prev_fp p ON c.target_id = p.target_id
       WHERE c.fp IS DISTINCT FROM p.fp
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "gen": generation,
                "pgen": previous_generation,
                "ns": namespace,
                "tk": target_kind,
                "reserved": reserved,
            },
        )
        return {str(r[0]) for r in cur.fetchall()}


def _carry_forward_by_exclusion(
    conn: psycopg.Connection,
    *,
    generation: str,
    previous_generation: str,
    namespace: str,
    target_kind: TargetKind,
    exclude_ids: set[str],
) -> int:
    """ivm-effective-attribute-view / task.plumb-dirty-scope: copy prior-snapshot
    rows VERBATIM into ``generation`` for every target NOT in ``exclude_ids`` (the
    merge set = raw-dirty UNION overlay-changed for this kind).

    This is the dirty-driven carry-forward: the merge set is the AUTHORITATIVE
    change signal, so carried targets need no raw_attrs / overlay re-check (they are
    disjoint from the merged set by construction). A retracted target is in the
    merge set (raw-dirty) but absent from the live graph, so it is neither staged
    nor merged nor carried -> correctly absent from the new snapshot. An empty
    ``exclude_ids`` (no-op cycle) carries the entire prior snapshot forward.
    Byte-identical to a full re-materialize when the merge set is complete (the
    oracle, task.oracle-harness, proves this). Returns rows carried.
    """
    sql = f"""
      INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
        (target_kind, target_id, generation_id, namespace, subtype, src, dst,
         edge_type, overlay_count, raw_text_hash, attrs_hash)
      SELECT p.target_kind, p.target_id, %(gen)s, p.namespace, p.subtype,
             p.src, p.dst, p.edge_type, p.overlay_count,
             p.raw_text_hash, p.attrs_hash
        FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} p
       WHERE p.generation_id = %(pgen)s
         AND p.namespace = %(ns)s
         AND p.target_kind = %(tk)s
         AND p.target_id <> ALL(%(exclude)s::text[])
      ON CONFLICT (target_kind, target_id, generation_id, namespace) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            {
                "gen": generation,
                "pgen": previous_generation,
                "ns": namespace,
                "tk": target_kind,
                "exclude": list(exclude_ids),
            },
        )
        return cur.rowcount


def refresh_effective_attribute_view_overlay_for_generation(
    conn: psycopg.Connection,
    *,
    generation_id: int | str,
    namespace: str = DEFAULT_NAMESPACE,
) -> int:
    """Re-apply the overlay onto the frozen raw base in the effective-attrs view
    for one generation, so a POST-publish overlay write (recorded against an
    already-published generation and re-materialized into
    ``attribute_effective_attrs``) propagates to the indexed snapshot.

    Raw attrs are immutable per generation, so the view's raw base is the
    authoritative merge input; only the overlay can change post-publish. This
    recomputes ``attrs = raw_base || overlay`` (reserved-overlay excluded)
    entirely from the view's own raw base + the live overlay table -- no chronos
    checkout needed. This is what keeps the SQL fast path in PARITY with the
    Python read path (which reads the overlay live). No-op when the view table is
    absent or has no rows for the generation (e.g. inside the publish txn before
    the view materialize runs). Returns rows updated.

    presence-means-overlay: raw_attrs is NULL when it equals attrs (no overlay),
    so the raw base is ``COALESCE(raw_attrs, attrs)`` -- and this write PERSISTS
    the raw base into raw_attrs the moment an overlay diverges attrs from it
    (``raw_attrs = NULLIF(base, merged)``). That keeps the base recoverable for a
    LATER refresh (a second overlay must merge onto raw, not onto the already-
    overlaid attrs) while collapsing back to NULL when the overlay is removed.
    """
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE):
        return 0
    generation = str(generation_id)
    reserved = sorted(RESERVED_EFFECTIVE_ATTR_KEYS)
    _assert_no_overlay_conflict(
        conn, generation_id=generation, namespace=namespace
    )
    # schema-interval-narrow-split: the payload lives in the version store, so a
    # post-publish overlay refresh (a) recomputes ``merged = base || overlay``
    # from the CURRENT version row's raw base, (b) writes any NEW payload into
    # the insert-once version store (hash computed by the one in-database hash
    # site), and (c) repoints the snapshot row's attrs_hash. One statement:
    # the data-modifying ``ins`` CTE and the UPDATE commit atomically, and the
    # composite FK is checked after both. A version row orphaned by the repoint
    # is reclaimed by the version GC in the prune loop.
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            f"""
            WITH overlay AS (
              SELECT target_kind, target_id,
                     jsonb_object_agg(attr_key, value) AS overlay_attrs,
                     count(*) AS n
                FROM {ATTRIBUTE_EFFECTIVE_TABLE}
               WHERE generation_id = %(gen)s
                 AND namespace = %(ns)s
                 AND attr_key <> ALL(%(reserved)s::text[])
               GROUP BY target_kind, target_id
            ),
            current_rows AS (
              SELECT v.target_kind, v.target_id, v.attrs_hash AS old_hash,
                     v.overlay_count AS old_count,
                     COALESCE(av.raw_attrs, av.attrs) AS base
                FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v
                JOIN {ATTRIBUTE_VERSIONS_TABLE} av
                  ON av.target_kind = v.target_kind
                 AND av.target_id = v.target_id
                 AND av.attrs_hash = v.attrs_hash
               WHERE v.generation_id = %(gen)s
                 AND v.namespace = %(ns)s
            ),
            recomputed AS (
              SELECT c.target_kind, c.target_id, c.old_hash, c.old_count,
                     (c.base || COALESCE(o.overlay_attrs, '{{}}'::jsonb))
                       AS new_attrs,
                     NULLIF(c.base,
                            c.base || COALESCE(o.overlay_attrs, '{{}}'::jsonb))
                       AS new_raw,
                     COALESCE(o.n, 0) AS new_count
                FROM current_rows c
                LEFT JOIN overlay o
                  ON o.target_kind = c.target_kind
                 AND o.target_id = c.target_id
            ),
            changed AS (
              SELECT r.*,
                     {ATTRIBUTE_CONTENT_HASH_FN}(r.new_attrs, r.new_raw)
                       AS new_hash
                FROM recomputed r
            ),
            to_write AS (
              SELECT * FROM changed
               WHERE new_hash IS DISTINCT FROM old_hash
                  OR new_count IS DISTINCT FROM old_count
            ),
            ins AS (
              INSERT INTO {ATTRIBUTE_VERSIONS_TABLE}
                (target_kind, target_id, attrs_hash, attrs, raw_attrs)
              SELECT target_kind, target_id, new_hash, new_attrs, new_raw
                FROM to_write
              ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING
            )
            UPDATE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} AS v
               SET attrs_hash = w.new_hash,
                   overlay_count = w.new_count,
                   materialized_at = now()
              FROM to_write w
             WHERE v.generation_id = %(gen)s
               AND v.namespace = %(ns)s
               AND v.target_kind = w.target_kind
               AND v.target_id = w.target_id
            """,
            {"gen": generation, "ns": namespace, "reserved": reserved},
        )
        return cur.rowcount


# PACT bound-okg-workspace-disk-growth, req.bound-disk.intervals-sidecar.
# The interval sidecar (okg.attribute_effective_view_intervals) is the DB's #2
# consumer (34 GB / ~18M rows, 90% CLOSED). Operator ruling (2026-07-17): the
# attribute plane is a DERIVED, rebuildable cache and does NOT inherit the
# chronos KEEP-ALL; CLOSED intervals get a bounded retention horizon MIRRORING
# the snapshot view's window (OKG_EFFECTIVE_VIEW_RETENTION_GENERATIONS). Per-cycle
# work is CAPPED so the publish path never issues an unbounded DELETE over the
# 16M-row backlog; the one-time drain is the standalone backlog function/CLI.
EFFECTIVE_INTERVAL_PRUNE_BATCH = max(
    1, int(os.environ.get("OKG_EFFECTIVE_INTERVAL_PRUNE_BATCH") or "5000")
)


# schema-interval-narrow-split / req.fk-and-gc: the targeted version GC. Every
# prune/delete over the two referencing tables RETURNs the (target_kind,
# target_id, attrs_hash) triples it removed and hands them here; a version row
# is deleted iff NO interval and NO snapshot row still references it. The
# composite FK ON DELETE RESTRICT is defense in depth behind the NOT EXISTS
# probes. Candidate processing is CAPPED per call (bounded work on the publish
# path); leftovers are reclaimed by ``prune_orphan_attribute_versions`` and
# surfaced by the ``check_attribute_version_gc`` growth gate.
_VERSION_GC_CANDIDATE_CAP = 4 * EFFECTIVE_INTERVAL_PRUNE_BATCH


def _version_key_triple(row: Any) -> tuple[str, str, bytes]:
    if isinstance(row, dict):
        return (
            str(row["target_kind"]),
            str(row["target_id"]),
            bytes(row["attrs_hash"]),
        )
    return (str(row[0]), str(row[1]), bytes(row[2]))


def _gc_attribute_versions_for_keys(
    cur: Any,
    keys: Iterable[Any],
    *,
    max_candidates: int | None = _VERSION_GC_CANDIDATE_CAP,
) -> int:
    unique = sorted({_version_key_triple(k) for k in keys})
    if max_candidates is not None:
        unique = unique[:max_candidates]
    if not unique:
        return 0
    kinds = [k[0] for k in unique]
    ids = [k[1] for k in unique]
    hashes = [k[2] for k in unique]
    cur.execute(
        f"""
        WITH candidates AS (
          SELECT DISTINCT c.target_kind, c.target_id, c.attrs_hash
            FROM unnest(%s::text[], %s::text[], %s::bytea[])
                 AS c(target_kind, target_id, attrs_hash)
        )
        DELETE FROM {ATTRIBUTE_VERSIONS_TABLE} v
         USING candidates c
         WHERE v.target_kind = c.target_kind
           AND v.target_id = c.target_id
           AND v.attrs_hash = c.attrs_hash
           AND NOT EXISTS (
                 SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
                  WHERE i.target_kind = v.target_kind
                    AND i.target_id = v.target_id
                    AND i.attrs_hash = v.attrs_hash
           )
           AND NOT EXISTS (
                 SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} s
                  WHERE s.target_kind = v.target_kind
                    AND s.target_id = v.target_id
                    AND s.attrs_hash = v.attrs_hash
           )
        """,
        (kinds, ids, hashes),
    )
    return int(cur.rowcount or 0)


def count_orphan_attribute_versions(conn: psycopg.Connection) -> int:
    """Read-only: version rows with NO interval and NO snapshot referent — the
    exact set the version GC exists to reclaim. The growth-gate signal."""
    if not _table_exists(conn, ATTRIBUTE_VERSIONS_TABLE):
        return 0
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*)
              FROM {ATTRIBUTE_VERSIONS_TABLE} v
             WHERE NOT EXISTS (
                     SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
                      WHERE i.target_kind = v.target_kind
                        AND i.target_id = v.target_id
                        AND i.attrs_hash = v.attrs_hash
             )
               AND NOT EXISTS (
                     SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} s
                      WHERE s.target_kind = v.target_kind
                        AND s.target_id = v.target_id
                        AND s.attrs_hash = v.attrs_hash
             )
            """
        )
        return int(cur.fetchone()[0])


def prune_orphan_attribute_versions(
    conn: psycopg.Connection,
    *,
    batch_size: int = EFFECTIVE_INTERVAL_PRUNE_BATCH,
    max_batches: int | None = None,
) -> dict[str, Any]:
    """Bounded-batch drain of ORPHANED version rows (no interval or snapshot
    referent). The catch-up path behind the targeted per-prune GC: run by the
    repair/maintenance flow, and the recovery verb when the growth gate reds.
    Each batch is its own statement; idempotent (converges to zero deletes)."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    report: dict[str, Any] = {
        "deleted": 0,
        "batches": 0,
        "batch_size": batch_size,
        "drained": True,
    }
    if not _table_exists(conn, ATTRIBUTE_VERSIONS_TABLE):
        return report
    total = 0
    batches = 0
    drained = True
    while max_batches is None or batches < max_batches:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                WITH candidates AS (
                  SELECT v.ctid
                    FROM {ATTRIBUTE_VERSIONS_TABLE} v
                   WHERE NOT EXISTS (
                           SELECT 1
                             FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
                            WHERE i.target_kind = v.target_kind
                              AND i.target_id = v.target_id
                              AND i.attrs_hash = v.attrs_hash
                   )
                     AND NOT EXISTS (
                           SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} s
                            WHERE s.target_kind = v.target_kind
                              AND s.target_id = v.target_id
                              AND s.attrs_hash = v.attrs_hash
                   )
                   LIMIT %(cap)s
                )
                DELETE FROM {ATTRIBUTE_VERSIONS_TABLE}
                 WHERE ctid IN (SELECT ctid FROM candidates)
                """,
                {"cap": batch_size},
            )
            deleted = int(cur.rowcount or 0)
        batches += 1
        total += deleted
        if deleted < batch_size:
            break
    else:
        drained = False
    report.update(deleted=total, batches=batches, drained=drained)
    if total:
        logger.info(
            "attribute version GC: deleted %d orphaned version rows over %d "
            "batch(es) (batch_size=%d, drained=%s)",
            total,
            batches,
            batch_size,
            drained,
        )
    return report


def check_attribute_version_gc(
    conn: psycopg.Connection,
    *,
    max_orphans: int = _VERSION_GC_CANDIDATE_CAP,
) -> dict[str, Any]:
    """The req.fk-and-gc GROWTH GATE: red when orphaned version rows exceed
    ``max_orphans`` — the signature of a disabled or broken version GC (the
    version store would otherwise re-create the unbounded-growth disease in the
    headline table). Doctor-style: returns status, never raises."""
    result: dict[str, Any] = {
        "status": "ok",
        "max_orphans": int(max_orphans),
        "orphan_count": 0,
        "version_count": 0,
    }
    if not _table_exists(conn, ATTRIBUTE_VERSIONS_TABLE):
        result["status"] = "absent"
        return result
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE}")
        result["version_count"] = int(cur.fetchone()[0])
    result["orphan_count"] = count_orphan_attribute_versions(conn)
    if result["orphan_count"] > max_orphans:
        result["status"] = "red"
        result["reason"] = (
            "orphaned attribute_versions rows exceed the growth bound; the "
            "version GC is disabled or broken — run "
            "prune_orphan_attribute_versions and investigate the prune loop"
        )
    return result


def check_attribute_version_hash_honesty(
    conn: psycopg.Connection,
    *,
    sample: int = 1000,
) -> dict[str, Any]:
    """The req.sigma-pinned DOCTOR CHECK: recompute the content hash from the
    STORED payload for up to ``sample`` version rows and compare against the
    stored ``attrs_hash``. Red on ANY mismatch — a dishonest hash silently
    corrupts every reader join, the keep-open comparison, and the parity
    fingerprint, so it must never pass quietly."""
    result: dict[str, Any] = {
        "status": "ok",
        "sampled": 0,
        "mismatched": 0,
        "sample_limit": int(sample),
    }
    if not _table_exists(conn, ATTRIBUTE_VERSIONS_TABLE):
        result["status"] = "absent"
        return result
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*) AS sampled,
                   count(*) FILTER (
                     WHERE s.attrs_hash IS DISTINCT FROM
                           {ATTRIBUTE_CONTENT_HASH_FN}(s.attrs, s.raw_attrs)
                   ) AS mismatched
              FROM (
                SELECT attrs_hash, attrs, raw_attrs
                  FROM {ATTRIBUTE_VERSIONS_TABLE}
                 ORDER BY target_kind, target_id, attrs_hash
                 LIMIT %(sample)s
              ) s
            """,
            {"sample": int(sample)},
        )
        sampled, mismatched = cur.fetchone()
    result["sampled"] = int(sampled)
    result["mismatched"] = int(mismatched)
    if result["mismatched"]:
        result["status"] = "red"
        result["reason"] = (
            "stored attrs_hash disagrees with the in-database recompute over "
            "the stored payload — the content-addressed store is dishonest"
        )
    return result


def delete_effective_attribute_view_for_generation(
    conn: psycopg.Connection,
    generation_id: int | str,
    *,
    namespace: str | None = None,
) -> int:
    """Prune the effective-attrs view snapshot for a generation. Tied to
    ``delete_generation`` so the snapshot does not leak after its checkpoint is
    removed (the per-generation snapshot is the unbounded-growth surface; the
    overlay table was tiny so it never mattered, a full node snapshot at scale
    does). Returns rows deleted. No-op if the table is absent."""
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE):
        return 0
    with conn.cursor() as cur:
        if namespace is None:
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s "
                "RETURNING target_kind, target_id, attrs_hash",
                (str(generation_id),),
            )
        else:
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = %s "
                "RETURNING target_kind, target_id, attrs_hash",
                (str(generation_id), namespace),
            )
        deleted_keys = cur.fetchall()
        _gc_attribute_versions_for_keys(cur, deleted_keys)
        return len(deleted_keys)


# okg#400: bound the view's disk footprint. Each per-generation snapshot is a
# FULL O(all) copy (GBs on a large graph); the read path only ever needs the
# LATEST generation's snapshot, so older snapshots otherwise accumulate up to
# the chronos generation-retention and fill the disk (on okg-workspace the view
# reached 32 GB = 5 generations x ~6.4 GB). Keep the most recently materialized
# N; env-overridable for deployments that want more historical pins.
EFFECTIVE_VIEW_RETENTION_GENERATIONS = max(
    1, int(os.environ.get("OKG_EFFECTIVE_VIEW_RETENTION_GENERATIONS") or "2")
)


def prune_effective_attribute_view_to_recent(
    conn: psycopg.Connection,
    *,
    keep_generations: int = EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    protected_generation_id: int | str | None = None,
) -> int:
    """Delete effective-attrs view snapshots older than the most recently
    materialized ``keep_generations`` generations, PER NAMESPACE. Returns rows
    deleted; no-op if the table is absent or ``keep_generations < 1``. See okg#400
    -- without this the view grew to 32 GB on okg-workspace and filled the disk.
    With no protected generation, generations are ranked by their latest
    ``materialized_at`` WITHIN each namespace.

    Publish-time callers pass the candidate generation as protected. The
    candidate does not yet have a checkpoint, so the remaining retention budget
    is reserved for snapshots whose checkpoint status is ``published``, ranked
    by checkpoint creation order rather than snapshot rebuild time. This
    prevents a run of blocked candidates from evicting the last readable
    published snapshot. A global rank would also let one busy namespace's newer
    generations evict another namespace's current generation, so both paths rank
    independently per namespace.

    okg#1324 -- this prune deliberately does NOT rebuild the table's indexes.
    It is the drain that left ``okg.attribute_effective_view`` carrying
    2,333 MB of index over 16 surviving rows on okg-workspace: VACUUM reclaims
    the dead tuples and leaves the btree pages behind, so the hot primary key
    kept walking a tree sized for millions of long-gone rows. A REINDEX here is
    nonetheless wrong in both available forms. ``CONCURRENTLY`` cannot run
    inside a transaction block, and both callers wrap this function in one --
    the publish-path materializer, and the lifecycle repair, which must keep
    the prune and its version GC atomic. Plain ``REINDEX`` does run in a
    transaction but takes ACCESS EXCLUSIVE on a table the read path is actively
    querying, so a publish would block every reader for the length of a
    multi-gigabyte rebuild: exactly the outage the operator avoided by running
    ``CONCURRENTLY`` by hand. The rebuild therefore stays an operator decision,
    and ``okg doctor --check disk`` reports the condition with the exact
    command (finding code ``index_larger_than_table``)."""
    if keep_generations < 1 or not _table_exists(
        conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE
    ):
        return 0
    protected = (
        str(protected_generation_id)
        if protected_generation_id is not None
        else None
    )
    with conn.cursor() as cur:
        checkpoints_readable = _table_exists(
            conn, "_chronos_branch_interval_checkpoints"
        )
        if protected is not None and not checkpoints_readable:
            # Without checkpoints there is no way to tell a published snapshot
            # from a blocked candidate, so retention degrades to the recency
            # rank below -- which is exactly the eviction the protected path
            # exists to prevent. Say so; never degrade in silence.
            logger.warning(
                "effective-attrs retention degraded: "
                "_chronos_branch_interval_checkpoints is not readable, so "
                "published snapshots cannot be distinguished from blocked "
                "candidates and generation %s is retained by recency only",
                protected,
            )
        if protected is not None and checkpoints_readable:
            published_budget = max(0, keep_generations - 1)
            cur.execute(
                f"""
                WITH per_generation AS (
                    SELECT generation_id, namespace,
                           MAX(materialized_at) AS max_materialized_at
                      FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
                     GROUP BY generation_id, namespace
                ),
                published AS (
                    SELECT snapshots.generation_id,
                           snapshots.namespace,
                           row_number() OVER (
                               PARTITION BY snapshots.namespace
                               ORDER BY checkpoints.created_at DESC,
                                        checkpoints.checkpoint_id DESC
                           ) AS rnk
                      FROM per_generation snapshots
                      JOIN _chronos_branch_interval_checkpoints checkpoints
                        ON checkpoints.checkpoint_id =
                           snapshots.generation_id
                     WHERE snapshots.generation_id <> %s
                       AND (
                           checkpoints.metadata::jsonb ->> 'status'
                       ) = 'published'
                ),
                -- Floor for a namespace with NO published snapshot at all:
                -- fall back to the recency rank the unprotected path would
                -- have kept. Without it such a namespace retains ONLY the
                -- candidate and this statement deletes every other generation
                -- in one shot -- a prune strictly MORE destructive than the
                -- one it replaces. A namespace that HAS a published snapshot
                -- is decided by publication order alone, or a blocked
                -- candidate would be readmitted by recency and the eviction
                -- this function exists to prevent would come back.
                recent AS (
                    SELECT generation_id, namespace,
                           row_number() OVER (
                               PARTITION BY namespace
                               ORDER BY max_materialized_at DESC,
                                        generation_id DESC
                           ) AS rnk
                      FROM per_generation
                     WHERE generation_id <> %s
                       AND namespace NOT IN (SELECT namespace FROM published)
                ),
                retained AS (
                    SELECT generation_id, namespace
                      FROM per_generation
                     WHERE generation_id = %s
                    UNION
                    SELECT generation_id, namespace
                      FROM published
                     WHERE rnk <= %s
                    UNION
                    SELECT generation_id, namespace
                      FROM recent
                     WHERE rnk <= %s
                )
                DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} t
                 WHERE (t.generation_id, t.namespace) NOT IN (
                     SELECT generation_id, namespace FROM retained
                 )
                 RETURNING t.target_kind, t.target_id, t.attrs_hash
                """,
                (
                    protected,
                    protected,
                    protected,
                    published_budget,
                    published_budget,
                ),
            )
            # req.fk-and-gc: the protected path is the one every publish now
            # takes, so it must reclaim orphaned version rows too. Returning
            # rowcount without this leaks okg.attribute_versions forever --
            # which is the growth okg#400 exists to stop.
            deleted_keys = cur.fetchall()
            _gc_attribute_versions_for_keys(cur, deleted_keys)
            return len(deleted_keys)
        cur.execute(
            f"""
            DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} t
             WHERE (t.generation_id, t.namespace) NOT IN (
                 SELECT generation_id, namespace
                   FROM (
                       -- row_number (not dense_rank) is a HARD cap of exactly
                       -- keep_generations per namespace; generation_id breaks
                       -- materialized_at ties deterministically so concurrent
                       -- same-instant publishes can't over-retain.
                       SELECT generation_id, namespace,
                              row_number() OVER (
                                  PARTITION BY namespace
                                  ORDER BY max_materialized_at DESC,
                                           generation_id DESC
                              ) AS rnk
                         FROM (
                             SELECT generation_id, namespace,
                                    MAX(materialized_at) AS max_materialized_at
                               FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
                              GROUP BY generation_id, namespace
                         ) per_generation
                   ) ranked
                  WHERE ranked.rnk <= %s
             )
             RETURNING t.target_kind, t.target_id, t.attrs_hash
            """,
            (keep_generations,),
        )
        deleted_keys = cur.fetchall()
        # req.fk-and-gc: reclaim version rows this prune orphaned, in the SAME
        # maintenance step (bounded candidate set — the just-deleted triples).
        _gc_attribute_versions_for_keys(cur, deleted_keys)
        return len(deleted_keys)


def prune_effective_interval_view_closed(
    conn: psycopg.Connection,
    *,
    keep_generations: int = EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    max_delete: int = EFFECTIVE_INTERVAL_PRUNE_BATCH,
    branch: str | None = None,
    protect_from_ord: int | None = None,
) -> dict[str, Any]:
    """Delete CLOSED interval rows that fell out of the retention window, in ONE
    bounded batch (<= ``max_delete`` rows). Returns a LOUD report.

    GENERATION -> ORDINAL WINDOW MAPPING (the honest mapping this function picks):
    the snapshot view keeps the most-recent ``keep_generations`` generations. The
    interval table is keyed on branch-scoped generation ORDINALS
    (``okg.generation_ordinals``) -- the CAUSAL order (chain-head derived), which
    is sounder than the snapshot prune's ``materialized_at`` wall-clock rank and
    is EXACTLY what a read stab pins (``from_ord <= ordinal(G) < to_ord``). So the
    window is expressed in ordinals: for each branch, ``keep_from_ord`` = the
    ``keep_generations``-th HIGHEST ordinal present for that branch (the oldest
    generation still inside the window). A read pinned to any retained generation
    stabs at an ordinal ``>= keep_from_ord``, and an interval is live at such an
    ordinal iff ``to_ord > keep_from_ord``. Therefore a CLOSED interval is
    prunable iff ``to_ord <= keep_from_ord`` -- it is not live at any retained
    generation. No typed reader recomputes a pruned interval: a typed MCP read
    (such as filter) pinned below the window is refused,
    saying so (#2621), while ``inspect`` of a node pinned to that generation,
    which reads the raw chronos attrs (KEEP-ALL) plus the overlay, still
    serves it. A branch with fewer than
    ``keep_generations`` ordinals has no cutoff row and is left fully intact.

    OPEN intervals (``to_ord = EFFECTIVE_INTERVAL_OPEN_ORD``, the current live
    front) are NEVER touched: they are excluded by the ``to_ord < OPEN`` guard AND
    are always ``> keep_from_ord``. Idempotent: a re-run deletes the next batch and
    eventually matches nothing (returns 0). ``branch`` scopes the prune to one
    branch (the publish path passes the branch it just published); ``None`` prunes
    every branch, each against its own per-branch cutoff.

    ``protect_from_ord`` LOWERS the cutoff to ``LEAST(keep_from_ord,
    protect_from_ord)`` so history a live consumer still needs survives the
    window. The publish path passes the ordinal of the latest attribute-integrity
    receipt: the incremental receipt computes its delta against the interval STAB
    at that ordinal, so pruning it (which ``keep_generations = 1`` does on every
    publish) leaves the delta reading an EMPTY old side -- a wrong count that
    fails the publish (okg#1603). Requires ``branch``; ordinals are branch-scoped.
    """
    if protect_from_ord is not None and branch is None:
        raise ValueError(
            "prune_effective_interval_view_closed: protect_from_ord requires a "
            "branch-scoped prune"
        )
    report: dict[str, Any] = {
        "deleted": 0,
        "versions_deleted": 0,
        "capped": False,
        "keep_generations": keep_generations,
        "max_delete": max_delete,
        "branch": branch,
        "protect_from_ord": protect_from_ord,
    }
    if (
        keep_generations < 1
        or max_delete < 1
        or not _table_exists(conn, ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE)
    ):
        return report
    branch_filter = "AND branch_id = %(branch)s" if branch is not None else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH branch_cutoff AS (
                -- keep_from_ord = the keep_generations-th highest ordinal per
                -- branch = the oldest generation still inside the window.
                SELECT branch_id, keep_from_ord
                  FROM (
                      SELECT branch_id,
                             ordinal AS keep_from_ord,
                             row_number() OVER (
                                 PARTITION BY branch_id ORDER BY ordinal DESC
                             ) AS rnk
                        FROM okg.generation_ordinals
                       WHERE TRUE {branch_filter}
                  ) ranked
                 WHERE ranked.rnk = %(keep)s
            ),
            candidates AS (
                SELECT i.ctid
                  FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
                  JOIN branch_cutoff c ON c.branch_id = i.branch_id
                 WHERE i.to_ord < %(open)s               -- CLOSED only
                   -- outside the window AND not protected for a live consumer
                   AND i.to_ord <= LEAST(
                           c.keep_from_ord,
                           COALESCE(%(protect)s::bigint, c.keep_from_ord)
                       )
                 LIMIT %(cap)s
            )
            DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
             WHERE ctid IN (SELECT ctid FROM candidates)
             RETURNING target_kind, target_id, attrs_hash
            """,
            {
                "keep": keep_generations,
                "open": EFFECTIVE_INTERVAL_OPEN_ORD,
                "cap": max_delete,
                "branch": branch,
                "protect": protect_from_ord,
            },
        )
        deleted_keys = cur.fetchall()
        deleted = len(deleted_keys)
        # req.fk-and-gc: the version GC runs in the SAME maintenance loop as
        # this closed-interval prune — reclaim version rows the prune orphaned.
        versions_deleted = _gc_attribute_versions_for_keys(cur, deleted_keys)
    report["deleted"] = deleted
    report["versions_deleted"] = versions_deleted
    report["capped"] = deleted >= max_delete
    if deleted:
        logger.info(
            "effective interval prune: deleted %d closed interval rows "
            "(keep_generations=%d, max_delete=%d, branch=%s, capped=%s)",
            deleted,
            keep_generations,
            max_delete,
            branch,
            report["capped"],
        )
    return report


def count_prunable_closed_intervals(
    conn: psycopg.Connection,
    *,
    keep_generations: int = EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    branch: str | None = None,
) -> int:
    """Read-only: how many CLOSED interval rows are outside the retention window
    (the set :func:`prune_effective_interval_view_closed` would delete for the
    same ``keep_generations`` and NO ``protect_from_ord``; the publish path
    protects the latest attribute-integrity receipt ordinal, so it can delete
    fewer than this).
    The single source of the window predicate for the CLI dry-run and the
    window-respecting witness -- ZERO open intervals are ever counted."""
    if keep_generations < 1 or not _table_exists(
        conn, ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
    ):
        return 0
    branch_filter = "AND branch_id = %(branch)s" if branch is not None else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH branch_cutoff AS (
                SELECT branch_id, keep_from_ord
                  FROM (
                      SELECT branch_id,
                             ordinal AS keep_from_ord,
                             row_number() OVER (
                                 PARTITION BY branch_id ORDER BY ordinal DESC
                             ) AS rnk
                        FROM okg.generation_ordinals
                       WHERE TRUE {branch_filter}
                  ) ranked
                 WHERE ranked.rnk = %(keep)s
            )
            SELECT count(*)
              FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
              JOIN branch_cutoff c ON c.branch_id = i.branch_id
             WHERE i.to_ord < %(open)s
               AND i.to_ord <= c.keep_from_ord
            """,
            {
                "keep": keep_generations,
                "open": EFFECTIVE_INTERVAL_OPEN_ORD,
                "branch": branch,
            },
        )
        return int(cur.fetchone()[0])


def prune_effective_interval_view_backlog(
    conn: psycopg.Connection,
    *,
    keep_generations: int = EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    batch_size: int = EFFECTIVE_INTERVAL_PRUNE_BATCH,
    max_batches: int | None = None,
    branch: str | None = None,
) -> dict[str, Any]:
    """Drain the CLOSED-interval backlog in bounded batches until the window is
    honored (or ``max_batches`` is hit). Standalone entry the orchestrator calls
    ONCE against the live DB to reclaim the accumulated ~16M-row sidecar; NOT run
    on the publish path (that uses a single capped
    :func:`prune_effective_interval_view_closed`). Each batch is its own
    statement/commit, so a long drain never holds one giant transaction. Returns
    a LOUD report: total deleted, batches run, and whether it fully drained."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    total = 0
    batches = 0
    drained = True
    while max_batches is None or batches < max_batches:
        result = prune_effective_interval_view_closed(
            conn,
            keep_generations=keep_generations,
            max_delete=batch_size,
            branch=branch,
        )
        batches += 1
        total += int(result["deleted"])
        if int(result["deleted"]) < batch_size:
            break
    else:
        # Exhausted max_batches without a short final batch: work may remain.
        drained = False
    report = {
        "deleted": total,
        "batches": batches,
        "batch_size": batch_size,
        "keep_generations": keep_generations,
        "branch": branch,
        "drained": drained,
    }
    logger.info(
        "effective interval backlog prune: deleted %d closed rows over %d "
        "batch(es) (batch_size=%d, keep_generations=%d, branch=%s, drained=%s)",
        total,
        batches,
        batch_size,
        keep_generations,
        branch,
        drained,
    )
    return report


def backfill_effective_interval_raw_attrs_dedup(
    conn: psycopg.Connection,
    *,
    batch_size: int = EFFECTIVE_INTERVAL_PRUNE_BATCH,
    max_batches: int | None = None,
    include_snapshot: bool = True,
) -> dict[str, Any]:
    """One-time bounded-batch backfill: NULL out ``raw_attrs`` on EXISTING rows
    where it is byte-identical to ``attrs`` (presence-means-overlay), reclaiming
    the pure 2x jsonb duplication (measured 100% of a 200k sample). Bounded
    batches, NOT an unbounded migration UPDATE. Idempotent: once a row is NULLed
    the ``raw_attrs IS NOT NULL`` predicate excludes it. NULLs ONLY exact
    ``raw_attrs = attrs`` pairs, so an overlay row (raw_attrs byte-differs) is
    never touched and reads stay equal to recompute. Covers the interval sidecar
    and, by default, the snapshot view so the two share the canonical
    representation the parity fingerprint compares. Returns a LOUD report.

    schema-interval-narrow-split: on a POST-SPLIT deployment the interval and
    snapshot tables no longer carry ``raw_attrs``/``attrs`` at all (payloads
    live in okg.attribute_versions, where presence-means-overlay is enforced by
    the writer's NULLIF and sealed by the content hash — a post-hoc NULLing
    would make the stored hash dishonest). This backfill then reports a no-op
    with an explicit reason instead of erroring on the missing column."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    def _has_raw_attrs_column(table: str) -> bool:
        schema, _, name = table.rpartition(".")
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                 WHERE table_schema = %s AND table_name = %s
                   AND column_name = 'raw_attrs'
                """,
                (schema or "public", name),
            )
            return cur.fetchone() is not None

    def _drain(table: str) -> tuple[int, int, bool]:
        if not _table_exists(conn, table) or not _has_raw_attrs_column(table):
            return (0, 0, True)
        total = 0
        batches = 0
        drained = True
        while max_batches is None or batches < max_batches:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    WITH candidates AS (
                        SELECT ctid
                          FROM {table}
                         WHERE raw_attrs IS NOT NULL
                           AND raw_attrs = attrs
                         LIMIT %(cap)s
                    )
                    UPDATE {table}
                       SET raw_attrs = NULL
                     WHERE ctid IN (SELECT ctid FROM candidates)
                    """,
                    {"cap": batch_size},
                )
                nulled = int(cur.rowcount or 0)
            batches += 1
            total += nulled
            if nulled < batch_size:
                break
        else:
            drained = False
        return (total, batches, drained)

    intervals_nulled, interval_batches, interval_drained = _drain(
        ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
    )
    snapshot_nulled, snapshot_batches, snapshot_drained = (
        _drain(ATTRIBUTE_EFFECTIVE_VIEW_TABLE) if include_snapshot else (0, 0, True)
    )
    report = {
        "intervals_nulled": intervals_nulled,
        "interval_batches": interval_batches,
        "snapshot_nulled": snapshot_nulled,
        "snapshot_batches": snapshot_batches,
        "batch_size": batch_size,
        "drained": interval_drained and snapshot_drained,
        "narrow_split_active": not _has_raw_attrs_column(
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE
        ),
    }
    logger.info(
        "effective interval raw_attrs dedup: nulled %d interval + %d snapshot "
        "duplicate raw_attrs (batch_size=%d, drained=%s)",
        intervals_nulled,
        snapshot_nulled,
        batch_size,
        report["drained"],
    )
    return report


def backfill_effective_attribute_view(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """MUST-FIX #3 (backfill): materialize the effective-attrs snapshot for the
    LATEST published generation if it doesn't already have one, so the SQL fast
    path turns on for an already-published deployment WITHOUT requiring a
    republish. Called from the upgrade/migrate path (``okg up`` / ``okg
    migrate``).

    IDEMPOTENT: re-running is a no-op when the latest published generation
    already has a snapshot (the materializer short-circuits on
    ``already_materialized``). Reads the raw base from the PINNED published
    checkpoint (``from_checkpoint=True``), not the live branch head.

    Returns a status dict. Best-effort and fail-soft on a deployment with no
    published generation or no attribute schema (returns ``materialized: False``
    with a reason) -- migrate must not fail because a snapshot couldn't be built.
    """
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE):
        return {"materialized": False, "reason": "view_table_absent"}
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_TABLE):
        return {"materialized": False, "reason": "attribute_schema_absent"}

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name, branch=branch, status="published"
    )
    if latest is None:
        return {"materialized": False, "reason": "no_published_generation"}
    generation_id = latest.checkpoint_id
    return materialize_effective_attribute_view_for_generation(
        conn,
        generation_id=generation_id,
        branch=branch,
        namespace=namespace,
        from_checkpoint=True,
    )


_EFFECTIVE_INTERVAL_REPAIR_FAILPOINT: Callable[[], None] | None = None
_EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT: Callable[[str], None] | None = None

# schema-interval-narrow-split: ``attrs_hash`` -- the payload term of the parity
# fingerprint below -- is CREATED by this migration. A database older than it
# still carries the wide jsonb shape, so the fingerprint cannot be computed at
# all, and the honest verdict is neither "parity holds" nor "parity is broken":
# the check is NOT APPLICABLE. Hashing only the columns that happen to exist
# would be worse than crashing -- it drops the payload term from BOTH sides, so
# two row sets that differ ONLY in their attributes fingerprint identically and
# the caller records a match from a comparison that never ran. Name the
# condition instead, so a caller can tell "not asked" from "asked and agreed".
_INTERVAL_NARROW_SPLIT_MIGRATION = "0020_interval_narrow_split"
_INTERVAL_PAYLOAD_PARITY_APPLICABLE = "applicable"
_INTERVAL_PAYLOAD_PARITY_PRE_SPLIT = "schema_predates_interval_narrow_split"
_INTERVAL_PAYLOAD_PARITY_RELATIONS_ABSENT = "attribute_relations_absent"


def _relation_payload_columns(
    conn: psycopg.Connection, table: str
) -> frozenset[str]:
    """Return which ``_INTERVAL_PAYLOAD_COLS`` ``table`` actually carries."""
    schema, _, name = table.rpartition(".")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
               AND column_name = ANY(%s)
            """,
            (schema or "public", name, list(_INTERVAL_PAYLOAD_COLS)),
        )
        return frozenset(str(row[0]) for row in cur.fetchall())


def _effective_interval_payload_parity_applicability(
    conn: psycopg.Connection,
) -> dict[str, Any]:
    """Report whether the snapshot-vs-interval parity fingerprint is computable.

    Applicable only when BOTH relations carry the FULL post-split payload column
    set. The fingerprint's meaning depends entirely on the two sides being
    hashed over the SAME columns, so a partially-migrated pair is as
    unanswerable as a fully pre-split one -- and a missing relation (never
    migrated at all) is unanswerable too. The report names the missing columns
    per relation and the migration that installs them, so the caller can act on
    the schema instead of guessing about the data.
    """
    missing: dict[str, list[str]] = {}
    absent: list[str] = []
    for table in (
        ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
        ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ):
        present = _relation_payload_columns(conn, table)
        if not present:
            # No shape of these relations, wide or narrow, has zero payload
            # columns -- so an empty probe means the relation is gone. Confirm
            # with an UNCACHED existence read: ``_table_exists`` memoizes
            # positives for the process, and a relation dropped out of band
            # would then be misreported as merely pre-split.
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s)", (table,))
                if cur.fetchone()[0] is None:
                    absent.append(table)
            missing[table] = list(_INTERVAL_PAYLOAD_COLS)
            continue
        gap = [
            column for column in _INTERVAL_PAYLOAD_COLS if column not in present
        ]
        if gap:
            missing[table] = gap
    if not missing:
        return {
            "applicable": True,
            "reason": _INTERVAL_PAYLOAD_PARITY_APPLICABLE,
            "payload_columns": list(_INTERVAL_PAYLOAD_COLS),
        }
    return {
        "applicable": False,
        # An absent relation outranks a stale one: "the schema is not installed
        # here" and "the schema is older than the split" want different fixes,
        # so they never share a reason string.
        "reason": (
            _INTERVAL_PAYLOAD_PARITY_RELATIONS_ABSENT
            if absent
            else _INTERVAL_PAYLOAD_PARITY_PRE_SPLIT
        ),
        "payload_columns": list(_INTERVAL_PAYLOAD_COLS),
        "missing_payload_columns": {key: missing[key] for key in sorted(missing)},
        "absent_relations": sorted(absent),
        "required_migration": _INTERVAL_NARROW_SPLIT_MIGRATION,
    }


def _effective_interval_payload_signatures(
    conn: psycopg.Connection,
    *,
    generation: str,
    namespace: str,
    branch_id: str,
    ordinal: int,
) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    """Return snapshot/stab cardinality plus two seeded row-set accumulators.

    ``EXCEPT`` over hundreds of thousands of JSON payloads spills and sorts the
    entire read model (30+ minutes on okg-ci).  Two independently seeded,
    commutative extended-hash sums strengthen the maintained-derivation
    fingerprint already used by interval backfill: order-independent,
    payload-complete, and paired with exact row counts.  It keeps both the
    idempotence check and post-write parity O(rows).

    Refuses -- loudly, naming the migration -- on a schema that cannot carry the
    full payload column set, rather than silently narrowing the comparison to
    the surviving columns.  Every caller here is a MUTATOR proving its own
    write; a fingerprint that skipped the payload term would sign off on a
    rebuild it never verified.
    """
    applicability = _effective_interval_payload_parity_applicability(conn)
    if not applicability["applicable"]:
        raise SubstrateError(
            "effective-attribute payload parity is not computable on this schema",
            data=applicability,
            fix_hint=(
                f"apply migration {_INTERVAL_NARROW_SPLIT_MIGRATION} "
                "(okg up runs it in the migrate phase), then re-run this check"
            ),
        )

    def fingerprint(alias: str, seed: int) -> str:
        columns = (
            "target_kind",
            "target_id",
            "namespace",
            *_INTERVAL_PAYLOAD_COLS,
        )
        payload = " || E'\\x1f' || ".join(
            f"COALESCE({alias}.{column}::text, E'\\x1e')"
            for column in columns
        )
        return f"COALESCE(sum(hashtextextended({payload}, {seed})::numeric), 0)"

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*)::bigint, {fingerprint('s', 0)}, "
            f"{fingerprint('s', 1)} "
            f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} AS s "
            "WHERE s.generation_id = %s AND s.namespace = %s",
            (generation, namespace),
        )
        snapshot_signature = tuple(cur.fetchone())
        cur.execute(
            f"SELECT count(*)::bigint, {fingerprint('i', 0)}, "
            f"{fingerprint('i', 1)} "
            f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} AS i "
            "WHERE i.branch_id = %s AND i.namespace = %s "
            "AND i.from_ord <= %s AND %s < i.to_ord",
            (branch_id, namespace, ordinal, ordinal),
        )
        interval_signature = tuple(cur.fetchone())
    return snapshot_signature, interval_signature


def _effective_interval_payload_parity(
    conn: psycopg.Connection,
    *,
    generation: str,
    namespace: str,
    branch_id: str,
    ordinal: int,
) -> bool:
    snapshot, interval = _effective_interval_payload_signatures(
        conn,
        generation=generation,
        namespace=namespace,
        branch_id=branch_id,
        ordinal=ordinal,
    )
    return snapshot == interval


def repair_effective_attribute_interval_view(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str = "default",
    namespace: str = DEFAULT_NAMESPACE,
    lock_wait_seconds: float = 30.0,
) -> dict[str, Any]:
    """Repair the latest published generation's Nomos-safe attribute read model.

    Interval-mode publishes intentionally keep only changed rows in the
    per-generation snapshot.  A deployment upgraded from an incomplete first
    interval publish can therefore have two partial derived caches that agree
    with neither the published graph nor its checkpoint.  This lifecycle repair
    rebuilds a FULL pinned snapshot when needed, reconciles intervals from that
    authority, and proves exact count and payload parity before returning.

    The graph facts and checkpoint are never modified.  The publisher session
    lock freezes the latest generation for the duration of both derived-cache
    transactions.  Snapshot reconstruction commits before interval reconcile
    because Chronos checkpoint checkout owns its transaction boundary; if the
    second transaction fails, the newly full snapshot is safe and the still-
    incomplete interval reader continues to fail closed until a retry.
    """
    from okg.substrate.errors import SubstrateError
    from okg.substrate.attribute_integrity import (
        check_attribute_integrity_fast_path,
        ensure_attribute_integrity_schema,
        finalize_full_attribute_integrity_anchor,
        mark_attribute_integrity_dirty,
        register_attribute_integrity_scope,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    if not conn.autocommit:
        raise SubstrateError(
            "effective-attribute repair requires an autocommit connection",
            fix_hint="open a dedicated autocommit connection for lifecycle repair",
        )
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    with publisher_publish_lock_on_connection(
        conn=conn,
        deployment=deployment_name,
        wait_seconds=lock_wait_seconds,
    ):
        latest = store.latest_generation(
            deployment_name=deployment_name,
            branch=branch,
            status="published",
        )
        if latest is None:
            return {
                "ok": True,
                "repaired": False,
                "reason": "no_published_generation",
            }
        generation = str(latest.checkpoint_id)
        metadata = dict(getattr(latest, "metadata", None) or {})
        try:
            checkpoint_counts = {
                "node": int(metadata["node_count"]),
                "edge": int(metadata["edge_count"]),
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise SubstrateError(
                "published generation has no trustworthy node/edge counts",
                data={"generation_id": generation},
                fix_hint="publish a generation with stamped checkpoint counts",
            ) from exc

        branch_id, ordinal = resolve_generation_ordinal_row(
            store.context.db, generation
        )
        register_attribute_integrity_scope(
            conn,
            deployment_name=deployment_name,
            branch=branch_id,
            namespace=namespace,
        )
        fast_path = check_attribute_integrity_fast_path(
            conn,
            deployment_name=deployment_name,
            branch=branch_id,
            namespace=namespace,
            generation_id=generation,
            ordinal=ordinal,
            checkpoint_counts=checkpoint_counts,
        )
        if fast_path.get("ok") is True:
            return {
                "ok": True,
                "repaired": False,
                "reason": "already_repaired",
                "generation_id": generation,
                "branch": branch_id,
                "ordinal": ordinal,
                "checkpoint_counts": checkpoint_counts,
                "snapshot_counts": checkpoint_counts,
                "interval_counts": checkpoint_counts,
                "payload_match": True,
                "snapshot_rebuilt": False,
                "interval_reconcile": {
                    "mode": "already_repaired",
                    "rows_written": 0,
                },
                "integrity_receipt": fast_path,
            }
        # Restore any missing DDL only after the receipt check has observed and
        # rejected the schema mismatch.  Repair then re-anchors against the
        # restored trigger/function identity.
        ensure_attribute_integrity_schema(conn)
        mark_attribute_integrity_dirty(
            conn,
            deployment_name=deployment_name,
            branch=branch_id,
            namespace=namespace,
            reason=str(fast_path.get("reason") or "receipt_fallback"),
        )

        session = store.checkout_generation(generation, branch=branch)
        live_counts = {
            "node": int(session.query("SELECT count(*) AS n FROM okg.graph_nodes", {})[0]["n"]),
            "edge": int(session.query("SELECT count(*) AS n FROM okg.graph_edges", {})[0]["n"]),
        }
        if live_counts != checkpoint_counts:
            raise SubstrateError(
                "published checkpoint counts do not match its pinned graph",
                data={
                    "generation_id": generation,
                    "checkpoint_counts": checkpoint_counts,
                    "live_counts": live_counts,
                },
            )

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = %s GROUP BY target_kind",
                (generation, namespace),
            )
            before_snapshot = {str(row[0]): int(row[1]) for row in cur.fetchall()}
        before_snapshot = {
            "node": before_snapshot.get("node", 0),
            "edge": before_snapshot.get("edge", 0),
        }
        rebuilt_snapshot = before_snapshot != checkpoint_counts
        materialized: dict[str, Any] | None = None
        if rebuilt_snapshot:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                        "WHERE generation_id = %s AND namespace = %s",
                        (generation, namespace),
                    )
            materialized = materialize_effective_attribute_view_for_generation(
                conn,
                generation_id=generation,
                branch=branch,
                previous_generation_id=None,
                namespace=namespace,
                from_checkpoint=True,
                empty_overlay_fast_path=False,
                dirty_targets=None,
            )

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = %s GROUP BY target_kind",
                (generation, namespace),
            )
            snapshot_counts = {str(row[0]): int(row[1]) for row in cur.fetchall()}
        snapshot_counts = {
            "node": snapshot_counts.get("node", 0),
            "edge": snapshot_counts.get("edge", 0),
        }
        if snapshot_counts != checkpoint_counts:
            raise SubstrateError(
                "full effective-attribute snapshot does not match checkpoint counts",
                data={
                    "generation_id": generation,
                    "checkpoint_counts": checkpoint_counts,
                    "snapshot_counts": snapshot_counts,
                },
            )

        with conn.transaction():
            # Repair authority begins at the latest published ordinal. Preserve
            # all earlier interval history, but do not drive the generic FULL
            # sync's payload-by-payload comparison over a damaged open front:
            # on a large deployment that degraded into a 20+ minute UPDATE.
            # If the latest stab is already exact this is a zero-write no-op;
            # otherwise close the older front once and reopen the full pinned
            # snapshot at this ordinal.
            (
                pre_snapshot_signature,
                pre_interval_signature,
            ) = _effective_interval_payload_signatures(
                conn,
                generation=generation,
                namespace=namespace,
                branch_id=branch_id,
                ordinal=ordinal,
            )
            needs_interval_rebuild = (
                pre_snapshot_signature != pre_interval_signature
            )
            if needs_interval_rebuild:
                with conn.cursor() as cur:
                    cur.execute(
                        f"LOCK TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                        "IN SHARE ROW EXCLUSIVE MODE"
                    )
                    cur.execute(
                        f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                        "WHERE branch_id = %s AND namespace = %s "
                        "AND from_ord = %s",
                        (branch_id, namespace, ordinal),
                    )
                    deleted_current = int(cur.rowcount or 0)
                    cur.execute(
                        f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                        "SET to_ord = %s, materialized_at = now() "
                        "WHERE branch_id = %s AND namespace = %s "
                        "AND to_ord = %s AND from_ord < %s",
                        (
                            ordinal,
                            branch_id,
                            namespace,
                            EFFECTIVE_INTERVAL_OPEN_ORD,
                            ordinal,
                        ),
                    )
                    closed_older = int(cur.rowcount or 0)
                    if _EFFECTIVE_INTERVAL_REPAIR_FAILPOINT is not None:
                        _EFFECTIVE_INTERVAL_REPAIR_FAILPOINT()
                    insert_cols = ", ".join(
                        (
                            "target_kind",
                            "target_id",
                            "namespace",
                            "branch_id",
                            "from_ord",
                            "to_ord",
                            *_INTERVAL_PAYLOAD_COLS,
                        )
                    )
                    selected_payload = ", ".join(
                        f"s.{column}" for column in _INTERVAL_PAYLOAD_COLS
                    )
                    cur.execute(
                        f"""
                        INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
                          ({insert_cols})
                        SELECT s.target_kind, s.target_id, s.namespace,
                               %s, %s, %s, {selected_payload}
                          FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} AS s
                         WHERE s.generation_id = %s AND s.namespace = %s
                        """,
                        (
                            branch_id,
                            ordinal,
                            EFFECTIVE_INTERVAL_OPEN_ORD,
                            generation,
                            namespace,
                        ),
                    )
                    opened = int(cur.rowcount or 0)
                _grant_interval_view_to_mcp(conn)
                interval_result = {
                    "generation_id": generation,
                    "branch_id": branch_id,
                    "ordinal": ordinal,
                    "mode": "latest_front_rebuild",
                    "deleted_current": deleted_current,
                    "closed_older": closed_older,
                    "opened": opened,
                    "rows_written": deleted_current + closed_older + opened,
                    "backfilled": True,
                }
            else:
                interval_result = {
                    "generation_id": generation,
                    "branch_id": branch_id,
                    "ordinal": ordinal,
                    "mode": "already_repaired",
                    "rows_written": 0,
                    "backfilled": True,
                }
            if needs_interval_rebuild:
                (
                    post_snapshot_signature,
                    post_interval_signature,
                ) = _effective_interval_payload_signatures(
                    conn,
                    generation=generation,
                    namespace=namespace,
                    branch_id=branch_id,
                    ordinal=ordinal,
                )
            else:
                post_snapshot_signature = pre_snapshot_signature
                post_interval_signature = pre_interval_signature
            payload_mismatch = (
                post_snapshot_signature != post_interval_signature
            )
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                    "WHERE branch_id = %s AND namespace = %s "
                    "AND from_ord <= %s AND %s < to_ord GROUP BY target_kind",
                    (branch_id, namespace, ordinal, ordinal),
                )
                interval_counts = {str(row[0]): int(row[1]) for row in cur.fetchall()}
            interval_counts = {
                "node": interval_counts.get("node", 0),
                "edge": interval_counts.get("edge", 0),
            }
            if interval_counts != checkpoint_counts or payload_mismatch:
                raise SubstrateError(
                    "effective-attribute interval repair failed parity verification",
                    data={
                        "generation_id": generation,
                        "checkpoint_counts": checkpoint_counts,
                        "snapshot_counts": snapshot_counts,
                        "interval_counts": interval_counts,
                        "payload_mismatch": payload_mismatch,
                    },
                )

        # The finalizer is deliberately after materialization, interval sync,
        # reconciliation, and both retention operations.  Their protected-table
        # triggers advance the mutation epoch; only the epoch observed here may
        # be stamped clean.  A failure before this transaction leaves the state
        # dirty and forces the exact same full fallback on retry.
        with conn.transaction():
            snapshot_pruned = prune_effective_attribute_view_to_recent(
                conn,
                protected_generation_id=generation,
            )
            interval_pruned = prune_effective_interval_view_closed(
                conn, branch=branch_id
            )
            # req.fk-and-gc: the repair maintenance pass also drains orphaned
            # version rows the targeted per-prune GC may have skipped (its
            # candidate set is capped on the publish path). Bounded batches.
            version_gc = prune_orphan_attribute_versions(conn, max_batches=4)
        with conn.transaction():
            integrity_receipt = finalize_full_attribute_integrity_anchor(
                conn,
                deployment_name=deployment_name,
                branch=branch_id,
                namespace=namespace,
                generation_id=generation,
                ordinal=ordinal,
                checkpoint_counts=checkpoint_counts,
            )

        return {
            "ok": True,
            "repaired": bool(rebuilt_snapshot or interval_result.get("rows_written")),
            "generation_id": generation,
            "branch": branch_id,
            "ordinal": ordinal,
            "checkpoint_counts": checkpoint_counts,
            "snapshot_counts": snapshot_counts,
            "interval_counts": interval_counts,
            "payload_match": True,
            "snapshot_rebuilt": rebuilt_snapshot,
            "snapshot_materialization": materialized,
            "interval_reconcile": interval_result,
            "retention": {
                "snapshot_rows_pruned": snapshot_pruned,
                "interval": interval_pruned,
                "version_gc": version_gc,
            },
            "integrity_receipt": integrity_receipt,
        }


#: Columns dropped from every relation fingerprint: bookkeeping that moves
#: without the derived content moving.
_FINGERPRINT_ALWAYS_EXCLUDED = ("materialized_at",)

#: Additionally dropped when fingerprinting the interval front PINNED at one
#: ordinal. ``to_ord`` is a FUTURE-facing column: for a row selected by
#: ``from_ord <= o < to_ord`` its exact value says nothing about the state at
#: ``o`` beyond ``> o``, which the WHERE clause already guarantees. A later
#: generation that supersedes the interval rewrites ``to_ord`` from OPEN to
#: that generation's ordinal -- ordinary forward progress that leaves the
#: pinned-ordinal read byte-identical.
#:
#: What went wrong when it was hashed: a publish cycle that runs the interval
#: sync and then ABORTS before its checkpoint commits leaves the close behind
#: (the sync + ordinal mint share a transaction that commits ahead of
#: ``checkpoint_generation``, publisher.py). The predecessor is still the
#: latest published generation, its pinned-ordinal rows are untouched, and yet
#: the digest had moved -- so the publisher refused a prepared recovery
#: authorization for state that had not changed, permanently. Note this is the
#: aborted-cycle case specifically: a BLOCKED generation does NOT move the
#: front, because the sync is gated on ``blocked_reason is None``
#: (publisher.py, the ``effective_interval_view_sync`` stage).
#:
#: What this DOES and DOES NOT still catch, stated exactly:
#:   * caught -- any content change to a row live at ``o`` (every payload
#:     column stays hashed), any row added to or removed from the pinned
#:     selection, and a ``to_ord`` that retreats to ``<= o`` (the row leaves
#:     the selection, so ``row_count`` moves).
#:   * NOT caught -- a post-prepare truncation of the OPEN front that leaves
#:     the pinned rows selected (closing every open interval at ``o+1``
#:     without reopening). The binding no longer sees that. It is not load
#:     bearing here: a recovery successor is forced full (``_dirty_targets``
#:     is cleared and the publish reanchors via
#:     ``reanchor_effective_interval_view_for_generation``), and
#:     ``finalize_full_attribute_integrity_anchor`` refuses a non-identical
#:     full payload -- so the front is rebuilt from the pinned snapshot rather
#:     than trusted. The integrity receipt provides no cover in this window:
#:     preparation's own writes leave it dirty, so it reads dirty either way.
_INTERVAL_FRONT_FINGERPRINT_EXCLUDED = (*_FINGERPRINT_ALWAYS_EXCLUDED, "to_ord")


def _effective_relation_fingerprint(
    conn: psycopg.Connection,
    *,
    table: str,
    where_sql: str,
    params: tuple[Any, ...],
    exclude_columns: tuple[str, ...] = _FINGERPRINT_ALWAYS_EXCLUDED,
) -> dict[str, Any]:
    """Return a bounded order-independent identity for TOCTOU revalidation."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*), "
            f"COALESCE(sum(hashtextextended("
            f"(to_jsonb(t) - %s::text[])::text, 0)::numeric), 0)::text "
            f"FROM {table} AS t WHERE {where_sql}",
            (list(exclude_columns), *params),
        )
        count, checksum = cur.fetchone()
    raw = f"{int(count)}:{checksum}".encode("utf-8")
    return {
        "row_count": int(count),
        "fingerprint": "sha256:" + hashlib.sha256(raw).hexdigest(),
    }


def classify_effective_attribute_recovery(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    branch: str = "default",
    namespace: str = DEFAULT_NAMESPACE,
) -> dict[str, Any]:
    """Classify read-model recovery without mutating graph or derived rows."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.attribute_integrity import (
        check_attribute_integrity_fast_path,
    )
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name,
        branch=branch,
        status="published",
    )
    if latest is None:
        payload = {
            "schema": "okg.effective_attribute_recovery_classification.v1",
            "classification": "no_published_generation",
            "deployment": deployment_name,
            "branch": branch,
        }
        payload["binding_digest"] = "sha256:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload

    # okg#1114: this classification is the first thing a published database
    # meets on the way to being upgraded -- ``okg up`` runs it in ``preflight``,
    # five phases before ``migrate``. On a database older than the interval
    # narrow split it asked for a column ``migrate`` had not created yet and
    # died, so the check guarding the upgrade required the upgrade to have
    # already happened. There are exactly two honest answers here and "the
    # payload matched" is neither of them: report the SCHEMA as the blocker,
    # with the parity verdict left unset rather than defaulted. ``migrate``
    # clears it, and the re-inspection the lifecycle takes afterwards
    # (``_phase_generation_recovery``) is a real, answerable classification.
    #
    # Deliberately AFTER the no-published-generation branch: the interval
    # relation is created lazily by the first publish, not by the migration
    # schema, so a freshly provisioned deployment legitimately has no interval
    # relation yet and must keep classifying as ``no_published_generation``
    # rather than demanding a migration that would change nothing.
    applicability = _effective_interval_payload_parity_applicability(conn)
    if not applicability["applicable"]:
        payload = {
            "schema": "okg.effective_attribute_recovery_classification.v1",
            "classification": "schema_migration_pending",
            "deployment": deployment_name,
            "branch": branch,
            # NOT False, and above all not True: no comparison was performed.
            "payload_match": None,
            "payload_parity": applicability,
            "fix_hint": (
                "apply migration "
                f"{applicability.get('required_migration')} and re-inspect; "
                "okg up does this in its migrate phase"
            ),
        }
        payload["binding_digest"] = "sha256:" + hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload
    generation = str(latest.checkpoint_id)
    metadata = dict(getattr(latest, "metadata", None) or {})
    try:
        checkpoint_counts = {
            "node": int(metadata["node_count"]),
            "edge": int(metadata["edge_count"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise SubstrateError(
            "published generation has no trustworthy node/edge counts",
            data={"generation_id": generation},
        ) from exc
    session = store.checkout_generation(generation, branch=branch)
    pinned_counts = {
        "node": int(session.query("SELECT count(*) AS n FROM okg.graph_nodes", {})[0]["n"]),
        "edge": int(session.query("SELECT count(*) AS n FROM okg.graph_edges", {})[0]["n"]),
    }
    branch_id, ordinal = resolve_generation_ordinal_row(store.context.db, generation)
    integrity_receipt = check_attribute_integrity_fast_path(
        conn,
        deployment_name=deployment_name,
        branch=branch_id,
        namespace=namespace,
        generation_id=generation,
        ordinal=ordinal,
        checkpoint_counts=checkpoint_counts,
        register_scope=False,
    )
    snapshot_identity = _effective_relation_fingerprint(
        conn,
        table=ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
        where_sql="generation_id = %s AND namespace = %s",
        params=(generation, namespace),
    )
    interval_identity = _effective_relation_fingerprint(
        conn,
        table=ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
        where_sql="branch_id = %s AND namespace = %s AND from_ord <= %s AND %s < to_ord",
        params=(branch_id, namespace, ordinal, ordinal),
        exclude_columns=_INTERVAL_FRONT_FINGERPRINT_EXCLUDED,
    )
    payload_match = _effective_interval_payload_parity(
        conn,
        generation=generation,
        namespace=namespace,
        branch_id=branch_id,
        ordinal=ordinal,
    )
    if checkpoint_counts != pinned_counts:
        classification = "corrupt_state_recovery"
    elif integrity_receipt.get("ok") is True:
        # Interval-mode publishes intentionally retain only changed targets in
        # the generation snapshot.  A clean receipt proves the full interval
        # front has the checkpoint's exact count and payload without requiring
        # the changed-only snapshot itself to be a full copy.
        classification = "no_repair"
    elif (
        snapshot_identity["row_count"] == sum(pinned_counts.values())
        and interval_identity["row_count"] == sum(pinned_counts.values())
        and payload_match
    ):
        classification = "no_repair"
    else:
        classification = "ordinary_repair"
    payload = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": classification,
        "deployment": deployment_name,
        "branch": branch,
        "branch_id": str(branch_id),
        "generation_id": generation,
        "ordinal": ordinal,
        "checkpoint_counts": checkpoint_counts,
        "pinned_counts": pinned_counts,
        "snapshot_identity": snapshot_identity,
        "interval_identity": interval_identity,
        "payload_match": payload_match,
        "integrity_receipt": integrity_receipt,
        "count_stamp_recovery": metadata.get("count_stamp_recovery"),
    }
    payload["binding_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def prepare_corrupt_effective_attribute_recovery(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    expected_binding_digest: str,
    expected_classification: dict[str, Any] | None = None,
    branch: str = "default",
    namespace: str = DEFAULT_NAMESPACE,
    lock_wait_seconds: float = 30.0,
) -> dict[str, Any]:
    """Rebuild derived rows from a mis-stamped predecessor and authorize one successor."""
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.graph_store_internal.generation_ordinal import (
        resolve_generation_ordinal_row,
    )
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_lock_on_connection,
    )

    if not conn.autocommit:
        raise SubstrateError("corrupt-state recovery requires an autocommit connection")
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    with publisher_publish_lock_on_connection(
        conn=conn,
        deployment=deployment_name,
        wait_seconds=lock_wait_seconds,
    ):
        observed = classify_effective_attribute_recovery(
            conn,
            deployment_name=deployment_name,
            branch=branch,
            namespace=namespace,
        )
        expected = expected_classification or observed
        expected_unsigned = dict(expected)
        supplied_digest = str(expected_unsigned.pop("binding_digest", ""))
        computed_digest = "sha256:" + hashlib.sha256(
            json.dumps(expected_unsigned, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if supplied_digest != expected_binding_digest or computed_digest != expected_binding_digest:
            raise SubstrateError("effective-attribute recovery expected binding is invalid")
        if observed.get("binding_digest") == expected_binding_digest:
            with conn.transaction(), conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO okg.effective_attribute_recovery_attempts "
                    "(classification_binding_digest, deployment_name, branch_name, "
                    "predecessor_generation_id, expected_classification, stage) "
                    "VALUES (%s, %s, %s, %s, %s, 'classified') "
                    "ON CONFLICT (classification_binding_digest) DO NOTHING",
                    (
                        expected_binding_digest,
                        deployment_name,
                        branch,
                        str(observed.get("generation_id") or ""),
                        json.dumps(expected),
                    ),
                )
        else:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT deployment_name, branch_name, predecessor_generation_id, "
                    "expected_classification, stage, authorization_payload "
                    "FROM okg.effective_attribute_recovery_attempts "
                    "WHERE classification_binding_digest = %s",
                    (expected_binding_digest,),
                )
                attempt = cur.fetchone()
            if (
                attempt is None
                or str(attempt[0]) != deployment_name
                or str(attempt[1]) != branch
                or str(attempt[2]) != str(observed.get("generation_id") or "")
                or dict(attempt[3]) != expected
            ):
                raise SubstrateError(
                    "effective-attribute recovery classification changed before mutation",
                    data={"expected_binding_digest": expected_binding_digest, "observed": observed},
                )
            if str(attempt[4]) == "prepared" and isinstance(attempt[5], dict):
                return {
                    "ok": True,
                    "prepared": True,
                    "resumed_prepared_state": True,
                    "classification": expected,
                    "repaired_classification": observed,
                    "authorization": dict(attempt[5]),
                }
        if _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT is not None:
            _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT("after_classification")
        if observed.get("classification") != "corrupt_state_recovery":
            return {"ok": True, "prepared": False, "classification": observed}
        generation = str(observed["generation_id"])
        pinned_counts = dict(observed["pinned_counts"])
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = %s",
                (generation, namespace),
            )
        materialized = materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=generation,
            branch=branch,
            previous_generation_id=None,
            namespace=namespace,
            from_checkpoint=True,
            empty_overlay_fast_path=False,
            dirty_targets=None,
        )
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.effective_attribute_recovery_attempts "
                "SET stage = 'snapshot_committed', updated_at = now() "
                "WHERE classification_binding_digest = %s",
                (expected_binding_digest,),
            )
        if _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT is not None:
            _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT("after_snapshot_commit")
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = %s GROUP BY target_kind",
                (generation, namespace),
            )
            snapshot_counts = {str(kind): int(count) for kind, count in cur.fetchall()}
        snapshot_counts = {
            "node": snapshot_counts.get("node", 0),
            "edge": snapshot_counts.get("edge", 0),
        }
        if snapshot_counts != pinned_counts:
            raise SubstrateError(
                "corrupt-state snapshot rebuild does not match pinned graph",
                data={"pinned_counts": pinned_counts, "snapshot_counts": snapshot_counts},
            )
        branch_id, ordinal = resolve_generation_ordinal_row(store.context.db, generation)
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                f"LOCK TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "IN SHARE ROW EXCLUSIVE MODE"
            )
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s AND namespace = %s AND from_ord = %s",
                (branch_id, namespace, ordinal),
            )
            cur.execute(
                f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} SET to_ord = %s, materialized_at = now() "
                "WHERE branch_id = %s AND namespace = %s AND to_ord = %s AND from_ord < %s",
                (ordinal, branch_id, namespace, EFFECTIVE_INTERVAL_OPEN_ORD, ordinal),
            )
            insert_cols = ", ".join((
                "target_kind", "target_id", "namespace", "branch_id", "from_ord", "to_ord",
                *_INTERVAL_PAYLOAD_COLS,
            ))
            selected_payload = ", ".join(f"s.{column}" for column in _INTERVAL_PAYLOAD_COLS)
            cur.execute(
                f"INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} ({insert_cols}) "
                f"SELECT s.target_kind, s.target_id, s.namespace, %s, %s, %s, {selected_payload} "
                f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} AS s "
                "WHERE s.generation_id = %s AND s.namespace = %s",
                (branch_id, ordinal, EFFECTIVE_INTERVAL_OPEN_ORD, generation, namespace),
            )
        _grant_interval_view_to_mcp(conn)
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.effective_attribute_recovery_attempts "
                "SET stage = 'interval_committed', updated_at = now() "
                "WHERE classification_binding_digest = %s",
                (expected_binding_digest,),
            )
        if _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT is not None:
            _EFFECTIVE_ATTRIBUTE_RECOVERY_FAILPOINT("after_interval_commit")
        if not _effective_interval_payload_parity(
            conn,
            generation=generation,
            namespace=namespace,
            branch_id=branch_id,
            ordinal=ordinal,
        ):
            raise SubstrateError("corrupt-state interval rebuild failed payload parity")
        repaired = classify_effective_attribute_recovery(
            conn,
            deployment_name=deployment_name,
            branch=branch,
            namespace=namespace,
        )
        if repaired["snapshot_identity"]["row_count"] != sum(pinned_counts.values()) or repaired[
            "interval_identity"
        ]["row_count"] != sum(pinned_counts.values()):
            raise SubstrateError("corrupt-state recovery failed derived-row count parity")
        authorization = _count_stamp_recovery_authorization(
            deployment_name=deployment_name,
            branch=branch,
            branch_id=str(branch_id),
            generation=generation,
            checkpoint_counts=dict(observed["checkpoint_counts"]),
            pinned_counts=pinned_counts,
            classification_binding_digest=expected_binding_digest,
            prepared_binding_digest=str(repaired["binding_digest"]),
        )
        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                "UPDATE okg.effective_attribute_recovery_attempts "
                "SET stage = 'prepared', authorization_payload = %s, updated_at = now() "
                "WHERE classification_binding_digest = %s",
                (json.dumps(authorization), expected_binding_digest),
            )
        return {
            "ok": True,
            "prepared": True,
            "classification": observed,
            "repaired_classification": repaired,
            "snapshot_materialization": materialized,
            "authorization": authorization,
        }


def _count_stamp_recovery_authorization(
    *,
    deployment_name: str,
    branch: str,
    branch_id: str,
    generation: str,
    checkpoint_counts: dict[str, int],
    pinned_counts: dict[str, int],
    classification_binding_digest: str,
    prepared_binding_digest: str,
) -> dict[str, Any]:
    authorization = {
        "schema": "okg.count_stamp_recovery_authorization.v1",
        "deployment": deployment_name,
        "branch": branch,
        "branch_id": branch_id,
        "predecessor_generation_id": generation,
        "checkpoint_counts": checkpoint_counts,
        "pinned_counts": pinned_counts,
        "classification_binding_digest": classification_binding_digest,
        "prepared_binding_digest": prepared_binding_digest,
    }
    authorization["authorization_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(authorization, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return authorization


def check_effective_attribute_view_integrity(
    conn: psycopg.Connection,
    *,
    deployment_name: str = "default",
    branch: str = "default",
    namespace: str = DEFAULT_NAMESPACE,
    sample: int = 25,
) -> dict[str, Any]:
    """SHOULD-FIX doctor/integrity check: for the latest PUBLISHED generation,
    assert the effective-attrs snapshot is COMPLETE (its row count equals the live
    node+edge count) or cleanly ABSENT (fast path off, not a silent partial), plus
    a sampled ``attrs == raw_attrs || overlay`` spot-check.

    Returns ``{"status": "ok"|"absent"|"degraded", ...}``. ``degraded`` is the
    silent-failure signal (a drifted / partial snapshot): doctor surfaces it so a
    missing-row or stale-merge snapshot is caught LOUDLY rather than serving wrong
    counts. ``absent`` means no snapshot for the generation -> the fast path falls
    back to Layer-1 (correct, not an error)."""
    result: dict[str, Any] = {
        "status": "ok",
        "checks": [],
        "generation_id": None,
    }
    if not _table_exists(conn, ATTRIBUTE_EFFECTIVE_VIEW_TABLE):
        result["status"] = "absent"
        result["reason"] = "view_table_absent"
        return result

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    latest = store.latest_generation(
        deployment_name=deployment_name, branch=branch, status="published"
    )
    if latest is None:
        result["status"] = "absent"
        result["reason"] = "no_published_generation"
        return result
    generation = str(latest.checkpoint_id)
    result["generation_id"] = generation

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND namespace = %s GROUP BY target_kind",
            (generation, namespace),
        )
        snapshot_counts = {row[0]: int(row[1]) for row in cur.fetchall()}
    snapshot_total = sum(snapshot_counts.values())
    if snapshot_total == 0:
        result["status"] = "absent"
        result["reason"] = "no_snapshot_for_generation"
        return result

    # Live node+edge count at the pinned generation.
    session = store.checkout_generation(generation, branch=branch)
    live_nodes = session.query("SELECT count(*) AS n FROM okg.graph_nodes", {})[0]["n"]
    live_edges = session.query("SELECT count(*) AS n FROM okg.graph_edges", {})[0]["n"]
    node_ok = snapshot_counts.get("node", 0) == int(live_nodes)
    edge_ok = snapshot_counts.get("edge", 0) == int(live_edges)
    result["checks"].append(
        {
            "name": "row_count_matches_live",
            "ok": node_ok and edge_ok,
            "snapshot_nodes": snapshot_counts.get("node", 0),
            "snapshot_edges": snapshot_counts.get("edge", 0),
            "live_nodes": int(live_nodes),
            "live_edges": int(live_edges),
        }
    )
    if not (node_ok and edge_ok):
        result["status"] = "degraded"

    # Sampled attrs == raw_base || overlay spot-check (reserved-overlay excluded),
    # computed from the snapshot's version-store payload + the live overlay
    # table (schema-interval-narrow-split: the payload is materialized through
    # the composite-FK join). presence-means-overlay: the raw base is
    # COALESCE(raw_attrs, attrs) (NULL raw_attrs == "raw == attrs"), so a
    # no-overlay row is NOT a false mismatch.
    reserved = sorted(RESERVED_EFFECTIVE_ATTR_KEYS)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH overlay AS (
              SELECT target_kind, target_id,
                     jsonb_object_agg(attr_key, value) AS overlay_attrs
                FROM {ATTRIBUTE_EFFECTIVE_TABLE}
               WHERE generation_id = %(gen)s AND namespace = %(ns)s
                 AND attr_key <> ALL(%(reserved)s::text[])
               GROUP BY target_kind, target_id
            )
            SELECT count(*) AS mismatched
              FROM (
                SELECT v.target_kind, v.target_id
                  FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v
                  {attribute_version_join_sql("v")}
                  LEFT JOIN overlay o
                    ON o.target_kind = v.target_kind
                   AND o.target_id = v.target_id
                 WHERE v.generation_id = %(gen)s AND v.namespace = %(ns)s
                   AND av.attrs IS DISTINCT FROM
                       (COALESCE(av.raw_attrs, av.attrs)
                        || COALESCE(o.overlay_attrs, '{{}}'::jsonb))
                 LIMIT %(sample)s
              ) s
            """,
            {
                "gen": generation,
                "ns": namespace,
                "reserved": reserved,
                "sample": int(sample),
            },
        )
        mismatched = int(cur.fetchone()[0])
    result["checks"].append(
        {
            "name": "sampled_attrs_equal_raw_overlay_merge",
            "ok": mismatched == 0,
            "mismatched_in_sample": mismatched,
        }
    )
    if mismatched != 0:
        result["status"] = "degraded"
    return result
