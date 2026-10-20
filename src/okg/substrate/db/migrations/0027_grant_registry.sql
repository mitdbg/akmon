SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- PACT schema-grant-coherence, req.registry-not-convention + req.scoped-catchup.
--
-- Two tables that make the grant surface data rather than statement order:
--
--   okg.grant_registry — the explicit (role, relation, privileges) allow-list.
--     It replaces the `v\_%` wildcard auto-grant loop that used to hand app_ro
--     SELECT on every view whose NAME matched a pattern, and it replaces the
--     blanket `GRANT SELECT ON ALL TABLES ... TO okg_mcp` catch-up in
--     runtime/sql/role_split.sql. views.sql seeds the substrate rows and both
--     scripts apply the SAME registry, which is what makes them commute.
--
--   okg.pending_grants — the durable retry queue for a grant the Chronos epoch
--     hook could not apply. It replaces a RAISE WARNING that dropped the grant
--     on the floor.
--
-- Ordinal pinned from disk at HEAD: 0026_identity_branch_and_deployment_names
-- is the highest ordinal present, so this is 0027. The runner enumerates the
-- NNNN files that exist and orders them; it does not require a dense sequence,
-- so a concurrently-landing sibling ordinal is not a conflict. Resolve any
-- merge conflict in the pinned lists by TAKING BOTH SIDES and recomputing from
-- the migrations directory; never renumber an ordinal already applied to a
-- database.
--
-- NOT destructive: it creates two tables and drops nothing. The runner derives
-- `destructive` from the filename slug and auto-SKIPS destructive migrations
-- behind an operator-confirmed adapter, which for a pure CREATE would leave
-- upgraded databases without relations a fresh install has.
--
-- Idempotent, and a no-op on a fresh install: schema.sql already authors both
-- tables, so every CREATE here is IF NOT EXISTS against a relation that is
-- already present. The migration exists for databases provisioned before this
-- change, where schema.sql's body is checksum-skipped and would not re-run.

CREATE TABLE IF NOT EXISTS okg.grant_registry (
  entry_ref       TEXT PRIMARY KEY,
  relation        TEXT    NOT NULL,
  object_kind     TEXT    NOT NULL DEFAULT 'TABLE',
  role_name       TEXT,
  privileges      TEXT,
  classification  TEXT    NOT NULL DEFAULT 'contract',
  is_pattern      BOOLEAN NOT NULL DEFAULT FALSE,
  source          TEXT    NOT NULL DEFAULT 'substrate',
  note            TEXT,
  CONSTRAINT grant_registry_object_kind_known
    CHECK (object_kind IN ('TABLE', 'SEQUENCE')),
  CONSTRAINT grant_registry_classification_known
    CHECK (classification IN ('contract', 'internal')),
  CONSTRAINT grant_registry_classification_shape
    CHECK (
      (classification = 'contract'
        AND role_name IS NOT NULL AND privileges IS NOT NULL)
      OR
      (classification = 'internal'
        AND role_name IS NULL AND privileges IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS grant_registry_grant_key
  ON okg.grant_registry (relation, object_kind, role_name)
  WHERE role_name IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS grant_registry_internal_key
  ON okg.grant_registry (relation, object_kind)
  WHERE role_name IS NULL;

CREATE TABLE IF NOT EXISTS okg.pending_grants (
  statement       TEXT PRIMARY KEY,
  first_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts        INTEGER     NOT NULL DEFAULT 1,
  last_error      TEXT
);
