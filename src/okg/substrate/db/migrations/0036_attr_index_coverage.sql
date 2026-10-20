SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30min';

-- Issue #3029: per-attribute coverage markers for okg.attr_index.
--
-- attr_index is re-derived after a publish only for the targets that changed
-- in that publish, so an attribute declared filterable after its data landed
-- (or retyped, or skipped by a snapshot-mode publish) left the index
-- incomplete, and the MCP pushdown -- a hard filter -- silently dropped
-- matching rows. These tables record where the index is known complete; the
-- readers use it only there and otherwise run the exact scan.
--
-- Additive and non-destructive: three new tables, one grant, and a queue
-- insert. A fresh database gets the same tables from the runtime schema ensure
-- (src/okg/substrate/attributes.py replays this DDL), so re-running is a no-op.
--
-- THE MIGRATION QUEUES THE ONE-OFF FULL REBUILD. The coverage table starts
-- empty, so every declared attribute reads "not covered" and every declared-key
-- read scans until its attribute is rebuilt. The INSERT at the end queues one
-- rebuild per (branch, declared attribute) for every branch that has published
-- an ordinal. The runtime publisher cycle drains one request per cycle after
-- its publish commits; `okg db rebuild-attr-index --queued --confirm --apply`
-- drains them on demand, with no publish needed first: when a branch has no
-- recorded derive front yet, the rebuild uses the head ordinal if the head's
-- publish synced the interval front (its checkpoint metadata records
-- effective_interval_view_sync.ran = true). A head that did not sync (snapshot
-- mode, a deferred publish, or one published before that stamp existed) waits
-- for the next interval publish; `okg doctor --check attr-index-coverage`
-- lists it. The front is not seeded here from max(ordinal): every published
-- checkpoint mints an ordinal, including snapshot-mode ones whose interval
-- front never synced.

-- One marker per (branch, target kind, declaring type, attribute).
-- declaration_digest names the declaration the rows were derived under;
-- [covered_from_ordinal, covered_through_ordinal] is the ordinal range the
-- index is complete for. A full rebuild writes [S, S]; a publish advances the
-- upper bound only when the digest matches and the range is contiguous.
CREATE TABLE IF NOT EXISTS okg.attr_index_coverage (
  branch_name TEXT NOT NULL,
  target_kind TEXT NOT NULL
    CHECK (target_kind IN ('node', 'edge')),
  type_name TEXT NOT NULL,
  attr_key TEXT NOT NULL,
  declaration_digest TEXT NOT NULL,
  covered_from_ordinal BIGINT NOT NULL,
  covered_through_ordinal BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT attr_index_coverage_range
    CHECK (covered_from_ordinal <= covered_through_ordinal),
  PRIMARY KEY (branch_name, target_kind, type_name, attr_key)
);

-- Attributes waiting for a bounded background full rebuild.
CREATE TABLE IF NOT EXISTS okg.attr_index_rebuild_requests (
  branch_name TEXT NOT NULL,
  attr_key TEXT NOT NULL,
  reason TEXT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (branch_name, attr_key)
);

-- The ordinal each branch's interval front was last derived at. A rebuild
-- from the interval rows is complete for exactly this ordinal.
CREATE TABLE IF NOT EXISTS okg.attr_index_derive_front (
  branch_name TEXT PRIMARY KEY,
  derived_through_ordinal BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The pushdown consults the markers on the constrained okg_mcp read pool.
DO $okg_mcp_attr_index_coverage_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    RETURN;
  END IF;
  BEGIN
    GRANT SELECT ON TABLE okg.attr_index_coverage TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_attr_index_coverage_grants$;

-- Queue the one-off rebuild: every declared attribute of the latest applied
-- catalog, on every branch that has published an ordinal. Guarded on the
-- ordinal registry, which is created at first publish rather than by the
-- schema snapshot; a database that never published has nothing to rebuild
-- (its first full-scope derivation writes the markers).
DO $attr_index_coverage_queue$
BEGIN
  IF to_regclass('okg.generation_ordinals') IS NULL THEN
    RETURN;
  END IF;
  INSERT INTO okg.attr_index_rebuild_requests (branch_name, attr_key, reason)
  SELECT DISTINCT g.branch_id, d.attr_key, 'migration_0036_attr_index_coverage'
    FROM okg.generation_ordinals g
    CROSS JOIN (
      SELECT DISTINCT jsonb_object_keys(s.metadata -> 'filterable_attrs')
               AS attr_key
        FROM okg.subtypes s
       WHERE s.catalog_version_id = (
               SELECT max(catalog_version_id) FROM okg.catalog_versions
                WHERE status = 'applied')
         AND jsonb_typeof(s.metadata -> 'filterable_attrs') = 'object'
    ) d
  ON CONFLICT (branch_name, attr_key) DO NOTHING;
END
$attr_index_coverage_queue$;
