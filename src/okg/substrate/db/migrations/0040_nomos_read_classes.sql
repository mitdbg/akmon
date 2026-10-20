SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '30min';

-- PACT lane-h-nomos-read-class-sql (Nomos SQL read filter, slice 2): the
-- catalog of stored Nomos read classes.
--
-- Every graph row version carries its read class (okg.graph_nodes /
-- okg.graph_edges .nomos_read_class, slice 1). This table holds, once per
-- class, the canonical facts the class id is the digest of, so a read can
-- decide every class once against the active policy bundle BEFORE its query
-- and push the visible classes into SQL (nomos.reader_view).
--
-- Content-addressed and append-only: the stamping helper
-- (nomos.read_class.stamp_rows) inserts a class the first time a writer
-- stamps it, in the writer's own transaction, and never updates a row. It is
-- not a Chronos table: a class id is the same for every generation that holds
-- a row of it, and a class no longer present costs one extra decision at most.
--
-- Additive and non-destructive: one new table and one role-guarded grant.
-- Rows stamped before this table existed have no catalog row; a read treats
-- them as uncatalogued (the exact per-row path) until a write restamps them.

CREATE TABLE IF NOT EXISTS okg.nomos_read_classes (
  read_class UUID PRIMARY KEY,
  class_facts JSONB NOT NULL,
  vocabulary_version INTEGER NOT NULL,
  first_written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The reader's view reads the catalog on the constrained okg_mcp read pool.
DO $okg_mcp_nomos_read_classes_grants$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    RETURN;
  END IF;
  BEGIN
    GRANT SELECT ON TABLE okg.nomos_read_classes TO okg_mcp;
  EXCEPTION WHEN insufficient_privilege OR undefined_table THEN NULL;
  END;
END
$okg_mcp_nomos_read_classes_grants$;
