-- clear_tombstone_payloads.sql -- one-off backfill for okg#3284.
--
-- WHAT IT DOES
--   A Chronos delete marker is a physical interval row with deleted = TRUE.
--   Before okg#3284 a marker copied every column of the row it deleted
--   (attrs, embedding, ...). OKG never reads those columns of a marker. This
--   script sets them to NULL on existing markers, keeping the primary key, the
--   interval columns (live_lo, live_hi, writer_segment_id, deleted) and the
--   columns OKG does read:
--     okg.graph_nodes  subtype, source
--     okg.graph_edges  edge_type, src, dst, source
--     any other table  primary key only
--   (the same keep sets as TOMBSTONE_KEEP_COLUMNS in
--   src/okg/substrate/graph_store_internal/schema.py).
--   Live rows (deleted = FALSE) are never touched.
--
-- IRREVERSIBLE. The cleared values are gone; there is no undo. Jason types the
-- go-ahead for each deployment, after reading the step 1 report for it.
--
-- MUTATION TOKENS. A marker whose writer_segment_id is the CURRENT segment of
-- any branch is skipped. The branch mutation token (publisher
-- chronos_graph_branch_mutation_token) hashes exactly those rows, so the
-- backfill changes no token and invalidates no in-flight plan (entity merge,
-- source retirement, liveness, install-stage receipts, incomplete-recovery
-- redrive). Those skipped markers keep their payload until the branch moves
-- to a new segment (the next checkpoint); a later run clears them.
--
-- SCOPE. One run clears every Chronos interval table and every branch in the
-- DATABASE it connects to, so the go-ahead and the quiet point cover every
-- deployment (environment) that shares that database.
--
-- QUIET POINT. Run with no ingest, no publish, no index reclaim and no
-- Chronos checkpoint, fork or merge running in that database.
-- The markers to clear are found once per table with no graph lock held.
-- Each batch then takes, with lock_timeout, the same locks every graph write
-- takes (the okg.chronos_graph advisory lock of every branch, then every
-- branch row FOR UPDATE), so a writer that starts anyway waits for at most
-- one batch, and a batch that cannot get the locks fails instead of waiting.
-- Chronos checkpoint, fork and merge lock branch rows in their own order
-- (_interval_backend.py: _create_checkpoint_unlocked, fast_forward_branch),
-- so running one during the clear can deadlock; PostgreSQL then aborts one.
-- The quiet point excludes them.
--
-- DISK. The UPDATE writes a new, smaller version of each marker and leaves the
-- old version (and its TOAST) dead. The cleared columns are indexed, so the
-- update is never a heap-only (HOT) update: every non-partial index of the
-- table also gets a new entry per cleared marker. It needs WAL and free space
-- for the new versions and index entries BEFORE anything is freed. Check
-- first (step 0):
--   * free disk on the data and WAL volumes (df on the host or pod), against
--     the heap_pages_touched * 8 kB estimate in the step 1 report plus index
--     growth (see the clearing plan for the measured ratio; full-page writes
--     after a checkpoint can make WAL that large);
--   * max_wal_size (step 0 prints it) and the current pg_wal size, read as a
--     superuser: SELECT pg_size_pretty(sum(size)) FROM pg_ls_waldir();
-- After the UPDATE, VACUUM (step 3, required) makes the dead space reusable
-- inside each table. It returns disk to the operating system only for empty
-- pages at the end of a file: on synthetic tables (lab-host, 10-05) it
-- returned 270 of 322 MB when the cleared payloads were large, and nothing
-- when they were small. Do not count on it.
-- Only pg_repack or VACUUM FULL returns disk; that is a SEPARATE step Jason
-- approves on its own (VACUUM FULL takes an ACCESS EXCLUSIVE lock for the
-- whole rewrite and needs free space for a full copy of the table).
-- Step 1 and step 4 report pg_total_relation_size before and after.
--
-- HOW TO RUN (psql, autocommit on, as the table owner):
--   psql "$DSN" -v ON_ERROR_STOP=1 -f scripts/storage/clear_tombstone_payloads.sql
--       prints steps 0 and 1 only (counts and sizes; changes nothing).
--   psql "$DSN" -v ON_ERROR_STOP=1 -v confirm_clear=1 \
--        -f scripts/storage/clear_tombstone_payloads.sql
--       also runs step 2 (the batched UPDATE, one commit per batch), step 3
--       (VACUUM ANALYZE of each interval table) and step 4 (the report again).
--   Re-running is safe: cleared markers no longer match, so a second run
--   updates 0 rows. An interrupted run keeps every committed batch; run it
--   again to finish.
--
-- Statements are separated by "-- >>>" lines (the test splits on them).

-- >>> step 0: preflight
SELECT current_database() AS database,
       pg_size_pretty(pg_database_size(current_database())) AS database_size,
       current_setting('max_wal_size') AS max_wal_size,
       (SELECT count(*) FROM _chronos_branch_interval_branches) AS branches,
       (SELECT array_agg(current_segment_id ORDER BY current_segment_id)
          FROM _chronos_branch_interval_branches) AS current_segments_skipped;

-- >>> helper: keep set per logical table (mirror of TOMBSTONE_KEEP_COLUMNS)
CREATE OR REPLACE FUNCTION pg_temp.okg_tombstone_keep(p_table text)
RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE p_table
    WHEN 'okg.graph_nodes' THEN ARRAY['subtype', 'source']
    WHEN 'okg.graph_edges' THEN ARRAY['edge_type', 'src', 'dst', 'source']
    ELSE ARRAY[]::text[]
  END
$$;

-- >>> helper: the columns a marker of this table drops
CREATE OR REPLACE FUNCTION pg_temp.okg_tombstone_dropped(
  p_table text, p_columns jsonb, p_pk jsonb
)
RETURNS text[]
LANGUAGE sql STABLE AS $$
  SELECT coalesce(array_agg(c ORDER BY ord), ARRAY[]::text[])
    FROM jsonb_array_elements_text(p_columns) WITH ORDINALITY AS x(c, ord)
   WHERE NOT (p_pk ? c)
     AND c <> ALL (pg_temp.okg_tombstone_keep(p_table))
$$;

-- >>> helper: per-table report
CREATE OR REPLACE FUNCTION pg_temp.okg_tombstone_report()
RETURNS TABLE (
  table_name text,
  physical_table text,
  dropped_columns text[],
  markers bigint,
  markers_to_clear bigint,
  markers_skipped_current_segment bigint,
  payload_bytes_to_clear bigint,
  heap_pages_touched bigint,
  total_relation_size bigint
)
LANGUAGE plpgsql AS $$
DECLARE
  r record;
  dropped text[];
  payload text;
  cond text;
BEGIN
  FOR r IN
    SELECT t.table_name, t.physical_table, t.columns::jsonb AS columns,
           t.pk_columns::jsonb AS pk
      FROM _chronos_branch_tables AS t
     WHERE t.backend = 'interval'
     ORDER BY t.table_name
  LOOP
    dropped := pg_temp.okg_tombstone_dropped(r.table_name, r.columns, r.pk);
    table_name := r.table_name;
    physical_table := r.physical_table;
    dropped_columns := dropped;
    total_relation_size := pg_total_relation_size(format('%I', r.physical_table)::regclass);
    IF cardinality(dropped) = 0 THEN
      EXECUTE format('SELECT count(*) FROM %I WHERE deleted', r.physical_table)
         INTO markers;
      markers_to_clear := 0;
      markers_skipped_current_segment := 0;
      payload_bytes_to_clear := 0;
      heap_pages_touched := 0;
      RETURN NEXT;
      CONTINUE;
    END IF;
    payload := (SELECT string_agg(format('coalesce(pg_column_size(p.%I), 0)', c), ' + ')
                  FROM unnest(dropped) AS c);
    cond := (SELECT string_agg(format('p.%I IS NOT NULL', c), ' OR ')
               FROM unnest(dropped) AS c);
    EXECUTE format(
      $q$
      SELECT count(*),
             count(*) FILTER (WHERE has_payload AND NOT in_current),
             count(*) FILTER (WHERE has_payload AND in_current),
             coalesce(sum(payload) FILTER (WHERE has_payload AND NOT in_current), 0),
             count(DISTINCT page) FILTER (WHERE has_payload AND NOT in_current)
        FROM (
          SELECT (%1$s) AS has_payload,
                 (%2$s)::bigint AS payload,
                 (p.ctid::text::point)[0]::bigint AS page,
                 p.writer_segment_id IN (
                   SELECT b.current_segment_id
                     FROM _chronos_branch_interval_branches AS b
                    WHERE b.current_segment_id IS NOT NULL
                 ) AS in_current
            FROM %3$I AS p
           WHERE p.deleted
        ) AS m
      $q$, cond, payload, r.physical_table)
      INTO markers, markers_to_clear, markers_skipped_current_segment,
           payload_bytes_to_clear, heap_pages_touched;
    RETURN NEXT;
  END LOOP;
END
$$;

-- >>> helper: the batched clear (one commit per batch)
-- Per table: (1) find the markers to clear ONCE, with no graph lock held
-- (one read of the table), and keep their keys in a temporary table in
-- physical order; (2) per batch of keys, take the graph write locks, UPDATE
-- those keys by primary key with every condition checked again under the
-- lock (still a marker, still carrying payload, not in any branch's current
-- segment), and commit. The lock is held for one batch of key lookups, not
-- for a scan of the table. A marker written after step (1) is not cleared by
-- this run; a later run clears it.
CREATE OR REPLACE PROCEDURE pg_temp.okg_clear_tombstone_payloads(
  p_batch_size integer DEFAULT 1000,
  p_lock_timeout text DEFAULT '5s'
)
LANGUAGE plpgsql AS $$
DECLARE
  r record;
  dropped text[];
  keys text[];
  set_sql text;
  cond text;
  key_list text;
  key_join text;
  current_segments text := $s$
    SELECT b.current_segment_id
      FROM _chronos_branch_interval_branches AS b
     WHERE b.current_segment_id IS NOT NULL
  $s$;
  candidates bigint;
  lo bigint;
  n bigint;
  total bigint;
  batches integer;
  started timestamptz;
  found_ms double precision;
  locked timestamptz;
  held double precision;
  held_max double precision;
  held_sum double precision;
BEGIN
  IF p_batch_size IS NULL OR p_batch_size < 1 THEN
    RAISE EXCEPTION 'batch size must be positive, got %', p_batch_size;
  END IF;
  FOR r IN
    SELECT t.table_name, t.physical_table, t.columns::jsonb AS columns,
           t.pk_columns::jsonb AS pk
      FROM _chronos_branch_tables AS t
     WHERE t.backend = 'interval'
     ORDER BY t.table_name
  LOOP
    dropped := pg_temp.okg_tombstone_dropped(r.table_name, r.columns, r.pk);
    CONTINUE WHEN cardinality(dropped) = 0;
    -- The physical primary key: the logical key plus live_lo.
    keys := ARRAY(SELECT c FROM jsonb_array_elements_text(r.pk) WITH ORDINALITY AS x(c, ord)
                   ORDER BY ord) || ARRAY['live_lo'];
    set_sql := (SELECT string_agg(format('%I = NULL', c), ', ')
                  FROM unnest(dropped) AS c);
    cond := (SELECT string_agg(format('t.%I IS NOT NULL', c), ' OR ')
               FROM unnest(dropped) AS c);
    key_list := (SELECT string_agg(format('t.%I', c), ', ') FROM unnest(keys) AS c);
    key_join := (SELECT string_agg(format('t.%I = c.%I', c, c), ' AND ')
                   FROM unnest(keys) AS c);

    -- (1) Candidates, found once; no graph lock is held here.
    started := clock_timestamp();
    IF to_regclass('pg_temp.okg_tombstone_candidates') IS NOT NULL THEN
      DROP TABLE pg_temp.okg_tombstone_candidates;  -- left by an interrupted call
    END IF;
    EXECUTE format(
      $q$
      CREATE TEMPORARY TABLE okg_tombstone_candidates AS
      SELECT row_number() OVER (ORDER BY t.ctid) AS seq, %1$s
        FROM %2$I AS t
       WHERE t.deleted
         AND (%3$s)
         AND t.writer_segment_id NOT IN (%4$s)
      $q$, key_list, r.physical_table, cond, current_segments);
    GET DIAGNOSTICS candidates = ROW_COUNT;
    CREATE INDEX ON pg_temp.okg_tombstone_candidates (seq);
    ANALYZE pg_temp.okg_tombstone_candidates;
    COMMIT;
    found_ms := extract(epoch FROM clock_timestamp() - started) * 1000;

    -- (2) One batch of keys per transaction, under the graph write locks.
    total := 0;
    batches := 0;
    held_max := 0;
    held_sum := 0;
    lo := 1;
    WHILE lo <= candidates LOOP
      PERFORM set_config('lock_timeout', p_lock_timeout, true);
      -- The locks every graph write takes, in the writers' order: the
      -- okg.chronos_graph advisory lock per branch, then the branch rows.
      PERFORM pg_advisory_xact_lock(hashtext('okg.chronos_graph'), hashtext(b.branch_id))
         FROM (SELECT branch_id FROM _chronos_branch_interval_branches
                ORDER BY branch_id) AS b;
      PERFORM 1 FROM _chronos_branch_interval_branches ORDER BY branch_id FOR UPDATE;
      locked := clock_timestamp();
      EXECUTE format(
        $q$
        UPDATE %1$I AS t
           SET %2$s
          FROM pg_temp.okg_tombstone_candidates AS c
         WHERE c.seq >= %3$s AND c.seq < %4$s
           AND %5$s
           AND t.deleted
           AND (%6$s)
           AND t.writer_segment_id NOT IN (%7$s)
        $q$, r.physical_table, set_sql, lo, lo + p_batch_size, key_join, cond,
        current_segments);
      GET DIAGNOSTICS n = ROW_COUNT;
      COMMIT;
      held := extract(epoch FROM clock_timestamp() - locked) * 1000;
      held_max := greatest(held_max, held);
      held_sum := held_sum + held;
      total := total + n;
      batches := batches + 1;
      lo := lo + p_batch_size;
    END LOOP;
    DROP TABLE pg_temp.okg_tombstone_candidates;
    COMMIT;
    RAISE NOTICE '% (%): found % markers in % ms (no lock); cleared % in % batches; graph write lock held max % ms, mean % ms',
      r.table_name, r.physical_table, candidates, round(found_ms::numeric), total, batches,
      round(held_max::numeric, 1),
      CASE WHEN batches = 0 THEN 0 ELSE round((held_sum / batches)::numeric, 1) END;
  END LOOP;
END
$$;

-- >>> step 1: report before (changes nothing)
SELECT * FROM pg_temp.okg_tombstone_report();

-- Clear only when confirm_clear is set to a true value (1, true, on, yes).
-- Unset, 0, false, off, no, or anything psql cannot read as a boolean: report
-- only. (A bare \if :{?confirm_clear} would be true for -v confirm_clear=0.)
\set okg_confirm_clear false
\if :{?confirm_clear}
\if :confirm_clear
\set okg_confirm_clear true
\endif
\endif
\if :okg_confirm_clear
-- >>> step 2: clear (IRREVERSIBLE)
CALL pg_temp.okg_clear_tombstone_payloads(1000, '5s');

-- >>> step 3: VACUUM ANALYZE each interval table (required; frees nothing to the OS)
SELECT format('VACUUM (ANALYZE) %I', physical_table)
  FROM _chronos_branch_tables
 WHERE backend = 'interval'
 ORDER BY table_name
\gexec

-- >>> step 4: report after
SELECT * FROM pg_temp.okg_tombstone_report();
\else
\echo 'Report only; nothing was changed. Re-run with -v confirm_clear=1 to clear (irreversible).'
\endif
