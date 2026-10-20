-- Substrate state added by the `okg.deployment` SDK build (Phase 2).
--
-- Per `build-deployment-sdk` Phase 1 §1.1.1 and the spec capability
-- `deployment-sdk`. Adds five tables that back SDK primitives:
--
--   * okg.escalation_facts     — bitemporal fact log for the
--                                escalation lifecycle (mirrors
--                                node_facts / edge_facts pattern).
--   * okg.escalation_live      — current projection of each
--                                escalation (mirrors nodes_live /
--                                edges_live pattern).
--   * okg.deployment_cost_ledger — durable backing store for
--                                `cost_budget.consume()`; primary
--                                key (scope, consumption_id) makes
--                                consume() idempotent under DBOS
--                                step retry per Phase 0 C2 / spec
--                                requirement "consume() is
--                                idempotent under retry".
--   * okg.deployment_cost_caps — per-scope $ caps for `cost_budget`.
--   * okg.deployment_correlations — `correlate_workflow` /
--                                `find_workflow_by_correlation`
--                                lookup index.
--
-- This file is applied by `bootstrap.apply_role_split` AFTER
-- `role_split.sql`. It is idempotent (every table is created with
-- IF NOT EXISTS; every constraint and index uses a guarded DO block
-- or IF NOT EXISTS).
--
-- The grants for these tables are added by `role_split.sql`'s
-- per-deployment-role section, which detects the tables' existence
-- with `IF EXISTS` blocks so the role-split script is safe to apply
-- on both Phase-1 and Phase-2 substrates.


-- =====================================================================
-- 1. okg.escalation_facts — bitemporal fact log for escalations.
--    Mirrors okg.node_facts: BIGSERIAL fact_id + observed_at partition
--    column + dedupe_key unique constraint. Promoted to a Timescale
--    hypertable for consistency with node_facts / edge_facts.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.escalation_facts (
  fact_id           BIGSERIAL,
  -- Logical escalation identifier — stable across the lifecycle
  -- (pending -> routed -> decided -> applied -> timed_out). Each
  -- transition writes a new fact row for the same escalation_id with
  -- the updated attrs.status.
  escalation_id     TEXT       NOT NULL,
  -- The OKG node the escalation is about (the candidate, the
  -- annotation, etc.). The catalog narrowing
  -- `(escalation, escalates, *)` lets escalation-edge writes from
  -- request_escalation reach any subject subtype.
  subject_node_id   TEXT       NOT NULL,
  -- attrs carries (status, requested_by_workflow, reason, view_ref,
  -- decision_schema_ref, timeout_at, decided_by, decided_at,
  -- decision). Schema is enforced by the SDK's `request_escalation`
  -- helper and the substrate's `transition_to_*` functions, not by
  -- this table.
  attrs             JSONB      NOT NULL DEFAULT '{}'::jsonb,
  op                CHAR(1)    NOT NULL DEFAULT 'I'
    CHECK (op IN ('I','R')),
  source            TEXT       NOT NULL,
  observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- See okg.node_facts.dedupe_key comment for cross-time dedup rules.
  dedupe_key        TEXT       NOT NULL,
  run_id            TEXT       NOT NULL,
  -- Primary key on a hypertable MUST include the partition column.
  PRIMARY KEY (fact_id, observed_at),
  UNIQUE (dedupe_key, observed_at)
);

-- Promote to a Timescale hypertable so retention / compression
-- policies apply consistently with node_facts and edge_facts. The
-- create_hypertable call is idempotent under if_not_exists => TRUE.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
  ) THEN
    PERFORM create_hypertable(
      'okg.escalation_facts', 'observed_at',
      chunk_time_interval => INTERVAL '7 days',
      if_not_exists => TRUE,
      migrate_data => TRUE
    );
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS escalation_facts_escalation_id_observed_idx
  ON okg.escalation_facts (escalation_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS escalation_facts_subject_idx
  ON okg.escalation_facts (subject_node_id, observed_at DESC);
CREATE INDEX IF NOT EXISTS escalation_facts_run_id_idx
  ON okg.escalation_facts (run_id, fact_id);


-- =====================================================================
-- 2. okg.escalation_live — bitemporal projection.
--    Mirrors okg.nodes_live: (escalation_id, valid_from_gen) primary
--    key, valid_to_gen close column, interval constraint + GIST
--    exclusion to prevent overlapping windows.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.escalation_live (
  escalation_id     TEXT       NOT NULL,
  subject_node_id   TEXT       NOT NULL,
  attrs             JSONB      NOT NULL DEFAULT '{}'::jsonb,
  valid_from_gen    BIGINT     NOT NULL,
  valid_to_gen      BIGINT,
  last_fact_id      BIGINT,
  PRIMARY KEY (escalation_id, valid_from_gen)
);

DO $escalation_live_bitemporal_contract$
DECLARE
  bad_intervals BIGINT;
  overlapping_ids BIGINT;
BEGIN
  SELECT count(*) INTO bad_intervals
    FROM okg.escalation_live
   WHERE valid_to_gen IS NOT NULL
     AND valid_to_gen <= valid_from_gen;
  IF bad_intervals > 0 THEN
    RAISE EXCEPTION
      'okg.escalation_live has % rows with valid_to_gen <= valid_from_gen; repair before migrating',
      bad_intervals
      USING ERRCODE = 'check_violation';
  END IF;

  SELECT count(*) INTO overlapping_ids
    FROM (
      SELECT DISTINCT a.escalation_id
        FROM okg.escalation_live a
        JOIN okg.escalation_live b
          ON b.escalation_id = a.escalation_id
         AND b.valid_from_gen > a.valid_from_gen
         AND int8range(a.valid_from_gen, a.valid_to_gen, '[)')
             && int8range(b.valid_from_gen, b.valid_to_gen, '[)')
    ) overlap_rows;
  IF overlapping_ids > 0 THEN
    RAISE EXCEPTION
      'okg.escalation_live has % escalation_id values with overlapping generation windows; repair before migrating',
      overlapping_ids
      USING ERRCODE = 'exclusion_violation';
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE connamespace = 'okg'::regnamespace
       AND conrelid = 'okg.escalation_live'::regclass
       AND conname = 'escalation_live_valid_interval_chk'
  ) THEN
    ALTER TABLE okg.escalation_live
      ADD CONSTRAINT escalation_live_valid_interval_chk
      CHECK (valid_to_gen IS NULL OR valid_to_gen > valid_from_gen);
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM pg_constraint
     WHERE connamespace = 'okg'::regnamespace
       AND conrelid = 'okg.escalation_live'::regclass
       AND conname = 'escalation_live_no_validity_overlap_excl'
  ) THEN
    ALTER TABLE okg.escalation_live
      ADD CONSTRAINT escalation_live_no_validity_overlap_excl
      EXCLUDE USING gist (
        escalation_id WITH =,
        (int8range(valid_from_gen, valid_to_gen, '[)')) WITH &&
      );
  END IF;
END
$escalation_live_bitemporal_contract$;

CREATE INDEX IF NOT EXISTS escalation_live_subject_idx
  ON okg.escalation_live (subject_node_id, valid_from_gen);
CREATE INDEX IF NOT EXISTS escalation_live_status_idx
  ON okg.escalation_live ((attrs->>'status'), valid_from_gen)
  WHERE valid_to_gen IS NULL;
CREATE INDEX IF NOT EXISTS escalation_live_current_idx
  ON okg.escalation_live (escalation_id)
  WHERE valid_to_gen IS NULL;
-- The operator response surface (`okg runtime escalation respond`)
-- looks up an escalation by id. The current-row index covers that
-- access pattern without scanning historical rows.


-- =====================================================================
-- 3. okg.deployment_cost_ledger — durable backing store for the
--    `cost_budget` SDK primitive. Primary key (scope, consumption_id)
--    is load-bearing: it is the idempotency boundary for
--    `cost_budget.consume()` under DBOS step retry. The SDK passes
--    the current step's `step_id` as consumption_id; a retried INSERT
--    fails with unique_violation, the cached step output is returned,
--    and consumed totals stay correct.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.deployment_cost_ledger (
  scope             TEXT        NOT NULL,
  -- DBOS step_id; the idempotency boundary for consume(). Two retry
  -- attempts share a step_id, so the second INSERT collides on the
  -- primary key (this is the desired behavior per Phase 0 C2).
  consumption_id    TEXT        NOT NULL,
  usd_consumed      NUMERIC     NOT NULL CHECK (usd_consumed >= 0),
  -- Optional workflow_id for cross-referencing the spending workflow.
  -- Not required; some operator-driven adjustments may not have a
  -- workflow_id (manual cap top-up / refund).
  workflow_id       TEXT,
  observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (scope, consumption_id)
);

CREATE INDEX IF NOT EXISTS deployment_cost_ledger_scope_observed_idx
  ON okg.deployment_cost_ledger (scope, observed_at DESC);
CREATE INDEX IF NOT EXISTS deployment_cost_ledger_workflow_idx
  ON okg.deployment_cost_ledger (workflow_id)
  WHERE workflow_id IS NOT NULL;


-- =====================================================================
-- 4. okg.deployment_cost_caps — per-scope $ cap. UPSERT on entry to a
--    cost_budget context manager. cap_usd may be NULL only when the
--    cap is uncapped (a workflow can declare a scope without a cap
--    for observability; SDK enforces).
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.deployment_cost_caps (
  scope             TEXT        PRIMARY KEY,
  cap_usd           NUMERIC     NOT NULL CHECK (cap_usd >= 0),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- =====================================================================
-- 5. okg.deployment_correlations — `correlate_workflow` /
--    `find_workflow_by_correlation` lookup. Composite primary key on
--    (deployment, topic, correlation_key) so re-registration on
--    workflow recovery is idempotent (UPSERT replaces the
--    workflow_id; the row count stays at 1).
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.deployment_correlations (
  deployment        TEXT        NOT NULL,
  topic             TEXT        NOT NULL,
  correlation_key   TEXT        NOT NULL,
  workflow_id       TEXT        NOT NULL,
  registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment, topic, correlation_key)
);

CREATE INDEX IF NOT EXISTS deployment_correlations_workflow_idx
  ON okg.deployment_correlations (workflow_id);


-- =====================================================================
-- 5b. Extend okg.graph_generations.status CHECK to include
--     'worker_write'. SDK mutation helpers invoked outside a publisher
--     cycle (via worker_write_generation) write a row at this status
--     so the publisher can distinguish its own generations from
--     out-of-cycle writes during recovery.
--
--     Implementation: detect the current CHECK constraint, drop it,
--     and re-add with the expanded literal set. Idempotent.
-- =====================================================================

DO $$
DECLARE
  con_name text;
  con_def text;
  has_status_col boolean;
BEGIN
  -- Defensive: only act if okg.graph_generations actually has a
  -- ``status`` column. Stub-shaped test schemas (and any future
  -- substrate that re-models the table) skip this step rather than
  -- raising an opaque "column does not exist" error.
  SELECT EXISTS (
    SELECT 1 FROM pg_attribute a
      JOIN pg_class c ON c.oid = a.attrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'okg'
       AND c.relname = 'graph_generations'
       AND a.attname = 'status'
       AND NOT a.attisdropped
  ) INTO has_status_col;

  IF NOT has_status_col THEN
    RETURN;
  END IF;

  SELECT c.conname, pg_get_constraintdef(c.oid)
    INTO con_name, con_def
    FROM pg_constraint c
   WHERE c.conrelid = 'okg.graph_generations'::regclass
     AND c.contype = 'c'
     AND pg_get_constraintdef(c.oid) ILIKE '%status%';
  IF con_name IS NOT NULL
     AND con_def NOT ILIKE '%worker_write%' THEN
    EXECUTE format(
      'ALTER TABLE okg.graph_generations DROP CONSTRAINT %I',
      con_name
    );
    ALTER TABLE okg.graph_generations
      ADD CONSTRAINT graph_generations_status_chk
      CHECK (status IN (
        'pending', 'published', 'blocked', 'worker_write'
      ));
  ELSIF con_name IS NULL THEN
    -- No existing status CHECK; add the canonical one.
    ALTER TABLE okg.graph_generations
      ADD CONSTRAINT graph_generations_status_chk
      CHECK (status IN (
        'pending', 'published', 'blocked', 'worker_write'
      ));
  END IF;
END
$$;

-- Helpful index for operator queries that filter to publisher-owned
-- rows. Only created when graph_generations has both the deployment_name
-- and status columns (defensive against stub schemas).
DO $$
DECLARE
  has_cols boolean;
BEGIN
  SELECT (
    EXISTS (SELECT 1 FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname='okg' AND c.relname='graph_generations'
               AND a.attname='status' AND NOT a.attisdropped)
    AND
    EXISTS (SELECT 1 FROM pg_attribute a
              JOIN pg_class c ON c.oid = a.attrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname='okg' AND c.relname='graph_generations'
               AND a.attname='deployment_name' AND NOT a.attisdropped)
  ) INTO has_cols;
  IF has_cols THEN
    CREATE INDEX IF NOT EXISTS graph_generations_publisher_status_idx
      ON okg.graph_generations (deployment_name, generation_id DESC)
      WHERE status <> 'worker_write';
  END IF;
END
$$;


-- =====================================================================
-- 6. Marker: this script applied successfully.
--    Operators can read `okg.escalation_schema_marker` to confirm
--    the Phase 2 extras are present without inspecting catalog rows.
-- =====================================================================

CREATE TABLE IF NOT EXISTS okg.escalation_schema_marker (
  applied_version   TEXT        PRIMARY KEY,
  applied_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO okg.escalation_schema_marker (applied_version)
VALUES ('phase2.1.1')
ON CONFLICT (applied_version) DO UPDATE SET applied_at = now();
