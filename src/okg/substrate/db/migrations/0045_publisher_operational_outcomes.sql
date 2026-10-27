-- PACT publisher-starvation-4039 (issue #4039): the scheduled publisher joins
-- okg_embed_batch and okg_source_sync as a participant of the typed
-- operational-outcome contract (0029).
--
-- This migration's applied_at is the earliest the publisher's own cutover can
-- be; the cutover is the later of it and the deployment's first registered
-- publisher outcome, so cycles that a worker still on the old code finishes
-- during a rolling upgrade are legacy too. The outcome reader treats a
-- publisher DBOS SUCCESS row that completed before the cutover as legacy,
-- never as workflow_outcome_unknown, because those cycles could not have
-- recorded an outcome. The shared 0029 cutover would have turned every
-- earlier publisher SUCCESS row into unknown evidence.
--
-- It changes no table shape: 0029's tables already accept the publisher's rows
-- (their outcome check allows 'skipped'). The one statement documents the
-- participants on the outcome table, so the migration is a real, replayable
-- schema change and not an empty file.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

COMMENT ON TABLE okg.workflow_operational_outcomes IS
  'Typed outcome of a successful participant workflow: okg_embed_batch, okg_source_sync and (migration 0045) okg_publisher_cycle. A publisher cycle that skipped is outcome skipped with details.reason and details.subject.';
