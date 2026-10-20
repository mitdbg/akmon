-- Add cascade audit trail to nodes_live.
--
-- Per `fix-contains-cascade-on-parent-retraction`:
-- `project_cascade_retractions` synthesizes op='R' node_facts for
-- children whose only live `contains` parent is being retracted in
-- the same cycle. This column records the triggering parent so the
-- substrate's trace, doctor, and rundown queries can attribute the
-- retraction.
--
-- Nullable: only rows whose retraction was cascade-driven carry a
-- non-null value. All other nodes_live rows have it NULL.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS is a no-op on re-application.

ALTER TABLE okg.nodes_live
  ADD COLUMN IF NOT EXISTS cascade_source_node_id TEXT;

CREATE INDEX IF NOT EXISTS nodes_live_cascade_source_idx
  ON okg.nodes_live (cascade_source_node_id)
 WHERE cascade_source_node_id IS NOT NULL;
