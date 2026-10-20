-- Edge-fact stranding sentinel + audit trail.
--
-- Per `fix-orphan-edge-projection-stranding`:
-- `project_new_edges` previously dropped op='I' edge_facts whose
-- endpoints were not visible at projection time, silently and
-- with no record. The edge_fact_watermark then advanced past
-- them, stranding the facts forever.
--
-- This table records every such drop. The projector's retry
-- phase consults it at the start of each publish and projects
-- rows whose endpoints are now visible. The doctor surfaces
-- long-stuck rows as integrity warnings.
--
-- `fact_id` is not foreign-keyed against `okg.edge_facts(fact_id)`
-- because that hypertable's PK is composite (fact_id, observed_at)
-- and edge_facts is append-only with no row deletion, so a plain
-- BIGINT is correct.
--
-- Idempotent: re-applying the migration is a no-op.

CREATE TABLE IF NOT EXISTS okg.edge_fact_strandings (
    fact_id        BIGINT  PRIMARY KEY,
    first_seen_gen BIGINT  NOT NULL
                   REFERENCES okg.graph_generations(generation_id),
    last_retry_gen BIGINT  NOT NULL
                   REFERENCES okg.graph_generations(generation_id),
    retry_count    INT     NOT NULL DEFAULT 0,
    reason         TEXT    NOT NULL
                   DEFAULT 'endpoint_invisible_at_projection'
);

CREATE INDEX IF NOT EXISTS edge_fact_strandings_by_first_seen
  ON okg.edge_fact_strandings (first_seen_gen);

CREATE INDEX IF NOT EXISTS edge_fact_strandings_by_last_retry
  ON okg.edge_fact_strandings (last_retry_gen);

COMMENT ON TABLE okg.edge_fact_strandings IS
  'Sentinel for op=I edge_facts dropped by project_new_edges due to endpoint invisibility at projection time. Retried on every subsequent publish; deleted when projection succeeds. See OpenSpec change fix-orphan-edge-projection-stranding.';
