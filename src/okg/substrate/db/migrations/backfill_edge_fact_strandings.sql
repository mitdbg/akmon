-- One-shot backfill: populate `okg.edge_fact_strandings` from every
-- existing op='I' edge_fact whose `edge_id` has no row in
-- `okg.edges_live`. Idempotent via ON CONFLICT.
--
-- The current published generation is used for both
-- `first_seen_gen` and `last_retry_gen` since we don't know the
-- historical first-stranding generation; this is conservative
-- and lets the doctor's "age in generations" computation work
-- from this point forward.

INSERT INTO okg.edge_fact_strandings
  (fact_id, first_seen_gen, last_retry_gen, retry_count, reason)
SELECT ef.fact_id,
       (SELECT max(generation_id) FROM okg.graph_generations
         WHERE status = 'published'),
       (SELECT max(generation_id) FROM okg.graph_generations
         WHERE status = 'published'),
       0,
       'backfill_initial'
  FROM okg.edge_facts ef
 WHERE ef.op = 'I'
   AND NOT EXISTS (
     SELECT 1 FROM okg.edges_live el
      WHERE el.edge_id = ef.edge_id
   )
ON CONFLICT (fact_id) DO NOTHING;
