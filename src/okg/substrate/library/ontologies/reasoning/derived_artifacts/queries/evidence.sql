-- Evidence Index for the context packet.
--
-- Closure-join on ancestor_name = 'evidence' so every Evidence-
-- refined subtype surfaces uniformly:
--   - GenericEvidence (concrete fallback in reasoning)
--   - scholarly.PaperCitationEvidence (when scholarly composed)
--   - engram_runs.ExperimentResult (when engram_runs composed)
--   - any future domain refinement landing as `is_a: Evidence`
--
-- Topic filter uses an `OR true` fallback so evidence rows
-- without a `topic_slugs` attr still surface (matches the
-- design.md: "Captures all Evidence-refined nodes relevant to
-- the topic" — topic-less evidence is still relevant when the
-- query's topic_slugs are unknown to that subtype).
--
-- Parameters:
--   generation_id    int     pinned graph generation
--   topic_slugs      [text]  topic slug list to match (advisory)
--   evidence_budget  int     row limit

SELECT n.node_id,
       n.subtype,
       coalesce(n.attrs->>'kind',
                n.attrs->>'support_polarity',
                'observation')         AS kind,
       coalesce(n.attrs->>'strength',
                n.attrs->>'weight')    AS strength,
       coalesce(n.attrs->>'statement',
                n.attrs->>'rationale',
                n.attrs->>'text')      AS statement,
       n.attrs->>'observed_at'         AS observed_at,
       n.attrs->'topic_slugs'          AS topic_slugs
  FROM okg.v_nodes n
  JOIN okg.subtype_ancestors sa
    ON sa.catalog_version_id = (
         SELECT catalog_version_id
           FROM okg.graph_generations
          WHERE generation_id = %(generation_id)s)
   AND sa.subtype_name = n.subtype
   AND sa.ancestor_name = 'evidence'
 WHERE (
         NOT (n.attrs ? 'topic_slugs')
      OR ((n.attrs->'topic_slugs') ?| %(topic_slugs)s)
   )
 ORDER BY CASE
            WHEN n.attrs->>'strength' = 'high'     THEN 0
            WHEN n.attrs->>'strength' = 'moderate' THEN 1
            WHEN n.attrs->>'strength' = 'low'      THEN 2
            ELSE 3
          END,
          (n.attrs->>'observed_at') DESC NULLS LAST,
          n.node_id
 LIMIT %(evidence_budget)s;
