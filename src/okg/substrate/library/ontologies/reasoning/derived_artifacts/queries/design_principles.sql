-- Design-principle entries for the context packet.
--
-- Closure-join on ancestor_name = 'design_principle' so any module
-- that refines DesignPrinciple (no module in this repository does;
-- an external design-knowledge module can) surfaces automatically. When no such module
-- is composed, `subtype_ancestors` has no `design_principle` rows
-- and this query returns zero rows gracefully.
--
-- Topic filter: `attrs->'topic_slugs' ?| %(topic_slugs)s` matches
-- rows whose `topic_slugs` JSONB array overlaps the inputs list.
-- Rows without `topic_slugs` are skipped (a principle without a
-- topic tag can't be retrieved).
--
-- Strength ordering uses the optional `strength` attribute when
-- present; rows without it fall back to 'moderate' so the ORDER
-- BY is total.
--
-- Parameters:
--   generation_id    int     pinned graph generation
--   topic_slugs      [text]  topic slug list to match
--   evidence_budget  int     row limit

SELECT n.node_id,
       n.subtype,
       n.attrs->>'statement'         AS statement,
       n.attrs->>'why_it_matters'    AS why_it_matters,
       n.attrs->'topic_slugs'        AS topic_slugs,
       coalesce(n.attrs->>'strength', 'moderate') AS strength,
       n.attrs->>'observed_at'       AS observed_at,
       n.attrs->'evidence_ids'       AS evidence_ids
  FROM okg.v_nodes n
  JOIN okg.subtype_ancestors sa
    ON sa.catalog_version_id = (
         SELECT catalog_version_id
           FROM okg.graph_generations
          WHERE generation_id = %(generation_id)s)
   AND sa.subtype_name = n.subtype
   AND sa.ancestor_name = 'design_principle'
 WHERE n.attrs ? 'topic_slugs'
   AND (n.attrs->'topic_slugs') ?| %(topic_slugs)s
 ORDER BY CASE coalesce(n.attrs->>'strength', 'moderate')
            WHEN 'high'     THEN 0
            WHEN 'moderate' THEN 1
            WHEN 'low'      THEN 2
            ELSE 3
          END,
          (n.attrs->>'observed_at') DESC NULLS LAST,
          n.node_id
 LIMIT %(evidence_budget)s;
