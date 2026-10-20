-- Mechanism entries for the context packet's "Mechanisms To Try"
-- section.
--
-- Closure-join on ancestor_name = 'mechanism'. Module-agnostic:
-- any composed module that refines Mechanism (e.g., an external
-- design-knowledge module) surfaces here.
--
-- Parameters mirror design_principles.sql.

SELECT n.node_id,
       n.subtype,
       coalesce(n.attrs->>'name', n.attrs->>'statement') AS name,
       n.attrs->>'applicability'    AS applicability,
       n.attrs->>'known_limits'     AS known_limits,
       n.attrs->'topic_slugs'       AS topic_slugs,
       coalesce(n.attrs->>'strength', 'moderate') AS strength,
       n.attrs->>'observed_at'      AS observed_at,
       n.attrs->'evidence_ids'      AS evidence_ids
  FROM okg.v_nodes n
  JOIN okg.subtype_ancestors sa
    ON sa.catalog_version_id = (
         SELECT catalog_version_id
           FROM okg.graph_generations
          WHERE generation_id = %(generation_id)s)
   AND sa.subtype_name = n.subtype
   AND sa.ancestor_name = 'mechanism'
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
