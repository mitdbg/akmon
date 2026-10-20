-- Anti-pattern entries for the context packet's "Anti-Patterns"
-- section.
--
-- Closure-join on ancestor_name = 'anti_pattern'. Returns zero
-- rows when no AntiPattern-refined subtype is composed.

SELECT n.node_id,
       n.subtype,
       n.attrs->>'approach'              AS approach,
       n.attrs->>'why_it_tends_to_fail'  AS why_it_tends_to_fail,
       n.attrs->'topic_slugs'            AS topic_slugs,
       coalesce(n.attrs->>'strength', 'moderate') AS strength,
       n.attrs->>'observed_at'           AS observed_at,
       n.attrs->'evidence_ids'           AS evidence_ids
  FROM okg.v_nodes n
  JOIN okg.subtype_ancestors sa
    ON sa.catalog_version_id = (
         SELECT catalog_version_id
           FROM okg.graph_generations
          WHERE generation_id = %(generation_id)s)
   AND sa.subtype_name = n.subtype
   AND sa.ancestor_name = 'anti_pattern'
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
