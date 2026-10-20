-- Jaccard similarity over edge neighborhoods.
--
-- Per `improve-mcp-tool-surface` (2026-04-27, task 1.4) — a
-- documented `run_sql` building block, NOT a substrate-installed
-- function. Agents copy this CTE into a `run_sql` call and
-- substitute parameters; operators authoring system prompts
-- can reference it by path.
--
-- Definition:
--   For two nodes A and B, let N(A) and N(B) be their (un-typed
--   or edge-type-restricted) neighbor sets in the graph at the
--   pinned generation. Jaccard similarity is
--   J(A, B) = |N(A) ∩ N(B)| / |N(A) ∪ N(B)|.
--
-- This file pairs with the `add-jaccard-similarity-tool` proposal
-- — that proposal's MCP wrapper (`similar_nodes(focal_node, ...)`)
-- waits for observation-log evidence that this pattern is used.
-- Until then, agents express Jaccard via `run_sql` directly.
--
-- ============================================================
-- Parameters (substitute when pasting into run_sql):
--   :focal       — node_id whose similar nodes you want
--   :edge_types  — text[] of edge_types to count, or NULL for all
--   :limit       — top-K results
-- ============================================================
--
-- Worked example (substituted, no placeholders):
--
--   WITH focal_neighbors AS (
--     SELECT DISTINCT
--            CASE WHEN src = 'doc:okg:docs/architecture.md'
--                 THEN dst ELSE src END AS neighbor
--       FROM okg.v_edges
--      WHERE (src = 'doc:okg:docs/architecture.md'
--             OR dst = 'doc:okg:docs/architecture.md')
--        AND edge_type = ANY(ARRAY['references']::text[])
--   ),
--   candidate_neighbors AS (
--     SELECT DISTINCT
--            CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
--                 THEN e.dst ELSE e.src END AS candidate,
--            CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
--                 THEN e.src ELSE e.dst END AS shared_neighbor
--       FROM okg.v_edges e
--      WHERE (e.src IN (SELECT neighbor FROM focal_neighbors)
--             OR e.dst IN (SELECT neighbor FROM focal_neighbors))
--        AND edge_type = ANY(ARRAY['references']::text[])
--        AND CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
--                 THEN e.dst ELSE e.src END
--            <> 'doc:okg:docs/architecture.md'
--   ),
--   shared AS (
--     SELECT candidate,
--            count(DISTINCT shared_neighbor) AS shared_count
--       FROM candidate_neighbors
--      GROUP BY candidate
--   ),
--   focal_size AS (
--     SELECT count(*)::float AS n FROM focal_neighbors
--   ),
--   candidate_size AS (
--     SELECT candidate,
--            count(DISTINCT
--                  CASE WHEN src = candidate THEN dst ELSE src END
--            )::float AS n
--       FROM okg.v_edges,
--            (SELECT DISTINCT candidate FROM candidate_neighbors) c
--      WHERE (src = candidate OR dst = candidate)
--        AND edge_type = ANY(ARRAY['references']::text[])
--      GROUP BY candidate
--   )
--   SELECT s.candidate AS node_id,
--          s.shared_count::float
--          / (focal_size.n + cs.n - s.shared_count) AS jaccard
--     FROM shared s
--     JOIN focal_size ON TRUE
--     JOIN candidate_size cs ON cs.candidate = s.candidate
--    WHERE s.shared_count > 0
--    ORDER BY jaccard DESC, s.candidate
--    LIMIT 10;
--
-- ============================================================
-- Reusable template (with named parameters as :focal etc.).
-- Paste this into run_sql, substitute :focal, :edge_types,
-- :limit with literals (run_sql doesn't bind named parameters
-- itself — substitute them in the SQL string).
-- ============================================================

WITH focal_neighbors AS (
  -- Every direct neighbor of the focal node, traversing edges
  -- in either direction. Edge-type filter is optional: pass
  -- NULL for "all edge types" or a text[] for the subset
  -- (e.g. ARRAY['references','member_of']).
  SELECT DISTINCT
         CASE WHEN src = :focal THEN dst ELSE src END AS neighbor
    FROM okg.v_edges
   WHERE (src = :focal OR dst = :focal)
     AND (:edge_types IS NULL
          OR edge_type = ANY(:edge_types::text[]))
),
candidate_neighbors AS (
  -- Every node that shares at least one neighbor with the focal.
  -- We track the shared_neighbor so we can count |N(A) ∩ N(B)|
  -- per candidate. Exclude the focal itself.
  SELECT DISTINCT
         CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
              THEN e.dst ELSE e.src END AS candidate,
         CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
              THEN e.src ELSE e.dst END AS shared_neighbor
    FROM okg.v_edges e
   WHERE (e.src IN (SELECT neighbor FROM focal_neighbors)
          OR e.dst IN (SELECT neighbor FROM focal_neighbors))
     AND (:edge_types IS NULL
          OR edge_type = ANY(:edge_types::text[]))
     AND CASE WHEN e.src IN (SELECT neighbor FROM focal_neighbors)
              THEN e.dst ELSE e.src END <> :focal
),
shared AS (
  -- |N(focal) ∩ N(candidate)| per candidate.
  SELECT candidate,
         count(DISTINCT shared_neighbor) AS shared_count
    FROM candidate_neighbors
   GROUP BY candidate
),
focal_size AS (
  SELECT count(*)::float AS n FROM focal_neighbors
),
candidate_size AS (
  -- |N(candidate)| for each candidate (independent of the focal).
  SELECT c.candidate,
         count(DISTINCT
               CASE WHEN e.src = c.candidate THEN e.dst ELSE e.src END
         )::float AS n
    FROM (SELECT DISTINCT candidate FROM candidate_neighbors) c
    JOIN okg.v_edges e
      ON (e.src = c.candidate OR e.dst = c.candidate)
   WHERE (:edge_types IS NULL
          OR edge_type = ANY(:edge_types::text[]))
   GROUP BY c.candidate
)
SELECT s.candidate AS node_id,
       s.shared_count::float
       / NULLIF(focal_size.n + cs.n - s.shared_count, 0) AS jaccard,
       s.shared_count AS shared_neighbor_count,
       cs.n::int AS candidate_neighbor_count,
       focal_size.n::int AS focal_neighbor_count
  FROM shared s
  JOIN focal_size ON TRUE
  JOIN candidate_size cs ON cs.candidate = s.candidate
 WHERE s.shared_count > 0
 ORDER BY jaccard DESC, shared_neighbor_count DESC, s.candidate
 LIMIT :limit;
