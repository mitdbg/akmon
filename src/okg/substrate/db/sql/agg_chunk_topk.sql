-- Top-K chunk aggregation pattern.
--
-- Per `improve-mcp-tool-surface` (2026-04-27, task 1.5) — a
-- documented `run_sql` building block, NOT a substrate-installed
-- function. Agents copy this CTE into a `run_sql` call and
-- substitute parameters; operators authoring system prompts
-- can reference it by path.
--
-- Problem solved:
--   BM25 search returns chunks; the agent often wants
--   *documents* ranked by their best chunks. A naive
--   `MAX(score) per doc` rewards docs with one strong hit
--   even when most chunks miss; a SUM-over-everything inflates
--   docs with many weak chunks. The substrate's `search_docs`-
--   style ranking pattern is **sum the top-K chunks per doc**:
--   robust to noise, weighted toward concentrated relevance.
--
-- This file pairs with the `add-doc-aware-search-tools`
-- proposal — that proposal's MCP wrapper (`search_docs`) waits
-- for observation-log evidence that this pattern is used. Until
-- then, agents express it via `run_sql` directly.
--
-- ============================================================
-- Parameters (substitute when pasting into run_sql):
--   :query   — text to search for (BM25 lexical query)
--   :top_k   — chunks per doc to sum (3 is a good default)
--   :limit   — top-K *docs* to return
--   :path_prefix — optional: 'docs/%' to scope to a tree
--                  (use '%' for "any path")
-- ============================================================
--
-- Worked example (substituted, no placeholders):
--
--   WITH chunk_hits AS (
--     -- BM25 runs against okg.v_nodes (which exposes `attrs`) so
--     -- the pg_textsearch operator <@> matches the BM25 index
--     -- `okg.nodes_live_text_bm25_idx` (built on `attrs->>'text'`).
--     -- Querying through okg.v_doc_chunks doesn't work — the BM25
--     -- operator validates the search expression literally matches
--     -- the index column, and view inlining doesn't preserve that.
--     SELECT n.node_id AS chunk_id,
--            (n.attrs->>'text' <@>
--             to_bm25query('publisher', 'okg.nodes_live_text_bm25_idx'))
--               AS score
--       FROM okg.v_nodes n
--      WHERE n.subtype = 'document_chunk'
--   ),
--   chunk_hits_with_doc AS (
--     -- JOIN parent-doc context (path) for filtering + display.
--     SELECT ch.chunk_id, ch.score, c.doc_id, c.path
--       FROM chunk_hits ch
--       JOIN okg.v_doc_chunks c ON c.chunk_id = ch.chunk_id
--      WHERE ch.score < 0           -- BM25 < 0 = match
--        AND c.path LIKE 'docs/%'
--   ),
--   ranked AS (
--     SELECT chunk_id, doc_id, path, score,
--            row_number() OVER (PARTITION BY doc_id ORDER BY score ASC) rn
--       FROM chunk_hits_with_doc
--   )
--   SELECT doc_id,
--          MAX(path) AS path,
--          SUM(score) AS doc_score,
--          COUNT(*) AS hit_chunks_in_topk
--     FROM ranked WHERE rn <= 3
--    GROUP BY doc_id
--    ORDER BY doc_score ASC          -- BM25: more-negative = better
--    LIMIT 10;
--
-- ============================================================
-- Reusable template. Paste into run_sql, substitute :query,
-- :top_k, :limit, :path_prefix with literals.
--
-- Notes on BM25 scoring (`pg_textsearch`):
--   - `<@>` returns floats: more-negative = better match,
--     0 = no match.
--   - `to_bm25query(text, index_name)` constructs the query
--     against the underlying BM25 index
--     `okg.nodes_live_text_bm25_idx` (created in schema.sql).
--   - The BM25 operator validates the search expression
--     against the index column LITERALLY — view inlining is
--     not enough. So this pattern queries the chunk-text via
--     okg.v_nodes (which exposes `attrs`), then JOINs to
--     okg.v_doc_chunks for parent-doc context.
--   - Sort `doc_score ASC` to get best matches first.
-- ============================================================

WITH chunk_hits AS (
  -- Score every document_chunk node against the query. The
  -- BM25 expression `(n.attrs->>'text' <@> to_bm25query(...))`
  -- matches the partial BM25 index on
  -- `(attrs->>'text') WHERE valid_to_gen IS NULL AND attrs ? 'text'`.
  SELECT
    n.node_id AS chunk_id,
    (n.attrs->>'text' <@>
     to_bm25query(:query, 'okg.nodes_live_text_bm25_idx')) AS score
    FROM okg.v_nodes n
   WHERE n.subtype = 'document_chunk'
),
chunk_hits_with_doc AS (
  -- JOIN to v_doc_chunks for the parent-doc path filter +
  -- display. Filter to actual matches (score < 0) here so
  -- non-matching chunks don't waste rows downstream.
  SELECT
    ch.chunk_id,
    ch.score,
    c.doc_id,
    c.path
    FROM chunk_hits ch
    JOIN okg.v_doc_chunks c ON c.chunk_id = ch.chunk_id
   WHERE ch.score < 0
     AND c.path LIKE :path_prefix
),
ranked AS (
  -- Rank chunks within each parent doc (BM25 ASC = best first).
  SELECT
    chunk_id, doc_id, path, score,
    row_number() OVER (PARTITION BY doc_id ORDER BY score ASC) AS rn
    FROM chunk_hits_with_doc
),
doc_scores AS (
  -- Sum the top-K chunks per doc. Robust to noise: a doc with
  -- one stellar hit + many misses gets credit for the hit; a
  -- doc with K medium hits beats a doc with one strong + many
  -- zero. Tie-breakers: more matched chunks wins, then path.
  SELECT
    doc_id,
    MAX(path)  AS path,
    SUM(score) AS doc_score,
    COUNT(*)   AS hit_chunks_in_topk
    FROM ranked
   WHERE rn <= :top_k
   GROUP BY doc_id
)
SELECT
  doc_id,
  path,
  doc_score,
  hit_chunks_in_topk
  FROM doc_scores
 ORDER BY doc_score ASC,
          hit_chunks_in_topk DESC,
          path
 LIMIT :limit;
