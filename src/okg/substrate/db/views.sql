-- Stable agent-facing view layer for `run_sql`.
--
-- THIS FILE IS THE READ PATH. It is not a description of one.
-- Until `schema-canonical-read-path`, the graph-backed half of this file
-- (v_nodes, v_edges and the whole document/chunk/session family) was
-- overwritten at migrate time by SQL generated in Python
-- (db/management.py), so an OSS reader of views.sql learned a read path
-- that served zero rows on every supported deployment. There is no
-- generated stub path any more: what you read here is what gets installed.
--
-- Generation pinning is the Chronos checkout's job, not a predicate in
-- these view bodies. The graph views read okg.graph_nodes /
-- okg.graph_edges, which resolve to the interval rows of whichever
-- checkpoint the caller's checkout pinned. `okg.current_pin()` reports
-- that pin's TEXT checkpoint ref from the session GUC
-- `okg.pin_generation` (D2); it is a session-authority accessor, not a
-- filter these views apply.
--
-- Schema-evolution discipline: the agent-facing contract is the
-- view layer. Physical-schema changes (`graph_nodes` adds a
-- column, `graph_edges` renames one, etc.) absorb at the view
-- definitions; agent SQL doesn't break.
--
-- Editing a view's column list: `CREATE OR REPLACE VIEW` may only
-- APPEND output columns. It cannot rename, reorder, retype, or drop a
-- column that an upgraded database's STORED view already has, and it
-- raises `cannot change name of view column ...` instead — which aborts
-- this whole script and the migrate around it. Appending at the end is
-- always safe; anything else is an upgrade-path change.
--
-- You do not have to hand-write a guard for that. `migrate()` runs this
-- script through `okg.substrate.db.view_convergence`, which drops and
-- recreates the affected views (restrictively — never CASCADE) when and
-- only when PostgreSQL refuses the replace, and covers every view
-- declared below. The `DROP VIEW IF EXISTS` lines that appear above a
-- few views predate that mechanism; they are kept because dropping one
-- known-divergent view up front is cheaper than the recovery pass, not
-- because a new view needs one.
--
-- Per `add-mcp-run-sql-tool` (2026-04-27).

-- ========== app_ro role + pin function ==========

-- Read-only role used by `run_sql` (via `SET LOCAL ROLE`).
-- NOLOGIN — the role cannot be used as a login user; it's
-- inhabited only by `SET LOCAL ROLE` from the substrate's
-- privileged connection.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    CREATE ROLE app_ro NOLOGIN;
  END IF;
END
$$;

-- Make sure subsequent `SET ROLE` from the migration owner works. The MCP
-- login role also gets app_ro below so run_sql can downgrade.
DO $okg_app_ro_owner_grant$
BEGIN
  EXECUTE format('GRANT app_ro TO %I', current_user);
END
$okg_app_ro_owner_grant$;

-- The session GUC the pin accessor consults. `current_setting(name,
-- true)` returns NULL when the GUC is unset, instead of erroring; that
-- gives `okg.current_pin()` a chance to raise a more specific
-- error message.
--
-- Marking the GUC `user-settable` requires `ALTER DATABASE` /
-- `ALTER SYSTEM`. Postgres lets unrecognized custom GUCs be
-- written via `SET LOCAL` without registration as long as the
-- name has a dotted prefix — `okg.pin_generation` qualifies.
-- No registration step is needed.
--
-- `okg.current_pin()` itself is defined FURTHER DOWN, immediately after
-- the last view that used to consult it (`okg.v_aliases`). D2 retypes it
-- from bigint to the TEXT checkpoint ref, and PostgreSQL cannot change a
-- function's return type in place — the retype is a DROP + CREATE, which
-- an upgraded database refuses while a stored view still depends on the
-- old function. Defining it after those views have been replaced is what
-- makes the retype reachable on the upgrade path. See the block below
-- `okg.v_aliases`.

-- ========== Live-node / live-edge views ==========
--
-- The Chronos-native graph read path (`schema-canonical-read-path`;
-- docs/reference/database-schema.md §2.1, rev 4). Chronos-native
-- is the only supported graph storage in v1.0: the legacy
-- `okg.nodes_live` / `okg.edges_live` bitemporal complex these views used
-- to read is deleted from schema.sql, and existing deployments drop it
-- during migrate (`management.retire_legacy_graph_storage`).
--
-- Why the guard arm below exists (fix-nomos-read-path-ux,
-- req.readux.raw-view-honesty, issue #644): on a Chronos-registered graph
-- the logical `okg.graph_nodes` / `okg.graph_edges` tables are schema
-- templates. The rows live in Chronos interval storage
-- (`public._chronos_b_interval_*`) and the logical tables are never
-- materialized in place, so a bare interactive SELECT returns 0 rows with
-- no error and a healthy published graph reads as empty.
-- `okg.require_graph_checkout()` restores the loud contract:
--
--   * RAISES a descriptive error when the consulted logical table is EMPTY
--     while Chronos-registered in `public._chronos_branch_tables` ("never
--     materialized": a bare read is deceptive);
--   * stays silent when the table has rows (someone materialized it) or is
--     NOT Chronos-registered (plain storage: "materialized empty" — an
--     empty result really means an empty graph).
--
-- Checkout-managed readers are unaffected by construction: MCP run_sql maps
-- okg.v_nodes/okg.v_edges to the physical graph tables BY NAME (it never
-- reads these view bodies) and executes through a Chronos checkout. The
-- guard rides a constant UNION ALL arm so it is evaluated even when the
-- base table scan yields zero rows; a WHERE clause on the base table alone
-- would never run on an empty table. Note: queries whose pushed-down
-- predicates constant-fold the guard arm away (e.g. WHERE node_id = 'x')
-- may still return 0 rows silently; bare scans and aggregates — the
-- diagnosis queries — always evaluate the guard.
--
-- EVERY granted view that reads okg.graph_nodes / okg.graph_edges carries
-- this arm. That is the rule, not a property of the two views that
-- happened to get it first: `v_branch_layered_nodes` /
-- `v_branch_layered_edges` were granted (the `okg.v_*` grant sweep near
-- the end of this file grants whatever exists) and read the graph tables
-- bare, so a bare read of THEM returned the deceptive 0 the guard exists
-- to prevent. They are defined below as views over the guarded
-- `okg.v_nodes` / `okg.v_edges`, so the arm cannot be forgotten again.

CREATE OR REPLACE FUNCTION okg.require_graph_checkout(graph_table text)
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog
AS $okg_require_graph_checkout$
DECLARE
  has_rows boolean;
  chronos_registered boolean;
BEGIN
  -- SECURITY DEFINER (least-privilege read roles see okg.v_* only, not the
  -- underlying tables or the Chronos registry), so the probe target is
  -- allow-listed rather than caller-chosen.
  IF graph_table NOT IN ('okg.graph_nodes', 'okg.graph_edges') THEN
    RAISE EXCEPTION
      'okg.require_graph_checkout only guards okg.graph_nodes / okg.graph_edges; got %',
      graph_table;
  END IF;
  IF to_regclass(graph_table) IS NULL THEN
    RETURN false;
  END IF;
  EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', graph_table)
     INTO has_rows;
  IF has_rows THEN
    RETURN false;
  END IF;
  IF to_regclass('public._chronos_branch_tables') IS NULL THEN
    RETURN false;
  END IF;
  SELECT EXISTS (
    SELECT 1 FROM public._chronos_branch_tables t
     WHERE t.table_name = graph_table
  ) INTO chronos_registered;
  IF NOT chronos_registered THEN
    RETURN false;
  END IF;
  RAISE EXCEPTION USING
    MESSAGE = format(
      'okg graph read requires a Chronos checkout: %s is a '
      'Chronos-registered checkout surface that is never materialized for '
      'bare SQL sessions, so this read would report an empty graph even '
      'when published generations exist',
      graph_table
    ),
    HINT = 'Supported read paths: `okg search` / `okg trace` (CLI); the '
           'MCP operators (inspect/search/expand/filter/map/aggregate and '
           'the bounded query tool), which rewrite graph reads to a pinned '
           'Chronos checkout; or ChronosGraphStore.checkout_generation() '
           'in Python. Check row counts with `okg --json status '
           '--deployment <name>`, not bare psql.',
    ERRCODE = 'object_not_in_prerequisite_state';
END
$okg_require_graph_checkout$;

-- `okg.v_nodes` — every node visible at the checkout's pinned
-- generation. The agent gets `node_id, subtype, attrs` directly.
--
-- The retired legacy definition also projected `valid_from_gen` and
-- `source_program_checksum`. Both were promoted to the Chronos path as
-- `NULL::bigint` / `NULL::text` — constant-NULL contract columns that
-- would have frozen at OSS as columns which never carry a value. They are
-- dropped rather than frozen (req.views-real). Nothing read them on the
-- real path: MCP run_sql rewrites okg.v_nodes to okg.graph_nodes by name,
-- and okg.graph_nodes has no such columns, so those selects already
-- failed rather than returning NULL.
CREATE OR REPLACE VIEW okg.v_nodes AS
SELECT
  node_id,
  subtype,
  attrs,
  text_hash
FROM okg.graph_nodes
UNION ALL
SELECT
  NULL::text,
  NULL::text,
  NULL::jsonb,
  NULL::text
WHERE okg.require_graph_checkout('okg.graph_nodes');

-- `okg.v_edges` — every edge visible at the checkout's pinned generation.
CREATE OR REPLACE VIEW okg.v_edges AS
SELECT
  edge_id,
  src,
  dst,
  edge_type,
  attrs
FROM okg.graph_edges
UNION ALL
SELECT
  NULL::bigint,
  NULL::text,
  NULL::text,
  NULL::text,
  NULL::jsonb
WHERE okg.require_graph_checkout('okg.graph_edges');

-- `okg.v_branch_layered_nodes` / `okg.v_branch_layered_edges` —
-- compatibility names for the branch-overlay read surface.
--
-- Branch overlay is no longer a view predicate. Under Chronos, a session
-- pinned to a branch reads through a checkout of that branch's checkpoint,
-- so the layering the legacy `okg.nodes_live` definitions performed with a
-- `okg.pinned_branch_id` GUC and a DISTINCT ON is done by the checkout
-- before these views are consulted. Defining them over `okg.v_nodes` /
-- `okg.v_edges` rather than over the graph tables directly is deliberate:
-- it makes the honest-read guard structural rather than a line someone has
-- to remember to copy.
--
-- The legacy shape also carried `valid_from_gen`, `valid_to_gen`,
-- `last_fact_id` and `branch_id`, promoted to the Chronos path as
-- constant NULLs. Dropped for the same reason as v_nodes' — a frozen
-- column that never carries a value is a contract nobody can honour.
CREATE OR REPLACE VIEW okg.v_branch_layered_nodes AS
SELECT node_id, subtype, attrs, text_hash FROM okg.v_nodes;

CREATE OR REPLACE VIEW okg.v_branch_layered_edges AS
SELECT edge_id, src, dst, edge_type, attrs FROM okg.v_edges;

-- History views (`okg.v_nodes_history`, `okg.v_edges_history`) are RETIRED
-- from the v1.0 surface, and are deliberately absent from this file and
-- from the grant list below.
--
-- Decision D6 (docs/reference/database-schema.md §3.6; operator,
-- 2026-07-31): "retire now; real history views post-OSS". Both views
-- returned LIVE rows with `NULL::bigint` generation bounds — the shape of
-- a bitemporal history view with none of the semantics. Freezing that at
-- OSS would have made fake history a permanent contract, while adding real
-- history views later is additive and post-OSS-safe. `okg.v_changes`
-- (an OpenSpec-change summary that the same decision names) is retired
-- with them.
--
-- The bitemporal-diff recipes in docs/reference/mcp.md that read
-- these views are retired alongside them; historical reads go through
-- `ChronosGraphStore.checkout_generation()` on two checkpoints.

-- `okg.v_generations` — published generation ledger, agent-queryable via
-- run_sql. Per `retire-redundant-mcp-tools` (2026-04-27): the retired
-- `list_generations_since` tool's behavior moves to a recipe over this
-- view.
--
-- D2: `id` is the TEXT checkpoint ref, which is the generation's identity.
-- The published generation ledger is Chronos' checkpoint table, not
-- `okg.graph_generations` — the latter's BIGSERIAL `generation_id` is an
-- ordinal (a position), and ordering generations is an ordinal-domain
-- operation that does not belong in the granted surface (§3.2.1).
--
-- No hand-written `DROP VIEW` above this statement. Reshaping it (the
-- legacy shape typed `id` as bigint and carried two Nomos policy columns)
-- is exactly the frozen-view-shape case `okg.substrate.db.view_convergence`
-- exists to absorb: PostgreSQL refuses the replace, the convergence pass
-- drops the affected views restrictively and re-runs this script.
CREATE OR REPLACE VIEW okg.v_generations AS
SELECT
  checkpoint_id AS id,
  (metadata::jsonb)->>'deployment_name' AS deployment_name,
  (metadata::jsonb)->>'status' AS status,
  created_at::timestamptz AS published_at,
  ((metadata::jsonb)->>'catalog_version_id')::bigint AS catalog_version_id,
  created_at::timestamptz AS created_at,
  COALESCE((metadata::jsonb)->>'correlation_id', checkpoint_id)
    AS correlation_id
FROM public._chronos_branch_interval_checkpoints
WHERE (metadata::jsonb)->>'kind' = 'okg_generation'
  AND (metadata::jsonb)->>'status' = 'published';

-- ========== Convenience views ==========

-- `okg.v_documents` — per-document summary. One row per
-- `document` node with a chunk count derived from the
-- `contains` edges to its `document_chunk` children.
--
-- `byte_count` is extracted with `->>` + NULLIF rather than with the jsonb
-- existence test this view used to run (`CASE WHEN n.attrs ? 'byte_count'
-- THEN ... END`). The rewrite is not cosmetic: MCP `run_sql` inlines a
-- composite okg.v_* view by round-tripping its definition through sqlglot
-- before handing it to the Chronos checkout, and that round-trip re-renders
-- `?` as a text-typed expression, so PostgreSQL rejected the expanded query
-- with "argument of CASE/WHEN must be type boolean". The effect was that
-- okg.v_documents — a granted view — could not be read at all on the
-- production path, which is the same class of defect as the retired stubs
-- this change removes. `->>` returns NULL for an absent key, so the extracted
-- value is unchanged; NULLIF additionally spares an empty string the cast.
-- derivation-read-lint: allow #444 db view document chunk count remains legacy.
CREATE OR REPLACE VIEW okg.v_documents AS
SELECT
  n.node_id        AS doc_id,
  n.attrs->>'path' AS path,
  n.attrs->>'repo' AS repo,
  n.attrs->>'format' AS format,
  NULLIF(n.attrs->>'byte_count', '')::bigint AS byte_count,
  n.attrs->>'content_sha256' AS content_sha256,
  n.attrs->>'title' AS title,
  COALESCE((
    SELECT count(*)::int
    FROM okg.v_edges e
    JOIN okg.v_nodes c
      ON c.node_id = e.dst AND c.subtype = 'document_chunk'
    WHERE e.src = n.node_id AND e.edge_type = 'contains'
  ), 0) AS chunk_count
FROM okg.v_nodes n
WHERE n.subtype = 'document';

-- `okg.v_doc_chunks` — chunks joined to their parent document.
-- One row per `document_chunk` carrying its parent's `path` and
-- the chunk's `heading_path` / `char_offset` / `char_length`
-- for ordered traversal. The join is the implicit "which doc
-- am I in?" lookup an agent typically wants alongside chunk
-- text.
CREATE OR REPLACE VIEW okg.v_doc_chunks AS
SELECT
  c.node_id              AS chunk_id,
  e.dst                  AS doc_id,
  d.attrs->>'path'       AS path,
  d.attrs->>'repo'       AS repo,
  c.attrs->>'text'       AS text,
  COALESCE(e.attrs->>'heading_path', c.attrs->>'heading_path') AS heading_path,
  COALESCE(
    NULLIF(e.attrs->>'char_offset', '')::int,
    NULLIF(c.attrs->>'char_offset', '')::int
  ) AS char_offset,
  COALESCE(
    NULLIF(e.attrs->>'char_length', '')::int,
    NULLIF(c.attrs->>'char_length', '')::int
  ) AS char_length,
  COALESCE(e.attrs->>'chunker_name', c.attrs->>'chunker_name') AS chunker_name,
  c.attrs->>'content_sha256' AS content_sha256,
  c.attrs->>'chunk_kind' AS chunk_kind,
  COALESCE(
    NULLIF(e.attrs->>'char_end', '')::int,
    NULLIF(c.attrs->>'char_end', '')::int
  ) AS char_end,
  COALESCE(e.attrs->>'table_grid_status', c.attrs->>'table_grid_status')
    AS table_grid_status,
  COALESCE(e.attrs->>'table_extraction_lane', c.attrs->>'table_extraction_lane')
    AS table_extraction_lane,
  COALESCE(e.attrs->>'table_grid_error_code', c.attrs->>'table_grid_error_code')
    AS table_grid_error_code,
  -- The occurrence's place in its document's emission order. Edge only: a
  -- chunk node is shared by every document that contains its text, so a
  -- node-level ordinal names just one of them.
  NULLIF(e.attrs->>'chunk_ordinal', '')::int AS chunk_ordinal
FROM okg.v_nodes c
JOIN okg.v_edges e
  ON e.src = c.node_id AND e.edge_type = 'member_of'
JOIN okg.v_nodes d
  ON d.node_id = e.dst AND d.subtype = 'document'
WHERE c.subtype = 'document_chunk';

-- `okg.v_chunk_sequence` — for each chunk, the LEAD/LAG of its
-- siblings in reading order within the parent doc: `chunk_ordinal`
-- (the member_of edge's emission order), then `char_offset`, then
-- `chunk_id`. Offsets alone cannot order a document: a chunk whose
-- producer reports no span stores no offset (graphs parsed before
-- doc-corpus reparse epoch 4 store 0 on every such chunk). Per the
-- design discussion (no prev/next edges), this view gives the agent
-- O(1) prev/next lookup through `run_sql` without polluting the graph
-- with edges.
CREATE OR REPLACE VIEW okg.v_chunk_sequence AS
SELECT
  chunk_id,
  doc_id,
  path,
  char_offset,
  char_length,
  heading_path,
  lag(chunk_id)  OVER w AS prev_chunk_id,
  lead(chunk_id) OVER w AS next_chunk_id,
  row_number()   OVER w AS chunk_index,
  count(*)       OVER (PARTITION BY doc_id) AS doc_chunk_total,
  chunk_kind,
  char_end
FROM okg.v_doc_chunks
WINDOW w AS (
  PARTITION BY doc_id ORDER BY chunk_ordinal NULLS LAST, char_offset, chunk_id
);

-- `okg.v_table_cells` — generation-pinned anchor cells projected on demand
-- from validated TableGridV1 payloads. Cells remain coordinates within their
-- table occurrence; they are not materialized as graph nodes.
CREATE OR REPLACE VIEW okg.v_table_cells AS
SELECT
  c.node_id || ':' || (row_item.value->>'row_index') || ':' ||
    (cell_item.value->>'column_index') AS cell_key,
  c.node_id AS table_occurrence_id,
  e.dst AS doc_id,
  d.attrs->>'path' AS path,
  d.attrs->>'repo' AS repo,
  COALESCE(e.attrs->>'table_extraction_lane', c.attrs->>'table_extraction_lane')
    AS table_extraction_lane,
  (row_item.value->>'row_index')::int AS row_index,
  row_item.value->>'row_label' AS row_label,
  (cell_item.value->>'column_index')::int AS column_index,
  cell_item.value->>'column_header' AS column_header,
  cell_item.value->>'text' AS cell_text,
  (cell_item.value->>'row_span')::int AS row_span,
  (cell_item.value->>'column_span')::int AS column_span,
  COALESCE(
    NULLIF(e.attrs->>'char_offset', '')::int,
    NULLIF(c.attrs->>'char_offset', '')::int
  ) AS source_char_offset,
  COALESCE(
    NULLIF(e.attrs->>'char_end', '')::int,
    NULLIF(c.attrs->>'char_end', '')::int
  ) AS source_char_end,
  COALESCE(
    NULLIF(e.attrs->>'source_page_index', '')::int,
    NULLIF(c.attrs->>'source_page_index', '')::int
  ) AS source_page_index,
  COALESCE(e.attrs->'source_bbox', c.attrs->'source_bbox') AS source_bbox,
  COALESCE(
    e.attrs->>'source_content_sha256',
    c.attrs->>'source_content_sha256'
  ) AS source_content_sha256,
  COALESCE(e.attrs->>'cache_signature', c.attrs->>'cache_signature')
    AS cache_signature
FROM okg.v_nodes c
JOIN okg.v_edges e
  ON e.src = c.node_id AND e.edge_type = 'member_of'
JOIN okg.v_nodes d
  ON d.node_id = e.dst AND d.subtype = 'document'
CROSS JOIN LATERAL jsonb_array_elements(c.attrs->'table_grid'->'rows')
  AS row_item(value)
CROSS JOIN LATERAL jsonb_array_elements(row_item.value->'cells')
  AS cell_item(value)
WHERE c.subtype = 'document_chunk'
  AND c.attrs->>'chunk_kind' = 'table'
  AND c.attrs->>'table_grid_status' = 'parsed'
ORDER BY e.dst, c.node_id,
  (row_item.value->>'row_index')::int,
  (cell_item.value->>'column_index')::int;

-- `okg.v_changes` is RETIRED from the v1.0 surface (decision D6,
-- docs/reference/database-schema.md §3.6; operator, 2026-07-31),
-- together with `okg.v_nodes_history` / `okg.v_edges_history`. It
-- summarised `open_spec_change` nodes for an OpenSpec workflow that this
-- repo has itself retired in favour of PACT, and it joined its child
-- counts by string-matching `attrs->>'change_id'` rather than by graph
-- edges — a shape nobody should inherit at OSS. Change summaries are read
-- through the MCP operators over `okg.v_nodes`, which carry the same rows
-- without the frozen convention.

-- `okg.v_subtypes` — per-subtype live-row counts at the checkout's pin,
-- joined to the catalog's per-subtype metadata (annotations like
-- `okg_searchable_text_fields` and `archetype`).
--
-- The catalog side resolves to the latest APPLIED catalog version rather
-- than to the pinned generation's own `catalog_version_id`: the pin is a
-- Chronos checkpoint ref (D2), and the generation→catalog join it used to
-- make went through the retired `okg.graph_generations.generation_id`
-- ordinal. Checkpoint-pinned catalog resolution is enforced in the
-- MCP/session layer, which is the only caller that knows the pin.
-- derivation-read-lint: allow #444 db view subtype summary remains legacy.
CREATE OR REPLACE VIEW okg.v_subtypes AS
SELECT
  n.subtype,
  count(*)::int AS live_count,
  s.metadata    AS catalog_metadata
FROM okg.v_nodes n
LEFT JOIN okg.subtypes s
  ON s.name = n.subtype
 AND s.catalog_version_id = (
       SELECT max(catalog_version_id)
       FROM okg.catalog_versions
       WHERE status = 'applied'
     )
GROUP BY n.subtype, s.metadata;

-- `okg.v_aliases` — active aliases at the latest applied catalog version.
-- Same reasoning as `okg.v_subtypes` above.
CREATE OR REPLACE VIEW okg.v_aliases AS
SELECT
  a.alias,
  a.canonical,
  a.scope
FROM okg.aliases a
WHERE a.catalog_version_id = (
  SELECT max(catalog_version_id)
  FROM okg.catalog_versions
  WHERE status = 'applied'
);

-- ========== The session pin accessor ==========
--
-- `okg.current_pin()` reports the generation the caller's session is
-- pinned to, as the TEXT Chronos checkpoint ref (D2,
-- docs/reference/database-schema.md §3.2: "the TEXT checkpoint ref
-- is the sole generation identity in every granted relation, view,
-- function, and API"). It is EXECUTE-granted to the read roles, which puts
-- it in the freeze surface (§5), which is why its bigint return was a
-- registered identity divergence owned by this change.
--
-- Defined HERE rather than at the top of the file, deliberately. Retyping
-- a function's return is a DROP + CREATE — `CREATE OR REPLACE FUNCTION`
-- refuses it — and an upgraded database refuses the DROP while a stored
-- view still depends on the old function. `okg.v_subtypes` and
-- `okg.v_aliases` (directly above) and `okg.v_nodes` / `okg.v_edges` were
-- those dependants; by this point in the script every one of them has been
-- replaced with a body that does not call the pin at all, so the drop is
-- reachable. Moving this block back to the top would wedge every upgrade.
--
-- The drop is conditional on the return type actually differing, so a
-- converged database takes no DDL here. If a dependant this repo does not
-- own (an operator's own view) still holds the old function, the DROP
-- raises `DependentObjectsStillExist` naming it — loudly, which is the
-- intended failure mode; there is no silent skip.
--
-- SCOPE, stated because half a convention is worse than none. This function
-- now REPORTS whatever the GUC holds, as text — a faithful accessor. What
-- WRITES the GUC has not been converged: `run_sql`'s operational path still
-- derives a legacy integer (`_legacy_pin_for_operational_sql`), and the SDK's
-- `pinned_generation()` still parses the GUC with `int()`. Converging the
-- writers is deliberately NOT done here, because that helper falls back to
-- the latest published generation when the parse fails — so writing a TEXT
-- ref today would make a pinned reader silently read an unpinned generation,
-- which is exactly the silent-fallback class this work exists to remove. The
-- writers and that fallback move together, in their own change.
DO $okg_current_pin_retype$
DECLARE
  pin_fn regprocedure;
BEGIN
  -- Nested IF, not `A IS NOT NULL AND f(A)`: plpgsql hands the whole boolean
  -- to SQL as one expression and does not promise to short-circuit, so the
  -- second operand's `::regprocedure` cast raises `undefined_function` on a
  -- fresh database where the function does not exist yet.
  pin_fn := to_regprocedure('okg.current_pin()');
  IF pin_fn IS NOT NULL THEN
    IF pg_get_function_result(pin_fn) <> 'text' THEN
      DROP FUNCTION okg.current_pin();
    END IF;
  END IF;
END
$okg_current_pin_retype$;

CREATE OR REPLACE FUNCTION okg.current_pin()
RETURNS text
LANGUAGE plpgsql STABLE PARALLEL SAFE
AS $$
DECLARE
  pin_text text;
BEGIN
  pin_text := current_setting('okg.pin_generation', true);
  IF pin_text IS NULL OR pin_text = '' THEN
    RAISE EXCEPTION 'okg.pin_generation GUC not set'
      USING HINT = 'SET LOCAL okg.pin_generation = ''<generation_ref>'' '
                 || 'before calling okg.current_pin()',
            ERRCODE = 'undefined_parameter';
  END IF;
  RETURN pin_text;
END
$$;

-- `okg.v_source_run_diagnostics` — source-progress backed run
-- telemetry. SourceRunner writes one `sync.stage_timings.run.<run_id>`
-- row per source run; this view promotes that compact JSON payload into
-- a queryable read-side surface without granting app_ro access to the
-- raw key/value table.
CREATE OR REPLACE VIEW okg.v_source_run_diagnostics AS
WITH progress_runs AS (
  SELECT
    p.deployment,
    p.source_name AS progress_source_name,
    p.key,
    p.value::jsonb AS payload,
    p.updated_at
  FROM okg.source_progress p
  WHERE p.key LIKE 'sync.stage_timings.run.%'
),
decoded AS (
  SELECT
    deployment,
    COALESCE(payload->>'source_name', progress_source_name) AS source_name,
    COALESCE(
      payload->>'registry_source_name',
      CASE
        WHEN progress_source_name LIKE deployment || '.%'
        THEN substring(progress_source_name FROM length(deployment) + 2)
        ELSE progress_source_name
      END
    ) AS registry_source_name,
    COALESCE(
      NULLIF(payload->>'run_id', ''),
      substring(key FROM 'sync\.stage_timings\.run\.(.*)$')
    ) AS run_id,
    payload->>'status' AS status,
    payload->>'run_mode' AS run_mode,
    COALESCE(payload->>'timing_status', 'unavailable') AS timing_status,
    payload->>'timing_unavailable_reason' AS timing_unavailable_reason,
    updated_at AS observed_at,
    CASE WHEN payload->>'input_count' ~ '^-?[0-9]+$'
      THEN (payload->>'input_count')::bigint END AS input_count,
    CASE WHEN payload->>'output_count' ~ '^-?[0-9]+$'
      THEN (payload->>'output_count')::bigint END AS output_count,
    CASE WHEN payload->>'nodes_appended' ~ '^-?[0-9]+$'
      THEN (payload->>'nodes_appended')::bigint ELSE 0 END AS nodes_appended,
    CASE WHEN payload->>'edges_appended' ~ '^-?[0-9]+$'
      THEN (payload->>'edges_appended')::bigint ELSE 0 END AS edges_appended,
    CASE WHEN payload->>'nodes_retracted' ~ '^-?[0-9]+$'
      THEN (payload->>'nodes_retracted')::bigint ELSE 0 END AS node_retractions,
    CASE WHEN payload->>'edges_retracted' ~ '^-?[0-9]+$'
      THEN (payload->>'edges_retracted')::bigint ELSE 0 END AS edge_retractions,
    COALESCE(payload->'timings_ms', '{}'::jsonb) AS timings_ms,
    COALESCE(payload->'sync_scope', '{}'::jsonb) AS sync_scope,
    COALESCE(payload->'source_stats', '{}'::jsonb) AS source_stats
  FROM progress_runs
)
SELECT
  d.deployment,
  d.source_name,
  d.registry_source_name,
  d.run_id,
  d.status,
  d.run_mode,
  d.timing_status,
  CASE
    WHEN d.timing_status = 'available' THEN d.timing_unavailable_reason
    ELSE COALESCE(
      d.timing_unavailable_reason,
      'stage_timing_payload_unavailable'
    )
  END AS timing_unavailable_reason,
  d.observed_at,
  d.observed_at AS started_at,
  d.observed_at AS finished_at,
  d.input_count,
  d.output_count,
  d.nodes_appended,
  d.edges_appended,
  d.node_retractions,
  d.edge_retractions,
  CASE WHEN d.timings_ms->>'total' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'total')::double precision END AS total_ms,
  CASE WHEN d.timings_ms->>'source_runner' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner')::double precision END AS source_runner_ms,
  CASE WHEN d.timings_ms->>'source_runner.adapter_run' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.adapter_run')::double precision END AS adapter_run_ms,
  CASE WHEN d.timings_ms->>'source_runner.flush.dedupe_persisted' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.flush.dedupe_persisted')::double precision END AS dedupe_persisted_ms,
  CASE WHEN d.timings_ms->>'source_runner.flush.chronos_apply' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.flush.chronos_apply')::double precision END AS chronos_apply_ms,
  CASE WHEN d.timings_ms->>'source_runner.flush_chunks' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.flush_chunks')::double precision END AS flush_chunks_ms,
  CASE WHEN d.timings_ms->>'source_runner.flush_final' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.flush_final')::double precision END AS flush_final_ms,
  CASE WHEN d.timings_ms->>'source_runner.stream_walk' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.stream_walk')::double precision END AS stream_walk_ms,
  CASE WHEN d.timings_ms->>'source_runner.record_set_chronos_verify' ~ '^-?[0-9]+(\.[0-9]+)?$'
    THEN (d.timings_ms->>'source_runner.record_set_chronos_verify')::double precision END AS record_set_chronos_verify_ms,
  slowest.phase AS slowest_phase,
  slowest.ms AS slowest_phase_ms,
  d.timings_ms,
  d.source_stats,
  d.sync_scope
FROM decoded d
-- `slowest_phase` phase vocabulary (issue #1325 item 4).
--
-- `timings_ms` is a MIXED tree, not a partition. `_run_source`
-- (src/okg/substrate/ingest/service.py) stamps a whole-call rollup under
-- `source_runner` and then re-publishes every SourceRunner label beneath it as
-- `source_runner.<label>`. Ranking the raw map therefore always returned
-- `source_runner` -- for a 0.3 s no-op and a 937 s bulk load alike -- because a
-- rollup is by construction >= each of its parts. The column named the same
-- phase for all 30 production sources and discriminated nothing.
--
-- The ranking now runs over TERMINAL phases only, defined by three rules:
--
--   1. Named aggregates are excluded. These are wall-clock wrappers whose time
--      is already re-reported by finer keys, so ranking them hides the phase
--      that actually costs:
--        total, source_runner, source_runner.total  -- whole-run rollups
--        source_runner.stream_walk                  -- wraps flush_chunks
--        source_runner.flush_chunks                 -- wraps flush.*
--        source_runner.flush_final                  -- wraps flush.*
--        source_runner.stream_footprint_tail_flush  -- wraps flush.*
--        source_runner.flush                        -- defensive: the
--                                                   -- parent form of
--                                                   -- flush.*, which
--                                                   -- is not emitted
--   2. Keys deeper than three dot-separated segments are excluded. Depth 3 is
--      the phase floor (`source_runner.flush.chronos_apply`); anything below it
--      is a Chronos backend sub-measurement stamped by
--      `add_chronos_backend_timings`, and those are NOT a partition of their
--      phase, so ranking them against a sibling phase compares unlike things.
--   3. Gauges are excluded. `timings_ms` is keyed by name only, so a key
--      ending in a count / rows / bytes / size segment is a magnitude, not a
--      duration, and would rank as a colossal number of milliseconds. The
--      producer filters these today (`_timing_detail_is_duration`,
--      src/okg/substrate/ingest/runner.py), so this arm is a floor rather
--      than the first line of defence -- but removing the rollups makes a
--      stray gauge reachable, and a gauge that outranks a phase is exactly
--      the kind of confident wrong answer this column just stopped giving.
--      It mirrors NON_DURATION_PHASE_SUFFIXES / _NAMES / _NAMESPACES in
--      src/okg/bench/extract.py, which publishes the same column from the
--      same payload and must not disagree with this view.
--
-- What remains is the vocabulary the operator can act on -- adapter_run,
-- flush.chronos_apply, flush.dedupe_persisted, stream_footprint_*,
-- record_set_*, ensure_checkout, change_probe, advance_watermark, and any
-- future SourceRunner label, which is terminal by default.
--
-- A run record carrying no terminal phase (rollups only) yields NULL rather
-- than a rollup name: absence of a breakdown must read as absence, not as an
-- answer.
LEFT JOIN LATERAL (
  SELECT kv.key AS phase, kv.value::double precision AS ms
  FROM jsonb_each_text(d.timings_ms) AS kv(key, value)
  WHERE kv.value ~ '^-?[0-9]+(\.[0-9]+)?$'
    AND kv.key NOT IN (
      -- Both spellings of each rollup. Every payload this view reads is
      -- written prefixed by `_run_source`; the bare forms are here so this
      -- set is literally the one src/okg/bench/extract.py excludes, and so
      -- an unprefixed runner map could never fail open.
      'total',
      'source_runner',
      'flush',
      'flush_chunks',
      'flush_final',
      'stream_footprint_tail_flush',
      'stream_walk',
      'source_runner.total',
      'source_runner.flush',
      'source_runner.flush_chunks',
      'source_runner.flush_final',
      'source_runner.stream_footprint_tail_flush',
      'source_runner.stream_walk'
    )
    AND array_length(string_to_array(kv.key, '.'), 1) <= 3
    AND kv.key !~ ('(^|[._])(count|count_max|row_count|rows|rows_read'
                || '|rows_written|bytes|bytes_read|bytes_written|size'
                || '|batch_size|marker_batch_size|payload_bytes|payload_size'
                || '|max_payload_bytes|max_payload_size|operation_count'
                || '|operations)$')
    AND kv.key !~ '(^|\.)(node_change_class|node_change_fields)(\.|$)'
  ORDER BY kv.value::double precision DESC, kv.key
  LIMIT 1
  ) slowest ON true;

-- `okg.v_source_frontier_state` — source-sync frontier and read-visibility
-- state reconstructed from the normalized operational tables. DBOS workflow
-- metadata is optional in clean/migrated databases, so the DBOS slice lives
-- behind a SECURITY DEFINER function that returns no rows when the DBOS schema
-- is absent instead of making the view fail to create.
CREATE OR REPLACE FUNCTION okg.source_frontier_active_workflows()
RETURNS TABLE (
  deployment text,
  source_name text,
  registry_source_name text,
  workflow_id text,
  workflow_status text,
  run_mode text,
  trigger_kind text,
  active_work_observed_at timestamptz,
  active_work_age_seconds double precision,
  active_work_stale boolean,
  priority integer,
  deduplication_id text,
  attribution_state text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $$
DECLARE
  created_type text;
  updated_type text;
  created_expr text;
  updated_expr text;
  priority_expr text;
BEGIN
  IF to_regclass('dbos.workflow_status') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name IN (
         'workflow_uuid',
         'status',
         'name',
         'queue_name',
         'deduplication_id'
       )
     GROUP BY table_schema, table_name
    HAVING count(*) = 5
  ) THEN
    RETURN;
  END IF;

  SELECT data_type INTO created_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'created_at';
  SELECT data_type INTO updated_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'updated_at';

  created_expr := CASE
    WHEN created_type IN (
      'bigint',
      'integer',
      'numeric',
      'double precision',
      'real'
    ) THEN 'to_timestamp(created_at::double precision / 1000.0)'
    WHEN created_type IS NULL THEN 'NULL::timestamptz'
    ELSE 'created_at::timestamptz'
  END;
  updated_expr := CASE
    WHEN updated_type IN (
      'bigint',
      'integer',
      'numeric',
      'double precision',
      'real'
    ) THEN 'to_timestamp(updated_at::double precision / 1000.0)'
    WHEN updated_type IS NULL THEN 'NULL::timestamptz'
    ELSE 'updated_at::timestamptz'
  END;
  priority_expr := CASE
    WHEN EXISTS (
      SELECT 1
        FROM information_schema.columns
       WHERE table_schema = 'dbos'
         AND table_name = 'workflow_status'
         AND column_name = 'priority'
    ) THEN 'priority::integer'
    ELSE 'NULL::integer'
  END;

  RETURN QUERY EXECUTE format($sql$
    WITH raw AS (
      SELECT
        workflow_uuid::text AS workflow_id,
        upper(status::text) AS workflow_status,
        coalesce(%2$s, %1$s) AS observed_at,
        %3$s AS priority,
        queue_name::text AS queue_name,
        deduplication_id::text AS deduplication_id
      FROM dbos.workflow_status
      WHERE name = 'okg_source_sync'
        AND upper(status::text) IN ('ENQUEUED', 'PENDING', 'DELAYED')
    ),
    decoded AS (
      SELECT
        coalesce(
          nullif(split_part(deduplication_id, ':', 2), ''),
          nullif(
            regexp_replace(
              regexp_replace(queue_name, '^okg-', ''),
              '-source-sync$',
              ''
            ),
            ''
          )
        ) AS deployment,
        CASE
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5)
                NOT IN ('', '_poll_batch')
          THEN split_part(deduplication_id, ':', 5)
          ELSE NULL
        END AS registry_source_name,
        workflow_id,
        workflow_status,
        CASE WHEN deduplication_id LIKE 'sync:%%'
          THEN nullif(split_part(deduplication_id, ':', 4), '')
        END AS run_mode,
        CASE WHEN deduplication_id LIKE 'sync:%%'
          THEN nullif(split_part(deduplication_id, ':', 3), '')
        END AS trigger_kind,
        observed_at,
        priority,
        deduplication_id,
        CASE
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5)
                NOT IN ('', '_poll_batch')
          THEN 'deduplication_id'
          WHEN deduplication_id LIKE 'sync:%%'
            AND split_part(deduplication_id, ':', 5) = '_poll_batch'
          THEN 'deployment_batch'
          ELSE 'deployment_only'
        END AS attribution_state
      FROM raw
    )
    SELECT
      d.deployment,
      CASE WHEN d.registry_source_name IS NULL THEN NULL
        ELSE d.deployment || '.' || d.registry_source_name
      END AS source_name,
      d.registry_source_name,
      d.workflow_id,
      d.workflow_status,
      d.run_mode,
      d.trigger_kind,
      d.observed_at AS active_work_observed_at,
      CASE WHEN d.observed_at IS NULL THEN NULL
        ELSE extract(epoch FROM (now() - d.observed_at))::double precision
      END AS active_work_age_seconds,
      CASE WHEN d.observed_at IS NULL THEN NULL
        ELSE d.observed_at < now() - interval '10 minutes'
      END AS active_work_stale,
      d.priority,
      d.deduplication_id,
      d.attribution_state
    FROM decoded d
    WHERE d.deployment <> ''
  $sql$, created_expr, updated_expr, priority_expr);
END
$$;

-- `okg.source_frontier_unpublished_writes()` — which sources have graph
-- writes that the latest published generation of their deployment does not
-- contain (issue #3024).
--
-- `okg.source_watermarks.last_published_rev` cannot answer this: no publish
-- writes it, so a healthy published source always read as unpublished. The
-- published checkpoint can. A Chronos checkpoint cuts the branch's segment
-- into a read-only snapshot and a mutable continuation, so every graph row
-- version written after the checkpoint is visible at the branch head and not
-- at the checkpoint. A retraction writes such a version too (a tombstone),
-- so removals count as well as additions and updates.
--
-- One row per deployment with a published generation: the generation it
-- judged against, whether the Chronos state it needs was present
-- (`evaluated`), and the sources with row versions visible at the branch
-- head but not at that generation. `evaluated` is false when the
-- checkpoint's segment or its branch head is gone; the caller must not read
-- that as "nothing unpublished". A deployment whose graph tables are not
-- registered yet has written no graph rows, so it is evaluated with no
-- unpublished sources. The segment-id bound only narrows the scan to the
-- `(writer_segment_id, pk)` index: segment ids come from one sequence, and
-- every segment that can hold a write made after the checkpoint was
-- allocated when or after the checkpoint split its parent. The head
-- condition, not the segment bound, excludes rows written on a branch forked
-- from the checkpoint; the "not visible at the checkpoint" condition is a
-- cheap guard that keeps the rule correct without relying on the layout.
--
-- `blocked_generation_id` / `blocked_generation_status` name the
-- deployment's newest `okg_generation` checkpoint when it is newer than the
-- published one and its status is blocked, rejected, failed or quarantined.
-- A blocked publish cuts such a checkpoint and leaves the published one in
-- place, so the next publish will not take the unpublished work until a
-- person clears the cause (issue #3024).
--
-- The function scans every deployment and every branch's graph writes made
-- since each deployment's published checkpoint, once per call.
DROP VIEW IF EXISTS okg.v_source_frontier_state;
DROP FUNCTION IF EXISTS okg.source_frontier_state();
DROP FUNCTION IF EXISTS okg.source_frontier_unpublished_writes();
CREATE OR REPLACE FUNCTION okg.source_frontier_unpublished_writes()
RETURNS TABLE (
  deployment text,
  generation_id text,
  evaluated boolean,
  unpublished_sources text[],
  blocked_generation_id text,
  blocked_generation_status text
)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $$
#variable_conflict use_column
DECLARE
  cp record;
  physical text;
  found_sources text[];
  table_sources text[];
BEGIN
  IF to_regclass('public._chronos_branch_interval_checkpoints') IS NULL
     OR to_regclass('public._chronos_branch_interval_segments') IS NULL
     OR to_regclass('public._chronos_branch_interval_branches') IS NULL THEN
    RETURN;
  END IF;

  FOR cp IN
    WITH latest AS (
      SELECT DISTINCT ON ((c.metadata::jsonb)->>'deployment_name')
        (c.metadata::jsonb)->>'deployment_name' AS deployment,
        c.checkpoint_id,
        c.branch_id,
        c.segment_id
      FROM public._chronos_branch_interval_checkpoints c
      WHERE (c.metadata::jsonb)->>'kind' = 'okg_generation'
        AND (c.metadata::jsonb)->>'status' = 'published'
      ORDER BY (c.metadata::jsonb)->>'deployment_name',
               c.created_at::timestamptz DESC NULLS LAST,
               c.checkpoint_id DESC
    ),
    newest AS (
      SELECT DISTINCT ON ((c.metadata::jsonb)->>'deployment_name')
        (c.metadata::jsonb)->>'deployment_name' AS deployment,
        c.checkpoint_id,
        lower((c.metadata::jsonb)->>'status') AS status
      FROM public._chronos_branch_interval_checkpoints c
      WHERE (c.metadata::jsonb)->>'kind' = 'okg_generation'
      ORDER BY (c.metadata::jsonb)->>'deployment_name',
               c.created_at::timestamptz DESC NULLS LAST,
               c.checkpoint_id DESC
    )
    SELECT
      l.deployment,
      l.checkpoint_id,
      ps.parent_segment_id AS published_parent_segment_id,
      ps.branch_point AS published_point,
      hs.branch_point AS head_point,
      CASE
        WHEN nw.checkpoint_id <> l.checkpoint_id
         AND nw.status IN ('blocked', 'rejected', 'failed', 'quarantined')
          THEN nw.checkpoint_id
      END AS blocked_checkpoint_id,
      CASE
        WHEN nw.checkpoint_id <> l.checkpoint_id
         AND nw.status IN ('blocked', 'rejected', 'failed', 'quarantined')
          THEN nw.status
      END AS blocked_status
    FROM latest l
    LEFT JOIN newest nw
      ON nw.deployment = l.deployment
    LEFT JOIN public._chronos_branch_interval_segments ps
      ON ps.segment_id = l.segment_id
    LEFT JOIN public._chronos_branch_interval_branches b
      ON b.branch_id = l.branch_id
    LEFT JOIN public._chronos_branch_interval_segments hs
      ON hs.segment_id = b.current_segment_id
  LOOP
    deployment := cp.deployment;
    generation_id := cp.checkpoint_id;
    blocked_generation_id := cp.blocked_checkpoint_id;
    blocked_generation_status := cp.blocked_status;
    IF cp.published_point IS NULL OR cp.head_point IS NULL THEN
      evaluated := false;
      unpublished_sources := NULL;
      RETURN NEXT;
      CONTINUE;
    END IF;
    found_sources := ARRAY[]::text[];
    IF to_regclass('public._chronos_branch_tables') IS NOT NULL THEN
      FOR physical IN
        SELECT DISTINCT t.physical_table
          FROM public._chronos_branch_tables t
         WHERE t.table_name IN ('okg.graph_nodes', 'okg.graph_edges')
           AND to_regclass(format('public.%I', t.physical_table)) IS NOT NULL
         ORDER BY t.physical_table
      LOOP
        EXECUTE format(
          'SELECT coalesce(array_agg(DISTINCT r.source), ARRAY[]::text[])'
          || '  FROM public.%I r'
          || ' WHERE r.source IS NOT NULL'
          || '   AND r.writer_segment_id > coalesce($1, -1)'
          || '   AND r.live_lo <= $2 AND $2 < r.live_hi'
          || '   AND NOT (r.live_lo <= $3 AND $3 < r.live_hi)',
          physical
        )
        INTO table_sources
        USING cp.published_parent_segment_id,
              cp.head_point,
              cp.published_point;
        found_sources := found_sources || table_sources;
      END LOOP;
    END IF;
    evaluated := true;
    SELECT coalesce(array_agg(DISTINCT s ORDER BY s), ARRAY[]::text[])
      INTO unpublished_sources
      FROM unnest(found_sources) AS s;
    RETURN NEXT;
  END LOOP;
END
$$;

DROP VIEW IF EXISTS okg.v_source_frontier_state;
DROP FUNCTION IF EXISTS okg.source_frontier_state();

CREATE OR REPLACE FUNCTION okg.source_frontier_state()
RETURNS TABLE (
  deployment text,
  source_name text,
  registry_source_name text,
  branch_name text,
  latest_published_generation_id text,
  latest_published_at timestamptz,
  watermark_source text,
  last_reconcile_at timestamptz,
  last_reconcile_age_seconds double precision,
  projection_cursor bigint,
  progress_observed_at timestamptz,
  last_run_observed_at timestamptz,
  last_run_id text,
  last_run_status text,
  last_run_mode text,
  last_run_completed_scope boolean,
  last_error text,
  nodes_appended bigint,
  edges_appended bigint,
  record_set_state text,
  record_set_sha256 text,
  record_set_record_count bigint,
  record_set_node_count bigint,
  record_set_edge_count bigint,
  cursor_blob_count integer,
  cursor_blob_bytes bigint,
  active_workflow_id text,
  workflow_status text,
  workflow_run_mode text,
  active_work_stale boolean,
  active_work_age_seconds double precision,
  runtime_state text,
  read_visibility_state text,
  frontier_state text,
  last_attempt_run_id text,
  last_attempt_status text,
  last_attempt_observed_at timestamptz
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, okg
AS $$
WITH progress_seen AS (
  SELECT
    p.deployment,
    p.source_name,
    CASE
      WHEN p.source_name LIKE p.deployment || '.%'
      THEN substring(p.source_name FROM length(p.deployment) + 2)
      ELSE p.source_name
    END AS registry_source_name,
    max(p.updated_at) AS progress_observed_at
  FROM okg.source_progress p
  GROUP BY p.deployment, p.source_name
),
latest_runs AS (
  SELECT DISTINCT ON (p.deployment, p.source_name)
    p.deployment,
    p.source_name,
    p.key,
    p.value::jsonb AS payload,
    p.updated_at AS last_run_observed_at
  FROM okg.source_progress p
  WHERE (
      p.key = 'sync.last_run'
      OR p.key LIKE 'sync.run.%'
    )
    AND p.value LIKE '{%'
  ORDER BY
    p.deployment,
    p.source_name,
    CASE WHEN p.key = 'sync.last_run' THEN 0 ELSE 1 END,
    p.updated_at DESC,
    p.key DESC
),
-- The newest finished run of each source, failed or not (issue #3024).
-- `sync.last_run` above is written only when a run succeeds; the ingest
-- service writes `sync.record_set_coverage` after every finished source job
-- with that job's status (ingest/service.py
-- `_upsert_source_record_set_coverage_state`), so it is the only progress
-- row that records a failed run.
latest_attempts AS (
  SELECT
    p.deployment,
    p.source_name,
    p.value::jsonb AS payload,
    p.updated_at AS attempt_observed_at
  FROM okg.source_progress p
  WHERE p.key = 'sync.record_set_coverage'
    AND p.value LIKE '{%'
),
watermark_rows AS (
  SELECT
    w.source,
    CASE
      WHEN w.source LIKE '%.%' THEN split_part(w.source, '.', 1)
      ELSE NULL
    END AS deployment_guess,
    CASE
      WHEN w.source LIKE '%.%'
      THEN substring(w.source FROM position('.' in w.source) + 1)
      ELSE w.source
    END AS registry_source_name,
    w.cutover_watermark,
    w.last_applied_rev,
    w.projection_cursor,
    w.last_reconcile_at,
    w.replay_required
  FROM okg.source_watermarks w
),
all_active_work AS (
  SELECT *
  FROM okg.source_frontier_active_workflows()
),
active_work_summary AS (
  SELECT
    deployment,
    count(*) FILTER (WHERE registry_source_name IS NULL)::int
      AS unattributed_active_count,
    count(*) FILTER (
      WHERE registry_source_name IS NULL AND active_work_stale
    )::int AS unattributed_stale_count
  FROM all_active_work
  GROUP BY deployment
),
ranked_active_work AS (
  SELECT
    aw.*,
    row_number() OVER (
      PARTITION BY aw.deployment, aw.registry_source_name
      ORDER BY
        CASE
          WHEN aw.run_mode IN (
            'release_unchanged',
            'release_new',
            'scope_complete',
            'reconcile'
          ) AND NOT coalesce(aw.active_work_stale, false) THEN 0
          WHEN aw.run_mode IN (
            'release_unchanged',
            'release_new',
            'scope_complete',
            'reconcile'
          ) THEN 1
          WHEN NOT coalesce(aw.active_work_stale, false) THEN 2
          ELSE 3
        END,
        CASE aw.workflow_status
          WHEN 'PENDING' THEN 0
          WHEN 'ENQUEUED' THEN 1
          WHEN 'DELAYED' THEN 2
          ELSE 9
        END,
        coalesce(nullif(aw.priority, 0), 100),
        coalesce(aw.active_work_age_seconds, 1.0e18),
        aw.workflow_id
    ) AS active_rank
  FROM all_active_work aw
  WHERE aw.registry_source_name IS NOT NULL
),
active_work AS (
  SELECT *
  FROM ranked_active_work
  WHERE active_rank = 1
),
source_keys AS (
  SELECT
    ps.deployment,
    ps.source_name,
    ps.registry_source_name
  FROM progress_seen ps
  UNION
  SELECT
    wr.deployment_guess AS deployment,
    wr.source AS source_name,
    wr.registry_source_name
  FROM watermark_rows wr
  UNION
  SELECT
    aw.deployment,
    aw.source_name,
    aw.registry_source_name
  FROM active_work aw
),
latest_published AS (
  SELECT DISTINCT ON (g.deployment_name)
    g.deployment_name AS deployment,
    g.id::text AS generation_id,
    g.published_at
  FROM okg.v_generations g
  ORDER BY g.deployment_name, g.published_at DESC NULLS LAST, g.id::text DESC
),
unpublished_writes AS (
  SELECT *
  FROM okg.source_frontier_unpublished_writes()
),
cursor_blobs AS (
  SELECT
    cb.source,
    count(*)::int AS cursor_blob_count,
    sum(cb.size_bytes)::bigint AS cursor_blob_bytes
  FROM okg.source_cursor_blobs cb
  GROUP BY cb.source
)
SELECT
  coalesce(sk.deployment, ps.deployment, aw.deployment) AS deployment,
  coalesce(ps.source_name, wr.source, aw.source_name, sk.source_name)
    AS source_name,
  coalesce(ps.registry_source_name, wr.registry_source_name,
           aw.registry_source_name, sk.registry_source_name)
    AS registry_source_name,
  'default'::text AS branch_name,
  lp.generation_id AS latest_published_generation_id,
  lp.published_at AS latest_published_at,
  wr.source AS watermark_source,
  wr.last_reconcile_at,
  CASE WHEN wr.last_reconcile_at IS NULL THEN NULL
    ELSE extract(epoch FROM (now() - wr.last_reconcile_at))::double precision
  END AS last_reconcile_age_seconds,
  wr.projection_cursor,
  ps.progress_observed_at,
  lr.last_run_observed_at,
  coalesce(
    nullif(lr.payload->>'run_id', ''),
    substring(lr.key FROM 'sync\.run\.(.*)$')
  ) AS last_run_id,
  nullif(coalesce(lr.payload->>'status', lr.payload->>'result'), '')
    AS last_run_status,
  nullif(coalesce(lr.payload->>'run_mode', lr.payload->>'mode'), '')
    AS last_run_mode,
  CASE
    WHEN lr.payload ? 'completed_scope'
    THEN (lr.payload->>'completed_scope')::boolean
    WHEN coalesce(lr.payload->>'run_mode', lr.payload->>'mode')
      IN ('release_unchanged', 'release_new', 'scope_complete', 'reconcile')
    THEN true
    WHEN lr.payload IS NULL THEN NULL
    ELSE false
  END AS last_run_completed_scope,
  nullif(coalesce(
    lr.payload->>'last_error',
    lr.payload->>'error',
    lr.payload->>'failure'
  ), '') AS last_error,
  CASE WHEN lr.payload->>'nodes_appended' ~ '^-?[0-9]+$'
    THEN (lr.payload->>'nodes_appended')::bigint END AS nodes_appended,
  CASE WHEN lr.payload->>'edges_appended' ~ '^-?[0-9]+$'
    THEN (lr.payload->>'edges_appended')::bigint END AS edges_appended,
  CASE
    WHEN wr.last_applied_rev ? 'record_set_ref' AND srb.source IS NULL
      THEN 'external_blob_missing'
    WHEN wr.last_applied_rev ? 'record_set_ref'
      THEN 'external_blob'
    WHEN wr.last_applied_rev ? 'record_set'
      THEN 'inline_cursor'
    WHEN srs.source IS NOT NULL
      THEN 'hot_row'
    WHEN wr.last_applied_rev ? 'record_set_summary'
      THEN 'summary_only'
    ELSE 'unavailable'
  END AS record_set_state,
  wr.last_applied_rev->'record_set_ref'->>'sha256' AS record_set_sha256,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'record_count',
      srb.record_set_summary->>'record_count',
      srs.record_set_summary->>'record_count'
    ),
    ''
  )::bigint AS record_set_record_count,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'node_count',
      srb.record_set_summary->>'node_count',
      srs.record_set_summary->>'node_count'
    ),
    ''
  )::bigint AS record_set_node_count,
  NULLIF(
    coalesce(
      wr.last_applied_rev->'record_set_summary'->>'edge_count',
      srb.record_set_summary->>'edge_count',
      srs.record_set_summary->>'edge_count'
    ),
    ''
  )::bigint AS record_set_edge_count,
  cb.cursor_blob_count,
  cb.cursor_blob_bytes,
  aw.workflow_id AS active_workflow_id,
  aw.workflow_status,
  aw.run_mode AS workflow_run_mode,
  aw.active_work_stale,
  aw.active_work_age_seconds,
  CASE
    WHEN aw.workflow_id IS NOT NULL AND aw.active_work_stale THEN 'stale'
    WHEN aw.workflow_id IS NOT NULL THEN 'active'
    WHEN coalesce(aws.unattributed_stale_count, 0) > 0
      THEN 'stale_unattributed'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0
      THEN 'active_unattributed'
    WHEN aw.attribution_state IS NULL THEN 'not_observed'
    ELSE aw.attribution_state
  END AS runtime_state,
  rv.read_visibility_state,
  CASE
    WHEN nullif(coalesce(
      lr.payload->>'last_error',
      lr.payload->>'error',
      lr.payload->>'failure'
    ), '') IS NOT NULL THEN 'blocked'
    WHEN coalesce(wr.replay_required, '{}'::jsonb) <> '{}'::jsonb
      THEN 'blocked'
    WHEN aw.workflow_id IS NOT NULL AND aw.active_work_stale THEN 'blocked'
    WHEN coalesce(aws.unattributed_stale_count, 0) > 0 THEN 'blocked'
    WHEN aw.workflow_id IS NOT NULL THEN 'replaying'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0 THEN 'replaying'
    WHEN coalesce(last_attempt.failed, false) THEN 'blocked'
    WHEN rv.read_visibility_state = 'published_state_unavailable'
      THEN 'blocked'
    WHEN rv.read_visibility_state = 'publish_blocked' THEN 'blocked'
    -- Applied work the published generation does not hold yet: an ingest
    -- with publish off, an embedding write or a lineage repair. The next
    -- publish takes it; nobody has to act (issue #3024).
    WHEN rv.read_visibility_state = 'unpublished_writes'
      THEN 'publish_pending'
    WHEN rv.read_visibility_state = 'read_visible'
      AND wr.last_reconcile_at IS NOT NULL THEN 'fresh'
    WHEN rv.read_visibility_state = 'read_visible' THEN 'read_visible'
    WHEN wr.source IS NULL AND ps.source_name IS NULL THEN 'unknown'
    ELSE 'partial'
  END AS frontier_state,
  -- The newest finished run, from `sync.record_set_coverage`. The
  -- `last_run_*` columns describe the newest SUCCESSFUL run; when the newest
  -- run failed these name it, so a reader never sees only a successful run id
  -- beside last_run_failed.
  nullif(la.payload->>'run_id', '') AS last_attempt_run_id,
  nullif(la.payload->>'status', '') AS last_attempt_status,
  la.attempt_observed_at AS last_attempt_observed_at
FROM source_keys sk
LEFT JOIN progress_seen ps
  ON ps.deployment IS NOT DISTINCT FROM sk.deployment
 AND ps.registry_source_name = sk.registry_source_name
LEFT JOIN latest_runs lr
  ON lr.deployment IS NOT DISTINCT FROM ps.deployment
 AND lr.source_name = ps.source_name
LEFT JOIN latest_attempts la
  ON la.deployment IS NOT DISTINCT FROM ps.deployment
 AND la.source_name = ps.source_name
LEFT JOIN watermark_rows wr
  ON (
       wr.deployment_guess IS NOT DISTINCT FROM sk.deployment
       AND wr.registry_source_name = sk.registry_source_name
     )
  OR (
       wr.deployment_guess IS NULL
       AND wr.registry_source_name = sk.registry_source_name
     )
LEFT JOIN active_work aw
  ON aw.deployment IS NOT DISTINCT FROM coalesce(sk.deployment, ps.deployment)
 AND aw.registry_source_name = sk.registry_source_name
LEFT JOIN active_work_summary aws
  ON aws.deployment IS NOT DISTINCT FROM coalesce(
       sk.deployment,
       ps.deployment,
       wr.deployment_guess,
       aw.deployment
     )
LEFT JOIN latest_published lp
  ON lp.deployment IS NOT DISTINCT FROM coalesce(
       sk.deployment,
       ps.deployment,
       wr.deployment_guess,
       aw.deployment
     )
LEFT JOIN unpublished_writes uw
  ON uw.deployment = lp.deployment
 AND uw.generation_id = lp.generation_id
LEFT JOIN okg.source_record_sets srs
  ON srs.source = wr.source
LEFT JOIN okg.source_record_set_blobs srb
  ON srb.source = wr.source
 AND srb.sha256 = wr.last_applied_rev->'record_set_ref'->>'sha256'
LEFT JOIN cursor_blobs cb
  ON cb.source = wr.source
-- The newest finished run failed: its coverage row says so and no successful
-- `sync.last_run` was written after it. The ordering guard keeps a stale
-- failure from outliving a newer success recorded by a path that does not
-- refresh the coverage row.
LEFT JOIN LATERAL (
  SELECT (
    nullif(la.payload->>'status', '') IS NOT NULL
    AND upper(la.payload->>'status') <> 'OK'
    AND (
      lr.last_run_observed_at IS NULL
      OR la.attempt_observed_at >= lr.last_run_observed_at
    )
  ) AS failed
) last_attempt ON true
LEFT JOIN LATERAL (
  SELECT CASE
    WHEN lp.generation_id IS NULL THEN 'no_published_generation'
    WHEN coalesce(wr.replay_required, '{}'::jsonb) <> '{}'::jsonb
      THEN 'replay_required'
    WHEN aw.workflow_id IS NOT NULL THEN 'frontier_advancing'
    WHEN coalesce(aws.unattributed_active_count, 0) > 0
      THEN 'frontier_advancing'
    -- Before no_watermark: a source whose first run failed may have no
    -- watermark, and the failure is what a reader must see.
    WHEN coalesce(last_attempt.failed, false) THEN 'last_run_failed'
    WHEN wr.source IS NULL THEN 'no_watermark'
    WHEN NOT coalesce(uw.evaluated, false)
      THEN 'published_state_unavailable'
    -- Unpublished work behind a blocked publish: the next publish will not
    -- take it until a person clears the cause (issue #3024).
    WHEN wr.source = ANY(uw.unpublished_sources)
      AND uw.blocked_generation_id IS NOT NULL
      THEN 'publish_blocked'
    -- Before unpublished_writes: a missing blob needs a person even while
    -- an embedding write or a no-publish ingest waits for the next publish.
    WHEN wr.last_applied_rev ? 'record_set_ref' AND srb.source IS NULL
      THEN 'record_set_blob_missing'
    WHEN wr.source = ANY(uw.unpublished_sources)
      THEN 'unpublished_writes'
    ELSE 'read_visible'
  END AS read_visibility_state
) rv ON true
ORDER BY deployment NULLS LAST, registry_source_name, source_name;
$$;

CREATE OR REPLACE VIEW okg.v_source_frontier_state AS
SELECT *
FROM okg.source_frontier_state();

-- `okg.v_source_runs` — compatibility-shaped per-run source
-- activity. Prefer SourceRunner's persisted `sync.run.<run_id>`
-- payloads for authoritative counts; use stage-timing diagnostics and
-- append-only fact logs as compatibility fallbacks. A source with no
-- per-run evidence projects ZERO rows ("no runs recorded") — never a
-- run-shaped placeholder with NULL run_id/counts. Watermark state is
-- readable via `okg.source_watermarks` / `v_source_frontier_state`.
CREATE OR REPLACE VIEW okg.v_source_runs AS
WITH payload_runs AS (
  SELECT
    sp.source_name AS source_name,
    (sp.value::jsonb ->> 'run_id') AS run_id,
    NULLIF(sp.value::jsonb ->> 'started_at', '')::timestamptz AS started_at,
    NULLIF(sp.value::jsonb ->> 'finished_at', '')::timestamptz AS finished_at,
    COALESCE((sp.value::jsonb ->> 'nodes_appended')::int, 0)
      AS nodes_appended,
    COALESCE((sp.value::jsonb ->> 'edges_appended')::int, 0)
      AS edges_appended,
    COALESCE((sp.value::jsonb ->> 'nodes_retracted')::int, 0)
      AS node_retractions,
    COALESCE((sp.value::jsonb ->> 'edges_retracted')::int, 0)
      AS edge_retractions
  FROM okg.source_progress sp
  WHERE sp.key LIKE 'sync.run.%'
    AND sp.value LIKE '{%'
    -- A payload missing run_id would project the exact NULL-run_id row
    -- this view bans; treat it as no evidence, not a run.
    AND (sp.value::jsonb ->> 'run_id') IS NOT NULL
),
diagnostic_runs AS (
  SELECT
    source_name,
    run_id,
    started_at,
    finished_at,
    nodes_appended,
    edges_appended,
    node_retractions,
    edge_retractions
  FROM okg.v_source_run_diagnostics
  WHERE run_id IS NOT NULL
),
fact_runs AS (
  SELECT
    source,
    run_id,
    observed_at,
    CASE WHEN op = 'I' THEN 1 ELSE 0 END AS nodes_appended,
    0 AS edges_appended,
    CASE WHEN op = 'R' THEN 1 ELSE 0 END AS node_retractions,
    0 AS edge_retractions
  FROM okg.node_facts
  UNION ALL
  SELECT
    source,
    run_id,
    observed_at,
    0 AS nodes_appended,
    CASE WHEN op = 'I' THEN 1 ELSE 0 END AS edges_appended,
    0 AS node_retractions,
    CASE WHEN op = 'R' THEN 1 ELSE 0 END AS edge_retractions
  FROM okg.edge_facts
),
fact_agg AS (
  SELECT
    source AS source_name,
    run_id::text AS run_id,
    min(observed_at) AS started_at,
    max(observed_at) AS finished_at,
    sum(nodes_appended)::int AS nodes_appended,
    sum(edges_appended)::int AS edges_appended,
    sum(node_retractions)::int AS node_retractions,
    sum(edge_retractions)::int AS edge_retractions
  FROM fact_runs
  WHERE run_id IS NOT NULL
  GROUP BY source, run_id
)
SELECT
  COALESCE(p.source_name, d.source_name, f.source_name) AS source_name,
  COALESCE(p.run_id, d.run_id, f.run_id) AS run_id,
  COALESCE(p.started_at, d.started_at, f.started_at) AS started_at,
  COALESCE(p.finished_at, d.finished_at, f.finished_at) AS finished_at,
  COALESCE(p.nodes_appended, d.nodes_appended, f.nodes_appended, 0)::int AS nodes_appended,
  COALESCE(p.edges_appended, d.edges_appended, f.edges_appended, 0)::int AS edges_appended,
  COALESCE(p.node_retractions, d.node_retractions, f.node_retractions, 0)::int AS node_retractions,
  COALESCE(p.edge_retractions, d.edge_retractions, f.edge_retractions, 0)::int AS edge_retractions
FROM payload_runs p
FULL OUTER JOIN diagnostic_runs d
  ON d.source_name = p.source_name
 AND d.run_id = p.run_id
FULL OUTER JOIN fact_agg f
  ON f.source_name = COALESCE(p.source_name, d.source_name)
 AND f.run_id = COALESCE(p.run_id, d.run_id);

-- `okg.projectable_node_facts` / `okg.projectable_edge_facts` are DELETED
-- (docs/reference/database-schema.md §2.1, rev 4). They were the
-- pre-Chronos Phase-A exclusion filter: a `NOT EXISTS` over rejected
-- admission fact ranges, consulted only by the live-table projection this
-- change also deleted. The Chronos path had already replaced them with
-- `WHERE false` zero-row stubs, so what would have frozen at OSS is two
-- granted views that structurally cannot return a row.
--
-- Rejection safety for committed derived facts did not live here and does
-- not move: it is the derived-fact loader's (source, run_id) envelope
-- deny-list (`publisher.py::_load_unapplied_derived_facts`), pinned by
-- tests/substrate/test_admission_rejection_projection.py.

-- `okg.v_producer_profiles` — normalized producer authority records.
-- Operators use this to see which producers are actively signature-
-- governed and which remain in compatibility mode.
CREATE OR REPLACE VIEW okg.v_producer_profiles AS
SELECT
  profile_id,
  deployment_name,
  producer_id,
  producer_kind,
  trust_label,
  admission_mode,
  status,
  catalog_version_id,
  profile_fingerprint,
  authority_scope,
  allowed_record_kinds,
  output_signature,
  policy_clauses,
  output_scope_summary,
  source_ref,
  catalog_evidence,
  created_at,
  updated_at
FROM okg.producer_profiles;

-- `okg.v_consistency_observations` — current consistency issue queue.
-- Physical consistency tables remain internal; agents review open/resolved
-- observations through this view using the bounded MCP `query` escape hatch.
CREATE OR REPLACE VIEW okg.v_consistency_observations AS
SELECT
  oc.observation_id,
  defs.deployment_name,
  oc.check_id,
  defs.check_kind,
  defs.predicate_family,
  oc.candidate_id,
  oc.latest_generation_id,
  oc.latest_run_id::text AS latest_run_id,
  oc.latest_event_id,
  oc.finding_status,
  oc.evaluation_status,
  oc.input_scope,
  oc.predicate_key,
  oc.normalized_predicate,
  oc.normalized_values,
  oc.participant_roles,
  oc.participant_anchors,
  oc.severity,
  oc.confidence,
  oc.confidence_type,
  oc.evidence,
  oc.payload,
  oc.updated_at
FROM okg.observation_current oc
JOIN okg.consistency_check_definitions defs
  ON defs.check_id = oc.check_id;

-- `okg.v_agent_sessions` — compact transcript session rows for
-- context-efficient agent exploration. Raw transcript payloads and full
-- attrs stay behind `okg.v_nodes`; this view exposes typed summary fields
-- useful for finding high-cost / high-tool sessions.
CREATE OR REPLACE VIEW okg.v_agent_sessions AS
SELECT
  n.node_id AS session_node_id,
  n.attrs->>'session_id' AS session_id,
  n.subtype AS session_subtype,
  n.attrs->>'model' AS model,
  n.attrs->>'cwd' AS cwd,
  n.attrs->>'repo' AS repo,
  n.attrs->>'transcript_path' AS transcript_path,
  CASE WHEN COALESCE(n.attrs->>'started_at', n.attrs->>'timestamp') ~
            '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN COALESCE(n.attrs->>'started_at', n.attrs->>'timestamp')::timestamptz
       ELSE NULL END AS started_at,
  CASE WHEN n.attrs->>'ended_at' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN (n.attrs->>'ended_at')::timestamptz ELSE NULL END AS ended_at,
  CASE WHEN COALESCE(n.attrs->>'last_active_at', n.attrs->>'timestamp') ~
            '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN COALESCE(n.attrs->>'last_active_at', n.attrs->>'timestamp')::timestamptz
       ELSE NULL END AS last_active_at,
  CASE WHEN n.attrs->>'message_count' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'message_count')::int END AS message_count,
  CASE WHEN n.attrs->>'tool_call_count' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'tool_call_count')::int END AS tool_call_count,
  CASE WHEN n.attrs->>'input_tokens' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'input_tokens')::bigint END AS input_tokens,
  CASE WHEN n.attrs->>'output_tokens' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'output_tokens')::bigint END AS output_tokens,
  CASE WHEN n.attrs->>'cache_read_tokens' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'cache_read_tokens')::bigint END AS cache_read_tokens,
  CASE WHEN n.attrs->>'cache_creation_tokens' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'cache_creation_tokens')::bigint END AS cache_creation_tokens,
  CASE WHEN n.attrs->>'total_tokens' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'total_tokens')::bigint END AS total_tokens,
  CASE WHEN n.attrs->>'cost_usd_estimate' ~ '^-?[0-9]+(\.[0-9]+)?$'
       THEN (n.attrs->>'cost_usd_estimate')::double precision END AS cost_usd_estimate,
  n.attrs->>'cost_confidence' AS cost_confidence,
  n.attrs->>'cost_basis' AS cost_basis
FROM okg.v_nodes n
WHERE n.subtype IN ('agent_session', 'sub_agent_session');

-- `okg.v_agent_messages` — ordered bounded excerpts for one session.
-- Content remains the source-provided bounded excerpt/hash pair; this view
-- intentionally omits raw transcript bodies.
CREATE OR REPLACE VIEW okg.v_agent_messages AS
SELECT
  n.node_id AS message_node_id,
  n.attrs->>'session_id' AS session_id,
  'session:' || (n.attrs->>'session_id') AS session_node_id,
  COALESCE(
    CASE WHEN n.attrs->>'turn_index' ~ '^-?[0-9]+$'
         THEN (n.attrs->>'turn_index')::int END,
    CASE WHEN n.node_id ~ ':[0-9]+$'
         THEN substring(n.node_id FROM ':([0-9]+)$')::int END
  ) AS turn_index,
  CASE WHEN n.attrs->>'chapter_index' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'chapter_index')::int END AS chapter_index,
  n.attrs->>'role' AS role,
  n.attrs->>'message_kind' AS message_kind,
  left(COALESCE(n.attrs->>'content_excerpt', ''), 500) AS content_excerpt,
  n.attrs->>'content_hash' AS content_hash,
  CASE WHEN n.attrs->>'tool_call_count' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'tool_call_count')::int END AS tool_call_count,
  CASE WHEN n.attrs->>'timestamp' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN (n.attrs->>'timestamp')::timestamptz ELSE NULL END AS timestamp
FROM okg.v_nodes n
WHERE n.subtype = 'agent_message';

-- `okg.v_tool_use_events` — ordered compact tool-call rows. Commands and
-- inputs are bounded source excerpts; full raw tool payloads are not exposed.
CREATE OR REPLACE VIEW okg.v_tool_use_events AS
SELECT
  n.node_id AS tool_event_node_id,
  n.attrs->>'event_id' AS event_id,
  n.attrs->>'session_id' AS session_id,
  'session:' || (n.attrs->>'session_id') AS session_node_id,
  CASE WHEN n.attrs->>'turn_index' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'turn_index')::int END AS turn_index,
  CASE WHEN n.attrs->>'block_index' ~ '^-?[0-9]+$'
       THEN (n.attrs->>'block_index')::int END AS block_index,
  n.attrs->>'tool_name' AS tool_name,
  n.attrs->>'mcp_server' AS mcp_server,
  n.attrs->>'mcp_deployment' AS mcp_deployment,
  n.attrs->>'mcp_operator' AS mcp_operator,
  n.attrs->>'file_path' AS file_path,
  left(COALESCE(n.attrs->>'command', ''), 500) AS command_excerpt,
  left(COALESCE(n.attrs->>'input_excerpt', ''), 500) AS input_excerpt,
  CASE WHEN n.attrs->>'timestamp' ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
       THEN (n.attrs->>'timestamp')::timestamptz ELSE NULL END AS timestamp
FROM okg.v_nodes n
WHERE n.subtype = 'tool_use_event';

-- `okg.v_admission_envelopes` — compact admission lifecycle rows.
-- Generation-agnostic operational view: admission state can exist
-- before a generation is published or after one is blocked.
-- schema-identity-convergence / D2: the legacy BIGINT `target_generation_id`
-- is gone from okg.admission_envelopes, so this view loses a column, which
-- CREATE OR REPLACE cannot do.
--
-- This deliberately does NOT hand-write a `DROP VIEW IF EXISTS` first. An
-- earlier draft did, justified by a repo grep finding no other reference to
-- the view -- but a grep over this tree cannot speak for a live database.
-- An operator's own view sitting on top of this one is invisible to it, and
-- against such a database the unconditional drop raises a bare
-- `DependentObjectsStillExist` that names nothing actionable, after having
-- already destroyed the grants and comments the convergence pass reports.
--
-- `view_convergence` (PR #1109) is the general mechanism for exactly this
-- hazard: it drops and recreates a frozen-shape view in dependency order,
-- reports which views it recreated, then the shared install boundary reapplies
-- and verifies `okg.grant_registry`. It refuses -- naming the blocker -- when a
-- dependant belongs to someone other than these scripts.
-- Leave the shape change to it; a per-view hand-written drop only defeats
-- the refusal path. Covered by
-- tests/substrate/db/test_view_convergence.py.
CREATE OR REPLACE VIEW okg.v_admission_envelopes AS
SELECT
  admission_id::text AS admission_id,
  deployment_name,
  producer_id,
  producer_kind,
  intent,
  admission_mode,
  status,
  catalog_version_id,
  source,
  run_id::text AS run_id,
  idempotency_key,
  authority,
  check_summary,
  verdict,
  workflow_ref,
  created_at,
  updated_at,
  decided_at,
  published_at,
  target_generation_ref,
  NULLIF(btrim(target_generation_ref), '') AS canonical_generation_ref
FROM okg.admission_envelopes;

COMMENT ON VIEW okg.v_admission_envelopes IS
  'Compact admission lifecycle rows. target_generation_ref is the only generation reference; the legacy numeric compatibility token was removed in v1.0 (D2).';
COMMENT ON COLUMN okg.v_admission_envelopes.target_generation_ref IS
  'Canonical text generation reference for the Chronos generation targeted by this admission envelope.';
COMMENT ON COLUMN okg.v_admission_envelopes.canonical_generation_ref IS
  'Canonical Chronos generation reference normalized from target_generation_ref; NULL when the envelope has not yet been bound to a generation.';

-- `okg.v_admission_events` — append-only lifecycle evidence for each
-- envelope, including duplicate coalescing and review decisions.
CREATE OR REPLACE VIEW okg.v_admission_events AS
SELECT
  event_id,
  admission_id::text AS admission_id,
  event_type,
  actor,
  reason,
  payload,
  workflow_ref,
  created_at
FROM okg.admission_events;

-- `okg.v_nomos_policy_bundles` — compiled policy artifacts and
-- lifecycle state. Bundle JSON is exposed for operator audit; raw
-- secrets do not belong in policy bundles.
CREATE OR REPLACE VIEW okg.v_nomos_policy_bundles AS
SELECT
  bundle_row_id,
  deployment_name,
  policy_bundle_id,
  policy_version,
  policy_fingerprint,
  compiler_version,
  source_generation_id,
  scope,
  default_effect,
  reason_code_catalog,
  bundle,
  status,
  effective_from,
  effective_until,
  compiled_at,
  metadata,
  created_at,
  updated_at
FROM okg.nomos_policy_bundles;

-- `okg.v_nomos_policy_activations` — active policy binding by
-- deployment/scope. Exactly one active row per scope is enforced by
-- the table's partial unique index.
CREATE OR REPLACE VIEW okg.v_nomos_policy_activations AS
SELECT
  activation_row_id,
  deployment_name,
  activation_id,
  scope,
  policy_fingerprint,
  status,
  activated_by,
  activation_decision_id,
  metadata,
  activated_at,
  superseded_at
FROM okg.nomos_policy_activations;

-- `okg.v_nomos_config_sets` — compiled configuration artifacts.
-- The owner table stores raw compiled entries for privileged runtime
-- paths; this app-readable view keeps keys/classification while
-- replacing entry payloads and secret references with boolean markers.
CREATE OR REPLACE VIEW okg.v_nomos_config_sets AS
SELECT
  c.config_row_id,
  c.deployment_name,
  c.config_set_id,
  c.config_version,
  c.config_fingerprint,
  c.compiler_version,
  c.source_generation_id,
  c.scope,
  redacted.entries AS entries,
  CASE
    WHEN jsonb_typeof(c.config_set) = 'object' THEN
      (c.config_set - 'entries') ||
      jsonb_build_object('entries', redacted.entries)
    ELSE jsonb_build_object('entries', redacted.entries)
  END AS config_set,
  c.status,
  c.effective_from,
  c.effective_until,
  c.compiled_at,
  CASE
    WHEN c.metadata = '{}'::jsonb THEN '{}'::jsonb
    ELSE jsonb_build_object('metadata_redacted', true)
  END AS metadata,
  c.created_at,
  c.updated_at
FROM okg.nomos_config_sets c
CROSS JOIN LATERAL (
  SELECT COALESCE(
    jsonb_agg(
      jsonb_build_object(
        'config_key', entry->'config_key',
        'classification', entry->'classification',
        'scope', entry->'scope',
        'owner', entry->'owner',
        'metadata_redacted',
          (entry ? 'metadata') AND (entry->'metadata' <> '{}'::jsonb),
        'value_redacted', entry ? 'value',
        'secret_ref_redacted', entry ? 'secret_ref'
      )
      ORDER BY ord
    ),
    '[]'::jsonb
  ) AS entries
  FROM jsonb_array_elements(c.entries) WITH ORDINALITY AS item(entry, ord)
) redacted;

-- `okg.v_nomos_config_activations` — active config binding by
-- deployment/scope. Runtime code uses this state instead of YAML/env
-- fallback once a scope is Nomos-owned.
CREATE OR REPLACE VIEW okg.v_nomos_config_activations AS
SELECT
  activation_row_id,
  deployment_name,
  activation_id,
  scope,
  config_fingerprint,
  status,
  activated_by,
  activation_decision_id,
  metadata,
  activated_at,
  superseded_at
FROM okg.nomos_config_activations;

-- `okg.v_nomos_identity_providers` — deployment-scoped external
-- identity providers trusted by Nomos. Secret-bearing key material is
-- not stored here; only references and policy metadata are exposed.
CREATE OR REPLACE VIEW okg.v_nomos_identity_providers AS
SELECT
  provider_row_id,
  deployment_name,
  provider_id,
  provider_type,
  issuer,
  audiences,
  subject_claim,
  group_claims,
  role_claims,
  entitlement_claims,
  assurance_claim,
  key_reference,
  secret_references,
  claim_allowlist,
  required_assurance,
  mfa_required,
  group_claims_approved,
  metadata_fingerprint,
  realm,
  service_principal,
  freshness_seconds,
  status,
  metadata,
  effective_from,
  effective_until,
  created_at,
  updated_at
FROM okg.nomos_identity_providers;

-- `okg.v_nomos_external_claims` — redacted/fingerprinted claim
-- evidence. Raw access tokens, SAML assertions, cookies, passwords, and
-- private keys are intentionally absent from this view and table.
CREATE OR REPLACE VIEW okg.v_nomos_external_claims AS
SELECT
  claim_row_id,
  deployment_name,
  provider_id,
  provider_type,
  issuer,
  external_subject,
  audiences,
  groups,
  roles,
  entitlements,
  assurance,
  assurance_values,
  mfa_satisfied,
  realm,
  service_principal,
  delegation_chain,
  expires_at,
  not_before,
  auth_time,
  claim_fingerprint,
  verification_status,
  verification_error,
  evidence,
  nomos_decision_id,
  observed_at
FROM okg.nomos_external_claims;

-- `okg.v_nomos_subject_bindings` — issuer-scoped mapping from
-- verified external claims to Nomos subjects.
CREATE OR REPLACE VIEW okg.v_nomos_subject_bindings AS
SELECT
  binding_row_id,
  deployment_name,
  binding_id,
  provider_id,
  issuer,
  claim_path,
  claim_value,
  subject,
  scope,
  priority,
  required_assurance,
  status,
  effective_from,
  effective_until,
  metadata,
  created_at,
  updated_at
FROM okg.nomos_subject_bindings;

-- `okg.v_nomos_permission_grants` — normalized grants consumed by
-- the Postgres-backed Nomos identity resolver.
CREATE OR REPLACE VIEW okg.v_nomos_permission_grants AS
SELECT
  grant_row_id,
  deployment_name,
  grant_id,
  subject,
  actions,
  scope,
  effect,
  resource_match,
  classifications,
  priority,
  governing_rule_ids,
  status,
  metadata,
  created_at,
  updated_at
FROM okg.nomos_permission_grants;

-- `okg.v_nomos_decisions` — durable policy decision evidence.
CREATE OR REPLACE VIEW okg.v_nomos_decisions AS
SELECT
  decision_row_id,
  deployment_name,
  decision_id,
  policy_bundle_id,
  policy_version,
  policy_fingerprint,
  input_hash,
  subject,
  action,
  resource,
  classification,
  context,
  effect,
  reason_code,
  matched_rule_ids,
  obligations,
  enforcement_point,
  target,
  created_at
FROM okg.nomos_decisions;

-- `okg.v_admission_record_refs` — compact pointers from envelopes to
-- high-volume facts or review payloads. Fact ranges are reconstructible
-- from `(source, run_id)` if a replay crashed before refs attached.
CREATE OR REPLACE VIEW okg.v_admission_record_refs AS
SELECT
  ref_id,
  admission_id::text AS admission_id,
  ref_key,
  ref_kind,
  source,
  run_id::text AS run_id,
  fact_id_min,
  fact_id_max,
  record_count,
  payload,
  created_at
FROM okg.admission_record_refs;

-- `okg.v_runtime_queue_health` -- active DBOS runtime queue health.
-- It reports an `unavailable` row when DBOS has not been bootstrapped instead
-- of letting missing runtime state look like healthy idle runtime.
DROP VIEW IF EXISTS okg.v_runtime_queue_health;
DROP FUNCTION IF EXISTS okg.runtime_queue_health(integer);

CREATE OR REPLACE FUNCTION okg.runtime_queue_health(
  p_stale_after_seconds integer DEFAULT 600
) RETURNS TABLE (
  queue_name text,
  status text,
  availability text,
  active_count bigint,
  pending_count bigint,
  waiting_count bigint,
  delayed_count bigint,
  running_count bigint,
  failed_count bigint,
  cancelled_count bigint,
  dlq_count bigint,
  terminal_history_count bigint,
  unknown_active_age_count bigint,
  oldest_active_age_seconds double precision,
  newest_active_age_seconds double precision,
  stale_after_seconds integer,
  stale_active_work_present boolean,
  generated_at timestamptz,
  reason text,
  workflow_name text
)
LANGUAGE plpgsql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, okg, dbos
AS $$
DECLARE
  has_status boolean;
  has_queue_name boolean;
  has_workflow_name boolean;
  updated_at_type text;
  created_at_type text;
  has_started_at_epoch_ms boolean;
  queue_expr text;
  workflow_expr text;
  observed_expr text;
  started_expr text;
  total_rows bigint;
BEGIN
  IF to_regclass('dbos.workflow_status') IS NULL THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'unavailable'::text,
      'unavailable'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      'dbos.workflow_status missing'::text,
      NULL::text;
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'status'
  ) INTO has_status;
  IF NOT has_status THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'unavailable'::text,
      'unavailable'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      'dbos.workflow_status.status missing'::text,
      NULL::text;
    RETURN;
  END IF;

  EXECUTE 'SELECT count(*)::bigint FROM dbos.workflow_status' INTO total_rows;
  IF total_rows = 0 THEN
    RETURN QUERY
    SELECT
      NULL::text,
      'idle'::text,
      'available'::text,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      0::bigint,
      NULL::double precision,
      NULL::double precision,
      p_stale_after_seconds,
      false,
      now(),
      NULL::text,
      NULL::text;
    RETURN;
  END IF;

  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'queue_name'
  ) INTO has_queue_name;
  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'name'
  ) INTO has_workflow_name;
  SELECT data_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'updated_at'
   LIMIT 1
    INTO updated_at_type;
  SELECT data_type
    FROM information_schema.columns
   WHERE table_schema = 'dbos'
     AND table_name = 'workflow_status'
     AND column_name = 'created_at'
   LIMIT 1
    INTO created_at_type;
  SELECT EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'dbos'
       AND table_name = 'workflow_status'
       AND column_name = 'started_at_epoch_ms'
  ) INTO has_started_at_epoch_ms;

  queue_expr := CASE
    WHEN has_queue_name THEN 'queue_name'
    ELSE 'NULL::text'
  END;
  workflow_expr := CASE
    WHEN has_workflow_name THEN 'name'
    ELSE 'NULL::text'
  END;
  observed_expr := CASE
    WHEN updated_at_type IN (
      'bigint', 'integer', 'numeric', 'double precision', 'real'
    ) THEN 'to_timestamp(updated_at::double precision / 1000.0)'
    WHEN updated_at_type IS NOT NULL THEN 'updated_at::timestamptz'
    WHEN created_at_type IN (
      'bigint', 'integer', 'numeric', 'double precision', 'real'
    ) THEN 'to_timestamp(created_at::double precision / 1000.0)'
    WHEN created_at_type IS NOT NULL THEN 'created_at::timestamptz'
    ELSE 'NULL::timestamptz'
  END;
  started_expr := CASE
    WHEN has_started_at_epoch_ms
    THEN 'started_at_epoch_ms IS NOT NULL AND started_at_epoch_ms > 0'
    ELSE 'false'
  END;

  RETURN QUERY EXECUTE format($runtime_queue_health$
    WITH workflow_rows AS (
      SELECT
        COALESCE(%1$s, 'default')::text AS queue_name,
        NULLIF(%5$s, '')::text AS workflow_name,
        upper(status::text) AS workflow_status,
        %2$s AS observed_at,
        (%3$s) AS started
      FROM dbos.workflow_status
    ),
    grouped AS (
      SELECT
        queue_name,
        workflow_name,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        )::bigint AS active_count,
        count(*) FILTER (WHERE workflow_status = 'PENDING')::bigint
          AS pending_count,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'DELAYED')
        )::bigint AS waiting_count,
        count(*) FILTER (WHERE workflow_status = 'DELAYED')::bigint
          AS delayed_count,
        count(*) FILTER (
          WHERE workflow_status = 'PENDING' AND started
        )::bigint AS running_count,
        count(*) FILTER (
          WHERE workflow_status IN (
            'ERROR', 'FAILED', 'MAX_RECOVERY_ATTEMPTS',
            'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
          )
        )::bigint AS failed_count,
        count(*) FILTER (WHERE workflow_status = 'CANCELLED')::bigint
          AS cancelled_count,
        count(*) FILTER (
          WHERE workflow_status IN (
            'CANCELLED', 'ERROR', 'FAILED', 'MAX_RECOVERY_ATTEMPTS',
            'MAX_RECOVERY_ATTEMPTS_EXCEEDED'
          )
        )::bigint AS dlq_count,
        count(*) FILTER (
          WHERE workflow_status NOT IN ('ENQUEUED', 'PENDING', 'DELAYED')
        )::bigint AS terminal_history_count,
        count(*) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
            AND observed_at IS NULL
        )::bigint AS unknown_active_age_count,
        min(observed_at) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        ) AS oldest_active_at,
        max(observed_at) FILTER (
          WHERE workflow_status IN ('ENQUEUED', 'PENDING', 'DELAYED')
        ) AS newest_active_at
      FROM workflow_rows
      GROUP BY queue_name, workflow_name
    )
    SELECT
      queue_name,
      CASE
        WHEN active_count = 0 AND dlq_count = 0 THEN 'idle'
        WHEN active_count > 0 AND unknown_active_age_count > 0
          THEN 'unavailable'
        WHEN dlq_count > 0 AND failed_count > 0 AND cancelled_count > 0
          THEN 'dlq'
        WHEN dlq_count > 0 AND failed_count > 0 THEN 'failed'
        WHEN dlq_count > 0 AND cancelled_count > 0 THEN 'cancelled'
        WHEN dlq_count > 0 THEN 'dlq'
        WHEN oldest_active_at IS NOT NULL
          AND EXTRACT(EPOCH FROM now() - oldest_active_at) > %4$s
          THEN 'stale'
        WHEN running_count > 0 THEN 'running'
        WHEN active_count > 0 THEN 'waiting'
        ELSE 'idle'
      END AS status,
      'available'::text AS availability,
      active_count,
      pending_count,
      waiting_count,
      delayed_count,
      running_count,
      failed_count,
      cancelled_count,
      dlq_count,
      terminal_history_count,
      unknown_active_age_count,
      CASE
        WHEN oldest_active_at IS NULL THEN NULL::double precision
        ELSE EXTRACT(EPOCH FROM now() - oldest_active_at)::double precision
      END AS oldest_active_age_seconds,
      CASE
        WHEN newest_active_at IS NULL THEN NULL::double precision
        ELSE EXTRACT(EPOCH FROM now() - newest_active_at)::double precision
      END AS newest_active_age_seconds,
      %4$s::integer AS stale_after_seconds,
      (
        oldest_active_at IS NOT NULL
        AND EXTRACT(EPOCH FROM now() - oldest_active_at) > %4$s
      ) AS stale_active_work_present,
      now() AS generated_at,
      NULL::text AS reason,
      workflow_name
    FROM grouped
    ORDER BY queue_name, workflow_name NULLS FIRST
  $runtime_queue_health$, queue_expr, observed_expr, started_expr,
  p_stale_after_seconds, workflow_expr);
END;
$$;

CREATE OR REPLACE VIEW okg.v_runtime_queue_health AS
SELECT * FROM okg.runtime_queue_health();

-- `okg.v_runtime_lock_surface_inventory` — authority map for runtime
-- lock/lease-like concepts. Advisory locks are session state, not durable
-- lease rows; table-backed claims expose their stale rows through
-- `okg.v_runtime_stale_locks`.
CREATE OR REPLACE VIEW okg.v_runtime_lock_surface_inventory AS
SELECT
  surface_id,
  surface_kind,
  authority,
  storage_relation,
  owner_column,
  expires_column,
  active_status,
  owner_semantics,
  expiry_semantics,
  cleanup_semantics,
  visibility_view,
  table_backed
FROM (
  VALUES
    (
      'publisher_advisory_lock'::text,
      'session_advisory_lock'::text,
      'Publisher.cycle DBOS workflow plus Postgres session advisory lock'::text,
      NULL::text,
      NULL::text,
      NULL::text,
      NULL::text,
      'Owner is the live backend session holding the advisory lock; no OKG row records a holder.'::text,
      'No row TTL. The lock is released by explicit unlock or backend session close.'::text,
      'Inspect publisher workflow/session state; do not recreate okg.publisher_leases.'::text,
      NULL::text,
      false::boolean
    ),
    (
      'catalog_apply_advisory_lock'::text,
      'session_advisory_lock'::text,
      'Catalog apply CLI plus Postgres session advisory lock'::text,
      NULL::text,
      NULL::text,
      NULL::text,
      NULL::text,
      'Owner is the live backend session applying catalog state; no OKG row records a holder.'::text,
      'No row TTL. The lock is released by explicit unlock or backend session close.'::text,
      'Inspect catalog apply process/session state; do not infer ownership from lease_audit.'::text,
      NULL::text,
      false::boolean
    ),
    (
      'legacy_publisher_lease_audit'::text,
      'retired_audit_log'::text,
      'Historical audit rows from retired okg.publisher_leases runtime'::text,
      'okg.lease_audit'::text,
      'holder_id'::text,
      NULL::text,
      NULL::text,
      'holder_id is historical evidence, not current lock ownership.'::text,
      'No active expiry. Rows describe past acquire/release/lost events.'::text,
      'Retained for audit only; current publisher coordination is advisory lock plus DBOS.'::text,
      NULL::text,
      true::boolean
    ),
    (
      'mailbox_message_claim'::text,
      'table_backed_lease'::text,
      'Federation mailbox recipient polling claim'::text,
      'okg.federation_mailbox_messages'::text,
      'lease_token'::text,
      'lease_expires_at'::text,
      'leased'::text,
      'lease_token is bearer material for the authenticated recipient and must not be exposed by read-side views.'::text,
      'lease_expires_at bounds the poll claim; expired leased messages are claimable again.'::text,
      'Polling reclaims expired leased messages with a fresh token; ack/deny require an active token.'::text,
      'okg.v_runtime_stale_locks'::text,
      true::boolean
    )
) AS inventory(
  surface_id,
  surface_kind,
  authority,
  storage_relation,
  owner_column,
  expires_column,
  active_status,
  owner_semantics,
  expiry_semantics,
  cleanup_semantics,
  visibility_view,
  table_backed
);

CREATE OR REPLACE VIEW okg.v_runtime_stale_locks AS
SELECT
  'mailbox_message_claim'::text AS surface_id,
  'okg.federation_mailbox_messages'::text AS storage_relation,
  msg.message_id AS resource_id,
  msg.status AS active_status,
  msg.recipient_authority_id AS owner_ref,
  (msg.lease_token IS NOT NULL AND length(msg.lease_token) > 0)
    AS token_present,
  msg.lease_expires_at,
  CASE
    WHEN msg.lease_token IS NULL OR length(msg.lease_token) = 0
      THEN 'missing_token'
    WHEN msg.lease_expires_at IS NULL
      THEN 'missing_expiry'
    WHEN msg.lease_expires_at <= now()
      THEN 'expired'
  END AS stale_reason,
  CASE
    WHEN msg.lease_expires_at IS NULL THEN NULL::bigint
    ELSE greatest(
      0,
      extract(epoch FROM now() - msg.lease_expires_at)::bigint
    )
  END AS stale_for_seconds,
  'mailbox polling can reclaim expired leased messages with a fresh token; ack/deny require an active token'::text
    AS cleanup_semantics,
  jsonb_build_object(
    'message_id', msg.message_id,
    'sender_authority_id', msg.sender_authority_id,
    'recipient_authority_id', msg.recipient_authority_id,
    'payload_mode', msg.payload_mode,
    'expires_at', msg.expires_at
  ) AS metadata
FROM okg.federation_mailbox_messages msg
WHERE msg.status = 'leased'
  AND (
    msg.lease_token IS NULL
    OR length(msg.lease_token) = 0
    OR msg.lease_expires_at IS NULL
    OR msg.lease_expires_at <= now()
  );

-- `okg.v_compute_host_metrics` — per-host, per-metric resource rollup over the
-- `okg.node_series` hypertable (issue #83 external-backed plane). One row per
-- (deployment, host node, metric): the latest reading plus 1h / 1d / 7d windowed
-- aggregates, sample counts, staleness, and first-seen. `node_series` lives
-- OUTSIDE Chronos, so this is a LIVE operational view (not generation-pinned) —
-- the same class as `v_runtime_queue_health` / `v_source_run_diagnostics`. The
-- resource-monitoring console view reads it; agents may also query it via
-- `run_sql` for fleet status. Pure over node_series (no graph join) so run_sql
-- keeps it on the app_ro/Postgres path rather than the Chronos checkout.
CREATE OR REPLACE VIEW okg.v_compute_host_metrics AS
WITH latest AS (
  -- sample_id DESC totally orders ties: observed_at is not unique per
  -- (deployment, node, metric), so it is the deterministic latest-row pick.
  SELECT DISTINCT ON (deployment_name, node_id, metric)
         deployment_name, node_id, metric,
         value       AS last_value,
         observed_at AS last_observed_at
  FROM okg.node_series
  ORDER BY deployment_name, node_id, metric, observed_at DESC, sample_id DESC
)
SELECT
  s.deployment_name,
  s.node_id,
  s.metric,
  l.last_value,
  l.last_observed_at,
  EXTRACT(EPOCH FROM now() - l.last_observed_at)::double precision
    AS staleness_seconds,
  count(*)::int                                                          AS samples_total,
  count(*) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 hour')::int AS samples_1h,
  avg(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 hour')  AS avg_1h,
  max(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 hour')  AS max_1h,
  min(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 hour')  AS min_1h,
  avg(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 day')   AS avg_1d,
  max(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 day')   AS max_1d,
  min(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '1 day')   AS min_1d,
  avg(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '7 days')  AS avg_7d,
  max(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '7 days')  AS max_7d,
  min(s.value) FILTER (WHERE s.observed_at >= now() - INTERVAL '7 days')  AS min_7d,
  min(s.observed_at)                                                     AS first_observed_at
FROM okg.node_series s
JOIN latest l USING (deployment_name, node_id, metric)
GROUP BY s.deployment_name, s.node_id, s.metric, l.last_value, l.last_observed_at;

-- ========== Indexes for the view layer ==========
--
-- Empty by design. The one index that lived here
-- (`edges_live_member_of_dst_live_idx`, a reverse `member_of` probe for
-- v_doc_chunks / v_chunk_sequence) was on `okg.edges_live` and went with
-- that table. The Chronos equivalents are created and maintained on the
-- live interval segment by
-- `graph_store_internal/schema.py::_ensure_interval_source_read_indexes`,
-- which runs on every migrate against the current segment — an index on
-- the logical `okg.graph_edges` template would index nothing.

-- ========== Permissions: app_ro default-deny, view-only ==========
--
-- Per `harden-mcp-run-sql-safety` (2026-04-27): default-deny on
-- `app_ro`. Replaces the prior explicit-revoke list (which left
-- 5 non-view tables — `invariants`, `catalog_versions`,
-- `link_cache`, `source_watermarks`, `mcp_trace_events`,
-- `trace_sessions`, `trace_backtracks` — quietly readable, in
-- contradiction to the spec).
--
-- Three layers of forward-safety:
--   1. REVOKE ALL ON ALL TABLES → revoke every table-level
--      privilege at migration time, regardless of how grants
--      accumulated.
--   2. ALTER DEFAULT PRIVILEGES → future CREATE TABLE in this
--      schema defaults to inaccessible to `app_ro` even if the
--      migration author forgets to revoke.
--   3. An explicit registry entry per granted relation → the only
--      privileges the role actually has are the ones named in
--      `okg.grant_registry`, applied once at the end of this file
--      by `okg.apply_grant_registry()`.
--
-- Layer 3 used to be ~40 hand-written GRANT lines here PLUS a loop
-- that granted app_ro SELECT on every view whose NAME matched
-- `v\_%`. `schema-grant-coherence` deleted both. The name-matching
-- loop meant any migration- or deployment-created view called
-- `v_something` silently joined the published read surface, which
-- reduced an audited allow-list to a naming convention; the
-- hand-written list could not be read by `role_split.sql`, which is
-- why the two scripts did not commute. The registry is one table
-- both scripts apply.
--
-- The CI test `tests/sql/roles/test_app_ro_default_deny.py`
-- asserts the contract: app_ro reads registered views and nothing
-- else in schema okg.

-- Layer 1: revoke any accumulated privileges.
--
-- The table/sequence revokes are ARMED BY THE REGISTRY. Only the registry puts
-- these grants back, and it is created by schema.sql / migration 0027 --- so on
-- a database that has neither, `install_views` (which replays this file WITHOUT
-- running migrations) would revoke the read surface and have nothing to restore
-- it. Guarding on the table's presence makes that path a no-op instead of an
-- outage; the next migrate creates the registry and converges normally.
-- Presence, not row count: on a fresh install schema.sql creates the table
-- empty and the seed further down this same file populates it.
DO $okg_app_ro_revoke$
BEGIN
  IF to_regclass('okg.grant_registry') IS NOT NULL THEN
    REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM app_ro;
    REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg FROM app_ro;
  END IF;
END
$okg_app_ro_revoke$;
-- Function EXECUTE is not registry-managed; this file re-grants what app_ro
-- needs unconditionally a few lines below, so it is safe to revoke always.
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA okg FROM app_ro;

-- Layer 2: future tables default to inaccessible. The grants
-- below are issued by *the role currently running migrate*
-- (postgres, by default); ALTER DEFAULT PRIVILEGES sets the
-- default for objects created by that role.
ALTER DEFAULT PRIVILEGES IN SCHEMA okg
  REVOKE ALL ON TABLES FROM app_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA okg
  REVOKE ALL ON SEQUENCES FROM app_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA okg
  REVOKE ALL ON FUNCTIONS FROM app_ro;

-- Layer 3: the relation allow-list is DATA. The app_ro rows are seeded into
-- `okg.grant_registry` and applied at the end of this file; see the
-- `okg_grant_registry_seed` block for the names that used to be written out
-- here as GRANT statements.
--
-- Schema USAGE and function EXECUTE stay statements: the registry governs
-- TABLE and SEQUENCE privileges, which is where the order-dependence and the
-- naming-convention widening both lived. Function EXECUTE grants are already
-- explicit and are re-asserted below their blanket grant within this file, so
-- they commute on their own.
GRANT USAGE ON SCHEMA okg TO app_ro;
GRANT EXECUTE ON FUNCTION okg.current_pin() TO app_ro;
GRANT EXECUTE ON FUNCTION okg.require_graph_checkout(text) TO app_ro;
GRANT EXECUTE ON FUNCTION okg.runtime_queue_health(integer) TO app_ro;
GRANT EXECUTE ON FUNCTION okg.source_frontier_active_workflows() TO app_ro;
GRANT EXECUTE ON FUNCTION okg.source_frontier_state() TO app_ro;
-- History-compaction read-guard functions are defined in schema.sql (and the
-- 0008/0009/0012 migrations), NOT in this file. `install_views()` deliberately
-- replays only this file so operators can refresh views on a live Timescale
-- database where unrelated schema DDL is blocked by compressed chunks. An
-- unguarded GRANT here would abort that whole refresh with "function does not
-- exist" on any database that has not yet taken those migrations, shutting the
-- one recovery path that exists when the full migrate is blocked. Guard on
-- presence, matching the discipline schema.sql already uses for these grants.
DO $$
BEGIN
  IF to_regprocedure('okg.acquire_history_generation_read_lease(TEXT, TEXT, TEXT, TEXT)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION okg.acquire_history_generation_read_lease(TEXT, TEXT, TEXT, TEXT) TO app_ro;
  END IF;
  IF to_regprocedure('okg.release_history_generation_read_lease(TEXT, TEXT, TEXT)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION okg.release_history_generation_read_lease(TEXT, TEXT, TEXT) TO app_ro;
  END IF;
  IF to_regprocedure('okg.compacted_generation_record(TEXT, TEXT)') IS NOT NULL THEN
    GRANT EXECUTE ON FUNCTION okg.compacted_generation_record(TEXT, TEXT) TO app_ro;
  END IF;
END
$$;

-- `okg.v_generation_interval_stab` — TEXT-ref-keyed read surface over the
-- generation-ordinal registry (D2 amendment (a), database-schema.md §3.2).
--
-- This view's position used to be load-bearing. When app_ro grants came from a
-- sweep over "whatever `okg.v_*` views exist AT THAT MOMENT", creating a view
-- after the sweep left it ungranted for a whole pass; a fresh install only
-- looked correct because views.sql gets replayed. The view-convergence pass
-- (PR #1109) re-runs these scripts ONCE after dropping a frozen-shape view,
-- which surfaced that latent ordering bug as an upgraded database whose read
-- layer had silently lost a view a fresh install has.
--
-- `schema-grant-coherence` removed the hazard rather than the symptom: grants
-- are now applied ONCE, at the very end of this file, after every CREATE VIEW
-- in it. Where a view is created relative to another view no longer affects
-- what it is granted.
--
-- Still guarded on the TABLE's presence: okg.generation_ordinals is not in
-- schema.sql, so on a database that has not yet materialized the registry
-- there is nothing to select from. That is a relation guard, not a role guard.
DO $create_generation_interval_stab$
BEGIN
  IF to_regclass('okg.generation_ordinals') IS NOT NULL THEN
    EXECUTE $stab_view$
      CREATE OR REPLACE VIEW okg.v_generation_interval_stab AS
      SELECT
        checkpoint_id          AS generation_ref,
        branch_id              AS chronos_branch_id,
        ordinal                AS stab_ordinal,
        previous_checkpoint_id AS previous_generation_ref,
        deployment_name
      FROM okg.generation_ordinals
    $stab_view$;
    COMMENT ON VIEW okg.v_generation_interval_stab IS
      'TEXT-ref-keyed read surface over the generation-ordinal registry (D2). '
      'Look rows up by generation_ref; stab_ordinal is the interval-stab '
      'coordinate, not a generation identity.';
  END IF;
END
$create_generation_interval_stab$;

-- The `v\_%` auto-grant loop that stood here is GONE
-- (`schema-grant-coherence` req.registry-not-convention). It granted app_ro
-- SELECT on every view whose NAME matched a pattern, so a migration- or
-- deployment-authored view called `v_anything` joined the published read
-- surface without anyone deciding that it should. Deployment views are still
-- grantable — they register rows in `okg.grant_registry` with a `source` other
-- than 'substrate', which the substrate seed never touches — and a `v_`-named
-- view with NO registry row is now both unreadable by app_ro and named by
-- `okg.grant_registry_drift()`.

-- ========== MCP call observation log ==========
--
-- Per `improve-mcp-tool-surface` (2026-04-27, task 1.2 + 1.3):
-- one row per MCP tool invocation. The leaner cousin of
-- `okg.mcp_trace_events` (which captures full request/response
-- JSON for the trace-replacement contract). This table is
-- agent-self-introspectable via `okg.v_mcp_calls` + `run_sql`,
-- and operator-queryable for vocabulary-evolution decisions
-- ("which tools earn their keep, which patterns recur, which
-- composed verbs deserve to ship").
--
-- TimescaleDB hypertable (chunk_time_interval = 7 days)
-- subsumes both "weekly partitions" and "daily retention cron"
-- from the proposal: hypertable IS partitioned-by-time
-- internally; `add_retention_policy` is the cron Timescale
-- manages for us. Functionally identical to native PG
-- declarative partitioning + a hand-rolled DROP cron, but
-- consistent with the substrate's other time-series tables
-- (mcp_trace_events, node_facts, edge_facts).
--
-- Default retention: 90 days, configurable via
-- `add_retention_policy` (operator override only — agent
-- queries don't tune retention).

DROP VIEW IF EXISTS okg.v_mcp_operator_costs;
DROP VIEW IF EXISTS okg.v_mcp_call_costs;
DROP VIEW IF EXISTS okg.v_mcp_calls;

CREATE TABLE IF NOT EXISTS okg.mcp_call_log (
  call_id      BIGSERIAL,
  ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
  session_id   TEXT        NOT NULL,
  deployment_name TEXT     NOT NULL DEFAULT 'default',
  tool_name    TEXT        NOT NULL,
  operator_name TEXT,
  replacement_operator TEXT,
  args         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  cost         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  pin          TEXT,
  elapsed_ms   INTEGER,
  result_count INTEGER,
  error_code   TEXT,
  PRIMARY KEY (ts, call_id)
);

-- Compatibility columns added by MCP-observability work. The
-- DEFAULT clauses backfill historical rows so existing operator
-- dashboards keep working; the NOT NULL on deployment_name +
-- cost matches the contract our single writer (log_call_to_log
-- in tracing.py) holds, and prevents NULL values that would
-- silently break per-deployment / per-cost analytics. The
-- view contract is additive: CREATE OR REPLACE VIEW cannot
-- drop columns on upgraded databases.
ALTER TABLE okg.mcp_call_log
  ADD COLUMN IF NOT EXISTS deployment_name TEXT NOT NULL DEFAULT 'default',
  ADD COLUMN IF NOT EXISTS operator_name TEXT,
  ADD COLUMN IF NOT EXISTS replacement_operator TEXT,
  ADD COLUMN IF NOT EXISTS cost JSONB NOT NULL DEFAULT '{}'::jsonb;

-- Guard the pin->TEXT conversion exactly as schema.sql guards the
-- mcp_trace_events generation_id->TEXT conversion
-- (fix-migrate-compressed-hypertable-idempotency): ALTER COLUMN ... TYPE forces
-- a table rewrite, which TimescaleDB rejects on a hypertable that already has
-- compressed chunks ("operation not supported on hypertables with compressed
-- chunks"). Fresh DBs CREATE pin as TEXT (above), so this ALTER only converts
-- ancient DBs where it was another type. Because views.sql is re-executed
-- wholesale on every migrate (the idempotency contract), an unconditional ALTER
-- broke `okg migrate` on any aged deployment once the 7-day compression policy
-- had compressed an mcp_call_log chunk. Skipping when pin is already TEXT issues
-- zero rewrite DDL on every current-schema DB. (That guard audit covered only
-- schema.sql, so this views.sql site was missed.)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
      FROM information_schema.columns
     WHERE table_schema = 'okg'
       AND table_name   = 'mcp_call_log'
       AND column_name  = 'pin'
       AND data_type   <> 'text'
  ) THEN
    ALTER TABLE okg.mcp_call_log
      ALTER COLUMN pin TYPE TEXT USING pin::text;
  END IF;
END $$;

SELECT create_hypertable(
  'okg.mcp_call_log', 'ts',
  chunk_time_interval => INTERVAL '7 days',
  if_not_exists       => TRUE,
  migrate_data        => TRUE
);

-- Tool-name + time-range queries are the dominant pattern
-- (operators asking "what's run_sql doing this week?",
-- agents asking "have I called this tool recently?").
CREATE INDEX IF NOT EXISTS mcp_call_log_tool_ts_idx
  ON okg.mcp_call_log (tool_name, ts DESC);
CREATE INDEX IF NOT EXISTS mcp_call_log_session_ts_idx
  ON okg.mcp_call_log (session_id, ts DESC);
CREATE INDEX IF NOT EXISTS mcp_call_log_deployment_operator_ts_idx
  ON okg.mcp_call_log (deployment_name, operator_name, ts DESC);
CREATE INDEX IF NOT EXISTS mcp_call_log_operator_ts_idx
  ON okg.mcp_call_log (operator_name, ts DESC);
-- Error-rate queries: filter on error_code IS NOT NULL.
CREATE INDEX IF NOT EXISTS mcp_call_log_error_ts_idx
  ON okg.mcp_call_log (ts DESC)
  WHERE error_code IS NOT NULL;

-- Compression after 7 days (one chunk's worth) — keeps
-- recent-week queries on uncompressed chunks (low latency)
-- and drops storage cost for the historical tail.
ALTER TABLE okg.mcp_call_log SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'tool_name',
  timescaledb.compress_orderby   = 'ts DESC'
);

SELECT add_compression_policy(
  'okg.mcp_call_log', INTERVAL '7 days', if_not_exists => TRUE
);

-- Retention: drop chunks older than 90 days. TimescaleDB
-- runs the policy on a daily background job — that IS the
-- "daily retention cron" the proposal asks for.
SELECT add_retention_policy(
  'okg.mcp_call_log', INTERVAL '90 days', if_not_exists => TRUE
);

-- `okg.v_mcp_calls` — agent-facing view over the log. Same
-- shape as the underlying table; the view exists so the
-- run_sql allow-list (which permits `okg.v_*` only) can
-- reach the log. No pin filter — the log is operator/agent
-- self-introspection, not a graph query, so it's
-- generation-agnostic.
--
-- Drop before recreate because this view's contract gained
-- deployment/operator columns after the original shape shipped.
-- PostgreSQL will not let CREATE OR REPLACE VIEW rename or insert
-- columns ahead of existing columns on an already-migrated database.
CREATE OR REPLACE VIEW okg.v_mcp_calls AS
SELECT
  call_id,
  ts,
  session_id,
  deployment_name,
  tool_name,
  operator_name,
  replacement_operator,
  args,
  cost,
  pin,
  elapsed_ms,
  result_count,
  error_code
FROM okg.mcp_call_log;

-- `okg.v_mcp_call_costs` — typed per-call cost columns projected from
-- the JSON cost vector. Agents can sort/filter without repeatedly writing
-- JSON casts against `okg.v_mcp_calls`.
CREATE OR REPLACE VIEW okg.v_mcp_call_costs AS
SELECT
  call_id,
  ts,
  session_id,
  deployment_name,
  tool_name,
  COALESCE(operator_name, tool_name) AS operator_name,
  replacement_operator,
  pin,
  elapsed_ms,
  result_count,
  error_code,
  CASE WHEN cost->>'args_bytes' ~ '^-?[0-9]+$'
       THEN (cost->>'args_bytes')::bigint END AS args_bytes,
  CASE WHEN cost->>'response_bytes' ~ '^-?[0-9]+$'
       THEN (cost->>'response_bytes')::bigint END AS response_bytes,
  CASE WHEN cost->>'args_token_estimate' ~ '^-?[0-9]+$'
       THEN (cost->>'args_token_estimate')::bigint END AS args_token_estimate,
  CASE WHEN cost->>'response_token_estimate' ~ '^-?[0-9]+$'
       THEN (cost->>'response_token_estimate')::bigint END AS response_token_estimate,
  CASE
    WHEN lower(cost->>'truncated') IN ('true', 't', '1', 'yes') THEN true
    WHEN lower(cost->>'truncated') IN ('false', 'f', '0', 'no') THEN false
    ELSE false
  END AS truncated,
  CASE WHEN cost->>'row_count' ~ '^-?[0-9]+$'
       THEN (cost->>'row_count')::bigint END AS row_count,
  cost->>'query_plan_summary' AS query_plan_summary
FROM okg.v_mcp_calls;

-- `okg.v_mcp_operator_costs` — compact aggregate used for context-budget
-- regression checks and operator summaries.
CREATE OR REPLACE VIEW okg.v_mcp_operator_costs AS
SELECT
  deployment_name,
  operator_name,
  count(*)::bigint AS calls,
  COALESCE(sum(response_bytes), 0)::bigint AS response_bytes,
  COALESCE(sum(args_bytes), 0)::bigint AS args_bytes,
  count(*) FILTER (WHERE truncated)::bigint AS truncated_calls,
  count(*) FILTER (WHERE error_code IS NOT NULL)::bigint AS error_count,
  percentile_disc(0.50) WITHIN GROUP (ORDER BY elapsed_ms) AS p50_ms,
  percentile_disc(0.90) WITHIN GROUP (ORDER BY elapsed_ms) AS p90_ms,
  max(response_bytes) AS max_response_bytes,
  max(ts) AS last_called_at
FROM okg.v_mcp_call_costs
GROUP BY deployment_name, operator_name;

-- Permissions:
-- - app_rw INSERT (the MCP server's logging wrapper writes here). app_rw is
--   not a governed read-surface role, so it keeps direct statements.
-- - app_ro SELECT on the three views: registry rows, applied at end of file.
GRANT INSERT, SELECT ON okg.mcp_call_log TO app_rw;
GRANT USAGE, SELECT ON SEQUENCE okg.mcp_call_log_call_id_seq TO app_rw;
-- Graph IR ledger views. Raw ledger tables remain app_rw-only; read-side
-- clients inspect graph-observable IR evidence through okg.v_*.
CREATE OR REPLACE VIEW okg.v_ir_programs AS
SELECT
  program_id,
  program_checksum,
  ir_version,
  program_kind,
  registry_id,
  registry_version,
  registry_checksum,
  effect_summary,
  determinism,
  op_counts,
  function_counts,
  cardinality_summary,
  payload_mode,
  program_json,
  program_ref,
  created_at
FROM okg.ir_programs;

CREATE OR REPLACE VIEW okg.v_ir_program_bindings AS
SELECT
  binding_id,
  program_id,
  deployment_name,
  owner_kind,
  owner_id,
  program_role,
  generation_id,
  branch_name,
  metadata,
  created_at
FROM okg.ir_program_bindings;

CREATE OR REPLACE VIEW okg.v_ir_invocations AS
SELECT
  invocation_id,
  program_id,
  binding_id,
  deployment_name,
  invocation_kind,
  branch_name,
  generation_id,
  status,
  verdict,
  reason,
  started_at,
  completed_at,
  result_count,
  node_delta_count,
  edge_delta_count,
  cost_proxy,
  quality_score,
  error_count,
  truncation_count,
  metrics,
  created_at
FROM okg.ir_invocations;

-- app_ro SELECT on the three IR views: registry rows, applied at end of file.

-- ========== okg_mcp: least-privilege MCP read-pool login role ==========
--
-- Per `least-privilege-mcp` (P1 of secure-academic-pi). The MCP server's
-- connection pool connects as THIS role instead of the deployment owner /
-- superuser. Posture: read-everything-the-read-surface-needs, no DDL, NOT a
-- superuser, and write only the registry-listed runtime records: the MCP
-- audit and telemetry rows, operator call samples (append-only) and their
-- retention watermark, the Nomos decision/claim upserts, and the external
-- probe cache (an overwritten cache, not an audit log). Never graph facts.
-- Its own connection reservation goes through three SECURITY DEFINER
-- functions (okg.connection_reservation_*, defined below), never a table grant.
--
-- Read scope is explicit in okg: graph checkout tables, catalog tables used
-- for resolution, generation/source status, and the audit rows the read pool
-- appends. Chronos internals live in public._chronos* tables and get
-- explicit catch-up grants because Chronos creates physical branch/checkpoint
-- tables lazily at publish time.
--
-- run_sql still does `SET LOCAL ROLE app_ro` for its non-graph path;
-- okg_mcp is granted app_ro so that downgrade works from this login role.
DO $okg_mcp_role$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    -- Managed-host safe default: grants can be applied to this role, but it
    -- cannot be used for login until an operator creates or alters a managed
    -- login and supplies OKG_MCP_RO_DSN. Local compose initdb creates the
    -- LOGIN role with the loopback-only development password before migrate.
    CREATE ROLE okg_mcp NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
END
$okg_mcp_role$;

-- Rotating-floor partial indexes are provable from a pinned branch-point
-- parameter only while PostgreSQL plans with that execution's value. Scope
-- this to read-side roles; never apply it to the deployment owner / shared
-- publisher role.
ALTER ROLE okg_mcp SET plan_cache_mode = force_custom_plan;
DO $okg_projection_custom_plans$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'projection_remediator') THEN
    ALTER ROLE projection_remediator SET plan_cache_mode = force_custom_plan;
  END IF;
END
$okg_projection_custom_plans$;

-- The owner role manages / can impersonate okg_mcp; okg_mcp inherits
-- app_ro so run_sql's SET LOCAL ROLE app_ro downgrade still resolves.
DO $okg_mcp_owner_grant$
BEGIN
  EXECUTE format('GRANT okg_mcp TO %I', current_user);
END
$okg_mcp_owner_grant$;
GRANT app_ro TO okg_mcp;

GRANT USAGE ON SCHEMA okg TO okg_mcp;
GRANT USAGE ON SCHEMA public TO okg_mcp;

-- Converge away from earlier blanket grants before adding the intended
-- direct read set. Armed by the registry for the same reason as the app_ro
-- block above: only okg.apply_grant_registry() puts these back, so a database
-- without the registry table must not have them taken away.
DO $okg_mcp_revoke$
BEGIN
  IF to_regclass('okg.grant_registry') IS NOT NULL THEN
    REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp;
    REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg FROM okg_mcp;
    REVOKE ALL ON ALL TABLES IN SCHEMA public FROM okg_mcp;
  END IF;
END
$okg_mcp_revoke$;
ALTER DEFAULT PRIVILEGES IN SCHEMA okg REVOKE SELECT ON TABLES FROM okg_mcp;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM okg_mcp;

-- The okg_mcp read allow-list is DATA. Every relation that used to be named in
-- a `GRANT SELECT ON ... TO okg_mcp` statement here is now a row in
-- `okg.grant_registry`, seeded and applied at the end of this file. Two things
-- fall out of that move:
--
--   * `role_split.sql` can apply the SAME list. It could not read a GRANT
--     statement, which is why it carried a blanket
--     `GRANT SELECT ON ALL TABLES IN SCHEMA okg TO okg_mcp` catch-up that
--     fought the REVOKE above — whichever script ran last decided whether the
--     MCP role could read raw fact tables, source blobs and watermarks.
--   * The lazily-created relations (`okg.attribute_effective_view_intervals`,
--     `okg.attribute_versions`,
--     `okg.v_generation_interval_stab`) need no hand-written existence guard.
--     The applier grants a registry row only when its relation exists and
--     re-applies on the next install, which is the same "grant WHEN PRESENT"
--     behaviour the guarded DO-block gave, expressed once.
--
-- D2 amendment (a), database-schema.md §3.2: okg_mcp's direct grant on
-- okg.generation_ordinals is REVOKED, and there is deliberately NO registry
-- row for it. The MCP sidecar reaches the registry through
-- okg.v_generation_interval_stab, which is keyed on the TEXT checkpoint ref --
-- you look a row up BY the ref, never by the ordinal. The ordinal is projected
-- as `stab_ordinal`: a physical interval coordinate the half-open stab
-- predicate needs, deliberately NOT named as a generation reference, so it can
-- never be mistaken for generation identity. The explicit REVOKE stays, to
-- converge a deployment that was granted before this landed.
DO $okg_mcp_ordinal_revoke$
BEGIN
  IF to_regclass('okg.generation_ordinals') IS NOT NULL THEN
    REVOKE ALL ON okg.generation_ordinals FROM okg_mcp;
  END IF;
END
$okg_mcp_ordinal_revoke$;

-- Default privileges are PER-CREATING-ROLE. Chronos branch/checkpoint
-- tables are created lazily AT RUNTIME by whichever role runs ingest/publish.
-- Do NOT grant public default privileges here: that would expose arbitrary
-- future public tables. The Chronos graph-store ensure/write path grants
-- okg_mcp SELECT on concrete public._chronos* tables after creating them.
DO $okg_mcp_rw_default_privs$
DECLARE
  writer text;
BEGIN
  FOREACH writer IN ARRAY ARRAY['app_rw', 'okg_substrate'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = writer) THEN
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA okg '
        'REVOKE SELECT ON TABLES FROM okg_mcp', writer);
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
        'REVOKE SELECT ON TABLES FROM okg_mcp', writer);
    END IF;
  END LOOP;
END
$okg_mcp_rw_default_privs$;

-- Read functions the tools call (current_pin, fuzzy_resolve_alias, ...).
-- The read-side SECURITY DEFINER exception is runtime_queue_health(integer),
-- which has a fixed search_path and only reads OKG/DBOS runtime state so
-- app_ro can inspect the registered view without table grants. Other function
-- EXECUTE grants confer no write
-- escalation: mutating functions run as okg_mcp and are blocked by the
-- absent table-write grants.
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA okg TO okg_mcp;
ALTER DEFAULT PRIVILEGES IN SCHEMA okg GRANT EXECUTE ON FUNCTIONS TO okg_mcp;

-- Transition functions mutate Publisher integrity authority and are therefore
-- not read-pool APIs.  Keep this deny-list immediately after the broad read
-- function grant; migration 0014 and the runtime schema ensure repeat it after
-- function creation because default privileges are materialized at CREATE.
DO $okg_mcp_attribute_transition_denies$
BEGIN
  IF to_regprocedure(
    'okg.begin_attribute_integrity_transition(text,text,text)'
  ) IS NOT NULL THEN
    REVOKE ALL ON FUNCTION
      okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT),
      okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT),
      okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT),
      okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT)
      FROM okg_mcp;
  END IF;
  IF to_regprocedure(
    'okg.register_attribute_integrity_scope(text,text,text)'
  ) IS NOT NULL THEN
    REVOKE ALL ON FUNCTION
      okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT),
      okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT),
      okg.finalize_attribute_integrity_receipt(
        TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT,
        BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT,
        NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT
      )
      FROM okg_mcp;
    -- app_rw is not a transition/finalizer principal either
    -- (harden-attribute-integrity-authority): reassert after any broad
    -- grant replay.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
      REVOKE ALL ON FUNCTION
        okg.begin_attribute_integrity_transition(TEXT, TEXT, TEXT),
        okg.attribute_integrity_transition_matches(TEXT, TEXT, TEXT),
        okg.finish_attribute_integrity_transition(TEXT, TEXT, TEXT),
        okg.abort_attribute_integrity_transition(TEXT, TEXT, TEXT),
        okg.register_attribute_integrity_scope(TEXT, TEXT, TEXT),
        okg.mark_attribute_integrity_scope_dirty(TEXT, TEXT, TEXT, TEXT),
        okg.finalize_attribute_integrity_receipt(
          TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT,
          BIGINT, BIGINT, BIGINT, BIGINT, INTEGER, TEXT, TEXT,
          NUMERIC, NUMERIC, NUMERIC, NUMERIC, TEXT, BIGINT
        )
        FROM app_rw;
    END IF;
  END IF;
END
$okg_mcp_attribute_transition_denies$;

-- The writes the read pool legitimately performs -- per-call audit logging
-- (mcp_call_log / mcp_trace_events), bounded unified telemetry rows for MCP
-- spans, and the two Nomos read-time authorization audit tables -- are
-- registry rows like everything else okg_mcp holds. They were three guarded
-- DO-blocks of near-identical `IF EXISTS ... THEN EXECUTE 'GRANT ...'`; the
-- applier does the existence guard once. See the `okg_grant_registry_seed`
-- block below for the table list and the reason each one is granted.
--
-- These are audit APPENDS, not graph mutation: nothing here lets the read pool
-- write a fact.

-- =====================================================================
-- The grant registry (`schema-grant-coherence`)
-- =====================================================================
--
-- Everything below runs LAST in this file, after every CREATE VIEW and every
-- role-scoped REVOKE above it. That position is the whole point: a view's
-- grant no longer depends on where the view was created, and the schema-wide
-- REVOKEs above can converge a drifted database without stripping a privilege
-- that nothing puts back.
--
-- `okg.apply_grant_registry()` is the ONLY thing in the substrate that issues
-- app_ro / okg_mcp table and sequence grants. `runtime/sql/role_split.sql`
-- calls the same function. That is what makes the two scripts commute: they no
-- longer hold competing opinions, they read one table.
--
-- The tables are authored in schema.sql and converged by migration
-- 0027_grant_registry, so on a database that predates both, every block here
-- is guarded and skips.

-- ---------------------------------------------------------------------
-- The applier.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION okg.apply_grant_registry()
RETURNS integer
LANGUAGE plpgsql
AS $okg_apply_grant_registry$
DECLARE
  entry       record;
  target      text;
  privilege   text;
  stmt        text;
  stmts       text[] := '{}'::text[];
  applied     integer := 0;
  contended   boolean;
  denied      boolean;
  last_error  text;
  queue_ready boolean := to_regclass('okg.pending_grants') IS NOT NULL;
BEGIN
  IF to_regclass('okg.grant_registry') IS NULL THEN
    -- A database that has not taken 0027 yet. Nothing to apply; the next
    -- migrate installs the table and re-runs this file.
    RETURN 0;
  END IF;

  -- Statements are COLLECTED first and applied through one handler below.
  -- Two reasons, both learned the hard way:
  --
  --   ORDER IS LOCK ORDER. Every GRANT takes a row lock on the target
  --   relation's pg_class tuple. A deployment bootstrap running role_split.sql
  --   can race a migrate, and the ingest layer runs several sources as separate
  --   PROCESSES. Two sessions issuing these grants in different orders would
  --   deadlock rather than serialise, so the registry is walked in one fixed
  --   order (ORDER BY entry_ref) and the collected list preserves it.
  --
  --   A LOST GRANT IS WORSE THAN A SLOW ONE. Even in a fixed order, a
  --   concurrent catalog writer makes a GRANT raise `tuple concurrently
  --   updated` (XX000). Raising out of here fails the whole bootstrap;
  --   swallowing drops a privilege silently. Same discipline as the Chronos
  --   catch-up (graph_store_internal/schema.py): bounded retry, then a durable
  --   row in okg.pending_grants that doctor reports.
  FOR entry IN
    SELECT g.entry_ref, g.relation, g.object_kind, g.role_name,
           g.privileges, g.is_pattern
      FROM okg.grant_registry g
     WHERE g.classification = 'contract'
     ORDER BY g.entry_ref
  LOOP
    -- Roles are cluster-level and provisioned separately (app_rw and
    -- okg_substrate may be absent on a single-role developer database), so a
    -- missing grantee is a skip, not an error.
    IF NOT EXISTS (
      SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = entry.role_name
    ) THEN
      CONTINUE;
    END IF;

    -- The privilege list is DATA, and a deployment can insert rows, so it is
    -- validated against the real privilege vocabulary rather than interpolated
    -- on trust. An unknown word is an authoring error and stops the install
    -- loudly instead of producing a syntax error at some later GRANT.
    FOREACH privilege IN ARRAY
      string_to_array(replace(entry.privileges, ' ', ''), ',')
    LOOP
      IF privilege NOT IN (
        'SELECT', 'INSERT', 'UPDATE', 'DELETE',
        'TRUNCATE', 'REFERENCES', 'TRIGGER', 'USAGE'
      ) THEN
        RAISE EXCEPTION
          'okg.grant_registry entry % names unknown privilege %',
          entry.entry_ref, privilege;
      END IF;
    END LOOP;

    IF entry.is_pattern THEN
      -- Chronos creates its physical `public._chronos*` tables lazily at
      -- publish time, so they cannot be enumerated by name at install. The
      -- pattern is the enumeration, and it is bounded by an explicit prefix --
      -- NOT the blanket "all tables in schema public" this replaces.
      FOR target IN
        SELECT pg_catalog.quote_ident(n.nspname) || '.'
               || pg_catalog.quote_ident(c.relname)
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = split_part(entry.relation, '.', 1)
           AND c.relname LIKE split_part(entry.relation, '.', 2) ESCAPE '\'
           AND (
             (entry.object_kind = 'SEQUENCE' AND c.relkind = 'S')
             OR (entry.object_kind = 'TABLE'
                 AND c.relkind IN ('r', 'p', 'v', 'm', 'f'))
           )
         ORDER BY 1
      LOOP
        stmts := stmts || format(
          'GRANT %s ON %s %s TO %I',
          entry.privileges, entry.object_kind, target, entry.role_name
        );
      END LOOP;
    ELSE
      -- to_regclass both guards existence and yields the canonical, correctly
      -- quoted identifier, so the registry's `relation` text is never
      -- interpolated into DDL as written. A relation that does not exist yet
      -- (created lazily at first publish, or belonging to a subsystem this
      -- deployment does not run) is a skip: the next install applies it.
      target := to_regclass(entry.relation)::text;
      IF target IS NULL THEN
        CONTINUE;
      END IF;
      stmts := stmts || format(
        'GRANT %s ON %s %s TO %I',
        entry.privileges, entry.object_kind, target, entry.role_name
      );
    END IF;
  END LOOP;

  -- ONE handler, reached by every grant.
  FOREACH stmt IN ARRAY stmts LOOP
    contended := false;
    denied := false;
    last_error := NULL;
    FOR attempt IN 1..3 LOOP
      BEGIN
        EXECUTE stmt;
        contended := false;
        denied := false;
        applied := applied + 1;
        EXIT;
      EXCEPTION
        -- The relation or role vanished between the catalog scan and the
        -- GRANT. Genuinely nothing to do, and nothing to retry.
        WHEN undefined_table OR undefined_object THEN
          EXIT;
        -- NOT the same thing, and it used to share the branch above. "This
        -- session may not grant that" is a real failure: the privilege the
        -- registry promises is absent afterwards, and swallowing it makes a
        -- zero-grant pass look like a successful one. Before this PACT the
        -- equivalent statements were bare GRANTs that RAISED.
        WHEN insufficient_privilege THEN
          denied := true;
          last_error := SQLERRM;
          EXIT;
        WHEN internal_error THEN
          -- `internal_error` is the XX000 CATEGORY code, so it also matches
          -- XX001 data_corrupted and XX002 index_corrupted. Only the
          -- concurrency error is benign; corruption stays loud, every attempt.
          IF SQLSTATE <> 'XX000' THEN
            RAISE;
          END IF;
          -- "tuple concurrently updated": a competing session updated this
          -- relation's pg_class row. A catalog update waits for the competing
          -- transaction, so by now it has committed and the next attempt sees
          -- the new tuple.
          contended := true;
          last_error := SQLERRM;
      END;
    END LOOP;

    -- Queue write is OUTSIDE the exception block: a PL/pgSQL BEGIN...EXCEPTION
    -- is a subtransaction, so bookkeeping that failed inside it would roll back
    -- the GRANT it was recording.
    IF contended OR denied THEN
      RAISE WARNING USING MESSAGE =
        'okg: grant registry could not apply, queued for retry ('
        || CASE WHEN denied THEN 'insufficient privilege'
                ELSE 'concurrent-update retries exhausted' END
        || '): ' || stmt;
      IF queue_ready THEN
        INSERT INTO okg.pending_grants (statement, last_error)
        VALUES (stmt, last_error)
        ON CONFLICT (statement) DO UPDATE
          SET attempts        = okg.pending_grants.attempts + 1,
              last_attempt_at = now(),
              last_error      = EXCLUDED.last_error;
      END IF;
    ELSIF queue_ready THEN
      -- Applied, or moot because the relation/role vanished. Only those two
      -- paths reach here now; a denial no longer dequeues itself.
      DELETE FROM okg.pending_grants WHERE statement = stmt;
    END IF;
  END LOOP;

  -- REFUSE A VACUOUS PASS. Both callers do `PERFORM okg.apply_grant_registry()`
  -- and discard the count, so without this a run that granted NOTHING -- wrong
  -- session role, registry present but every statement denied -- would be
  -- indistinguishable from a successful one, and the schema-wide REVOKEs that
  -- ran just above it would stand unopposed. That is the default-value-as-
  -- fallback shape this repo treats as a P0. Before this PACT the equivalent
  -- statements were bare GRANTs, and a session that could not issue them
  -- failed the whole install loudly; keep that property.
  IF applied = 0 AND EXISTS (
    SELECT 1 FROM okg.grant_registry WHERE classification = 'contract'
  ) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'okg.apply_grant_registry() applied 0 of '
        || (SELECT count(*) FROM okg.grant_registry
             WHERE classification = 'contract')::text
        || ' registered grants',
      HINT = 'the session running this script cannot GRANT on the registered '
             'relations (it must own them or hold GRANT OPTION); the '
             'schema-wide REVOKEs above have already run, so leaving this '
             'silent would strip the read roles. Re-run migrate as the '
             'database owner.',
      ERRCODE = 'insufficient_privilege';
  END IF;

  RETURN applied;
END
$okg_apply_grant_registry$;

COMMENT ON FUNCTION okg.apply_grant_registry() IS
  'Apply okg.grant_registry. The only substrate path that grants app_ro / '
  'okg_mcp table and sequence privileges; called by db/views.sql and by '
  'runtime/sql/role_split.sql so the two commute (schema-grant-coherence).';

-- ---------------------------------------------------------------------
-- The drift check. Loud, not silent.
-- ---------------------------------------------------------------------
--
-- Four findings, each a widening or a narrowing nobody decided:
--
--   unregistered_view   an `okg.v_*` view with no registry entry at all. Under
--                       the old wildcard loop this view would have been
--                       granted to app_ro by NAME; now it is unreadable, and
--                       named here so the omission cannot pass as a decision.
--   unregistered_grant  a governed role directly holds a privilege the
--                       registry does not list. Inherited privileges are NOT
--                       counted -- okg_mcp is a member of app_ro, so
--                       has_table_privilege would report every app_ro view as
--                       an okg_mcp grant. This walks the relation ACL.
--   internal_exposed    a relation classified `internal` became readable by a
--                       governed role.
--   missing_grant       a registry entry whose relation and role both exist
--                       but whose privilege is absent, i.e. something revoked
--                       it after the apply.
CREATE OR REPLACE FUNCTION okg.grant_registry_drift()
RETURNS TABLE (
  finding_kind text,
  relation     text,
  role_name    text,
  detail       text
)
LANGUAGE sql
STABLE
AS $okg_grant_registry_drift$
  WITH governed AS (
    SELECT unnest(ARRAY['app_ro', 'okg_mcp']) AS rolname
  ),
  -- API view grants are also checked for absence, but they are deliberately
  -- kept out of the legacy app_ro/okg_mcp effective-grant sweep below.  That
  -- preserves the two read-pool diagnostics while still catching a direct
  -- API ACL drift or a missing registered API view grant.
  governed_surface AS (
    SELECT rolname FROM governed
    UNION ALL
    SELECT 'okg_external_action_api'::text
  ),
  -- Relations in scope. TimescaleDB internal chunk relations are excluded:
  -- they are not directly queryable and inherit their hypertable's ACL.
  scoped AS (
    SELECT c.oid,
           n.nspname,
           c.relname,
           c.relkind,
           n.nspname || '.' || c.relname AS qualified,
           c.relacl
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname IN ('okg', 'public')
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
       AND c.relname NOT LIKE '\_hyper\_%' ESCAPE '\'
       AND c.relname NOT LIKE '\_compressed\_hypertable\_%' ESCAPE '\'
  ),
  acl_grants AS (
    SELECT s.qualified,
           s.relkind,
           acl.grantee              AS grantee_oid,
           coalesce(r.rolname, 'PUBLIC') AS source_grantee,
           acl.privilege_type::text AS privilege_type
      FROM scoped s
      CROSS JOIN LATERAL aclexplode(s.relacl) acl
      LEFT JOIN pg_catalog.pg_roles r ON r.oid = acl.grantee
  ),
  direct_grants AS (
    SELECT a.qualified,
           a.relkind,
           a.source_grantee AS grantee,
           a.privilege_type
      FROM acl_grants a
     WHERE a.source_grantee IN (SELECT rolname FROM governed_surface)
  ),
  -- PUBLIC and inherited-role ACLs can expose a relation without placing a
  -- direct ACL row on either governed role. They are still part of the
  -- effective security boundary, and no registry row may silently authorize
  -- them under some other grantee name.
  inherited_or_public_grants AS (
    SELECT DISTINCT a.qualified,
           a.relkind,
           gov.rolname AS grantee,
           a.source_grantee,
           a.privilege_type
      FROM acl_grants a
      CROSS JOIN governed gov
     WHERE a.source_grantee NOT IN (SELECT rolname FROM governed)
       AND (
         a.grantee_oid = 0
         OR pg_has_role(gov.rolname, a.grantee_oid, 'USAGE')
       )
  ),
  registered AS (
    SELECT g.relation,
           g.object_kind,
           g.role_name,
           g.is_pattern,
           upper(btrim(p)) AS privilege_type
      FROM okg.grant_registry g
      CROSS JOIN LATERAL unnest(string_to_array(g.privileges, ',')) AS p
     WHERE g.classification = 'contract'
  ),
  registered_targets AS (
    SELECT g.relation AS registry_relation,
           s.qualified AS relation,
           s.relkind,
           g.object_kind,
           g.role_name,
           g.is_pattern,
           g.privilege_type
      FROM registered g
      JOIN scoped s
        ON g.object_kind = CASE WHEN s.relkind = 'S'
                                THEN 'SEQUENCE' ELSE 'TABLE' END
       AND (
         (NOT g.is_pattern AND g.relation = s.qualified)
         OR (g.is_pattern
             AND split_part(g.relation, '.', 1) = split_part(s.qualified, '.', 1)
             AND split_part(s.qualified, '.', 2)
                 LIKE split_part(g.relation, '.', 2) ESCAPE '\')
       )
  )
  SELECT 'unregistered_view'::text,
         'okg.' || s.relname,
         NULL::text,
         'okg.v_* view/materialized view with no registry entry: unreadable by '
         'app_ro, and not a decision anyone recorded'::text
    FROM scoped s
   WHERE s.nspname = 'okg'
     AND s.relkind IN ('v', 'm')
     AND s.relname LIKE 'v\_%' ESCAPE '\'
     AND NOT EXISTS (
       SELECT 1
         FROM okg.grant_registry g
        WHERE g.relation = 'okg.' || s.relname
     )

  UNION ALL

  SELECT 'unregistered_grant'::text,
         d.qualified,
         d.grantee,
         'direct ' || d.privilege_type || ' privilege with no registry entry'
    FROM direct_grants d
   WHERE NOT EXISTS (
     SELECT 1
       FROM registered g
      WHERE g.role_name = d.grantee
        AND g.privilege_type = d.privilege_type
        AND g.object_kind = CASE WHEN d.relkind = 'S'
                                 THEN 'SEQUENCE' ELSE 'TABLE' END
        AND (
          (NOT g.is_pattern AND g.relation = d.qualified)
          OR (g.is_pattern
              AND split_part(g.relation, '.', 1) = split_part(d.qualified, '.', 1)
              AND split_part(d.qualified, '.', 2)
                  LIKE split_part(g.relation, '.', 2) ESCAPE '\')
        )
   )

  UNION ALL

  SELECT 'unregistered_grant'::text,
         e.qualified,
         e.grantee,
         'effective ' || e.privilege_type || ' privilege inherited via '
         || e.source_grantee || ' with no registry entry'
    FROM inherited_or_public_grants e
   WHERE NOT EXISTS (
     SELECT 1
       FROM registered g
      WHERE g.role_name = e.source_grantee
        AND g.privilege_type = e.privilege_type
        AND g.object_kind = CASE WHEN e.relkind = 'S'
                                 THEN 'SEQUENCE' ELSE 'TABLE' END
        AND (
          (NOT g.is_pattern AND g.relation = e.qualified)
          OR (g.is_pattern
              AND split_part(g.relation, '.', 1) = split_part(e.qualified, '.', 1)
              AND split_part(e.qualified, '.', 2)
                  LIKE split_part(g.relation, '.', 2) ESCAPE '\')
        )
   )

  UNION ALL

  SELECT 'internal_exposed'::text,
         g.relation,
         gov.rolname,
         'relation classified internal is readable by a governed role'::text
    FROM okg.grant_registry g
    CROSS JOIN governed gov
   WHERE g.classification = 'internal'
     AND to_regclass(g.relation) IS NOT NULL
     AND EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = gov.rolname)
     AND (
       (g.object_kind = 'TABLE'
        AND has_table_privilege(gov.rolname, to_regclass(g.relation), 'SELECT'))
       OR (g.object_kind = 'SEQUENCE'
           AND has_sequence_privilege(
                 gov.rolname, to_regclass(g.relation), 'SELECT'))
     )

  UNION ALL

  -- Walks the ACL, NOT has_table_privilege. okg_mcp is a member of app_ro, so
  -- has_table_privilege reports every app_ro-readable relation as granted to
  -- okg_mcp too -- it would report "present" for a direct grant that the
  -- applier never landed, which is exactly the failure this arm exists to
  -- catch. The unregistered_grant arm above already walks the ACL for the same
  -- reason; this one has to agree with it or the two arms disagree about what
  -- "granted" means. registered_targets expands pattern rows over the live
  -- catalog so a revoked Chronos physical is just as loud as an exact entry.
  SELECT 'missing_grant'::text,
         g.relation,
         g.role_name,
         'registered ' || g.privilege_type || ' privilege is absent'
    FROM registered_targets g
   WHERE EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = g.role_name)
     AND NOT EXISTS (
       SELECT 1
         FROM direct_grants d
        WHERE d.grantee = g.role_name
          AND d.privilege_type = g.privilege_type
          AND d.qualified = g.relation
     )
$okg_grant_registry_drift$;

COMMENT ON FUNCTION okg.grant_registry_drift() IS
  'Rows = grant-surface drift. Empty is the contract (schema-grant-coherence '
  'req.registry-not-convention); asserted by tests and by '
  'okg doctor --check grant-registry-drift.';

-- ---------------------------------------------------------------------
-- The substrate seed.
-- ---------------------------------------------------------------------
--
-- `source = 'substrate'` rows are owned by this file and replaced wholesale on
-- every install, so deleting a name here really does remove the grant on the
-- next migrate. Rows with any other `source` are the deployment extension
-- point and are never touched.

-- ========== External action API role and redacted read surface ==========
--
-- A deployment operator supplies a distinct LOGIN role and exactly one active
-- row in external_action_api_role_bindings.  This shared role is NOLOGIN and
-- has no raw-table privilege; callers use only the deployment-scoped views and
-- fixed-signature security-definer helpers.  Full canonical payload bytes are
-- intentionally absent from every view in this generic substrate surface.
DO $okg_external_action_roles$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_api') THEN
    CREATE ROLE okg_external_action_api NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'okg_external_action_function_owner') THEN
    CREATE ROLE okg_external_action_function_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE;
  END IF;
  -- Converge pre-existing installs as well as fresh roles.  Neither shared
  -- role may be used as a runtime login or bypass row-level security.
  -- PostgreSQL refuses ANY mention of SUPERUSER or BYPASSRLS in ALTER ROLE
  -- from a role lacking that attribute, even to restate the current value,
  -- so an unconditional ALTER stops a non-superuser migration (issue #2239).
  -- Alter only a role that has drifted: an already-exact role needs no
  -- change, and a drifted one is still converged -- or, for a migrating
  -- role that cannot remove SUPERUSER or BYPASSRLS, refused loudly.
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
     WHERE rolname = 'okg_external_action_api'
       AND (rolcanlogin OR rolsuper OR rolbypassrls OR rolcreatedb
            OR rolcreaterole OR NOT rolinherit)
  ) THEN
    ALTER ROLE okg_external_action_api
      NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
     WHERE rolname = 'okg_external_action_function_owner'
       AND (rolcanlogin OR rolsuper OR rolbypassrls OR rolcreatedb
            OR rolcreaterole OR rolinherit)
  ) THEN
    ALTER ROLE okg_external_action_function_owner
      NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
END
$okg_external_action_roles$;

GRANT USAGE ON SCHEMA okg TO okg_external_action_api;
REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_external_action_api;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg FROM okg_external_action_api;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA okg FROM okg_external_action_api;

-- The function owner is a separate NOLOGIN role.  It may inspect only the
-- login-to-deployment binding used by the SECURITY DEFINER helper; the API
-- role itself never receives raw-table access.  Reset first so an operator
-- cannot broaden this boundary by reusing a role from an older install.
GRANT USAGE ON SCHEMA okg TO okg_external_action_function_owner;
REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_external_action_function_owner;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg FROM okg_external_action_function_owner;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA okg FROM okg_external_action_function_owner;
GRANT SELECT ON okg.external_action_api_role_bindings
  TO okg_external_action_function_owner;

-- SECURITY DEFINER bodies run with this role's privileges.  Keep the role
-- NOLOGIN and grant only the normalized relations needed by the fixed bridge;
-- the API role receives none of these raw-table privileges.
GRANT SELECT, INSERT, UPDATE ON
  okg.external_action_proposals,
  okg.external_action_payload_versions,
  okg.external_action_auth_audits,
  okg.external_action_dpop_replay_nonces,
  okg.external_action_decisions,
  okg.external_action_transition_idempotency,
  okg.external_action_executions,
  okg.external_action_outcomes,
  okg.external_action_events,
  okg.external_action_owner_unavailability_attestations,
  okg.external_action_execution_epoch_leases,
  okg.external_action_restore_attestations,
  okg.external_action_restore_fences,
  okg.external_action_restore_sessions,
  okg.external_action_restore_checkpoint_staging,
  okg.external_action_restore_finalize_idempotency,
  okg.external_action_restore_checkpoint_entries,
  okg.external_action_restore_recoveries
TO okg_external_action_function_owner;
GRANT SELECT ON
  okg.nomos_policy_bundles,
  okg.nomos_policy_activations,
  okg.nomos_identity_providers,
  okg.nomos_subject_bindings,
  okg.nomos_decisions
TO okg_external_action_function_owner;
GRANT USAGE, SELECT ON
  okg.external_action_decisions_decision_row_id_seq,
  okg.external_action_dpop_replay_nonces_dpop_replay_nonce_id_seq,
  okg.external_action_events_event_id_seq,
  okg.external_action_execution_epoch_leases_lease_row_id_seq,
  okg.external_action_outcomes_outcome_id_seq,
  okg.external_action_owner_unavailability_attesta_attestation_id_seq,
  okg.external_action_payload_versions_payload_version_id_seq,
  okg.external_action_restore_attestations_attestation_row_id_seq,
  okg.external_action_restore_checkpoint__checkpoint_entry_row_id_seq,
  okg.external_action_restore_checkpoin_checkpoint_staging_row_id_seq,
  okg.external_action_restore_finaliz_finalize_idempotency_row_id_seq,
  okg.external_action_restore_recoveries_recovery_row_id_seq,
  okg.external_action_restore_sessions_restore_session_row_id_seq,
  okg.external_action_transition_idempo_transition_idempotency_id_seq
TO okg_external_action_function_owner;

CREATE OR REPLACE VIEW okg.v_external_action_proposals WITH (security_barrier = true) AS
SELECT proposal_id, deployment_name, execution_epoch, effect_instance_key,
       submission_content_hash, proposer_separation_subject, automation_id,
       automation_version, run_id, correlation_id, action_type, target_ref,
       risk_tier, action_schema_digest, action_contract_digest,
       executor_contract_digest, payload_renderer_digest,
       governed_tool_binding_digest, provider_connection_contract_digest,
       provider_capability_digest, provider_operation_id,
       provider_operation_descriptor_digest, original_payload_hash,
       requested_review_expires_at, effective_review_expires_at, summary_code,
       evidence_digests, review_state, execution_state, lifecycle_version,
       payload_version, current_payload_hash, current_decision_id,
       current_decision_binding_hash, execute_before, created_at, updated_at,
       expired_at
  FROM okg.external_action_proposals
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_payload_versions WITH (security_barrier = true) AS
SELECT deployment_name, proposal_id, payload_version, payload_hash,
       parent_payload_hash, difference_field_codes, created_at
  FROM okg.external_action_payload_versions
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_decisions WITH (security_barrier = true) AS
SELECT decision_id, deployment_name, proposal_id, verdict,
       approved_lifecycle_version, approved_payload_version,
       approved_payload_hash, action_type, target_ref, action_schema_digest,
       action_contract_digest, executor_contract_digest, payload_renderer_digest,
       governed_tool_binding_digest, provider_connection_contract_digest,
       provider_capability_digest, provider_operation_id,
       provider_operation_descriptor_digest, execution_epoch,
       effect_instance_key, approver_principal, approver_separation_subject,
       policy_digest, execute_before, reason_code, decision_binding_hash,
       request_hash, created_at
  FROM okg.external_action_decisions
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_events WITH (security_barrier = true) AS
SELECT event_id, deployment_name, proposal_id, execution_epoch, effect_instance_key,
       sequence, lifecycle_version, event_type, actor_kind, actor_principal,
       actor_separation_subject, authentication_audit_ref, nomos_decision_ref,
       deadline_kind, policy_digest, before_review_state, after_review_state,
       before_execution_state, after_execution_state, payload_version,
       payload_hash, decision_id, decision_binding_hash, execution_id,
       execution_binding_hash, transition_reason_code, outcome,
       outcome_reason_code, outcome_request_hash, restore_recovery_digest,
       effective_at, recorded_at, previous_event_hash, event_hash
  FROM okg.external_action_events
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_executions WITH (security_barrier = true) AS
SELECT execution_id, deployment_name, proposal_id, decision_id, execution_epoch,
       execution_epoch_lease_digest, effect_instance_key,
       worker_separation_subject, approved_lifecycle_version,
       approved_payload_version, approved_payload_hash, action_type, target_ref,
       action_schema_digest, action_contract_digest, executor_contract_digest,
       payload_renderer_digest, governed_tool_binding_digest,
       provider_connection_contract_digest, provider_capability_digest,
       provider_operation_id, provider_operation_descriptor_digest, policy_digest,
       execution_binding_hash, request_hash, okg_claim_handoff_receipt_digest,
       provider_idempotency_key_digest, claimed_at, execute_before
  FROM okg.external_action_executions
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_outcomes WITH (security_barrier = true) AS
SELECT outcome_id, deployment_name, execution_id, effect_instance_key,
       worker_or_reconciler_separation_subject, outcome, outcome_reason_code,
       provider_receipt_digest, recovery_journal_digest,
       owner_unavailability_attestation_digest, request_hash, recorded_at
  FROM okg.external_action_outcomes
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_restore_fences WITH (security_barrier = true) AS
SELECT deployment_name, highest_accepted_epoch, lease_digest, quarantine_state,
       service_mode, lease_chain_digest, verification_config_digest,
       verification_login_binding_digest, restore_attestation_digest,
       accepted_at, mode_changed_at
  FROM okg.external_action_restore_fences
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_restore_recoveries WITH (security_barrier = true) AS
SELECT recovery_row_id, deployment_name, old_epoch, new_epoch, effect_instance_key,
       submission_content_hash, checkpoint_digest, restore_attestation_digest,
       classification, source_proposal_id, source_decision_id,
       source_execution_id, recovered_review_state, recovered_execution_state,
       recovered_outcome, effective_at, recorded_at,
       pre_restore_effect_event_head_hash, restored_effect_event_head_hash,
       signed_worker_checkpoint_digest
  FROM okg.external_action_restore_recoveries
 WHERE deployment_name = okg.external_action_session_deployment();

CREATE OR REPLACE VIEW okg.v_external_action_restore_sessions WITH (security_barrier = true) AS
SELECT restore_session_row_id, deployment_name, restore_id, request_hash,
       idempotency_key_hash, operator_separation_subject,
       restore_attestation_envelope_digest, status,
       completion_receipt_envelope_digest, created_at, updated_at
  FROM okg.external_action_restore_sessions
 WHERE deployment_name = okg.external_action_session_deployment();

-- The staging row is immutable.  This receipt-backed projection supplies the
-- terminal state for readers without rewriting staging evidence: a committed
-- completion receipt is the sole authority for `complete`.
CREATE OR REPLACE VIEW okg.v_external_action_restore_session_terminal
WITH (security_barrier = true) AS
SELECT s.restore_session_row_id, s.deployment_name, s.restore_id, s.request_hash,
       s.idempotency_key_hash, s.operator_separation_subject,
       s.restore_attestation_envelope_digest,
       CASE WHEN r.completion_receipt_row_id IS NOT NULL THEN 'complete' ELSE s.status END AS status,
       COALESCE(r.completion_receipt_envelope_digest,
                s.completion_receipt_envelope_digest) AS completion_receipt_envelope_digest,
       r.completed_at, s.created_at, s.updated_at
  FROM okg.external_action_restore_sessions s
  LEFT JOIN okg.external_action_restore_completion_receipts r
    ON r.deployment_name = s.deployment_name AND r.restore_id = s.restore_id
 WHERE s.deployment_name = okg.external_action_session_deployment();

GRANT SELECT ON okg.v_external_action_restore_session_terminal
  TO okg_external_action_api;

REVOKE ALL ON FUNCTION okg.external_action_session_deployment() FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_assert_deployment(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_store_snapshot() FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_store_startup(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_store_snapshot() TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_authorized_read(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT, TEXT, TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_store_startup(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_api;
DO $okg_external_action_function_owners$
BEGIN
  ALTER FUNCTION okg.external_action_session_deployment() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_assert_deployment(TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_store_snapshot() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_authorized_read(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_nomos_policy_bundle(TEXT, TEXT, TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_nomos_authority_lock() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_store_resolve_principal(TEXT, TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_insert_dpop_replay_nonce(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ, TIMESTAMPTZ) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_store_startup(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_store_insert_lease(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_materialize_expiry(TEXT) OWNER TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT, TEXT, TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
END
$okg_external_action_function_owners$;

-- ---------------------------------------------------------------------------
-- Connection reservations for the MCP read role (issue #2280).
--
-- Every `okg mcp-serve` start claims its connection demand in
-- okg.connection_reservations before it opens its pool, on the read DSN, so
-- as okg_mcp. okg_mcp holds NO privilege on that table. A table grant would
-- let any holder of the okg_mcp credential -- agents with a shell hold it --
-- delete or backdate another process's row, which makes that process exit
-- within one heartbeat (db/capacity.py self-fences with os._exit(70)), or
-- insert an inflated claim that refuses every later start.
--
-- Instead three fixed SECURITY DEFINER entry points do exactly what the
-- reservation protocol needs for an MCP server, and nothing else:
--
--   claim     sweeps claims whose heartbeat is older than 300 s, refuses a
--             demand above the MCP maximum or one that does not fit beside
--             every other live claim, and records or refreshes the caller's
--             own row. Serialized with the direct path by the same advisory
--             lock key (0x0C0FFEE1 = 202374881) db/capacity.py takes.
--   heartbeat sets heartbeat_at = now() on the caller's own row only.
--   release   deletes the caller's own row only.
--
-- "Own" means the holder token. The caller generates 32 random bytes per
-- claim, keeps them in process memory, and presents them as hex on every
-- call; the row stores only their SHA-256. okg_mcp cannot read the table, so
-- it cannot learn another holder's digest, and a digest does not reveal the
-- token. A claim on an existing row with a different or absent digest is
-- refused, so a caller cannot take over a row it did not create. Claims are
-- also limited to `okg-mcp-%` application names, so the MCP role can never
-- write a worker's row. The token is always a bound parameter, never SQL
-- text, so pg_stat_activity does not show it.
--
-- The TTL, the cap, the lock key and the server's usable connection count
-- are all fixed or read server side; none is a caller argument. The cap is
-- the largest demand any valid MCP declaration can produce: every counted
-- independent term of db/concurrency_budget.py at its maximum declarable
-- width of 64, i.e. 5 x 64 = 320. tests/substrate/test_migrate.py pins the
-- 300, 320 and lock-key constants against the Python ones.
--
-- The owner is a separate NOLOGIN NOINHERIT role holding exactly the four
-- verbs on that one table, so the definer bodies can reach nothing else.
-- ---------------------------------------------------------------------------
DO $okg_connection_reservation_owner_role$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
     WHERE rolname = 'okg_connection_reservation_owner'
  ) THEN
    CREATE ROLE okg_connection_reservation_owner
      NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
  -- Converge a drifted role, but only when it has drifted: a non-superuser
  -- migration role may not mention SUPERUSER or BYPASSRLS at all when it
  -- alters a role (issue #2239).
  IF EXISTS (
    SELECT 1 FROM pg_catalog.pg_roles
     WHERE rolname = 'okg_connection_reservation_owner'
       AND (rolcanlogin OR rolsuper OR rolbypassrls OR rolcreatedb
            OR rolcreaterole OR rolinherit)
  ) THEN
    ALTER ROLE okg_connection_reservation_owner
      NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
  END IF;
  -- The ownership transfer below needs the migrating role to be able to SET
  -- ROLE to the owner, and each later CREATE OR REPLACE needs it to inherit
  -- the owner's rights. On PostgreSQL 16 and later a non-superuser that
  -- creates a role gets only ADMIN OPTION on it (createrole_self_grant is
  -- empty by default), so a CREATEROLE migration (issue #2239) would stop at
  -- `must be able to SET ROLE`. Grant it back to the migrating role once. A
  -- superuser needs nothing, and a role that already has both is left alone.
  IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles
           WHERE rolname = current_user)
     AND NOT (pg_has_role(current_user, 'okg_connection_reservation_owner', 'SET')
              AND pg_has_role(current_user, 'okg_connection_reservation_owner',
                              'USAGE')) THEN
    GRANT okg_connection_reservation_owner TO CURRENT_USER
      WITH INHERIT TRUE, SET TRUE;
  END IF;
END
$okg_connection_reservation_owner_role$;

GRANT USAGE ON SCHEMA okg TO okg_connection_reservation_owner;
REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_connection_reservation_owner;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA okg FROM okg_connection_reservation_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON okg.connection_reservations
  TO okg_connection_reservation_owner;

CREATE OR REPLACE FUNCTION okg.connection_reservation_claim(
    p_application_name TEXT,
    p_pid INTEGER,
    p_deployment_name TEXT,
    p_declared_demand INTEGER,
    p_holder_token TEXT
)
RETURNS TABLE (other_claims BIGINT, usable INTEGER)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, okg, pg_temp
AS $okg_connection_reservation_claim$
DECLARE
  v_digest BYTEA;
  v_existing BYTEA;
  v_found BOOLEAN := false;
  v_held BIGINT;
  v_holders TEXT;
  v_usable INTEGER;
BEGIN
  IF p_application_name IS NULL OR p_application_name NOT LIKE 'okg-mcp-%' THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR03',
      MESSAGE = 'connection reservation refused: this entry point records '
             || 'only okg-mcp-* claims';
  END IF;
  -- Bound what a caller can store per row. Deployment names are short
  -- identifiers; 200 characters leaves room without letting a claim carry
  -- an arbitrary payload.
  IF length(p_application_name) > 200
     OR length(coalesce(p_deployment_name, '')) > 200 THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR03',
      MESSAGE = 'connection reservation refused: application and deployment '
             || 'names are limited to 200 characters';
  END IF;
  IF p_pid IS NULL OR p_pid <= 0 THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR03',
      MESSAGE = 'connection reservation refused: pid must be positive';
  END IF;
  IF p_holder_token IS NULL OR p_holder_token !~ '^[0-9a-f]{64}$' THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR03',
      MESSAGE = 'connection reservation refused: the holder token must be '
             || '64 lowercase hex characters';
  END IF;
  IF p_declared_demand IS NULL OR p_declared_demand < 0 THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR03',
      MESSAGE = 'connection reservation refused: declared demand must be '
             || 'between 0 and 320';
  END IF;
  IF p_declared_demand > 320 THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR02',
      MESSAGE = format(
        'connection reservation refused: %s declares %s connections, above '
        'the MCP maximum of 320 (every counted concurrency term at its '
        'maximum declarable width)',
        p_application_name, p_declared_demand);
  END IF;

  v_digest := sha256(convert_to(p_holder_token, 'UTF8'));
  PERFORM pg_advisory_xact_lock(202374881);

  DELETE FROM okg.connection_reservations
   WHERE heartbeat_at < now() - interval '300 seconds';

  SELECT r.holder_token_sha256, true
    INTO v_existing, v_found
    FROM okg.connection_reservations AS r
   WHERE r.application_name = p_application_name AND r.pid = p_pid
   FOR UPDATE;
  IF v_found AND v_existing IS DISTINCT FROM v_digest THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR04',
      MESSAGE = format(
        'connection reservation refused: %s (pid %s) is held by another '
        'holder token; a claim cannot take over a row it did not create',
        p_application_name, p_pid);
  END IF;

  SELECT coalesce(sum(r.declared_demand), 0),
         string_agg(
           format('%s(pid %s)=%s', r.application_name, r.pid,
                  r.declared_demand),
           ', ' ORDER BY r.application_name, r.pid)
    INTO v_held, v_holders
    FROM okg.connection_reservations AS r
   WHERE NOT (r.application_name = p_application_name AND r.pid = p_pid);

  v_usable := current_setting('max_connections')::integer
            - current_setting('superuser_reserved_connections')::integer;
  IF v_held + p_declared_demand > v_usable THEN
    RAISE EXCEPTION USING ERRCODE = 'OKR01',
      MESSAGE = format(
        'host-wide connection reservations cannot fit this process: %s '
        'needs %s, other live claims hold %s [%s], and the server can serve '
        '%s (max_connections=%s minus %s superuser-reserved). Short by %s. '
        'Stop one of the holders, narrow a declared width, or raise '
        'max_connections. Refused before opening any pool, so nothing was '
        'taken from the processes already running.',
        p_application_name, p_declared_demand, v_held,
        coalesce(v_holders, 'none'), v_usable,
        current_setting('max_connections'),
        current_setting('superuser_reserved_connections'),
        v_held + p_declared_demand - v_usable);
  END IF;

  INSERT INTO okg.connection_reservations
      (application_name, pid, deployment_name, declared_demand,
       claimed_at, heartbeat_at, holder_token_sha256)
  VALUES (p_application_name, p_pid, p_deployment_name, p_declared_demand,
          now(), now(), v_digest)
  ON CONFLICT (application_name, pid) DO UPDATE
     SET declared_demand = EXCLUDED.declared_demand,
         deployment_name = EXCLUDED.deployment_name,
         heartbeat_at = now()
   WHERE okg.connection_reservations.holder_token_sha256 = v_digest;

  other_claims := v_held;
  usable := v_usable;
  RETURN NEXT;
END
$okg_connection_reservation_claim$;

CREATE OR REPLACE FUNCTION okg.connection_reservation_heartbeat(
    p_application_name TEXT,
    p_pid INTEGER,
    p_holder_token TEXT
)
RETURNS INTEGER
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, okg, pg_temp
AS $okg_connection_reservation_heartbeat$
DECLARE
  v_updated INTEGER;
BEGIN
  UPDATE okg.connection_reservations
     SET heartbeat_at = now()
   WHERE application_name = p_application_name
     AND pid = p_pid
     AND holder_token_sha256
         = sha256(convert_to(coalesce(p_holder_token, ''), 'UTF8'));
  GET DIAGNOSTICS v_updated = ROW_COUNT;
  RETURN v_updated;
END
$okg_connection_reservation_heartbeat$;

CREATE OR REPLACE FUNCTION okg.connection_reservation_release(
    p_application_name TEXT,
    p_pid INTEGER,
    p_holder_token TEXT
)
RETURNS INTEGER
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, okg, pg_temp
AS $okg_connection_reservation_release$
DECLARE
  v_deleted INTEGER;
BEGIN
  DELETE FROM okg.connection_reservations
   WHERE application_name = p_application_name
     AND pid = p_pid
     AND holder_token_sha256
         = sha256(convert_to(coalesce(p_holder_token, ''), 'UTF8'));
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END
$okg_connection_reservation_release$;

-- Hand the three functions to their owner. ALTER FUNCTION ... OWNER TO needs
-- the new owner to hold CREATE on schema okg (issue #2239). The grant is
-- made and withdrawn inside this one DO block, so no other session ever sees
-- it and the owner keeps only USAGE at rest. Done here rather than in
-- migrate._open_function_owner_transfer because every path that runs this
-- file (migrate, the views-only refresh, the job-runtime cleanup) may run it
-- on a database where this role did not exist until a few statements ago.
DO $okg_connection_reservation_function_owner$
BEGIN
  GRANT CREATE ON SCHEMA okg TO okg_connection_reservation_owner;
  ALTER FUNCTION okg.connection_reservation_claim(TEXT, INTEGER, TEXT, INTEGER, TEXT)
    OWNER TO okg_connection_reservation_owner;
  ALTER FUNCTION okg.connection_reservation_heartbeat(TEXT, INTEGER, TEXT)
    OWNER TO okg_connection_reservation_owner;
  ALTER FUNCTION okg.connection_reservation_release(TEXT, INTEGER, TEXT)
    OWNER TO okg_connection_reservation_owner;
  REVOKE CREATE ON SCHEMA okg FROM okg_connection_reservation_owner;
END
$okg_connection_reservation_function_owner$;

-- Only the MCP read role may call them. PUBLIC's default EXECUTE is removed,
-- and so is app_ro's, because run_sql downgrades agent SQL to app_ro.
REVOKE ALL ON FUNCTION
  okg.connection_reservation_claim(TEXT, INTEGER, TEXT, INTEGER, TEXT),
  okg.connection_reservation_heartbeat(TEXT, INTEGER, TEXT),
  okg.connection_reservation_release(TEXT, INTEGER, TEXT)
  FROM PUBLIC, app_ro;
GRANT EXECUTE ON FUNCTION
  okg.connection_reservation_claim(TEXT, INTEGER, TEXT, INTEGER, TEXT),
  okg.connection_reservation_heartbeat(TEXT, INTEGER, TEXT),
  okg.connection_reservation_release(TEXT, INTEGER, TEXT)
  TO okg_mcp;

DO $okg_grant_registry_seed$
BEGIN
  IF to_regclass('okg.grant_registry') IS NULL THEN
    RETURN;
  END IF;

  DELETE FROM okg.grant_registry WHERE source = 'substrate';

  -- app_ro: the agent-facing read surface. Every substrate `okg.v_*` view is
  -- here BY NAME. Adding a view no longer grants it; adding a row does.
  INSERT INTO okg.grant_registry
    (entry_ref, relation, object_kind, role_name, privileges, note)
  SELECT
    'grant:app_ro:' || v,
    v,
    'TABLE',
    'app_ro',
    'SELECT',
    'agent-facing read surface (run_sql, operator console)'
  FROM unnest(ARRAY[
    'okg.v_nodes',
    'okg.v_edges',
    'okg.v_branch_layered_nodes',
    'okg.v_branch_layered_edges',
    -- okg.v_nodes_history / okg.v_edges_history / okg.v_changes are absent
    -- because they are absent from the schema: retired by decision D6. Do not
    -- re-add a name here without the view, nor the view without real
    -- generation bounds.
    'okg.v_generations',
    'okg.v_generation_interval_stab',
    'okg.v_documents',
    'okg.v_doc_chunks',
    'okg.v_chunk_sequence',
    'okg.v_table_cells',
    'okg.v_subtypes',
    'okg.v_aliases',
    'okg.v_agent_sessions',
    'okg.v_agent_messages',
    'okg.v_tool_use_events',
    'okg.v_source_runs',
    'okg.v_source_run_diagnostics',
    'okg.v_source_frontier_state',
    'okg.v_producer_profiles',
    'okg.v_consistency_observations',
    'okg.v_admission_envelopes',
    'okg.v_admission_events',
    'okg.v_admission_record_refs',
    'okg.v_exchange_boundary_violations',
    'okg.v_nomos_policy_bundles',
    'okg.v_nomos_policy_activations',
    'okg.v_nomos_config_sets',
    'okg.v_nomos_config_activations',
    'okg.v_nomos_identity_providers',
    'okg.v_nomos_external_claims',
    'okg.v_nomos_subject_bindings',
    'okg.v_nomos_permission_grants',
    'okg.v_nomos_decisions',
    'okg.v_runtime_queue_health',
    'okg.v_runtime_lock_surface_inventory',
    'okg.v_runtime_stale_locks',
    'okg.v_compute_host_metrics',
    'okg.v_mcp_calls',
    'okg.v_mcp_call_costs',
    'okg.v_mcp_operator_costs',
    'okg.v_telemetry_trace_summary',
    'okg.v_telemetry_span_tree',
    'okg.v_ir_programs',
    'okg.v_ir_program_bindings',
    'okg.v_ir_invocations'
  ]) AS v;

  -- external_action_api: the only review read surface for the standalone
  -- authenticated service.  These are security-barrier views and omit
  -- canonical payload bytes; no raw external_action_* table is registered.
  INSERT INTO okg.grant_registry
    (entry_ref, relation, object_kind, role_name, privileges, note)
  VALUES
    ('grant:okg_external_action_api:okg.v_external_action_proposals',
     'okg.v_external_action_proposals', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'deployment-scoped external-action review projection'),
    ('grant:okg_external_action_api:okg.v_external_action_payload_versions',
     'okg.v_external_action_payload_versions', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'external-action payload hashes and bounded diff codes only'),
    ('grant:okg_external_action_api:okg.v_external_action_decisions',
     'okg.v_external_action_decisions', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'deployment-scoped external-action decisions'),
    ('grant:okg_external_action_api:okg.v_external_action_events',
     'okg.v_external_action_events', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'deployment-scoped external-action audit chain'),
    ('grant:okg_external_action_api:okg.v_external_action_executions',
     'okg.v_external_action_executions', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'external-action claim binding without canonical payload'),
    ('grant:okg_external_action_api:okg.v_external_action_outcomes',
     'okg.v_external_action_outcomes', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'external worker terminal outcome projection'),
    ('grant:okg_external_action_api:okg.v_external_action_restore_fences',
     'okg.v_external_action_restore_fences', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'restore quarantine and service-mode projection'),
    ('grant:okg_external_action_api:okg.v_external_action_restore_recoveries',
     'okg.v_external_action_restore_recoveries', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'restore recovery classification projection'),
    ('grant:okg_external_action_api:okg.v_external_action_restore_sessions',
     'okg.v_external_action_restore_sessions', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'restore reconciliation session projection'),
    ('grant:okg_external_action_api:okg.v_external_action_restore_session_terminal',
     'okg.v_external_action_restore_session_terminal', 'TABLE',
     'okg_external_action_api', 'SELECT',
     'receipt-backed terminal restore session projection');

  -- okg_mcp: the least-privilege MCP read pool (issue #366). Physical tables
  -- appear here only where an operator read path genuinely needs the table
  -- rather than a view; each group says which path.
  INSERT INTO okg.grant_registry
    (entry_ref, relation, object_kind, role_name, privileges, note)
  VALUES
    ('grant:okg_mcp:okg.graph_nodes', 'okg.graph_nodes', 'TABLE',
     'okg_mcp', 'SELECT', 'graph checkout read path'),
    ('grant:okg_mcp:okg.graph_edges', 'okg.graph_edges', 'TABLE',
     'okg_mcp', 'SELECT', 'graph checkout read path'),
    ('grant:okg_mcp:okg.graph_generations', 'okg.graph_generations', 'TABLE',
     'okg_mcp', 'SELECT', 'generation status read path'),
    -- MCP startup verifies the deployed code/database migration contract
    -- before serving reads; this ledger holds migration identity and
    -- checksums only.
    ('grant:okg_mcp:okg.schema_migrations', 'okg.schema_migrations', 'TABLE',
     'okg_mcp', 'SELECT', 'startup migration-contract verification'),
    -- The SAME startup check reads the multiphase progress ledger:
    -- migration_status() calls multiphase_in_progress_rows(), which selects
    -- okg.schema_migration_phases. That table was added after the row above
    -- and never joined it, so `okg mcp serve` on a least-privilege deployment
    -- died with `permission denied for table schema_migration_phases` before
    -- serving a single read -- the pod never became Ready and the Kubernetes
    -- lifecycle proof timed out waiting for it. Progress metadata only: phase
    -- identity, state, batch counts and a watermark.
    ('grant:okg_mcp:okg.schema_migration_phases',
     'okg.schema_migration_phases', 'TABLE', 'okg_mcp', 'SELECT',
     'startup migration-contract verification: multiphase progress'),
    -- producer/admission provenance read by the lineage + search read paths
    -- (tests/substrate/mcp/test_nomos_source_lineage_mcp.py and the
    -- operator-surface/search dispatch tests).
    ('grant:okg_mcp:okg.producer_profiles', 'okg.producer_profiles', 'TABLE',
     'okg_mcp', 'SELECT', 'lineage + search provenance'),
    ('grant:okg_mcp:okg.catalog_versions', 'okg.catalog_versions', 'TABLE',
     'okg_mcp', 'SELECT', 'catalog resolution'),
    ('grant:okg_mcp:okg.subtypes', 'okg.subtypes', 'TABLE',
     'okg_mcp', 'SELECT', 'catalog resolution'),
    -- The refinement closure. Profiled search expands a profile over it and
    -- MEASURES closure completeness against okg.subtypes.parent_subtype
    -- (mcp/search_profiles.py:285, :360) on the okg_mcp read pool. It was
    -- never in views.sql's allow-list; it worked only when role_split.sql's
    -- blanket `GRANT SELECT ON ALL TABLES` happened to run last, which is the
    -- order-dependence this PACT removes. Registering it converts an
    -- accidental grant into a decided one -- without this row, profiled search
    -- fails `permission denied for table subtype_ancestors` on exactly the
    -- deployments where migrate ran last.
    ('grant:okg_mcp:okg.subtype_ancestors', 'okg.subtype_ancestors', 'TABLE',
     'okg_mcp', 'SELECT', 'catalog resolution: profiled-search closure'),
    ('grant:okg_mcp:okg.narrowings', 'okg.narrowings', 'TABLE',
     'okg_mcp', 'SELECT', 'catalog resolution'),
    ('grant:okg_mcp:okg.aliases', 'okg.aliases', 'TABLE',
     'okg_mcp', 'SELECT', 'alias resolution'),
    -- catalog entity-resolution patterns read by
    -- okg.find_identifier_mentions(), an EXECUTE-granted catalog function that
    -- runs AS okg_mcp and so needs the caller's own SELECT.
    ('grant:okg_mcp:okg.identifier_patterns', 'okg.identifier_patterns',
     'TABLE', 'okg_mcp', 'SELECT', 'find_identifier_mentions() runs as caller'),
    ('grant:okg_mcp:okg.source_progress', 'okg.source_progress', 'TABLE',
     'okg_mcp', 'SELECT', 'source status read path'),
    ('grant:okg_mcp:okg.source_watermarks', 'okg.source_watermarks', 'TABLE',
     'okg_mcp', 'SELECT', 'source status read path'),
    -- scale-operator-attr-reads: the attr operators (inspect/aggregate over
    -- attribute paths) read the effective-attribute surface directly on this
    -- pool. Without them every attr-path MCP read fails permission denied
    -- (tests/substrate/mcp/test_operator_attr_pushdown.py).
    ('grant:okg_mcp:okg.attribute_observations', 'okg.attribute_observations',
     'TABLE', 'okg_mcp', 'SELECT', 'attr operator pushdown'),
    ('grant:okg_mcp:okg.attribute_effective_attrs',
     'okg.attribute_effective_attrs', 'TABLE', 'okg_mcp', 'SELECT',
     'attr operator pushdown'),
    ('grant:okg_mcp:okg.attribute_effective_view',
     'okg.attribute_effective_view', 'TABLE', 'okg_mcp', 'SELECT',
     'attr operator pushdown'),
    -- ivm-effective-attribute-view + schema-interval-narrow-split +
    -- schema-attr-filter-index. These four are created lazily at first
    -- publish, not by the migration schema, so the applier grants them only
    -- once they exist. Until then the reader falls back to the per-generation
    -- snapshot (tests/substrate/
    -- test_effective_attribute_view_interval_readers.py).
    ('grant:okg_mcp:okg.attribute_effective_view_intervals',
     'okg.attribute_effective_view_intervals', 'TABLE', 'okg_mcp', 'SELECT',
     'interval reader; created at first publish'),
    ('grant:okg_mcp:okg.attribute_versions', 'okg.attribute_versions', 'TABLE',
     'okg_mcp', 'SELECT', 'version store behind interval/snapshot readers'),
    -- okg#3112 step 2: the expression-index filter path reads each declared
    -- index's readiness (state, digest, drift flag) before it narrows by it.
    ('grant:okg_mcp:okg.declared_attr_indexes', 'okg.declared_attr_indexes',
     'TABLE', 'okg_mcp', 'SELECT', 'declared attribute index readiness'),
    -- first-class-images (#3046): inspect(kind="image") reads stored image
    -- bytes. The view holds only kind = 'image' rows; okg.blobs itself is
    -- deliberately not registered for okg_mcp.
    ('grant:okg_mcp:okg.image_blobs', 'okg.image_blobs', 'TABLE',
     'okg_mcp', 'SELECT', 'image bytes for inspect(kind=image)'),
    ('grant:okg_mcp:okg.v_generation_interval_stab',
     'okg.v_generation_interval_stab', 'TABLE', 'okg_mcp', 'SELECT',
     'D2: the TEXT-ref-keyed view REPLACES a grant on generation_ordinals'),
    -- Integrity state/receipts are read-only proof metadata. The backend-bound
    -- transition table stays out: it is publisher authority, not a read
    -- surface, and role_split.sql revokes it explicitly.
    ('grant:okg_mcp:okg.attribute_effective_integrity_state',
     'okg.attribute_effective_integrity_state', 'TABLE', 'okg_mcp', 'SELECT',
     'read-only integrity proof metadata'),
    ('grant:okg_mcp:okg.attribute_effective_integrity_receipts',
     'okg.attribute_effective_integrity_receipts', 'TABLE', 'okg_mcp',
     'SELECT', 'read-only integrity proof metadata'),
    ('grant:okg_mcp:okg.nomos_policy_bundles', 'okg.nomos_policy_bundles',
     'TABLE', 'okg_mcp', 'SELECT', 'read-time authorization'),
    -- PACT lane-h-nomos-read-class-sql: the reader's view decides every
    -- catalogued read class before its query.
    ('grant:okg_mcp:okg.nomos_read_classes', 'okg.nomos_read_classes',
     'TABLE', 'okg_mcp', 'SELECT', 'reader view class catalog'),
    -- PACT openrouter-embeddings-hnsw: semantic search reads a vector set's
    -- vectors, document links and pin watermarks. No okg.v_* view and no
    -- app_ro grant: whether a hash has a vector is not an agent read surface.
    ('grant:okg_mcp:okg.embedding_sets', 'okg.embedding_sets',
     'TABLE', 'okg_mcp', 'SELECT', 'semantic search vector sets'),
    ('grant:okg_mcp:okg.text_embeddings', 'okg.text_embeddings',
     'TABLE', 'okg_mcp', 'SELECT', 'semantic search vectors'),
    ('grant:okg_mcp:okg.node_documents', 'okg.node_documents',
     'TABLE', 'okg_mcp', 'SELECT', 'semantic search document links'),
    ('grant:okg_mcp:okg.embedding_watermarks', 'okg.embedding_watermarks',
     'TABLE', 'okg_mcp', 'SELECT', 'semantic search pin watermarks'),
    ('grant:okg_mcp:okg.embedding_templates', 'okg.embedding_templates',
     'TABLE', 'okg_mcp', 'SELECT', 'semantic search document templates'),
    ('grant:okg_mcp:okg.nomos_policy_activations',
     'okg.nomos_policy_activations', 'TABLE', 'okg_mcp', 'SELECT',
     'read-time authorization'),
    ('grant:okg_mcp:okg.nomos_identity_providers',
     'okg.nomos_identity_providers', 'TABLE', 'okg_mcp', 'SELECT',
     'read-time authorization'),
    ('grant:okg_mcp:okg.nomos_subject_bindings', 'okg.nomos_subject_bindings',
     'TABLE', 'okg_mcp', 'SELECT', 'read-time authorization'),
    ('grant:okg_mcp:okg.nomos_permission_grants',
     'okg.nomos_permission_grants', 'TABLE', 'okg_mcp', 'SELECT',
     'read-time authorization'),

    -- The writes the read pool legitimately performs: per-call audit logging,
    -- bounded MCP telemetry, and the two Nomos read-time audit tables.
    -- Append-only in effect -- none of these is a graph relation.
    ('grant:okg_mcp:okg.mcp_call_log', 'okg.mcp_call_log', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'per-call audit log'),
    ('grant:okg_mcp:okg.mcp_call_log_call_id_seq',
     'okg.mcp_call_log_call_id_seq', 'SEQUENCE', 'okg_mcp', 'USAGE, SELECT',
     'per-call audit log'),
    ('grant:okg_mcp:okg.mcp_trace_events', 'okg.mcp_trace_events', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'trace-replacement audit rows'),
    ('grant:okg_mcp:okg.mcp_trace_events_event_id_seq',
     'okg.mcp_trace_events_event_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'trace-replacement audit rows'),
    ('grant:okg_mcp:okg.telemetry_traces', 'okg.telemetry_traces', 'TABLE',
     'okg_mcp', 'INSERT, SELECT, UPDATE', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_spans', 'okg.telemetry_spans', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_events', 'okg.telemetry_events', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_events_event_id_seq',
     'okg.telemetry_events_event_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_graph_effect_manifests',
     'okg.telemetry_graph_effect_manifests', 'TABLE', 'okg_mcp',
     'INSERT, SELECT, UPDATE', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_graph_effects',
     'okg.telemetry_graph_effects', 'TABLE', 'okg_mcp',
     'INSERT, SELECT, DELETE', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_metrics', 'okg.telemetry_metrics', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_metrics_metric_id_seq',
     'okg.telemetry_metrics_metric_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'unified telemetry for MCP spans'),
    ('grant:okg_mcp:okg.telemetry_artifacts', 'okg.telemetry_artifacts',
     'TABLE', 'okg_mcp', 'INSERT, SELECT, UPDATE',
     'unified telemetry for MCP spans'),
    -- Operator call samples (issue #2011, PACT call-sample-policy). The
    -- tracing wrapper that writes them runs on the okg_mcp READ pool, so the
    -- sample insert needs okg_mcp grants here -- the app_rw grants in
    -- schema.sql do not reach it. Without these rows every live sample insert
    -- fails with permission denied, the tracer catches it, and the windowed
    -- reader then reports `no_data`: a call that happened would read exactly
    -- like a call that never did.
    --
    -- INSERT + SELECT only on the three class tables: a stored sample is
    -- append-only and must not be rewritten or deleted by the read pool. The
    -- watermark is an upsert (monotonic-min first_observed_at), so it alone
    -- also carries UPDATE -- and still no DELETE, because losing a watermark
    -- row is what turns `aged_out` back into `no_data`.
    ('grant:okg_mcp:okg.call_sample_hot', 'okg.call_sample_hot', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'operator call samples (hot class)'),
    ('grant:okg_mcp:okg.call_sample_hot_sample_id_seq',
     'okg.call_sample_hot_sample_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'operator call samples (hot class)'),
    ('grant:okg_mcp:okg.call_sample_warm', 'okg.call_sample_warm', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'operator call samples (warm class)'),
    ('grant:okg_mcp:okg.call_sample_warm_sample_id_seq',
     'okg.call_sample_warm_sample_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'operator call samples (warm class)'),
    ('grant:okg_mcp:okg.call_sample_cold', 'okg.call_sample_cold', 'TABLE',
     'okg_mcp', 'INSERT, SELECT', 'operator call samples (cold class)'),
    ('grant:okg_mcp:okg.call_sample_cold_sample_id_seq',
     'okg.call_sample_cold_sample_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'operator call samples (cold class)'),
    ('grant:okg_mcp:okg.call_sample_watermark', 'okg.call_sample_watermark',
     'TABLE', 'okg_mcp', 'INSERT, SELECT, UPDATE',
     'operator call sample retention watermark'),
    -- External-backed probe cache (issue #83, req.external-backed-nodes).
    -- `inspect(target={"kind": "node", "hydrate": ...})` hydrates `probe`
    -- fields on the okg_mcp READ pool, and every outcome -- allowed, denied,
    -- unavailable -- is written to this cache before the field is returned;
    -- a cache-first field is also READ from it. The app_rw grant in
    -- schema.sql does not reach this pool, so without these privileges every
    -- hydrated probe read fails permission denied and the whole inspect call
    -- ends as operator_execution_failed.
    --
    -- Unlike the call-sample tables this is a cache, not an audit log: one
    -- row per (deployment, node, backing fingerprint, field), OVERWRITTEN by
    -- an upsert, hence UPDATE (and SELECT, which ON CONFLICT DO UPDATE needs).
    -- Still no DELETE or TRUNCATE: the read pool never removes a row. The
    -- primary key is natural (no sequence), so no sequence grant is needed.
    ('grant:okg_mcp:okg.external_probe_cache', 'okg.external_probe_cache',
     'TABLE', 'okg_mcp', 'INSERT, SELECT, UPDATE',
     'external-backed probe cache written by inspect(node, hydrate)'),
    -- evaluate_identity_claim_from_postgres reads the Nomos policy/identity
    -- tables above and appends two audit rows; both writes upsert, hence
    -- UPDATE alongside INSERT.
    ('grant:okg_mcp:okg.nomos_external_claims', 'okg.nomos_external_claims',
     'TABLE', 'okg_mcp', 'INSERT, SELECT, UPDATE',
     'read-time authorization audit'),
    ('grant:okg_mcp:okg.nomos_external_claims_claim_row_id_seq',
     'okg.nomos_external_claims_claim_row_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'read-time authorization audit'),
    ('grant:okg_mcp:okg.nomos_decisions', 'okg.nomos_decisions', 'TABLE',
     'okg_mcp', 'INSERT, SELECT, UPDATE', 'read-time authorization audit'),
    ('grant:okg_mcp:okg.nomos_decisions_decision_row_id_seq',
     'okg.nomos_decisions_decision_row_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'read-time authorization audit'),

    -- The PACT coordination ledger (#1064's handoff, v1.0 design §2.18).
    -- `okg pact` writes coordination events AS okg_mcp. Before this registry
    -- the grant came only from the ledger's own ensure path, which is skipped
    -- when a probe says the tables already exist -- so the schema-wide
    -- `REVOKE ALL ON ALL TABLES IN SCHEMA okg FROM okg_mcp` above stripped a
    -- privilege nothing put back, and coordination writes failed until a
    -- database that had never run migrate happened to bootstrap. The registry
    -- owns it now: the REVOKE and the re-GRANT are in the same file, in that
    -- order.
    ('grant:okg_mcp:okg.pact_coordination_events',
     'okg.pact_coordination_events', 'TABLE', 'okg_mcp', 'INSERT, SELECT',
     'okg pact writes coordination events as okg_mcp'),
    ('grant:okg_mcp:okg.pact_coordination_events_id_seq',
     'okg.pact_coordination_events_id_seq', 'SEQUENCE', 'okg_mcp',
     'USAGE, SELECT', 'okg pact writes coordination events as okg_mcp'),
    ('grant:okg_mcp:okg.pact_decisions', 'okg.pact_decisions', 'TABLE',
     'okg_mcp', 'SELECT', 'review-queue reads'),
    ('grant:okg_mcp:okg.pact_intents', 'okg.pact_intents', 'TABLE',
     'okg_mcp', 'SELECT', 'worktree-claim read model'),

    -- Chronos physicals. This ONE pattern row replaces the blanket
    -- `GRANT SELECT ON ALL TABLES IN SCHEMA okg/public TO okg_mcp` that
    -- role_split.sql used to issue. The prefix is the scope: a non-Chronos
    -- table created in public by any role is NOT covered.
    ('grant:okg_mcp:public._chronos%', 'public.\_chronos%', 'TABLE',
     'okg_mcp', 'SELECT',
     'Chronos branch/checkpoint physicals, created lazily at publish time');

  UPDATE okg.grant_registry
     SET is_pattern = TRUE
   WHERE entry_ref = 'grant:okg_mcp:public._chronos%';

  -- INTERNAL: never granted, to anyone, ever. v1.0 design §2.2 upheld the
  -- panel dispute -- these mirror tables are a second full copy of the graph
  -- maintained by TRUNCATE + re-insert each publish (audit F14), and their
  -- only consumers are the in-engine SQL consistency checks. Delta
  -- maintenance is the named precondition of ever exposing them; until then
  -- the drift check treats any governed-role read privilege on them as a
  -- finding.
  INSERT INTO okg.grant_registry
    (entry_ref, relation, object_kind, classification, note)
  VALUES
    ('internal:okg.chronos_graph_nodes_current',
     'okg.chronos_graph_nodes_current', 'TABLE', 'internal',
     'v1.0 design §2.2: mirror table, never granted'),
    ('internal:okg.chronos_graph_edges_current',
     'okg.chronos_graph_edges_current', 'TABLE', 'internal',
     'v1.0 design §2.2: mirror table, never granted');
END
$okg_grant_registry_seed$;

-- Apply. Last statement in the file that touches a privilege.
DO $okg_grant_registry_apply$
BEGIN
  IF to_regprocedure('okg.apply_grant_registry()') IS NOT NULL THEN
    PERFORM okg.apply_grant_registry();
  END IF;
END
$okg_grant_registry_apply$;

-- The semantic command entry points are the only mutation API.  Keep the
-- compatibility bridge above for old clients, but do not expose raw tables or
-- helper functions to the login role.
REVOKE ALL ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_json_bytes(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_hash(TEXT,JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_effect_key(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_startup_login_binding() FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_record_auth_audit(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_submit(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_review_transition(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_claim(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_prepare_claim(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_authorized_read(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_report_outcome(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_owner_unavailability(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_restore_create(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_restore_batch(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_restore_prepare(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_restore_finalize(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_restore_inventory(TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_store_insert_lease(JSONB) FROM PUBLIC;
REVOKE ALL ON FUNCTION okg.external_action_materialize_expiry(TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION okg.external_action_record_auth_audit(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_nomos_persist_decision(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_submit(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_review_transition(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_claim(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_prepare_claim(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_authorized_read(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_report_outcome(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_owner_unavailability(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_api;
DO $external_action_semantic_function_owner$
BEGIN
  GRANT SELECT, INSERT, UPDATE ON
    okg.external_action_proposals,
    okg.external_action_payload_versions,
    okg.external_action_auth_audits,
    okg.external_action_dpop_replay_nonces,
    okg.external_action_decisions,
    okg.external_action_transition_idempotency,
    okg.external_action_executions,
    okg.external_action_outcomes,
    okg.external_action_events,
    okg.external_action_owner_unavailability_attestations,
    okg.external_action_execution_epoch_leases,
    okg.external_action_restore_attestations,
    okg.external_action_restore_fences,
    okg.external_action_restore_sessions,
    okg.external_action_restore_checkpoint_staging,
    okg.external_action_restore_completion_receipts,
    okg.external_action_restore_recoveries
  TO okg_external_action_function_owner;
  GRANT SELECT, UPDATE ON okg.external_action_api_role_bindings
    TO okg_external_action_function_owner;
  GRANT SELECT ON okg.nomos_policy_bundles, okg.nomos_policy_activations,
    okg.nomos_identity_providers, okg.nomos_subject_bindings, okg.nomos_decisions
    TO okg_external_action_function_owner;
  GRANT SELECT, INSERT ON okg.nomos_decisions TO okg_external_action_function_owner;
  GRANT USAGE, SELECT ON
    okg.external_action_decisions_decision_row_id_seq,
    okg.external_action_dpop_replay_nonces_dpop_replay_nonce_id_seq,
    okg.external_action_events_event_id_seq,
    okg.external_action_execution_epoch_leases_lease_row_id_seq,
    okg.external_action_outcomes_outcome_id_seq,
    okg.external_action_owner_unavailability_attesta_attestation_id_seq,
    okg.external_action_payload_versions_payload_version_id_seq,
    okg.external_action_restore_attestations_attestation_row_id_seq,
    okg.external_action_restore_checkpoin_checkpoint_staging_row_id_seq,
    okg.external_action_restore_completio_completion_receipt_row_id_seq,
    okg.external_action_restore_recoveries_recovery_row_id_seq,
    okg.external_action_restore_sessions_restore_session_row_id_seq,
    okg.external_action_transition_idempo_transition_idempotency_id_seq,
    okg.nomos_decisions_decision_row_id_seq
  TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_session_deployment() TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_assert_deployment(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_json_bytes(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_logical_json_record(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_hash(TEXT,JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_effect_key(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_api_role_binding_guard() TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_verifier_capability_status() TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_startup_login_binding() TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_claim_owner_unavailability(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_require_command(JSONB,TEXT[],TEXT[]) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_json_bytes(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_logical_json_record(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_hash(TEXT,JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_effect_key(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_startup_login_binding() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_claim_owner_unavailability(TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_record_auth_audit(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_hmac_sha256(BYTEA,BYTEA) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_api_role_binding_guard() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_verifier_capability_status() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_verify_verifier_assertion(JSONB,TEXT,JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_nomos_authority_snapshot(TEXT,TEXT,TEXT,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_nomos_persist_decision(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_submit(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_review_transition(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_claim(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_prepare_claim(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_authorized_read(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_report_outcome(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_owner_unavailability(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_create(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_batch(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_prepare(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_finalize(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_classification(JSONB,TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_build_plan(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_inventory_digest(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_inventory(TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_event_root(TEXT) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_assert_coverage(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_restore_lifecycle_gate() OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_store_insert_lease(JSONB) OWNER TO okg_external_action_function_owner;
  ALTER FUNCTION okg.external_action_materialize_expiry(TEXT) OWNER TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_nomos_policy_bundle(TEXT, TEXT, TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_store_insert_lease(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_materialize_expiry(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_create(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_batch(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_prepare(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_finalize(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_classification(JSONB,TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_build_plan(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory_digest(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_inventory(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_checkpoint_set_digest(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_event_root(TEXT) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_assert_coverage(JSONB) TO okg_external_action_function_owner;
  GRANT EXECUTE ON FUNCTION okg.external_action_restore_lifecycle_gate() TO okg_external_action_function_owner;
END
$external_action_semantic_function_owner$;

-- The deployment-mapped review login must not inherit an unrelated privileged
-- read or trigger surface through PostgreSQL's default PUBLIC EXECUTE ACL.
-- Each function below already has explicit grants for its intended existing
-- application role; this only removes the ambient grant inherited by every
-- login. Keep this after the DROP/CREATE view helpers above so convergence
-- cannot recreate their default ACLs after migration 0030 hardens them.
DO $external_action_unrelated_security_definer_acl$
BEGIN
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_rows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_rows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.mark_attribute_effective_integrity_dirty_update()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.mark_attribute_effective_integrity_dirty_update() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_chronos_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_chronos_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.require_graph_checkout(text)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.require_graph_checkout(text) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.runtime_queue_health(integer)') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.runtime_queue_health(integer) FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_active_workflows()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_active_workflows() FROM PUBLIC';
  END IF;
  IF to_regprocedure('okg.source_frontier_unpublished_writes()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_unpublished_writes() FROM PUBLIC';
    -- The schema-wide default privilege above grants okg_mcp EXECUTE on every
    -- new function. Readers reach this one only through the SECURITY DEFINER
    -- okg.source_frontier_state(), which runs it as the owner, so okg_mcp
    -- keeps no direct grant (issue #3024).
    IF to_regrole('okg_mcp') IS NOT NULL THEN
      EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_unpublished_writes() FROM okg_mcp';
    END IF;
  END IF;
  IF to_regprocedure('okg.source_frontier_state()') IS NOT NULL THEN
    EXECUTE 'REVOKE ALL ON FUNCTION okg.source_frontier_state() FROM PUBLIC';
  END IF;
END
$external_action_unrelated_security_definer_acl$;

-- BEGIN external automation run grants (also converged by views.sql).
REVOKE ALL ON okg.external_automation_runs, okg.external_automation_run_proposals FROM PUBLIC, okg_external_action_api;
GRANT SELECT, INSERT ON okg.external_automation_runs, okg.external_automation_run_proposals TO okg_external_action_function_owner;
DO $run_generation_grant$
BEGIN
  -- Fresh schema precedes views.sql, which unconditionally converges this
  -- grant after creating the authoritative view. Upgrades already have it.
  IF to_regclass('okg.v_generations') IS NOT NULL THEN
    GRANT SELECT (id, deployment_name) ON okg.v_generations TO okg_external_action_function_owner;
  END IF;
END $run_generation_grant$;
REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM PUBLIC, okg_external_action_api;
DO $run_read_role_revoke$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM app_ro;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'okg_mcp') THEN
    REVOKE ALL ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) FROM okg_mcp;
  END IF;
END $run_read_role_revoke$;
ALTER FUNCTION okg.external_automation_transaction_context() OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_register(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_read(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_link(JSONB) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_authority(JSONB,TEXT,TEXT) OWNER TO okg_external_action_function_owner;
ALTER FUNCTION okg.external_automation_projection(TEXT) OWNER TO okg_external_action_function_owner;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB) TO okg_external_action_api;
GRANT EXECUTE ON FUNCTION okg.external_automation_transaction_context(), okg.external_automation_register(JSONB), okg.external_automation_read(JSONB), okg.external_automation_link(JSONB), okg.external_automation_authority(JSONB,TEXT,TEXT), okg.external_automation_projection(TEXT) TO okg_external_action_function_owner;
-- END external automation run grants.
