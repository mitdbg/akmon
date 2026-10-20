-- schema-canonical-read-path / D6 (docs/architecture/db-schema-v1-design.md
-- §3.6, decided by the operator 2026-07-31: "retire now; real history views
-- post-OSS").
--
-- views.sql creates; it never drops. So deleting a view's definition from the
-- canonical file makes it absent from a FRESH install and leaves it standing
-- on every UPGRADED one — where the `okg.v_*` grant sweep at the end of
-- views.sql then re-grants it, because that sweep grants whatever exists.
-- Retirement that only edits the file is therefore retirement on new
-- deployments and nothing at all on the deployments that have the problem.
-- This migration is the other half.
--
-- What goes, and why each is a lie rather than merely unused:
--
--   okg.v_nodes_history / okg.v_edges_history
--     Returned LIVE rows with `NULL::bigint` valid_from_gen / valid_to_gen —
--     the column shape of a bitemporal history view with none of the
--     semantics. An agent following the documented bitemporal-diff recipe got
--     a confident answer computed from two constant NULLs. Real history reads
--     go through ChronosGraphStore.checkout_generation() on two checkpoints.
--
--   okg.v_changes
--     Summarised `open_spec_change` nodes for the OpenSpec workflow this repo
--     has itself retired, joining child counts by string-matching
--     `attrs->>'change_id'` instead of by graph edges.
--
--   okg.projectable_node_facts / okg.projectable_edge_facts
--     The pre-Chronos Phase-A admission-rejection filter, read only by the
--     live-table projection that the Chronos cutover stopped calling. The
--     cutover had already replaced them with `WHERE false` stubs, so what
--     stands on an upgraded database is two granted views that structurally
--     cannot return a row. Rejection safety for committed derived facts is
--     the derived-fact loader's (source, run_id) envelope deny-list
--     (publisher.py::_load_unapplied_derived_facts) and stays there.
--
-- RESTRICT, not CASCADE. If a deployment-local or operator-authored view sits
-- on one of these, this migration fails and NAMES it, which is the outcome we
-- want: removing something nobody asked to remove is how a "cleanup" becomes
-- an outage. The operator re-points their view and re-runs.
--
-- Not flagged destructive in manifest.yaml, for the same reason 0023 was not:
-- destructive migrations are auto-skipped by the generic runner, which would
-- leave the upgrade path permanently diverged from a fresh install and red the
-- migration-convergence lane. The label has to describe the effect, and the
-- effect here is a view drop — no table, no column, no row. Every fact these
-- views projected remains readable from the relations they read.
--
-- The filename follows from that. The runner cross-checks the manifest's
-- `destructive` against a filename heuristic (a `retire_` / `drop_` /
-- `remove_` slug prefix means destructive) and REFUSES when the two disagree,
-- so a migration named `0024_retire_...` cannot carry `destructive: false`.
-- Renaming is the honest resolution here: the word describes the decision this
-- migration implements, not the operation it performs on the database.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

DROP VIEW IF EXISTS okg.v_nodes_history;
DROP VIEW IF EXISTS okg.v_edges_history;
DROP VIEW IF EXISTS okg.v_changes;
DROP VIEW IF EXISTS okg.projectable_node_facts;
DROP VIEW IF EXISTS okg.projectable_edge_facts;

-- The legacy live complex is retired by management.retire_legacy_graph_storage
-- on every migrate, which drops okg.nodes_live / okg.edges_live themselves.
-- Their TRIGGERS go with the tables, because a trigger belongs to its table.
-- Standalone FUNCTIONS do not: a plpgsql body is a string to PostgreSQL, so
-- no dependency is tracked, `DROP TABLE ... RESTRICT` succeeds, and the
-- function is left behind pointing at a relation that no longer exists. Worse,
-- views.sql's `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA okg TO okg_mcp` then
-- re-grants it on every migrate.
--
-- schema.sql defined seven such functions over the live tables and now defines
-- none, so all seven are dropped here. Only a PRE-cutover database can still
-- carry them — #368's filter stopped installing them at migrate time long
-- before this change, and the pinned convergence dump contains none — but
-- "probably nobody has it" is the reasoning that leaves orphans behind, and
-- `DROP FUNCTION IF EXISTS` on an absent function costs nothing.
DROP FUNCTION IF EXISTS okg.find_concept_links(TEXT, TEXT, TEXT, TEXT, REAL);
DROP FUNCTION IF EXISTS okg._enforce_narrowing();
DROP FUNCTION IF EXISTS okg._fill_live_branch_id();
DROP FUNCTION IF EXISTS okg._enforce_edge_endpoints_visible();
DROP FUNCTION IF EXISTS okg._edge_endpoint_visible_at(TEXT, BIGINT);
DROP FUNCTION IF EXISTS okg._edge_endpoint_subtype_at(TEXT, BIGINT);
DROP FUNCTION IF EXISTS okg._native_view_visible_at(TEXT, BIGINT);
