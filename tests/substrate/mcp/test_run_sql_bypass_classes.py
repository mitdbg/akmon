"""Bypass classes for the run_sql safety guard (#2406, #2585).

PARSER-AGNOSTIC: this file imports only the public entry points
(``validate_run_sql``, ``validate_probe_query``) and never pglast or
sqlglot, so any implementation of the guard -- the pglast one or the
sqlglot port (PR #2579) -- must pass the same tables unchanged.

Each row is ``(sql, expected)``. ``expected`` is ``ALLOWED`` or
``(error_code, detail)``, where ``detail`` is ``None`` or a
``(data_key, value)`` pair naming a SQL-level fact the refusal must
report: the deny-listed function a cast or resolver reaches
(``function``), or the relation that failed the allow-list
(``relation``). Rows record no parser-internal names. ``error_code``
may be a tuple of acceptable codes: the first is the rule's own code,
and ``run_sql_invalid`` after it admits a parser that cannot read the
shape at all and refuses it for that reason (failing closed). The detail
is checked only when the first code is returned.

The rule each group enforces, in terms of the query's shape:

* W (writes): a query containing INSERT, UPDATE, DELETE or MERGE at any
  depth (a data-modifying WITH, a WITH nested in a subquery or in a CTE
  body), SELECT ... INTO, or FOR UPDATE / FOR SHARE at any depth is
  refused as run_sql_non_select.
* S (scope): a bare relation name is allowed only when it names a CTE
  visible at that point under PostgreSQL's rules -- a CTE is visible to
  the query whose WITH declares it and to that query's subqueries; in a
  plain WITH a CTE body sees only the CTEs declared before it (not
  itself, not later ones); in WITH RECURSIVE every body sees every name
  in its list; a CTE declared inside a subquery or a nested WITH is not
  visible outside it; names compare exactly as PostgreSQL folds them
  (unquoted -> lower case, quoted -> as written). A write target never
  resolves to a CTE. Every other name must be okg.v_* compared with
  ASCII letters folded to lower case (so ``"OKG"."V_NODES"`` passes, as
  the graph-view rewrite also matches it), with no database part;
  anything else is run_sql_disallowed_relation reporting the relation.
* R (string -> reference): any use of a reg* type (regproc,
  regprocedure, regclass, regtype, regoper, regoperator, regnamespace,
  regrole, ...) or a pg_* catalog row type (pg_proc, pg_type, ...) is
  refused as run_sql_forbidden_function, however it is written: ``::``,
  CAST(... AS ...), a typed literal (``regproc 'x'``), an array form, a
  quoted or pg_catalog-qualified type name, or a column definition list
  (``AS t(x regproc)``). The same cast written as a function call
  (``regclass('x')``, ``pg_catalog.regprocedure('x')``, the input
  functions ``regprocin`` / ``regclassin`` / ``*recv``) or as a field
  (``('x'::text).regclass``, ``t.regclass``) is refused, as are every
  to_reg* call and pg_input_is_valid, pg_input_error_info and
  pg_get_object_address. When the string names a deny-listed function,
  the refusal reports that function with schema, quotes and argument
  list stripped.
* X (functions that execute SQL text): a function that runs a query or
  reads a table named by a string argument is refused as
  run_sql_forbidden_function wherever it appears: ts_stat, ts_rewrite,
  the query_to_xml / cursor_to_xml / table_to_xml / schema_to_xml /
  database_to_xml families (including *_xmlschema), and dblink*.
* F (functions): sequence writes (nextval, setval), the advisory-lock
  and large-object families, and the server-state classes (names
  starting pg_stat_get, pg_show, pg_hba, pg_ident, pg_config,
  pg_control_, pg_ls, pg_read, pg_file, pg_create, pg_replication,
  pg_logical, pg_backup, pg_promote, pg_log_, pg_import,
  pg_export_snapshot, brin_summarize, gin_clean, txid_) are refused as
  run_sql_forbidden_function, in the select list or in FROM.
* M (FROM items): a FROM item must be a relation, a subquery, a join of
  those, or an allow-listed row generator (generate_series,
  generate_subscripts, unnest, the json/jsonb row functions,
  regexp_matches, regexp_split_to_table, string_to_table), unqualified
  or in pg_catalog. Any other function in FROM, XMLTABLE, JSON_TABLE or
  TABLESAMPLE is refused as run_sql_forbidden_function.
* A (allowed): ordinary reads -- CTEs in scope, recursive CTEs, LATERAL,
  set operations, VALUES, ordinary casts -- still pass.
"""
from __future__ import annotations

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.sql_safety import validate_probe_query, validate_run_sql


ALLOWED = "allowed"
NON_SELECT = "run_sql_non_select"
RELATION = "run_sql_disallowed_relation"
FUNCTION = "run_sql_forbidden_function"
INVALID = "run_sql_invalid"
PROBE_REFUSED = "external_probe_query_refused"

ISSUE_2406_REPRO = (
    "WITH external_probe_cache AS (SELECT 1), "
    + "y AS (UPDATE external_probe_cache SET status = 'ok' RETURNING 1) "
    + "SELECT * FROM y"
)


def _fn(name: str) -> tuple[str, tuple[str, str]]:
    return (FUNCTION, ("function", name))


def _rel(name: str) -> tuple[str, tuple[str, str]]:
    return (RELATION, ("relation", name))


# ---- the exact repros from the issues ----

ISSUE_REPROS: list[tuple[str, object]] = [
    (ISSUE_2406_REPRO, (NON_SELECT, None)),
    ("SELECT 'pg_sleep(int)'::regprocedure", _fn("pg_sleep")),
    ("SELECT to_regprocedure('pg_sleep(double precision)')", _fn("pg_sleep")),
]

# ---- W: writes at any depth ----

WRITES: list[tuple[str, object]] = [
    ("WITH w AS (DELETE FROM okg.v_nodes RETURNING 1) SELECT * FROM w",
     (NON_SELECT, None)),
    ("WITH w AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT * FROM w",
     (NON_SELECT, None)),
    ("WITH w AS (UPDATE t SET x = 1 RETURNING 1) SELECT * FROM w",
     (NON_SELECT, None)),
    ("WITH w AS (MERGE INTO t USING okg.v_nodes n ON true "
     + "WHEN MATCHED THEN DELETE RETURNING 1) SELECT * FROM w",
     ((NON_SELECT, INVALID), None)),
    ("SELECT * FROM okg.v_nodes WHERE EXISTS ("
     + "WITH w AS (DELETE FROM t RETURNING 1) SELECT 1 FROM w)",
     (NON_SELECT, None)),
    ("WITH a AS (WITH b AS (INSERT INTO t VALUES (1) RETURNING 1) "
     + "SELECT * FROM b) SELECT * FROM a",
     (NON_SELECT, None)),
    ("WITH w AS (UPDATE okg.v_nodes SET subtype = 'x' RETURNING 1) "
     + "SELECT * FROM w",
     (NON_SELECT, None)),
    ("SELECT 1 INTO t", (NON_SELECT, None)),
    ("SELECT * INTO TEMP t FROM okg.v_nodes", (NON_SELECT, None)),
    ("SELECT * FROM okg.v_nodes FOR UPDATE", (NON_SELECT, None)),
    ("SELECT * FROM okg.v_nodes FOR SHARE", (NON_SELECT, None)),
    ("SELECT * FROM (SELECT * FROM okg.v_nodes FOR UPDATE) s",
     (NON_SELECT, None)),
]

# ---- S: CTE names resolve by scope, exactly as PostgreSQL does ----

SCOPE: list[tuple[str, object]] = [
    # Plain WITH: a CTE body does not see itself ...
    ("WITH graph_nodes AS (SELECT * FROM graph_nodes) "
     + "SELECT * FROM graph_nodes", _rel("graph_nodes")),
    # ... nor a later sibling.
    ("WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a",
     _rel("b")),
    # A CTE declared in a subquery is not visible outside it.
    ("SELECT * FROM (WITH t AS (SELECT 1) SELECT * FROM t) s, t", _rel("t")),
    ("SELECT * FROM okg.v_nodes WHERE EXISTS "
     + "(WITH t AS (SELECT 1) SELECT 1 FROM t) AND node_id IN "
     + "(SELECT x FROM t)", _rel("t")),
    # Nested WITH: the inner name is not visible to the outer query.
    ("WITH a AS (WITH inner_t AS (SELECT 1) SELECT * FROM inner_t) "
     + "SELECT * FROM a, inner_t", _rel("inner_t")),
    # Quoting changes the name: "Graph_Nodes" is not graph_nodes.
    ('WITH "Graph_Nodes" AS (SELECT 1) SELECT * FROM graph_nodes',
     _rel("graph_nodes")),
    ('WITH graph_nodes AS (SELECT 1) SELECT * FROM "Graph_Nodes"',
     _rel("graph_nodes")),
    # A schema-qualified reference never resolves to a CTE.
    ("WITH graph_nodes AS (SELECT 1) SELECT * FROM okg.graph_nodes",
     _rel("okg.graph_nodes")),
    ("WITH graph_nodes AS (SELECT 1) SELECT * FROM public.graph_nodes",
     _rel("public.graph_nodes")),
    ("WITH v_nodes AS (SELECT 1) SELECT * FROM public.v_nodes",
     _rel("public.v_nodes")),
    # A database-qualified name is never the view layer.
    ("SELECT * FROM okg_db.okg.v_nodes", _rel("okg_db.okg.v_nodes")),
    # LATERAL and set operations inherit scope, not out-of-scope names.
    ("WITH a AS (SELECT 1 AS x) SELECT * FROM a, LATERAL "
     + "(SELECT * FROM b WHERE b.x = a.x) l", _rel("b")),
    ("WITH a AS (SELECT 1) SELECT * FROM a UNION ALL SELECT * FROM c",
     _rel("c")),
]

# ---- R: strings that become catalog references ----

STRING_TO_REFERENCE: list[tuple[str, object]] = [
    # Naming a deny-listed function, every spelling.
    ("SELECT 'pg_sleep( int )'::regprocedure", _fn("pg_sleep")),
    ("SELECT 'pg_catalog.pg_sleep(int)'::regprocedure", _fn("pg_sleep")),
    ("SELECT '\"pg_sleep\"(int)'::regprocedure", _fn("pg_sleep")),
    ("SELECT 'pg_catalog.\"pg_sleep\"'::regproc", _fn("pg_sleep")),
    ("SELECT 'pg_sleep(int)'::pg_catalog.regprocedure", _fn("pg_sleep")),
    ("SELECT 'pg_sleep(int)'::\"regprocedure\"",
     ((FUNCTION, INVALID), ("function", "pg_sleep"))),
    ("SELECT CAST('pg_sleep(int)' AS regprocedure)", _fn("pg_sleep")),
    ("SELECT CAST('pg_sleep' AS pg_catalog.regproc)", _fn("pg_sleep")),
    ("SELECT regprocedure 'pg_sleep(int)'", _fn("pg_sleep")),
    ("SELECT pg_catalog.to_regprocedure('pg_sleep(int)')", _fn("pg_sleep")),
    ('SELECT "to_regprocedure"(\'pg_sleep(int)\')', _fn("pg_sleep")),
    ("SELECT to_regproc('pg_catalog.set_config')", _fn("set_config")),
    # ... in a subquery, LATERAL, VALUES and a CTE.
    ("SELECT * FROM okg.v_nodes WHERE node_id IN "
     + "(SELECT to_regprocedure('pg_sleep(int)')::text)", _fn("pg_sleep")),
    ("SELECT * FROM okg.v_nodes n, LATERAL "
     + "(SELECT 'pg_sleep(int)'::regprocedure AS p) l", _fn("pg_sleep")),
    ("SELECT * FROM (VALUES ('pg_sleep(int)'::regprocedure)) v(p)",
     _fn("pg_sleep")),
    ("WITH p AS (SELECT CAST('pg_sleep(int)' AS regprocedure) AS f) "
     + "SELECT * FROM p", _fn("pg_sleep")),
    # Name-resolving types and resolvers, whatever they name.
    ("SELECT 'okg.v_nodes'::regclass", (FUNCTION, None)),
    ("SELECT CAST('graph_nodes' AS pg_catalog.regclass)", (FUNCTION, None)),
    ("SELECT to_regclass('okg.graph_nodes')", (FUNCTION, None)),
    ("SELECT pg_catalog.to_regclass('okg.graph_nodes')", (FUNCTION, None)),
    ("SELECT 'upper'::regproc", (FUNCTION, None)),
    ("SELECT to_regproc('upper')", (FUNCTION, None)),
    ("SELECT '+'::regoper", (FUNCTION, None)),
    ("SELECT '+(int,int)'::regoperator", (FUNCTION, None)),
    ("SELECT 'public'::regnamespace", (FUNCTION, None)),
    ("SELECT to_regnamespace('public')", (FUNCTION, None)),
    ("SELECT 'text'::regtype", (FUNCTION, None)),
    ("SELECT 'postgres'::regrole", (FUNCTION, None)),
    ("SELECT '{upper}'::regproc[]", ((FUNCTION, INVALID), None)),
    # Column definition list: string -> regproc with no cast.
    ("SELECT x FROM json_to_record('{\"x\":\"pg_sleep\"}') "
     + "AS t(x regproc)", (FUNCTION, None)),
    # Catalog row types carry reg* fields parsed from text.
    ("SELECT json_populate_record(NULL::pg_catalog.pg_proc, '{}')",
     (FUNCTION, None)),
    ("SELECT ('(x)'::pg_type)", (FUNCTION, None)),
]

# ---- F: executors and writes behind a function call ----

FUNCTIONS: list[tuple[str, object]] = [
    ("SELECT query_to_xml('SELECT * FROM okg.graph_nodes', true, false, '')",
     _fn("query_to_xml")),
    ("SELECT table_to_xml('okg.graph_nodes', true, false, '')",
     _fn("table_to_xml")),
    ("SELECT nextval('okg.some_seq')", _fn("nextval")),
    ("SELECT pg_catalog.setval('okg.some_seq', 1)", _fn("setval")),
    ("SELECT pg_try_advisory_lock(1)", _fn("pg_try_advisory_lock")),
    ("SELECT lo_unlink(1)", _fn("lo_unlink")),
    ("SELECT dblink_exec('x', 'y')", _fn("dblink_exec")),
]

# ---- X: functions that execute SQL text ----

SQL_TEXT_EXECUTORS: list[tuple[str, object]] = [
    ("SELECT * FROM ts_stat('SELECT to_tsvector(''simple'', relname::text) "
     + "FROM pg_class')", (FUNCTION, None)),
    ("SELECT word FROM ts_stat('SELECT to_tsvector(''simple'', relname::text)"
     + " FROM pg_class WHERE relname = ''pg_authid''')", (FUNCTION, None)),
    ("SELECT s.* FROM okg.v_nodes n, ts_stat('SELECT to_tsvector(''simple'', "
     + "usename) FROM pg_user') s LIMIT 5", (FUNCTION, None)),
    ("SELECT s.word FROM okg.v_nodes n, ts_stat('SELECT to_tsvector("
     + "''simple'', coalesce(args::text, '''')) FROM okg.mcp_call_log') s",
     (FUNCTION, None)),
    ("SELECT pg_catalog.ts_stat('SELECT to_tsvector(''a'')')",
     (FUNCTION, None)),
    ("SELECT ts_rewrite('a'::tsquery, 'SELECT ''a''::tsquery, "
     + "''zz''::tsquery FROM pg_class LIMIT 1')", (FUNCTION, None)),
    ("SELECT * FROM ts_stat('WITH d AS (DELETE FROM t RETURNING 1) "
     + "SELECT to_tsvector(''a'')')", (FUNCTION, None)),
    ("SELECT query_to_xmlschema('select 1', true, true, '')",
     (FUNCTION, None)),
    ("SELECT query_to_xml_and_xmlschema('select 1', true, true, '')",
     (FUNCTION, None)),
    ("SELECT cursor_to_xml('c', 1, true, true, '')", (FUNCTION, None)),
    ("SELECT schema_to_xml('okg', true, true, '')", (FUNCTION, None)),
    ("SELECT database_to_xml(true, true, '')", (FUNCTION, None)),
    ("SELECT dblink('dbname=x', 'SELECT 1')", (FUNCTION, None)),
]

# ---- R, continued: casts written as function calls or fields ----

CAST_AS_CALL_OR_FIELD: list[tuple[str, object]] = [
    ("SELECT regprocedure('pg_sleep(float8)')", (FUNCTION, None)),
    ("SELECT pg_catalog.regprocedure('pg_sleep(float8)')", (FUNCTION, None)),
    ("SELECT regproc('pg_sleep')", (FUNCTION, None)),
    ("SELECT regprocin('pg_sleep')", (FUNCTION, None)),
    ("SELECT regprocedurein('pg_sleep(float8)')", (FUNCTION, None)),
    ("SELECT regclassin('okg.graph_nodes')", (FUNCTION, None)),
    ("SELECT regclass('pg_authid')", (FUNCTION, None)),
    ("SELECT text_regclass('okg.graph_nodes')", (FUNCTION, None)),
    ("SELECT ('pg_authid'::text).regclass", (FUNCTION, None)),
    ("SELECT x.regclass FROM (SELECT 'okg.graph_nodes'::text AS a) x",
     (FUNCTION, None)),
    ("SELECT pg_input_is_valid('pg_sleep', 'regproc')", (FUNCTION, None)),
    ("SELECT * FROM pg_input_error_info('pg_sleep', 'regproc')",
     (FUNCTION, None)),
    ("SELECT * FROM pg_get_object_address('function', '{pg_sleep}', "
     + "'{float8}')", (FUNCTION, None)),
]

# ---- F / M: server state and set-returning functions in FROM ----

SERVER_STATE_AND_FROM: list[tuple[str, object]] = [
    ("SELECT count(query) FROM pg_stat_get_activity(NULL)", (FUNCTION, None)),
    ("SELECT (pg_stat_get_activity(NULL)).query", (FUNCTION, None)),
    ("SELECT pg_stat_get_backend_activity(s) "
     + "FROM pg_stat_get_backend_idset() s", (FUNCTION, None)),
    ("SELECT count(*) FROM pg_show_all_settings()", (FUNCTION, None)),
    ("SELECT * FROM pg_hba_file_rules()", (FUNCTION, None)),
    ("SELECT * FROM pg_show_all_file_settings()", (FUNCTION, None)),
    ("SELECT * FROM pg_catalog.pg_ls_waldir()", (FUNCTION, None)),
    ("SELECT * FROM pg_control_system()", (FUNCTION, None)),
    ("SELECT * FROM pg_config()", (FUNCTION, None)),
    ("SELECT txid_current()", (FUNCTION, None)),
    ("SELECT pg_current_xact_id()", (FUNCTION, None)),
    ("SELECT pg_create_logical_replication_slot('x', 'test_decoding')",
     (FUNCTION, None)),
    ("SELECT pg_replication_origin_create('x')", (FUNCTION, None)),
    ("SELECT pg_import_system_collations('pg_catalog')", (FUNCTION, None)),
    ("SELECT pg_backup_start('x')", (FUNCTION, None)),
    ("SELECT pg_log_backend_memory_contexts(1)", (FUNCTION, None)),
    ("SELECT pg_promote()", (FUNCTION, None)),
    ("SELECT pg_export_snapshot()", (FUNCTION, None)),
    ("SELECT brin_summarize_new_values('okg.some_idx')", (FUNCTION, None)),
    ("SELECT gin_clean_pending_list('okg.some_idx')", (FUNCTION, None)),
    # Any FROM function outside the row-generator allow-list.
    ("SELECT * FROM pg_get_keywords()", (FUNCTION, None)),
    ("SELECT * FROM pg_options_to_table(ARRAY['a=b'])", (FUNCTION, None)),
    ("SELECT * FROM pg_available_extensions()", (FUNCTION, None)),
    ("SELECT * FROM ts_debug('x')", (FUNCTION, None)),
    ("SELECT * FROM okg.runtime_queue_health(600)", (FUNCTION, None)),
    ("SELECT * FROM public.generate_series(1, 3)", (FUNCTION, None)),
    ("SELECT * FROM okg.v_nodes n JOIN pg_show_all_settings() s ON true",
     (FUNCTION, None)),
    ("SELECT * FROM XMLTABLE('/a' PASSING '<a/>' COLUMNS x text PATH '.')",
     ((FUNCTION, INVALID), None)),
    ("SELECT * FROM JSON_TABLE('[1]'::jsonb, '$[*]' COLUMNS "
     + "(a int PATH '$'))", ((FUNCTION, INVALID), None)),
]

# ---- A: legitimate reads still pass ----

LEGITIMATE: list[tuple[str, object]] = [
    ("WITH RECURSIVE graph_nodes AS (SELECT 1 AS x UNION ALL "
     + "SELECT x + 1 FROM graph_nodes WHERE x < 3) SELECT * FROM graph_nodes",
     ALLOWED),
    ("WITH RECURSIVE a AS (SELECT * FROM b), b AS (SELECT 1 AS x) "
     + "SELECT * FROM a", ALLOWED),
    ("WITH a AS (SELECT * FROM okg.v_nodes), b AS (SELECT * FROM a) "
     + "SELECT * FROM b", ALLOWED),
    ("WITH a AS (SELECT node_id FROM okg.v_nodes) SELECT * FROM okg.v_nodes "
     + "WHERE node_id IN (SELECT node_id FROM a)", ALLOWED),
    ("WITH a AS (SELECT node_id FROM okg.v_nodes) SELECT * FROM okg.v_nodes n,"
     + " LATERAL (SELECT * FROM a WHERE a.node_id = n.node_id) l", ALLOWED),
    ("WITH a AS (SELECT 1 AS x) SELECT x FROM a UNION ALL SELECT x FROM a",
     ALLOWED),
    ("WITH a AS (SELECT 1 AS x), b AS (WITH c AS (SELECT * FROM a) "
     + "SELECT * FROM c) SELECT * FROM b", ALLOWED),
    ('WITH "Graph_Nodes" AS (SELECT 1) SELECT * FROM "Graph_Nodes"', ALLOWED),
    ('WITH graph_nodes AS (SELECT 1) SELECT * FROM "graph_nodes"', ALLOWED),
    ("WITH nodes_live AS (SELECT 1 AS x) SELECT * FROM nodes_live", ALLOWED),
    ("WITH c AS (SELECT * FROM okg.v_nodes) SELECT c.node_id::text, "
     + "(c.attrs->>'n')::int, CAST(c.attrs AS jsonb), '{}'::jsonb, "
     + "now()::date, 1::bigint, 1.5::numeric, 't'::boolean, "
     + "ARRAY['a']::text[] FROM c", ALLOWED),
    ("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(x, y)", ALLOWED),
    # Case-folded view name: the graph rewrite routes it like okg.v_nodes.
    ('SELECT * FROM "OKG"."V_NODES"', ALLOWED),
    ('SELECT * FROM okg."V_NODES"', ALLOWED),
    # Allow-listed row generators in FROM.
    ("SELECT * FROM generate_series(1, 3) g", ALLOWED),
    ("SELECT * FROM pg_catalog.generate_series(1, 3) g", ALLOWED),
    ("SELECT n.node_id, k FROM okg.v_nodes n, "
     + "LATERAL jsonb_object_keys(n.attrs) k", ALLOWED),
    ("SELECT * FROM unnest(ARRAY[1, 2]) WITH ORDINALITY u(x, i)", ALLOWED),
    ("SELECT * FROM ROWS FROM (generate_series(1, 3), "
     + "unnest(ARRAY['a'])) r", ALLOWED),
    ("SELECT e.key, e.value FROM okg.v_nodes n, "
     + "jsonb_each_text(n.attrs) e", ALLOWED),
    ("SELECT to_tsvector('simple', 'x') @@ 'x'::tsquery", ALLOWED),
    ("SELECT regexp_replace(subtype, 'a', 'b'), "
     + "regr_slope(1.0, 2.0) FROM okg.v_nodes GROUP BY subtype", ALLOWED),
    ("SELECT count(*) FROM okg.v_edges e JOIN okg.v_nodes n "
     + "ON n.node_id = e.src GROUP BY n.subtype", ALLOWED),
]

RUN_SQL_CASES: list[tuple[str, object]] = (
    ISSUE_REPROS + WRITES + SCOPE + STRING_TO_REFERENCE + FUNCTIONS
    + SQL_TEXT_EXECUTORS + CAST_AS_CALL_OR_FIELD + SERVER_STATE_AND_FROM
    + LEGITIMATE
)

# ---- the external probe check shares the function and type rules ----
# (sleeps stay allowed there: each probe runs under its own timeout)

PROBE_CASES: list[tuple[str, str]] = [
    ("SELECT 'pg_read_file(text)'::regprocedure", PROBE_REFUSED),
    ("SELECT to_regprocedure('pg_read_file(text)')", PROBE_REFUSED),
    ("SELECT 'x'::regclass", PROBE_REFUSED),
    (ISSUE_2406_REPRO, PROBE_REFUSED),
    ("SELECT * FROM ts_stat('SELECT to_tsvector(''a'')')", PROBE_REFUSED),
    ("SELECT regprocedure('pg_read_file(text)')", PROBE_REFUSED),
    ("SELECT ('x'::text).regclass", PROBE_REFUSED),
    ("SELECT count(*) FROM pg_stat_get_activity(NULL)", PROBE_REFUSED),
    ("SELECT pg_sleep(1)", ALLOWED),
]


def _outcome(fn, sql: str) -> tuple[str, dict]:
    try:
        fn(sql)
    except SubstrateError as err:
        return err.code, dict(err.data or {})
    return ALLOWED, {}


@pytest.mark.parametrize(("sql", "expected"), RUN_SQL_CASES)
def test_run_sql_case(sql: str, expected: object) -> None:
    code, data = _outcome(validate_run_sql, sql)
    if expected == ALLOWED:
        assert code == ALLOWED, (sql, code, data)
        return
    want, detail = expected  # type: ignore[misc]
    codes = want if isinstance(want, tuple) else (want,)
    assert code in codes, (sql, code, data)
    if detail is not None and code == codes[0]:
        key, value = detail
        assert data.get(key) == value, (sql, key, data)


@pytest.mark.parametrize(("sql", "expected"), PROBE_CASES)
def test_probe_case(sql: str, expected: str) -> None:
    code, data = _outcome(validate_probe_query, sql)
    assert code == expected, (sql, code, data)


def test_too_deep_to_classify_is_a_structured_refusal() -> None:
    """A tree deeper than the guard can follow is refused with a structured
    run_sql error, never an escaping RecursionError."""
    depth = 600
    sql = "SELECT * FROM (" * depth + "SELECT 1" + ") s" * depth
    code, _ = _outcome(validate_run_sql, sql)
    assert code.startswith("run_sql_") and code != ALLOWED
