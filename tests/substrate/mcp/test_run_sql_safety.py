"""Tests for the run_sql SQL-safety guard.

Per `add-mcp-run-sql-tool` (2026-04-27).

These are pure-Python tests — no DB required. The actual
execution path with `app_ro` role + `statement_timeout` is
exercised in `test_run_sql_tool.py`.
"""
from __future__ import annotations

import pytest


# ---------- happy path: SELECT / WITH allowed ----------

def test_simple_select_passes() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql("SELECT * FROM okg.v_nodes")


def test_runtime_queue_health_view_passes() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "SELECT status, sum(active_count) FROM okg.v_runtime_queue_health "
        "GROUP BY status"
    )


def test_with_cte_passes() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "WITH chunks AS (SELECT * FROM okg.v_doc_chunks) "
        "SELECT chunk_id FROM chunks LIMIT 10"
    )


def test_select_with_okg_helper_function_passes() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql("SELECT okg.current_pin()")


def test_trailing_semicolon_allowed() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql("SELECT * FROM okg.v_nodes ; ")


def test_leading_whitespace_and_comments() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "-- top comment\n"
        "/* block comment */\n"
        "  SELECT count(*) FROM okg.v_documents"
    )


def test_complex_query_with_joins_groupby_orderby() -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql("""
        SELECT path, count(*) AS chunk_count
        FROM okg.v_doc_chunks c
        JOIN okg.v_edges e ON e.src = c.chunk_id
        WHERE c.path LIKE 'docs/%'
        GROUP BY path
        ORDER BY 2 DESC
        LIMIT 10
    """)


# ---------- statement-prefix rejection ----------

@pytest.mark.parametrize("sql", [
    "DELETE FROM okg.nodes_live",
    "UPDATE okg.nodes_live SET subtype = 'x'",
    "INSERT INTO okg.nodes_live VALUES (1)",
    "DROP TABLE okg.nodes_live",
    "CREATE TABLE foo (x int)",
    "ALTER TABLE okg.nodes_live ADD COLUMN x int",
    "TRUNCATE okg.nodes_live",
    "GRANT SELECT ON okg.nodes_live TO public",
    "REVOKE SELECT ON okg.nodes_live FROM public",
    "SET search_path = public",
    "RESET search_path",
    "EXPLAIN SELECT 1",
    "VACUUM okg.nodes_live",
    "ANALYZE okg.nodes_live",
    "COPY okg.nodes_live TO STDOUT",
    "LOCK TABLE okg.nodes_live",
])
def test_non_select_refused(sql: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlNonSelect,
    )
    with pytest.raises(RunSqlNonSelect) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.code == "run_sql_non_select"
    assert "first_token" in exc_info.value.data


# ---------- multi-statement rejection ----------

def test_multi_statement_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlMultiStatement,
    )
    with pytest.raises(RunSqlMultiStatement):
        validate_run_sql("SELECT 1; SELECT 2")


def test_multi_statement_with_dml_refused_at_prefix_first() -> None:
    """Even with two statements, the first-token check fires
    when the first one isn't SELECT/WITH."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlNonSelect,
    )
    with pytest.raises(RunSqlNonSelect):
        validate_run_sql("DELETE FROM okg.nodes_live; SELECT 1")


def test_select_then_drop_refused() -> None:
    """Caller can't sneak a DROP after a SELECT — multi-statement
    refusal catches it."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlMultiStatement,
    )
    with pytest.raises(RunSqlMultiStatement):
        validate_run_sql("SELECT 1; DROP TABLE okg.nodes_live")


# ---------- relation allow-list ----------

@pytest.mark.parametrize("sql,relation", [
    ("SELECT * FROM okg.nodes_live", "okg.nodes_live"),
    ("SELECT * FROM okg.edges_live", "okg.edges_live"),
    ("SELECT * FROM okg.node_facts", "okg.node_facts"),
    ("SELECT * FROM okg.edge_facts", "okg.edge_facts"),
    ("SELECT * FROM okg.subtypes", "okg.subtypes"),
    ("SELECT * FROM okg.aliases", "okg.aliases"),
    ("SELECT * FROM okg.graph_generations", "okg.graph_generations"),
    ("SELECT * FROM okg.jobs", "okg.jobs"),
])
def test_disallowed_relation(sql: str, relation: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    with pytest.raises(RunSqlDisallowedRelation) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.code == "run_sql_disallowed_relation"
    assert exc_info.value.data["relation"] == relation


def test_disallowed_in_subquery_refused() -> None:
    """An attacker hiding the table reference in a subquery."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    with pytest.raises(RunSqlDisallowedRelation):
        validate_run_sql(
            "SELECT * FROM okg.v_nodes "
            "WHERE node_id IN (SELECT node_id FROM okg.nodes_live)"
        )


def test_disallowed_in_join_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    with pytest.raises(RunSqlDisallowedRelation):
        validate_run_sql(
            "SELECT * FROM okg.v_nodes n "
            "JOIN okg.edges_live e ON e.src = n.node_id"
        )


# ---------- forbidden functions ----------

@pytest.mark.parametrize("sql,fn", [
    ("SELECT pg_sleep(60)", "pg_sleep"),
    ("SELECT pg_terminate_backend(1)", "pg_terminate_backend"),
    ("SELECT pg_read_file('/etc/passwd')", "pg_read_file"),
    ("SELECT * FROM okg.v_nodes WHERE pg_advisory_lock(1) IS NOT NULL",
     "pg_advisory_lock"),
    ("SELECT set_config('okg.pin_generation', '999', true)", "set_config"),
    ("SELECT current_setting('okg.pin_generation')", "current_setting"),
])
def test_forbidden_functions(sql: str, fn: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlForbiddenFunction,
    )
    with pytest.raises(RunSqlForbiddenFunction) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.data["function"] == fn



# Advisory locks: a graph query that takes one can hold the history-lease or
# compaction lock key (lane C's #2893 review). Every lock, try-lock and unlock
# spelling is refused, qualified or quoted, on run_sql and on the probe path.
_ADVISORY_LOCK_FUNCTIONS = [
    f"pg_{try_}advisory_{xact}lock{shared}"
    for try_ in ("", "try_")
    for xact in ("", "xact_")
    for shared in ("", "_shared")
] + ["pg_advisory_unlock", "pg_advisory_unlock_shared", "pg_advisory_unlock_all"]


@pytest.mark.parametrize("fn", _ADVISORY_LOCK_FUNCTIONS)
@pytest.mark.parametrize("spelling", ["{fn}", "pg_catalog.{fn}", '"{upper}"'])
def test_every_advisory_lock_function_is_refused(fn: str, spelling: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        ProbeQueryRefused, RunSqlForbiddenFunction, validate_probe_query, validate_run_sql,
    )
    call = spelling.format(fn=fn, upper=fn.upper())
    args = "()" if fn == "pg_advisory_unlock_all" else "(1)"
    sql = f"SELECT {call}{args} FROM okg.v_nodes"
    with pytest.raises(RunSqlForbiddenFunction) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.data["function"] == fn
    with pytest.raises(ProbeQueryRefused):
        validate_probe_query(sql)


# ---------- string-literal hiding ----------

def test_disallowed_inside_string_literal_is_safe() -> None:
    """Innocent SELECT containing a forbidden token inside a
    string literal must not be rejected."""
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "SELECT 'DELETE FROM okg.nodes_live' AS payload "
        "FROM okg.v_nodes LIMIT 1"
    )


def test_attacker_cant_hide_table_in_string_literal() -> None:
    """Strings ARE stripped before scanning, so an attacker
    can't smuggle a privileged-table reference via a string —
    the reference disappears with the string. (Reverse of the
    test above: confirms stripping is symmetric.)"""
    from okg.substrate.mcp.sql_safety import validate_run_sql
    # This is allowed because the okg.nodes_live reference is
    # entirely inside the string literal — it's data, not a
    # query. The DB executes a no-op SELECT.
    validate_run_sql(
        "SELECT 'okg.nodes_live' AS data FROM okg.v_nodes LIMIT 1"
    )


def test_dollar_quoted_block() -> None:
    """Dollar-quoted strings are stripped too."""
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "SELECT $tag$DELETE FROM okg.nodes_live$tag$ AS payload "
        "FROM okg.v_nodes"
    )


# ---------- input-shape errors ----------

def test_none_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )
    with pytest.raises(RunSqlInvalid):
        validate_run_sql(None)


def test_empty_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )
    with pytest.raises(RunSqlInvalid):
        validate_run_sql("")


def test_whitespace_only_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )
    with pytest.raises(RunSqlInvalid):
        validate_run_sql("   \n  -- only a comment\n  ")


def test_non_string_refused() -> None:
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlInvalid,
    )
    with pytest.raises(RunSqlInvalid):
        validate_run_sql(123)  # type: ignore[arg-type]


# ---------- harden-mcp-run-sql-safety bypass tests ----------
#
# A design review of the prior regex safety guard
# named three concrete bypass surfaces. Each is now closed by the
# pglast AST walker (per harden-mcp-run-sql-safety, 2026-04-27).
# These parametrizations exist as binding regressions: if the AST
# walker ever loses the property "what the parser sees is what the
# database sees," these tests fail loudly.


@pytest.mark.parametrize("sql,canonical", [
    # Quoted relation name. The regex bypass: \b...\b breaks at the
    # `"`. The AST walker: pglast normalizes quoted/unquoted to the
    # same `relname`.
    ('SELECT * FROM okg."nodes_live" LIMIT 1', "okg.nodes_live"),
    # Schema and relation both quoted.
    ('SELECT * FROM "okg"."nodes_live" LIMIT 1', "okg.nodes_live"),
    # Mixed-case quoted: lower-cased canonical form (we fold to
    # lower for the allow-list check; case-sensitive comparison
    # would let `okg."NODES_LIVE"` evade).
    ('SELECT * FROM okg."NODES_LIVE" LIMIT 1', "okg.nodes_live"),
    # Quoted identifier in a JOIN.
    (
        'SELECT * FROM okg.v_nodes n '
        'JOIN okg."edges_live" e ON e.src = n.node_id',
        "okg.edges_live",
    ),
    # Quoted identifier in a subquery.
    (
        'SELECT * FROM okg.v_nodes WHERE node_id IN '
        '(SELECT node_id FROM "okg"."node_facts")',
        "okg.node_facts",
    ),
])
def test_quoted_identifier_bypass_attempts_rejected(
    sql: str, canonical: str,
) -> None:
    """Per harden-mcp-run-sql-safety P0-2(b): quoted identifiers
    must reach the relation allow-list with the same canonical
    form as their unquoted equivalents. The AST walker reads
    pglast's already-normalized `relname` field so case + quoting
    can't slip through."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    with pytest.raises(RunSqlDisallowedRelation) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.data["relation"] == canonical


def test_nested_block_comment_does_not_hide_forbidden_function() -> None:
    """Per harden-mcp-run-sql-safety P0-2(a): the prior regex was
    `/\\*.*?\\*/` (non-recursive). For a nested block comment the
    regex stripped only the inner `/* */`, leaving forbidden tokens
    in the residue. pglast's parser strips the entire nested
    comment correctly — and even if it left tokens, those tokens
    would be inside a comment as far as the parser is concerned,
    so the forbidden FuncCall wouldn't appear in the AST. Either
    way: the test passes cleanly without the bypass.

    NOTE: Because pglast strips the comment correctly, the SQL
    below is a *valid* SELECT against `okg.v_nodes` — not a
    forbidden-function rejection but an allowed query. We assert
    that no error is raised.
    """
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "SELECT 1 /* /* x */ pg_sleep(5) */ FROM okg.v_nodes"
    )


def test_nested_block_comment_with_active_forbidden_call_rejected() -> None:
    """Sanity check: an actual `pg_sleep(5)` outside a comment is
    rejected even when comments surround it."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlForbiddenFunction,
    )
    with pytest.raises(RunSqlForbiddenFunction) as exc_info:
        validate_run_sql(
            "SELECT /* x */ pg_sleep(5) FROM okg.v_nodes"
        )
    assert exc_info.value.data["function"] == "pg_sleep"


@pytest.mark.parametrize("sql,fn", [
    # Schema-qualified forbidden function. The regex's `\b(name)\(`
    # caught these by accident (`.` isn't a word char so `\b`
    # matched). The AST walker catches them by funcname[-1].
    ("SELECT pg_catalog.set_config('a','b',true)", "set_config"),
    ("SELECT pg_catalog.pg_sleep(5)", "pg_sleep"),
    (
        "SELECT pg_catalog.current_setting('okg.pin_generation')",
        "current_setting",
    ),
])
def test_schema_qualified_forbidden_function_rejected(
    sql: str, fn: str,
) -> None:
    """Per harden-mcp-run-sql-safety §"Forbidden-function deny-
    list": schema-qualified function calls reduce to the bare
    function name and trigger the same deny-list check."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlForbiddenFunction,
    )
    with pytest.raises(RunSqlForbiddenFunction) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.data["function"] == fn


@pytest.mark.parametrize("sql,fn", [
    # regproc casts wrap a string-literal function name.
    ("SELECT 'pg_sleep'::regproc", "pg_sleep"),
    ("SELECT 'set_config'::regproc", "set_config"),
    # regprocedure (with arg-list signature) — same canonicalization.
    ("SELECT 'pg_sleep(int)'::regprocedure", "pg_sleep"),
    # Schema-qualified within the cast string.
    ("SELECT 'pg_catalog.pg_sleep'::regproc", "pg_sleep"),
])
def test_regproc_cast_to_forbidden_function_rejected(
    sql: str, fn: str,
) -> None:
    """Per harden-mcp-run-sql-safety §"Forbidden-function deny-
    list": `'name'::regproc(...)` is equivalent to a direct call;
    the AST walker rejects via TypeCast→regproc detection.

    regprocedure carries the argument signature (e.g.
    `pg_sleep(int)`); the schema qualifier, quotes and argument list
    are stripped before the deny-list lookup (#2585)."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlForbiddenFunction,
    )
    with pytest.raises(RunSqlForbiddenFunction) as exc_info:
        validate_run_sql(sql)
    assert exc_info.value.data["function"] == fn


def test_cte_shadow_of_forbidden_table_name_allowed() -> None:
    """Per harden-mcp-run-sql-safety §"CTE shadowing": a CTE
    declared with a name that matches a forbidden physical table
    is allowed; the subsequent FROM resolves to the CTE, not the
    table. Without this rule, the agent loses standard SQL CTE
    naming (which the grammar SHOULD support)."""
    from okg.substrate.mcp.sql_safety import validate_run_sql
    validate_run_sql(
        "WITH nodes_live AS (SELECT 1 AS x) "
        "SELECT * FROM nodes_live"
    )


def test_cte_shadow_does_not_smuggle_schema_qualified_reference() -> None:
    """Even if a CTE is named `nodes_live`, a SCHEMA-QUALIFIED
    reference `okg.nodes_live` still points at the physical table.
    pglast's RangeVar carries the schemaname; we only allow the
    CTE bypass when schemaname is None."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    with pytest.raises(RunSqlDisallowedRelation):
        validate_run_sql(
            "WITH nodes_live AS (SELECT 1 AS x) "
            "SELECT * FROM okg.nodes_live"
        )


def test_unicode_escape_identifier_rejected() -> None:
    """Postgres supports U&'\\xxxx' Unicode-escape syntax for
    identifiers. The regex couldn't normalize these — pglast's
    parser does (the AST has the resolved identifier). NOTE: this
    test uses the U& syntax that the parser accepts; the resolved
    form is `okg.nodes_live`."""
    from okg.substrate.mcp.sql_safety import (
        validate_run_sql, RunSqlDisallowedRelation,
    )
    # `U&"nodes_live"` is the same as `"nodes_live"` (no escape
    # sequence). The point is that pglast strips the U& prefix
    # and exposes the resolved relname, which we canonicalize and
    # check against the allow-list.
    with pytest.raises(RunSqlDisallowedRelation) as exc_info:
        validate_run_sql(
            'SELECT * FROM okg.U&"nodes_live" LIMIT 1'
        )
    assert exc_info.value.data["relation"] == "okg.nodes_live"


# ---------- sql-safety-without-pglast: parser-differential bypasses ----------
#
# The guard parses with sqlglot, cross-checked by a lexer written from
# PostgreSQL's scanner rules (okg.substrate.postgres_sql). Each case below is
# a spelling where a parser that disagrees with PostgreSQL could hide SQL the
# database would execute. Where PostgreSQL itself reads the text as harmless,
# the guard must allow it; everywhere else it must refuse.


def _refusal(sql: str):
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, validate_run_sql

    try:
        validate_run_sql(sql)
    except RunSqlInvalid as exc:
        return exc
    return None


@pytest.mark.parametrize("sql", [
    # A nested comment is one comment to PostgreSQL; the hidden text never runs.
    "SELECT 1 /* /* */ FROM okg.nodes_live */ FROM okg.v_nodes",
    "SELECT 1 FROM okg.v_nodes /* ; DELETE FROM okg.nodes_live */",
    "SELECT 1 FROM okg.v_nodes -- ; DELETE FROM okg.nodes_live",
    # `;` inside strings and dollar quotes is data.
    "SELECT ';DELETE FROM okg.nodes_live' AS s FROM okg.v_nodes",
    "SELECT $a$ ; DELETE FROM okg.nodes_live; $a$ AS s FROM okg.v_nodes",
    "SELECT $a$ $b$ ; $A$ ; $a$ AS s FROM okg.v_nodes",
    # In E'' a backslash escapes the quote, so the string runs to the end.
    "SELECT E'\\' , (SELECT 1 FROM okg.nodes_live) --' AS s FROM okg.v_nodes",
    # Quoted and Unicode-escaped spellings of an allowed view.
    'SELECT * FROM "okg"."v_nodes"',
    'SELECT * FROM okg.U&"v_nodes"',
    'SELECT * FROM U&"okg".U&"v_n\\006fdes"',
    # A recursive CTE may name itself.
    "WITH RECURSIVE r AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM r "
    "WHERE x < 3) SELECT * FROM r",
    # Later CTEs see earlier siblings.
    "WITH a AS (SELECT 1 AS x), b AS (SELECT * FROM a) SELECT * FROM b",
    # Keywords sqlglot consumes as syntax without an Identifier node.
    "SELECT node_id FROM okg.v_nodes ORDER BY updated_at DESC NULLS LAST",
    "SELECT extract(year FROM created_at) FROM okg.v_nodes",
    "SELECT created_at AT TIME ZONE 'UTC' FROM okg.v_nodes",
    "SELECT row_number() OVER (PARTITION BY subtype ORDER BY node_id "
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM okg.v_nodes",
    "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY n) FROM okg.v_nodes",
    "SELECT trim(both ' ' from text) FROM okg.v_nodes",
    'SELECT * FROM "OKG"."V_NODES"',
    # Allow-listed FROM functions.
    "SELECT * FROM generate_series(1, 3) g",
    "SELECT * FROM pg_catalog.generate_series(1, 3) g",
    "SELECT * FROM unnest(ARRAY[1, 2]) u",
    "SELECT n.node_id, e FROM okg.v_nodes n, LATERAL jsonb_array_elements(n.attrs->'a') e",
    "SELECT * FROM okg.v_nodes n JOIN LATERAL jsonb_each_text(n.attrs) kv ON true",
    "SELECT * FROM (VALUES (1), (2)) v(x)",
    # PR #2886's FROM allow-list: the json row generators, and ROWS FROM
    # when every function inside is allow-listed.
    "SELECT * FROM json_to_record('{}') AS t(a int)",
    "SELECT * FROM ROWS FROM (generate_series(1, 2))",
    # WITH attachment PostgreSQL accepts: the list covers the whole set
    # operation, and an outer WITH is visible inside nested queries.
    "WITH x AS (SELECT 1) SELECT * FROM x UNION SELECT * FROM x",
    "WITH x AS (SELECT 1) (SELECT * FROM x) UNION (SELECT * FROM x)",
    "(WITH x AS (SELECT 1) SELECT * FROM x UNION SELECT * FROM x)",
    "SELECT 1 UNION (WITH x AS (SELECT 1) SELECT * FROM x)",
    "WITH x AS (SELECT 1) SELECT * FROM (SELECT * FROM (SELECT * FROM x) a) b",
])
def test_parser_agrees_with_postgres_on_harmless_text(sql: str) -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql

    validate_run_sql(sql)


@pytest.mark.parametrize("sql,code", [
    # Multiple statements, however separated.
    ("SELECT 1 FROM okg.v_nodes;\nDELETE FROM okg.nodes_live", "run_sql_multi_statement"),
    ("SELECT 1 -- c\n; SELECT 2", "run_sql_multi_statement"),
    ("SELECT 1 /* c */; SELECT 2", "run_sql_multi_statement"),
    # A backslash in a plain string means different things under
    # standard_conforming_strings on and off. run_sql runs the text with it
    # on (issue #3666), and the checks read it that way: the string ends at
    # the quote after the backslash, so the subquery is code and refused.
    ("SELECT '\\' , (SELECT 1 FROM okg.nodes_live) --' FROM okg.v_nodes", "run_sql_disallowed_relation"),
    # A backslash-escaped backslash closes the E'' string; the subquery is code.
    ("SELECT E'\\\\' , (SELECT 1 FROM okg.nodes_live) AS s", "run_sql_disallowed_relation"),
    # Statements that are not queries.
    ("COPY okg.nodes_live TO STDOUT", "run_sql_non_select"),
    ("COPY (SELECT 1) TO PROGRAM 'id'", "run_sql_non_select"),
    ("DO $$ BEGIN PERFORM 1; END $$", "run_sql_non_select"),
    ("CALL okg.something()", "run_sql_non_select"),
    ("EXPLAIN ANALYZE DELETE FROM okg.nodes_live", "run_sql_non_select"),
    ("SET ROLE postgres", "run_sql_non_select"),
    ("BEGIN; SELECT 1", "run_sql_non_select"),
    ("/* lead */ DELETE FROM okg.nodes_live", "run_sql_non_select"),
    # Writes hidden inside a query.
    ("SELECT * INTO okg.v_evil FROM okg.v_nodes", "run_sql_non_select"),
    ("SELECT * INTO TEMP stolen FROM okg.v_nodes", "run_sql_non_select"),
    ("WITH d AS (DELETE FROM okg.v_nodes RETURNING 1) SELECT * FROM d", "run_sql_non_select"),
    ("WITH u AS (UPDATE okg.v_nodes SET subtype = 'x' RETURNING 1) SELECT * FROM u", "run_sql_non_select"),
    ("WITH i AS (INSERT INTO okg.v_nodes VALUES (1) RETURNING 1) SELECT * FROM i", "run_sql_non_select"),
    ("SELECT * FROM okg.v_nodes FOR UPDATE", "run_sql_non_select"),
    ("SELECT * FROM okg.v_nodes FOR SHARE", "run_sql_non_select"),
    ("SELECT * FROM okg.v_nodes FOR NO KEY UPDATE", "run_sql_non_select"),
    ("SELECT * FROM okg.v_nodes FOR KEY SHARE", "run_sql_non_select"),
    # sqlglot drops `OF` from its tree; the strict identifier check refuses.
    ("SELECT * FROM okg.v_nodes FOR UPDATE OF v_nodes SKIP LOCKED", "run_sql_invalid"),
    # Functions that sleep, read files, open connections, write, or run a
    # SQL string past the relation allow-list.
    ("SELECT pg_sleep(1)", "run_sql_forbidden_function"),
    ('SELECT "pg_sleep"(1)', "run_sql_forbidden_function"),
    ('SELECT U&"pg_sl\\0065ep"(1)', "run_sql_forbidden_function"),
    ('SELECT pg_catalog."set_config"(\'a\', \'b\', false)', "run_sql_forbidden_function"),
    ("SELECT dblink('host=x', 'SELECT 1')", "run_sql_forbidden_function"),
    ("SELECT dblink_exec('host=x', 'DROP TABLE t')", "run_sql_forbidden_function"),
    ("SELECT lo_import('/etc/hosts')", "run_sql_forbidden_function"),
    ("SELECT pg_read_file('/etc/hosts')", "run_sql_forbidden_function"),
    ("SELECT pg_ls_waldir()", "run_sql_forbidden_function"),
    ("SELECT pg_try_advisory_lock(1)", "run_sql_forbidden_function"),
    ("SELECT nextval('okg.some_seq')", "run_sql_forbidden_function"),
    ("SELECT pg_notify('c', 'x')", "run_sql_forbidden_function"),
    ("SELECT query_to_xml('SELECT * FROM okg.nodes_live', true, false, '')", "run_sql_forbidden_function"),
    ("SELECT table_to_xml('okg.nodes_live', true, false, '')", "run_sql_forbidden_function"),
    ("SELECT * FROM okg.v_nodes, LATERAL pg_sleep(1)", "run_sql_forbidden_function"),
    ("SELECT * FROM okg.v_nodes WHERE node_id = (SELECT current_setting('x'))", "run_sql_forbidden_function"),
    ("SELECT 'set_config'::regproc", "run_sql_forbidden_function"),
    ("SELECT 'count'::regprocedure", "run_sql_forbidden_function"),
    ("SELECT CAST('pg_sleep' AS pg_catalog.regproc)", "run_sql_forbidden_function"),
    # Relations outside okg.v_*, however spelled or nested.
    ("SELECT * FROM public.v_nodes", "run_sql_disallowed_relation"),
    ("SELECT * FROM v_nodes", "run_sql_disallowed_relation"),
    ("SELECT * FROM pg_catalog.pg_authid", "run_sql_disallowed_relation"),
    ("SELECT * FROM information_schema.tables", "run_sql_disallowed_relation"),
    ("SELECT * FROM otherdb.okg.v_nodes", "run_sql_disallowed_relation"),
    ('SELECT * FROM U&"okg".U&"\\006eodes_live"', "run_sql_disallowed_relation"),
    ("SELECT * FROM ONLY okg.nodes_live", "run_sql_disallowed_relation"),
    ("SELECT (SELECT count(*) FROM okg.jobs) FROM okg.v_nodes", "run_sql_disallowed_relation"),
    ("SELECT * FROM okg.v_nodes ORDER BY (SELECT 1 FROM okg.jobs LIMIT 1)", "run_sql_disallowed_relation"),
    ("SELECT coalesce((SELECT 1 FROM okg.jobs), 0)", "run_sql_disallowed_relation"),
    (
        "SELECT * FROM okg.v_nodes WHERE EXISTS "
        "(SELECT 1 FROM (SELECT * FROM (SELECT * FROM okg.edges_live) a) b)",
        "run_sql_disallowed_relation",
    ),
    ("SELECT 1 FROM okg.v_nodes UNION SELECT 1 FROM okg.nodes_live", "run_sql_disallowed_relation"),
    ("VALUES ((SELECT 1 FROM okg.nodes_live))", "run_sql_disallowed_relation"),
    # CTE names follow PostgreSQL's scoping: a name outside its WITH, a later
    # sibling, or a non-recursive self-reference resolves to a real table.
    ("WITH a AS (SELECT * FROM nodes_live), nodes_live AS (SELECT 1) SELECT * FROM a", "run_sql_disallowed_relation"),
    ("WITH nodes_live AS (SELECT * FROM nodes_live) SELECT * FROM nodes_live", "run_sql_disallowed_relation"),
    (
        "SELECT * FROM (WITH nodes_live AS (SELECT 1) SELECT * FROM nodes_live) s, nodes_live",
        "run_sql_disallowed_relation",
    ),
    # Text either parser cannot read the same way is refused, never guessed.
    ("SELECT 1FROM okg.nodes_live", "run_sql_invalid"),
    ("TABLE okg.nodes_live", "run_sql_invalid"),
    ("SELECT U&'d!0061t' UESCAPE '!' FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT 1 /* unterminated", "run_sql_invalid"),
    ("SELECT $a$ unterminated", "run_sql_invalid"),
    ('SELECT "" FROM okg.v_nodes', "run_sql_invalid"),
    # Added after the independent review.
    ("SELECT 1 ` , 2 FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT E'a'\n'\\' , (SELECT 1 FROM okg.nodes_live) --' FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT 'a'\n'b' FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT B'1''0' FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT 1_ FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT 0b12 FROM okg.v_nodes", "run_sql_invalid"),
    ("SELECT 1 FROM okg.v_nodes WHERE 1 OPERATOR(nodes_live.=) 1", "run_sql_forbidden_function"),
    ("SELECT loread(0, 1)", "run_sql_forbidden_function"),
    # Third review: FROM items are allow-listed, not deny-listed.
    ("SELECT * FROM pg_catalog.pg_show_all_settings() s", "run_sql_forbidden_function"),
    ("SELECT * FROM pg_stat_get_activity(NULL) a", "run_sql_forbidden_function"),
    ("SELECT * FROM okg.v_nodes, LATERAL pg_get_viewdef('okg.v_nodes') d", "run_sql_forbidden_function"),
    # json_to_record and ROWS FROM of allow-listed generators pass (PR
    # #2886); a reg* column type, or any other function inside ROWS FROM,
    # still refuses.
    ("SELECT * FROM json_to_record('{}') AS t(a regclass)", "run_sql_forbidden_function"),
    ("SELECT * FROM ROWS FROM (pg_get_keywords())", "run_sql_forbidden_function"),
    ("SELECT * FROM ROWS FROM (generate_series(1, 2), pg_get_keywords())", "run_sql_forbidden_function"),
    ("SELECT * FROM ROWS FROM (okg.generate_series(1, 2))", "run_sql_forbidden_function"),
    # Refused earlier: sqlglot drops `PASSING` from its tree.
    ("SELECT * FROM XMLTABLE('/a' PASSING '<a/>' COLUMNS a int) t", "run_sql_invalid"),
    ("SELECT * FROM okg.generate_series(1, 2) g", "run_sql_forbidden_function"),
    ("SELECT pg_relation_filepath('okg.v_nodes')", "run_sql_forbidden_function"),
    ("SELECT pg_drop_replication_slot('s')", "run_sql_forbidden_function"),
    ("SELECT pg_copy_logical_replication_slot('a', 'b')", "run_sql_forbidden_function"),
    ("SELECT brin_summarize_new_values('i')", "run_sql_forbidden_function"),
    ("SELECT gin_clean_pending_list('i')", "run_sql_forbidden_function"),
    # Third review: WITH attachment. A CTE is visible only inside the query
    # it is attached to; PostgreSQL resolves the same name elsewhere through
    # search_path.
    ("(WITH x AS (SELECT 1) SELECT * FROM x) UNION SELECT * FROM x", "run_sql_disallowed_relation"),
    ("SELECT * FROM x UNION (WITH x AS (SELECT 1) SELECT * FROM x)", "run_sql_disallowed_relation"),
    (
        "((WITH x AS (SELECT 1) SELECT * FROM x) UNION (SELECT 1)) UNION SELECT * FROM x",
        "run_sql_disallowed_relation",
    ),
    ("SELECT * FROM (WITH x AS (SELECT 1) SELECT * FROM x) a, (SELECT * FROM x) b", "run_sql_disallowed_relation"),
    (
        "SELECT * FROM (WITH x AS (SELECT 1) SELECT * FROM x) a "
        "WHERE EXISTS (SELECT * FROM x)",
        "run_sql_disallowed_relation",
    ),
    (
        "SELECT * FROM okg.v_nodes WHERE node_id IN "
        "(WITH y AS (SELECT 1) SELECT * FROM y) AND EXISTS (SELECT * FROM y)",
        "run_sql_disallowed_relation",
    ),
])
def test_parser_differential_bypasses_refused(sql: str, code: str) -> None:
    refusal = _refusal(sql)
    assert refusal is not None, f"guard allowed {sql!r}"
    assert refusal.code == code, (sql, refusal.code, refusal.data)


def test_checked_run_sql_returns_the_decoded_statement() -> None:
    from okg.substrate.mcp.sql_safety import checked_run_sql

    assert checked_run_sql(
        "-- lead\nSELECT * FROM okg.U&\"v_n\\006fdes\" /* keep */ WHERE 1 = 1 ;"
    ) == 'SELECT * FROM okg."v_nodes" /* keep */ WHERE 1 = 1'


def test_lexer_follows_postgres_comment_and_quote_rules() -> None:
    from okg.substrate import postgres_sql as pg

    tokens = pg.lex(
        "SELECT /* a /* b */ c */ 'x''y', E'\\'', $t$ $T$ $t$, \"q\"\"r\""
    )
    kinds = [(t.kind, t.value) for t in tokens if t.kind != pg.WORD]
    assert kinds == [
        (pg.BLOCK_COMMENT, ""),
        (pg.STRING, "x'y"),
        (pg.PUNCT, ","),
        (pg.ESTRING, "'"),
        (pg.PUNCT, ","),
        (pg.DOLLAR, " $T$ "),
        (pg.PUNCT, ","),
        (pg.QIDENT, 'q"r'),
    ]
    assert pg.normalize('SELECT U&"a\\0062" FROM U&"x\\+000079"') == (
        'SELECT "ab" FROM "xy"'
    )
    with pytest.raises(pg.PostgresSqlError):
        pg.lex("SELECT U&'\\zzzz'")


def test_cross_check_refuses_when_sqlglot_reads_a_literal_differently() -> None:
    """Simulate a sqlglot tokenizer that ends a string early."""
    from sqlglot.dialects.postgres import Postgres

    from okg.substrate import postgres_sql as pg

    text = "SELECT 'a;b' FROM okg.v_nodes"
    tokens = pg.lex(text)
    glot = list(Postgres().tokenize(text))
    literal = next(t for t in glot if t.token_type.name == "STRING")
    shortened = type(literal)(
        literal.token_type,
        literal.text,
        literal.line,
        literal.col,
        literal.start,
        literal.end - 2,
        literal.comments,
    )
    glot[glot.index(literal)] = shortened
    with pytest.raises(pg.PostgresSqlError, match="disagree"):
        pg._cross_check(text, tokens, glot)


def test_rewrite_graph_views_splices_relations_and_keeps_literals() -> None:
    from okg.substrate.mcp.run_sql import _rewrite_graph_views

    sql = (
        "SELECT 'okg.v_nodes' AS s, n.node_id FROM \"okg\".\"v_nodes\" n "
        "JOIN okg.U&\"v_edges\" e ON e.src = n.node_id -- okg.v_nodes"
    )
    assert _rewrite_graph_views(sql) == (
        "SELECT 'okg.v_nodes' AS s, n.node_id FROM okg.graph_nodes n "
        "JOIN okg.graph_edges e ON e.src = n.node_id -- okg.v_nodes"
    )
    assert _rewrite_graph_views("SELECT 1 FROM okg.v_documents") == (
        "SELECT 1 FROM okg.v_documents"
    )


class _NoViews:
    """A connection whose pg_get_viewdef lookup finds no view."""

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[str, ...]) -> None:
            return None

        def fetchone(self):
            return None

    def cursor(self):
        return self._Cursor()


_CHECKOUT_NODES = (
    'SELECT "node_id" FROM "_chronos_nodes" WHERE live_lo <= :_chronos_branch_point'
)


def test_graph_path_executes_the_checked_text_with_only_names_spliced() -> None:
    """#2899 (fixed by #2906) on the sqlglot port: the executed text is the
    checked text, byte for byte, except the relation name and the row-limit
    wrapper. sqlglot's renderer would change ``x::float`` and the comments."""
    from okg.substrate.mcp import graph_sql_splice as splice
    from okg.substrate.mcp.sql_safety import checked_run_sql

    checked = checked_run_sql(
        "/* lead */ SELECT x::float, 'it''s' AS s /* keep */ FROM "
        'okg.U&"v_n\\006fdes" n -- tail\n ; -- trailing'
    )
    assert checked == (
        "SELECT x::float, 'it''s' AS s /* keep */ FROM okg.\"v_nodes\" n"
    )
    plan = splice.plan_graph_sql(checked, _NoViews())
    assert plan.routes_to_graph
    spliced = splice.render_graph_sql(
        plan, {"okg.graph_nodes": _CHECKOUT_NODES}, {"_chronos_branch_point": 7},
        row_cap=2,
    )
    splice.verify_splice(plan, spliced)
    head, _, tail = checked.partition('okg."v_nodes"')
    assert spliced.sql == (
        "SELECT * FROM (\n" + head
        + '(SELECT "node_id" FROM "_chronos_nodes" WHERE live_lo <= 7)'
        + tail + "\n) AS _okg_bounded LIMIT 3"
    )


# The #2899 repro. By PostgreSQL's rules E'\\' is one backslash and
# pg_sleep(1) sits inside the string ' || pg_sleep(1) || ', so the checker
# passes it as data. A renderer that reads the E-string's escapes differently
# would turn that string text into code; the graph path therefore refuses any
# E'...' string before it splices anything. The one-backslash spelling leaves
# a string unterminated and is refused by the checker's lexer.
ISSUE_2899_REPRO = "SELECT E'\\\\' AS a, ' || pg_sleep(1) || ' AS b FROM okg.v_nodes"
ISSUE_2899_REPRO_ONE_BACKSLASH = (
    "SELECT E'\\' AS a, ' || pg_sleep(1) || ' AS b FROM okg.v_nodes"
)


def test_issue_2899_repro_is_refused_before_the_graph_path_runs() -> None:
    from okg.substrate.mcp import graph_sql_splice as splice
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, checked_run_sql

    checked = checked_run_sql(ISSUE_2899_REPRO)
    assert "E'\\\\'" in checked
    plan = splice.plan_graph_sql(checked, _NoViews())
    assert plan.routes_to_graph
    with pytest.raises(splice.RunSqlGraphLiteralRefused) as err:
        splice.refuse_escape_dependent_literals(checked)
    assert err.value.data["form"] == "E'...' string"
    with pytest.raises(RunSqlInvalid) as parse_err:
        checked_run_sql(ISSUE_2899_REPRO_ONE_BACKSLASH)
    assert parse_err.value.code == "run_sql_invalid"


def test_a_backslash_in_a_plain_string_passes_the_run_sql_checks_only() -> None:
    """Issue #3666: run_sql accepts '\\d+' (it sets standard strings where
    the text runs), and reads it as one string, so a relation hidden after a
    backslash is still seen; every other strict caller still refuses it."""
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, checked_run_sql
    from okg.substrate.postgres_sql import PostgresSqlError, parse

    sql = "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ '\\d+'"
    assert checked_run_sql(sql) == sql
    # Under standard strings 'a\' ends at the quote after the backslash: the
    # subquery is code, and its relation is refused.
    with pytest.raises(RunSqlInvalid) as err:
        checked_run_sql("SELECT 'a\\' , (SELECT 1 FROM okg.nodes_live) AS s FROM okg.v_nodes")
    assert err.value.code == "run_sql_disallowed_relation"
    # A string continuation can switch a later string to escape reading; it
    # stays refused.
    with pytest.raises(RunSqlInvalid, match="string continuation"):
        checked_run_sql("SELECT E'a'\n'\\d' FROM okg.v_nodes")
    with pytest.raises(PostgresSqlError, match="backslash"):
        parse(sql, strict=True)


def test_routing_helpers_read_relations_not_strings() -> None:
    from okg.substrate.mcp.run_sql import (
        _references_composite_view,
        _references_physical_graph_view,
    )

    assert _references_physical_graph_view('SELECT 1 FROM "okg"."graph_nodes"')
    assert not _references_physical_graph_view("SELECT 'okg.graph_nodes'")
    assert _references_composite_view("SELECT 1 FROM okg.U&\"v_documents\"")
    assert not _references_composite_view("SELECT 1 FROM okg.v_nodes")


def test_invariant_relation_scan_and_canonicalize() -> None:
    from okg.substrate.catalog.invariant_graph_read import (
        _canonicalize,
        dropped_graph_relations_in,
    )

    assert dropped_graph_relations_in(
        "SELECT 1 FROM okg.\"nodes_live\" WHERE x = %(pin)s -- okg.edges_live"
    ) == ["okg.nodes_live"]
    assert _canonicalize('SELECT node_id FROM okg.U&"graph_node\\0073"') == (
        'SELECT node_id FROM okg."graph_nodes"'
    )

# ---------- external probe queries (review of #2401) ----------

_PROBE_CTE_WRITE = (
    "WITH w AS (DELETE FROM okg.external_probe_cache RETURNING 1) "
    "SELECT 1 FROM w"
)
_PROBE_NESTED_WRITE = (
    "SELECT * FROM (SELECT 1) s WHERE EXISTS "
    "(WITH w AS (INSERT INTO t VALUES (1) RETURNING 1) SELECT 1 FROM w)"
)


@pytest.mark.parametrize(("sql", "reason"), [
    ("", "empty"),
    ("   ", "empty"),
    ("-- only a comment", "empty"),
    ("SELEC 1", "parse_error"),
    ("COMMIT; INSERT INTO t VALUES (1); SELECT 1", "multi_statement"),
    ("SELECT 1; SELECT 2", "multi_statement"),
    ("INSERT INTO t VALUES (1)", "non_select"),
    ("COMMIT", "non_select"),
    ("SET ROLE postgres", "non_select"),
    (_PROBE_CTE_WRITE, "data_modifying"),
    (_PROBE_NESTED_WRITE, "data_modifying"),
    ("SELECT 1 INTO TEMP t", "select_into"),
    ("SELECT * FROM okg.external_probe_cache FOR UPDATE", "locking_clause"),
    ("SELECT pg_catalog.set_config('x', 'y', false)", "forbidden_function"),
    ("SELECT 'pg_read_file'::regproc", "forbidden_function"),
    ("SELECT dblink('x', 'y')", "forbidden_function"),
    # sql-safety-without-pglast: the same rules on the sqlglot front end.
    ("SELECT 1 /* x */ ; -- y\n COMMIT", "multi_statement"),
    ("SELECT ';' ; COMMIT", "multi_statement"),
    ("EXPLAIN ANALYZE DELETE FROM t", "non_select"),
    ("DO $$ BEGIN END $$", "non_select"),
    ("COPY (SELECT 1) TO PROGRAM 'id'", "non_select"),
    ("SELECT * FROM t FOR SHARE", "locking_clause"),
    ('SELECT U&"pg_read_fil\\0065"(\'x\')', "forbidden_function"),
    ("SELECT query_to_xml('DELETE FROM t', true, false, '')", "forbidden_function"),
    ("SELECT 'x'::pg_catalog.regprocedure", "forbidden_function"),
    ("SELECT '\\' , 1", "parse_error"),
])
def test_probe_query_refusals_are_named(sql: str, reason: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        PROBE_QUERY_REFUSED,
        ProbeQueryRefused,
        validate_probe_query,
    )

    with pytest.raises(ProbeQueryRefused) as err:
        validate_probe_query(sql)
    assert err.value.code == PROBE_QUERY_REFUSED
    assert err.value.data["reason"] == reason


@pytest.mark.parametrize("target", ["lex", "parse"])
def test_probe_query_internal_error_refuses_as_parse_error(
    monkeypatch: pytest.MonkeyPatch, target: str,
) -> None:
    """Any exception while reading the query refuses as parse_error, as
    dev's pglast version did, and the refusal neither quotes the query nor
    chains the original exception."""
    import okg.substrate.postgres_sql as pg
    from okg.substrate.mcp.sql_safety import ProbeQueryRefused, validate_probe_query

    def boom(*_args, **_kwargs):
        raise ValueError("internal detail with SELECT secret_marker")

    monkeypatch.setattr(pg, target, boom)
    with pytest.raises(ProbeQueryRefused) as err:
        validate_probe_query("SELECT secret_marker FROM t")
    assert err.value.data["reason"] == "parse_error"
    assert "secret_marker" not in str(err.value)
    assert err.value.__cause__ is None
    assert err.value.__suppress_context__ is True


def test_run_sql_internal_error_is_a_structured_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.postgres_sql as pg
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, validate_run_sql

    def boom(*_args, **_kwargs):
        raise ValueError("internal")

    monkeypatch.setattr(pg, "parse", boom)
    with pytest.raises(RunSqlInvalid) as err:
        validate_run_sql("SELECT 1 FROM okg.v_nodes")
    assert err.value.code == "run_sql_invalid"


def test_every_refused_node_class_exists_in_sqlglot() -> None:
    """A sqlglot rename must not silently shrink the refused set."""
    from sqlglot import exp

    from okg.substrate.mcp import sql_safety

    missing = [
        name for name in sql_safety._REFUSED_NODE_NAMES
        if not isinstance(getattr(exp, name, None), type)
    ]
    assert missing == []
    assert len(sql_safety._refused_node_types()) == len(sql_safety._REFUSED_NODE_NAMES)


def test_refused_node_types_fails_loudly_on_a_missing_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.mcp import sql_safety

    monkeypatch.setattr(
        sql_safety,
        "_REFUSED_NODE_NAMES",
        (*sql_safety._REFUSED_NODE_NAMES, "NoSuchSqlglotNode"),
    )
    with pytest.raises(RuntimeError, match="NoSuchSqlglotNode"):
        sql_safety._refused_node_types()


@pytest.mark.parametrize("sql", [
    "SELECT regproc 'pg_sleep'",
    "SELECT regprocedure 'pg_read_file(text)'",
])
def test_bare_regproc_typed_literal_is_refused(sql: str) -> None:
    """`regproc 'x'` (a typed literal, no `::`) is parsed as a cast and
    refused like one, by both checkers."""
    from okg.substrate.mcp.sql_safety import (
        ProbeQueryRefused,
        RunSqlForbiddenFunction,
        validate_probe_query,
        validate_run_sql,
    )

    with pytest.raises(ProbeQueryRefused) as err:
        validate_probe_query(sql)
    assert err.value.data["reason"] == "forbidden_function"
    with pytest.raises(RunSqlForbiddenFunction):
        validate_run_sql(sql)


@pytest.mark.parametrize("sql", [
    "SELECT 1 AS up",
    "SELECT count(*) AS n FROM okg.external_probe_cache",
    "WITH x AS (SELECT 1 AS a) SELECT a FROM x",
    "SELECT status FROM some_schema.some_table WHERE id = 3",
    # The probe's own statement_timeout bounds a sleep.
    "SELECT pg_sleep(3)",
])
def test_probe_query_plain_selects_pass(sql: str) -> None:
    from okg.substrate.mcp.sql_safety import validate_probe_query

    validate_probe_query(sql)


# Security review of the #2886 port (cc1628aa06): quoted, mixed-case
# spellings of deny-listed names fold ASCII letters, as the pglast guard's
# lower-casing did, and OPERATOR's schema is read from the lexer's tokens.
QUOTED_DENY_LISTED = [
    ('SELECT "PG_SLEEP"(1)', "pg_sleep"),
    ('SELECT pg_catalog."PG_SLEEP"(1)', "pg_sleep"),
    ('SELECT "PG_CATALOG"."Pg_Sleep"(1)', "pg_sleep"),
    ("SELECT \"Pg_Read_File\"('x')", "pg_read_file"),
    ('SELECT "LO_UNLINK"(1)', "lo_unlink"),
    ("SELECT \"TS_STAT\"('SELECT 1')", "ts_stat"),
    ("SELECT \"To_RegClass\"('x')", "to_regclass"),
    ('SELECT "PG_STAT_GET_ACTIVITY"(NULL)', "pg_stat_get_activity"),
    ("SELECT U&\"PG_SLEEP\"(1)", "pg_sleep"),
]

OPERATOR_SCHEMA_REFUSED = [
    'SELECT 1 OPERATOR("PG_CATALOG".=) 1',
    'SELECT 1 OPERATOR("Pg_Catalog".=) 1',
    'SELECT 1 OPERATOR(U&"PG_CATALOG".=) 1',
    'SELECT 1 OPERATOR("pg_catalog " .=) 1',
    "SELECT 1 OPERATOR(public.=) 1",
    'SELECT 1 OPERATOR("PG_SLEEP".=) 1',
    "SELECT 1 OPERATOR(=) 1",
    "SELECT 1 OPERATOR(x.pg_catalog.=) 1",
]

OPERATOR_SCHEMA_ALLOWED = [
    "SELECT 1 OPERATOR(pg_catalog.=) 1",
    "SELECT 1 OPERATOR(PG_CATALOG.=) 1",
    'SELECT 1 OPERATOR("pg_catalog".=) 1',
    'SELECT 1 OPERATOR(U&"pg_catalog".+) 1',
    "SELECT 1 OPERATOR(pg_catalog . /* c */ =) 1",
]


@pytest.mark.parametrize(("sql", "fn"), QUOTED_DENY_LISTED)
def test_quoted_mixed_case_deny_listed_name_refused(sql: str, fn: str) -> None:
    from okg.substrate.mcp.sql_safety import (
        ProbeQueryRefused,
        RunSqlForbiddenFunction,
        validate_probe_query,
        validate_run_sql,
    )

    with pytest.raises(RunSqlForbiddenFunction) as err:
        validate_run_sql(sql)
    assert err.value.data["function"] == fn
    if fn == "pg_sleep":
        # The probe path still allows the sleep family, in any spelling.
        validate_probe_query(sql)
    else:
        with pytest.raises(ProbeQueryRefused) as probe_err:
            validate_probe_query(sql)
        assert probe_err.value.data["function"] == fn


@pytest.mark.parametrize("sql", OPERATOR_SCHEMA_REFUSED)
def test_operator_schema_is_read_with_its_quoting(sql: str) -> None:
    """Refused as a forbidden operator, or earlier as unparseable (a
    multi-part operator name loses an identifier in sqlglot's tree)."""
    from okg.substrate.mcp.sql_safety import RunSqlInvalid, validate_run_sql

    with pytest.raises(RunSqlInvalid) as err:
        validate_run_sql(sql)
    assert err.value.code in {"run_sql_forbidden_function", "run_sql_invalid"}
    if err.value.code == "run_sql_forbidden_function":
        assert err.value.data["function"].startswith("OPERATOR(")


@pytest.mark.parametrize("sql", OPERATOR_SCHEMA_ALLOWED)
def test_pg_catalog_operator_spellings_pass(sql: str) -> None:
    from okg.substrate.mcp.sql_safety import validate_run_sql

    validate_run_sql(sql)


# Delta review of 9018dce46c: the probe path checks OPERATOR's schema too.
PROBE_OPERATOR_REFUSED = [
    "SELECT id FROM okg.v_nodes WHERE id OPERATOR(public.=) 'a'",
    "SELECT id FROM okg.v_nodes WHERE id OPERATOR(\"PG_CATALOG\".=) 'a'",
]


@pytest.mark.parametrize("sql", PROBE_OPERATOR_REFUSED)
def test_probe_operator_outside_pg_catalog_refused(sql: str) -> None:
    from okg.substrate.mcp.sql_safety import ProbeQueryRefused, validate_probe_query

    with pytest.raises(ProbeQueryRefused) as err:
        validate_probe_query(sql)
    assert err.value.data["reason"] == "forbidden_function"
    assert err.value.data["function"].startswith("OPERATOR(")


def test_probe_pg_catalog_operator_passes() -> None:
    from okg.substrate.mcp.sql_safety import validate_probe_query

    validate_probe_query("SELECT id FROM okg.v_nodes WHERE id OPERATOR(pg_catalog.=) 'a'")


# The token pass fails closed on names: an identifier spelling an exact
# deny-listed name is refused wherever it stands (an alias, a CTE name),
# and a prefix-class name followed by "(" is refused even when it is a CTE
# or alias with a column list.
@pytest.mark.parametrize(("sql", "fn"), [
    ('SELECT 1 AS "PG_SLEEP"', "pg_sleep"),
    ("WITH lo_rows(a) AS (SELECT 1) SELECT a FROM lo_rows", "lo_rows"),
])
def test_deny_listed_spelling_refused_even_as_a_name(sql: str, fn: str) -> None:
    from okg.substrate.mcp.sql_safety import RunSqlForbiddenFunction, validate_run_sql

    with pytest.raises(RunSqlForbiddenFunction) as err:
        validate_run_sql(sql)
    assert err.value.data["function"] == fn


# ---------- final review round of #2579 (2026-09-29) ----------


@pytest.mark.parametrize("sql", [
    "SELECT 1 FROM okg.v_nodes TABLESAMPLE SYSTEM (10)",
    "SELECT 1 FROM okg.v_nodes TABLESAMPLE BERNOULLI (5) REPEATABLE (1)",
    "SELECT 1 FROM okg.v_nodes n TABLESAMPLE okg_custom_sampler (5)",
    "SELECT 1 FROM okg.v_nodes n JOIN okg.v_edges e TABLESAMPLE system (1) ON true",
    "SELECT * FROM (SELECT 1 FROM okg.v_nodes TABLESAMPLE SYSTEM (1)) s",
])
def test_tablesample_is_refused_with_any_method(sql: str) -> None:
    # dev's pglast guard refused every RangeTableSample; the method is a
    # function PostgreSQL looks up by name, so no method is admitted.
    refusal = _refusal(sql)
    assert refusal is not None, f"guard allowed {sql!r}"
    assert refusal.code == "run_sql_forbidden_function", (sql, refusal.data)
    assert refusal.data["function"] == "tablesample"


@pytest.mark.parametrize("sql", [
    'SELECT 1 FROM okg.U&"v_nodes"U&"x"',
    'SELECT 1 FROM okg."v_nodes"U&"x"',
])
def test_unicode_literal_touching_a_quote_is_refused(sql: str) -> None:
    # PostgreSQL reads U&"v_nodes"U&"x" as a name and an alias (checked on
    # PostgreSQL 17.10); decoded naively it would become "v_nodes""x", one
    # identifier.
    from okg.substrate import postgres_sql as pg

    with pytest.raises(pg.PostgresSqlError, match="touches another quoted token"):
        pg.normalize(sql)
    refusal = _refusal(sql)
    assert refusal is not None and refusal.code == "run_sql_invalid", (sql, refusal)


@pytest.mark.parametrize("sql", [
    "SELECT U&'a'U&'b' FROM okg.v_nodes",
    "SELECT 'a'U&'b' FROM okg.v_nodes",
])
def test_unicode_string_touching_a_string_is_refused(sql: str) -> None:
    # Decoded naively, 'a'U&'b' would read 'a''b', one string. The lexer
    # refuses the pair first (two adjacent string constants); the
    # normalize rule would refuse it too.
    from okg.substrate import postgres_sql as pg

    with pytest.raises(pg.PostgresSqlError):
        pg.normalize(sql)
    refusal = _refusal(sql)
    assert refusal is not None and refusal.code == "run_sql_invalid", (sql, refusal)


def test_unicode_identifier_separated_from_its_alias_still_passes() -> None:
    from okg.substrate import postgres_sql as pg
    from okg.substrate.mcp.sql_safety import checked_run_sql

    sql = 'SELECT 1 FROM okg.U&"v_nodes" U&"x"'
    assert pg.normalize(sql) == 'SELECT 1 FROM okg."v_nodes" "x"'
    assert checked_run_sql(sql) == 'SELECT 1 FROM okg."v_nodes" "x"'
    # One U& identifier holding a doubled quote is one name to PostgreSQL
    # (v_nodes"x) and to normalize; this rule does not touch it.
    assert pg.normalize('SELECT 1 FROM okg.U&"v_nodes""x"') == (
        'SELECT 1 FROM okg."v_nodes""x"'
    )


@pytest.mark.parametrize("sql", [
    "SELECT 'a' -- c\n'b' FROM okg.v_nodes",
    "SELECT 'a'\n-- c\n'b' FROM okg.v_nodes",
    "SELECT E'a' -- c\n'\\' , (SELECT 1 FROM okg.nodes_live) --' FROM okg.v_nodes",
    "SELECT 'a' /* c */\n'b' FROM okg.v_nodes",
    "SELECT 'a' /* c\n */ 'b' FROM okg.v_nodes",
    # No newline: PostgreSQL reports a syntax error, dev's pglast guard
    # refused it, and sqlglot must not be left to read it some other way.
    "SELECT 'a' /* c */ 'b'",
    "SELECT 'a' 'b' FROM okg.v_nodes",
])
def test_string_continuation_across_a_comment_is_refused(sql: str) -> None:
    # PostgreSQL 17 joins 'a' -- c <newline> 'b' into the one string 'ab'
    # (scan.l quotecontinue allows -- comments in the gap).
    from okg.substrate import postgres_sql as pg

    with pytest.raises(pg.PostgresSqlError, match="string continuation"):
        pg.lex(sql)
    refusal = _refusal(sql)
    assert refusal is not None and refusal.code == "run_sql_invalid", (sql, refusal)
