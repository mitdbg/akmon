"""Unit proof for issue #2899: run_sql's graph path splices, never re-renders.

No database: a fake connection answers the one ``pg_get_viewdef`` lookup.
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from okg.substrate.mcp import graph_sql_splice as splice
from okg.substrate.mcp.sql_safety import RunSqlInvalid


RELATIONS = {
    "okg.graph_nodes": (
        'SELECT "node_id", "subtype" FROM "_chronos_nodes" '
        "WHERE live_lo <= :_chronos_branch_point "
        "AND :_chronos_branch_point < live_hi AND deleted = FALSE"
    ),
    "okg.graph_edges": (
        'SELECT "edge_id", "src" FROM "_chronos_edges" '
        "WHERE live_lo <= :_chronos_branch_point"
    ),
}
PARAMS = {"_chronos_branch_point": 42}


class _Cursor:
    def __init__(self, views: dict[str, str]) -> None:
        self._views = views
        self._row = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[str, ...]) -> None:
        body = self._views.get(params[0])
        self._row = None if body is None else (body,)

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self, views: dict[str, str] | None = None) -> None:
        self.views = views or {}

    def cursor(self) -> _Cursor:
        return _Cursor(self.views)


def _splice(sql: str, *, views: dict[str, str] | None = None, row_cap: int = 5):
    plan = splice.plan_graph_sql(sql, _Conn(views))
    spliced = splice.render_graph_sql(plan, RELATIONS, PARAMS, row_cap=row_cap)
    splice.verify_splice(plan, spliced)
    return plan, spliced


def test_placeholder_and_percent_text_in_caller_literals_is_unchanged() -> None:
    sql = (
        "SELECT node_id, '%(_chronos_branch_point)s' AS pct, "
        "$t$ :_chronos_branch_point $t$ AS colon "
        "FROM okg.v_nodes AS n WHERE n.node_id LIKE 'n:%'"
    )
    plan, spliced = _splice(sql)
    assert plan.routes_to_graph
    assert "'%(_chronos_branch_point)s' AS pct" in spliced.sql
    assert "$t$ :_chronos_branch_point $t$ AS colon" in spliced.sql
    assert "LIKE 'n:%'" in spliced.sql
    # The branch point is an integer literal; no placeholder is left anywhere
    # outside the caller's own literals.
    assert "live_lo <= 42 AND 42 < live_hi" in spliced.sql
    assert spliced.sql.count(":_chronos_branch_point") == 1


def test_unaliased_view_gets_its_own_name_as_alias() -> None:
    _, spliced = _splice(
        "SELECT v_nodes.node_id, e.edge_id FROM okg.v_nodes "
        "LEFT JOIN okg.v_edges e ON e.src = v_nodes.node_id"
    )
    assert 'AND deleted = FALSE) AS "v_nodes" LEFT JOIN' in spliced.sql
    assert 'WHERE live_lo <= 42) e ON e.src = v_nodes.node_id' in spliced.sql


def test_caller_text_is_copied_byte_for_byte_around_the_splices() -> None:
    sql = (
        "SELECT  node_id /* keep 'me' */,\n  'it''s' AS q\n"
        "FROM okg.v_nodes n -- trailing\n"
        "ORDER BY node_id;  -- after the statement"
    )
    _, spliced = _splice(sql, row_cap=9)
    head, _, tail = sql.partition("okg.v_nodes")
    statement_tail = tail.split(";")[0]
    assert spliced.sql.startswith("SELECT * FROM (\n" + head + "(SELECT ")
    assert spliced.sql.endswith(statement_tail + "\n) AS _okg_bounded LIMIT 10")


def test_graph_view_inside_composite_view_is_spliced_into_the_view_body() -> None:
    views = {
        "v_test_pages": (
            " SELECT v_nodes.node_id\n   FROM okg.v_nodes\n"
            "  WHERE v_nodes.subtype = 'page'::text;"
        )
    }
    _, spliced = _splice("SELECT node_id FROM okg.v_test_pages", views=views)
    assert (
        'FROM (SELECT v_nodes.node_id\n   FROM (SELECT "node_id", "subtype" FROM "_chronos_nodes"'
        in spliced.sql
    )
    assert (
        "AS \"v_nodes\"\n  WHERE v_nodes.subtype = 'page'::text\n) AS \"v_test_pages\""
        in spliced.sql
    )


def test_operational_view_is_not_routed_or_spliced() -> None:
    views = {"v_runtime_queue_health": " SELECT 1 AS queue_name;"}
    plan = splice.plan_graph_sql(
        "SELECT queue_name FROM okg.v_runtime_queue_health", _Conn(views)
    )
    assert plan.routes_to_graph is False


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT E'a' FROM okg.v_nodes",
        "SELECT e'a' FROM okg.v_nodes",
        "SELECT U&'a' FROM okg.v_nodes",
    ],
)
def test_escape_dependent_literals_are_refused(sql: str) -> None:
    with pytest.raises(splice.RunSqlGraphLiteralRefused) as excinfo:
        splice.refuse_escape_dependent_literals(sql)
    assert excinfo.value.code == "run_sql_graph_literal_refused"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 'a' FROM okg.v_nodes",
        "SELECT 'it''s' FROM okg.v_nodes",
        "SELECT $$a\\b$$ FROM okg.v_nodes",
        'SELECT node_id FROM U&"okg".U&"v_nodes"',
        # Issue #3666: run_sql runs the text with standard strings, where a
        # backslash in '...' is an ordinary character (a regex like \\d+).
        "SELECT 'a\\b' FROM okg.v_nodes",
        "SELECT node_id FROM okg.v_nodes WHERE attrs->>'text' ~ '\\d+'",
    ],
)
def test_plain_literals_and_unicode_identifiers_are_accepted(sql: str) -> None:
    splice.refuse_escape_dependent_literals(sql)


@pytest.mark.parametrize(
    ("sql", "rewritten"),
    [
        (
            "SELECT 1 FROM okg.v_nodes WHERE attrs->>'text' ~ E'\\\\d+'",
            "SELECT 1 FROM okg.v_nodes WHERE attrs->>'text' ~ $$\\d+$$",
        ),
        ("SELECT e'it\\'s', E'a''b' FROM okg.v_nodes", "SELECT $$it's$$, $$a'b$$ FROM okg.v_nodes"),
        # A defined escape keeps its value: a newline stays a newline.
        ("SELECT E'a\\nb' FROM okg.v_nodes", "SELECT $$a\nb$$ FROM okg.v_nodes"),
        ("SELECT E'\\x41\\u0042' FROM okg.v_nodes", "SELECT $$AB$$ FROM okg.v_nodes"),
        # An unknown escape (PostgreSQL drops the backslash), a short hex or
        # code point, a value that would close the dollar quote, U& strings
        # and text with no E string: no rewrite.
        ("SELECT E'\\d' FROM okg.v_nodes", None),
        ("SELECT E'\\xZ' FROM okg.v_nodes", None),
        ("SELECT E'\\u00' FROM okg.v_nodes", None),
        ("SELECT E'a$$b' FROM okg.v_nodes", None),
        ("SELECT E'a$' FROM okg.v_nodes", None),
        ("SELECT E'a', U&'b' FROM okg.v_nodes", None),
        ("SELECT 'a' FROM okg.v_nodes", None),
        ("SELECT E'unterminated FROM okg.v_nodes", None),
    ],
)
def test_dollar_quoted_rewrite_is_offered_only_when_exact(sql: str, rewritten) -> None:
    from okg.substrate.postgres_sql import lex

    assert splice.dollar_quoted_rewrite(sql) == rewritten
    if rewritten is not None:
        # The rewrite is accepted on the graph path and keeps every value.
        splice.refuse_escape_dependent_literals(rewritten)
        before = [t.value for t in lex(sql) if t.kind in {"estring", "string"}]
        after = [t.value for t in lex(rewritten) if t.kind in {"dollar", "string"}]
        assert before == after


def test_safety_net_refuses_a_changed_caller_token() -> None:
    plan, spliced = _splice("SELECT node_id, 'a' AS x FROM okg.v_nodes")
    tampered = replace(spliced, sql=spliced.sql.replace("'a' AS x", "'b' AS x"))
    with pytest.raises(splice.RunSqlGraphSpliceMismatch) as excinfo:
        splice.verify_splice(plan, tampered)
    assert excinfo.value.code == "run_sql_graph_splice_mismatch"


def test_safety_net_refuses_a_moved_caller_token() -> None:
    plan, spliced = _splice("SELECT node_id FROM okg.v_nodes WHERE true")
    tampered = replace(spliced, sql=spliced.sql.replace("WHERE true", "WHERE  true"))
    with pytest.raises(splice.RunSqlGraphSpliceMismatch):
        splice.verify_splice(plan, tampered)


def test_missing_checkout_relation_is_refused() -> None:
    plan = splice.plan_graph_sql("SELECT edge_id FROM okg.v_edges", _Conn())
    with pytest.raises(RunSqlInvalid, match="does not expose"):
        splice.render_graph_sql(
            plan,
            {"okg.graph_nodes": RELATIONS["okg.graph_nodes"]},
            PARAMS,
            row_cap=1,
        )


@pytest.mark.parametrize("params", [{"_chronos_branch_point": "42"}, {}, {"x": 1}])
def test_non_integer_or_unknown_branch_parameters_are_refused(params) -> None:
    plan = splice.plan_graph_sql("SELECT node_id FROM okg.v_nodes", _Conn())
    with pytest.raises(RunSqlInvalid, match="cannot bind"):
        splice.render_graph_sql(plan, RELATIONS, params, row_cap=1)


@pytest.mark.parametrize(
    ("sql", "form"),
    [
        ("SELECT node_id FROM ONLY okg.v_nodes", "ONLY"),
        ("SELECT node_id FROM okg.v_nodes *", "*"),
        ("TABLE okg.v_nodes", "TABLE"),
        ("SELECT node_id FROM ONLY okg.graph_nodes", "ONLY"),
    ],
)
def test_only_star_and_table_forms_on_graph_relations_are_refused(
    sql: str, form: str
) -> None:
    plan = splice.plan_graph_sql(sql, _Conn())
    assert plan.routes_to_graph
    with pytest.raises(splice.RunSqlGraphFormUnsupported) as excinfo:
        splice.refuse_unsupported_forms(plan)
    assert excinfo.value.code == "run_sql_graph_form_unsupported"
    assert excinfo.value.data["form"] == form
    assert "FROM okg." in (excinfo.value.fix_hint or "")
    # The render step refuses the same form if the up-front check is skipped.
    with pytest.raises(splice.RunSqlGraphFormUnsupported):
        splice.render_graph_sql(plan, RELATIONS, PARAMS, row_cap=1)


def test_only_form_inside_a_composite_view_over_the_graph_is_refused() -> None:
    views = {"v_test_pages": " SELECT node_id\n   FROM ONLY okg.v_nodes;"}
    plan = splice.plan_graph_sql("SELECT node_id FROM okg.v_test_pages", _Conn(views))
    with pytest.raises(splice.RunSqlGraphFormUnsupported):
        splice.refuse_unsupported_forms(plan)


def test_forms_on_non_graph_relations_are_left_alone() -> None:
    views = {"v_runtime_queue_health": " SELECT 1 AS queue_name;"}
    plan = splice.plan_graph_sql("TABLE okg.v_runtime_queue_health", _Conn(views))
    assert plan.routes_to_graph is False
    splice.refuse_unsupported_forms(plan)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT n.node_id FROM okg.v_nodes n",
        "SELECT node_id FROM okg.v_nodes WHERE true",
        "SELECT node_id FROM okg.v_nodes, okg.v_edges",
        "SELECT (SELECT count(*) FROM okg.v_nodes) AS c",
    ],
)
def test_safety_net_refuses_a_replaced_range_one_token_too_wide(
    sql: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A too-wide replaced range is caught by the token check, not PostgreSQL."""
    real_end = splice._reference_end

    def one_token_too_wide(tokens, location, parts, text):
        end = real_end(tokens, location, parts, text)
        following = [t for t in tokens if t.start >= end]
        return following[0].end if following else end

    monkeypatch.setattr(splice, "_reference_end", one_token_too_wide)
    plan = splice.plan_graph_sql(sql, _Conn())
    spliced = splice.render_graph_sql(plan, RELATIONS, PARAMS, row_cap=1)
    with pytest.raises(splice.RunSqlGraphSpliceMismatch) as excinfo:
        splice.verify_splice(plan, spliced)
    assert excinfo.value.code == "run_sql_graph_splice_mismatch"
    assert "more than the relation name" in excinfo.value.data["reason"]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT node_id FROM okg . /* c */ v_nodes AS n",
        'SELECT node_id FROM U&"okg".U&"v_nodes"',
        "SELECT node_id FROM v_nodes",
    ],
)
def test_replaced_range_check_accepts_exact_qualified_names(sql: str) -> None:
    _splice(sql)


# ---------------------------------------------------------------------------
# Fail closed on view bodies (final review round of #2579): a graph relation
# in a composite view body that the plan did not find must refuse, never be
# left in place to read the live view.


_PAGES_BODY = (
    " SELECT v_nodes.node_id\n   FROM okg.v_nodes\n"
    "  WHERE v_nodes.subtype = 'page'::text;"
)


def _drop_refs_named(monkeypatch: pytest.MonkeyPatch, relname: str) -> None:
    """Simulate sqlglot leaving a relation out of its tree."""
    real = splice._relation_refs

    def dropping(stmt):
        return [ref for ref in real(stmt) if ref[2] != relname]

    monkeypatch.setattr(splice, "_relation_refs", dropping)


def test_view_body_graph_reference_the_plan_missed_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _drop_refs_named(monkeypatch, "v_nodes")
    with pytest.raises(splice.RunSqlGraphViewUnplanned) as excinfo:
        splice.plan_graph_sql(
            "SELECT node_id FROM okg.v_test_pages",
            _Conn({"v_test_pages": _PAGES_BODY}),
        )
    assert excinfo.value.code == "run_sql_graph_view_unplanned"
    assert excinfo.value.data["view"] == "okg.v_test_pages"
    assert excinfo.value.data["name"] == "okg.v_nodes"


def test_nested_composite_view_the_plan_missed_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The outer body reads a composite view over the graph; if that reference
    # is dropped, the outer view looks operational and would read live.
    views = {
        "v_outer": " SELECT v_test_pages.node_id\n   FROM okg.v_test_pages;",
        "v_test_pages": _PAGES_BODY,
    }
    _drop_refs_named(monkeypatch, "v_test_pages")
    with pytest.raises(splice.RunSqlGraphViewUnplanned) as excinfo:
        splice.plan_graph_sql("SELECT node_id FROM okg.v_outer", _Conn(views))
    assert excinfo.value.data["name"] == "okg.v_test_pages"


@pytest.mark.parametrize(
    "body",
    [
        # Unqualified: planned without a schema, so never routed.
        " SELECT v_nodes.node_id\n   FROM v_nodes;",
        " SELECT g.node_id\n   FROM graph_edges g;",
        # A graph name where the tree holds no relation (here an alias).
        ' SELECT 1 AS "v_edges";',
    ],
    ids=["unqualified-v_nodes", "unqualified-graph_edges", "alias-spelled-as-graph"],
)
def test_view_body_naming_a_graph_relation_outside_a_planned_reference_is_refused(
    body: str,
) -> None:
    with pytest.raises(splice.RunSqlGraphViewUnplanned):
        splice.plan_graph_sql(
            "SELECT * FROM okg.v_test_x", _Conn({"v_test_x": body})
        )


def test_view_body_checks_leave_real_view_shapes_alone() -> None:
    views = {
        "v_test_mixed": (
            " SELECT v_nodes.node_id, okg.current_pin() AS pin, e.edge_id\n"
            "   FROM okg.v_nodes\n"
            "     LEFT JOIN okg.v_edges e ON e.src = v_nodes.node_id\n"
            "     LEFT JOIN okg.v_ops o ON o.id = v_nodes.node_id;"
        ),
        "v_ops": " SELECT jobs.id\n   FROM okg.jobs;",
    }
    plan = splice.plan_graph_sql("SELECT * FROM okg.v_test_mixed", _Conn(views))
    assert plan.routes_to_graph
    # A caller's own text is judged by checked_run_sql, not by this check.
    plan = splice.plan_graph_sql("SELECT 1 AS v_nodes", _Conn())
    assert not plan.routes_to_graph
