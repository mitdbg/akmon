"""The text agents see matches what the tools accept (PACT
tool-text-accuracy).

From the C4 comp-ops traces: an oversize `inspect` refusal told agents to
narrow with `select`, which most inspect kinds ignore, and never said how
large the page was; a grep call with `context_chars` but no windows view was
refused as if it were a windows read; misplaced arguments (`subtype` on
search, top-level `at` on inspect) got no pointer; and the descriptions
named an argument search does not have. No argument, field or value is
newly accepted. Everything here runs without a database: refusal builders
are called directly, and served text is read from the server in
surface-probe mode, as test_tool_surface_budget does.
"""
from __future__ import annotations

import pytest

from okg.substrate.mcp import response_bounds
from okg.substrate.mcp.record_window_search import (
    WindowArgumentUnsupported,
    validate_window_call,
)


# ---------- oversize refusals ----------

def _remedy(operator: str, arguments: dict, *, single_row: bool = False,
            needed: int | None = 9000) -> dict:
    return response_bounds._oversize_refusal(
        operator, budget=4096, single_row=single_row, needed=needed,
        arguments=arguments,
    )["error"]


def test_an_inspect_kind_that_ignores_select_is_told_the_budget_it_needs():
    error = _remedy("inspect", {"target": {"kind": "session_start"}})
    assert error["needed_bytes"] == 9000
    assert "needs about 9000 bytes" in error["message"]
    # The offered budget carries a margin: a page rebuilt at a larger budget
    # states that budget, which can be a digit longer.
    assert error["remedy"] == (
        "inspect kind 'session_start' ignores select; pass max_bytes of at least 9064")


def test_inspect_node_names_its_select_fields_and_the_text_read():
    remedy = _remedy("inspect", {"target": {"kind": "node", "node_id": "x"}})["remedy"]
    assert "select (e.g. node_id, subtype, attrs)" in remedy
    assert '"kind":"text"' in remedy
    assert remedy.endswith("pass max_bytes of at least 9064")


def test_inspect_node_names_its_largest_parts_and_a_ready_text_read():
    """#3605: 233 inspect(node) calls were refused over budget, mostly large
    jira issues; the refusal names what is large and the paged text read
    for that id."""
    payload = {
        "node_id": "jira:BIG-1", "subtype": "jira_issue",
        "attrs": {"description": "d" * 5000, "comments": "c" * 2000, "key": "BIG-1"},
        "edges": ["e"] * 10,
    }
    error = response_bounds._oversize_refusal(
        "inspect", budget=4096, single_row=False, needed=9000,
        arguments={"target": {"kind": "node", "node_id": "jira:BIG-1"}},
        payload=payload,
    )["error"]
    remedy = error["remedy"]
    assert remedy.startswith(
        "the largest parts are attrs 7,051 B (description 5,002 B, comments 2,002 B, "
        "key 7 B), edges 50 B; read the text page by page with "
        'inspect(target={"kind":"text","node_id":"jira:BIG-1"}), or select fields '
        "without them, or ")
    assert remedy.endswith("pass max_bytes of at least 9064")


@pytest.mark.parametrize("kind", ["graph", "coverage"])
def test_inspect_kinds_that_honour_select_still_suggest_it(kind):
    remedy = _remedy("inspect", {"target": {"kind": kind}})["remedy"]
    assert remedy.startswith("narrow the request (select/projection/filter)")


def test_query_and_other_tools_get_their_own_remedy():
    assert _remedy("query", {})["remedy"] == (
        "add LIMIT, select fewer columns, or cut long text with "
        "left(attrs->>'text', 200); or pass max_bytes of at least 9064")
    single = _remedy("filter", {}, single_row=True)
    assert single["remedy"] == (
        "request a narrower select/projection, or pass max_bytes of at least 9064")
    # read-passage-rows-3666 (D3): search does not publish max_bytes, so
    # its remedy names a smaller limit and offset paging, never a budget.
    assert _remedy("search", {}, single_row=True)["remedy"] == (
        "pass a smaller limit and page with offset")
    assert "LIMIT" not in _remedy("inspect", {"target": {"kind": "graph"}})["remedy"]


def test_a_size_over_the_maximum_is_never_offered_as_a_budget():
    big = response_bounds.MAX_RESPONSE_BYTES + 1000
    error = _remedy("inspect", {"target": {"kind": "ontology"}}, needed=big)
    assert "max_bytes of at least" not in error["remedy"]
    assert error["remedy"] == (
        f"inspect kind 'ontology' ignores select and it needs more than the "
        f"{response_bounds.MAX_RESPONSE_BYTES}-byte maximum; it cannot be served "
        "whole on this server")
    assert error["needed_bytes"] == big


def test_a_continuation_is_told_to_restart_from_its_target():
    remedy = _remedy("inspect", {"next": "tok"})["remedy"]
    assert remedy == (
        "a continuation keeps its signed budget; restart the read from its "
        "target with max_bytes of at least 9064")
    assert "select" not in remedy


def test_the_fitter_names_a_budget_that_serves_the_page():
    """End to end through `_fit_to_budget`: every row is too large for the
    budget, the refusal names a size above the budget, and a rerun at the
    offered budget fits. The size is in the row's id, which a cut row keeps
    whole (#3666 step 3b), so the row cannot be cut to fit."""
    import inspect
    import json

    # `filter`, not `search`: a search remedy names no budget (D3,
    # read-passage-rows-3666).
    def operator(limit: int = 5, offset: int = 0) -> dict:
        return {"items": [{"node_id": f"n{i}" + "x" * 600}
                            for i in range(offset, offset + limit)],
                "limit": limit, "offset": offset, "next_offset": offset + limit}

    def measure(payload: dict) -> int:
        return len(json.dumps(payload))

    signature = inspect.signature(operator)
    arguments = {"limit": 5, "offset": 0}
    refusal = response_bounds._fit_to_budget(
        "filter", operator, bound=signature.bind(**arguments), signature=signature,
        call_arguments=dict(arguments), arguments=dict(arguments), budget=500,
        measure=measure,
    )
    error = refusal["error"]
    assert error["code"] == "response_byte_budget_exceeded"
    assert error["needed_bytes"] > 500
    offered = int(error["remedy"].rsplit(" ", 1)[1])
    retried = response_bounds._fit_to_budget(
        "filter", operator, bound=signature.bind(**arguments), signature=signature,
        call_arguments=dict(arguments), arguments=dict(arguments), budget=offered,
        measure=measure,
    )
    assert retried.get("status") != "refused", retried
    assert retried["items"]


def test_without_a_measured_size_the_refusal_says_nothing_it_does_not_know():
    error = _remedy("filter", {}, needed=None)
    assert "needed_bytes" not in error
    assert "needs about" not in error["message"]
    assert error["remedy"].endswith("request a larger valid max_bytes")


# ---------- windows refusals ----------

_WINDOW_BASE = {"view": None, "matching": None, "context_chars": None, "method": "grep",
                "record_ids": None, "query": "x"}


def _window_refusal(**arguments) -> WindowArgumentUnsupported:
    with pytest.raises(WindowArgumentUnsupported) as caught:
        validate_window_call(**{**_WINDOW_BASE, **arguments})
    return caught.value


def _window_refusal_or_none(**arguments):
    return validate_window_call(**{**_WINDOW_BASE, **arguments})


@pytest.mark.parametrize("argument", ["matching", "context_chars"])
def test_a_windows_argument_without_the_view_names_the_call_made(argument):
    value = "record" if argument == "matching" else 200
    error = _window_refusal(**{argument: value})
    message = str(error)
    assert message.startswith("search: ")
    assert "which this call did not pass" in message
    assert error.data == {"argument": argument, "view": None}
    # #3605 (`winscope A`): one argument away from a served call.
    assert error.fix_hint == (
        f'add view="windows" to read this grep\'s top records as windows, '
        f"or drop {argument}")
    if argument == "context_chars":
        assert "grep cards' match windows have a fixed width" in message


def test_a_lexical_context_chars_is_pointed_at_max_chars():
    error = _window_refusal(method="lexical", context_chars=200)
    message = str(error)
    assert "card previews take max_chars" in message
    assert "fixed width" not in message
    assert error.fix_hint.startswith("pass max_chars for longer card previews")


def test_windows_without_a_scope_read_this_greps_top_records():
    """#3605 (`winscope A`): 286 grep calls asked for windows with no record
    scope and were refused; that call is now served from the grep's hits."""
    assert _window_refusal_or_none(view="windows", record_ids=None) == ("record", 200)


def test_a_windows_call_keeps_its_windows_prefix():
    error = _window_refusal(view="windows", record_ids=["r"], select=["node_id"])
    # True for the direct caller this refusal is for (a list of row keys is
    # refused here too) and for the served tool, which cuts the rows itself.
    assert str(error).startswith(
        "search view='windows': select is not taken by the windows read, whose rows are fixed")
    assert error.fix_hint.startswith("omit select here; the served tool cuts")
    assert error.data["view"] == "windows"


# ---------- misplaced arguments ----------

@pytest.mark.parametrize(("tool", "argument", "said"), [
    ("search", "subtype", '"field":"type"'),
    ("search", "type", '"field":"type"'),
    ("inspect", "at", "put it inside target"),
    ("inspect", "node_id", 'target={"kind":"node","node_id":...}'),
    ("expand", "node_id", 'input={"node_id":...}'),
])
def test_a_misplaced_argument_is_pointed_at_its_place(tool, argument, said):
    assert said in response_bounds._ARGUMENT_INSTEAD[(tool, argument)]


def test_the_next_refusal_says_how_to_change_the_page_size():
    payload = response_bounds._next_budget_refusal("inspect", "not-a-token")
    assert "restart the read from its target with the new max_bytes" in (
        payload["error"]["fix_hint"])


# ---------- the served text ----------

@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def served_surface(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


def _flat(text: str) -> str:
    return " ".join(text.split())


@pytest.mark.anyio
async def test_the_descriptions_say_what_the_tools_do(served_surface):
    tools = {t.name: _flat(t.description) for t in await served_surface.list_tools()}
    search, inspect, expand = tools["search"], tools["inspect"], tools["expand"]
    # #3666 M10: each description opens with what the tool is for, and shows
    # only the forms the lean surface publishes.
    assert search.startswith("Find records by words, best first.")
    # search has no `subtype` argument; its type filter is a `where` type.
    assert "Without a type in `where`, the deployment's default search types" in search
    assert "A page is the best matches, not all of them" in search
    assert 'method="grep" is exact, case-insensitive' in search
    for hidden in ("view=", "rank_by", "profile", "source_family", "context_chars",
                   "summary", "max_per_parent"):
        assert hidden not in search, hidden
    schema = {t.name: t.inputSchema for t in await served_surface.list_tools()}
    props = schema["search"]["properties"]
    assert not {"matching", "context_chars", "summary"} & set(props)
    # read-passage-rows-3666 (D1): `view` is published for the short rows.
    assert "list" in props["view"]["description"]
    assert "`at` inside target" not in inspect
    # #3634 (`nextresize A`): `next` may also take a new page size.
    assert "at its page size, or at a new max_bytes" in inspect
    # #3647 (`nestedargs A`, `nodepartial A`).
    assert "`select` and `max_bytes` go beside `target`, not in it" in inspect
    assert 'inspect(target={"kind":"node","node_id":"<id>"}, select=["attrs"])' in inspect
    assert "A `node` over max_bytes comes back with its largest parts cut, each named." in inspect
    assert "in pages of max_bytes (default\n" not in inspect
    assert "65536, up to 262144" in inspect
    assert "`where` and `order_by` (on the far record)" in expand
    assert 'metrics=["count"]' in tools["aggregate"]
    assert "a declared attrs.<key>" in tools["filter"]
    # PACT read-map-compute: map adds computed columns.
    assert tools["map"].startswith("Add computed columns to every row of a set; never drops a row.")


@pytest.mark.anyio
async def test_inspect_shows_the_graph_overview_before_any_other_example(served_surface):
    # #3666 (read-inspect-start-3666): the overview is the first call shown.
    tools = {t.name: _flat(t.description) for t in await served_surface.list_tools()}
    inspect = tools["inspect"]
    examples = [at for at in range(len(inspect)) if inspect.startswith("inspect(", at)]
    start = 'Start with inspect(target={"kind":"graph"}): record and link type counts.'
    assert start in inspect
    assert "`target.kind` picks the subject." in inspect
    assert inspect.index(start) + len("Start with ") == examples[0]
    assert inspect.startswith('inspect(target={"kind":"node",', examples[1])
    assert "`select` and `max_bytes` go beside `target`, not in it" in inspect


@pytest.mark.anyio
async def test_the_instructions_do_not_point_at_a_file_no_tool_can_read(served_surface):
    assert "docs/reference/mcp.md" not in served_surface.instructions


@pytest.mark.anyio
async def test_the_served_refusal_points_a_misplaced_argument_home(served_surface):
    from okg.substrate.mcp.response_bounds import UnknownOperatorArgument

    with pytest.raises(UnknownOperatorArgument) as caught:
        served_surface._refuse_unknown_operator_arguments(
            "search", {"query": "x", "subtype": "ticket"})
    error = caught.value
    assert error.data["unknown_arguments"] == ["subtype"]
    assert error.data["instead"]["subtype"].startswith('pass where={"op":"eq"')
    assert error.fix_hint.startswith('subtype: pass where=')


@pytest.mark.anyio
async def test_grep_with_scope_is_pointed_at_the_windows_read(served_surface):
    from okg.substrate.mcp import search_where

    tool = served_surface._tool_manager._tools["search"]
    with pytest.raises(search_where.SearchScopeUnsupported) as caught:
        tool.fn(query="x", method="grep", scope={"record": "r"})
    assert "view='windows'" in caught.value.fix_hint
