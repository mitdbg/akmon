"""The tool text says which `select` fields each search method and expand
shape returns, and a select refusal says what to use instead (PACT
select-field-instructions).

The published `search` schema keeps its union `select` enum and adds a
description built from the same field tables; `expand`'s description names
its two row shapes. Refusals keep their code, their condition and their
valid-field list, and add one hint per unknown name that has one. Nothing
refused before is accepted. The served-schema test runs the real server in
surface-probe mode (no database), as test_tool_surface_budget does.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.mcp.capability_probe import ServerCapabilities
from okg.substrate.mcp.envelope import (
    SelectUnknownField,
    select_instead,
    select_retry_from,
    validate_select,
)
from okg.substrate.mcp.grep_search import GREP_FIELDS
from okg.substrate.mcp.search import _ALIAS_FIELDS, _LEXICAL_FIELDS, search_select_elsewhere
from okg.substrate.mcp.tools import (
    _FIND_MENTIONS_FIELDS,
    _LIST_NEIGHBORS_FIELDS,
    _TRAVERSE_FIELDS,
)


# ---------- the published description ----------

def _caps(available: list[str]) -> SimpleNamespace:
    fields = (
        ("lexical", (*_LEXICAL_FIELDS, "source_lineage")),
        ("alias", _ALIAS_FIELDS),
        ("identifier_mentions", _FIND_MENTIONS_FIELDS),
        ("grep", GREP_FIELDS),
    )
    return SimpleNamespace(select_fields=fields, available_methods=lambda: available)


def test_the_description_lists_lexical_whole_and_grep_as_its_difference():
    text = ServerCapabilities.select_fields_description(
        _caps(["lexical", "alias", "identifier_mentions", "grep"]))
    assert text.startswith("Fields differ by method. lexical: node_id, ")
    # PACT select-fields-by-method: the fields grep LACKS come first, so an
    # agent reusing a lexical select sees `recency` is not there. grep
    # serves `handle` (#3470, `grephandle A`), so it is not listed.
    grep = text[text.index("grep has no "):]
    lacks = grep[len("grep has no "):grep.index("; otherwise as lexical")]
    assert lacks.split(", ") == [
        "phrase_match", "recency",
        "rollup_best_chunk_id", "score", "source_lineage"]
    assert "; otherwise as lexical, plus match_windows." in grep
    assert "alias: a refusal lists their fields." in text


def test_identifier_mentions_is_listed_whole_and_has_no_node_id():
    text = ServerCapabilities.select_fields_description(
        _caps(["lexical", "alias", "identifier_mentions", "grep"]))
    assert ("identifier_mentions: only evidence_kind, evidence_kind_provenance, "
            "id_type, span_end, span_start, value.") in text
    mentions = text[text.index("identifier_mentions: "):]
    assert "node_id" not in mentions.split(".")[0]
    assert "identifier_mentions" not in text[text.index("alias:"):]


def test_an_unavailable_method_is_not_described():
    text = ServerCapabilities.select_fields_description(_caps(["lexical"]))
    assert "grep" not in text and "alias" not in text
    assert ServerCapabilities.select_fields_description(_caps([])) == ""


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def _probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)


@pytest.mark.anyio
async def test_the_served_schemas_carry_the_field_text(_probe, monkeypatch):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    mcp = build_server(dsn=PROBE_DSN)
    try:
        tools = {t.name: t for t in await mcp.list_tools()}
        select = tools["search"].inputSchema["properties"]["select"]
        # The items enum is not published (#3666): the description lists the
        # shown methods' fields (lexical whole, grep as its difference), then
        # the row field holding each of `where`'s title, path and source.
        # Hidden methods are not named.
        assert select["items"] == {"type": "string"}
        assert select["description"].startswith("Fields differ by method. lexical: node_id")
        assert "grep has no phrase_match, " in select["description"]
        assert "handle" not in select["description"].split("grep has no ")[1].split(";")[0]
        # read-passage-rows-3666 (D6): then the dotted form, one generic example.
        assert select["description"].endswith(
            " title and path are in handle; source is in provenance."
            ' One attribute: attrs.<key>, e.g. "attrs.status".'
        )
        assert "identifier_mentions" not in select["description"]
        expand = " ".join(tools["expand"].description.split())
        # Issue #3666: a row is the linked record; link rows are a select.
        assert (
            "rows are the linked records (node_id, subtype, attrs, provenance; "
            "1-hop adds link, link.attrs)" in expand
        )
        assert (
            "A 1-hop select of only edge_id, src, dst, edge_type, direction "
            "(attrs: the link's) returns one row per link." in expand
        )
        assert "returns the records at the end" in expand
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


# ---------- the refusals ----------

def _refusal(select, allowed, tool_name, elsewhere=None) -> SelectUnknownField:
    with pytest.raises(SelectUnknownField) as caught:
        validate_select(select, allowed, tool_name=tool_name, elsewhere=elsewhere)
    return caught.value


def test_lexical_id_points_at_node_id_and_is_still_refused():
    error = _refusal(["id", "excerpt"], _LEXICAL_FIELDS, "search(method=lexical)",
                     search_select_elsewhere("lexical"))
    assert error.data["unknown_fields"] == ["id"]
    assert error.data["instead"] == {"id": "use 'node_id'"}
    assert "(id: use 'node_id')" in str(error)
    assert error.fix_hint.startswith(
        "id: use 'node_id'; retry with select=['node_id', 'excerpt']; "
        "the tool's selectable fields are")
    assert error.data["valid_fields"] == sorted(_LEXICAL_FIELDS)


def test_grep_names_the_method_that_returns_lexical_fields():
    error = _refusal(["node_id", "score", "recency"], GREP_FIELDS,
                     "search(method=grep)", search_select_elsewhere("grep"))
    assert error.data["unknown_fields"] == ["score", "recency"]
    for name in ("score", "recency"):
        assert error.data["instead"][name] == (
            "returned by search(method=lexical), not method=grep")


def test_grep_refusal_names_the_ready_retry_select():
    """Lane X: agents reused [excerpt, recency, node_id, subtype] on
    grep. The refusal hands back that select without `recency`, in the
    caller's order, and still says where `recency` is returned."""
    error = _refusal(["excerpt", "recency", "node_id", "subtype"], GREP_FIELDS,
                     "search(method=grep)", search_select_elsewhere("grep"))
    assert error.data["unknown_fields"] == ["recency"]
    assert error.data["retry_select"] == ["excerpt", "node_id", "subtype"]
    assert error.fix_hint.startswith(
        "recency: returned by search(method=lexical), not method=grep; "
        "retry with select=['excerpt', 'node_id', 'subtype']; ")
    assert str(error) == (
        "search(method=grep): unknown field(s) in select: ['recency'] "
        "(recency: returned by search(method=lexical), not method=grep)")


def test_grep_accepts_handle():
    """#3470 (`grephandle A`): 41% of lane X's grep-using attempts asked
    grep for `handle` after #3535's text and refusal; grep now serves it."""
    assert "handle" in GREP_FIELDS
    validate_select(["excerpt", "handle", "node_id"], GREP_FIELDS,
                    tool_name="search(method=grep)",
                    elsewhere=search_select_elsewhere("grep"))
    assert "handle" not in search_select_elsewhere("grep")


def test_a_refusal_of_every_field_says_drop_select():
    error = _refusal(["recency", "score"], GREP_FIELDS, "search(method=grep)",
                     search_select_elsewhere("grep"))
    assert error.data["retry_select"] is None
    assert "retry with select" not in error.fix_hint
    assert error.fix_hint.endswith(
        f"; drop select or choose from: {sorted(GREP_FIELDS)}")


def test_the_retry_select_keeps_order_and_drops_repeats():
    error = _refusal(["subtype", "recency", "excerpt", "subtype"], GREP_FIELDS,
                     "search(method=grep)")
    assert error.data["retry_select"] == ["subtype", "excerpt"]


def test_the_retry_puts_a_named_replacement_in_place_and_still_refuses():
    # `url` -> `citation` (a conventional name): replaced in place.
    error = _refusal(["url", "node_id"], _LEXICAL_FIELDS, "search(method=lexical)",
                     search_select_elsewhere("lexical"))
    assert error.data["unknown_fields"] == ["url"]
    assert error.data["retry_select"] == ["citation", "node_id"]
    assert "retry with select=['citation', 'node_id']" in error.fix_hint
    # A typo -> its one close spelling, in place.
    error = _refusal(["node_id", "excrept"], _LEXICAL_FIELDS, "search(method=lexical)")
    assert error.data["instead"] == {"excrept": "use 'excerpt'"}
    assert error.data["retry_select"] == ["node_id", "excerpt"]
    # A replacement the caller already named: the refused name is dropped.
    error = _refusal(["citation", "url"], _LEXICAL_FIELDS, "search(method=lexical)")
    assert error.data["retry_select"] == ["citation"]


def test_the_retry_is_rebuilt_from_the_callers_select_under_source_lineage():
    """The server rewrites ["source_lineage", "excerpt", "id"] to the
    operator list ["excerpt", "id", "attrs"] before validation; the retry
    names the caller's own list, never the rewritten one."""
    from okg.substrate.mcp.response_shape import lineage_select_plan

    caller = ["source_lineage", "excerpt", "id"]
    operator_select, requested, _, _ = lineage_select_plan(
        caller, default_without_attrs=("excerpt",))
    assert requested and operator_select == ["excerpt", "id", "attrs"]
    with pytest.raises(SelectUnknownField) as caught:
        with select_retry_from(caller):
            validate_select(operator_select, _LEXICAL_FIELDS,
                            tool_name="search(method=lexical)",
                            elsewhere=search_select_elsewhere("lexical"))
    error = caught.value
    assert error.data["unknown_fields"] == ["id"]
    assert error.data["retry_select"] == ["source_lineage", "excerpt", "node_id"]
    assert error.fix_hint == (
        "id: use 'node_id'; retry with select=['source_lineage', 'excerpt', "
        f"'node_id']; the tool's selectable fields are {sorted(_LEXICAL_FIELDS)}")
    # Without the rebuild the retry would carry the server's `attrs`.
    plain = _refusal(operator_select, _LEXICAL_FIELDS, "search(method=lexical)",
                     search_select_elsewhere("lexical"))
    assert plain.data["retry_select"] == ["excerpt", "node_id", "attrs"]


def test_a_caller_select_of_only_the_token_and_refused_names_keeps_the_token():
    with pytest.raises(SelectUnknownField) as caught:
        with select_retry_from(["source_lineage", "zzz"]):
            validate_select(["zzz", "attrs"], _LEXICAL_FIELDS,
                            tool_name="search(method=lexical)")
    assert caught.value.data["retry_select"] == ["source_lineage"]


def test_lexical_id_type_names_identifier_mentions():
    error = _refusal(["id_type"], _LEXICAL_FIELDS, "search(method=lexical)",
                     search_select_elsewhere("lexical"))
    assert error.data["instead"] == {
        "id_type": "returned by search(method=identifier_mentions), not method=lexical"}


def test_expand_node_fields_point_at_the_edge_and_the_path_shape():
    from okg.substrate.mcp.tools import EXPAND_SELECT_ELSEWHERE

    error = _refusal(["node_id", "subtype"], _LIST_NEIGHBORS_FIELDS, "expand",
                     EXPAND_SELECT_ELSEWHERE)
    assert "the neighbour's id is dst" in error.data["instead"]["node_id"]
    assert "path and depth>1 rows carry node_id" in error.data["instead"]["node_id"]
    # 1-hop refuses target_subtype; its type filter is `filter`.
    assert "target_subtype" not in error.data["instead"]["subtype"]
    assert '"field":"subtype"' in error.data["instead"]["subtype"]


def test_a_close_spelling_is_suggested_and_a_name_with_no_answer_gets_none():
    assert select_instead(["exerpt"], _LEXICAL_FIELDS) == {"exerpt": "use 'excerpt'"}
    assert select_instead(["label", "*"], _LIST_NEIGHBORS_FIELDS) == {}
    error = _refusal(["label"], _LIST_NEIGHBORS_FIELDS, "expand")
    assert "instead" not in error.data
    assert str(error) == "expand: unknown field(s) in select: ['label']"


def test_alias_rows_serve_node_id_when_selected():
    # Issue #3666 step 3a: the record an alias names is `node_id` there too,
    # selected only, so nothing points elsewhere for it.
    from okg.substrate.mcp.search import _ALIAS_SELECTABLE

    assert "node_id" not in _ALIAS_FIELDS and "node_id" in _ALIAS_SELECTABLE
    assert "node_id" not in search_select_elsewhere("alias")


def test_a_conventional_name_is_hinted_only_when_its_field_exists_here():
    # `node_id` is not an expand 1-hop field, so `id` gets no "use node_id".
    assert "id" not in select_instead(["id"], _LIST_NEIGHBORS_FIELDS)
    assert select_instead(["type"], _TRAVERSE_FIELDS) == {"type": "use 'subtype'"}


# ---------- PostgreSQL: the hints the real tools send ----------

def _instead(page: dict) -> dict:
    assert page.get("status") == "refused", page
    error = page["error"]
    assert error["code"] == "select_unknown_field", error
    return (error.get("data") or {}).get("instead") or error.get("instead") or {}


@pytest.fixture
def served(clean_runtime, okg_mcp_can_log_in, tmp_path):
    from tests.substrate.mcp.test_search_list import Served
    from tests.substrate.mcp.test_search_records import _basic_facts, _publish

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_basic_facts())
    server = Served(clean_runtime, deployment)
    try:
        yield server
    finally:
        server.close()


def _expand(served, **arguments) -> dict:
    """One `expand` call through an in-memory MCP client, JSON result."""
    return _call(served, "expand", **arguments)


def _call(served, tool: str, **arguments) -> dict:
    """One tool call through an in-memory MCP client, JSON result."""
    import json

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    out: list = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(served.mcp) as client:
            out.append(await client.call_tool(
                tool, arguments, meta={"okg/result_format": "json"},
            ))

    anyio.run(drive)
    return json.loads(out[0].content[0].text)


@pytest.mark.postgres
@pytest.mark.parametrize(("arguments", "name", "hint"), [
    ({"method": "lexical", "select": ["id", "excerpt"]}, "id", "use 'node_id'"),
    ({"method": "grep", "select": ["recency"]}, "recency",
     "returned by search(method=lexical), not method=grep"),
    ({"method": "lexical", "select": ["id_type"]}, "id_type",
     "returned by search(method=identifier_mentions), not method=lexical"),
])
def test_search_refusals_carry_the_hint(served, arguments, name, hint):
    page = served.json(query="zirconium", limit=3, **arguments)
    assert _instead(page)[name] == hint


@pytest.mark.postgres
def test_served_search_retry_names_the_callers_lineage_select(served):
    page = served.json(query="zirconium", limit=3, method="lexical",
                       select=["source_lineage", "excerpt", "id"])
    _instead(page)
    data = page["error"]["data"]
    assert data["retry_select"] == ["source_lineage", "excerpt", "node_id"]
    assert "retry with select=['source_lineage', 'excerpt', 'node_id']" in (
        page["error"]["fix_hint"])


@pytest.mark.postgres
def test_served_inspect_retry_names_the_callers_lineage_select(served):
    page = _call(served, "inspect", target={"kind": "node", "node_id": "ticket:long"},
                 select=["source_lineage", "subtype", "id"])
    _instead(page)
    assert page["error"]["data"]["retry_select"] == [
        "source_lineage", "subtype", "node_id"]


@pytest.mark.postgres
def test_served_expand_retry_names_the_callers_lineage_select(served):
    page = _expand(served, input={"node_id": "ticket:long"},
                   select=["source_lineage", "id", "edge_type"])
    _instead(page)
    # Issue #3666: a 1-hop row is the linked record, so `id` is its node_id.
    assert page["error"]["data"]["retry_select"] == [
        "source_lineage", "node_id", "edge_type"]


@pytest.mark.postgres
@pytest.mark.parametrize("arguments", [
    {},  # plain 1-hop: the linked records
    {"order_by": "node_id"},  # 1-hop through the effective-attrs page
])
def test_one_hop_expand_serves_the_record_fields(served, arguments):
    """Issue #3666: these two calls were refused (`node_id` and `subtype`
    are a record's fields, and a 1-hop row was a link). Both paths now
    return the linked records, shaped alike."""
    page = _expand(served, input={"node_id": "ticket:long"},
                   select=["node_id", "subtype"], **arguments)
    assert page.get("status") != "refused", page
    for row in page["neighbors"]:
        assert set(row) == {"node_id", "subtype", "title", "link", "link_count"}
        assert set(row["link"]) == {"edge_type", "direction", "edge_id"}


@pytest.mark.postgres
def test_path_expand_refusal_names_the_one_hop_shape(served):
    page = _expand(served, input={"node_id": "ticket:long"}, depth=2,
                   edge_type="contains", select=["src"])
    assert "a 1-hop expand has this field" in _instead(page)["src"]
