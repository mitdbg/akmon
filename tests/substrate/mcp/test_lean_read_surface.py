"""The read tools publish only their shown forms; every old form still works.

Issue #3666, step M10 (PACT read-surface-diet-3666). `lean_surface` removes
arguments, `search` methods and `inspect` kinds from the PUBLISHED schemas.
These tests pin what is published, and that every argument, method and kind
the previous surface published still passes the checks a call meets before
a tool body runs: the grammar rewrite, the declared-argument check and the
argument model. Hermetic: `OKG_MCP_SURFACE_PROBE=1` builds the pools
unopened, so nothing here needs Postgres.
"""
from __future__ import annotations

import ast
import re
from typing import Any

import pytest

from okg.substrate.mcp.lean_surface import (
    HIDDEN_ARGUMENTS,
    PUBLISHED_AGAIN,
    SHOWN_INSPECT_KINDS,
    SHOWN_SEARCH_METHODS,
    WHERE_ADDED,
)
from okg.substrate.mcp.read_grammar import rewrite_call

pytestmark = pytest.mark.anyio

READ_TOOLS = ("search", "inspect", "expand", "filter", "map", "aggregate")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _build(
    monkeypatch: pytest.MonkeyPatch, *, lean: bool = True,
    allowlist: str | None = None, **kwargs: Any,
) -> Any:
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    if allowlist is None:
        monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    else:
        monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", allowlist)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod, "_server_dsn_resolution",
        lambda _dsn, _name: {"dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"}},
    )
    if not lean:
        import okg.substrate.mcp.lean_surface as lean_mod

        monkeypatch.setattr(lean_mod, "publish_lean_read_surface", lambda mcp: 0)
    return server_mod.build_server(dsn=PROBE_DSN, **kwargs)


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if getattr(mcp, "_okg_write_pool", None):
        mcp._okg_write_pool.close()


@pytest.fixture
def lean(monkeypatch):
    mcp = _build(monkeypatch)
    try:
        yield mcp
    finally:
        _close(mcp)


@pytest.fixture
def full(monkeypatch):
    """The surface as it was published before this change."""
    mcp = _build(monkeypatch, lean=False)
    try:
        yield mcp
    finally:
        _close(mcp)


async def _schemas(mcp: Any) -> dict[str, dict]:
    return {t.name: t.inputSchema for t in await mcp.list_tools()}


async def _descriptions(mcp: Any) -> dict[str, str]:
    return {t.name: t.description or "" for t in await mcp.list_tools()}


def _accepted(mcp: Any, tool: str, arguments: dict[str, Any]) -> None:
    """The checks a call meets before the tool body: the grammar rewrite,
    the declared-argument check and the argument model. Raises on refusal."""
    rewritten = rewrite_call(tool, arguments)
    mcp._refuse_unknown_operator_arguments(tool, rewritten.arguments)
    mcp._validate_operator_arguments(tool, rewritten.arguments)


# ---------- what is published ----------


async def test_the_hidden_arguments_are_not_published(lean):
    schemas = await _schemas(lean)
    for tool in READ_TOOLS:
        published = set(schemas[tool]["properties"])
        assert not published & set(HIDDEN_ARGUMENTS[tool]), tool
        # The top level stays open: a client validating against it does not
        # refuse a hidden argument.
        assert schemas[tool].get("additionalProperties") is not False, tool
    for tool in WHERE_ADDED + ("search",):
        where = schemas[tool]["properties"]["where"]
        assert where["type"] == "object" and '"op": "prefix"' in where["description"]
    for tool in ("search", "filter"):
        assert "between" in schemas[tool]["properties"]["where"]["description"]
    assert "select" in schemas["map"]["properties"]
    # read-passage-rows-3666 (D3): search keeps `select` published and hides
    # `max_bytes`; every other read tool keeps `max_bytes`.
    assert "select" in schemas["search"]["properties"]
    assert "max_bytes" not in schemas["search"]["properties"]
    assert "max_bytes" in HIDDEN_ARGUMENTS["search"]
    for tool in READ_TOOLS:
        if tool != "search":
            assert "max_bytes" in schemas[tool]["properties"], tool
    assert schemas["aggregate"]["required"] == ["by", "metrics"]
    assert "required" not in schemas["filter"] and "required" not in schemas["map"]


async def test_the_arguments_published_again_are_published_with_their_short_text(lean):
    """PACT publish-paging-args-3666: `offset` on every tool whose cut page
    names it, and `search.max_chars`, each with its one-line description;
    none of them is listed as hidden. `map.input` and `aggregate.input`
    (named by retained-page and link-count hints) are the description alone
    and never required."""
    from okg.substrate.mcp.lean_surface import UNTYPED

    schemas = await _schemas(lean)
    # read-passage-rows-3666 (D1): `view` too, for the short rows ("list").
    assert set(PUBLISHED_AGAIN["search"]) == {
        "offset", "continuation_token", "max_chars", "expand_concepts", "view",
    }
    assert "list" in str(schemas["search"]["properties"]["view"])
    assert set(PUBLISHED_AGAIN["map"]) == set(PUBLISHED_AGAIN["aggregate"]) == {"offset", "input"}
    assert "input" not in PUBLISHED_AGAIN["filter"]
    assert PUBLISHED_AGAIN["expand"]["offset"] == "One hop only."
    for tool, arguments in PUBLISHED_AGAIN.items():
        for argument, description in arguments.items():
            body = schemas[tool]["properties"][argument]
            assert body.get("description", "") == description, (tool, argument)
            assert "default" not in body, (tool, argument)
            assert argument not in HIDDEN_ARGUMENTS[tool], (tool, argument)
            if (tool, argument) in UNTYPED:
                assert body == {"description": description}, (tool, argument)
                assert argument not in schemas[tool].get("required", []), (tool, argument)
    assert schemas["search"]["properties"]["max_chars"]["maximum"] == 4096
    assert '{"ref": id}' in schemas["map"]["properties"]["input"]["description"]


def test_publishing_again_an_undeclared_argument_refuses_the_build(monkeypatch):
    """The failure case: an argument the tool does not declare is refused at
    build time, not published as an empty shape."""
    import okg.substrate.mcp.lean_surface as lean_mod
    from okg.substrate.mcp.operator_models import ToolSchemaUnpublishable

    monkeypatch.setitem(lean_mod.PUBLISHED_AGAIN, "inspect", {"offset": "x"})
    with pytest.raises(ToolSchemaUnpublishable, match="inspect.offset"):
        _close(_build(monkeypatch))


def test_an_entry_the_diet_never_hid_is_refused_at_import():
    """The import-time check: every published-again argument was hidden by
    the diet. A name outside that list (a typo, or a never-hidden argument)
    is refused, and the shipped lists pass."""
    import okg.substrate.mcp.lean_surface as lean_mod
    from okg.substrate.mcp.operator_models import ToolSchemaUnpublishable

    lean_mod.check_published_again(lean_mod.PUBLISHED_AGAIN, lean_mod._DIET_HIDDEN)
    with pytest.raises(ToolSchemaUnpublishable, match="never hid"):
        lean_mod.check_published_again({"search": {"max_char": ""}}, lean_mod._DIET_HIDDEN)
    with pytest.raises(ToolSchemaUnpublishable, match="never hid"):
        lean_mod.check_published_again({"search": {"where": ""}}, lean_mod._DIET_HIDDEN)


async def test_search_publishes_lexical_and_grep_only(lean):
    props = (await _schemas(lean))["search"]["properties"]
    # In words, not a closed enum: a validating client must still accept
    # the methods the server runs and no longer shows.
    assert props["method"]["description"] == " or ".join(SHOWN_SEARCH_METHODS) + "."
    assert "enum" not in props["method"]
    assert props["method"]["default"] == "lexical"
    # The select description names only the shown methods.
    for hidden in ("semantic", "alias", "identifier_mentions", "attrs:"):
        assert hidden not in props["select"]["description"]
    # The names an agent may select, lexical whole and grep as its
    # difference: with the list cut, agents selected `where`'s title, path
    # and source (#3666). The description names the field holding each.
    description = props["select"]["description"]
    lexical = dict(lean._okg_capabilities.select_fields)["lexical"]
    listed = description.split("lexical: ", 1)[1].split(".", 1)[0].split(", ")
    assert sorted(listed) == sorted(set(lexical)), description
    assert "grep has no " in description
    # read-passage-rows-3666 (D6): then the dotted form, with one generic
    # example; nothing presents attrs as the way to a record's full text.
    assert description.endswith(
        " title and path are in handle; source is in provenance."
        ' One attribute: attrs.<key>, e.g. "attrs.status".'
    )
    assert "full text" not in description
    # Failure case: no `where`-only name is offered as a field.
    assert not {"title", "path", "source"} & set(listed)
    assert {"handle", "provenance"} <= set(listed)
    # No items enum: a validating client must not refuse `title` before the
    # server serves it from `handle`, nor a hidden method's own field.
    assert props["select"]["items"] == {"type": "string"}
    # A field only a hidden method returns is in the `surface` listing.
    (mentions,) = [
        line for line in lean._okg_lean_hidden_lines
        if line.startswith('search(method="identifier_mentions", select=...)')
    ]
    assert "id_type" in mentions and "node_id" not in mentions.split(": ", 1)[1]
    assert lean._okg_surface_freshness["lean_read_surface"]["shown_search_methods"] == [
        "lexical", "grep",
    ]


async def test_inspect_publishes_the_six_shown_kinds(lean):
    target = (await _schemas(lean))["inspect"]["properties"]["target"]
    kinds: list[str] = []
    *members, open_member = target["anyOf"]
    # The last member is open to every kind no shown member names, so a
    # client that validates its arguments still accepts a hidden kind and
    # still refuses a malformed shown one.
    assert open_member["required"] == ["kind"]
    assert sorted(open_member["properties"]["kind"]["not"]["enum"]) == sorted(
        SHOWN_INSPECT_KINDS
    )
    for member in members:
        kind = member["properties"]["kind"]
        kinds += [kind["const"]] if "const" in kind else kind["enum"]
    assert sorted(kinds) == sorted(SHOWN_INSPECT_KINDS)
    assert target["description"].startswith("Required unless `next` is given.")


def _published_kinds(schema: dict) -> set[str]:
    kinds: set[str] = set()
    for member in schema["properties"]["target"]["anyOf"][:-1]:
        kind = member["properties"]["kind"]
        kinds |= {kind["const"]} if "const" in kind else set(kind["enum"])
    return kinds


async def test_a_write_launch_also_publishes_the_write_kinds(monkeypatch):
    """A write agent needs the admission, record and surface kinds; a write
    launch keeps publishing every write-side kind it serves."""
    mcp = _build(monkeypatch, allow_writes=True)
    try:
        kinds = _published_kinds((await _schemas(mcp))["inspect"])
    finally:
        _close(mcp)
    assert set(SHOWN_INSPECT_KINDS) | {
        "admission", "record", "record_templates", "record_prefixes", "surface",
    } <= kinds
    assert "coverage" not in kinds and "ontology" not in kinds


async def test_a_read_launch_lists_what_it_accepts_and_does_not_show(lean):
    """`inspect(target={"kind": "surface"})` is served read-only on a read
    launch: one line per hidden argument, method and kind, and no process
    report. Expanding the session is still refused without a ceiling."""
    from okg.substrate.mcp.operators import OPERATOR_TOOLS

    assert "surface" not in _published_kinds((await _schemas(lean))["inspect"])
    tool = lean._tool_manager._tools["inspect"]
    page = tool.fn(target={"kind": "surface"})
    assert page["surface"] == "read_only"
    assert set(page["served_tools"]) == set(OPERATOR_TOOLS)
    lines = page["accepted_not_shown"].split("\n")
    for expected in ("search(method=...): also", "expand(depth=...)",
                     "filter(input=...)", 'inspect target kind "coverage"', "where: also"):
        assert any(line.startswith(expected) for line in lines), expected
    # read-passage-rows-3666 (D1): search's `view` is published, so it is
    # not listed as accepted-but-hidden.
    assert not any(line.startswith("search(view=...)") for line in lines)
    # Nothing is cut, and the forms say what they do.
    assert not any(line.endswith("...") for line in lines)
    assert any(line.startswith("search(rank_by=...)") and "`self` ranks" in line
               for line in lines)
    assert any(line.startswith("search(summary=...)") and "top 50 groups" in line
               for line in lines)
    assert any(line.startswith("dates: ") and "bare `date` is refused" in line
               for line in lines)
    assert "principal_readiness" not in page and "capabilities" not in page
    # Failure case: the expansion door stays shut on a launch with no ceiling.
    refused = tool.fn(target={"kind": "surface", "action": "expand"})
    assert refused["status"] == "refused"
    assert refused["error"]["code"] == "mcp_tool_surface_expansion_unavailable"


def test_search_still_accepts_the_max_bytes_it_does_not_publish(lean):
    """read-passage-rows-3666 (D3): hidden, not refused. The `surface`
    listing says what it does."""
    _accepted(lean, "search", {"query": "deploy", "max_bytes": 8192})
    (line,) = [
        hidden for hidden in lean._okg_lean_hidden_lines
        if hidden.startswith("search(max_bytes=...)")
    ]
    assert "keeps every row" in line


@pytest.mark.parametrize("tool,arguments", [
    ("filter", {}),
    ("filter", {"select": ["node_id"], "limit": 5}),
    ("map", {"select": ["node_id", "attrs.path"]}),
    ("aggregate", {"by": ["subtype"], "metrics": ["count"]}),
])
async def test_no_where_and_no_input_is_every_record_with_a_note(lean, tool, arguments):
    rewritten = rewrite_call(tool, arguments)
    assert rewritten.arguments["input"] == {"kind": "nodes"}
    if tool == "filter":
        assert rewritten.arguments["predicate"] == {}
    assert [r["rule"] for r in rewritten.adjusted] == ["scope_defaulted"]
    _accepted(lean, tool, arguments)


def test_a_where_or_an_input_is_never_defaulted():
    """Failure case: a call that names its set is served as sent."""
    for tool, arguments in [
        ("filter", {"where": {"type": "doc"}}),
        ("map", {"input": {"kind": "edges"}, "projection": ["edge_id"]}),
        ("aggregate", {"where": {}, "by": ["subtype"], "metrics": ["count"]}),
    ]:
        rewritten = rewrite_call(tool, arguments)
        assert not any(r["rule"] == "scope_defaulted" for r in rewritten.adjusted)


async def test_the_read_tools_publish_far_fewer_tokens(lean, full):
    """The six read tools' schemas and descriptions, in cl100k tokens:
    exactly the measured 2,825 (from 4,672 on dev at c8e12c31c68). The shorter descriptions
    are in the docstrings, so the surface with this pass switched off already
    has them; the pass alone removes about 1,450 more (3,938 to 2,488)."""
    from okg.substrate.mcp.surface_budget import count_tokens

    def total(tools) -> int:
        return sum(
            count_tokens(t.model_dump_json(by_alias=True, exclude_none=True))
            for t in tools if t.name in READ_TOOLS
        )

    after = total(await lean.list_tools())
    before = total(await full.list_tools())
    # Jason 2026-10-14, option (c): the 2,500 target belongs to the separate
    # redesign guidance set (OKG_MCP_GUIDANCE=diet, built later); the in-repo
    # guidance is pinned at its measured value and re-measured when the
    # guidance item lands. Measured on the lab at read-v2 (M10 with dev's 3b
    # and #3714, M7, the expand fixes and 2c): 2,621; then 2,609 when inspect
    # dropped the `surface` sentence; then 2,636 when search and filter
    # named `like` (loose-match-3666, +27); then 2,762 when offset (five
    # tools, +15 each bare; expand +7 more for "One hop only."), search's
    # continuation_token (+8, bare) and max_chars (+36 with its line) were
    # published again (publish-paging-args-3666, +126); then 2,825 when the
    # arguments other hints name were published too: search's
    # expand_concepts (+8), map's input (+19) and aggregate's input (+36),
    # both untyped with one line (+63). The diet guidance set's 2,500 target
    # is not on dev; when it lands it takes these +189 with it.
    # Then read-passage-rows-3666 merged onto dev (measured on the lab):
    # search hides max_bytes, its max_chars line names no default, its
    # select line adds the attrs.<key> form, and search publishes `view`
    # again (D1, "list"); on segment B alone that was -8 then +29.
    # Then read-defects-rd-3666 merged (Format v7): inspect says `lines`
    # reads "a list of" [first,last] line ranges (+4), filter names
    # `contains` with text on attrs.<key> beside its `like` sentence (+11),
    # and search's "filter's like matches substrings" gives way to
    # "`contains` finds text in attrs.<key>, path or title." (+7): +22, as
    # the full read pins (per tool, measured on the lab).
    # Then 2,898 (+30, Format v10 item C): aggregate's description shows
    # by=["match"] with a where text regex, and its where lists `text`.
    # Then 2,927 (+29, read-reply-size-3666): search's description says
    # "Read narrow first: ids and titles (select=[...] or a small limit),
    # then read the records that matter."
    # Then 2,973 (+46, read-search-caps-3666): search's description gains
    # the within="record" sentence and its schema the `within` enum.
    # Then 3,005 (+32, read-aggregate-variants-3666): aggregate's
    # description names norm(match), words(match) and the capture group.
    # Then 3,026 (+21, read-names-list-3666): search's description gains
    # the grep names-list sentence.
    # Then 3,108 (+82, read fixes): live `each`, kept-answer
    # sum/min/max, and inspect text `matching`.
    assert after == 3_108, after
    assert before - after >= 1_200, (before, after)


# ---------- every old form is still accepted ----------


async def test_every_argument_the_old_surface_published_is_still_declared(lean, full):
    old = await _schemas(full)
    for tool in READ_TOOLS:
        declared = set(lean._tool_manager._tools[tool].fn_metadata.arg_model.model_fields)
        missing = set(old[tool]["properties"]) - declared
        assert not missing, (tool, missing)


async def test_every_old_search_method_and_inspect_kind_still_validates(lean, full):
    old = await _schemas(full)
    for method in old["search"]["properties"]["method"]["enum"]:
        _accepted(lean, "search", {"query": "x", "method": method})
    members = old["inspect"]["properties"]["target"]["oneOf"]
    samples = {
        "node": {"node_id": "n:1"}, "nodes": {"node_ids": ["n:1"]},
        "text": {"node_id": "n:1"}, "subtype": {"subtype": "doc"},
        "texts": {"items": [{"key": "a", "node_id": "n:1"}]},
    }
    seen = []
    for member in members:
        kind = member["properties"]["kind"]
        for value in [kind["const"]] if "const" in kind else kind["enum"]:
            _accepted(lean, "inspect", {"target": {"kind": value, **samples.get(value, {})}})
            seen.append(value)
    assert {"coverage", "ontology", "view", "attr", "generation", "session"} <= set(seen)


#: Argument shapes the existing operator tests send (test_search_list,
#: test_grep_list, test_record_windows_e2e, test_search_profiles,
#: test_result_references, test_typed_operator_inputs, test_expand_*,
#: test_aggregate_periods), each in a spelling the lean surface no longer
#: publishes.
OLD_CALLS: list[tuple[str, dict[str, Any]]] = [
    ("search", {"query": "x", "view": "list", "limit": 100}),
    ("search", {"query": "x", "method": "grep", "view": "windows",
                "matching": "record", "context_chars": 120}),
    ("search", {"query": "x", "retain": True, "view": "metadata", "summary": ["subtype"]}),
    ("search", {"query": "x", "rank_by": "self", "max_per_parent": 2}),
    ("search", {"query": "x", "profile": "all", "source_family": ["git"]}),
    ("search", {"query": "x", "offset": 10, "max_chars": 1024, "expand_concepts": False}),
    ("search", {"query": "x", "method": "grep", "continuation_token": "t"}),
    ("search", {"query": "x", "generation_id": 7, "authority": {"principal": "p"}}),
    ("search", {"query": "x", "allow_unverified_embedding_provenance": True,
                "method": "semantic"}),
    ("search", {"query": "x", "subtype_filter": "doc", "attr_filters": {"k": "v"}}),
    ("search", {"query": "x", "filter": {"subtype": "doc"}}),
    ("search", {"query": "doc:1", "method": "identifier_mentions"}),
    ("search", {"method": "attrs", "where": {"op": "eq", "field": "attrs.k", "value": "v"}}),
    ("search", {"query": "x", "method": "alias"}),
    ("inspect", {"target": {"kind": "node", "node_id": "n:1", "hydrate": True}}),
    ("inspect", {"target": {"kind": "graph"}, "generation_id": "g", "authority": {}}),
    ("inspect", {"target": {"kind": "coverage"}}),
    ("inspect", {"target": {"kind": "ontology"}}),
    ("inspect", {"target": {"kind": "view", "name": "v_nodes"}}),
    ("inspect", {"target": {"kind": "generation_delta", "from_generation": 1}}),
    ("inspect", {"target": {"kind": "session", "action": "refresh"}}),
    ("expand", {"input": {"node_id": "n:1"}, "edge_type": "e", "depth": 2}),
    ("expand", {"input": "n:1", "edge_type": "e", "target_subtype": "doc",
                "path": ["e:out"]}),
    ("expand", {"input": {"node_id": "n:1"}, "edge_type": "e", "offset": 50,
                "filter": {"op": "eq", "field": "subtype", "value": "doc"}}),
    ("expand", {"input": {"kind": "node", "node_id": "n:1"}, "include_siblings": True}),
    ("expand", {"input": "n:1", "continuation_token": "t", "generation_id": 3}),
    ("expand", {"input": "n:1", "live": {"relation": "r", "args": {}}}),
    ("filter", {"input": {"kind": "nodes", "subtype": "doc"},
                "predicate": {"op": "suffix", "field": "attrs.path", "value": "a.md"},
                "offset": 50, "generation_id": 1}),
    ("filter", {"input": {"kind": "edges", "edge_type": "e"}, "predicate": {}}),
    ("filter", {"input": {"ref": "r1.abc"}, "predicate": {}, "retain": True}),
    ("map", {"input": {"kind": "nodes", "subtype": "doc"}, "projection": ["node_id"],
             "offset": 50}),
    ("map", {"input": {"kind": "nodes"}, "projection": {"p": "attrs.path"}}),
    ("map", {"input": {"ref": "r1.abc"}}),
    ("aggregate", {"input": {"kind": "nodes", "subtype": "doc"}, "by": ["attrs.k"],
                   "metrics": ["count"], "offset": 50, "authority": {}}),
    ("aggregate", {"input": "n:1", "by": ["edge_type", "dst_subtype"], "metrics": ["count"]}),
    ("aggregate", {"input": {"kind": "edges"}, "by": ["edge_type"], "metrics": ["count"]}),
]


@pytest.mark.parametrize("tool,arguments", OLD_CALLS, ids=[
    f"{tool}-{i}" for i, (tool, _) in enumerate(OLD_CALLS)
])
async def test_every_old_argument_spelling_is_still_accepted(lean, tool, arguments):
    _accepted(lean, tool, arguments)


async def test_a_validating_client_accepts_every_old_spelling(lean):
    """A client that checks its arguments against `tools/list` must not be
    told a call the server accepts is invalid: the top-level objects stay
    open, `inspect.target` ends with an open member and `expand.input`
    still takes an object."""
    import jsonschema

    schemas = await _schemas(lean)
    for tool, arguments in OLD_CALLS:
        jsonschema.validate(arguments, schemas[tool])


async def test_an_unknown_argument_is_still_refused(lean):
    """Failure case: hiding arguments did not loosen the declared-argument
    check; a name no model declares is refused by name."""
    from okg.substrate.mcp.response_bounds import UnknownOperatorArgument

    with pytest.raises(UnknownOperatorArgument):
        _accepted(lean, "filter", {"where": {"type": "doc"}, "sumary": True})
    with pytest.raises(Exception):
        _accepted(lean, "search", {"query": "x", "method": "fuzzy"})


# ---------- every example in the served text is served as written ----------

#: Values for the placeholders the descriptions use.
_PLACEHOLDERS = {
    "<id>": "doc:001", "<words>": "deploy", "<type>": "doc",
    "<link>": "contains", "<key>": "path", "<subtype>": "doc",
}
_CALL = re.compile(r"\b(search|inspect|expand|filter|map|aggregate)\(")


def example_calls(text: str) -> list[str]:
    """Every `tool(...)` written out in `text`, balanced on parentheses."""
    out = []
    for match in _CALL.finditer(text):
        depth, start = 0, match.start()
        if text[start - 1:start] == "/":
            # A slash list of tools (`map/filter/aggregate(...)`) names
            # where an id goes; it is not one call.
            continue
        for at in range(match.end() - 1, len(text)):
            depth += {"(": 1, ")": -1}.get(text[at], 0)
            if depth == 0:
                out.append(text[start:at + 1])
                break
    return out


def parse_call(call: str) -> tuple[str, dict[str, Any]]:
    tool, body = call.split("(", 1)
    body = body[:-1]
    for placeholder, value in _PLACEHOLDERS.items():
        body = body.replace(placeholder, value)
    tree = ast.parse(f"f({body})", mode="eval").body
    assert isinstance(tree, ast.Call)
    return tool, {kw.arg: ast.literal_eval(kw.value) for kw in tree.keywords}


def _runnable(call: str) -> bool:
    """A call with arguments: `search(query, where?, ...)`-style signatures
    and bare names (`map/filter/aggregate(input=...)`) are not examples."""
    return "=" in call and "?" not in call and "..." not in call


async def test_every_example_call_in_the_descriptions_passes_the_argument_checks(lean):
    descriptions = await _descriptions(lean)
    checked = 0
    for tool in READ_TOOLS:
        for call in example_calls(" ".join(descriptions[tool].split())):
            if not _runnable(call):
                continue
            name, arguments = parse_call(call)
            rewritten = rewrite_call(name, arguments)
            assert not rewritten.adjusted, (call, rewritten.adjusted)
            _accepted(lean, name, arguments)
            checked += 1
    assert checked >= 6, checked


async def test_aggregate_names_the_text_condition_and_counting_per_match(lean):
    """Format v10 item C (#3666): agents never found `text` in aggregate's
    `where` nor that by=["match"] over a regex groups each distinct match.
    The description shows the form, and the form is served as written."""
    import json

    description = " ".join((await _descriptions(lean))["aggregate"].split())
    shown = '{"text":{"op":"matches","value":"re:/<regex>/"}}'
    assert f'by=["match"] with where {shown} counts each distinct match' in description
    where = (await _schemas(lean))["aggregate"]["properties"]["where"]["description"]
    assert "Fields: node_id, type, text, attrs.<key>, date.<name>." in where
    arguments = {
        "where": json.loads(shown.replace("<regex>", "ABC-[0-9]+")),
        "by": ["match"], "metrics": ["count"],
    }
    assert rewrite_call("aggregate", arguments).adjusted == ()
    _accepted(lean, "aggregate", arguments)


def test_the_example_reader_reads_nested_calls():
    text = 'e.g. aggregate(where={"type":"<subtype>"}, by=["attrs.<key>"], metrics=["count"]).'
    (call,) = example_calls(text)
    assert parse_call(call) == (
        "aggregate", {"where": {"type": "doc"}, "by": ["attrs.path"], "metrics": ["count"]},
    )


# ---------- a search page never lists its profile's type list ----------


def _page(**extra: Any) -> dict[str, Any]:
    return {
        "operator_name": "search", "method": "lexical", "status": "ok",
        "results": [{"node_id": "doc:1", "subtype": "doc", "excerpt": "x"}],
        "total_count": 1, "search_profile": "default",
        "scoped_subtypes": ["doc", "ticket", "commit"], **extra,
    }


def _first_line(text: str) -> str:
    return text.split("\n", 1)[0]


# #3666 (read-passage-rows, R1): the profile's type list was printed on a
# session's first search and named by profile after that (M10). Readers
# never acted on it, so a search status line no longer prints it at all;
# the JSON form keeps `scoped_subtypes`. A caller's own condition is still
# printed as sent, on every page.


def test_the_profile_types_are_never_listed_on_a_search_status_line():
    from okg.substrate.mcp.result_text import render

    state: dict[str, Any] = {}
    for _ in range(2):
        first = _first_line(render(_page(), tool="search", session_state=state))
        assert "where" not in first and "ticket" not in first, first
    # Without a session (a measurement) the list is not printed either, so
    # a fitted page never grows when served.
    assert "where" not in _first_line(render(_page(), tool="search"))


def test_a_callers_condition_is_printed_every_time():
    """Failure case: dropping the profile's list must not drop the caller's
    own condition, with or without a session."""
    from okg.substrate.mcp.result_text import render

    state: dict[str, Any] = {}
    for session in (state, state, None):
        line = _first_line(render(
            _page(where_applied="type = doc"), tool="search", session_state=session,
        ))
        assert "where type = doc" in line, line
    # A scope that is not a named profile's default is still said.
    line = _first_line(render(_page(search_profile=None), tool="search"))
    assert 'where type in ["doc","ticket","commit"]' in line, line


# ---------- the instructions name exactly the tools this launch lists ----------


@pytest.mark.parametrize("allowlist,profile", [
    # Six read tools, no `query` (the X2 setup offered these, #3666).
    ("inspect,search,expand,filter,map,aggregate", None),
    ("inspect,search,filter", None),
    (None, "discovery"),
    (None, None),
])
async def test_the_tool_sentence_names_exactly_the_listed_tools(
        monkeypatch, allowlist, profile):
    """Generated from what this launch registers, never a fixed list: a tool
    the launch does not list (here `query`, and `expand` where it is cut) is
    never named, and every listed tool is."""
    from okg.substrate.mcp.tool_profiles import render_tool_list

    mcp = _build(monkeypatch, allowlist=allowlist, tool_profile=profile)
    try:
        listed = [t.name for t in await mcp.list_tools()]
        (said,) = re.findall(r"serves exactly these tools: ([^.]*)\.", mcp.instructions)
        assert said == render_tool_list(listed)
        named = set(re.findall(r"[a-z_]+", said)) - {"and"}
        assert named == set(listed), (named, listed)
        if allowlist is not None:
            assert set(listed) == set(allowlist.split(","))
            # Failure case: query is registered by no allowlist here.
            assert "query" not in named and "`query`" not in mcp.instructions
    finally:
        _close(mcp)


# ---------- a select naming a `where` field ----------


@pytest.mark.parametrize("arguments,served,renamed", [
    # The calls agents sent (X2 board e3mcp, comp-ops 42), as sent.
    ({"query": "delete rule child rule detach", "limit": 8,
      "select": ["node_id", "subtype", "title", "excerpt", "source", "attrs"]},
     ["node_id", "subtype", "handle", "excerpt", "provenance", "attrs"],
     ["select `title`", "select `source`"]),
    ({"query": "relval batchor assignor", "limit": 10,
      "select": ["node_id", "subtype", "title", "path", "excerpt", "source"]},
     ["node_id", "subtype", "handle", "excerpt", "provenance"],
     ["select `title`, `path`", "select `source`"]),
    ({"query": "CondorJobsFraction", "limit": 10,
      "select": ["node_id", "subtype", "source", "path", "excerpt", "attrs"], "max_bytes": 8000},
     ["node_id", "subtype", "provenance", "handle", "excerpt", "attrs"],
     ["select `source`", "select `path`"]),
    # On grep, beside a field already named: served once.
    ({"query": "deploy", "method": "grep", "select": ["node_id", "handle", "path"]},
     ["node_id", "handle"], ["select `path`"]),
])
def test_a_where_field_in_select_is_served_as_the_field_that_holds_it(
        arguments, served, renamed):
    rewritten = rewrite_call("search", arguments)
    assert rewritten.arguments["select"] == served
    notes = [r for r in rewritten.adjusted if r["rule"] == "select_renamed"]
    assert [r["sent"] for r in notes] == renamed
    assert all(r["served"].startswith(("`handle`, which holds the title, path and date",
                                       "`provenance`, which holds the source"))
               for r in notes)
    assert rewritten.select_retry is None


@pytest.mark.parametrize("arguments,retry", [
    # A name no row field holds: refused, and the call without it offered.
    ({"query": "deploy", "limit": 10,
      "select": ["node_id", "subtype", "title", "excerpt", "created"]},
     ["node_id", "subtype", "handle", "excerpt"]),
    ({"query": "deploy", "select": ["node_id", "snippet", "attrs"]},
     ["node_id", "excerpt", "attrs"]),
])
def test_a_select_no_row_field_answers_is_refused_with_the_call_without_it(arguments, retry):
    # The tool refuses; the rewrite carries the call it offers.
    rewritten = rewrite_call("search", arguments)
    assert not rewritten.adjusted
    assert rewritten.arguments["select"] == arguments["select"]
    assert rewritten.select_retry == {**arguments, "select": retry}
    assert rewritten.select_retry_mode == "corrected"


def test_nothing_is_offered_when_no_selected_name_is_a_row_field():
    """Failure case: no field left to select gives words, not a call."""
    rewritten = rewrite_call("search", {"query": "deploy", "select": ["created", "label"]})
    assert rewritten.select_retry is None
    # And a `where` field is never read on another tool's rows.
    expand = rewrite_call("expand", {"input": {"node_id": "doc:001"},
                                     "select": ["title", "dst"]})
    assert not any(r["rule"] == "select_renamed" for r in expand.adjusted)
    assert expand.select_retry is None
    # A grep list row names its record by its handle (PACT
    # read-names-list-3666): `title` is served as the handle there now. A
    # name no grep list row holds is still refused with the row's fields (a
    # view call is not offered).
    from okg.substrate.mcp.read_grammar import GrammarRefusal

    listed = rewrite_call("search", {"query": "deploy", "method": "grep", "view": "list",
                                     "select": ["node_id", "title"]})
    assert any(r["rule"] == "select_renamed" for r in listed.adjusted)
    with pytest.raises(GrammarRefusal) as caught:
        rewrite_call("search", {"query": "deploy", "method": "grep", "view": "list",
                                "select": ["node_id", "label"]})
    assert caught.value.code == "select_unknown_field"
    assert "matches" in caught.value.select_data["valid_fields"]
    assert caught.value.retry_arguments is None
