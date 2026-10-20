"""Every argument a served tool description names is in that tool's schema.

Issue #3666: served descriptions named arguments the tool does not publish
(`search` told callers about `retain`; `expand` about `target_subtype`), and
`inspect` named a target kind its enum does not list. An agent that follows
such a sentence is refused. No database: the surfaces are built in probe
mode, on the read surface and on the write/admin surface.

The rule, per tool:

- `name=` or `name:` written in a call is an argument, or a field inside
  one (`target={"kind": ...}`); it must be a property somewhere in the
  tool's published input schema. Text inside a call to another tool
  (`filter(input={"ref": id})` in search's description) is that tool's and
  is checked against that tool.
- A backticked name that is a parameter of the tool's Python function is an
  argument of the tool; it must be published. (Other backticked names are
  return fields and values, and are not checked.)
- A backticked name or `kind: "x"` in a description of `inspect` that is an
  inspect target kind on some surface must be in this surface's enum.
"""

from __future__ import annotations

import inspect as pyinspect
import re
from typing import Any

import pytest

from okg.substrate.mcp.surface_budget import PROBE_DSN

pytestmark = pytest.mark.anyio

_NAME = r"[a-z_][a-z0-9_]*"
_BACKTICKED = re.compile(rf"`({_NAME})`")
#: `name=`, `name:value`, or a quoted key `"name": ...`.
_ASSIGNED = re.compile(
    rf"(?<![\w.<])(?:({_NAME})(?:=(?!=)|:(?![/<\s]))|[\"']({_NAME})[\"']\s*:)"
)


def _assigned(text: str) -> set[str]:
    return {a or b for a, b in _ASSIGNED.findall(text)}
_KIND = re.compile(r"""["']?kind["']?\s*[:=]\s*["']([a-z_]+)["']""")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)


async def _served(allow_writes: bool) -> tuple[dict[str, Any], dict[str, set[str]]]:
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server

    previous = server_mod._server_dsn_resolution
    server_mod._server_dsn_resolution = lambda _dsn, _name: {
        "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
    }
    try:
        mcp = build_server(dsn=PROBE_DSN, allow_writes=allow_writes)
    finally:
        server_mod._server_dsn_resolution = previous
    try:
        tools = {tool.name: tool for tool in await mcp.list_tools()}
        python_params = {
            name: set(pyinspect.signature(registered.fn).parameters) - {"ctx"}
            for name, registered in mcp._tool_manager._tools.items()
        }
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return tools, python_params


def _top_level(tool: Any) -> set[str]:
    return set((tool.inputSchema or {}).get("properties", {}))


def _properties(schema: Any) -> set[str]:
    """Every property name at any depth of a schema."""
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            props = node.get("properties")
            if isinstance(props, dict):
                found.update(props)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(schema)
    return found


def _kinds(schema: Any) -> set[str]:
    """Every enum or const value of a `kind` property anywhere in the schema."""
    found: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            props = node.get("properties")
            kind = props.get("kind") if isinstance(props, dict) else None
            if isinstance(kind, dict):
                found.update(str(v) for v in kind.get("enum", []))
                if "const" in kind:
                    found.add(str(kind["const"]))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(schema)
    return found


def _spans(description: str, tool_names: set[str]) -> list[tuple[str, str]]:
    """`(tool, text)`: each call to a named tool, and the rest (`""`)."""
    pieces: list[tuple[str, str]] = []
    rest: list[str] = []
    call = re.compile(r"\b(" + "|".join(sorted(tool_names, key=len, reverse=True)) + r")\(")
    i = 0
    while True:
        match = call.search(description, i)
        if match is None:
            rest.append(description[i:])
            break
        rest.append(description[i:match.start()])
        depth, j = 1, match.end()
        while j < len(description) and depth:
            depth += {"(": 1, ")": -1}.get(description[j], 0)
            j += 1
        pieces.append((match.group(1), description[match.end():j - 1]))
        i = j
    pieces.append(("", " ".join(rest)))
    return pieces


#: Arguments whose value is a condition (`input.filter` is reached as the
#: `"filter":` key inside `input`). Prose may also write the value after the
#: name and a space (aggregate: `with where {"text":{"op":...}}`, Format v10).
_CONDITION_ARGUMENTS = ("where", "filter", "predicate")
_CONDITION_START = re.compile(
    r"""(?<![\w.<])["']?(""" + "|".join(_CONDITION_ARGUMENTS)
    + r""")["']?\s*(?:=(?!=)|:|\s)\s*\{"""
)


def _without_condition_values(text: str, schema: Any) -> str:
    """`text` with the value of each condition argument the schema declares
    blanked out, the argument's own name kept (so it is still checked).

    Short-form conditions use field names as keys (`where={"type": ...}`),
    and this rule was written before that form existed: a quoted key inside
    a condition's value is a condition key, not an argument. Everything
    outside such a value is checked exactly as before.
    """
    declared = _properties(schema)
    out: list[str] = []
    i = 0
    while True:
        match = _CONDITION_START.search(text, i)
        if match is None:
            out.append(text[i:])
            return "".join(out)
        if match.group(1) not in declared:
            out.append(text[i:match.end()])
            i = match.end()
            continue
        out.append(text[i:match.end() - 1])
        depth, j = 1, match.end()
        while j < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[j], 0)
            j += 1
        out.append("{}")
        i = j


def _argument_offences(tools: dict[str, Any], python_params: dict[str, set[str]]) -> list[str]:
    offences: list[str] = []
    for name, tool in sorted(tools.items()):
        for owner, text in _spans(tool.description or "", set(tools)):
            checked = owner or name
            schema = tools[checked].inputSchema
            text = _without_condition_values(text, schema)
            for arg in sorted(_assigned(text) - _properties(schema)):
                offences.append(f"{name}: writes `{arg}=`, which {checked} does not publish")
            if not owner:
                unpublished = python_params.get(name, set()) - _top_level(tool)
                for arg in sorted(set(_BACKTICKED.findall(text)) & unpublished):
                    offences.append(f"{name}: names its argument `{arg}`, which it does not publish")
    return offences


def _kind_offences(tools: dict[str, Any], every_kind: set[str]) -> list[str]:
    kinds = _kinds(tools["inspect"].inputSchema)
    offences = []
    for name, tool in sorted(tools.items()):
        for owner, text in _spans(tool.description or "", set(tools)):
            if (owner or name) != "inspect":
                continue
            said = set(_KIND.findall(text))
            if not owner:
                said |= set(_BACKTICKED.findall(text)) & every_kind
            for kind in sorted(said - kinds):
                offences.append(f"{name}: names inspect target kind {kind!r}, not in the enum")
    return offences


@pytest.mark.parametrize("allow_writes", [False, True], ids=["read", "write-admin"])
async def test_every_argument_a_description_names_is_published(allow_writes: bool) -> None:
    tools, python_params = await _served(allow_writes)
    offences = _argument_offences(tools, python_params)
    assert offences == [], "\n".join(offences)


async def test_every_inspect_target_kind_a_description_names_is_in_the_enum() -> None:
    surfaces = [await _served(flag) for flag in (False, True)]
    every_kind = set().union(*(_kinds(t["inspect"].inputSchema) for t, _ in surfaces))
    assert {"graph", "node", "subtype", "text"} <= every_kind, sorted(every_kind)
    offences = [o for tools, _ in surfaces for o in _kind_offences(tools, every_kind)]
    assert offences == [], "\n".join(offences)


class _Tool:
    def __init__(self, name: str, description: str, schema: dict[str, Any]) -> None:
        self.name, self.description, self.inputSchema = name, description, schema


def test_the_check_catches_each_reported_kind_of_false_sentence() -> None:
    """The three sentences found on experimental4 (#3666), on synthetic tools:
    an argument the tool does not take, an argument it takes but does not
    publish, and a target kind its enum lacks. A call to another tool is
    checked against that tool."""
    inspect_schema = {"properties": {"target": {"properties": {
        "kind": {"enum": ["graph", "node", "subtype", "text"]}}}}}
    tools = {
        "search": _Tool(
            "search",
            "`retain=true` adds a reference for filter(input={\"ref\": id}).",
            {"properties": {"query": {}}},
        ),
        "expand": _Tool(
            "expand", "`target_subtype` filters the terminal node.",
            {"properties": {"input": {}}},
        ),
        "filter": _Tool("filter", "", {"properties": {"input": {"properties": {"ref": {}}}}}),
        "inspect": _Tool("inspect", "`surface` with action='expand' widens it.", inspect_schema),
    }
    params = {"search": {"query"}, "expand": {"input", "target_subtype"}}
    assert _argument_offences(tools, params) == [
        "expand: names its argument `target_subtype`, which it does not publish",
        "inspect: writes `action=`, which inspect does not publish",
        "search: writes `retain=`, which search does not publish",
    ]
    assert _kind_offences(tools, {"graph", "node", "subtype", "text", "surface"}) == [
        "inspect: names inspect target kind 'surface', not in the enum",
    ]


def test_a_short_form_condition_key_is_not_an_argument() -> None:
    """A field name used as a key inside a declared condition (`where`) is a
    condition key: short-form conditions use field names as keys, and the
    rule was written before that form existed."""
    where_only = {"properties": {"where": {"type": "object"}, "limit": {}}}
    tools = {"filter": _Tool(
        "filter",
        'filter(where={"type":"doc","attrs.path":{"op":"suffix","value":"x"}}, limit=5).',
        where_only,
    )}
    assert _argument_offences(tools, {"filter": {"where", "limit"}}) == []


def test_a_condition_written_after_its_name_in_prose_is_a_condition() -> None:
    """Format v10's aggregate sentence writes the value after the name and a
    space (`with where {"text":{"op":...}}`); its keys are condition keys.
    Failure case: a made-up argument beside it is still refused."""
    where_only = {"properties": {"where": {"type": "object"}, "by": {}}}
    sentence = 'by=["match"] with where {"text":{"op":"matches","value":"re:/x/"}} counts'
    tools = {"aggregate": _Tool("aggregate", sentence, where_only)}
    assert _argument_offences(tools, {"aggregate": {"where", "by"}}) == []
    tools = {"aggregate": _Tool("aggregate", sentence + ' colour="red"', where_only)}
    assert _argument_offences(tools, {"aggregate": {"where", "by"}}) == [
        "aggregate: writes `colour=`, which aggregate does not publish",
    ]


def test_a_made_up_argument_beside_a_condition_is_still_refused() -> None:
    """Failure cases: the exemption covers only the value of a condition the
    schema declares. A top-level argument outside it is checked as before,
    and a condition argument the schema does not declare exempts nothing."""
    where_only = {"properties": {"where": {"type": "object"}}}
    tools = {"filter": _Tool(
        "filter", 'filter(where={"type":"doc"}, colour="red").', where_only,
    )}
    assert _argument_offences(tools, {"filter": {"where"}}) == [
        "filter: writes `colour=`, which filter does not publish",
    ]
    undeclared = {"filter": _Tool(
        "filter", 'filter(where={"type":"doc"}).', {"properties": {"limit": {}}},
    )}
    assert _argument_offences(undeclared, {"filter": {"limit"}}) == [
        "filter: writes `type=`, which filter does not publish",
        "filter: writes `where=`, which filter does not publish",
    ]
