"""The seven read operators accept only typed inputs (issue #2009).

PACT `typed-operator-inputs`: req.toi.typed-inputs and
req.toi.no-rejected-params.

Before this change `inspect.target`, `expand.input`, `filter.input`,
`filter.predicate`, `map.input`, `map.projection` and `aggregate.input` were
bare `dict`/`Any`. The listed JSON schema said `{"type": "object"}` and an
agent learned the grammar by reading prose and provoking `ValueError`s.
`search` also listed a `scope` parameter the v1 runtime rejected outright.

These tests read the SDK's own `tools/list` payload — the bytes a client
actually receives — and drive the tool manager, which is the layer that
validates arguments before the operator body runs.

Hermetic: `OKG_MCP_SURFACE_PROBE=1` builds both connection pools UNOPENED, so
the real surface registers against an unreachable loopback DSN. Nothing here
needs Postgres.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from tests.substrate.mcp._result_payload import payload as _result_payload

from okg.substrate.mcp.effective_surface import READ_INSPECT_KINDS
from okg.substrate.mcp.operator_models import (
    INSPECT_TARGET_KINDS,
    PageResult,
    MAX_AGGREGATE_BY,
    MAX_DEPTH,
    MAX_OFFSET,
    MAX_PAGE_LIMIT,
    MAX_PATH_HOPS,
    MAX_PREVIEW_CHARS,
    MAX_SUBTYPE_PAGE_SIZE,
    PREDICATE_OPS,
    TNode,
    TSurface,
)
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
)


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


pytestmark = pytest.mark.anyio

#: Every operator input the PACT names, plus the two bound parameters that
#: carry a maximum. Kept as data so a new operator cannot quietly skip the
#: sweep tests below.
TYPED_INPUTS = {
    "inspect": ("target",),
    "search": ("method", "rank_by"),
    "expand": ("input", "direction", "filter", "path", "depth"),
    "filter": ("input", "predicate"),
    "map": ("input", "projection"),
    "aggregate": ("input", "by", "metrics"),
    "query": ("row_cap", "timeout_ms"),
}

#: Every bounded numeric parameter and the maximum it must publish.
#: `max_bytes` is DELIBERATELY absent and stays `Any`: `bound_read_tool`
#: answers a non-integer or out-of-range `max_bytes` with its own typed
#: refusal envelope naming the valid range, and
#: `test_response_byte_budget.py` pins that behavior. Typing it here would
#: replace a purpose-built refusal with a generic schema rejection.
EXPECTED_MAXIMA = {
    ("inspect", "target"): None,
    ("search", "limit"): MAX_PAGE_LIMIT,
    ("search", "offset"): MAX_OFFSET,
    # `max_per_parent` deliberately has NO maximum: `validate_max_per_parent`
    # refuses only values below 1, and a ceiling invented here would refuse
    # calls the runtime accepts.
    ("search", "max_chars"): MAX_PREVIEW_CHARS,
    ("expand", "limit"): MAX_PAGE_LIMIT,
    ("expand", "offset"): MAX_OFFSET,
    ("expand", "depth"): MAX_DEPTH,
    ("filter", "limit"): MAX_PAGE_LIMIT,
    ("filter", "offset"): MAX_OFFSET,
    ("map", "limit"): MAX_PAGE_LIMIT,
    ("map", "offset"): MAX_OFFSET,
    ("aggregate", "limit"): MAX_PAGE_LIMIT,
    ("aggregate", "offset"): MAX_OFFSET,
    ("query", "row_cap"): 10_000,
    ("query", "timeout_ms"): 60_000,
}


def _refusal(result) -> dict:
    """The refusal envelope the SDK actually hands a client.

    Read through the shared helper rather than off `content[0].text`: PACT
    `operator-output-contract` (issue #2010) moves the payload into
    `structuredContent`, and a test that reaches for the text block is
    asserting the transport rather than the refusal.
    """
    return _result_payload(result)


def test_tracing_records_a_typed_target_as_the_callers_mapping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tracer sits outside the body that calls ``as_mapping``.

    FastMCP therefore hands it a validated model. The trace must keep the
    caller's object shape for downstream target resolution, while the actual
    tool and retry path continue receiving the untouched model.
    """
    from okg.substrate.mcp import tracing as tracing_module

    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    traced_requests: list[dict] = []
    logged_args: list[dict] = []
    received: list[object] = []
    monkeypatch.setattr(
        tracing_module,
        "trace_tool_call",
        lambda *_args, **kwargs: traced_requests.append(kwargs["request"]),
    )
    monkeypatch.setattr(
        tracing_module,
        "log_call_to_log",
        lambda *_args, **kwargs: logged_args.append(kwargs["args"]),
    )

    def raw_inspect(target: object) -> dict:
        received.append(target)
        return {"kind": "node"}

    wrapped = tracing_module.make_tracer(
        object(),  # type: ignore[arg-type]
        deployment_name="default",
    )("inspect")(raw_inspect)
    target = TNode(kind="node", node_id="docs:intro")

    assert wrapped(target=target) == {"kind": "node"}
    assert received == [target]
    assert received[0] is target
    expected = {"kind": "node", "node_id": "docs:intro"}
    assert traced_requests == [
        {"args": (), "kwargs": {"target": expected}},
    ]
    assert logged_args == [{"target": expected}]


def test_tracing_still_skips_a_typed_surface_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Surface introspection stays independent of the deployment database."""
    from okg.substrate.mcp import tracing as tracing_module

    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "1")
    traced_requests: list[dict] = []
    logged_args: list[dict] = []
    received: list[object] = []
    monkeypatch.setattr(
        tracing_module,
        "trace_tool_call",
        lambda *_args, **kwargs: traced_requests.append(kwargs["request"]),
    )
    monkeypatch.setattr(
        tracing_module,
        "log_call_to_log",
        lambda *_args, **kwargs: logged_args.append(kwargs["args"]),
    )

    def raw_inspect(target: object) -> dict:
        received.append(target)
        return {"kind": "surface"}

    wrapped = tracing_module.make_tracer(
        object(),  # type: ignore[arg-type]
        deployment_name="default",
    )("inspect")(raw_inspect)
    target = TSurface(kind="surface")

    assert wrapped(target=target) == {"kind": "surface"}
    assert received == [target]
    assert received[0] is target
    assert traced_requests == []
    assert logged_args == []


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    # Every capability available (#3151 slice 1), so the published `search`
    # enums equal what the validator accepts. A bare server publishes fewer
    # methods; that profile is pinned in test_tool_surface_budget.py.
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    # This file pins the typed grammar of EVERY declared argument as the
    # models publish it. The lean read surface (#3666, M10) then hides most
    # of those arguments from `tools/list` while still accepting them; its
    # published copy is pinned in test_lean_read_surface.py.
    monkeypatch.setattr(
        "okg.substrate.mcp.lean_surface.publish_lean_read_surface", lambda _mcp: 0,
    )


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch):
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


@pytest.fixture
def write_server(monkeypatch: pytest.MonkeyPatch):
    """A write/admin launch: every `inspect` kind is reachable here."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.server import build_server
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = build_server(dsn=PROBE_DSN, allow_writes=True)
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()
        write_pool = getattr(mcp, "_okg_write_pool", None)
        if write_pool is not None:
            write_pool.close()


async def _schemas(server) -> dict[str, dict]:
    return {tool.name: tool.inputSchema for tool in await server.list_tools()}


def _resolve(schema: dict, node: dict) -> dict:
    """Follow one `$ref` into the schema's own `$defs`."""
    ref = node.get("$ref")
    if not ref:
        return node
    return schema["$defs"][ref.rsplit("/", 1)[-1]]


def _branches(schema: dict, node: dict) -> list[dict]:
    """A property's alternatives: `anyOf`/`oneOf` members, or itself."""
    members = node.get("anyOf") or node.get("oneOf")
    if not members:
        return [_resolve(schema, node)]
    return [_resolve(schema, member) for member in members]


# ── the listed schema shows the grammar ───────────────────────────────

async def test_no_named_operator_input_is_an_untyped_object(server) -> None:
    """The defect this change exists to remove.

    A bare `dict[str, Any]` renders as `{"additionalProperties": true,
    "type": "object"}` — a schema that admits anything and teaches nothing.
    """
    schemas = await _schemas(server)
    untyped = []
    for tool, params in TYPED_INPUTS.items():
        properties = schemas[tool]["properties"]
        for param in params:
            node = properties[param]
            if node.get("type") == "object" and node.get(
                "additionalProperties"
            ) is True:
                untyped.append(f"{tool}.{param}")
    assert not untyped, (
        f"these operator inputs are still untyped objects: {untyped}. The "
        "listed schema is what an agent reads before its first call."
    )


async def test_inspect_target_is_discriminated_on_kind(server) -> None:
    schemas = await _schemas(server)
    schema = schemas["inspect"]
    target = _resolve(schema, schema["properties"]["target"])
    assert target["discriminator"]["propertyName"] == "kind"
    assert target["oneOf"], "a discriminated union needs its member list"


def _listed_kinds(schema: dict, *, fold_aliases: bool = False) -> set[str]:
    """The kinds the published union lists.

    `fold_aliases` maps `attribute` and `attribute_provenance` onto `attr`.
    They are two extra SPELLINGS of one kind, which `INSPECT_TARGET_KINDS`
    lists once, so comparing a raw schema reading against a kind set counts
    them as surplus that is not there.
    """
    from okg.substrate.mcp.operator_models import INSPECT_KIND_ALIASES

    target = _resolve(schema, schema["properties"]["target"])
    listed: set[str] = set()
    for member in _branches(schema, target):
        kind = member["properties"]["kind"]
        if "const" in kind:
            listed.add(kind["const"])
        else:
            listed.update(kind["enum"])
    if fold_aliases:
        listed = {INSPECT_KIND_ALIASES.get(k, k) for k in listed}
    return listed


async def test_inspect_target_covers_every_kind_the_READ_surface_routes(
    server,
) -> None:
    """Non-vacuity partner for the discriminator test.

    A union that declared a discriminator but listed three kinds would pass
    the test above while refusing calls that used to work.

    The expected set became surface-dependent in PACT
    `effective-surface-declaration` (issue #2012). This fixture is a
    read-only, unnarrowed launch, which publishes the read kinds only: it has
    no `record` tool, so the two record-shape kinds would document a call it
    cannot make, and no expansion ceiling, so `surface` has no door to open.
    The write surface is checked below.
    """
    listed = _listed_kinds(
        (await _schemas(server))["inspect"], fold_aliases=True,
    )
    # Plus `surface`, served read-only on every launch (#3666, M10).
    expected = set(READ_INSPECT_KINDS) | {"surface"}
    assert listed == expected, (
        f"read surface kind mismatch: missing "
        f"{sorted(expected - listed)}, unexpected "
        f"{sorted(listed - expected)}"
    )


async def test_inspect_target_covers_every_kind_on_a_write_surface(
    write_server,
) -> None:
    """The union still carries all seventeen where they are all reachable.

    This is the other half of the non-vacuity pair: without it, a narrowing
    that dropped a kind everywhere would pass the read test above.
    """
    from okg.substrate.mcp.effective_surface import IMAGE_INSPECT_KINDS

    listed = _listed_kinds(
        (await _schemas(write_server))["inspect"], fold_aliases=True,
    )
    # `image` follows a declared image source, which this server has not
    # (#3046); test_an_image_source_publishes_the_image_kind covers it.
    reachable = set(INSPECT_TARGET_KINDS) - IMAGE_INSPECT_KINDS
    assert reachable <= listed, (
        f"inspect target kinds missing from the write union: "
        f"{sorted(reachable - listed)}"
    )
    assert "image" not in listed


@pytest.mark.parametrize("tool,param", [
    ("filter", "predicate"), ("expand", "filter"),
])
async def test_the_predicate_operator_set_is_closed(
    server, tool: str, param: str,
) -> None:
    schemas = await _schemas(server)
    schema = schemas[tool]
    node = _branches(schema, schema["properties"][param])[0]
    assert node["properties"]["op"]["enum"] == list(PREDICATE_OPS), (
        f"{tool}.{param} no longer publishes the closed v1 operator set"
    )


async def test_search_where_does_not_publish_the_text_operator(server) -> None:
    """#3666 step M7: search's `where` refuses a text condition, so its
    published operator list leaves `matches` out; the set tools keep it."""
    schemas = await _schemas(server)
    assert '"matches"' not in json.dumps(schemas["search"])
    assert '"matches"' in json.dumps(schemas["filter"])


@pytest.mark.parametrize("tool", ["filter", "map", "aggregate"])
async def test_the_set_scope_kind_is_enumerated(server, tool: str) -> None:
    schemas = await _schemas(server)
    schema = schemas[tool]
    kinds: set[str] = set()
    for member in _branches(schema, schema["properties"]["input"]):
        kind = member.get("properties", {}).get("kind")
        if isinstance(kind, dict):
            kinds.update(kind.get("enum") or [kind.get("const")])
    assert {"nodes", "edges"} <= kinds, (
        f"{tool}.input no longer enumerates the node/edge scope kinds"
    )


async def test_map_projection_is_typed(server) -> None:
    schemas = await _schemas(server)
    branches = schemas["map"]["properties"]["projection"]["anyOf"]
    shapes = {b.get("type") for b in branches}
    assert shapes == {"array", "object"}
    listed = next(b for b in branches if b["type"] == "array")
    assert listed["items"] == {"type": "string"}
    aliased = next(b for b in branches if b["type"] == "object")
    assert aliased["additionalProperties"] == {"type": "string"}


async def test_the_published_enums_match_what_the_runtime_accepts(
    server,
) -> None:
    schemas = await _schemas(server)
    assert schemas["search"]["properties"]["method"]["enum"] == [
        "lexical", "semantic", "alias", "identifier_mentions", "attrs", "grep",
    ]
    assert schemas["expand"]["properties"]["direction"]["enum"] == ["out", "in"]
    metrics = schemas["aggregate"]["properties"]["metrics"]
    # count, and sum/min/max of a column of a kept live answer (the
    # read fixes): the schema says so instead of an unconstrained array.
    assert metrics["items"]["pattern"] == r"^(count|(sum|min|max)\(attrs\.[^().]+\))$"


async def test_every_bounded_parameter_publishes_its_maximum(server) -> None:
    schemas = await _schemas(server)
    missing = []
    for (tool, param), expected in EXPECTED_MAXIMA.items():
        if expected is None:
            continue
        node = schemas[tool]["properties"][param]
        if node.get("maximum") != expected:
            missing.append(f"{tool}.{param}={node.get('maximum')!r}")
    assert not missing, (
        f"these bounds are missing or wrong in the listed schema: {missing}"
    )


async def test_the_path_and_by_lists_are_bounded(server) -> None:
    schemas = await _schemas(server)
    assert schemas["expand"]["properties"]["path"]["maxItems"] == MAX_PATH_HOPS
    assert schemas["aggregate"]["properties"]["by"]["maxItems"] == (
        MAX_AGGREGATE_BY
    )


# ── no listed parameter is rejected by the runtime ────────────────────

async def test_search_lists_only_the_record_scope_it_serves(server) -> None:
    """An earlier `scope` was listed and the v1 runtime raised on it. The
    record scope (PACT read-model-s3-where-scope) and the children scope
    (PACT search-children-of-3363) are served, and the listing names exactly
    their keys."""
    schemas = await _schemas(server)
    scope = schemas["search"]["properties"]["scope"]
    assert set(scope["properties"]) == {"record", "records", "children_of", "edge_type"}, scope


def test_the_declared_subtype_page_size_is_the_one_the_runtime_enforces() -> None:
    """A declared bound is only useful if it is the REAL bound.

    `MAX_SUBTYPE_PAGE_SIZE` is restated in `operator_models` rather than
    imported, so nothing but this test stops the two drifting apart and the
    schema advertising a page size the runtime refuses.
    """
    from okg.substrate.mcp.subtype_summary import MAX_SECTION_PAGE_SIZE

    assert MAX_SUBTYPE_PAGE_SIZE == MAX_SECTION_PAGE_SIZE


def test_the_declared_preview_ceiling_is_the_one_the_runtime_enforces() -> None:
    """`MAX_PREVIEW_CHARS` is restated in `operator_models` rather than
    imported, and the docstring claims this test keeps the two in step. It
    did not exist until the second review noticed the claim."""
    from okg.substrate.mcp.search import MAX_PREVIEW_CHARS as RUNTIME_MAX

    assert MAX_PREVIEW_CHARS == RUNTIME_MAX


async def test_a_hidden_tool_is_refused_as_hidden_not_as_mis_argued(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The unknown-argument check stands aside for the visibility gate.

    A caller who cannot see a tool must be told that, not handed a critique
    of arguments for a tool it may not know exists.
    """
    from okg.substrate.mcp.session_tool_surface import SessionToolSurface

    checked: list[str] = []
    monkeypatch.setattr(
        server,
        "_okg_dispatch_principal_gate",
        lambda name: checked.append(name),
        raising=False,
    )
    monkeypatch.setattr(
        server,
        "_okg_session_tool_surface",
        SessionToolSurface(
            base=frozenset({"inspect"}),
            ceiling=frozenset(INSPECT_VISIBLE_CEILING),
        ),
    )
    payload = _refusal(
        await call_json(server, "query", {"sql": "SELECT 1", "nonsense": 1}),
    )
    assert payload["error"]["code"] == "mcp_tool_not_in_session_surface", (
        f"a hidden tool was refused for its arguments instead: {payload}"
    )
    assert checked == ["query"], "the direct call crossed the principal gate twice"


INSPECT_VISIBLE_CEILING = ("inspect", "query")


INSPECT_DECLARED_NOT_ENFORCED = {
    "detail": {"kind": "subtype", "subtype": "s", "detail": "full_everything"},
    "page_size": {"kind": "subtype", "subtype": "s", "page_size": "many"},
}


@pytest.mark.parametrize("field", sorted(INSPECT_DECLARED_NOT_ENFORCED))
async def test_inspect_subtype_declares_its_bounds_and_leaves_them_to_the_runtime(
    server, field: str,
) -> None:
    """Same split as `expand`, for the same reason.

    `_inspect_subtype` refuses a bad `detail` or `page_size` with
    `invalid_subtype_inspection` and `error.data.field`, telling the caller
    WHICH field it got wrong. `test_mcp_actionable_bounds_e2e.py` pins that
    envelope against a real database; this pins that the schema does not
    pre-empt it.
    """
    from mcp.server.fastmcp.exceptions import ToolError

    schema = (await _schemas(server))["inspect"]
    # Found by its `kind` const, not by a `$defs` key: #2792 inlines every
    # member, so the published schema has no `$defs` to look it up in.
    member = next(
        branch for branch in _branches(schema, schema["properties"]["target"])
        if branch["properties"]["kind"].get("const") == "subtype"
    )["properties"][field]
    assert {"enum", "maximum"} & set(member), (
        f"inspect subtype.{field} publishes no bound: {member}"
    )
    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "inspect", {"target": INSPECT_DECLARED_NOT_ENFORCED[field]},
        )
    assert "validation error" not in str(excinfo.value), (
        f"inspect subtype.{field} is now enforced by the schema, which "
        f"replaces the typed refusal that names the field: {excinfo.value}"
    )


async def test_max_per_parent_publishes_no_maximum_it_does_not_have(
    server,
) -> None:
    """A published bound must be a bound the runtime really applies.

    `validate_max_per_parent` refuses only values below 1. An `le` here
    would have refused a call the server accepts, which is worse than
    publishing nothing: the schema would be lying in the direction that
    costs a caller an answer.
    """
    node = (await _schemas(server))["search"]["properties"]["max_per_parent"]
    assert node.get("minimum") == 1
    assert "maximum" not in node


SEARCH_DECLARED_NOT_ENFORCED = {
    "max_chars": {"query": "x", "max_chars": 2},
    "max_per_parent": {"query": "x", "max_per_parent": 0},
}


@pytest.mark.parametrize("field", sorted(SEARCH_DECLARED_NOT_ENFORCED))
async def test_search_declares_its_bounds_and_leaves_them_to_the_runtime(
    server, field: str,
) -> None:
    """The same split as `expand` and `inspect`, applied consistently.

    `InvalidPreviewCharBudget` and `DiversityBoundInvalid` are typed
    substrate errors carrying the accepted range and a fix hint; a pydantic
    constraint would win the race and replace them with a generic schema
    message.

    `rank_by` is NOT in this list, and that is deliberate rather than an
    oversight: `validate_rank_by` raises a bare `ValueError`, which the byte
    bound flattens into `operator_execution_failed`, so enforcing the enum in
    the schema gives the caller strictly more than the runtime would. The
    rule is "leave it to the runtime where the runtime answers better", not
    "leave everything to the runtime".
    """
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "search", SEARCH_DECLARED_NOT_ENFORCED[field],
        )
    assert "validation error" not in str(excinfo.value), (
        f"search.{field} is now enforced by the schema, which replaces the "
        f"typed refusal that names the accepted range: {excinfo.value}"
    )


async def test_rank_by_stays_enforced_by_the_schema(server) -> None:
    """Non-vacuity partner for the rule above: it is applied, not blanket."""
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "search", {"query": "x", "rank_by": "vibes"},
        )
    assert "rank_by" in str(excinfo.value)


@pytest.mark.parametrize("arguments,expected", [
    pytest.param({"input": {}}, "node", id="aggregate-empty-input"),
    pytest.param({"input": {}}, "node", id="expand-empty-input"),
])
async def test_an_input_naming_nothing_is_refused(
    server, arguments: dict, expected: str, request: pytest.FixtureRequest,
) -> None:
    """`aggregate(input={})` must not become "every node in the graph".

    `AggregateScope.kind` defaults to nodes so the legacy short form works,
    and an empty object would therefore dump as `{"kind": "nodes"}`, which
    `is_set_input` reads as a set scope. Both union members refuse an input
    that names nothing, so which one pydantic picks cannot decide whether a
    caller gets an anchor error or the whole graph.
    """
    from mcp.server.fastmcp.exceptions import ToolError

    tool = "aggregate" if "aggregate" in request.node.callspec.id else "expand"
    full = dict(arguments)
    if tool == "aggregate":
        full.update({"by": ["subtype"], "metrics": ["count"]})
    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(tool, full)
    assert "validation error" in str(excinfo.value)


@pytest.mark.parametrize("scope", [
    pytest.param({"subtype": "pact_change"}, id="subtype-only"),
    pytest.param({"filter": {"op": "exists", "field": "attrs.x"}}, id="filter-only"),
])
async def test_aggregate_accepts_the_kind_less_scope_the_runtime_accepts(
    server, scope: dict,
) -> None:
    """`set_operators` defaults a missing kind to nodes, and the docstring
    taught the short form. Requiring `kind` here would have refused calls
    that work, which a typing change has no business doing."""
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "aggregate", {"input": scope, "by": ["subtype"], "metrics": ["count"]},
        )
    assert "validation error" not in str(excinfo.value), (
        f"aggregate refused a kind-less scope the runtime accepts: "
        f"{excinfo.value}"
    )


async def test_filter_still_requires_the_scope_kind(server) -> None:
    """Partner to the test above: `filter` routes through `scope_kind`, which
    REFUSES a missing kind, so its model must keep requiring one."""
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "filter", {"input": {"subtype": "x"}, "predicate": {}},
        )
    assert "kind" in str(excinfo.value)


def test_compaction_never_deletes_a_property_named_like_a_keyword() -> None:
    """A schema key inside `properties` is a NAME, not a keyword.

    Stripping a field genuinely called `title` while its entry in `required`
    survived would publish a schema that demands a property it does not
    define — one no instance can satisfy. Counterexample pinned because the
    stripping is by key name at every depth.
    """
    from okg.substrate.mcp.operator_models import compact_schema

    source = {
        "type": "object",
        "title": "Outer",
        "properties": {
            "title": {"type": "string", "title": "Title"},
            "description": {"type": "integer"},
            "default": {"type": "boolean"},
        },
        "required": ["title", "description", "default"],
    }
    out = compact_schema(source)
    assert "title" not in out, "the outer title is a keyword and should go"
    assert set(out["properties"]) == {"title", "description", "default"}
    assert out["required"] == ["title", "description", "default"]
    assert "title" not in out["properties"]["title"], (
        "the inner title IS a keyword on that property and should still go"
    )


EXPAND_DECLARED_NOT_ENFORCED = {
    "direction": {"input": "n", "direction": "sideways"},
    "depth": {"input": "n", "depth": 9},
    "path": {"input": "n", "path": []},
    "limit": {"input": "n", "path": ["a:out"], "limit": 0},
}


@pytest.mark.parametrize("param", sorted(EXPAND_DECLARED_NOT_ENFORCED))
async def test_expand_declares_its_bounds_and_leaves_them_to_the_runtime(
    server, param: str,
) -> None:
    """`expand` publishes each bound but does NOT enforce it in the schema.

    `path_diagnostics` already refuses every one of these with a typed
    envelope naming the hop index, the requested shape and a fix hint — a
    far better answer than "Input should be less than or equal to 3". A
    pydantic constraint would win the race and throw that away, so the bound
    is declared to the caller and enforced where the better message lives.
    `test_expand_path_diagnostics.py` pins the envelopes against a real
    database; this test pins the split, so nobody "tightens" the annotation
    later and silently deletes the diagnostics.
    """
    from mcp.server.fastmcp.exceptions import ToolError

    listed = (await _schemas(server))["expand"]["properties"][param]
    assert {"enum", "minimum", "maximum", "minItems", "maxItems"} & set(listed), (
        f"expand.{param} publishes no bound at all: {listed}"
    )
    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(
            "expand", EXPAND_DECLARED_NOT_ENFORCED[param],
        )
    assert "validation error" not in str(excinfo.value), (
        f"expand.{param} is now enforced by the schema, which replaces the "
        f"typed path-diagnostics envelope: {excinfo.value}"
    )


def test_the_v1_scope_rejection_path_is_gone() -> None:
    """The parameter and its refusal are removed together.

    Dropping only the listing would leave dead code that the next reader
    would take as evidence the parameter still exists somewhere.
    """
    source = pathlib.Path(
        "src/okg/substrate/mcp/server.py"
    ).read_text(encoding="utf-8")
    assert "scope is not supported" not in source


@pytest.mark.parametrize("tool,arguments,key", [
    pytest.param("search", {"query": "x", "scop": {}}, "scop", id="a-misspelt-scope"),
    pytest.param("expand", {"input": "n", "edg_type": "a"}, "edg_type", id="a-typo"),
    pytest.param(
        "filter",
        {"input": {"kind": "nodes"}, "predicate": {}, "subtype": "x"},
        "subtype",
        id="a-key-that-belongs-on-the-input",
    ),
])
async def test_an_undeclared_argument_is_refused_and_named(
    server, tool: str, arguments: dict, key: str,
) -> None:
    """An argument no operator declares is refused, not dropped.

    This is the review finding that mattered most. The SDK builds each
    tool's argument model from its signature and silently DISCARDS any
    keyword the model does not declare, so removing `search.scope` turned a
    loud "scope is not supported in v1" into a perfectly normal, completely
    unscoped page with nothing saying the argument had been ignored. The
    first version of this test pinned that silent ignore as if it were the
    goal.
    """
    payload = _refusal(await call_json(server, tool, arguments))
    assert payload["error"]["code"] == "unknown_operator_argument"
    assert payload["error"]["data"]["unknown_arguments"] == [key]
    assert key in payload["error"]["fix_hint"] or key in payload["error"]["message"]


async def test_a_declared_argument_is_not_refused_as_unknown(server) -> None:
    """Non-vacuity partner: a check that refused everything would pass above."""
    payload = _refusal(
        await call_json(server, "search", {"query": "x", "method": "lexical"}),
    )
    assert payload["error"]["code"] != "unknown_operator_argument"


# ── malformed input is refused, and named ─────────────────────────────

@pytest.mark.parametrize("tool,arguments,expected", [
    pytest.param(
        "filter",
        {"input": {"kind": "nodes"}, "predicate": {"op": "frobnicate", "field": "a"}},
        "predicate.op",
        id="predicate-op-outside-the-closed-set",
    ),
    # loose-match-3666 (#3790): an operator word with no one meaning here
    # (`regex`) is refused by the grammar, which names the argument and
    # says what to write instead.
    pytest.param(
        "filter",
        {"input": {"kind": "nodes"}, "predicate": {"op": "regex", "field": "a"}},
        "predicate",
        id="predicate-op-a-known-word-with-no-one-meaning",
    ),
    # An unknown STRING kind ("nope") now gets the guard's named refusal,
    # agent_surface_inspect_kind_unserved (Jason 179a; pinned in
    # test_effective_surface.py). A non-string kind is still malformed input.
    pytest.param(
        "inspect", {"target": {"kind": 5}}, "target",
        id="non-string-inspect-kind",
    ),
    pytest.param(
        "inspect",
        {"target": {"kind": "node", "node_id": "n", "hydrat": True}},
        "hydrat",
        id="misspelled-target-key-is-not-silently-dropped",
    ),
    pytest.param(
        "filter",
        {"input": {"kind": "nodes"}, "predicate": {"field": "subtype"}},
        "predicate",
        id="predicate-with-no-op",
    ),
    pytest.param(
        "expand", {"input": {"node_id": "n", "nodeid": "n"}}, "nodeid",
        id="misspelled-anchor-key-is-not-silently-dropped",
    ),
    pytest.param(
        "aggregate",
        # A group key the set has (#3666: `by` is checked first, by name).
        {"input": {"kind": "nodes"}, "by": ["attrs.a"], "metrics": ["sum"]},
        "metrics",
        id="metric-v1-does-not-compute",
    ),
    pytest.param(
        "filter",
        # `attrs.a`: since #3666 step 2c a field no record has is refused
        # by name first.
        {"input": {"kind": "nodes"}, "predicate": {"op": "eq", "field": "attrs.a"},
         "limit": 100_000},
        "limit",
        id="limit-past-its-maximum",
    ),
    pytest.param(
        "map", {"input": {"kind": "nodes"}, "projection": 7}, "projection",
        id="projection-that-is-neither-list-nor-mapping",
    ),
    pytest.param(
        "filter",
        {"input": {"kind": "everything"}, "predicate": {"op": "eq", "field": "a"}},
        "input",
        id="scope-kind-outside-the-enum",
    ),
])
async def test_a_malformed_input_is_refused_and_the_field_is_named(
    server, tool: str, arguments: dict, expected: str,
) -> None:
    """The failure case. Validation happens BEFORE the operator body, so the
    refusal names the offending field instead of surfacing whatever the
    implementation happened to raise part-way through."""
    payload = _refusal(await call_json(server, tool, arguments))
    assert payload["error"]["code"] == "invalid_operator_arguments"
    assert any(
        expected in field for field in payload["error"]["data"]["invalid_fields"]
    ), (
        f"{tool} refused malformed input without naming {expected!r}: "
        f"{payload}"
    )


async def test_input_validation_refusal_is_bounded_and_does_not_echo_values(
    server,
) -> None:
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES

    secret = "raw-secret-value-that-must-not-be-reflected"
    result = await call_json(server,
        "inspect",
        {
            "target": {"kind": "node", "node_id": "n", "hydrat": secret},
            "max_bytes": MIN_RESPONSE_BYTES,
        },
    )
    payload = _refusal(result)

    assert payload["error"]["code"] == "invalid_operator_arguments"
    assert payload["error"]["data"]["invalid_fields"] == ["target.node.hydrat"]
    assert secret not in result.model_dump_json(by_alias=True, exclude_none=True)
    assert len(result.model_dump_json(by_alias=True, exclude_none=True).encode()) <= (
        MIN_RESPONSE_BYTES
    )


async def test_output_validation_error_is_not_classified_as_bad_input(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the pre-dispatch argument model may emit the input error code."""
    from okg.substrate.mcp import operators as operators_mod

    def invalid_output(value: int) -> dict[str, int]:
        return {"count": {"raw": "must-stay-internal"}}  # type: ignore[dict-item]

    server.tool(name="probe_invalid_output")(invalid_output)
    monkeypatch.setattr(
        operators_mod,
        "OPERATOR_TOOLS",
        tuple(operators_mod.OPERATOR_TOOLS) + ("probe_invalid_output",),
    )

    payload = _refusal(
        await call_json(server, "probe_invalid_output", {"value": 1}),
    )
    assert payload["error"]["code"] == "operator_execution_failed"
    assert "must-stay-internal" not in json.dumps(payload)


@pytest.mark.parametrize("tool,arguments", [
    ("inspect", {"target": {"kind": "graph"}}),
    ("inspect", {"target": {"kind": "node", "node_id": "n", "hydrate": True}}),
    ("inspect", {"target": {"kind": "subtype", "subtype": "s",
                            "detail": "sample_nodes"}}),
    ("inspect", {"target": {"kind": "attribute_provenance", "target_id": "n"}}),
    ("expand", {"input": "n", "edge_type": "contains", "direction": "in"}),
    ("expand", {"input": {"node_id": "n"}, "path": ["contains:out"]}),
    ("filter", {"input": {"kind": "nodes", "subtype": "source_file"},
                "predicate": {"op": "suffix", "field": "attrs.path",
                              "value": "a.py"}}),
    ("filter", {"input": {"kind": "edges"},
                "predicate": {"op": "and", "clauses": [
                    {"op": "exists", "field": "attrs.x"},
                    {"op": "not", "clause": {"op": "eq", "field": "src",
                                             "value": "n"}}]}}),
    ("map", {"input": {"kind": "nodes"}, "projection": {"id": "node_id"}}),
    ("filter", {"input": {"kind": "nodes"}, "predicate": {}}),
    ("filter", {"input": {"kind": "nodes", "filter": {}},
                "predicate": {"op": "exists", "field": "attrs.x"}}),
    ("aggregate", {"input": {"kind": "nodes", "subtype": "pact_change"},
                   "by": ["attrs.status"], "metrics": ["count"]}),
])
async def test_the_shapes_the_operators_document_still_validate(
    server, tool: str, arguments: dict,
) -> None:
    """Non-vacuity partner for the refusal sweep.

    A model that refused everything would pass every test above. These are
    the spellings the tool docstrings and the runbooks tell an agent to
    write; each must clear validation. The call then fails to REACH a
    database (the probe pool is never opened), which is exactly the boundary
    this test is about: argument validation, not execution.
    """
    from mcp.server.fastmcp.exceptions import ToolError

    with pytest.raises(ToolError) as excinfo:
        await server._tool_manager.call_tool(tool, arguments)
    assert "validation error" not in str(excinfo.value), (
        f"{tool} rejected a documented shape at validation: {excinfo.value}"
    )


async def test_a_parameter_that_shadows_a_basemodel_attribute_is_declared(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FastMCP renames `schema` (and `copy`, `json`, `dict`, `validate`).

    Those names collide with `BaseModel` attributes, so the SDK stores them
    as `field_<name>` and keeps the wire name as the field's ALIAS. Reading
    the field names alone would make a perfectly good argument look
    undeclared, and the caller would be refused for naming a parameter the
    tool really does have. No operator uses such a name today; this pins the
    behavior before one does.
    """
    from okg.substrate.mcp import operators as operators_mod

    def shadowing(schema: str, copy: int = 1) -> PageResult:
        return {"schema": schema, "copy": copy}

    # Registered with structured output, like every real operator: once the
    # probe is named in OPERATOR_TOOLS the structured conversion applies to
    # it too, and a probe that opted out would trip the guard that says the
    # SDK's return shape changed.
    server.tool(name="probe_shadowing", structured_output=True)(shadowing)
    monkeypatch.setattr(
        operators_mod,
        "OPERATOR_TOOLS",
        tuple(operators_mod.OPERATOR_TOOLS) + ("probe_shadowing",),
    )
    registered = server._tool_manager._tools["probe_shadowing"]
    assert "field_schema" in registered.fn_metadata.arg_model.model_fields, (
        "the SDK stopped renaming shadowing parameters; this test no longer "
        "covers anything"
    )

    result = await call_json(server, "probe_shadowing", {"schema": "s"})
    structured = getattr(result, "structuredContent", None)
    rendered = json.dumps(
        result if structured is None else structured,
        default=lambda value: getattr(value, "text", str(value)),
    )
    assert "unknown_operator_argument" not in rendered, (
        "a parameter the tool really declares was refused as undeclared "
        f"because the SDK renamed it: {rendered}"
    )
    assert "schema" in rendered and "s" in rendered


async def test_a_denied_principal_is_refused_as_denied_not_as_mis_argued(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatch principal gate decides before the argument check.

    The gate is what writes the Nomos refusal row through
    `_persist_principal_refusal`. If the unknown-argument check answered
    first, a denied principal that also misspelled an argument would be told
    "unknown argument", the denial would never be evaluated, and no audit
    row would be written — the denial would simply not be on the record.

    The gate is stubbed rather than driven through Postgres because the
    defect is the ORDERING, not the policy: `test_principal_mapping.py`
    already proves the real gate resolves a subject, denies it and persists
    the row. What no test covered was whether the gate gets to run at all,
    so this asserts the side effect fired, that its refusal is what the
    caller receives, and that the argument check did not pre-empt it.
    """
    from okg.substrate.errors import SubstrateError

    persisted: list[str] = []

    class _Denied(SubstrateError):
        code = "nomos_identity_denied"

    def gate(name: str) -> None:
        # Stands in for the real gate, which persists before it raises.
        persisted.append(name)
        raise _Denied(
            f"principal is denied {name!r}",
            data={"resolved_subject": "subject-b", "effect": "deny"},
        )

    # `raising=False`: a probe server without a principal mapping never sets
    # the attribute, and the mixin reads it with `getattr(..., None)`.
    monkeypatch.setattr(server, "_okg_dispatch_principal_gate", gate, raising=False)

    payload = _refusal(
        await call_json(server, "search", {"query": "x", "scop": {}}),
    )
    assert payload["error"]["code"] == "nomos_identity_denied", (
        "the argument check answered before the principal gate, so the "
        f"denial was never evaluated and no audit row was written: {payload}"
    )
    assert persisted == ["search"], (
        "the gate never ran, so nothing persisted the refusal"
    )
    assert payload["error"]["data"]["effect"] == "deny"


async def test_the_argument_check_still_runs_for_an_allowed_principal(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-vacuity partner: moving the gate first must not disable the
    argument check for everyone who passes the gate."""
    allowed: list[str] = []
    monkeypatch.setattr(
        server,
        "_okg_dispatch_principal_gate",
        lambda name: allowed.append(name),
        raising=False,
    )

    payload = _refusal(
        await call_json(server, "search", {"query": "x", "scop": {}}),
    )
    assert allowed == ["search"]
    assert payload["error"]["code"] == "unknown_operator_argument"


async def test_a_valid_direct_call_checks_the_principal_once(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct server boundary authorizes once before normal dispatch."""
    checked: list[str] = []
    monkeypatch.setattr(
        server,
        "_okg_dispatch_principal_gate",
        lambda name: checked.append(name),
        raising=False,
    )

    result = await call_json(server, "inspect", {"target": {"kind": "surface"}})

    assert result
    assert checked == ["inspect"], "the direct call crossed the principal gate twice"


async def test_unknown_argument_refusal_delivers_a_pending_surface_notification(
    server, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-dispatch validation stays inside the projected call error path."""
    from okg.substrate.mcp.session_tool_surface import SessionToolSurface

    class _Session:
        def __init__(self) -> None:
            self.frames = 0

        async def send_tool_list_changed(self) -> None:
            self.frames += 1

    session = _Session()
    surface = SessionToolSurface(
        base=frozenset({"search"}),
        ceiling=frozenset({"inspect", "search"}),
    )
    surface.expand(session)
    monkeypatch.setattr(server, "_okg_session_tool_surface", surface)
    monkeypatch.setattr(server, "okg_current_session", lambda: session)

    payload = _refusal(
        await call_json(server, "search", {"query": "x", "scop": {}}),
    )

    assert payload["error"]["code"] == "unknown_operator_argument"
    assert session.frames == 1
    assert surface.take_pending_notification(session) is False
