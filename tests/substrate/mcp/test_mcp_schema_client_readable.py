"""Published tool schemas a weaker tool-calling client can read (issue #2792).

PACT `mcp-schema-inline-types`.

An Ollama-served model (qwen3.8:27B, the example-kb public chat model) converts
each tool argument by the top-level `type` its schema declares. An argument
spelled only as a `$ref` pointer, or as a `oneOf`/`anyOf` union with no
`type` beside it, came back as a string holding JSON, and the server refused
all 16 such calls in a three-question loop. These tests pin the published
fix and, separately, that the server accepts and refuses exactly what it did
before: only the PUBLISHED schema changes.

Hermetic: `OKG_MCP_SURFACE_PROBE=1` builds the pools unopened, so nothing
here needs Postgres. A call that clears validation then fails to reach the
database, which is the boundary these tests are about.
"""
from __future__ import annotations

import json
from typing import Annotated, Any, Optional, Union

import jsonschema
import pytest
from pydantic import BaseModel, Field
from typing_extensions import TypeAliasType

# The new helpers are imported inside the tests that exercise them, so the
# surface and behavior tests below also run -- and fail for the right
# reason -- against a `dev` checkout that predates them.
from okg.substrate.mcp.operators import OPERATOR_TOOLS
from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES
from tests.substrate.mcp._result_payload import payload as _result_payload
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


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _hermetic_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)


def _build(monkeypatch: pytest.MonkeyPatch, **kwargs: Any) -> Any:
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    return server_mod.build_server(dsn=PROBE_DSN, **kwargs)


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    write_pool = getattr(mcp, "_okg_write_pool", None)
    if write_pool is not None:
        write_pool.close()


#: The launches whose `tools/list` a client can receive. `reader` is the
#: example-kb public chat's profile; write/admin serves every tool.
SURFACES = {
    "full read": {},
    "reader": {"tool_profile": "reader"},
    "discovery": {"tool_profile": "discovery"},
    "write/admin": {"allow_writes": True},
}


@pytest.fixture(params=sorted(SURFACES), ids=sorted(SURFACES))
async def listed(request, monkeypatch: pytest.MonkeyPatch) -> dict[str, dict]:
    mcp = _build(monkeypatch, **SURFACES[request.param])
    try:
        return {tool.name: tool.inputSchema for tool in await mcp.list_tools()}
    finally:
        _close(mcp)


# ── what a client reads ───────────────────────────────────────────────


def _walk(node: Any, path: str):
    """Yield (path, schema node) for every schema node, skipping name maps."""
    if isinstance(node, list):
        for index, item in enumerate(node):
            yield from _walk(item, f"{path}[{index}]")
        return
    if not isinstance(node, dict):
        return
    yield path, node
    for key, value in node.items():
        if key in {"properties", "$defs", "patternProperties"} and isinstance(
            value, dict,
        ):
            for name, body in value.items():
                yield from _walk(body, f"{path}.{key}.{name}")
        else:
            yield from _walk(value, f"{path}.{key}")


def _resolve(root: dict, node: dict) -> dict:
    ref = node.get("$ref")
    if isinstance(ref, str):
        return root.get("$defs", {}).get(ref.rsplit("/", 1)[-1], {})
    return node


def _declared_types(node: dict) -> set[str]:
    kind = node.get("type")
    if isinstance(kind, str):
        return {kind}
    if isinstance(kind, list):
        return set(kind)
    return set()


def _accepts_object(root: dict, node: dict, seen: frozenset = frozenset()) -> bool:
    """Whether `node` admits an object, following pointers and union members.

    Judged from what the schema MEANS, not from how it is spelled, so a bare
    `$ref` to an object definition counts as accepting an object: that is
    precisely the spelling the Ollama parser could not see through.
    """
    ref = node.get("$ref")
    if isinstance(ref, str):
        if ref in seen:
            return True
        return _accepts_object(root, _resolve(root, node), seen | {ref})
    if "object" in _declared_types(node) or "properties" in node:
        return True
    return any(
        isinstance(branch, dict) and _accepts_object(root, branch, seen)
        for keyword in ("anyOf", "oneOf", "allOf")
        for branch in node.get(keyword) or ()
    )


async def test_no_published_input_schema_carries_a_pointer(listed) -> None:
    """The failure this change exists to remove, half one: no `$ref`.

    Fails on `dev` before #2792: `filter.input`, `filter.predicate`,
    `expand.filter`, `expand.live` and every `inspect.target` member were
    pointers into `$defs`.
    """
    found = [
        f"{tool}{path}"
        for tool, schema in listed.items()
        for path, node in _walk(schema, "")
        if "$ref" in node or "$defs" in node
    ]
    assert not found, (
        "these published input schemas still use a $ref or $defs, which an "
        f"Ollama-served model reads as a string argument: {found}"
    )


async def test_every_argument_that_accepts_an_object_says_so(listed) -> None:
    """Half two: an object-accepting node declares `type` at its own top.

    Checked on every node of every tool, not just the argument roots: a
    union member or a nested scope is converted by the same parser rule.
    An unconstrained `{}` (a predicate's `value`) accepts anything and has
    no type to state without narrowing it, so it is not counted.

    Fails on `dev`: `inspect.target` was a `oneOf` with no `type`, and
    `filter.input` a bare pointer.
    """
    untyped = []
    for tool, schema in listed.items():
        for path, node in _walk(schema, ""):
            if not node or path.endswith(".discriminator"):
                continue
            if _accepts_object(schema, node) and "object" not in _declared_types(
                node,
            ):
                untyped.append(f"{tool}{path or '.<root>'}")
    assert not untyped, (
        "these schema nodes accept an object but declare no top-level "
        f"\"type\" containing \"object\": {untyped}"
    )


async def test_every_union_states_its_type(listed) -> None:
    """`source_family` (string | array) and `generation_id` (integer | string)
    reach the same parser. A union whose members all declare a type states
    their union at the top, so a list is not delivered as a JSON string."""
    untyped = [
        f"{tool}{path}"
        for tool, schema in listed.items()
        for path, node in _walk(schema, "")
        if ("anyOf" in node or "oneOf" in node) and "type" not in node
    ]
    assert not untyped, f"unions without a top-level type: {untyped}"


async def test_max_bytes_publishes_the_minimum_the_runtime_enforces(
    listed,
) -> None:
    """`inspect` refused `max_bytes=4000` from a model that could not know
    the floor. The floor is now published; no maximum is, because the
    ceiling is per deployment (#2141 decision 3A)."""
    served = [name for name in OPERATOR_TOOLS if name in listed]
    assert served
    for name in served:
        if name == "search":
            # read-passage-rows-3666 (D3): accepted, not published; a search
            # page shortens its text to fit instead.
            assert "max_bytes" not in listed[name]["properties"]
            continue
        if name == "run":
            # `run` shares its budget among its steps and its own refusal
            # names the value that works; it does not publish the floor.
            assert listed[name]["properties"]["max_bytes"] == {"type": "integer"}
            continue
        assert listed[name]["properties"]["max_bytes"] == {
            "minimum": MIN_RESPONSE_BYTES, "type": "integer",
        }, name
        assert "max_bytes" not in listed[name].get("required", []), name


async def test_the_predicate_cut_is_named_and_typed(monkeypatch) -> None:
    """The self-referencing predicate is cut at `INLINE_RECURSION_DEPTH`,
    visibly: the nested node is an object that says what it is."""
    from okg.substrate.mcp.operator_models import INLINE_RECURSION_DEPTH

    assert INLINE_RECURSION_DEPTH == 1
    # The lean read surface (#3666, M10) publishes `where` as one described
    # object and hides `predicate`; the cut is read where the inliner makes
    # it, before that pass.
    monkeypatch.setattr(
        "okg.substrate.mcp.lean_surface.publish_lean_read_surface", lambda _mcp: 0,
    )
    mcp = _build(monkeypatch, tool_profile="reader")
    try:
        schemas = {t.name: t.inputSchema for t in await mcp.list_tools()}
    finally:
        _close(mcp)
    predicate = schemas["filter"]["properties"]["predicate"]
    assert predicate["properties"]["op"]["enum"][:3] == ["and", "or", "not"]
    cut = {
        "type": "object",
        "description": "Predicate: same shape as the enclosing one",
    }
    assert predicate["properties"]["clause"] == cut
    assert predicate["properties"]["clauses"]["items"] == cut
    assert predicate["properties"]["clauses"]["minItems"] == 1


# ── the transform refuses rather than guessing ────────────────────────


@pytest.mark.parametrize("schema,match", [
    pytest.param(
        {"properties": {"a": {"$ref": "https://example.test/x"}}},
        "non-local", id="non-local-pointer",
    ),
    pytest.param(
        {"properties": {"a": {"$ref": "#/$defs/Missing"}}, "$defs": {}},
        "names no definition", id="dangling-pointer",
    ),
    pytest.param(
        {
            "properties": {"a": {"$ref": "#/$defs/A", "type": "string"}},
            "$defs": {"A": {"type": "object"}},
        },
        "overwrite", id="sibling-contradicts-definition",
    ),
])
def test_inlining_refuses_a_pointer_it_cannot_publish_faithfully(
    schema: dict, match: str,
) -> None:
    from okg.substrate.mcp.operator_models import (
        ToolSchemaUnpublishable,
        inline_schema_refs,
    )

    with pytest.raises(ToolSchemaUnpublishable, match=match):
        inline_schema_refs(schema)


def test_a_union_with_an_untyped_member_is_left_for_the_test_to_name() -> None:
    from okg.substrate.mcp.operator_models import type_schema_unions

    node = {"anyOf": [{"type": "string"}, {}]}
    assert type_schema_unions(node) == node


def test_publishing_refuses_when_the_sdk_internals_move() -> None:
    from okg.substrate.mcp.operator_models import (
        ToolSchemaUnpublishable,
        publish_client_readable_schemas,
    )

    class NoManager:
        pass

    with pytest.raises(ToolSchemaUnpublishable, match="_tool_manager"):
        publish_client_readable_schemas(NoManager())


# ── what the server accepts and refuses has not moved ─────────────────
#
# Each case is sent through `call_tool`, the path a client's request takes.
# The outcome is the refusal code when validation refused it, else
# "passes": the call then fails to reach the probe database, which is past
# every check this change could have touched.
#
# The expected column was checked on `dev` at 8f0b81c136, before this
# change, and the test also rebuilds the server with the new publishing step
# switched off and demands the same column, so "unchanged" is shown both
# against history and side by side.
#
# ONE deliberate exception, from #2793 (part 2): a condition-style
# `search.filter`. On `dev` it cleared argument validation and then raised
# inside `search()` -- an opaque `operator_execution_failed` on a real
# server. It is now refused at validation, by name. `DEV_OUTCOME` records the
# `dev` column for exactly those rows, and
# `test_every_newly_refused_call_was_already_refused_by_search` shows that
# `dev`'s own `search()` refuses each of them, so no call that worked
# before is refused now.

VALIDATION_CODES = frozenset({
    "invalid_operator_arguments",
    "unknown_operator_argument",
    "invalid_response_byte_budget",
    "agent_surface_inspect_kind_unserved",
})

PASSES = "passes"
BAD = "invalid_operator_arguments"
UNKNOWN = "unknown_operator_argument"
BUDGET = "invalid_response_byte_budget"
UNSERVED = "agent_surface_inspect_kind_unserved"

_EQ = {"op": "eq", "field": "subtype", "value": "x"}
_EDGE_EQ = {"op": "eq", "field": "edge_type", "value": "x"}

CASES: list[tuple[str, str, dict, str]] = [
    # inspect
    ("inspect", "graph", {"target": {"kind": "graph"}}, PASSES),
    ("inspect", "node", {"target": {"kind": "node", "node_id": "n"}}, PASSES),
    ("inspect", "subtype-page",
     {"target": {"kind": "subtype", "subtype": "s", "page_size": 32}}, PASSES),
    ("inspect", "max-bytes-floor",
     {"target": {"kind": "graph"}, "max_bytes": 4096}, PASSES),
    ("inspect", "select", {"target": {"kind": "graph"}, "select": ["counts"]},
     PASSES),
    ("inspect", "target-as-json-string", {"target": '{"kind": "graph"}'}, BAD),
    # Issue #3153: under the floor is raised to it (and said), not refused.
    ("inspect", "max-bytes-under-floor",
     {"target": {"kind": "graph"}, "max_bytes": 4000}, PASSES),
    ("inspect", "max-bytes-as-string",
     {"target": {"kind": "graph"}, "max_bytes": "8192"}, BAD),
    # A kind in no inspect union gets the guard's named refusal, the same
    # code a withheld kind gets (Jason 179a); it was `union_tag_invalid`.
    # Only a STRING kind that names no kind moves: a non-string or missing
    # kind, and a real kind with a bad field, keep the schema refusal.
    ("inspect", "unknown-kind", {"target": {"kind": "nope"}}, UNSERVED),
    ("inspect", "kind-not-a-string", {"target": {"kind": 5}}, BAD),
    ("inspect", "kind-missing", {"target": {}}, BAD),
    ("inspect", "real-kind-bad-field",
     {"target": {"kind": "surface", "action": "shrink"}}, BAD),
    ("inspect", "withheld-kind-bad-field",
     {"target": {"kind": "admission"}, "max_bytes": "8192"}, BAD),
    # Deliberate: an unknown kind wins over another bad argument, so the
    # caller hears about the kind first.
    ("inspect", "unknown-kind-bad-max-bytes",
     {"target": {"kind": "nope"}, "max_bytes": "x"}, UNSERVED),
    # An alias is a real kind: with a bad field it keeps the schema refusal.
    ("inspect", "alias-kind-bad-field",
     {"target": {"kind": "attribute", "target_kind": "row"}}, BAD),
    ("inspect", "withheld-kind", {"target": {"kind": "admission"}}, UNSERVED),
    ("inspect", "target-missing", {}, BAD),
    ("inspect", "undeclared-arg", {"target": {"kind": "graph"}, "x": 1}, UNKNOWN),
    # search
    ("search", "lexical", {"query": "q"}, PASSES),
    ("search", "source-family-list", {"query": "q", "source_family": ["a"]},
     PASSES),
    ("search", "limit-past-max", {"query": "q", "limit": 1001}, BAD),
    ("search", "bad-method", {"query": "q", "method": "regex"}, BAD),
    ("search", "filter-subtype",
     {"query": "q", "filter": {"subtype": "document_chunk"}}, PASSES),
    ("search", "filter-exact-attrs",
     {"query": "q", "filter": {"attr_filters": {"path": "a.py"}}}, PASSES),
    ("search", "filter-source", {"query": "q", "filter": {"source": "s"}},
     PASSES),
    ("search", "filter-empty", {"query": "q", "filter": {}}, PASSES),
    # Declared, not enforced: a non-string subtype still reaches `search()`,
    # exactly as it did when `filter` was a bare dict.
    ("search", "filter-subtype-not-a-string",
     {"query": "q", "filter": {"subtype": 5}}, PASSES),
    ("search", "filter-as-json-string",
     {"query": "q", "filter": '{"subtype": "document_chunk"}'}, BAD),
    # The three shapes qwen3.8 sent in the #2792 probe (#2793).
    ("search", "filter-condition-contains",
     {"query": "q", "filter": {"field": "attrs.text", "op": "contains",
                               "value": "NewEventStreamFileReader"}}, BAD),
    ("search", "filter-condition-suffix",
     {"query": "q", "filter": {"field": "attrs.path", "op": "suffix",
                               "value": ".py"}}, BAD),
    ("search", "filter-condition-clauses",
     {"query": "q", "filter": {"op": "and", "clauses": [
         {"field": "subtype", "op": "eq", "value": "document_chunk"},
         {"field": "attrs.text", "op": "contains", "value": "crab"}]}}, BAD),
    ("search", "filter-subtype-plus-condition",
     {"query": "q", "filter": {"subtype": "document_chunk", "op": "eq"}}, BAD),
    # Methods that never reached `search()`'s key check keep accepting other
    # keys, exactly as on `dev`: the attrs lookup reads `subtypes`, and
    # identifier_mentions ignores `filter`.
    ("search", "attrs-lookup-reads-subtypes",
     {"filter": {"attr_filters": {"path": "a.py"}, "subtypes": ["source_file"]}},
     PASSES),
    ("search", "attrs-method-with-condition",
     {"method": "attrs", "attr_filters": {"path": "a.py"},
      "filter": {"field": "attrs.path", "op": "suffix", "value": ".py"}},
     PASSES),
    ("search", "identifier-mentions-ignores-filter",
     {"query": "Foo", "method": "identifier_mentions",
      "filter": {"op": "eq", "field": "a", "value": "b"}}, PASSES),
    # No embedder on this server: the semantic refusal still comes first.
    ("search", "semantic-unavailable-before-filter-check",
     {"query": "q", "method": "semantic", "filter": {"op": "eq"}}, PASSES),
    # expand
    ("expand", "string-anchor", {"input": "n"}, PASSES),
    ("expand", "object-anchor", {"input": {"node_id": "n"}}, PASSES),
    ("expand", "filtered",
     {"input": "n", "filter": {"op": "not", "clause": _EQ}}, PASSES),
    ("expand", "live", {"input": "n", "live": {"relation": "r"}}, PASSES),
    ("expand", "anchor-as-json-string", {"input": '{"node_id": "n"}'}, PASSES),
    ("expand", "empty-anchor", {"input": {}}, BAD),
    ("expand", "input-missing", {}, BAD),
    ("expand", "filter-as-json-string", {"input": "n", "filter": json.dumps(_EQ)},
     BAD),
    ("expand", "live-without-relation", {"input": "n", "live": {}}, BAD),
    # filter
    ("filter", "eq", {"input": {"kind": "nodes"}, "predicate": _EQ}, PASSES),
    ("filter", "empty-predicate", {"input": {"kind": "nodes"}, "predicate": {}},
     PASSES),
    # A field of a link: since #3666 step 2c a condition on a set of links
    # naming a record's field (`subtype`) is refused by name.
    ("filter", "nested-three-deep",
     {"input": {"kind": "edges", "filter": {"op": "or", "clauses": [_EDGE_EQ]}},
      "predicate": {"op": "and", "clauses": [
          {"op": "not", "clause": {"op": "or", "clauses": [_EDGE_EQ, _EDGE_EQ]}}]}},
     PASSES),
    ("filter", "input-as-json-string",
     {"input": '{"kind": "nodes"}', "predicate": _EQ}, BAD),
    ("filter", "predicate-as-json-string",
     {"input": {"kind": "nodes"}, "predicate": json.dumps(_EQ)}, BAD),
    ("filter", "op-outside-set",
     {"input": {"kind": "nodes"}, "predicate": {"op": "regex", "field": "a"}},
     BAD),
    ("filter", "nested-op-outside-set",
     {"input": {"kind": "nodes"},
      "predicate": {"op": "and", "clauses": [
          {"op": "not", "clause": {"op": "regex", "field": "a"}}]}}, BAD),
    ("filter", "empty-clauses",
     {"input": {"kind": "nodes"}, "predicate": {"op": "and", "clauses": []}},
     BAD),
    ("filter", "kind-missing", {"input": {}, "predicate": _EQ}, BAD),
    # map
    ("map", "list-projection",
     {"input": {"kind": "nodes"}, "projection": ["node_id"]}, PASSES),
    ("map", "alias-projection",
     {"input": {"kind": "nodes"}, "projection": {"id": "node_id"}}, PASSES),
    ("map", "projection-number", {"input": {"kind": "nodes"}, "projection": 7},
     BAD),
    # aggregate
    ("aggregate", "kindless-scope",
     {"input": {"subtype": "s"}, "by": ["subtype"], "metrics": ["count"]},
     PASSES),
    ("aggregate", "string-anchor",
     {"input": "n", "by": ["subtype"], "metrics": ["count"]}, PASSES),
    ("aggregate", "empty-scope",
     {"input": {}, "by": ["subtype"], "metrics": ["count"]}, BAD),
    ("aggregate", "metric-sum",
     {"input": {"kind": "nodes"}, "by": ["a"], "metrics": ["sum"]}, BAD),
    # query
    ("query", "select", {"sql": "select 1"}, PASSES),
    ("query", "row-cap-past-max", {"sql": "select 1", "row_cap": 10_001}, BAD),
]


#: The `dev` outcome where it differs from the shipped one: only the
#: condition-style `search.filter` rows, all of which `dev`'s `search()`
#: then refused in its body.
DEV_OUTCOME: dict[str, str] = {
    "search:filter-condition-contains": PASSES,
    "search:filter-condition-suffix": PASSES,
    "search:filter-condition-clauses": PASSES,
    "search:filter-subtype-plus-condition": PASSES,
}


#: Rows the published schema refuses although validation passes them on to
#: the body, stricter on the wire on purpose: a declared-not-enforced value
#: type, and `search.filter`'s closed key set, which the runtime enforces on
#: the ranked methods only (see `SearchFilter`).
PUBLISHED_STRICTER = frozenset({
    "search:filter-subtype-not-a-string",
    "search:attrs-lookup-reads-subtypes",
    "search:attrs-method-with-condition",
    "search:identifier-mentions-ignores-filter",
    "search:semantic-unavailable-before-filter-check",
    # Issue #3153: the schema keeps max_bytes minimum 4096 so a validating
    # client learns the floor up front; the server raises a smaller value to
    # it (and says so) for clients that do not validate.
    "inspect:max-bytes-under-floor",
})


async def _outcomes(mcp: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for tool, label, arguments, _expected in CASES:
        payload = _result_payload(await call_json(mcp, tool, dict(arguments)))
        code = (payload.get("error") or {}).get("code") if isinstance(
            payload.get("error"), dict,
        ) else None
        out[f"{tool}:{label}"] = code if code in VALIDATION_CODES else PASSES
    return out


async def test_the_server_accepts_and_refuses_what_it_did_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Server-side validation did not loosen or tighten.

    Three columns must agree for every case: the outcome on `dev` before
    #2792 (except the `DEV_OUTCOME` rows), the outcome with the new
    publishing step switched off, and the outcome as shipped. Among the refusals: every JSON-string object
    argument the qwen3.8 probe actually sent is still refused (except
    `expand.input`, whose string branch is a node id and always accepted a
    string), and a nested predicate op outside the closed set is refused
    three levels below the published cut.
    """
    from okg.substrate.mcp import server as server_mod

    expected = {f"{tool}:{label}": want for tool, label, _a, want in CASES}

    shipped = _build(monkeypatch, tool_profile="reader")
    try:
        after = await _outcomes(shipped)
    finally:
        _close(shipped)

    # `raising=False` so the same test runs against a `dev` checkout, where
    # the step does not exist yet and the server is already "switched off".
    monkeypatch.setattr(
        server_mod, "publish_client_readable_schemas", lambda _mcp: 0,
        raising=False,
    )
    # The lean pass narrows the inlined copy; with inlining off it is off too.
    monkeypatch.setattr(
        "okg.substrate.mcp.lean_surface.publish_lean_read_surface", lambda _mcp: 0,
        raising=False,
    )
    unpublished = _build(monkeypatch, tool_profile="reader")
    try:
        before = await _outcomes(unpublished)
        still_pointers = json.dumps(
            [t.inputSchema for t in await unpublished.list_tools()],
        )
    finally:
        _close(unpublished)
    assert "$ref" in still_pointers, (
        "the switched-off control still published inlined schemas, so it "
        "compares the change against itself"
    )

    assert set(DEV_OUTCOME) <= set(expected)
    assert before == expected, {
        k: (before[k], expected[k]) for k in expected if before[k] != expected[k]
    }
    assert after == expected, {
        k: (after[k], expected[k]) for k in expected if after[k] != expected[k]
    }


async def test_the_published_schema_admits_every_call_the_server_accepts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The inlined schema is not stricter than the server for any real call.

    A client that validates its own arguments against `tools/list` (some
    do) must not be told a call the server accepts is invalid. The one
    known exception is deliberate and older than this change: the
    published schema omits the explicit-null branch of optional arguments.
    The other is `PUBLISHED_STRICTER`: a value the schema DECLARES a type
    for while the body keeps the check (see `declared` in
    operator_models.py), so the published schema is stricter by design.
    """
    mcp = _build(monkeypatch, tool_profile="reader")
    try:
        schemas = {t.name: t.inputSchema for t in await mcp.list_tools()}
    finally:
        _close(mcp)
    rejected = []
    for tool, label, arguments, want in CASES:
        if want != PASSES or f"{tool}:{label}" in PUBLISHED_STRICTER:
            continue
        try:
            jsonschema.validate(arguments, schemas[tool])
        except jsonschema.ValidationError as exc:
            rejected.append(f"{tool}:{label}: {exc.message}")
    assert not rejected, rejected


@pytest.mark.parametrize("label", sorted(DEV_OUTCOME))
def test_every_newly_refused_call_was_already_refused_by_search(
    label: str,
) -> None:
    """The #2793 exception refuses nothing that used to work.

    Each row whose outcome moved cleared argument validation on `dev` and
    then reached `search()`, which refuses any filter key outside subtype,
    attr_filters and source before it touches a connection. Called here
    with no connection at all, as that function's own argument-only tests
    do, to show the refusal is unconditional.
    """
    from okg.substrate.mcp.search import search

    tool, name = label.split(":", 1)
    arguments, shipped = next(
        (a, w) for t, n, a, w in CASES if (t, n) == (tool, name)
    )
    # The moved rows moved from "cleared validation" to a named refusal,
    # and nowhere else.
    assert (DEV_OUTCOME[label], shipped) == (PASSES, BAD)
    assert arguments.get("method", "lexical") == "lexical"
    with pytest.raises(ValueError, match="unknown filter keys"):
        search(None, None, arguments["query"], method="lexical",  # type: ignore[arg-type]
               filter=arguments["filter"])


@pytest.mark.parametrize("label", sorted(DEV_OUTCOME))
async def test_a_condition_filter_on_search_is_refused_by_name(
    label: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#2793: refused as bad arguments, naming the keys, with the remedy.

    Not `operator_execution_failed`: that code says the server broke, when
    the caller used the `filter` tool's predicate grammar on `search`.
    """
    tool, name = label.split(":", 1)
    arguments = next(a for t, n, a, _w in CASES if (t, n) == (tool, name))
    mcp = _build(monkeypatch, tool_profile="reader")
    try:
        payload = _result_payload(await call_json(mcp, tool, dict(arguments)))
    finally:
        _close(mcp)
    error = payload["error"]
    assert error["code"] == "invalid_operator_arguments", error
    bad = sorted(set(arguments["filter"]) - {"subtype", "attr_filters", "source"})
    assert sorted(
        f.removeprefix("filter.") for f in error["data"]["invalid_fields"]
    ) == bad, error
    assert "attr_filters" in error["fix_hint"], error
    assert "`where`" in error["fix_hint"], error


async def test_search_publishes_where_not_the_old_filter_arguments(listed) -> None:
    """Design D9 (PACT read-model-s3-where-scope): `where` and `scope` are
    published; `filter`, `subtype_filter` and `attr_filters` are still
    accepted (the argument model validates them, see the refusal test
    above) but no longer published, so the startup bytes pay for one
    spelling of a filter, not two."""
    assert "search" in listed, "every surface here serves search"
    properties = listed["search"]["properties"]
    assert not {"filter", "subtype_filter", "attr_filters"} & set(properties)
    # One described object since the lean surface (#3666, M10): the short
    # form and the ops are said in words, not as a closed property set.
    assert properties["where"]["type"] == "object"
    assert "prefix" in properties["where"]["description"]
    assert "additionalProperties" not in properties["where"]
    assert set(properties["scope"]["properties"]) == {
        "record", "records", "children_of", "edge_type",
    }


#: JSON Schema keywords that make a whole input schema conditional. Claude
#: Code 2.1.292 drops a tool whose input schema has one at the top level
#: (PACT mcp-schema-no-top-level-anyof).
TOP_LEVEL_COMBINATORS = ("anyOf", "oneOf", "allOf", "not", "if", "then", "else")

#: Every surface a client can receive, plus the two-tool allowlist launch.
COMBINATOR_SURFACES = {**SURFACES, "inspect+search": {}}


async def _served_schemas(
    monkeypatch: pytest.MonkeyPatch, surface: str,
) -> dict[str, dict]:
    if surface == "inspect+search":
        monkeypatch.setenv("OKG_MCP_TOOL_ALLOWLIST", "inspect,search")
    mcp = _build(monkeypatch, **COMBINATOR_SURFACES[surface])
    try:
        return {tool.name: tool.inputSchema for tool in await mcp.list_tools()}
    finally:
        _close(mcp)


@pytest.mark.parametrize("surface", sorted(COMBINATOR_SURFACES))
async def test_no_served_tool_schema_has_a_top_level_combinator(
    surface: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A client that drops such a tool never sees it, with no error.

    A combinator on a property (an optional or a union type) stays: only
    the schema's own top level is checked.
    """
    schemas = await _served_schemas(monkeypatch, surface)
    assert "inspect" in schemas
    offending = {
        name: sorted(k for k in TOP_LEVEL_COMBINATORS if k in schema)
        for name, schema in schemas.items()
        if any(k in schema for k in TOP_LEVEL_COMBINATORS)
    }
    assert not offending, offending
    for name, schema in schemas.items():
        assert schema.get("type") == "object", name


@pytest.mark.parametrize("tool,argument,message", [
    ("inspect", "target",
     "inspect needs a `target`, or `next` to continue a paged read"),
    ("expand", "input",
     "expand needs an `input` node, or `next` to continue a path read"),
])
async def test_a_call_with_neither_argument_nor_next_is_refused(
    tool: str, argument: str, message: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The published schema no longer says "one of the two"; the body does,
    with the refusal it gave before, through the client's `call_tool` path."""
    mcp = _build(monkeypatch, tool_profile="reader")
    try:
        schemas = {t.name: t.inputSchema for t in await mcp.list_tools()}
        payload = _result_payload(await call_json(mcp, tool, {}))
    finally:
        _close(mcp)
    assert argument not in schemas[tool].get("required", [])
    error = payload["error"]
    assert error["code"] == "invalid_operator_arguments", error
    assert error["message"] == message, error


@pytest.mark.parametrize("surface", sorted(COMBINATOR_SURFACES))
async def test_target_input_and_next_describe_the_next_rule(
    surface: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no top-level `anyOf`, the property descriptions carry the rule."""
    schemas = await _served_schemas(monkeypatch, surface)
    described = 0
    for tool, argument in (("inspect", "target"), ("expand", "input")):
        if tool not in schemas:
            continue
        properties = schemas[tool]["properties"]
        assert properties[argument]["description"] == (
            "Required unless `next` is given."
            + (" What to read; select and max_bytes go beside it, not in it."
               if tool == "inspect" else "")
        ), tool
        assert properties["next"]["description"] == (
            f"Use instead of `{argument}` to continue a page."
        ), tool
        described += 1
    assert described, f"{surface} serves neither inspect nor expand"


@pytest.mark.parametrize("value", [
    {},
    {"subtype": None},
    {"subtype": "document_chunk", "attr_filters": {"a": {"nested": [1]}}},
    {"subtype": 5, "source": "s"},
    {"attr_filters": {"path": "a.py"}, "subtypes": ["source_file"]},
])
def test_the_search_body_receives_the_callers_filter_unchanged(value) -> None:
    """The body reads the mapping it read when `filter` was a `dict`.

    Explicit nulls, non-string values and extra keys all survive the
    validated model's round trip, so nothing after validation can tell the
    difference.
    """
    from okg.substrate.mcp.operator_models import (
        SearchFilter,
        search_filter_mapping,
    )

    assert search_filter_mapping(SearchFilter.model_validate(value)) == value
    assert search_filter_mapping(value) is value


def test_an_annotation_beside_a_pointer_is_the_fields_own_text() -> None:
    """`Annotated[Model, Field(description=...)]` renders a pointer with a
    sibling description. That is an override, not a contradiction, so a
    deployment tool written that way must not stop the server starting."""
    from okg.substrate.mcp.operator_models import inline_schema_refs

    schema = {
        "properties": {"a": {"$ref": "#/$defs/A", "description": "field text"}},
        "$defs": {"A": {"type": "object", "description": "model text"}},
    }
    assert inline_schema_refs(schema) == {
        "properties": {"a": {"type": "object", "description": "field text"}},
    }


def test_a_recursive_union_is_cut_and_says_so() -> None:
    """A JSON-value alias recurses through a union with no single type."""
    from okg.substrate.mcp.operator_models import inline_schema_refs

    schema = {
        "properties": {"a": {"$ref": "#/$defs/J"}},
        "$defs": {"J": {"anyOf": [
            {"type": "string"},
            {"type": "array", "items": {"$ref": "#/$defs/J"}},
        ]}},
    }
    inlined = inline_schema_refs(schema)["properties"]["a"]
    assert inlined["anyOf"][1]["items"] == {
        "description": "J: same shape as the enclosing one",
        "type": ["string", "array"],
    }
    assert "$ref" not in json.dumps(inlined)


def test_default_and_enum_values_are_data_not_schema() -> None:
    from okg.substrate.mcp.operator_models import (
        inline_schema_refs,
        type_schema_unions,
    )

    schema = {
        "properties": {
            "a": {"type": "object", "default": {"$ref": "#/$defs/Nope"}},
            "b": {"enum": [{"anyOf": [1]}]},
        },
    }
    assert type_schema_unions(inline_schema_refs(schema)) == schema


#: A deployment tool's signature, at module level because this module's
#: `from __future__ import annotations` makes FastMCP resolve the annotations
#: from module globals.
_JSONValue = TypeAliasType(
    "_JSONValue",
    Union[str, int, list["_JSONValue"], dict[str, "_JSONValue"]],
)


class _Query(BaseModel):
    """Model text."""

    term: str


def _deployment_style_lookup(
    q: Annotated[_Query, Field(description="field text")],
    extra: Optional[_JSONValue] = None,
) -> str:
    return "ok"


def test_a_deployment_style_tool_publishes_without_refusing_the_launch() -> None:
    """Every registered tool goes through the rewrite, a deployment's own
    agent tools included; the two signatures review found must publish."""
    from mcp.server.fastmcp import FastMCP

    from okg.substrate.mcp.operator_models import publish_client_readable_schemas

    mcp = FastMCP("probe")
    mcp.tool()(_deployment_style_lookup)
    publish_client_readable_schemas(mcp)
    published = mcp._tool_manager._tools["_deployment_style_lookup"].parameters
    assert "$ref" not in json.dumps(published)
    assert published["properties"]["q"]["type"] == "object"
    assert published["properties"]["q"]["description"] == "field text"
