"""Promise tests for `Unknown`, `read_ok` and `to_wire` (issue #2573, slice 1).

A read that lacks a fact says "unknown" and why. These tests pin the value's
refusals (no truth value, no number formatting, no silent JSON), the verdict
reader, the wire form, and that each of the three read exits -- the CLI JSON
emitter, the viz read routes, and MCP tool results -- turns an `Unknown` into
`null` plus a reason instead of raising or printing a plausible value.
"""
from __future__ import annotations

import asyncio
import dataclasses
import io
import json

import pytest

from okg.substrate.unknown import UNKNOWN_REASONS_KEY, Unknown, read_ok, to_wire
from tests.substrate.mcp._result_payload import (
    call_json,
    json_client_sessions,
    payload as _json_payload,
)


# ---------- the value -------------------------------------------------------



@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


def test_unknown_has_no_truth_value() -> None:
    value = Unknown("unpriced_calls:3_of_40")
    with pytest.raises(TypeError, match="no truth value"):
        bool(value)
    with pytest.raises(TypeError):
        if value:  # noqa: SIM108 - the refusal is the behavior under test
            pass


def test_unknown_refuses_number_formatting() -> None:
    value = Unknown("unpriced_calls:3_of_40")
    with pytest.raises(TypeError):
        f"{value:.6f}"  # noqa: B018 - formatting is the behavior under test
    with pytest.raises(TypeError):
        f"{value}"  # noqa: B018
    with pytest.raises(TypeError):
        format(value, "")


def test_unknown_equality_hash_and_repr_follow_the_reason() -> None:
    assert Unknown("a") == Unknown("a")
    assert Unknown("a") != Unknown("b")
    assert Unknown("a") != 0
    assert Unknown("a") != None  # noqa: E711 - no equality with a stand-in
    assert hash(Unknown("a")) == hash(Unknown("a"))
    assert repr(Unknown("poll_value_not_json")) == "Unknown('poll_value_not_json')"
    assert not hasattr(Unknown("a"), "__dict__")
    assert not dataclasses.is_dataclass(Unknown)


@pytest.mark.parametrize("reason", ["", "   ", None, 3])
def test_unknown_requires_a_reason(reason: object) -> None:
    with pytest.raises(ValueError):
        Unknown(reason)  # type: ignore[arg-type]


def test_unknown_is_not_arithmetic() -> None:
    with pytest.raises(TypeError):
        Unknown("x") + 1  # type: ignore[operator]
    with pytest.raises(TypeError):
        sum([1.0, Unknown("x")])  # type: ignore[list-item]


def test_asdict_keeps_unknown_and_json_refuses_it() -> None:
    @dataclasses.dataclass
    class Row:
        model: str
        cost_usd: object

    row = dataclasses.asdict(Row("gpt-x", Unknown("unpriced_calls:3_of_40")))
    assert isinstance(row["cost_usd"], Unknown)
    assert row["cost_usd"].reason == "unpriced_calls:3_of_40"
    with pytest.raises(TypeError):
        json.dumps(row)


# ---------- read_ok ---------------------------------------------------------


@pytest.mark.parametrize("ok", [True, False])
def test_read_ok_returns_a_real_boolean(ok: bool) -> None:
    assert read_ok({"ok": ok}, source="probe") is ok


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        (None, "probe_not_an_object"),
        ([{"ok": True}], "probe_not_an_object"),
        ("ok", "probe_not_an_object"),
        ({}, "probe_reported_no_ok"),
        ({"detail": "no verdict"}, "probe_reported_no_ok"),
        ({"ok": None}, "probe_ok_not_boolean"),
        ({"ok": 1}, "probe_ok_not_boolean"),
        ({"ok": "true"}, "probe_ok_not_boolean"),
    ],
)
def test_read_ok_names_why_a_verdict_is_unknown(payload: object, reason: str) -> None:
    verdict = read_ok(payload, source="probe")
    assert isinstance(verdict, Unknown)
    assert verdict.reason == reason


# ---------- to_wire ---------------------------------------------------------


def test_to_wire_replaces_unknown_with_null_and_collects_reasons() -> None:
    payload = {
        "cost_usd": Unknown("unpriced_calls:3_of_40"),
        "groups": [
            {"model": "a", "cost_usd": 1.5},
            {"model": "b", "cost_usd": Unknown("unpriced_calls:1_of_1")},
        ],
        "pair": (Unknown("latency_missing"), 2),
        "nested": {"deeper": {"ok": Unknown("probe_reported_no_ok")}},
    }

    wired = to_wire(payload)

    assert wired["cost_usd"] is None
    assert wired["groups"][0] == {"model": "a", "cost_usd": 1.5}
    assert wired["groups"][1]["cost_usd"] is None
    assert list(wired["pair"]) == [None, 2]
    assert wired["nested"]["deeper"]["ok"] is None
    assert wired[UNKNOWN_REASONS_KEY] == {
        "cost_usd": "unpriced_calls:3_of_40",
        "groups.1.cost_usd": "unpriced_calls:1_of_1",
        "pair.0": "latency_missing",
        "nested.deeper.ok": "probe_reported_no_ok",
    }
    # The input is not mutated, and the result is plain JSON.
    assert isinstance(payload["cost_usd"], Unknown)
    json.dumps(wired)


def test_to_wire_leaves_a_known_payload_untouched() -> None:
    payload = {"calls": 40, "cost_usd": 1.2041, "rows": [{"a": None}]}
    wired = to_wire(payload)
    assert wired is payload
    assert UNKNOWN_REASONS_KEY not in wired


def test_to_wire_is_idempotent() -> None:
    once = to_wire({"x": Unknown("r")})
    assert to_wire(once) == once


def test_to_wire_refuses_an_unknown_it_cannot_explain() -> None:
    with pytest.raises(TypeError, match="mapping"):
        to_wire(Unknown("top_level"))
    with pytest.raises(TypeError, match="mapping"):
        to_wire([Unknown("in_a_list")])


def test_to_wire_merges_existing_reasons_and_refuses_a_conflict() -> None:
    merged = to_wire({"a": Unknown("r1"), UNKNOWN_REASONS_KEY: {"b": "r0"}})
    assert merged[UNKNOWN_REASONS_KEY] == {"a": "r1", "b": "r0"}
    with pytest.raises(ValueError, match="conflict"):
        to_wire({"a": Unknown("r1"), UNKNOWN_REASONS_KEY: {"a": "other"}})


# ---------- exit 1: the CLI JSON emitter -----------------------------------


def test_cli_emit_result_json_carries_null_and_reason() -> None:
    from okg.substrate.cli._output import emit_result

    out = io.StringIO()
    emit_result(
        {"calls": 40, "cost_usd": Unknown("unpriced_calls:3_of_40")},
        as_json=True,
        stream=out,
    )
    doc = json.loads(out.getvalue())
    assert doc["cost_usd"] is None
    assert doc[UNKNOWN_REASONS_KEY] == {"cost_usd": "unpriced_calls:3_of_40"}
    assert "Unknown(" not in out.getvalue()


def test_cli_emit_result_text_names_the_reason() -> None:
    from okg.substrate.cli._output import emit_result

    out = io.StringIO()
    emit_result(
        {"calls": 40, "cost_usd": Unknown("unpriced_calls:3_of_40")},
        as_json=False,
        stream=out,
    )
    text = out.getvalue()
    assert "cost_usd: None" in text
    assert "unpriced_calls:3_of_40" in text


def test_cli_emit_ok_goes_through_the_same_exit(capsys: pytest.CaptureFixture[str]) -> None:
    from okg.substrate.cli.main import _emit_ok

    _emit_ok({"ok": Unknown("probe_reported_no_ok")}, as_json=True)
    doc = json.loads(capsys.readouterr().out)
    assert doc["ok"] is None
    assert doc[UNKNOWN_REASONS_KEY] == {"ok": "probe_reported_no_ok"}


# ---------- exit 2: viz read responses -------------------------------------


def test_viz_read_route_serves_null_and_reason() -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from okg.viz import endpoints

    router = endpoints.APIRouter(prefix="/viz/api", route_class=endpoints.WireRoute)

    @router.get("/probe")
    def probe() -> dict[str, object]:
        return {"cost_usd": Unknown("unpriced_calls:1_of_1"), "calls": 1}

    @router.get("/probe-async")
    async def probe_async() -> dict[str, object]:
        return {"rows": [{"ok": Unknown("probe_reported_no_ok")}]}

    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)

    sync_doc = client.get("/viz/api/probe").json()
    assert sync_doc == {
        "cost_usd": None,
        "calls": 1,
        UNKNOWN_REASONS_KEY: {"cost_usd": "unpriced_calls:1_of_1"},
    }
    async_doc = client.get("/viz/api/probe-async").json()
    assert async_doc[UNKNOWN_REASONS_KEY] == {"rows.0.ok": "probe_reported_no_ok"}


def test_viz_build_router_uses_the_wire_route() -> None:
    from okg.viz import endpoints

    router = endpoints.build_router(lambda: None)  # type: ignore[arg-type,return-value]
    assert router.route_class is endpoints.WireRoute
    api_routes = [r for r in router.routes if isinstance(r, endpoints.APIRoute)]
    assert api_routes
    assert all(isinstance(r, endpoints.WireRoute) for r in api_routes)


def test_viz_prebuilt_envelope_responses_are_wired() -> None:
    import msgpack

    from okg.viz import endpoints

    payload = {"nodes": [], "meta": {"cost_usd": Unknown("unpriced_calls:1_of_1")}}
    json_doc = json.loads(endpoints._envelope_response(payload, "json").body)
    assert json_doc["meta"]["cost_usd"] is None
    assert json_doc[UNKNOWN_REASONS_KEY] == {"meta.cost_usd": "unpriced_calls:1_of_1"}
    packed = msgpack.unpackb(endpoints._envelope_response(payload, None).body)
    assert packed[UNKNOWN_REASONS_KEY] == {"meta.cost_usd": "unpriced_calls:1_of_1"}


# ---------- exit 3: MCP tool results ---------------------------------------


def _registered_mcp(fn):  # noqa: ANN001, ANN202
    from mcp.server.fastmcp import FastMCP

    from okg.substrate.mcp.tool_registration import _register_tool

    mcp = FastMCP("unknown-probe")
    _register_tool(mcp, tool_name="probe", fn=fn, traced=lambda _name: (lambda f: f))
    return mcp


def _tool_payload(result: object) -> dict:
    """The dict a client receives: structured content, else the text block.

    Unwired, FastMCP renders an `Unknown` through `str()` into the text block
    as "Unknown('...')" -- a string where a fact should be.
    """
    if isinstance(result, tuple):
        _blocks, structured = result
        return structured
    (block,) = result  # type: ignore[misc]
    return json.loads(block.text)


def test_mcp_tool_result_carries_null_and_reason() -> None:
    def probe() -> dict:
        return {"cost_usd": Unknown("unpriced_calls:1_of_1"), "calls": 1}

    mcp = _registered_mcp(probe)
    result = asyncio.run(call_json(mcp, "probe", {}))
    doc = _tool_payload(result)
    assert doc["cost_usd"] is None
    assert doc[UNKNOWN_REASONS_KEY] == {"cost_usd": "unpriced_calls:1_of_1"}


def test_mcp_async_tool_result_carries_null_and_reason() -> None:
    async def probe() -> dict:
        return {"ok": Unknown("probe_reported_no_ok")}

    mcp = _registered_mcp(probe)
    doc = _tool_payload(asyncio.run(call_json(mcp, "probe", {})))
    assert doc["ok"] is None
    assert doc[UNKNOWN_REASONS_KEY] == {"ok": "probe_reported_no_ok"}


# ---------- review fixes (PR #2680) ----------------------------------------


def test_unknown_refuses_str_so_default_str_cannot_leak_it() -> None:
    value = Unknown("unpriced_calls:1_of_1")
    with pytest.raises(TypeError):
        str(value)
    with pytest.raises(TypeError):
        json.dumps({"cost_usd": value}, default=str)
    # repr stays readable for %r logging and debugging.
    assert "%r" % (value,) == "Unknown('unpriced_calls:1_of_1')"


def test_to_wire_refuses_a_duplicate_dotted_path() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        to_wire({"a.b": Unknown("x"), "a": {"b": Unknown("y")}})
    with pytest.raises(ValueError, match="duplicate"):
        to_wire({1: Unknown("x"), "1": Unknown("y")})


def test_to_wire_refuses_unknown_inside_a_container_it_does_not_walk() -> None:
    from pydantic import BaseModel, ConfigDict

    @dataclasses.dataclass
    class Row:
        cost_usd: object

    class Model(BaseModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)
        cost_usd: object

    for holder in (
        Row(Unknown("r")),
        {Unknown("r")},
        frozenset({Unknown("r")}),
        Model(cost_usd=Unknown("r")),
        [Row(cost_usd=[Unknown("r")])],
    ):
        with pytest.raises(TypeError, match="does not walk"):
            to_wire({"payload": holder})
    # The same containers without an Unknown pass through untouched.
    known = {"row": Row(1.0), "tags": {"a"}, "model": Model(cost_usd=2)}
    assert to_wire(known) is known


def test_to_wire_keeps_a_namedtuple_type() -> None:
    from collections import namedtuple

    Pair = namedtuple("Pair", "cost calls")
    wired = to_wire({"pair": Pair(Unknown("r"), 3)})
    assert isinstance(wired["pair"], Pair)
    assert wired["pair"] == Pair(None, 3)
    assert wired[UNKNOWN_REASONS_KEY] == {"pair.0": "r"}


def test_wire_callable_flag_does_not_leak_through_functools_wraps() -> None:
    import functools

    from okg.substrate.unknown import wire_callable

    inner = wire_callable(lambda: {"x": 1})

    @functools.wraps(inner)
    def outer() -> dict:
        return {"x": Unknown("added_outside")}

    wired_outer = wire_callable(outer)
    assert wired_outer is not outer
    assert wired_outer()[UNKNOWN_REASONS_KEY] == {"x": "added_outside"}


def test_mcp_core_read_tool_serves_unknown_within_its_byte_budget() -> None:
    from okg.substrate.mcp.response_bounds import (
        ByteBoundedFastMCP,
        public_call_tool_result_bytes,
    )
    from okg.substrate.mcp.tool_registration import register_core_read_tools

    def make(name: str):  # noqa: ANN202
        # `input` and `predicate`: a filter, map or aggregate call that names
        # no set is served over every record (#3666, M10), so the read
        # grammar passes both; a stub that declared neither would refuse.
        def tool(
            max_bytes: int | None = None,
            input: dict | None = None,  # noqa: A002 - the tools' own name
            predicate: dict | None = None,
        ) -> dict:
            del input, predicate
            return {
                "operator_name": name,
                "cost_usd": Unknown("unpriced_calls:1_of_1"),
                "rows": [{"ok": Unknown("probe_reported_no_ok")}],
            }

        tool.__name__ = name
        return tool

    names = ("inspect", "search", "expand", "filter", "map", "aggregate", "query")
    mcp = ByteBoundedFastMCP("unknown-core-read")
    register_core_read_tools(
        mcp,
        traced=lambda _name: (lambda f: f),
        **{f"{name}_tool": make(name) for name in names},
    )
    for name in names:
        result = asyncio.run(call_json(mcp, name, {"max_bytes": 4096}))
        doc = _json_payload(result)
        assert not result.isError, doc
        assert doc["cost_usd"] is None, doc
        assert doc[UNKNOWN_REASONS_KEY] == {
            "cost_usd": "unpriced_calls:1_of_1",
            "rows.0.ok": "probe_reported_no_ok",
        }
        assert public_call_tool_result_bytes(doc) <= 4096
