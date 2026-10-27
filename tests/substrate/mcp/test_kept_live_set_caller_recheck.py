"""The caller check a read of a kept live set asks again (hermetic).

A first read of a live type runs `check_caller`. A later read of the kept set
(an `l1.` id) meets its validated arguments in `server._live_open`, which asks
`check_caller_sync` there: the same decision, run on a call worker. These
rows hold the decision itself, with a stand-in server that carries only what
it looks at (visible tools, agent surface, dispatch gate); the real functions
run. The reads through a real server are the PostgreSQL rows in
`test_live_graph_keep.py` (kept set, JSON-string input, run step, live.each).
"""
from __future__ import annotations

import asyncio
import types

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import live_resolver, live_result_sets
from okg.substrate.mcp import result_references as rr


class Server:
    def __init__(self) -> None:
        self.visible = frozenset({"filter", "expand", "inspect"})
        self.surface = types.SimpleNamespace(live_types=("site",))
        self.gate_asked: list[str] = []

    @property
    def _okg_agent_surface(self):
        return self.surface

    def okg_visible_tools(self):
        return self.visible

    def _okg_access_rules_state(self):
        return False

    def _okg_dispatch_principal_gate(self, tool):
        self.gate_asked.append(tool)

    async def _okg_run_blocking(self, fn, *args, **kwargs):
        return fn(*args, **kwargs)


def _reason(exc: SubstrateError) -> str:
    return (getattr(exc, "data", None) or {}).get("reason", "")


def _first_read(server, tool="filter", subtype="site"):
    asyncio.run(live_resolver.check_caller(server, tool, subtype))


def _kept_read(server, tool="filter", subtype="site"):
    live_resolver.check_caller_sync(server, tool, subtype)


def _deny_expand(tool):
    if tool == "expand":
        err = SubstrateError("denied")
        err.code = "nomos_identity_denied"
        raise err


def _expand_taken_away(server):
    server.visible = frozenset({"filter", "inspect"})


def _type_unserved(server):
    server.surface = types.SimpleNamespace(live_types=())


def _gate_denies_expand(server):
    server._okg_dispatch_principal_gate = _deny_expand


CASES = [
    pytest.param(_expand_taken_away, "expand_not_served", id="expand_taken_away"),
    pytest.param(_type_unserved, "live_type_not_served", id="type_unserved"),
    pytest.param(_gate_denies_expand, "", id="gate_denies_expand"),
]


@pytest.mark.parametrize("change,reason", CASES)
def test_a_first_read_is_refused(change, reason) -> None:
    server = Server()
    change(server)
    with pytest.raises(SubstrateError) as caught:
        _first_read(server)
    assert _reason(caught.value) == reason


@pytest.mark.parametrize("change,reason", CASES)
def test_a_kept_read_is_refused_as_a_first_read_is(change, reason) -> None:
    server = Server()
    _kept_read(server)  # passes while the caller passes
    change(server)
    with pytest.raises(SubstrateError) as kept:
        _kept_read(server)
    with pytest.raises(SubstrateError) as first:
        _first_read(server)
    assert _reason(kept.value) == _reason(first.value) == reason
    assert getattr(kept.value, "code", None) == getattr(first.value, "code", None)


def test_a_kept_read_asks_the_gate_about_expand() -> None:
    server = Server()
    _kept_read(server, tool="aggregate")
    assert server.gate_asked == ["expand"]


def test_a_server_without_a_gate_or_surface_list_still_checks_the_type() -> None:
    server = Server()
    server._okg_dispatch_principal_gate = None
    server.visible = None  # no per-session list: the tool manager decides
    server._tool_manager = types.SimpleNamespace(get_tool=lambda name: object())
    _kept_read(server)
    _type_unserved(server)
    with pytest.raises(SubstrateError) as caught:
        _kept_read(server)
    assert _reason(caught.value) == "live_type_not_served"


def test_another_reader_cannot_look_up_a_kept_id() -> None:
    """The store keys a kept set to its reader, so a set held by another
    reader never reaches the caller check: it is unknown."""
    store = live_result_sets.new_store()
    draft = store.mint(
        reader_key="reader-a", binding={}, members=(), header_claim=None,
        multi_user=False, payload=object(), payload_bytes=1, payload_rows=1,
    )
    store.commit(draft)
    assert store.lookup(draft.id, reader_key="reader-a").id == draft.id
    with pytest.raises(rr.ResultReferenceInvalid) as caught:
        store.lookup(draft.id, reader_key="reader-b")
    assert "unknown" in str(getattr(caught.value, "data", "")) + str(caught.value)


def _raw_kept_read(server, tool="filter"):
    """What a kept raw answer (no live type) is asked: expand's own check."""
    live_resolver.ask_expand_gate(server, tool)
    live_resolver.check_expand_served(server, tool)


@pytest.mark.parametrize("change,reason", [CASES[0], CASES[2]])
def test_a_kept_raw_read_is_refused_as_expand_is(change, reason) -> None:
    server = Server()
    _raw_kept_read(server)
    change(server)
    with pytest.raises(SubstrateError) as caught:
        _raw_kept_read(server)
    assert _reason(caught.value) == reason
    assert (caught.value.data or {}).get("tool_called") == "filter"


def test_a_kept_raw_read_does_not_ask_for_a_live_type() -> None:
    server = Server()
    _type_unserved(server)  # a raw answer has no live type to serve
    _raw_kept_read(server)
    assert server.gate_asked == ["expand"]
