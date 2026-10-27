"""A hand chain's kept live answers read as one set, through the tools (PACT
read-multi-handle-input).

PostgreSQL tests on a small published graph whose `page` type declares three
neutral fixture relations: `lrs_rows(q, size)`, `lrs_min(q, min_size)` and
`lrs_other(q)`, answered by a local per-argument fake system that counts its
calls. Every call goes through a real in-memory MCP client session, so the
calls share one logical session (the hint memory and the handles are bound to
it) and every assertion reads the reply as a client receives it.

MF marks a test that fails on the base (c429dfda44, PR #4060 dev-merged) for
the reason its docstring gives: there, no reply carries `chain_hint`.
"""
from __future__ import annotations

import contextlib
import json
import pathlib
import threading
import uuid
from typing import Any, Awaitable, Callable, Iterator

import pytest

from okg.substrate.external_backed.live_dispatch import LiveAdapterError, UpstreamCutRows
from okg.substrate.external_backed.live_relations import ANNOTATION_KEY, parse_live_relations
from okg.substrate.mcp import live_result_sets

pytestmark = pytest.mark.postgres


def _relation(args: dict[str, Any]) -> dict[str, Any]:
    return {
        "action": "external.fixture.read", "principal_mode": "service",
        "sample_policy": "inline", "retention_class": "warm",
        "deadline_seconds": 40.0, "args": args,
    }


Q = {"type": "string", "required": True, "maxLength": 200}
RELATIONS = {
    "lrs_rows": _relation({
        "q": Q, "size": {"type": "integer", "default": 10, "minimum": 1, "maximum": 100},
    }),
    "lrs_min": _relation({
        "q": Q, "min_size": {"type": "integer", "default": 1, "minimum": 0, "maximum": 1000},
    }),
    "lrs_other": _relation({"q": Q}),
    "lrs_state": _relation({
        "q": Q,
        "state": {"type": "string", "enum": ["open", "closed", "any"], "default": "any"},
    }),
}


class Upstream:
    """The fake system: one answer per value of `q`, counted."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[dict[str, Any]] = []
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.sizes: dict[str, int] = {}
        self.fail: set[str] = set()
        self.echo = False
        self.cut: set[str] = set()
        self.wait: dict[str, threading.Event] = {}

    def __call__(self, *, config, args, node_id, deadline_seconds):
        with self.lock:
            self.calls.append(dict(args))
        q = str(args["q"])
        gate = self.wait.get(q)
        if gate is not None:
            assert gate.wait(30), f"the test never released q={q}"
        if q in self.fail:
            raise LiveAdapterError("upstream_unavailable")
        if q in self.rows:
            return [dict(row) for row in self.rows[q]]
        row = {"name": f"ds-{q}", "size": self.sizes.get(q, 10), "files": 3}
        if q in self.cut:
            return UpstreamCutRows([row], received=1)
        return [{"q": q, **row} if self.echo else row]

    def total(self) -> int:
        with self.lock:
            return len(self.calls)


class CountingGate:
    def __call__(self, relation, principal, node_id):
        from okg.substrate.external_backed.live_dispatch import GateOutcome

        return GateOutcome(allowed=True, reason="allow", principal=principal,
                           action=relation.action)


def _publish(dsn: str) -> str:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import (
        SubtypeSpec,
        bind_test_catalog_writer,
        seed_catalog,
    )

    sfx = uuid.uuid4().hex[:8]
    hub, peer = f"n:hub-{sfx}", f"n:peer-{sfx}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=[SubtypeSpec("page")],
                     narrowings=[("page", "cites", "page")])
        ChronosGraphStore.from_connection(conn).apply_facts([
            NodeFact(hub, "page", {"text": "hub"}, {"id": hub}, {"v": 1}),
            NodeFact(peer, "page", {"text": "peer"}, {"id": peer}, {"v": 1}),
            EdgeFact(src=peer, dst=hub, edge_type="cites", attrs={},
                     source_record_id={"src": peer, "dst": hub},
                     source_revision={"v": 1}),
        ])
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published
    declared = parse_live_relations({ANNOTATION_KEY: RELATIONS}, subtype="page")
    stored = json.dumps({"live_relations": {
        name: rel.to_metadata() for name, rel in declared.items()}})
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = COALESCE(metadata, '{}'::jsonb) "
            "|| %s::jsonb WHERE name = 'page'", (stored,),
        )
    return hub


def _deployment(root: pathlib.Path) -> None:
    import yaml

    d = root / "default"
    d.mkdir(parents=True, exist_ok=True)
    (d / "schemas").mkdir(exist_ok=True)
    (d / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    bindings = {name: {"kind": "http", "config": {"url": "http://127.0.0.1:9"}}
                for name in RELATIONS}
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "default", "description": "chain handle fixture",
        "postgres": {"dsn": "${OKG_LIVEBIND_DSN}"},
        "schema_dir": "schemas", "source_registry": "source_registry.yaml",
        "modules": [], "enrichers": [],
        "agent_surface": {
            "served": ["inspect", "search", "expand", "filter", "map", "aggregate", "run"],
            "live_bindings": bindings,
        },
    }, sort_keys=False), encoding="utf-8")


class Clock:
    def __init__(self) -> None:
        import time

        self.now = time.time()

    def __call__(self) -> float:
        return self.now


class Client:
    """One in-memory MCP client session: the calls share a logical session."""

    def __init__(self, client: Any) -> None:
        self.client = client

    async def call(self, tool: str, arguments: dict[str, Any], fmt: str = "json") -> Any:
        return await self.client.call_tool(tool, arguments, meta={"okg/result_format": fmt})

    async def ok(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = await self.call(tool, arguments)
        body = json.loads(result.content[0].text)
        assert result.isError is False, json.dumps(body)[:2000]
        return body

    async def text(self, tool: str, arguments: dict[str, Any]) -> str:
        result = await self.call(tool, arguments, "text")
        text = result.content[0].text
        assert result.isError is False, text[:2000]
        return text

    async def refused(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = await self.call(tool, arguments)
        body = json.loads(result.content[0].text)
        assert result.isError is True, json.dumps(body)[:2000]
        return body["error"]


class Lab:
    def __init__(self, mcp: Any, upstream: Upstream, hub: str, dsn: str, clock: Clock) -> None:
        self.mcp, self.upstream, self.hub, self.dsn, self.clock = mcp, upstream, hub, dsn, clock

    def sessions(self, count: int, script: Callable[..., Awaitable[Any]]) -> Any:
        """Run `script(*clients)` with `count` client sessions open."""
        import anyio
        from mcp.shared.memory import create_connected_server_and_client_session

        async def drive() -> Any:
            async with contextlib.AsyncExitStack() as stack:
                clients = [
                    Client(await stack.enter_async_context(
                        create_connected_server_and_client_session(self.mcp),
                    ))
                    for _ in range(count)
                ]
                return await script(*clients)

        return anyio.run(drive)

    def call(self, q: Any, relation: str = "lrs_rows", **args: Any) -> dict[str, Any]:
        return {"input": {"node_id": self.hub}, "live": {
            "relation": relation, "args": {"q": q, **args}}}

    def samples(self, relation: str = "lrs_rows") -> list[dict[str, Any]]:
        import psycopg

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT tool_name, status, attrs FROM okg.call_sample_warm "
                "WHERE relation = %s ORDER BY observed_at, sample_id", (relation,),
            ).fetchall()
        return [{"tool": r[0], "status": r[1], "attrs": r[2]} for r in rows]


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Lab]:
    from okg.substrate.mcp.server import build_server

    clock = Clock()
    previous = live_result_sets.install_live_store(live_result_sets.new_store(clock=clock))
    try:
        hub = _publish(clean_runtime)
        _deployment(tmp_path)
        monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
        monkeypatch.setenv("OKG_LIVEBIND_DSN", clean_runtime)
        upstream = Upstream()

        def runner(**kwargs: Any) -> Any:
            return upstream(**kwargs)

        mcp = build_server(dsn=clean_runtime, live_adapters={"http": runner},
                           auto_advance="always", deployment_name="default")
        mcp._okg_live_gate = CountingGate()  # type: ignore[attr-defined]
        try:
            yield Lab(mcp, upstream, hub, clean_runtime, clock)
        finally:
            for attr in ("_okg_pool", "_okg_write_pool"):
                pool = getattr(mcp, attr, None)
                if pool is not None:
                    pool.close()
    finally:
        live_result_sets.install_live_store(previous)


def _hint(body: dict[str, Any]) -> Any:
    return body["external_live"].get("chain_hint")


def _kept(body: dict[str, Any]) -> str:
    reference = body["external_live"]["result_reference"]
    assert reference["retained"] is True, reference
    return str(reference["id"])


def _filled(call: dict[str, Any], field: str = "size") -> dict[str, Any]:
    """A hinted call with `<field>` set, as an agent sends it."""
    text = json.dumps(call["arguments"]).replace("<field>", field)
    return json.loads(text)


def _store() -> Any:
    return live_result_sets.live_store()


def _chains() -> list[Any]:
    from okg.substrate.mcp.live_handle_lists import LiveChainSet

    return [e for e in _store()._entries.values() if isinstance(e.payload, LiveChainSet)]  # noqa: SLF001


async def _chain_of(
    client: Client, lab: Lab, values: list[Any], *, max_bytes: Any = None, **args: Any,
) -> list[dict[str, Any]]:
    extra = {} if max_bytes is None else {"max_bytes": max_bytes}
    return [await client.ok("expand", {**lab.call(v, **args), **extra}) for v in values]


# ============================================================ 3: the chain line and the read


def test_the_third_kept_answer_names_a_chain_handle_and_reading_it_equals_the_fan_out(lab) -> None:
    """MF 3: the call-3 reply carries the chain line and a committed chain
    handle; its total and ranking equal `expand(each)` + `aggregate` on the
    same values, with no upstream call. Base: no chain line."""
    lab.upstream.sizes.update({"a": 7, "b": 11, "c": 5})

    async def script(client: Client) -> Any:
        first, second, third = await _chain_of(client, lab, ["a", "b", "c"])
        assert _hint(first) is None and _hint(second) is None
        hint = _hint(third)
        assert hint["answers"] == 3 and hint["argument"] == "q"
        chain = hint["chain"]
        assert _store().held_for(
            chain, reader_key=_store()._entries[chain].reader_key,  # noqa: SLF001
            quota_key=_store()._entries[chain].quota_key,  # noqa: SLF001
        ) == "committed"
        calls = lab.upstream.total()
        total = await client.ok("aggregate", _filled(hint["calls"][0]))
        rank = await client.ok("aggregate", _filled(hint["calls"][1]))
        assert lab.upstream.total() == calls, "a chain read makes no upstream call"
        fan = await client.ok("expand", {"input": {"node_id": lab.hub}, "live": {
            "relation": "lrs_rows", "each": {"q": ["a", "b", "c"]}}})
        fan_total = await client.ok("aggregate", {
            "input": _kept(fan), "by": [], "metrics": ["sum(attrs.size)"]})
        # The fourth call's text names the newer chain of 4.
        text = await client.text("expand", lab.call("d"))
        return text, total, rank, fan_total, chain

    text, total, rank, fan_total, chain = lab.sessions(1, script)
    assert "! these 4 answers are kept as one handle; read them with no new upstream calls" in text
    assert 'total aggregate(input="l1.' in text and '"by":["attrs.q"]' not in text
    assert total["groups"][0]["sum(attrs.size)"] == 23 == fan_total["groups"][0]["sum(attrs.size)"]
    assert total["totals_cover"] == fan_total["totals_cover"]
    assert total["combined"]["chain"] == chain and total["combined"]["answers"] == 3
    assert total["handle"]["from"] == chain
    assert [g["max(attrs.size)"] for g in rank["groups"]] == [11, 7, 5]


def test_the_chain_read_prints_its_input_line_and_writes_one_sample_per_source(lab) -> None:
    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, ["a", "b", "c"])
        chain = _hint(replies[-1])["chain"]
        before = len(lab.samples())
        text = await client.text("aggregate", {
            "input": chain, "by": [], "metrics": ["sum(attrs.size)"]})
        return chain, before, text

    chain, before, text = lab.sessions(1, script)
    assert "input: 3 kept answers of lrs_rows, one per q · 3 rows · read " in text
    rows = lab.samples()[before:]
    sources = [r for r in rows if r["attrs"].get("combined_into") == chain]
    assert len(sources) == 3 and all(r["attrs"]["rows_served"] == 0 for r in sources)
    (read,) = [r for r in rows if r["attrs"].get("combined_from") == 3]
    assert read["attrs"]["rows_served"] == 1 and len(rows) == 4


# ============================================================ 4: a failed call


def test_the_hint_survives_one_failed_call_and_names_it(lab) -> None:
    """MF 4 (M1): a failed call has no handle; the chain holds the rest and
    the hint names it. Base: no chain line."""
    lab.upstream.fail.add("b")

    async def script(client: Client) -> Any:
        await client.ok("expand", lab.call("a"))
        # A failed live read sets isError (lane re, issue 4); the graph part
        # beside it is still served, and it makes no handle.
        raw = await client.call("expand", lab.call("b"))
        failed = json.loads(raw.content[0].text)
        assert raw.isError is True, json.dumps(failed)[:2000]
        assert failed["external_live"]["state"] == "error"
        assert failed["external_live"]["error"]["failure"] == "upstream_unavailable"
        assert _hint(failed) is None
        await client.ok("expand", lab.call("c"))
        body = await client.ok("expand", lab.call("d"))
        text = await client.text("expand", lab.call("e"))
        return body, text

    body, text = lab.sessions(1, script)
    hint = _hint(body)
    assert hint["answers"] == 3 and hint["failed"] == ["b"]
    assert "! these 4 answers are kept as one handle (1 call failed: q=b);" in text


# ============================================================ 5, 5b: replacement


def test_a_newer_chain_handle_replaces_the_older_which_answers_replaced(lab) -> None:
    """MF 5 (N2): the older id answers `replaced` naming the newer one to its
    owner; another session hears `unknown`. Base: no chain handle."""

    async def script(owner: Client, other: Client) -> Any:
        replies = await _chain_of(owner, lab, ["a", "b", "c", "d"])
        older, newer = _hint(replies[2])["chain"], _hint(replies[3])["chain"]
        calls = lab.upstream.total()
        gone = await owner.refused("filter", {"input": older})
        foreign = await other.refused("filter", {"input": older})
        foreign_new = await other.refused("filter", {"input": newer})
        page = await owner.ok("filter", {"input": newer})
        return older, newer, gone, foreign, foreign_new, page, calls

    older, newer, gone, foreign, foreign_new, page, calls = lab.sessions(2, script)
    assert older != newer
    assert gone["code"] == "result_reference_invalid"
    assert gone["data"]["reason"] == "replaced" and gone["data"]["replaced_by"] == newer
    assert "holds these answers and 1 more" in gone["message"]
    assert gone["data"]["retry_call"] == {"tool": "filter", "arguments": {"input": newer}}
    assert foreign["data"]["reason"] == "unknown" and "replaced_by" not in foreign["data"]
    assert foreign_new["data"]["reason"] == "unknown"
    assert len(page["items"]) == 4 and lab.upstream.total() == calls
    assert len(_chains()) == 1


@pytest.mark.parametrize("order", ["lower_commits_last", "lower_commits_first"])
def test_two_calls_in_one_turn_leave_the_higher_numbered_chain_live(lab, monkeypatch, order) -> None:
    """MF 5b (N1): two calls in flight together. The later-noted call counts
    the other's draft as kept; whichever reply commits last, the live chain
    handle is the higher-numbered one and the lower id answers `replaced`.
    Base: no chain handle."""
    import anyio

    from okg.substrate.mcp import live_run_hint

    go = threading.Event()
    if order == "lower_commits_last":
        # c is noted first (the lower number) and held until d's reply arrives.
        real = live_run_hint.note_live_call

        def held_note(*args: Any, **kwargs: Any) -> Any:
            out = real(*args, **kwargs)
            if args[3].get("q") == "c":
                assert go.wait(30), "d's reply never arrived"
            return out

        monkeypatch.setattr(live_run_hint, "note_live_call", held_note)
    else:
        # d's upstream call waits for c's reply: c is noted and committed first.
        lab.upstream.wait["d"] = go

    async def script(client: Client) -> Any:
        await _chain_of(client, lab, ["a", "b"])
        found: dict[str, Any] = {}

        async def one(q: str) -> None:
            found[q] = await client.ok("expand", lab.call(q))
            go.set()

        async with anyio.create_task_group() as group:
            group.start_soon(one, "c")
            await anyio.sleep(0.2)
            group.start_soon(one, "d")
        c_chain, d_chain = _hint(found["c"]), _hint(found["d"])
        lower, higher = (c_chain, d_chain)
        gone = await client.refused("filter", {"input": lower["chain"]})
        live = await client.ok("filter", {"input": higher["chain"]})
        return found, lower, higher, gone, live

    found, lower, higher, gone, live = lab.sessions(1, script)
    assert higher["answers"] == 4 and lower["answers"] == 3
    assert _kept(found["c"]) in [_store()._entries[higher["chain"]].payload.source_ids[i]  # noqa: SLF001
                                 for i in range(4)]
    assert gone["data"]["reason"] == "replaced" and gone["data"]["replaced_by"] == higher["chain"]
    assert len(live["items"]) == 4
    assert [e.id for e in _chains()] == [higher["chain"]]


# ============================================================ 6, 7, 13b: what never combines


def _plant(store: Any, source_ids: list[str], values: list[Any], *, varying: str = "q") -> str:
    """A chain handle the hint never mints, planted through the store."""
    from okg.substrate.mcp.live_handle_lists import LiveChainSet

    entry = store._entries[source_ids[0]]  # noqa: SLF001
    payload = LiveChainSet(
        chain_key=f"chain:planted-{uuid.uuid4().hex}", seq=10**9,
        source_ids=tuple(source_ids), relation="lrs_rows", node_id=entry.payload.node_id,
        varying=varying, values=tuple(values), shared={"size": 10},
    )
    draft = store.mint(
        reader_key=entry.reader_key, binding=dict(entry.binding), members=(),
        header_claim=None, multi_user=False, payload=payload,
        payload_bytes=payload.size(), payload_rows=0, quota_key=entry.quota_key,
    )
    return store.commit(draft).id


def test_two_answers_of_the_same_call_are_refused_same_call(lab) -> None:
    """MF 6 (H1): a planted chain over one repeated call is refused
    `same_call` at the second source. Base: no chain kind."""

    async def script(client: Client) -> Any:
        first = _kept(await client.ok("expand", lab.call("a")))
        again = _kept(await client.ok("expand", lab.call("a")))
        chain = _plant(_store(), [first, again], ["a", "a"])
        return await client.refused("aggregate", {
            "input": chain, "by": [], "metrics": ["count"]})

    error = lab.sessions(1, script)
    assert error["code"] == "live_handles_not_combinable"
    assert error["data"]["reason"] == "same_call" and error["data"]["index"] == 2
    assert len(error["data"]["handles"]) == 2


def test_overlapping_answers_are_counted_and_a_pattern_chain_mints_none(lab) -> None:
    """MF 7 (H2): rows shared by answers are counted and said, never dropped;
    a pattern-valued chain mints no handle. Base: no chain."""
    shared = {"name": "same", "size": 4}
    lab.upstream.rows.update({
        "a": [shared, {"name": "only-a", "size": 1}], "b": [shared], "c": [{"name": "c", "size": 2}],
    })

    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, ["a", "b", "c"])
        chain = _hint(replies[-1])["chain"]
        body = await client.ok("aggregate", {
            "input": chain, "by": [], "metrics": ["count", "sum(attrs.size)"]})
        text = await client.text("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        patterns = await _chain_of(client, lab, ["/A*", "/B*", "/C*"])
        return body, text, patterns

    body, text, patterns = lab.sessions(1, script)
    assert body["combined"]["rows_in_several_answers"] == 1
    assert body["groups"][0]["count"] == 4 and body["groups"][0]["sum(attrs.size)"] == 11
    assert "! 1 identical rows appear in more than one answer" in text
    assert all(_hint(reply) is None for reply in patterns)


def test_a_numeric_threshold_mints_no_chain_and_an_echoed_key_still_counts_as_shared(lab) -> None:
    """MF 13b (H2 residual): `min_size` 10, 20, 30 mints no chain; a row
    that echoes the varying key is still counted as shared."""
    lab.upstream.echo = True
    lab.upstream.rows.update({
        "x": [{"q": "x", "name": "same", "size": 3}],
        "y": [{"q": "y", "name": "same", "size": 3}],
        "z": [{"q": "z", "name": "z", "size": 1}],
    })

    async def script(client: Client) -> Any:
        thresholds = [
            await client.ok("expand", lab.call("t", relation="lrs_min", min_size=n))
            for n in (10, 20, 30)
        ]
        replies = await _chain_of(client, lab, ["x", "y", "z"])
        body = await client.ok("aggregate", {
            "input": _hint(replies[-1])["chain"], "by": [], "metrics": ["count"]})
        return thresholds, body

    thresholds, body = lab.sessions(1, script)
    assert all(_hint(reply) is None for reply in thresholds)
    assert body["combined"]["rows_in_several_answers"] == 1


# ============================================================ 8, 9, 10: re-checks


def test_an_expired_source_is_answered_over_and_named(lab) -> None:
    """MF 8: the read answers `over 29 of 30 answers; 1 no longer kept`,
    with the gone source's index and retry; the others are untouched; with
    every source gone, `no_source_kept`. Base: no chain."""
    values = [f"i{i:02d}" for i in range(30)]

    async def script(client: Client) -> Any:
        first = _kept(await client.ok("expand", lab.call(values[0])))
        lab.clock.now += 300
        replies = await _chain_of(client, lab, values[1:])
        chain = _hint(replies[-1])["chain"]
        assert _hint(replies[-1])["answers"] == 30
        lab.clock.now += 301
        body = await client.ok("aggregate", {
            "input": chain, "by": [], "metrics": ["count", "sum(attrs.size)"]})
        text = await client.text("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        kept = [_kept(r) for r in replies]
        for set_id in kept:
            _store().discard(set_id)
        empty = await client.refused("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        return first, body, text, empty

    first, body, text, empty = lab.sessions(1, script)
    combined = body["combined"]
    assert combined["answers"] == 29 and combined["of"] == 30
    (gone,) = combined["not_kept"]
    assert gone["index"] == 1 and gone["value"] == "i00" and gone["reason"] == "expired"
    assert gone["retry"]["arguments"]["live"]["args"]["q"] == "i00"
    assert body["groups"][0]["count"] == 29 and body["groups"][0]["sum(attrs.size)"] == 290
    assert "! over 29 of 30 answers; 1 no longer kept: q=i00" in text
    assert empty["code"] == "live_handles_not_combinable", empty
    assert empty.get("data", {}).get("reason") == "no_source_kept", empty
    assert empty["data"]["index"] == 0


def test_another_session_cannot_read_the_chain_handle(lab) -> None:
    """MF 9: another session gets `unknown`, zero calls; the owner reads it."""

    async def script(owner: Client, other: Client) -> Any:
        chain = _hint((await _chain_of(owner, lab, ["a", "b", "c"]))[-1])["chain"]
        calls = lab.upstream.total()
        foreign = await other.refused("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        mine = await owner.ok("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        return foreign, mine, calls

    foreign, mine, calls = lab.sessions(2, script)
    assert foreign["data"]["reason"] == "unknown"
    assert mine["groups"][0]["count"] == 3 and lab.upstream.total() == calls


def test_a_caller_who_lost_expand_hears_the_caller_refusal_on_a_chain_read(lab) -> None:
    """MF 10 (L6, #4002): a chain read asks expand's decision for every
    source; the refusal keeps its code, with the source position."""
    from tests.substrate.mcp.test_live_result_sets import _deny_expand_only

    async def script(client: Client) -> Any:
        chain = _hint((await _chain_of(client, lab, ["a", "b", "c"]))[-1])["chain"]
        lab.mcp._okg_dispatch_principal_gate = _deny_expand_only
        try:
            return await client.refused("aggregate", {
                "input": chain, "by": [], "metrics": ["count"]})
        finally:
            del lab.mcp._okg_dispatch_principal_gate

    error = lab.sessions(1, script)
    assert error["code"] == "nomos_identity_denied", error


# ============================================================ 11: decode and size


def test_the_combined_rows_are_reserved_in_the_decode_budget(lab, monkeypatch) -> None:
    """MF 11 (M9): the sources fit the per-call budget, sources plus the
    combined rows do not: `live_decode_capacity`, scope call. Over 8 MiB
    combined: `too_large`."""
    from okg.substrate.mcp import handles

    lab.upstream.rows.update({
        v: [{"name": f"{v}-{i}", "size": i} for i in range(200)] for v in ("a", "b", "c")
    })

    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, ["a", "b", "c"])
        chain = _hint(replies[-1])["chain"]
        sources = sum(
            len(_store()._entries[_kept(r)].payload.rows_json) for r in replies  # noqa: SLF001
        )
        monkeypatch.setattr(handles, "decode_budget", handles.DecodeBudget(
            per_call_bytes=sources + 64, per_process_bytes=10 * sources,
        ))
        decode = await client.refused("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        monkeypatch.setattr(handles, "decode_budget", handles.DecodeBudget())
        monkeypatch.setattr(live_result_sets, "MAX_SET_BYTES", sources)
        large = await client.refused("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        return decode, large

    decode, large = lab.sessions(1, script)
    assert decode["code"] == "live_decode_capacity" and decode["data"]["scope"] == "call"
    assert large["code"] == "live_handles_not_combinable"
    assert large["data"]["reason"] == "too_large"


# ============================================================ 12b, 12c: the store under a chain


def test_a_60_source_chain_read_three_times_keeps_every_source(lab) -> None:
    """MF 12b (N3): total, rank and a narrowing filter keep all 60 sources;
    the store grows only by the small handles the reads mint (no combined
    copy); at the count cap a new groups handle evicts an unpinned entry."""
    values = [f"s{i:02d}" for i in range(60)]
    for i, v in enumerate(values):
        lab.upstream.rows[v] = [{"name": f"{v}-{j}", "size": i, "pad": "x" * 900} for j in range(100)]
    # A cut reply has no room for the chain line: the last answer is small,
    # so its reply prints the chain over all 60.
    lab.upstream.rows[values[-1]] = [{"name": "last", "size": 59}]

    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, values)
        hint = _hint(replies[-1])
        assert hint["answers"] == 60
        chain = hint["chain"]
        kept = [_kept(r) for r in replies]
        before = _store().total_bytes()
        total = await client.ok("aggregate", _filled(hint["calls"][0]))
        rank = await client.ok("aggregate", _filled(hint["calls"][1]))
        narrowed = await client.ok("filter", {
            "input": chain, "where": {"attrs.size": {"op": "gt", "value": 58}}})
        after = _store().total_bytes()
        minted = [total["handle"]["id"], rank["handle"]["id"], narrowed["handle"]["id"]]
        added = sum(_store()._entries[i].canonical_bytes for i in minted)  # noqa: SLF001
        fourth = await client.ok("aggregate", {"input": chain, "by": ["attrs.q"], "metrics": ["count"]})
        return chain, kept, before, after, minted, added, total, fourth

    chain, kept, before, after, minted, added, total, fourth = lab.sessions(1, script)
    store = _store()
    entries = store._entries  # noqa: SLF001
    reader, quota = entries[chain].reader_key, entries[chain].quota_key
    assert all(store.held_for(i, reader_key=reader, quota_key=quota) == "committed" for i in kept)
    assert after - before == added and added < 200_000
    assert total["groups"][0]["sum(attrs.size)"] == 100 * sum(range(59)) + 59
    assert store.quota_count(quota) == 64
    # The fourth read's groups handle evicted the oldest unpinned entry.
    assert minted[0] not in entries and fourth["handle"]["id"] in entries


def test_a_pin_never_refuses_a_cut_reply_and_the_chain_names_the_evicted_source(lab, monkeypatch) -> None:
    """MF 12c (P1): with a 60-source chain filling its session's 32 MiB, a
    cut reply on another relation is kept and pages; the oldest pinned
    source is evicted instead, and the next chain read says `over 59 of 60
    answers; 1 no longer kept`, its total over the 59. Base: no pins (and
    no chain)."""
    from okg.substrate.mcp import handles

    monkeypatch.setattr(handles, "decode_budget", handles.DecodeBudget(
        per_call_bytes=256 * 1024 * 1024, per_process_bytes=512 * 1024 * 1024,
    ))
    monkeypatch.setattr(live_result_sets, "MAX_SET_BYTES", 64 * 1024 * 1024)
    values = [f"p{i:02d}" for i in range(60)]
    for i, v in enumerate(values):
        lab.upstream.rows[v] = [{"name": v, "size": i + 1, "pad": "x" * 560_000}]
    lab.upstream.rows[values[-1]] = [{"name": "last", "size": 60}]
    lab.upstream.rows["big"] = [{"name": f"o{j}", "pad": "y" * 1000} for j in range(1000)]

    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, values)
        chain = _hint(replies[-1])["chain"]
        cut = await client.ok("expand", {**lab.call("big", relation="lrs_other"), "max_bytes": 16_384})
        token = cut["external_live"]["next"]
        page = await client.ok("expand", {"next": token})
        text = await client.text("aggregate", {"input": chain, "by": [], "metrics": ["count"]})
        body = await client.ok("aggregate", {
            "input": chain, "by": [], "metrics": ["count", "sum(attrs.size)"]})
        return cut, page, text, body

    cut, page, text, body = lab.sessions(1, script)
    assert cut["external_live"]["result_reference"]["retained"] is True
    assert page["external_live"]["rows_returned"] >= 1
    assert "! over 59 of 60 answers; 1 no longer kept: q=p00" in text
    assert body["combined"]["not_kept"][0]["reason"] == "evicted"
    assert body["groups"][0]["count"] == 59
    assert body["groups"][0]["sum(attrs.size)"] == sum(range(2, 61))


# ============================================================ 13: room and pages


def test_a_hint_dropped_for_room_leaves_no_chain_handle(lab, monkeypatch) -> None:
    """MF 13 (L8): a chain line that does not fit is dropped and its chain
    handle released, never committed."""
    from okg.substrate.mcp import live_handle_lists

    real = live_handle_lists.chain_hint
    monkeypatch.setattr(
        live_handle_lists, "chain_hint",
        lambda chain_id, chain: {**real(chain_id, chain), "pad": "z" * 200_000},
    )

    async def script(client: Client) -> Any:
        return await _chain_of(client, lab, ["a", "b", "c"])

    replies = lab.sessions(1, script)
    assert all(_hint(reply) is None for reply in replies)
    assert _chains() == [] and _store().pending_count() == 0


def test_a_cut_page_of_a_chain_read_goes_on_on_the_chain_id(lab) -> None:
    """MF 13 (L8): `next` of a cut chain read names the chain id, which the
    earlier reply committed."""
    values = [f"n{i:02d}" for i in range(12)]

    async def script(client: Client) -> Any:
        chain = _hint((await _chain_of(client, lab, values))[-1])["chain"]
        page = await client.ok("filter", {"input": chain, "limit": 5})
        follow = await client.ok(page["next_call"]["tool"], page["next_call"]["arguments"])
        return chain, page, follow

    chain, page, follow = lab.sessions(1, script)
    assert page["next_call"]["arguments"]["input"] == {"ref": chain}
    assert page["handle"]["id"] == chain and len(follow["items"]) == 5


# ============================================================ 16: the q021 replay


#: 30 sizes between 10^13 and 10^17, fixed: their float sum is not exact.
SIZES = [
    10_000_000_000_037 * (k + 1) + 3_141_592_653 * k * k + 97 * k
    for k in range(30)
]
SIZES[-1] = 99_999_999_999_999_989


def test_the_q021_replay_totals_thirty_answers_exactly_from_the_hinted_call(lab) -> None:
    """MF 16: 30 direct calls; the total call the call-30 hint names, run
    as printed with `<field>` set to `size`, is the exact integer sum (no
    `sum_not_exact`), the rank call the 5 largest in order, and the fake
    system saw 30 calls. Base: no hint."""
    assert all(10**13 <= s < 10**17 for s in SIZES)
    assert float(sum(float(s) for s in SIZES)) != sum(SIZES)
    values = [f"/Fixture{k:02d}/RunA/KIND" for k in range(30)]
    lab.upstream.sizes.update(dict(zip(values, SIZES)))

    async def script(client: Client) -> Any:
        replies = await _chain_of(client, lab, values)
        hint = _hint(replies[-1])
        total = await client.ok("aggregate", _filled(hint["calls"][0]))
        rank = await client.ok("aggregate", _filled(hint["calls"][1]))
        return hint, total, rank

    hint, total, rank = lab.sessions(1, script)
    assert hint["answers"] == 30
    (group,) = total["groups"]
    assert group["sum(attrs.size)"] == sum(SIZES) and "sum_not_exact" not in total
    top = sorted(zip(SIZES, values), reverse=True)[:5]
    assert [g["max(attrs.size)"] for g in rank["groups"]] == [s for s, _v in top]
    assert lab.upstream.total() == 30


def test_an_enum_argument_mints_no_chain(lab) -> None:
    """MF review L7 (8.2): an enum is a switch, not an identifier; three
    calls differing only in `state` name no chain handle."""

    async def script(client: Client) -> Any:
        return [
            await client.ok("expand", lab.call("e", relation="lrs_state", state=s))
            for s in ("open", "closed", "any")
        ] + [
            await client.ok("expand", lab.call(q, relation="lrs_state", state="open"))
            for q in ("x", "y", "z")
        ]

    replies = lab.sessions(1, script)
    assert all(reply["external_live"]["state"] == "ok" for reply in replies)
    assert all(_hint(reply) is None for reply in replies[:3])
    # Positive control: the same relation varying its string `q` forms one.
    assert _hint(replies[-1])["answers"] == 4 and _hint(replies[-1])["argument"] == "q"


def test_a_chain_read_over_a_cut_answer_says_it_as_the_cut_change_does(lab) -> None:
    """MF (live-kept-answer-cut merged in): a chain whose second answer the
    external system cut is read as short: `upstream_cut` summed over the
    sources in calls, `total_count_basis` lower_bound, and the cover line's
    "at least" (count) and "over the rows received only" (sum) wording."""
    lab.upstream.cut.add("b")

    async def script(client: Client) -> Any:
        chain = _hint((await _chain_of(client, lab, ["a", "b", "c"]))[-1])["chain"]
        return await client.ok("aggregate", {
            "input": chain, "by": [], "metrics": ["count", "sum(attrs.size)"]})

    body = lab.sessions(1, script)
    assert body["upstream_cut"] == {"rows": 3, "calls_cut": 1, "calls": 3}
    assert body["total_count_basis"] == "lower_bound"
    assert body["combined"]["upstream_calls_cut"] == 1
    cover = body["totals_cover"]
    assert "the external system cut 1 of 3 calls" in cover
    assert "count is at least the value shown" in cover
    assert "sum(attrs.size) is over the rows received only" in cover
