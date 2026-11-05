"""`run` live steps, set steps and totals through the served tool (#3666, lane ri).

PostgreSQL tests on a small published graph whose `page` type declares four
fixture live relations, served through the `live_adapters` test seam:
`fx_list(pattern)` lists items, `fx_summary(item)` sizes one,
`fx_sites(expr)` lists sites and `fx_attrs(site)` their attributes. Every
call goes through the server's own `call_tool`, so a live step meets the
principal gate, the session's tool surface, the trace, the call log and
the call sample a call sent alone meets, and every assertion reads the
reply as a client receives it.

Gate 1: every call a reply or a served text suggests is executed here (the
tool description's examples, the instruction sentence's example, the H3
hint). Gate 2: an exact total from a replayed answer.
"""
from __future__ import annotations

import contextlib
import json
import pathlib
import threading
import time
import uuid
from typing import Any, Iterator

import pytest

from okg.substrate.external_backed.live_dispatch import LiveAdapterError, UpstreamCutRows
from okg.substrate.external_backed.live_relations import ANNOTATION_KEY, parse_live_relations
from okg.substrate.mcp import live_result_sets

pytestmark = pytest.mark.postgres

DATA = pathlib.Path(__file__).parent / "data"


def _relation(args: dict[str, Any], deadline: float = 40.0) -> dict[str, Any]:
    return {
        "action": "external.fixture.read", "principal_mode": "service",
        "sample_policy": "metadata", "retention_class": "warm",
        "deadline_seconds": deadline, "args": args,
    }


TEXT = {"type": "string", "required": True, "maxLength": 300}
RELATIONS = {
    "fx_list": _relation({"pattern": TEXT}),
    "fx_summary": _relation({
        "item": TEXT,
        "valid_only": {"type": "integer", "default": 1, "minimum": 0, "maximum": 1},
    }),
    "fx_sites": _relation({"expr": TEXT}),
    "fx_attrs": _relation({"site": TEXT}),
}


class Upstream:
    """The fixture system: answers by the argument each relation takes."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.calls: list[dict[str, Any]] = []
        self.listings: dict[str, list[str]] = {}
        self.sizes: dict[str, int] = {}
        self.behaviour: dict[str, str] = {}
        self.sites: dict[str, list[dict[str, Any]]] = {}
        self.attrs: dict[str, dict[str, Any]] = {}
        self.delay: dict[str, float] = {}
        self.entered = threading.Event()
        self.release: threading.Event | None = None

    def __call__(self, *, config, args, node_id, deadline_seconds):
        with self.lock:
            self.calls.append(dict(args))
        if "pattern" in args:
            time.sleep(self.delay.get("fx_list", 0.0))
            return [{"item": name} for name in self.listings[args["pattern"]]]
        if "item" in args:
            self.entered.set()
            if self.release is not None:
                self.release.wait(60)
            time.sleep(self.delay.get("fx_summary", 0.0))
            name = args["item"]
            how = self.behaviour.get(name)
            if how == "empty":
                raise LiveAdapterError("upstream_empty_result")
            if how == "not_found":
                raise LiveAdapterError("upstream_not_found")
            if how == "cut":
                return UpstreamCutRows([{"item": name, "file_size": 1}], received=1)
            return [{"item": name, "file_size": self.sizes.get(name, 10), "num_file": 3}]
        if "expr" in args:
            return [dict(row) for row in self.sites[args["expr"]]]
        if "site" in args:
            return [{"site": args["site"], **self.attrs.get(args["site"], {})}]
        raise AssertionError(args)

    def made(self, key: str) -> list[dict[str, Any]]:
        with self.lock:
            return [call for call in self.calls if key in call]


class CountingGate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def __call__(self, relation, principal, node_id):
        from okg.substrate.external_backed.live_dispatch import GateOutcome

        self.calls.append((relation.name, principal, node_id))
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
        "name": "default", "description": "run live steps fixture",
        "postgres": {"dsn": "${OKG_LIVEBIND_DSN}"},
        "schema_dir": "schemas", "source_registry": "source_registry.yaml",
        "modules": [], "enrichers": [],
        "agent_surface": {
            "served": ["inspect", "search", "expand", "filter", "map", "aggregate", "run"],
            "live_bindings": bindings,
        },
    }, sort_keys=False), encoding="utf-8")


class Lab:
    def __init__(self, mcp: Any, upstream: Upstream, hub: str, dsn: str) -> None:
        self.mcp, self.upstream, self.hub, self.dsn = mcp, upstream, hub, dsn

    def call(self, tool: str, arguments: dict[str, Any], fmt: str = "json") -> Any:
        import anyio

        from okg.substrate.mcp.result_text import result_format_scope

        async def drive() -> Any:
            with result_format_scope(fmt):
                return await self.mcp.call_tool(tool, arguments)

        return anyio.run(drive)

    def json(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        result = self.call(tool, arguments)
        body = json.loads(result.content[0].text)
        assert result.isError is False, body
        return body

    def text(self, tool: str, arguments: dict[str, Any]) -> str:
        result = self.call(tool, arguments, "text")
        text = result.content[0].text
        assert result.isError is False, text[:2000]
        return text

    def run(self, steps: list[Any], **extra: Any) -> dict[str, Any]:
        return self.json("run", {"steps": steps, **extra})

    def session(self, *requests: tuple[str, dict[str, Any], str]) -> list[Any]:
        """Each ``(tool, arguments, format)`` through ONE client session, so
        the calls have a stable logical MCP session (``call`` has none)."""
        import anyio
        from mcp.shared.memory import create_connected_server_and_client_session

        out: list[Any] = []

        async def drive() -> None:
            async with create_connected_server_and_client_session(self.mcp) as client:
                for tool, arguments, fmt in requests:
                    out.append(await client.call_tool(
                        tool, arguments, meta={"okg/result_format": fmt},
                    ))

        anyio.run(drive)
        for result in out:
            assert result.isError is False, result.content[0].text[:2000]
        return out

    def anchor(self) -> dict[str, str]:
        return {"node_id": self.hub}

    def listing(self, pattern: str, show: bool = False) -> dict[str, Any]:
        return {"expand": {"input": self.anchor(), "live": {
            "relation": "fx_list", "args": {"pattern": pattern}}}, "show": show}

    def sizes(self, source: Any, show: bool = False) -> dict[str, Any]:
        return {"expand": {"input": self.anchor(), "live": {
            "relation": "fx_summary", "args": {"valid_only": 1},
            "each": {"item": source}}}, "show": show}

    def samples(self, relation: str) -> list[dict[str, Any]]:
        import psycopg

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT status, decision, attrs FROM okg.call_sample_warm "
                "WHERE relation = %s ORDER BY observed_at, sample_id", (relation,),
            ).fetchall()
        return [{"status": r[0], "decision": r[1], "attrs": r[2]} for r in rows]


@contextlib.contextmanager
def _lab(dsn: str, tmp_path, monkeypatch) -> Iterator[Lab]:
    from okg.substrate.mcp.server import build_server

    hub = _publish(dsn)
    _deployment(tmp_path)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_LIVEBIND_DSN", dsn)
    upstream = Upstream()

    def runner(**kwargs: Any) -> Any:
        # A plain function, as the built-in runners are (a callable object
        # hid a prepared call that bound it as a method).
        return upstream(**kwargs)

    mcp = build_server(dsn=dsn, live_adapters={"http": runner},
                       auto_advance="always", deployment_name="default")
    mcp._okg_live_gate = CountingGate()  # type: ignore[attr-defined]
    try:
        yield Lab(mcp, upstream, hub, dsn)
    finally:
        for attr in ("_okg_pool", "_okg_write_pool"):
            pool = getattr(mcp, attr, None)
            if pool is not None:
                pool.close()


@pytest.fixture
def lab(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Lab]:
    fresh = live_result_sets.new_store()
    previous = live_result_sets.install_live_store(fresh)
    try:
        with _lab(clean_runtime, tmp_path, monkeypatch) as built:
            yield built
    finally:
        live_result_sets.install_live_store(previous)


def _step(reply: dict[str, Any], number: int) -> dict[str, Any]:
    step = reply["steps"][number - 1]
    assert step["status"] == "ok", step
    return step


def _unfence(value: Any) -> Any:
    if isinstance(value, str) and "\n" in value:
        return value.split("\n")[1]
    return value


# ============================================================ gate 2: replays


def test_a_replayed_listing_is_one_run_with_the_exact_total(lab) -> None:
    """A listing of 30 items totalling 2,100,435,002,090,900
    bytes. The replayed answer is the fixture system's data; one
    run lists, sizes each item and totals, exactly."""
    recorded = json.loads((DATA / "run_replay_listing.json").read_text())
    names = [row["item"] for row in recorded["rows"]]
    lab.upstream.listings[recorded["pattern"]] = names
    lab.upstream.sizes = {row["item"]: row["bytes"] for row in recorded["rows"]}
    steps = [
        lab.listing(recorded["pattern"]),
        lab.sizes("$1.item"),
        {"aggregate": {"input": "$2", "by": [],
                       "metrics": ["count", "sum(file_size)", "sum(num_file)"]}},
    ]
    reply = lab.run(steps)
    assert "partial" not in reply, json.dumps(reply["steps"][:2])[:3000]
    live = _step(reply, 2)
    assert live["calls"]["ok"] == 30 and live["rows"] == 30
    (group,) = _step(reply, 3)["result"]["groups"]
    assert group["count"] == recorded["items"] == 30
    assert group["sum(attrs.file_size)"] == recorded["total_bytes"] == 2_100_435_002_090_900
    assert len(lab.upstream.made("pattern")) == 1
    assert len(lab.upstream.made("item")) == 30
    text = lab.text("run", {"steps": steps})
    assert text.splitlines()[0] == "run ok · 3 steps"
    assert "2100435002090900" in text
    assert "! totals cover: 30 rows of step 2" in text


def test_the_three_call_chain_is_one_run_with_its_replayed_total(lab) -> None:
    """A three-call chain: a listing kept, a
    fan-out over the kept answer, a sum over the fan-out: three calls, and
    an agent who must know to chain them. One run gives the same 142 rows
    and 1,216,783,012,389,891 bytes, with the listing's own 142 names."""
    recorded = json.loads((DATA / "run_replay_chain.json").read_text())
    lab.upstream.listings[recorded["pattern"]] = recorded["listing"]
    lab.upstream.sizes = {row["item"]: row["file_size"] for row in recorded["rows"]}
    reply = lab.run([
        lab.listing(recorded["pattern"]),
        lab.sizes("$1.item"),
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count", "sum(file_size)"]}},
    ])
    assert "partial" not in reply
    assert _step(reply, 2)["rows"] == 142
    (group,) = _step(reply, 3)["result"]["groups"]
    assert group == {"count": 142, "sum(attrs.file_size)": 1_216_783_012_389_891}
    assert group["sum(attrs.file_size)"] == recorded["total_bytes"]


# ============================================================ gate 1: served examples


def _example(text: str, prefix: str, fills: dict[str, str]) -> Any:
    """The JSON example starting at `prefix` in a served text, with its
    placeholders filled."""
    assert prefix in text, (prefix, text)
    for placeholder, value in fills.items():
        text = text.replace(placeholder, value)
        prefix = prefix.replace(placeholder, value)
    start = text.index(prefix)
    obj, _end = json.JSONDecoder().raw_decode(text, start)
    return obj


def test_every_example_in_the_run_description_executes(lab) -> None:
    import anyio

    tools = {t.name: t for t in anyio.run(lab.mcp.list_tools)}
    description = tools["run"].description
    fills = {"<id>": lab.hub, "<name>": "fx_summary", "<arg>": "item"}
    live = _example(description, '{"expand"', {**fills, "<column>": "item"})
    total = _example(description, '{"aggregate"', {**fills, "<column>": "file_size"})
    minus = _example(description, '{"minus"', {**fills, "<column>": "item"})
    lab.upstream.listings["/A*"] = ["/A1", "/A2", "/A3"]
    reply = lab.run([
        lab.listing("/A*"),
        live,
        {**total, "show": True},
        {"values": {"item": ["/A2"]}},
        {"minus": ["$1", "$4"], "on": minus["on"]},
    ])
    assert reply["refused_steps"] == 0 and reply["skipped_steps"] == 0
    (group,) = _step(reply, 3)["result"]["groups"]
    assert group["count"] == 3 and group["sum(attrs.file_size)"] == 30
    assert [_unfence(r["item"]) for r in _step(reply, 5)["result"]["rows"]] == ["/A1", "/A3"]
    # The minus example as written: step 1's rows against step 2's.
    reply = lab.run([lab.listing("/A*"), lab.sizes("$1.item"), minus])
    assert _step(reply, 3)["rows"] == 0


def test_the_instruction_sentence_example_executes(lab) -> None:
    instructions = lab.mcp.instructions or ""
    sentence = 'In run, an expand live step with "each": {"<arg>": "$1.<column>"}'
    assert sentence in instructions
    each = _example(instructions, '{"<arg>"', {"<arg>": "item", "<column>": "item"})
    lab.upstream.listings["/B*"] = ["/B1", "/B2"]
    reply = lab.run([
        lab.listing("/B*"),
        {"expand": {"input": lab.anchor(), "live": {"relation": "fx_summary", "each": each}}},
    ])
    assert _step(reply, 2)["calls"]["ok"] == 2


def _summary_call(lab: Lab, item: str) -> dict[str, Any]:
    return {"input": lab.anchor(), "live": {
        "relation": "fx_summary", "args": {"item": item}}}


def _external_live(result: Any) -> dict[str, Any]:
    return json.loads(result.content[0].text)["external_live"]


def test_h3_names_the_each_form_built_from_the_agents_own_calls_and_it_runs(lab) -> None:
    first, second, third, fourth = lab.session(
        ("expand", _summary_call(lab, "/H1"), "json"),
        ("expand", _summary_call(lab, "/H2"), "json"),
        ("expand", _summary_call(lab, "/H3"), "json"),
        ("expand", _summary_call(lab, "/H4"), "text"),
    )
    assert "run_hint" not in _external_live(first)
    assert "run_hint" not in _external_live(second)
    hint = _external_live(third)["run_hint"]
    assert hint["argument"] == "item" and hint["values"] == 3
    call = hint["call"]
    assert call["tool"] == "run"
    text = fourth.content[0].text
    assert "calls differ only in item; one run makes them all" in text
    before = len(lab.upstream.made("item"))
    reply = lab.run(**call["arguments"])
    live = _step(reply, 1)
    assert live["calls"]["ok"] == 3
    assert len(lab.upstream.made("item")) == before + 3


def test_h3_never_names_another_sessions_values_and_needs_a_session(lab) -> None:
    """FAILS BEFORE (review H2): the hint memory was keyed on the constant
    "mcp", so session B's first call was hinted with session A's values,
    and calls with no stable session were hinted too."""
    a_calls = lab.session(
        ("expand", _summary_call(lab, "/A1"), "json"),
        ("expand", _summary_call(lab, "/A2"), "json"),
    )
    b_calls = lab.session(
        ("expand", _summary_call(lab, "/B1"), "json"),
        ("expand", _summary_call(lab, "/B2"), "json"),
        ("expand", _summary_call(lab, "/B3"), "json"),
    )
    assert all("run_hint" not in _external_live(r) for r in a_calls)
    assert "run_hint" not in _external_live(b_calls[0])
    assert "run_hint" not in _external_live(b_calls[1])
    hint = _external_live(b_calls[2])["run_hint"]
    (step,) = hint["call"]["arguments"]["steps"]
    assert step["expand"]["live"]["each"] == {"item": ["/B1", "/B2", "/B3"]}
    for result in b_calls:
        assert "/A1" not in result.content[0].text and "/A2" not in result.content[0].text
    # No stable MCP session (stateless HTTP; a call outside a session): no
    # memory, so no hint, however many calls.
    for item in ("/N1", "/N2", "/N3", "/N4"):
        assert "run_hint" not in lab.json("expand", _summary_call(lab, item))["external_live"]


# ============================================================ checks a call sent alone meets


def test_a_principal_denied_expand_makes_no_upstream_call_inside_run(lab) -> None:
    from okg.substrate.mcp.principal_mapping import unidentified_principal_error

    asked: list[str] = []

    def gate(name: str) -> None:
        asked.append(name)
        if name == "expand":
            raise unidentified_principal_error(
                "test: this principal may not call expand", channel="no_subject_binding",
            )

    lab.mcp._okg_dispatch_principal_gate = gate  # type: ignore[attr-defined]
    lab.upstream.listings["/P*"] = ["/P1"]
    result = lab.call("run", {"steps": [lab.listing("/P*"), lab.sizes("$1.item", True)]})
    body = json.loads(result.content[0].text)
    assert body["steps"][0]["status"] == "refused"
    assert body["steps"][1]["status"] == "skipped"
    assert "expand" in asked and "run" in asked
    assert lab.upstream.calls == []  # the spy: no adapter call at all
    assert lab.samples("fx_list") == []


def test_under_access_rules_run_is_refused_before_any_live_step(lab) -> None:
    lab.mcp._okg_run_backend.refused_reason = lambda: "rules on"  # type: ignore[attr-defined]
    lab.upstream.listings["/R*"] = ["/R1"]
    result = lab.call("run", {"steps": [lab.listing("/R*", True)]})
    assert result.isError is True
    assert "run_refused_under_access_rules" in result.content[0].text
    assert lab.upstream.calls == []


def test_the_operator_switch_turns_live_steps_off_and_leaves_direct_calls(lab) -> None:
    lab.mcp._okg_run_live_steps = False  # type: ignore[attr-defined]
    lab.upstream.listings["/S*"] = ["/S1"]
    reply = lab.json("run", {"steps": [lab.listing("/S*", True)]})
    step = reply["steps"][0]
    assert step["status"] == "refused"
    assert step["result"]["error"]["code"] == "run_live_steps_off"
    assert lab.upstream.calls == []
    direct = lab.json("expand", {"input": lab.anchor(), "live": {
        "relation": "fx_list", "args": {"pattern": "/S*"}}})
    assert direct["external_live"]["state"] == "ok"


def test_a_live_step_writes_its_sample_trace_and_call_log(lab) -> None:
    import psycopg

    lab.upstream.listings["/T*"] = ["/T1", "/T2"]
    lab.run([lab.listing("/T*"), lab.sizes("$1.item", True)])
    (sample,) = lab.samples("fx_summary")
    assert sample["status"] == "ok" and sample["decision"] == "allow"
    attrs = sample["attrs"]
    assert attrs["served_to"] == "run" and attrs["adapter_ran"] is True
    assert attrs["run_outcomes"]["ok"] == 2 and attrs["run_rows"] == 2
    with psycopg.connect(lab.dsn, autocommit=True) as conn:
        logged = conn.execute(
            "SELECT count(*) FROM okg.mcp_call_log WHERE tool_name = 'expand' "
            "AND args::text LIKE %s", ("%fx_summary%",),
        ).fetchone()[0]
        traced = conn.execute(
            "SELECT count(*) FROM okg.mcp_trace_events WHERE tool_name = 'expand' "
            "AND request_json::text LIKE %s", ("%fx_summary%",),
        ).fetchone()[0]
    assert logged == 1 and traced == 1


def _until(predicate: Any, seconds: float = 20.0) -> Any:
    """Poll `predicate` on the loop until it is true, or fail."""
    import anyio

    async def wait() -> None:
        with anyio.fail_after(seconds):
            while not await anyio.to_thread.run_sync(predicate):
                await anyio.sleep(0.05)

    return wait()


def test_two_sessions_of_one_reader_fan_out_at_once_and_a_third_is_refused(lab) -> None:
    """FAILS BEFORE (review M3): one fan-out run per reader was keyed on the
    reader alone, so on a single-user server a second agent's run was
    refused run_live_busy. Now: one per session, two per reader."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    lab.upstream.release = threading.Event()
    replies: dict[str, Any] = {}

    async def fan_out(tag: str) -> None:
        async with create_connected_server_and_client_session(lab.mcp) as client:
            replies[tag] = await client.call_tool("run", {"steps": [
                {"values": {"item": [f"/{tag}1", f"/{tag}2"]}},
                lab.sizes("$1.item", True),
            ]}, meta={"okg/result_format": "json"})

    def calling(tag: str) -> bool:
        return any(c["item"].startswith(f"/{tag}") for c in lab.upstream.made("item"))

    async def drive() -> None:
        try:
            async with anyio.create_task_group() as group:
                group.start_soon(fan_out, "A")
                group.start_soon(fan_out, "B")
                await _until(lambda: calling("A") and calling("B"))
                # A and B hold the reader's two fan-out runs.
                await fan_out("C")
                lab.upstream.release.set()
        finally:
            lab.upstream.release.set()

    anyio.run(drive)
    body = {tag: json.loads(result.content[0].text) for tag, result in replies.items()}
    for tag in ("A", "B"):
        assert _step(body[tag], 2)["calls"]["ok"] == 2, body[tag]
    refused = body["C"]["steps"][1]
    assert refused["status"] == "refused"
    assert refused["result"]["error"]["code"] == "run_live_busy"
    assert not calling("C")
    # The claims are given back when the runs answer.
    assert _step(lab.run([{"values": {"item": ["/D1"]}}, lab.sizes("$1.item")]), 2)


def test_a_single_call_that_failed_is_refused_and_the_minus_reading_it_skipped(lab) -> None:
    """FAILS BEFORE (review M7): a failed listing was an ok step with 0 rows,
    so `minus` kept every row it should have dropped."""
    lab.upstream.sites = {"retired=True": [{"site": "A_HOT"}, {"site": "B_COLD"}]}

    def listing(expr: str, show: bool = False) -> dict[str, Any]:
        return {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_sites", "args": {"expr": expr}}}, "show": show}

    # No answer is set for "kind=COLD": that call fails upstream.
    reply = lab.run([
        listing("retired=True"), listing("kind=COLD"),
        {"minus": ["$1", "$2"], "on": "site", "show": True},
    ])
    assert _step(reply, 1)["rows"] == 2
    failed = reply["steps"][1]
    assert failed["status"] == "refused", failed
    assert failed["result"]["error"]["code"] == "run_live_step_refused"
    assert "gave no answer" in json.dumps(failed["result"])
    assert reply["steps"][2]["status"] == "skipped"
    # The call was made, so its sample is written.
    assert [s["attrs"]["run_outcomes"]["failed"] for s in lab.samples("fx_sites")] == [0, 1]


def test_a_cancelled_run_still_writes_the_call_sample_of_its_live_step(lab) -> None:
    """FAILS BEFORE (review M1): the client's cancel dropped the step's
    result, so calls made upstream had no call sample."""
    import anyio

    from okg.substrate.mcp.result_text import result_format_scope

    lab.upstream.release = threading.Event()

    async def call() -> None:
        with result_format_scope("json"):
            await lab.mcp.call_tool("run", {"steps": [
                {"values": {"item": ["/Z1", "/Z2", "/Z3"]}},
                lab.sizes("$1.item", True),
            ]})

    async def drive() -> None:
        try:
            async with anyio.create_task_group() as group:
                group.start_soon(call)
                await _until(lab.upstream.entered.is_set)
                group.cancel_scope.cancel()  # the client went away
            # The loop runs on, as a server's does.
            await _until(lambda: bool(lab.samples("fx_summary")))
        finally:
            lab.upstream.release.set()

    anyio.run(drive)
    (sample,) = lab.samples("fx_summary")
    assert sample["attrs"]["adapter_ran"] is True
    assert sample["attrs"]["run_stopped"] == "cancelled"
    assert sample["attrs"]["run_calls_started"] >= 1


# ============================================================ values, outcomes, partial


def test_an_empty_answer_is_counted_empty_and_the_run_stays_whole(lab) -> None:
    lab.upstream.behaviour = {"/E2": "empty", "/E3": "not_found"}
    reply = lab.run([
        {"values": {"item": ["/E1", "/E2", "/E3"]}},
        lab.sizes("$1.item"),
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count"]}},
    ])
    assert "partial" not in reply, json.dumps(reply["steps"][:2])[:3000]
    calls = _step(reply, 2)["calls"]
    assert (calls["ok"], calls["empty"], calls["not_found"]) == (1, 1, 1)
    assert _step(reply, 3)["result"]["groups"][0]["count"] == 1


def test_a_value_cut_by_the_systems_own_limit_makes_the_step_partial(lab) -> None:
    lab.upstream.behaviour = {"/C2": "cut"}
    steps = [
        {"values": {"item": ["/C1", "/C2"]}},
        lab.sizes("$1.item"),
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count"]}},
    ]
    reply = lab.run(steps)
    assert reply["partial"] is True
    live = _step(reply, 2)
    assert live["calls"]["cut"] == 1 and live["partial"] is True
    assert live["missing"][0]["value"] == "/C2" and live["missing"][0]["outcome"] == "cut"
    text = lab.text("run", {"steps": steps})
    assert text.splitlines()[0] == "run partial · 3 steps · 1 live values not read"
    assert "/C2 (cut by the system's own limit" in text
    assert "! PARTIAL: step 2 did not read every value" in text


def test_the_upstream_call_count_is_flat_once_the_run_answers_at_its_deadline(
    lab, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_RUN_WALL_SECONDS", "3")
    lab.upstream.delay["fx_summary"] = 0.5
    values = [f"/W{i}" for i in range(200)]
    started = time.monotonic()
    reply = lab.run([
        {"values": {"item": values}},
        lab.sizes("$1.item"),
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count"]}},
    ])
    assert time.monotonic() - started < 3 + 3
    live = _step(reply, 2)
    assert reply["partial"] is True and live["calls"]["not_started"] > 100
    assert live["stopped"] == "deadline"
    time.sleep(1.0)
    made = len(lab.upstream.made("item"))
    assert made == live["calls_started"]
    time.sleep(2.0)
    assert len(lab.upstream.made("item")) == made


def test_a_graph_step_after_a_30_second_live_step_still_runs(lab) -> None:
    """FAILS BEFORE: the run's 15 s budget was one wall clock, so every
    graph step after a long live step was refused over budget."""
    lab.upstream.listings["/G*"] = ["/G1"]
    lab.upstream.delay["fx_list"] = 30.0
    reply = lab.run([
        lab.listing("/G*"),
        {"filter": {"where": {"type": "page"}}, "show": True},
    ])
    assert _step(reply, 1)["rows"] == 1
    assert _step(reply, 2)["count"] == 2


def test_four_kept_answers_held_then_a_five_live_step_run_keeps_every_row(lab) -> None:
    """FAILS BEFORE: live steps went through the kept-answer store (4 a
    reader), so a fifth answer was not kept and its rows were cut to the
    reply's room. A run's rows are its own."""
    names = [f"/K{i:03d}" for i in range(120)]
    for pattern in ("/k1*", "/k2*", "/k3*", "/k4*", "/k5*"):
        lab.upstream.listings[pattern] = names
    for pattern in ("/k1*", "/k2*", "/k3*", "/k4*"):
        kept = lab.json("expand", {"input": lab.anchor(), "live": {
            "relation": "fx_list", "args": {"pattern": pattern}, "retain": True}})
        assert kept["external_live"]["result_reference"]["retained"] is True
    store = live_result_sets.live_store()
    assert store.count() == 4
    reply = lab.run([
        lab.listing("/k1*"), lab.listing("/k2*"), lab.listing("/k3*"),
        lab.listing("/k4*"), lab.listing("/k5*"),
        {"intersect": ["$1", "$2", "$3", "$4", "$5"], "on": "item"},
        {"aggregate": {"input": "$6", "by": [], "metrics": ["count"]}},
    ])
    for number in range(1, 6):
        assert _step(reply, number)["rows"] == 120
    assert _step(reply, 7)["result"]["groups"][0]["count"] == 120
    assert store.count() == 4


def test_a_fan_out_holds_no_call_worker_while_a_graph_call_runs_at_width_1(lab) -> None:
    lab.mcp._okg_tool_call_width = 1  # type: ignore[attr-defined]
    lab.upstream.release = threading.Event()
    replies: list[Any] = []
    runner = threading.Thread(target=lambda: replies.append(lab.run([
        {"values": {"item": ["/X1", "/X2"]}}, lab.sizes("$1.item", True),
    ])))
    runner.start()
    try:
        assert lab.upstream.entered.wait(30), "the fan-out never called"
        graph: list[Any] = []
        reader = threading.Thread(target=lambda: graph.append(
            lab.json("filter", {"where": {"type": "page"}}),
        ))
        reader.start()
        reader.join(30)
        assert not reader.is_alive(), "the graph call waited for the fan-out's worker"
        assert graph and graph[0]["total_count"] == 2
    finally:
        lab.upstream.release.set()
        runner.join(60)
    assert _step(replies[0], 2)["calls"]["ok"] == 2


# ============================================================ sets and totals (wide and narrow shapes)


def test_minus_on_a_column_matches_the_systems_own_expression(lab) -> None:
    """Sites with retired=True, not COLD, not tier 1 or 0, not
    Z2_AA_ALPHA; the system's single expression answers the same set."""
    every = [{"site": f"Z{tier}_R{i}", "kind": "COLD" if i % 5 == 0 else "HOT",
              "tier": tier} for tier in (0, 1, 2) for i in range(12)]
    every.append({"site": "Z2_AA_ALPHA", "kind": "HOT", "tier": 2})
    lab.upstream.sites = {
        "retired=True": every,
        "kind=COLD": [r for r in every if r["kind"] == "COLD"],
        "tier=1": [r for r in every if r["tier"] == 1],
        "tier=0": [r for r in every if r["tier"] == 0],
    }

    def listing(expr: str) -> dict[str, Any]:
        return {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_sites", "args": {"expr": expr}}}, "show": False}

    reply = lab.run([
        listing("retired=True"), listing("kind=COLD"), listing("tier=1"), listing("tier=0"),
        {"values": {"site": ["Z2_AA_ALPHA"]}},
        {"minus": ["$1", "$2", "$3", "$4", "$5"], "on": "site", "show": True},
    ])
    expected = sorted(
        r["site"] for r in every
        if r["kind"] != "COLD" and r["tier"] not in (0, 1) and r["site"] != "Z2_AA_ALPHA"
    )
    got = sorted(_unfence(row["site"]) for row in _step(reply, 6)["result"]["rows"])
    assert got == expected and _step(reply, 6)["rows"] == len(expected)


def test_filter_over_live_rows_equals_step_1_minus_the_excluded(lab) -> None:
    names = ["Z1_A", "Z2_B", "Z2_B_Test", "Z3_C", "Z2_D_Temp", "Z0_E"]
    lab.upstream.sites = {"oidc_support=True": [{"site": n} for n in names]}
    reply = lab.run([
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_sites", "args": {"expr": "oidc_support=True"}}}},
        {"filter": {"input": "$1", "where": {"op": "and", "clauses": [
            {"op": "not", "clause": {"field": "site", "op": "suffix", "value": "_Test"}},
            {"op": "not", "clause": {"field": "site", "op": "suffix", "value": "_Temp"}},
            {"op": "not", "clause": {"field": "site", "op": "prefix", "value": "Z3_"}},
        ]}}},
    ])
    kept = [_unfence(r["site"]) for r in _step(reply, 2)["result"]["rows"]]
    assert kept == ["Z1_A", "Z2_B", "Z0_E"]
    assert _step(reply, 2)["from_rows"] == 6


def test_keep_aggregate_shape_a_missing_attribute_is_the_absent_group(lab) -> None:
    sites = [f"R{i}" for i in range(40)]
    lab.upstream.sites = {"*": [{"site": r} for r in sites]}
    lab.upstream.attrs = {
        **{r: {"keep_flag": True} for r in sites[:27]},
        **{r: {"keep_flag": False} for r in sites[27:30]},
    }
    steps = [
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_sites", "args": {"expr": "*"}}}},
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_attrs", "each": {"site": "$1.site"}}}},
        {"aggregate": {"input": "$2", "by": ["keep_flag"], "metrics": ["count"]}},
    ]
    reply = lab.run(steps)
    groups = {
        json.dumps(_unfence(g["attrs.keep_flag"])): g["count"]
        for g in _step(reply, 3)["result"]["groups"]
    }
    assert groups == {"true": 27, "false": 3, '"(absent)"': 10}
    assert _step(reply, 3)["absent_keys"] == {"attrs.keep_flag": 10}
    text = lab.text("run", {"steps": steps})
    assert "! 10 rows have no keep_flag: they are the group (absent)" in text


def test_a_column_no_row_carries_is_refused_naming_the_columns_there(lab) -> None:
    lab.upstream.listings["/U*"] = ["/U1"]
    reply = lab.run([
        lab.listing("/U*"),
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_summary", "each": {"item": "$1.items"}}}},
    ])
    step = reply["steps"][1]
    assert step["status"] == "refused"
    error = step["result"]["error"]
    assert error["code"] == "run_live_step_refused"
    assert "did you mean item?" in error["fix_hint"]
    assert len(lab.upstream.made("item")) == 0


def test_a_graph_condition_naming_a_live_step_is_refused_with_the_input_form(lab) -> None:
    lab.upstream.listings["/V*"] = ["/V1"]
    reply = lab.run([lab.listing("/V*"), {"filter": {"where": "$1"}}])
    step = reply["steps"][1]
    assert step["status"] == "refused"
    assert '"input": "$1"' in step["result"]["error"]["fix_hint"]


def test_no_step_block_says_a_pin_scope_the_caller_did_not_set(lab) -> None:
    """Item 8: each step was read at the generation the run pinned, so the
    step's own `pin scope: explicit_call` line read as the caller's pin."""
    text = lab.text("run", {"steps": [
        {"filter": {"where": {"type": "page"}}, "show": True},
        {"aggregate": {"where": "$1", "by": ["subtype"], "metrics": ["count"]}},
    ]})
    assert "pin scope" not in text


# ============================================================ argument shapes models send (lane re)


def _kept_listing(lab: Lab, pattern: str, names: list[str]) -> str:
    """A listing called alone, as the model did before its run: its id."""
    lab.upstream.listings[pattern] = names
    reply = lab.json("expand", {"input": lab.anchor(), "live": {
        "relation": "fx_list", "args": {"pattern": pattern}}})
    return reply["external_live"]["result_reference"]["id"]


def test_a_max_bytes_under_the_floor_is_raised_and_said(lab) -> None:
    """FAILS BEFORE: refused invalid_response_byte_budget (3 + 4 calls in
    lane re's runs); raised to 4,096 and said, as every other read tool
    does (#3153). Taken from lane ri's read-run-argument-shapes."""
    lab.upstream.listings["/F*"] = ["/F1", "/F2"]
    steps = [lab.listing("/F*"), lab.sizes("$1.item", True)]
    reply = lab.run(steps, max_bytes=2000)
    assert reply["max_bytes_raised"] == {"requested": 2000, "applied": 4096}
    assert _step(reply, 2)["calls"]["ok"] == 2
    text = lab.text("run", {"steps": steps, "max_bytes": 1000})
    assert text.splitlines()[1] == "! max_bytes 1000 raised to 4096 (the minimum)"
    # A value over the ceiling is still refused, never lowered.
    result = lab.call("run", {"steps": steps, "max_bytes": 10_000_000})
    assert result.isError is True and "invalid_response_byte_budget" in result.content[0].text


def test_arguments_that_stay_the_same_inside_each_are_read_as_args(lab) -> None:
    """FAILS BEFORE: `live.each names one argument and its values` (GC
    g002 t1 #15). Taken from lane ri's read-run-argument-shapes."""
    each = {"item": ["/E1", "/E2"], "valid_only": [0]}
    step = {"expand": {"input": lab.anchor(), "live": {
        "relation": "fx_summary", "each": each}}, "show": True}
    reply = lab.run([step])
    live = _step(reply, 1)
    assert live["calls"]["ok"] == 2 and live["query"] == {"valid_only": 0}
    assert "adjusted: each.valid_only [0] read as args.valid_only 0" in live["notes"]
    assert live["adjusted"] == [{
        "rule": "each_constant_as_args", "sent": "each.valid_only [0]",
        "served": "args.valid_only 0",
    }]
    assert all(call.get("valid_only") == 0 for call in lab.upstream.made("item"))
    ambiguous = {"expand": {"input": lab.anchor(), "live": {
        "relation": "fx_summary", "each": {"item": ["/E1", "/E2"], "valid_only": [0, 1]}}}}
    result = lab.call("run", {"steps": [ambiguous]})
    assert result.isError is True
    error = json.loads(result.content[0].text)["error"]
    assert "names the one argument whose values vary" in error["message"]
    # Two varying lists: no list is offered as one argument (review M2).
    assert error["fix_hint"] == (
        'keep one varying argument in each, e.g. "each": {"item": ["/E1", "/E2"]}, '
        "and send the values of valid_only as separate steps or runs"
    )
    assert '"args"' not in error["fix_hint"]
    written = {"expand": {"input": lab.anchor(), "live": {
        "relation": "fx_summary", "args": {"valid_only": 1},
        "each": {"item": ["/E1", "/E2"]}}}, "show": True}
    assert _step(lab.run([written]), 1)["calls"]["ok"] == 2


def test_retain_in_a_run_live_step_is_set_aside_with_one_note(lab) -> None:
    """FAILS BEFORE: `live does not take retain in a run` (G7 c003 t3 #28,
    GC g002 t2 #8): `expand` advertises retain and models copy it."""
    lab.upstream.listings["/R*"] = ["/R1", "/R2"]
    lab.upstream.sizes = {"/R1": 5, "/R2": 7}
    steps = [
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_list", "args": {"pattern": "/R*"}, "retain": True}}},
        {"expand": {"input": lab.anchor(), "retain": True, "live": {
            "relation": "fx_summary", "each": {"item": "$1.item"}}}},
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count", "sum(file_size)"]}},
    ]
    reply = lab.run(steps)
    assert reply["refused_steps"] == 0
    for number in (1, 2):
        assert _step(reply, number)["notes"] == [
            "retain ignored: a run keeps a live step's rows for its later steps",
        ]
    (group,) = _step(reply, 3)["result"]["groups"]
    assert group == {"count": 2, "sum(attrs.file_size)": 12}
    text = lab.text("run", {"steps": steps})
    assert "! retain ignored: a run keeps a live step's rows for its later steps" in text
    # A run keeps nothing past its answer: retain minted no handle.
    assert "l1." not in text


def test_each_reads_the_column_of_a_kept_answer(lab) -> None:
    """FAILS BEFORE: `live.each names a kept answer; in a run, name the step
    that lists the values` (GC q021 t2 #4); the model pasted 30 literal
    names instead. Read with or without `attrs.`, as expand reads it alone."""
    names = [f"/E{i}" for i in range(30)]
    kept = _kept_listing(lab, "/E*", names)
    lab.upstream.sizes = {name: 1_000 + i for i, name in enumerate(names)}
    for field in ("attrs.item", "item"):
        before = len(lab.upstream.made("item"))
        reply = lab.run([
            {"expand": {"input": lab.anchor(), "live": {
                "relation": "fx_summary", "each": {"item": {"ref": kept, "field": field}}}}},
            {"aggregate": {"input": "$1", "by": [], "metrics": ["count", "sum(file_size)"]}},
        ])
        assert reply["refused_steps"] == 0, json.dumps(reply)[:3000]
        assert _step(reply, 1)["calls"]["ok"] == 30
        (group,) = _step(reply, 2)["result"]["groups"]
        assert group == {"count": 30, "sum(attrs.file_size)": sum(lab.upstream.sizes.values())}
        assert len(lab.upstream.made("item")) - before == 30
    text = lab.text("run", {"steps": [
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_summary", "each": {"item": {"ref": kept, "field": "item"}}}}},
    ]})
    assert "each item <- kept answer's item" in text


def test_a_kept_answers_values_meet_the_run_fan_out_limit(lab, monkeypatch) -> None:
    from okg.substrate.mcp import run_steps

    kept = _kept_listing(lab, "/F*", [f"/F{i}" for i in range(4)])
    monkeypatch.setattr(run_steps, "MAX_FAN_OUT_VALUES", 3)
    reply = lab.run([{"expand": {"input": lab.anchor(), "live": {
        "relation": "fx_summary", "each": {"item": {"ref": kept, "field": "item"}}}}}])
    error = reply["steps"][0]["result"]["error"]
    assert error["code"] == "run_live_step_refused"
    assert "4 distinct values of item; a live step calls with at most 3" in error["message"]
    assert lab.upstream.made("item") == []


def test_a_filter_over_a_kept_answer_feeds_a_later_step(lab) -> None:
    """FAILS BEFORE: step 1 read the kept answer, and step 2's
    `$1.attrs.dataset` was refused: `names step 1, whose records are a
    graph set` (GC q021 t2 #5)."""
    names = [f"/D{i}" for i in range(6)] + ["/X0"]
    kept = _kept_listing(lab, "/D*", names)
    lab.upstream.sizes = {name: 100 * (i + 1) for i, name in enumerate(names)}
    reply = lab.run([
        {"filter": {"input": {"ref": kept},
                    "where": {"field": "attrs.item", "op": "prefix", "value": "/D"}},
         "show": False},
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_summary", "each": {"item": "$1.attrs.item"}}}},
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count", "sum(file_size)"]}},
    ])
    assert reply["refused_steps"] == 0, json.dumps(reply)[:3000]
    assert _step(reply, 2)["calls"]["ok"] == 6
    (group,) = _step(reply, 3)["result"]["groups"]
    assert group == {"count": 6, "sum(attrs.file_size)": 100 + 200 + 300 + 400 + 500 + 600}
    assert [call["item"] for call in lab.upstream.made("item")] == names[:6]


def test_an_aggregate_over_a_kept_answer_is_its_alone_call_and_its_groups_are_rows(lab) -> None:
    """The step answers as the same aggregate sent alone; a later step reads
    its groups as rows (by the bare column, as a run names columns)."""
    names = ["/G1", "/G2", "/G3"]
    kept = _kept_listing(lab, "/G*", names)
    alone = lab.json("aggregate", {
        "input": {"ref": kept}, "by": ["attrs.item"], "metrics": ["count"]})
    reply = lab.run([
        {"aggregate": {"input": kept, "by": ["attrs.item"], "metrics": ["count"]},
         "show": True},
        {"filter": {"input": "$1", "where": {"item": "/G2"}}},
    ])
    assert reply["refused_steps"] == 0, json.dumps(reply)[:3000]
    served = _step(reply, 1)["result"]
    assert [g["count"] for g in served["groups"]] == [g["count"] for g in alone["groups"]]
    assert served["total_count"] == alone["total_count"] == 3
    later = _step(reply, 2)
    assert later["rows"] == 1 and later["from_rows"] == 3
    assert [_unfence(row["item"]) for row in later["result"]["rows"]] == ["/G2"]


def test_a_kept_answer_feeds_a_run_in_the_client_session_that_kept_it(lab) -> None:
    """A kept answer is bound to the reader and its logical MCP session: the
    run's own read of the step's rows is made in that same session (the
    eval's clients hold one), not answered as an id never issued."""
    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    lab.upstream.listings["/S*"] = ["/S1", "/S2", "/T1"]
    lab.upstream.sizes = {"/S1": 3, "/S2": 4, "/T1": 50}
    replies: list[Any] = []

    async def drive() -> None:
        async with create_connected_server_and_client_session(lab.mcp) as client:
            meta = {"okg/result_format": "json"}
            kept = await client.call_tool("expand", {"input": lab.anchor(), "live": {
                "relation": "fx_list", "args": {"pattern": "/S*"}}}, meta=meta)
            ref = json.loads(kept.content[0].text)["external_live"]["result_reference"]["id"]
            replies.append(await client.call_tool("run", {"steps": [
                {"filter": {"input": ref, "where": {
                    "field": "attrs.item", "op": "prefix", "value": "/S"}}},
                {"expand": {"input": lab.anchor(), "live": {
                    "relation": "fx_summary", "each": {"item": "$1.item"}}}},
                {"aggregate": {"input": "$2", "by": [], "metrics": ["sum(file_size)"]}},
            ]}, meta=meta))
            # Another client session cannot read it, in or out of a run:
            # as a step's input, or as each's values (review L3).
        async with create_connected_server_and_client_session(lab.mcp) as other:
            meta = {"okg/result_format": "json"}
            replies.append(await other.call_tool("run", {"steps": [
                {"filter": {"input": ref}},
                {"aggregate": {"input": "$1", "by": [], "metrics": ["count"]}},
            ]}, meta=meta))
            replies.append(await other.call_tool("run", {"steps": [
                {"expand": {"input": lab.anchor(), "live": {
                    "relation": "fx_summary", "each": {"item": {"ref": ref, "field": "item"}}}}},
            ]}, meta=meta))

    made = len(lab.upstream.made("item"))
    anyio.run(drive)
    mine = json.loads(replies[0].content[0].text)
    assert replies[0].isError is False and mine["refused_steps"] == 0, json.dumps(mine)[:3000]
    (group,) = _step(mine, 3)["result"]["groups"]
    assert group == {"sum(attrs.file_size)": 7}
    assert len(lab.upstream.made("item")) - made == 2, "only the owner's run called"
    theirs = json.loads(replies[1].content[0].text)
    assert theirs["steps"][1]["status"] == "skipped"
    for refused in (theirs["steps"][0], json.loads(replies[2].content[0].text)["steps"][0]):
        assert refused["status"] == "refused", json.dumps(refused)[:2000]
        # The dead-handle answer: exactly as an id never issued.
        error = refused["result"]["error"]
        assert error["code"] == "result_reference_invalid", error
        assert error["data"]["reason"] == "unknown", error


def test_an_ordered_aggregate_over_a_kept_answer_gives_later_steps_its_rank(lab) -> None:
    """Review L2: as over a run's own rows, an ordered aggregate's groups
    carry `rank`, so a later step can keep the top groups."""
    kept = _kept_listing(lab, "/O*", ["/O1", "/O2", "/O3"])
    reply = lab.run([
        {"aggregate": {"input": kept, "by": ["attrs.item"], "metrics": ["count"],
                       "order_by": "attrs.item desc"}},
        {"filter": {"input": "$1", "where": {"field": "rank", "op": "lte", "value": 2}}},
    ])
    assert reply["refused_steps"] == 0, json.dumps(reply)[:3000]
    later = _step(reply, 2)
    assert later["rows"] == 2 and later["from_rows"] == 3
    assert [(row["rank"], _unfence(row["item"])) for row in later["result"]["rows"]] == [
        (1, "/O3"), (2, "/O2"),
    ]


def test_kept_groups_with_numeric_keys_read_in_run_as_the_kept_answer_reads(lab) -> None:
    """Lane ri's merge review (read-stored-metrics, #4239): numbers group by
    value now (1 and 1.0 are one group) and a stored number no float holds
    is an ExactNumber. Over a kept groups answer, a run's filter `=`,
    order_by and aggregate read the group keys as the same aggregate sent
    alone serves them, and print them as it does (1, not 1.0).
    A live answer cannot carry an ExactNumber into a kept groups answer: an
    answer holding a number JSON cannot round-trip is not kept at all, so no
    run step can receive one from this path."""
    from decimal import Decimal

    lab.upstream.sites = {
        "*": [{"site": "A", "load": 1}, {"site": "B", "load": 1.0},
              {"site": "C", "load": 0.1}, {"site": "D", "load": 2.5},
              {"site": "E", "load": 0.1}],
        "exact": [{"site": "X", "load": Decimal("0.1000000000000000055")}],
    }
    listed = lab.json("expand", {"input": lab.anchor(), "live": {
        "relation": "fx_sites", "args": {"expr": "*"}}})
    kept = listed["external_live"]["result_reference"]["id"]
    grouped = {"input": kept, "by": ["attrs.load"], "metrics": ["count"],
               "order_by": "attrs.load desc"}
    alone = lab.json("aggregate", grouped)
    expected = [(g["rank"], g["attrs.load"], g["count"]) for g in alone["groups"]]
    assert expected == [(1, 2.5, 1), (2, 1, 2), (3, 0.1, 2)]

    steps = [
        {"aggregate": grouped, "show": False},
        {"filter": {"input": "$1", "where": {"load": 1}}, "show": True},
        {"aggregate": {"input": "$1", "by": ["load"], "metrics": ["sum(count)"],
                       "order_by": "load asc"}, "show": True},
        {"aggregate": {"input": "$1", "by": [], "metrics": [
            "sum(count)", "max(load)", "min(load)"]}, "show": True},
    ]
    reply = lab.run(steps)
    assert reply["refused_steps"] == 0, json.dumps(reply)[:3000]
    (one,) = _step(reply, 2)["result"]["rows"]
    assert (one["rank"], _unfence(one["load"]), one["count"]) == expected[1]
    ordered = _step(reply, 3)["result"]["groups"]
    assert [(_unfence(g["attrs.load"]), g["sum(attrs.count)"]) for g in ordered] == [
        (0.1, 2), (1, 2), (2.5, 1),
    ]
    (total,) = _step(reply, 4)["result"]["groups"]
    assert total == {"sum(attrs.count)": 5, "max(attrs.load)": 2.5, "min(attrs.load)": 0.1}
    text = lab.text("run", {"steps": steps})
    printed = [line for line in text.splitlines() if line.startswith("[")]
    assert "[1] load 1 · count 2 · rank 2" in printed, text
    assert not any(" 1.0 " in f"{line} " for line in printed), text

    exact = lab.json("expand", {"input": lab.anchor(), "live": {
        "relation": "fx_sites", "args": {"expr": "exact"}}})
    assert exact["external_live"]["result_reference"] == {
        "retained": False, "reason": "rows_not_objects",
    }


def test_a_kept_step_no_later_step_reads_holds_no_rows(lab) -> None:
    kept = _kept_listing(lab, "/H*", ["/H1"])
    reply = lab.run([{"aggregate": {"input": {"ref": kept}, "by": [], "metrics": ["count"]}}])
    assert _step(reply, 1)["result"]["groups"][0]["count"] == 1


def test_input_naming_a_graph_set_is_read_as_where(lab) -> None:
    """FAILS BEFORE: `input "$1" names a step whose records are a graph
    set; a graph set is a condition` (7 errored calls in one batch)."""
    first = {"filter": {"where": {"type": "page"}}, "show": False}
    as_input = lab.run([first, {"aggregate": {
        "input": "$1", "by": ["subtype"], "metrics": ["count"]}}])
    as_where = lab.run([first, {"aggregate": {
        "where": "$1", "by": ["subtype"], "metrics": ["count"]}}])
    assert as_input["refused_steps"] == 0
    assert _step(as_input, 2)["result"]["groups"] == _step(as_where, 2)["result"]["groups"]
    assert _step(as_input, 2)["count"] == _step(as_where, 2)["count"] == 2
    text = lab.text("run", {"steps": [first, {"filter": {"input": "$1"}}]})
    assert '! adjusted: "input": "$1" read as "where": "$1"' in text


# ============================================================ the switch off teaches nothing


@pytest.fixture
def lab_off(clean_runtime: str, tmp_path, monkeypatch) -> Iterator[Lab]:
    """A server started with `OKG_MCP_RUN_LIVE_STEPS=0`: the three places
    that teach a live step (the run description, the card sentence, the H3
    hint) must be silent, not teach a step the server then refuses."""
    monkeypatch.setenv("OKG_MCP_RUN_LIVE_STEPS", "0")
    fresh = live_result_sets.new_store()
    previous = live_result_sets.install_live_store(fresh)
    try:
        with _lab(clean_runtime, tmp_path, monkeypatch) as built:
            yield built
    finally:
        live_result_sets.install_live_store(previous)


def test_switch_off_serves_no_live_step_teaching_and_no_run_hint(lab_off) -> None:
    import anyio

    lab = lab_off
    assert lab.mcp._okg_run_live_steps is False  # type: ignore[attr-defined]
    tools = {t.name: t for t in anyio.run(lab.mcp.list_tools)}
    description = tools["run"].description
    assert description.startswith("Up to 8 reads in one call")
    assert "A live step" not in description and '"live"' not in description
    assert '"each"' not in description
    instructions = lab.mcp.instructions or ""
    assert "In run, an expand live step" not in instructions
    assert '"each"' not in instructions
    # Direct live calls still work; the third, which the switch-on test above
    # hints, carries no hint (one client session, so the memory has a key).
    calls = lab.session(*(
        ("expand", _summary_call(lab, item), "json")
        for item in ("/O1", "/O2", "/O3")
    ))
    for result in calls:
        assert _external_live(result)["state"] == "ok"
        assert "run_hint" not in _external_live(result)


# ============================================================ compute over held rows (PACT read-run-rows-compute)


def _sized(lab: Lab, sizes: dict[str, int]) -> list[dict[str, Any]]:
    """Steps 1-2: a listing of `sizes`' items, then one size call per item."""
    lab.upstream.listings["/M*"] = list(sizes)
    lab.upstream.sizes = dict(sizes)
    return [lab.listing("/M*"), lab.sizes("$1.item")]


def _graph_reads(lab: Lab, monkeypatch) -> list[str]:
    """Every composed graph read the run's backend makes, by method name."""
    backend = lab.mcp._okg_run_backend  # type: ignore[attr-defined]
    made: list[str] = []
    for name in ("current_generation", "step_set", "ids_set", "values_sql",
                 "materialize", "count", "join_far", "join_facts"):
        original = getattr(backend, name)

        def spy(*args: Any, _name: str = name, _original: Any = original, **kwargs: Any) -> Any:
            made.append(_name)
            return _original(*args, **kwargs)

        monkeypatch.setattr(backend, name, spy)
    return made


def test_map_compute_over_a_live_steps_rows_feeds_later_steps(lab, monkeypatch) -> None:
    """FAILS ON THE BASE ("map over the rows of step 2 does not take
    compute"). Computed columns over the rows step 2 holds, read by a
    filter, an aggregate (by, sum, order_by) and a live step's `each`; the
    compute step itself makes no upstream call and no graph read."""
    steps = _sized(lab, {"/M1": 3000, "/M2": 9000, "/M3": 1500, "/M4": 6000})
    steps += [
        {"map": {"input": "$2", "compute": {
            "per_file": "file_size / num_file", "big": "file_size >= 6000",
            "again": "concat(item, '')",
        }}},
        {"filter": {"input": "$3", "where": {"calc.big": True}}},
        {"aggregate": {"input": "$3", "by": ["calc.big"],
                       "metrics": ["count", "sum(calc.per_file)"], "order_by": "count desc"}},
        {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_summary", "each": {"item": "$4.calc.again"}}}, "show": True},
    ]
    reads = _graph_reads(lab, monkeypatch)
    reply = lab.run([*steps[:2], {**steps[2], "show": True}])
    mapped = _step(reply, 3)
    assert mapped["rows"] == 4 and mapped["from_rows"] == 4
    assert mapped["computed"]["calc.per_file"]["states"] == {"ok": 4}
    assert sorted(r["calc.per_file"] for r in mapped["result"]["rows"]) == [500, 1000, 2000, 3000]
    upstream_before = len(lab.upstream.calls)
    reads_before = list(reads)
    reply = lab.run([*steps[:4], {**steps[4], "show": True}])
    # Steps 1-2 call upstream (1 listing + 4 sizes); step 3 neither calls
    # nor reads the graph.
    assert len(lab.upstream.calls) - upstream_before == 5
    assert reads == reads_before + ["current_generation"]
    kept = _step(reply, 4)
    assert kept["rows"] == 2 and kept["from_rows"] == 4
    groups = {g["calc.big"]: g for g in _step(reply, 5)["result"]["groups"]}
    assert groups[True]["count"] == 2 and groups[True]["sum(calc.per_file)"] == 5000
    assert groups[False]["count"] == 2 and groups[False]["sum(calc.per_file)"] == 1500
    reply = lab.run(steps)
    fan = _step(reply, 6)
    assert fan["calls"]["ok"] == 2 and fan["rows"] == 2
    assert sorted(_unfence(r["item"]) for r in fan["result"]["rows"]) == ["/M2", "/M4"]
    text = lab.text("run", {"steps": [*steps[:2], {**steps[2], "show": True}]})
    assert "[step 3] map over $2: 4 rows · computed calc.per_file, calc.big, calc.again" in text
    assert "! calc.per_file: 4 ok" in text
    assert '"calc.per_file" 1000' in text


def test_map_compute_over_values_and_an_aggregates_groups(lab) -> None:
    """FAILS ON THE BASE. A values step's rows, and an aggregate step's
    groups (a metric by its label, as over a groups handle)."""
    reply = lab.run([
        {"values": {"n": [1, 2, 3]}},
        {"map": {"input": "$1", "compute": {"tens": "n * 10"}}, "show": True},
    ])
    assert [r["calc.tens"] for r in _step(reply, 2)["result"]["rows"]] == [10, 20, 30]
    steps = _sized(lab, {"/M1": 3000, "/M2": 9000})
    reply = lab.run([
        *steps,
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count", "sum(file_size)"]}},
        {"map": {"input": "$3", "compute": {
            "mean": "sum(attrs.file_size) / count"}}, "show": True},
    ])
    (row,) = _step(reply, 4)["result"]["rows"]
    assert row["calc.mean"] == 6000 and row["count"] == 2


def test_a_computed_error_row_is_set_aside_by_later_steps(lab) -> None:
    """FAILS ON THE BASE. A division by zero is that row's state, counted;
    a later filter never matches it (not even under not), an aggregate
    leaves it out of every group and says so."""
    steps = _sized(lab, {"/M1": 3000, "/M2": 0, "/M3": 1500})
    steps += [
        {"map": {"input": "$2", "compute": {"inv": "1000 / file_size"}}},
        {"filter": {"input": "$3", "where": {"op": "not", "clause": {
            "op": "gt", "field": "calc.inv", "value": 0.5}}}, "show": True},
        {"aggregate": {"input": "$3", "by": [], "metrics": ["count", "min(calc.inv)"]},
         "show": True},
    ]
    reply = lab.run([*steps[:2], {**steps[2], "show": True}])
    states = _step(reply, 3)["computed"]["calc.inv"]
    assert states["states"] == {"ok": 2, "error:division_by_zero": 1}
    reply = lab.run(steps)
    negated = _step(reply, 4)
    assert negated["rows"] == 1 and negated["not_computed"]["count"] == 1
    total = _step(reply, 5)
    assert total["result"]["groups"][0]["count"] == 2
    assert total["not_computed"]["count"] == 1
    text = lab.text("run", {"steps": steps})
    assert "! calc.inv: 2 ok · 1 error:division_by_zero" in text
    assert "1 rows have no value in a computed column this step needs" in text


def test_compute_over_held_rows_keeps_its_bounds(lab, monkeypatch) -> None:
    """FAILS ON THE BASE. The row bound (exact count in the refusal) and
    the evaluation clock are map(compute)'s own; a refused compute step
    skips the step that reads it."""
    from okg.substrate.mcp import map_compute, map_expressions

    monkeypatch.setattr(map_compute, "MAX_ROWS", 2)
    reply = lab.run([
        {"values": {"n": [1, 2, 3]}},
        {"map": {"input": "$1", "compute": {"x": "n + 1"}}},
        {"aggregate": {"input": "$2", "by": [], "metrics": ["count"]}},
    ])
    refused = reply["steps"][1]
    assert refused["status"] == "refused", refused
    error = refused["result"]["error"]
    assert error["data"]["reason"] == "too_many_rows" and error["data"]["rows"] == 3
    assert reply["steps"][2]["status"] == "skipped"
    monkeypatch.setattr(map_compute, "MAX_ROWS", 50_000)
    monkeypatch.setattr(map_expressions, "EVAL_SECONDS", -1.0)
    reply = lab.run([
        {"values": {"n": [1, 2]}},
        {"map": {"input": "$1", "compute": {"x": "n + 1"}}},
    ])
    assert reply["steps"][1]["result"]["error"]["data"]["reason"] == "evaluation_budget"


def test_compute_over_held_rows_refuses_what_map_compute_refuses(lab) -> None:
    """FAILS ON THE BASE (every refusal was "does not take compute"). The
    expression module's own refusals, by name, before anything is computed."""
    cases = [
        ({"n": "n"}, "column_shadows_field"),
        ({"count": "n"}, "reserved_column_name"),
        ({"x": "node_id"}, "record_column_on_raw_rows"),
        ({"x": "attrs.nope + 1"}, "key_absent_on_every_row"),
        ({"x": "n +"}, None),
    ]
    for compute, reason in cases:
        reply = lab.run([
            {"values": {"n": [1, 2]}},
            {"map": {"input": "$1", "compute": compute}},
        ])
        step = reply["steps"][1]
        assert step["status"] == "refused", (compute, step)
        error = step["result"]["error"]
        assert "does not take" not in error["message"], error
        if reason is not None:
            assert error["data"]["reason"] == reason, (compute, error)
    reply = lab.run([
        {"values": {"n": [1]}},
        {"map": {"input": "$1", "compute": {"x": "n"}}},
        {"filter": {"input": "$2", "where": {"calc.y": 1}}},
    ])
    assert reply["steps"][2]["result"]["error"]["data"]["reason"] == "calc_column_unknown"


def test_compute_in_run_under_access_rules_computes_nothing(lab, monkeypatch) -> None:
    """Regression and FAILS ON THE BASE for the second half: under access
    rules the run is refused before any step (as today); and a compute
    step is refused by map(compute)'s own rule whenever the rules say on,
    even if the run's own check let it through."""
    lab.mcp._okg_run_backend.refused_reason = lambda: "rules on"  # type: ignore[attr-defined]
    result = lab.call("run", {"steps": [
        {"values": {"n": [1]}}, {"map": {"input": "$1", "compute": {"x": "n"}}},
    ]})
    assert result.isError is True
    assert "run_refused_under_access_rules" in result.content[0].text
    lab.mcp._okg_run_backend.refused_reason = lambda: None  # type: ignore[attr-defined]
    from okg.substrate.mcp import live_access_rules

    asked_on: list[str] = []

    def rules_on(server: Any) -> bool:
        asked_on.append(threading.current_thread().name)
        return True

    monkeypatch.setattr(live_access_rules, "access_rules_state", rules_on)
    reply = lab.run([
        {"values": {"n": [1]}}, {"map": {"input": "$1", "compute": {"x": "n"}}},
    ])
    step = reply["steps"][1]
    assert step["status"] == "refused"
    assert step["result"]["error"]["data"]["reason"] == "compute_under_access_rules"
    # Review M1: the rule reads the deployment's Nomos mode, so it is asked
    # on a call worker, never on the event loop (here the main thread).
    assert asked_on and threading.main_thread().name not in asked_on, asked_on


def test_what_compute_in_run_does_not_change(lab) -> None:
    """Regression: map over held rows without compute still takes select
    and keeps only those columns; a graph step's set is still refused
    compute (stored sets wait for stored handles); no step prints a handle."""
    reply = lab.run([
        {"values": {"n": [1, 2]}},
        {"map": {"input": "$1"}},
    ])
    assert "takes select" in reply["steps"][1]["result"]["error"]["message"]
    reply = lab.run([
        {"values": {"n": [1, 2]}},
        {"map": {"input": "$1", "select": ["n"]}, "show": True},
    ])
    assert _step(reply, 2)["result"]["rows"] == [{"n": 1}, {"n": 2}]
    reply = lab.run([
        {"filter": {"where": {"type": "page"}}},
        {"map": {"where": "$1", "compute": {"x": "attrs.text"}}},
    ])
    error = reply["steps"][1]["result"]["error"]
    assert error["data"]["reason"] == "compute_on_stored_set", error
    text = lab.text("run", {"steps": [
        {"values": {"n": [1]}}, {"map": {"input": "$1", "compute": {"x": "n"}}},
    ]})
    assert "handle" not in text and "l1." not in text


def _grouped(lab: Lab) -> list[dict[str, Any]]:
    """Steps 1-3: four items sized, then grouped by item (one group each)."""
    return [*_sized(lab, {"/M1": 3000, "/M2": 9000, "/M3": 1500, "/M4": 6000}),
            {"aggregate": {"input": "$2", "by": ["item"], "metrics": ["count", "sum(file_size)"]}}]


def test_text_formulas_over_an_aggregate_steps_group_keys_read_the_raw_keys(lab) -> None:
    """FAILS ON THE BASE (refused) AND ON ff8adbc2d (review H1: the held
    groups kept their keys fenced, so len gave 67, starts_with false, and a
    later filter on the result matched 0 of 4 rows, every state ok). The
    printed rows stay fenced as untrusted text."""
    steps = [*_grouped(lab), {"map": {"input": "$3", "compute": {
        "n": "len(attrs.item)", "s": "starts_with(attrs.item, '/M')", "l": "lower(attrs.item)",
        "mean": "sum(attrs.file_size) / count"}}}]
    reply = lab.run([*steps[:3], {**steps[3], "show": True}])
    rows = sorted(_step(reply, 4)["result"]["rows"], key=lambda r: _unfence(r["calc.l"]))
    assert [r["calc.n"] for r in rows] == [3, 3, 3, 3]
    assert [r["calc.s"] for r in rows] == [True, True, True, True]
    assert [_unfence(r["calc.l"]) for r in rows] == ["/m1", "/m2", "/m3", "/m4"]
    assert [r["calc.mean"] for r in rows] == [3000, 9000, 1500, 6000]
    assert _step(reply, 4)["computed"]["calc.n"]["states"] == {"ok": 4}
    kept = lab.run([*steps, {"filter": {"input": "$4", "where": {"calc.s": True}}, "show": True}])
    assert _step(kept, 5)["rows"] == 4
    text = lab.text("run", {"steps": [*steps[:3], {**steps[3], "show": True}]})
    printed = text.split("[step 4]")[1]
    assert "UNTRUSTED_DATA" in printed and "/m1" in printed
    # The aggregate step's own groups print fenced, as before.
    grouped = lab.text("run", {"steps": [*steps[:2], {**steps[2], "show": True}]})
    assert "UNTRUSTED_DATA" in grouped.split("[step 3]")[1]


def test_a_filter_on_an_aggregate_steps_text_group_key_matches(lab) -> None:
    """FAILS ON THE BASE (dev 3d646f6e535 too: the held groups' keys were
    fenced text, so a filter on a by column matched 0 rows with no note,
    and a live step's each sent the fenced text upstream). Raw keys: the
    filter matches, by the bare name or as printed, and each sends the
    value itself."""
    steps = _grouped(lab)
    for where in ({"attrs.item": "/M2"}, {"item": "/M2"}):
        reply = lab.run([*steps, {"filter": {"input": "$3", "where": where}, "show": True}])
        assert _step(reply, 4)["rows"] == 1, (where, _step(reply, 4))
    before = len(lab.upstream.made("item"))
    reply = lab.run([*steps, {"filter": {"input": "$3", "where": {"item": "/M2"}}},
                     {"expand": {"input": lab.anchor(), "live": {
                         "relation": "fx_summary", "each": {"item": "$4.item"}}}, "show": True}])
    assert _step(reply, 5)["calls"]["ok"] == 1
    assert [c["item"] for c in lab.upstream.made("item")[before:]][-1] == "/M2"


def test_an_upstream_key_shaped_like_a_computed_column_is_refused(lab) -> None:
    """Regression on the base; FAILS ON ff8adbc2d (review L1: a filter on
    calc.fake over rows whose upstream key is NUL + "calc.fake" matched 0
    rows with no note). Refused by name before any row is read."""
    lab.upstream.sites = {"*": [{"site": "S1"}, {"site": "S2"}]}
    lab.upstream.attrs = {"S1": {"\x00calc.fake": 5}, "S2": {"\x00calc.fake": 5}}
    reply = lab.run([
        {"expand": {"input": lab.anchor(), "live": {"relation": "fx_sites", "args": {"expr": "*"}}}},
        {"expand": {"input": lab.anchor(), "live": {"relation": "fx_attrs", "each": {"site": "$1.site"}}}},
        {"filter": {"input": "$2", "where": {"calc.fake": 5}}, "show": True},
    ])
    step = reply["steps"][2]
    assert step["status"] == "refused", step


def test_an_each_value_not_read_is_named_fenced(lab) -> None:
    """FAILS ON ed8ae58889 AND ON DEV for live rows (review r2 M-A): a value
    `each` read from upstream rows that its call did not read was named
    unfenced in the step's note and in JSON `missing[].value`, so a planted
    fence closer reached the reply verbatim. Group keys and live rows alike."""
    from okg.substrate.mcp.result_text import FENCE_CLOSE

    planted = f"/P{FENCE_CLOSE}injected"
    lab.upstream.listings["/Q*"] = ["/Q1", planted]
    lab.upstream.behaviour = {planted: "cut"}
    for steps in (
        [lab.listing("/Q*"), lab.sizes("$1.item", True)],
        [lab.listing("/Q*"),
         {"aggregate": {"input": "$1", "by": ["item"], "metrics": ["count"]}},
         lab.sizes("$2.item", True)],
    ):
        reply = lab.run(steps)
        live = _step(reply, len(steps))
        (missed,) = live["missing"]
        assert missed["value"] != planted and "injected" in missed["value"], missed
        assert all(planted not in note for note in live["notes"]), live["notes"]
        text = lab.text("run", {"steps": steps})
        assert planted not in text and "injected" in text


def test_a_group_key_named_like_a_metric_or_rank_is_refused_to_later_steps(lab) -> None:
    """FAILS ON ed8ae58889 (review r2 M-B: a `by` column named `count` was
    overwritten by the count metric, so a filter on it matched 0 rows and a
    formula on it was refused as unknown, silently). The aggregate step
    still prints its groups; a later step that reads them is refused by
    name, never read as the other column."""
    lab.upstream.sites = {"*": [
        {"site": "A", "count": "x", "rank": "r1"}, {"site": "B", "count": "x", "rank": "r1"},
        {"site": "C", "count": "y", "rank": "r2"},
    ]}
    listing = {"expand": {"input": lab.anchor(), "live": {"relation": "fx_sites", "args": {"expr": "*"}}}}
    for grouped in (
        {"aggregate": {"input": "$1", "by": ["count"], "metrics": ["count"]}},
        {"aggregate": {"input": "$1", "by": ["rank"], "metrics": ["count"], "order_by": "count desc"}},
    ):
        shown = lab.run([listing, {**grouped, "show": True}])
        assert len(_step(shown, 2)["result"]["groups"]) == 2
        for later in (
            {"filter": {"input": "$2", "where": {"attrs.count": "x"}}},
            {"map": {"input": "$2", "compute": {"twice": "count * 2"}}},
        ):
            reply = lab.run([listing, grouped, {**later, "show": True}])
            step = reply["steps"][2]
            assert step["status"] == "refused", step
            assert "cannot tell the group key from the metric" in step["result"]["error"]["message"]


def test_a_steps_own_groups_read_their_by_column_by_every_name(lab) -> None:
    """Issue #4253, lane re's exact repro. FAILS ON DEV 3d646f6e535: a run's
    aggregate over its own rows held its groups under the printed name, so
    a later filter on {"load": 1} or {"attrs.load": 1} matched 0 rows with
    no note, and "$2.attrs.load" was refused "no row of step 2 has 'load'".
    Fixed by holding raw group keys under their bare names (PACT
    read-run-rows-compute, review H1): 1 and 1.0 are one group of 2."""
    lab.upstream.sites = {"*": [
        {"site": "A", "load": 1, "item": "/L1"}, {"site": "B", "load": 1.0, "item": "/L1"},
        {"site": "C", "load": 0.1, "item": "/L2"}, {"site": "D", "load": 2.5, "item": "/L2"},
        {"site": "E", "load": 0.1, "item": "/L2"},
    ]}
    listing = {"expand": {"input": lab.anchor(), "live": {"relation": "fx_sites", "args": {"expr": "*"}}}}
    grouped = {"aggregate": {"input": "$1", "by": ["load"], "metrics": ["count"]}}
    for where in ({"load": 1}, {"attrs.load": 1}):
        reply = lab.run([listing, grouped, {"filter": {"input": "$2", "where": where}, "show": True}])
        rows = _step(reply, 3)["result"]["rows"]
        assert [(r["load"], r["count"]) for r in rows] == [(1, 2)], (where, rows)
    # A later step names the column as "$2.attrs.load" / "$2.load".
    reply = lab.run([listing, grouped, {"values": {"load": [1, 0.1]}},
                     {"intersect": ["$2", "$3"], "on": "attrs.load", "show": True}])
    assert sorted((r["load"], r["count"]) for r in _step(reply, 4)["result"]["rows"]) == [(0.1, 2), (1, 2)]
    for column in ("$2.attrs.load", "$2.load"):
        reply = lab.run([listing, grouped, {"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_attrs", "each": {"site": column}}}, "show": True}])
        step = reply["steps"][2]
        assert "no row of step 2 has" not in json.dumps(step), (column, step)
    by_item = {"aggregate": {"input": "$1", "by": ["attrs.item"], "metrics": ["count"]}}
    for column in ("$2.item", "$2.attrs.item"):
        reply = lab.run([listing, by_item, lab.sizes(column, True)])
        assert _step(reply, 3)["calls"]["ok"] == 2, column


def test_a_census_groups_a_live_steps_rows_by_a_computed_column(lab) -> None:
    """The census shape (lane ri lead): a live step's rows, a map that
    computes a column over them, then an aggregate grouped by that column.
    FAILS ON origin/dev (the map over a step's rows does not take compute).
    The group column is named as the agent wrote it, `calc.<name>`. An
    aggregate step does not take `compute` itself; it is refused by name."""
    steps = _sized(lab, {"/M1": 3000, "/M2": 9000, "/M3": 1500, "/M4": 6000, "/M5": 6000})
    census = [*steps,
              {"map": {"input": "$2", "compute": {"size_class": "if(file_size >= 6000, 'large', 'small')"}}},
              {"aggregate": {"input": "$3", "by": ["calc.size_class"], "metrics": ["count", "sum(file_size)"],
                             "order_by": "count desc"}, "show": True}]
    reply = lab.run(census)
    groups = _step(reply, 4)["result"]["groups"]
    assert [(_unfence(g["calc.size_class"]), g["count"], g["sum(attrs.file_size)"]) for g in groups] == [
        ("large", 3, 21000), ("small", 2, 4500),
    ]
    assert _step(reply, 4)["by"] == ["calc.size_class"]
    text = lab.text("run", {"steps": census})
    assert "calc.size_class" in text.split("[step 4]")[1]
    # The bare name reads the computed column too, said as adjusted.
    bare = lab.run([*census[:3], {"aggregate": {"input": "$3", "by": ["size_class"],
                                                "metrics": ["count"]}, "show": True}])
    assert sorted((_unfence(g["calc.size_class"]), g["count"]) for g in _step(bare, 4)["result"]["groups"]) == [
        ("large", 3), ("small", 2),
    ]
    assert any("by size_class read as calc.size_class" in n for n in _step(bare, 4)["notes"])
    # compute inside the aggregate step itself is refused by name.
    refused = lab.run([*steps, {"aggregate": {"input": "$2", "compute": {"x": "file_size"},
                                              "by": ["calc.x"], "metrics": ["count"]}}])
    error = refused["steps"][2]["result"]["error"]
    assert "does not take compute" in error["message"], error


# --------------------------------------------- kept answers x compute-in-run


SITES = [
    {"site": "A", "zone": "east", "load": 2}, {"site": "B", "zone": "west", "load": 4},
    {"site": "C", "zone": "east", "load": 0}, {"site": "D", "zone": "west", "load": 2},
    {"site": "E", "zone": "north", "load": 4},
]
SITES_LISTING_ARGS = {"relation": "fx_sites", "args": {"expr": "*"}}
#: calc.x is text ("g50", "g25"); site C (load 0) is an error row.
X_COMPUTE = {"x": "concat('g', string(100 / load))"}


def _refusals(reply: dict[str, Any]) -> list[tuple[Any, ...]]:
    """Each step not ok: its number, status and error message."""
    return [(s["step"], s["status"], ((s.get("result") or {}).get("error") or {}).get("message"))
            for s in reply["steps"] if s["status"] != "ok"]


def _crossing_steps(first: list[dict[str, Any]], n: int) -> list[dict[str, Any]]:
    """The steps after the rows that carry calc.x (step `n`): a filter that
    keeps every row, a group by calc.x, a fan-out on "$k.calc.x", a compute
    reading calc.x, and a filter on that computed column."""
    f, g, m = n + 1, n + 2, n + 4
    return [
        *first,
        {"filter": {"input": f"${n}", "where": {"field": "attrs.load", "op": "gte", "value": 0}},
         "show": True},
        {"aggregate": {"input": f"${f}", "by": ["calc.x"], "metrics": ["count", "sum(load)"],
                       "order_by": "count desc"}, "show": True},
        {"expand": {"input": None, "live": {"relation": "fx_attrs", "each": {"site": f"${g}.calc.x"}}},
         "show": True},
        {"map": {"input": f"${f}", "compute": {"y": "concat(calc.x, '!')"}}, "show": True},
        {"filter": {"input": f"${m}", "where": {"calc.y": "g50!"}}, "show": True},
    ]


def _anchored(lab: Lab, steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for step in steps:
        body = step.get("expand")
        if body is not None and body.get("input") is None:
            body["input"] = lab.anchor()
    return steps


def _crossing_view(reply: dict[str, Any], n: int) -> dict[str, Any]:
    """What the steps after `n` answered, as a client reads them."""
    out: dict[str, Any] = {}
    filtered = _step(reply, n + 1)
    out["filter"] = (filtered["rows"], filtered["from_rows"],
                     sorted(str(_unfence(r.get("calc.x"))) for r in filtered["result"]["rows"]))
    grouped = _step(reply, n + 2)
    out["groups"] = [(_unfence(g["calc.x"]), g["count"], g["sum(attrs.load)"])
                     for g in grouped["result"]["groups"]]
    out["groups_by"] = grouped["by"]
    out["groups_aside"] = (grouped.get("not_computed") or {}).get("count")
    fan = _step(reply, n + 3)
    out["fan"] = (fan["calls"]["ok"], sorted(_unfence(r["site"]) for r in fan["result"]["rows"]))
    mapped = _step(reply, n + 4)
    out["y_states"] = mapped["computed"]["calc.y"]["states"]
    out["y"] = sorted(str(_unfence(r.get("calc.y"))) for r in mapped["result"]["rows"])
    last = _step(reply, n + 5)
    out["last"] = (last["rows"], sorted(_unfence(r["site"]) for r in last["result"]["rows"]),
                   (last.get("not_computed") or {}).get("count"))
    return out


def test_a_kept_compute_answer_reads_in_run_as_the_same_pipeline_built_in_run(lab) -> None:
    """Lane re's kept answers x this PACT's computed columns in held rows.
    FAILS ON THE MERGE BEFORE THE ALIGNMENT: a kept map(compute) answer's
    rows were held with plain `calc.x` keys and no computed mark, so the
    run read calc.x as a missing computed column (grouped nothing, the
    error row not set aside). A stored map(compute) answer with one error
    row, then filter, group by calc.x, each "$n.calc.x" and a compute
    reading calc.x in run, equals the same pipeline built entirely inside
    run; the error row is set aside (never matched), nothing is refused
    reserved_key, and calc.x is printed once."""
    lab.upstream.sites = {"*": [dict(row) for row in SITES]}
    listed = lab.json("expand", {"input": lab.anchor(), "live": SITES_LISTING_ARGS})
    kept = listed["external_live"]["result_reference"]["id"]
    mapped = lab.json("map", {"input": {"ref": kept}, "compute": X_COMPUTE})
    assert mapped["computed"]["calc.x"]["states"] == {"ok": 4, "error:division_by_zero": 1}, mapped
    stored = mapped["handle"]["id"]

    inside = _anchored(lab, _crossing_steps([
        {"expand": {"input": lab.anchor(), "live": SITES_LISTING_ARGS}},
        {"map": {"input": "$1", "compute": X_COMPUTE}},
    ], 2))
    through = _anchored(lab, _crossing_steps([
        {"filter": {"input": {"ref": stored}}},
    ], 1))
    built = lab.run(inside)
    read = lab.run(through)
    for reply in (built, read):
        assert reply["refused_steps"] == 0, _refusals(reply)
        assert "reserved_key" not in json.dumps(reply)
    expected = _crossing_view(built, 2)
    assert expected["groups"] == [("g25", 2, 8), ("g50", 2, 4)] or expected["groups"] == [
        ("g50", 2, 4), ("g25", 2, 8)]
    assert expected["groups_by"] == ["calc.x"]
    assert expected["groups_aside"] == 1
    assert expected["fan"] == (2, ["g25", "g50"])
    assert expected["last"][:2] == (2, ["A", "D"])
    assert _crossing_view(read, 1) == expected
    # The error row is held, and set aside where calc.x is read: the filter
    # on load keeps it (5 rows); the group and the compute never read it.
    assert _crossing_view(read, 1)["filter"][:2] == (5, 5)
    # calc.x printed once on the held rows' step, in JSON and in text.
    for row in _step(read, 2)["result"]["rows"]:
        assert [k for k in row if k.endswith("calc.x")] == ["calc.x"], row
    # A kept step's `select` keeps the computed column's value and mark.
    selected = lab.run([
        {"filter": {"input": {"ref": stored}, "select": ["attrs.site", "calc.x"]}},
        {"aggregate": {"input": "$1", "by": ["calc.x"], "metrics": ["count"]}, "show": True},
    ])
    assert selected["refused_steps"] == 0, _refusals(selected)
    grouped = _step(selected, 2)
    assert sorted((_unfence(g["calc.x"]), g["count"]) for g in grouped["result"]["groups"]) == [
        ("g25", 2), ("g50", 2)]
    assert grouped["not_computed"]["count"] == 1
    text = lab.text("run", {"steps": through})
    step2 = text.split("[step 2]")[1].split("[step 3]")[0]
    assert "\x00" not in text
    for line in step2.splitlines():
        if line.startswith("["):
            assert line.count("calc.x") <= 1, line


def _zone_view(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return sorted((_unfence(r.get("zone", r.get("attrs.zone"))), r.get("count"),
                   r.get("calc.d"), r.get("calc.n")) for r in rows)


def test_compute_over_a_kept_groups_answer_equals_compute_over_an_in_run_aggregate(lab) -> None:
    """The stored-groups-answer case: a compute over a kept groups answer
    (count * 2, len(attrs.zone)), in run over the kept aggregate step's
    rows, and as a kept map over the groups handle read by a later run
    step, equals the same compute over an in-run aggregate of the rows."""
    lab.upstream.sites = {"*": [dict(row) for row in SITES]}
    listed = lab.json("expand", {"input": lab.anchor(), "live": SITES_LISTING_ARGS})
    kept = listed["external_live"]["result_reference"]["id"]
    compute = {"d": "count * 2", "n": "len(attrs.zone)"}
    built = lab.run([
        {"expand": {"input": lab.anchor(), "live": SITES_LISTING_ARGS}},
        {"aggregate": {"input": "$1", "by": ["zone"], "metrics": ["count"]}},
        {"map": {"input": "$2", "compute": compute}, "show": True},
        {"filter": {"input": "$3", "where": {"calc.d": 4}}, "show": True},
    ])
    assert built["refused_steps"] == 0, _refusals(built)
    expected = _zone_view(_step(built, 3)["result"]["rows"])
    assert expected == [("east", 2, 4, 4), ("north", 1, 2, 5), ("west", 2, 4, 4)]
    expected_two = _zone_view(_step(built, 4)["result"]["rows"])
    assert expected_two == [("east", 2, 4, 4), ("west", 2, 4, 4)]

    over_held = lab.run([
        {"aggregate": {"input": kept, "by": ["attrs.zone"], "metrics": ["count"]}},
        {"map": {"input": "$1", "compute": compute}, "show": True},
        {"filter": {"input": "$2", "where": {"calc.d": 4}}, "show": True},
    ])
    assert over_held["refused_steps"] == 0, _refusals(over_held)
    assert _zone_view(_step(over_held, 2)["result"]["rows"]) == expected
    assert _zone_view(_step(over_held, 3)["result"]["rows"]) == expected_two

    groups = lab.json("aggregate", {"input": {"ref": kept}, "by": ["attrs.zone"],
                                    "metrics": ["count"]})
    stored = lab.json("map", {"input": {"ref": groups["handle"]["id"]}, "compute": compute})
    assert stored["computed"]["calc.d"]["states"] == {"ok": 3}, stored
    over_kept = lab.run([
        {"filter": {"input": {"ref": stored["handle"]["id"]}}},
        {"filter": {"input": "$1", "where": {"calc.d": 4}}, "show": True},
        {"aggregate": {"input": "$1", "by": [], "metrics": ["sum(calc.n)"]}, "show": True},
    ])
    assert over_kept["refused_steps"] == 0, _refusals(over_kept)
    assert "reserved_key" not in json.dumps(over_kept)
    assert _zone_view(_step(over_kept, 2)["result"]["rows"]) == expected_two
    (total,) = _step(over_kept, 3)["result"]["groups"]
    assert total == {"sum(calc.n)": 13}


def test_an_error_in_a_kept_groups_answer_stays_an_error_in_run(lab) -> None:
    """Review H1 (P0, an error read as a value). A stored map(compute)
    over a groups handle with one error row (site C: 100 / 0), read in
    run and computed over again: the error stays that row's state, as
    the same map sent alone keeps it. FAILS AT bc20763e4a: the run rebuilt
    the groups rows without their computed mark, so calc.r read as an ok
    null and coalesce(calc.r, -1) gave -1 (states ok 5)."""
    lab.upstream.sites = {"*": [dict(row) for row in SITES]}
    listed = lab.json("expand", {"input": lab.anchor(), "live": SITES_LISTING_ARGS})
    kept = listed["external_live"]["result_reference"]["id"]
    groups = lab.json("aggregate", {"input": {"ref": kept}, "by": ["attrs.site"],
                                    "metrics": ["count", "sum(attrs.load)"]})
    stored = lab.json("map", {"input": {"ref": groups["handle"]["id"]},
                              "compute": {"r": "100 / col['sum(attrs.load)']"}})
    assert stored["computed"]["calc.r"]["states"] == {"ok": 4, "error:division_by_zero": 1}
    again = {"z": "coalesce(calc.r, -1)"}
    outside = lab.json("map", {"input": {"ref": stored["handle"]["id"]}, "compute": again})
    expected = outside["computed"]["calc.z"]["states"]
    assert expected == {"ok": 4, "error:division_by_zero": 1}, expected
    reply = lab.run([
        {"filter": {"input": {"ref": stored["handle"]["id"]}}},
        {"map": {"input": "$1", "compute": again}, "show": True},
        {"filter": {"input": "$2", "where": {"field": "calc.z", "op": "lt", "value": 0}},
         "show": True},
        {"aggregate": {"input": "$1", "by": [], "metrics": ["count", "sum(calc.r)"]},
         "show": True},
    ])
    assert reply["refused_steps"] == 0, _refusals(reply)
    mapped = _step(reply, 2)
    assert mapped["computed"]["calc.z"]["states"] == expected
    by_site = {_unfence(r.get("site", r.get("attrs.site"))): r.get("calc.z") for r in mapped["result"]["rows"]}
    assert by_site["C"] is None and -1 not in by_site.values(), by_site
    # Never coalesced, never matched, never summed.
    assert _step(reply, 3)["rows"] == 0
    assert _step(reply, 3)["not_computed"]["count"] == 1
    total = _step(reply, 4)
    assert total["not_computed"]["count"] == 1
    assert total["result"]["groups"][0]["sum(calc.r)"] == 50 + 25 + 50 + 25

def test_a_kept_groups_answer_reads_its_by_column_by_every_name_as_in_run(lab) -> None:
    """Issue #4253 through a kept answer: the pipeline of
    test_a_steps_own_groups_read_their_by_column_by_every_name with the
    groups kept by an aggregate sent alone, read in run; every result is
    identical to the in-run pipeline's."""
    lab.upstream.sites = {"*": [
        {"site": "A", "load": 1, "item": "/L1"}, {"site": "B", "load": 1.0, "item": "/L1"},
        {"site": "C", "load": 0.1, "item": "/L2"}, {"site": "D", "load": 2.5, "item": "/L2"},
        {"site": "E", "load": 0.1, "item": "/L2"},
    ]}
    listing = {"expand": {"input": lab.anchor(), "live": {"relation": "fx_sites", "args": {"expr": "*"}}}}
    listed = lab.json("expand", {"input": lab.anchor(), "live": {
        "relation": "fx_sites", "args": {"expr": "*"}}})
    kept = listed["external_live"]["result_reference"]["id"]

    def both(later: list[dict[str, Any]], by: str) -> tuple[dict[str, Any], dict[str, Any]]:
        inside = lab.run([listing, {"aggregate": {"input": "$1", "by": [by], "metrics": ["count"]}},
                          *[json.loads(json.dumps(s).replace('"$2', '"$2')) for s in later]])
        shifted = [json.loads(json.dumps(s).replace('"$2', '"$1').replace('"$3', '"$2'))
                   for s in later]
        through = lab.run([{"aggregate": {"input": kept, "by": [f"attrs.{by}"], "metrics": ["count"]}},
                           *shifted])
        return inside, through

    for where in ({"load": 1}, {"attrs.load": 1}):
        inside, through = both([{"filter": {"input": "$2", "where": where}, "show": True}], "load")
        rows_in = [(r["load"], r["count"]) for r in _step(inside, 3)["result"]["rows"]]
        rows_kept = [(r["load"], r["count"]) for r in _step(through, 2)["result"]["rows"]]
        assert rows_in == rows_kept == [(1, 2)], (where, rows_in, rows_kept)
    inside, through = both([{"values": {"load": [1, 0.1]}},
                            {"intersect": ["$2", "$3"], "on": "attrs.load", "show": True}], "load")
    a = sorted((r["load"], r["count"]) for r in _step(inside, 4)["result"]["rows"])
    b = sorted((r["load"], r["count"]) for r in _step(through, 3)["result"]["rows"])
    assert a == b == [(0.1, 2), (1, 2)]
    for column in ("$2.attrs.load", "$2.load"):
        inside, through = both([{"expand": {"input": lab.anchor(), "live": {
            "relation": "fx_attrs", "each": {"site": column}}}, "show": True}], "load")
        a_step, b_step = inside["steps"][2], through["steps"][1]
        assert "no row of step" not in json.dumps(a_step), (column, a_step)
        assert "no row of step" not in json.dumps(b_step), (column, b_step)
        assert a_step["status"] == b_step["status"], (column, a_step, b_step)
        assert (a_step.get("calls") or {}) == (b_step.get("calls") or {}), (column, a_step, b_step)
    for column in ("$2.item", "$2.attrs.item"):
        inside, through = both([lab.sizes(column, True)], "item")
        assert _step(inside, 3)["calls"]["ok"] == _step(through, 2)["calls"]["ok"] == 2, column
