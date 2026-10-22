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
