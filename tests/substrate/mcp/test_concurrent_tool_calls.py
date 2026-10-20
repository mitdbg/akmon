"""Tool calls from different MCP sessions run at the same time (issue #3493).

FastMCP calls a synchronous tool function directly on the event loop, and
every okg graph tool is synchronous (blocking psycopg I/O). Before #3493 one
``okg mcp-serve`` process therefore ran tool calls strictly one at a time
across all sessions. ``_RawResultFastMCP.call_tool`` now runs a sync tool in a
worker thread, in a copy of its own request's context, and adopts back what
the tool set in that context, so the response shaping after it sees exactly
what it saw before.

Two layers:

* **Hermetic** (no mark): a ``ByteBoundedFastMCP`` served over streamable
  HTTP with uvicorn, tools registered through okg's own registration paths.
  Four sessions overlap; a slow call does not delay the others; per-request
  state (principal, raised byte cap, pending images, a context variable the
  tool sets) stays with its own call; the thread width is bounded.
* **Postgres** (``postgres`` + ``serial_db``): the real ``build_server`` over
  a real publish. Two principals call at the same time and each keeps its
  own Nomos-governed rows, byte cap and result reference; four real searches
  run at the same time and each returns its own query's record.

Overlap is proven with ``threading.Barrier``: a tool waits until all of its
concurrent peers are inside the tool. On the event loop (before #3493) the
first call blocks the loop, no peer can enter, and the barrier times out.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import socket
import threading
import time
from contextlib import AsyncExitStack
from typing import Any, Callable, Iterator

import pytest

pytest.importorskip("mcp")
pytest.importorskip("uvicorn")

from mcp import ClientSession  # noqa: E402
from mcp.client.streamable_http import streamablehttp_client  # noqa: E402

from okg.substrate.mcp.response_bounds import (  # noqa: E402
    ByteBoundedFastMCP,
    MaxBytes,
    attach_images,
    bound_read_tool,
)
from okg.substrate.mcp.tool_registration import _register_tool  # noqa: E402
from tests.substrate.mcp._result_payload import (  # noqa: E402
    call_json,
    json_client_sessions,
    payload,
)

#: How long a tool waits for its concurrent peers, or for the test's release,
#: before giving up. It is a failure detector, not a measurement: a server
#: that overlaps the calls fills the barrier in milliseconds, and one that
#: runs them one at a time can never fill it, however long it waits. A
#: correct server never waits this long, so it is generous; the overlap and
#: lock tests that use it decide on events, not on how fast the calls ran.
BARRIER_TIMEOUT_S = 30.0

PRINCIPAL_HEADER = "x-probe-principal"


def _untraced(_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    return lambda fn: fn


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextlib.contextmanager
def _serve_http(mcp: ByteBoundedFastMCP) -> Iterator[str]:
    """Serve ``mcp`` over streamable HTTP on a loopback port, in a thread."""
    import uvicorn

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            mcp.streamable_http_app(), host="127.0.0.1", port=port, log_level="error",
        )
    )
    thread = threading.Thread(target=server.run, name="probe-uvicorn", daemon=True)
    thread.start()
    deadline = time.monotonic() + 20.0
    while not server.started:
        assert thread.is_alive(), "uvicorn exited before it started"
        assert time.monotonic() < deadline, "uvicorn did not start in 20 s"
        time.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        server.should_exit = True
        thread.join(timeout=40.0)


async def _open_sessions(
    stack: AsyncExitStack, url: str, headers: list[dict[str, str] | None],
) -> list[ClientSession]:
    sessions = []
    for header in headers:
        read, write, _ = await stack.enter_async_context(
            streamablehttp_client(url, headers=header)
        )
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()
        sessions.append(session)
    return sessions


class _InFlight:
    """Counts calls inside a tool at once; remembers the peak."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.now = 0
        self.peak = 0
        self.threads: set[int] = set()

    @contextlib.contextmanager
    def inside(self) -> Iterator[None]:
        with self._lock:
            self.now += 1
            self.peak = max(self.peak, self.now)
            self.threads.add(threading.get_ident())
        try:
            yield
        finally:
            with self._lock:
                self.now -= 1


# ============================================================ hermetic


@pytest.mark.parametrize("route", ["register_tool", "add_tool"])
def test_four_sessions_run_a_blocking_sync_tool_at_the_same_time(
    route: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.mcp-concurrent-tool-calls-3493.overlap, over streamable HTTP.

    ``register_tool`` is how every graph, write and admin tool is registered
    (``tool_registration._register_tool``); ``add_tool`` is how deployment
    tools are (``deployment_tools.register_deployment_tools``). Both must
    overlap: the offload sits in ``call_tool``, not in a registration path.
    """
    json_client_sessions(monkeypatch)
    mcp = ByteBoundedFastMCP("concurrency-probe")
    inflight = _InFlight()

    barrier = threading.Barrier(4, timeout=BARRIER_TIMEOUT_S)

    def slow_search(query: str) -> dict[str, Any]:
        """Stands in for a search doing blocking psycopg I/O."""
        with inflight.inside():
            # Returns only once all four calls are inside the tool. With the
            # calls one at a time the first waits alone, the barrier breaks,
            # and every call comes back as an error.
            barrier.wait()
        return {"status": "ok", "query": query}

    if route == "register_tool":
        _register_tool(mcp, tool_name="slow_search", fn=slow_search, traced=_untraced)
    else:
        mcp._tool_manager.add_tool(slow_search, name="slow_search", structured_output=False)

    async def scenario() -> list[Any]:
        async with AsyncExitStack() as stack:
            sessions = await _open_sessions(stack, url, [None] * 4)
            return list(await asyncio.gather(*(
                session.call_tool("slow_search", {"query": f"q{index}"})
                for index, session in enumerate(sessions)
            )))

    with _serve_http(mcp) as url:
        results = asyncio.run(scenario())

    # The proof: all four calls were inside the tool at once. There is no
    # wall-clock bound: the time four overlapped calls take over HTTP depends
    # on the runner (a slow one once took 2.24 s for four 0.5 s calls and
    # failed a ratio bound while this count was 4), and the count does not.
    assert inflight.peak == 4, (
        f"at most {inflight.peak} of 4 calls were inside the tool at once"
    )
    assert len(inflight.threads) == 4, inflight.threads
    assert [result.isError for result in results] == [False] * 4, [
        payload(result) for result in results
    ]
    assert [payload(result)["query"] for result in results] == ["q0", "q1", "q2", "q3"]


def test_a_slow_call_does_not_delay_another_sessions_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One session's call stays inside its tool until three other sessions'
    calls have returned.

    The held call is released only after the quick calls come back, so the
    order is fixed by events, not by sleeps: on a server that runs calls one
    at a time the quick calls cannot return while the held one waits, the
    held one gives up after ``BARRIER_TIMEOUT_S`` unreleased, and the test
    fails. No duration is compared.
    """
    json_client_sessions(monkeypatch)
    mcp = ByteBoundedFastMCP("concurrency-probe")
    held_inside = threading.Event()
    release = threading.Event()

    def work(hold: bool) -> dict[str, Any]:
        if not hold:
            return {"status": "ok", "held": False}
        held_inside.set()
        return {"status": "ok", "held": True, "released": release.wait(BARRIER_TIMEOUT_S)}

    _register_tool(mcp, tool_name="work", fn=work, traced=_untraced)

    async def scenario() -> tuple[list[Any], bool, Any]:
        async with AsyncExitStack() as stack:
            slow, *quick = await _open_sessions(stack, url, [None] * 4)
            slow_task = asyncio.ensure_future(slow.call_tool("work", {"hold": True}))
            try:
                inside = await asyncio.to_thread(held_inside.wait, BARRIER_TIMEOUT_S)
                assert inside, "the held call never entered its tool"
                quick_results = await asyncio.gather(*(
                    session.call_tool("work", {"hold": False}) for session in quick
                ))
                slow_still_running = not slow_task.done()
            finally:
                release.set()
            return list(quick_results), slow_still_running, await slow_task

    with _serve_http(mcp) as url:
        quick_results, slow_still_running, slow_result = asyncio.run(scenario())

    assert [payload(result) for result in quick_results] == [
        {"status": "ok", "held": False}
    ] * 3
    # Every quick call returned while the held one was still inside its tool:
    # it was released (by this test, after the quick calls) and did not time out.
    assert payload(slow_result) == {"status": "ok", "held": True, "released": True}, (
        f"the held call was not released within {BARRIER_TIMEOUT_S:.0f} s: "
        "the quick calls did not return while it was inside its tool"
    )
    assert slow_still_running


_TOOL_SET: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "probe_tool_set", default=None,
)


def test_state_two_principals_set_stays_with_each_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.mcp-concurrent-tool-calls-3493.request-state, hermetic half.

    Two sessions call one read tool at the same moment (a barrier proves it).
    Each call reads its principal from its own request, sends its own byte
    cap through the real byte-bound wrapper, and queues its own image; the
    response carries only its own principal, cap disclosure and image.
    """
    json_client_sessions(monkeypatch)
    mcp = ByteBoundedFastMCP("state-probe")
    barrier = threading.Barrier(2, timeout=BARRIER_TIMEOUT_S)

    def inspect(target: str, max_bytes: MaxBytes = None) -> dict[str, Any]:
        """A read that queues an image, like inspect(kind="image")."""
        request = mcp.get_context().request_context.request
        principal = request.headers[PRINCIPAL_HEADER]
        barrier.wait()  # both calls are inside the tool now
        attach_images([(f"image-of-{principal}".encode(), "image/png")])
        return {
            "status": "ok", "principal": principal, "target": target,
            "max_bytes_seen": max_bytes,
        }

    _register_tool(
        mcp, tool_name="inspect", fn=bound_read_tool("inspect", inspect),
        traced=_untraced,
    )

    async def scenario() -> list[Any]:
        async with AsyncExitStack() as stack:
            alice, bob = await _open_sessions(stack, url, [
                {PRINCIPAL_HEADER: "alice"}, {PRINCIPAL_HEADER: "bob"},
            ])
            return list(await asyncio.gather(
                alice.call_tool("inspect", {"target": "t", "max_bytes": 1000}),
                bob.call_tool("inspect", {"target": "t", "max_bytes": 16384}),
            ))

    with _serve_http(mcp) as url:
        alice_result, bob_result = asyncio.run(scenario())

    for result, principal in ((alice_result, "alice"), (bob_result, "bob")):
        assert result.isError is False, payload(result)
        images = [block for block in result.content if block.type == "image"]
        assert [base64.b64decode(block.data) for block in images] == [
            f"image-of-{principal}".encode()
        ]
    alice_body, bob_body = payload(alice_result), payload(bob_result)
    assert alice_body["principal"] == "alice" and bob_body["principal"] == "bob"
    # Alice's 1000 was raised to the 4 KiB minimum and the page says so;
    # Bob's 16 KiB was not raised, so his page carries no disclosure.
    assert alice_body["max_bytes_seen"] == 4096
    assert alice_body["max_bytes_raised"] == {"requested": 1000, "applied": 4096}
    assert bob_body["max_bytes_seen"] == 16384
    assert "max_bytes_raised" not in bob_body


def test_a_context_variable_a_sync_tool_sets_is_seen_after_it_by_its_own_call_only() -> None:
    """The adopt-back half of the mechanism, without a transport.

    Two calls run their bodies in worker threads at once (barrier). Each sets
    a context variable and does not reset it. After the await, each caller
    reads its own value -- as it would have with the body on the loop -- and
    the caller that started neither call sees nothing.
    """
    from okg.substrate.mcp.response_bounds import run_sync_in_request_context

    barrier = threading.Barrier(2, timeout=BARRIER_TIMEOUT_S)
    loop_thread: list[int] = []

    def body(tag: str) -> int:
        _TOOL_SET.set(tag)
        barrier.wait()
        return threading.get_ident()

    async def one(tag: str) -> tuple[str | None, int]:
        ident = await run_sync_in_request_context(body, tag)
        return _TOOL_SET.get(), ident

    async def main() -> list[tuple[str | None, int]]:
        loop_thread.append(threading.get_ident())
        return list(await asyncio.gather(one("a"), one("b")))

    (seen_a, thread_a), (seen_b, thread_b) = asyncio.run(main())
    assert (seen_a, seen_b) == ("a", "b")
    assert loop_thread[0] not in {thread_a, thread_b}
    assert thread_a != thread_b
    assert _TOOL_SET.get() is None


def test_an_exception_in_the_thread_still_reaches_the_caller_with_its_state() -> None:
    """A raising tool body: the exception propagates unchanged, and what it
    set before raising is still adopted, as it was when it ran on the loop."""
    from okg.substrate.mcp.response_bounds import run_sync_in_request_context

    def body() -> None:
        _TOOL_SET.set("set-before-raise")
        raise LookupError("tool failed")

    async def main() -> str | None:
        with pytest.raises(LookupError, match="tool failed"):
            await run_sync_in_request_context(body)
        return _TOOL_SET.get()

    assert asyncio.run(main()) == "set-before-raise"


def test_concurrent_sync_tool_calls_are_bounded_by_the_server_width() -> None:
    """``_okg_tool_call_width`` caps the threads one server runs tools in.

    ``build_server`` sets it from the read pool (see the postgres test); a
    call beyond the width waits on the event loop for a slot instead of
    taking a thread that would then wait on the pool.
    """
    mcp = ByteBoundedFastMCP("width-probe")
    mcp._okg_tool_call_width = 2
    inflight = _InFlight()
    # Two calls are inside at once however slow the runner is; the sleep
    # after it gives a third call time to enter if the width did not hold.
    barrier = threading.Barrier(2, timeout=BARRIER_TIMEOUT_S)

    def work(index: int) -> dict[str, Any]:
        with inflight.inside():
            barrier.wait()
            time.sleep(0.3)
        return {"status": "ok", "index": index}

    _register_tool(mcp, tool_name="work", fn=work, traced=_untraced)

    async def main() -> float:
        started = time.perf_counter()
        await asyncio.gather(*(mcp.call_tool("work", {"index": i}) for i in range(4)))
        return time.perf_counter() - started

    elapsed = asyncio.run(main())
    assert inflight.peak == 2, inflight.peak
    assert elapsed >= 0.55, elapsed  # two rounds of 0.3 s
    # A second event loop is bounded by the same width.
    assert asyncio.run(main()) >= 0.55
    assert inflight.peak == 2


def test_an_async_tool_still_runs_on_the_event_loop() -> None:
    """Only sync tools are offloaded; an async tool keeps running on the loop."""
    mcp = ByteBoundedFastMCP("async-probe")
    seen: list[int] = []

    async def awaitable_tool() -> dict[str, Any]:
        seen.append(threading.get_ident())
        return {"status": "ok"}

    mcp._tool_manager.add_tool(awaitable_tool, name="awaitable_tool", structured_output=False)

    async def main() -> int:
        await mcp.call_tool("awaitable_tool", {})
        return threading.get_ident()

    loop_ident = asyncio.run(main())
    assert seen == [loop_ident]


def test_a_cancelled_call_keeps_its_slot_until_its_thread_finishes() -> None:
    """L1: asyncio cancellation never frees a slot a thread still uses.

    Width 1. Call A is inside its tool; call B waits for the slot. B is
    cancelled while waiting (it must never run); A is cancelled while its
    thread runs (the thread goes on). Call C must not start until A's thread
    has finished: the bound counts threads, not awaiting tasks.
    """
    mcp = ByteBoundedFastMCP("cancel-probe")
    mcp._okg_tool_call_width = 1
    inflight = _InFlight()
    ran: list[str] = []
    a_inside = threading.Event()
    release_a = threading.Event()

    def work(tag: str) -> dict[str, Any]:
        with inflight.inside():
            ran.append(tag)
            if tag == "a":
                a_inside.set()
                assert release_a.wait(BARRIER_TIMEOUT_S)
        return {"status": "ok", "tag": tag}

    _register_tool(mcp, tool_name="work", fn=work, traced=_untraced)

    async def main() -> list[str]:
        a = asyncio.ensure_future(mcp.call_tool("work", {"tag": "a"}))
        await asyncio.to_thread(a_inside.wait, BARRIER_TIMEOUT_S)
        b = asyncio.ensure_future(mcp.call_tool("work", {"tag": "b"}))
        await asyncio.sleep(0.1)  # B is waiting for the slot
        b.cancel()
        a.cancel()  # A's thread is still inside the tool
        for task in (a, b):
            with pytest.raises(asyncio.CancelledError):
                await task
        c = asyncio.ensure_future(mcp.call_tool("work", {"tag": "c"}))
        await asyncio.sleep(0.3)
        started_before_release = list(ran)
        release_a.set()
        await c
        return started_before_release

    started_before_release = asyncio.run(main())
    assert started_before_release == ["a"], started_before_release
    assert ran == ["a", "c"], ran
    assert inflight.peak == 1


def test_a_session_blocked_on_its_own_state_lock_never_blocks_the_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M2: two calls in ONE session, one holding that session's state lock.

    ``bind`` stands in for ``_open_bound_default``: it holds the session's
    lock across slow work in its thread. ``read`` then returns a page whose
    serving needs the same lock (the stale-notice state, served in the text
    format for a session-pinned page). While both wait, another session's
    call must complete: the waiting is done in worker threads, never on the
    event loop. ``bind`` is released only after that call returns, so the
    order is fixed by events: no duration is compared.
    """
    mcp = ByteBoundedFastMCP("lock-probe")
    session_lock = threading.Lock()
    bind_holds = threading.Event()
    release_bind = threading.Event()

    @contextlib.contextmanager
    def notice_session(context: Any) -> Iterator[dict[str, Any]]:
        with session_lock:
            yield {}

    mcp._okg_notice_session = notice_session

    def bind() -> dict[str, Any]:
        with session_lock:
            bind_holds.set()
            released = release_bind.wait(BARRIER_TIMEOUT_S)
        return {"status": "ok", "bound": True, "released": released}

    def read() -> dict[str, Any]:
        return {"status": "ok", "pin_scope": "transport_session", "rows": []}

    def quick() -> dict[str, Any]:
        return {"status": "ok", "quick": True}

    for name, fn in (("bind", bind), ("read", read), ("quick", quick)):
        _register_tool(mcp, tool_name=name, fn=fn, traced=_untraced)

    async def scenario() -> tuple[Any, bool]:
        async with AsyncExitStack() as stack:
            same, other = await _open_sessions(stack, url, [None, None])
            bind_call = asyncio.ensure_future(same.call_tool("bind", {}))
            await asyncio.to_thread(bind_holds.wait, BARRIER_TIMEOUT_S)
            read_call = asyncio.ensure_future(same.call_tool("read", {}))
            await asyncio.sleep(0.2)  # read's serving now waits for the lock
            try:
                await other.call_tool("quick", {})
                read_waiting = not read_call.done()
            finally:
                release_bind.set()
            bind_result = await bind_call
            result = await read_call
            assert result.isError is False
            return bind_result, read_waiting

    with _serve_http(mcp) as url:
        bind_result, read_waiting = asyncio.run(scenario())
    assert read_waiting, "the read was not waiting on the session lock"
    # The other session's call returned while bind still held the lock: bind
    # was released by this test after that call, not by its own timeout.
    # (This test keeps the text format: the read's page is served in it.)
    bind_text = bind_result.content[0].text
    assert "released: true" in bind_text.splitlines(), (
        f"bind was not released within {BARRIER_TIMEOUT_S:.0f} s: the other "
        "session's call did not return while the session lock was held: "
        + bind_text
    )


def test_a_raised_refusal_stamped_under_the_session_lock_never_blocks_the_loop() -> None:
    """M2, error path: a typed refusal is stamped by the logical-session
    stamper, which takes the session lock. That stamping runs in a worker
    thread, so another call completes while it waits."""
    from okg.substrate.errors import SubstrateError

    mcp = ByteBoundedFastMCP("stamp-probe")
    session_lock = threading.Lock()

    def stamper(payload: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        with session_lock:
            payload["stamped"] = True
        return payload

    mcp._okg_logical_session_stamper = stamper

    def search(query: str) -> dict[str, Any]:
        raise SubstrateError(f"refused {query}")

    def quick() -> dict[str, Any]:
        return {"status": "ok"}

    _register_tool(mcp, tool_name="search", fn=search, traced=_untraced)
    _register_tool(mcp, tool_name="quick", fn=quick, traced=_untraced)

    holding = threading.Event()
    release = threading.Event()
    released: list[bool] = []

    def hold_the_lock() -> None:
        # Another call of the same session, mid-bind. Bounded, so a server
        # that does block its loop fails this test instead of hanging it.
        with session_lock:
            holding.set()
            released.append(release.wait(BARRIER_TIMEOUT_S))

    holder = threading.Thread(target=hold_the_lock, name="session-bind")
    holder.start()
    assert holding.wait(BARRIER_TIMEOUT_S)

    async def main() -> tuple[Any, bool]:
        try:
            refused = asyncio.ensure_future(
                call_json(mcp, "search", {"query": "x"})
            )
            await asyncio.sleep(0.2)
            await mcp.call_tool("quick", {})
            waiting = not refused.done()
        finally:
            release.set()
        return await refused, waiting

    result, waiting = asyncio.run(main())
    holder.join()
    assert waiting, "the refusal was not waiting on the session lock"
    # The quick call returned while the lock was held: the holder was
    # released by this test, not by its own timeout.
    assert released == [True], (
        f"the lock holder was not released within {BARRIER_TIMEOUT_S:.0f} s"
    )
    assert result.isError is True
    assert payload(result)["stamped"] is True


def test_grep_regex_parse_does_not_swap_the_process_warning_filters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """L2: ``warnings.catch_warnings`` swaps process-wide state and is not
    thread-safe; a grep parse running in a worker thread must not use it.
    The nested-set FutureWarning it hid is still not surfaced."""
    import warnings

    from okg.substrate.mcp import grep_query

    def refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("catch_warnings is not thread-safe")

    real_catch_warnings = warnings.catch_warnings
    with real_catch_warnings(record=True) as seen:  # the test's own scope
        monkeypatch.setattr(grep_query.warnings, "catch_warnings", refuse)
        try:
            assert grep_query.literal_run("[[a]") == 0
        finally:
            monkeypatch.undo()
    assert not [w for w in seen if issubclass(w.category, FutureWarning)]


#: The thread-name prefix of a server's request workers.
CALL_WORKER_PREFIX = "okg-mcp-call"


def test_a_sync_resource_is_read_in_a_call_worker() -> None:
    """``read_resource`` runs a sync resource function (and the principal
    check inside it) in one of the server's request workers, not on the
    loop, and still returns the SDK's content."""
    mcp = ByteBoundedFastMCP("resource-probe")
    mcp._okg_tool_call_width = 2
    seen: list[str] = []

    def require_principal(_name: str) -> None:
        seen.append(f"check:{threading.current_thread().name}")

    @mcp.resource("okg://probe/resource", name="probe", mime_type="application/json")
    def probe_resource() -> str:
        require_principal("probe")
        seen.append(f"body:{threading.current_thread().name}")
        return '{"ok": true}'

    async def main() -> tuple[Any, str]:
        contents = await mcp.read_resource("okg://probe/resource")
        return list(contents), threading.current_thread().name

    contents, loop_thread = asyncio.run(main())
    assert [c.content for c in contents] == ['{"ok": true}']
    assert contents[0].mime_type == "application/json"
    assert len(seen) == 2, seen
    for entry in seen:
        assert entry.split(":", 1)[1].startswith(CALL_WORKER_PREFIX), seen
        assert entry.split(":", 1)[1] != loop_thread


def test_closing_a_built_server_shuts_its_call_workers_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closing the read pool (how every caller closes a built server) also
    shuts the server's request workers down: no idle threads outlive it."""
    from okg.substrate.mcp import server as server_mod
    from okg.substrate.mcp.surface_budget import PROBE_DSN

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_TRACE_ENABLED", "0")
    monkeypatch.setenv("OKG_MCP_CAPABILITY_FIXTURE", "full")
    monkeypatch.setenv("OKG_MCP_RO_DSN", PROBE_DSN)
    monkeypatch.delenv("OKG_MCP_TOOL_ALLOWLIST", raising=False)
    monkeypatch.delenv("OKG_MCP_ALLOW_WRITES", raising=False)
    monkeypatch.setattr(
        server_mod,
        "_server_dsn_resolution",
        lambda _dsn, _name: {
            "dsn": PROBE_DSN, "authority": {"source": "surface_probe_stub"},
        },
    )
    mcp = server_mod.build_server(dsn=PROBE_DSN)
    try:
        executor = mcp._okg_tool_executor()
        assert executor is not None
        assert executor.submit(threading.current_thread).result(5).name.startswith(
            CALL_WORKER_PREFIX
        )
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    assert executor._shutdown is True  # noqa: SLF001
    with pytest.raises(RuntimeError):
        executor.submit(lambda: None)


# ============================================================ postgres


def _barrier_tool(mcp: Any, name: str, parties: int, inflight: _InFlight | None = None) -> None:
    """Make tool ``name`` wait until ``parties`` calls are inside it."""
    tool = mcp._tool_manager._tools[name]
    original = tool.fn
    barrier = threading.Barrier(parties, timeout=BARRIER_TIMEOUT_S)

    def gated(**kwargs: Any) -> Any:
        with inflight.inside() if inflight is not None else contextlib.nullcontext():
            barrier.wait()
            return original(**kwargs)

    tool.fn = gated


@pytest.mark.postgres
@pytest.mark.serial_db
def test_two_principals_at_once_keep_their_rows_caps_and_result_references(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.mcp-concurrent-tool-calls-3493.request-state on the real server.

    The two-principal Nomos fixture: Alice may read the Drive source, Bob is
    denied it. Every pair of calls below is made at the same moment (each
    tool is gated by a two-party barrier), so the calls overlap in threads.
    """
    from okg.substrate.mcp import result_references
    from okg.substrate.mcp.result_references import ResultReferenceStore
    from tests.substrate.mcp.test_principal_mapping import (
        PRINCIPAL_A,
        PRINCIPAL_B,
        PRINCIPAL_HEADER as MAPPED_HEADER,
        SUBJECT_A,
        SUBJECT_B,
        _activate_two_principal_policy,
        _configure_header_identity,
        _decision_rows,
        _multi_user_server,
        _publish_two_source_graph,
        request_headers,
    )

    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(SUBJECT_A,),
    )
    _activate_two_principal_policy(clean_runtime, b_effect_on_drive="deny")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    # If either request fell back to the process subject, the decision
    # subjects below would show it.
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")
    store = ResultReferenceStore()
    previous = result_references.install_store(store)

    async def as_principal(principal: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        with request_headers({MAPPED_HEADER: principal}):
            return payload(await call_json(mcp, name, arguments))

    def pair(name: str, a_args: dict[str, Any], b_args: dict[str, Any]) -> list[dict[str, Any]]:
        async def both() -> list[dict[str, Any]]:
            return list(await asyncio.gather(
                as_principal(PRINCIPAL_A, name, a_args),
                as_principal(PRINCIPAL_B, name, b_args),
            ))

        return asyncio.run(both())

    filter_args = {
        "input": {"kind": "nodes"}, "predicate": {},
        "select": ["node_id"], "limit": 50,
    }
    try:
        with _multi_user_server(clean_runtime) as mcp:
            for name in ("filter", "search", "map"):
                _barrier_tool(mcp, name, 2)
            a_rows, b_rows = pair(
                "filter",
                {**filter_args, "max_bytes": 1000},
                {**filter_args, "max_bytes": 16384},
            )
            a_search, b_search = pair(
                "search",
                {"query": "docs", "retain": True},
                {"query": "docs", "retain": True},
            )
            a_ref = a_search["result_reference"]["id"]
            b_ref = b_search["result_reference"]["id"]
            # Alice reads her own reference; Bob, at the same moment, is
            # handed Alice's id and must get the unknown-id refusal.
            a_map, b_map = pair(
                "map", {"input": {"ref": a_ref}}, {"input": {"ref": a_ref}},
            )
    finally:
        result_references.install_store(previous)

    a_ids = {row["node_id"] for row in a_rows["items"]}
    b_ids = {row["node_id"] for row in b_rows["items"]}
    assert a_ids == {"node:private-drive", "node:public-docs"}, a_rows
    assert b_ids == {"node:public-docs"}, b_rows
    assert a_rows["max_bytes_raised"] == {"requested": 1000, "applied": 4096}
    assert "max_bytes_raised" not in b_rows

    assert a_ref != b_ref
    entries = {entry.id: entry for entry in store._entries.values()}  # noqa: SLF001
    assert set(entries) == {a_ref, b_ref}
    assert entries[a_ref].principal_key != entries[b_ref].principal_key

    assert a_map.get("status") != "refused", a_map
    assert {row["node_id"] for row in a_map["items"]} == {"node:public-docs"}
    assert b_map["status"] == "refused", b_map
    assert b_map["error"]["data"]["reason"] == "unknown", b_map

    request_subjects = {
        row["subject"]
        for row in _decision_rows(clean_runtime)
        if row["action"] in {"graph.read.mcp", "mcp.tool.call"}
    }
    assert request_subjects == {SUBJECT_A, SUBJECT_B}


_WORDS = ("alpha", "bravo", "charlie", "delta")


@pytest.mark.postgres
@pytest.mark.serial_db
def test_four_real_searches_run_at_once_and_each_returns_its_own_record(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """req.mcp-concurrent-tool-calls-3493.overlap on a real publish.

    Four lexical searches, each for a word only one record holds, are made at
    once. A four-party barrier inside the search tool proves all four are in
    flight together; each answer holds only its own word's record.
    """
    from okg.substrate.db.concurrency_budget import width
    from okg.substrate.mcp.server import tool_call_width
    from tests.substrate.mcp.test_result_references_db import _publish, _record, _server

    facts: list[Any] = []
    for index, word in enumerate(_WORDS):
        facts += _record(f"doc:{word}", index, f"{word} cladding failed at the weld.")
    _publish(clean_runtime, facts)

    inflight = _InFlight()
    with _server({"dsn": clean_runtime}, monkeypatch) as mcp:
        # (16 - 2 reserved) // 2 connections per call at the default pool.
        assert mcp._okg_tool_call_width == tool_call_width(width("mcp.read_pool"))
        assert tool_call_width(16) == 7
        _barrier_tool(mcp, "search", len(_WORDS), inflight)

        async def main() -> tuple[list[Any], float]:
            started = time.perf_counter()
            results = await asyncio.gather(*(
                call_json(mcp, "search", {"query": word, "subtype_filter": "document"})
                for word in _WORDS
            ))
            return list(results), time.perf_counter() - started

        results, elapsed = asyncio.run(main())

    assert inflight.peak == len(_WORDS)
    assert len(inflight.threads) == len(_WORDS)
    for word, result in zip(_WORDS, results):
        body = payload(result)
        assert result.isError is False, body
        assert [str(row["node_id"]).strip() for row in body["results"]] == [f"doc:{word}"], (
            word, body,
        )
    print(f"four concurrent searches: {elapsed:.3f}s")


_HTTP_TOKEN = "f1e2d3c4b5a6978877665544332211000ffeeddccbbaa99887766554433221100"


@pytest.mark.postgres
@pytest.mark.serial_db
def test_the_multi_user_dispatch_gate_runs_off_the_loop_and_keeps_each_principal(
    clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M1: the real dispatch handler with a principal mapping, over HTTP.

    Every principal resolution (the dispatch gate on ``tools/call`` and
    ``tools/list``, the call adapter's own gate, the per-tool gate) borrows a
    pool connection. None may run on the event-loop thread, where a slow
    checkout would stall every session. Two principals call ``filter`` at
    once (a two-party barrier inside the tool proves the overlap); each gets
    its own Nomos rows, and a principal bound to nobody is refused.
    """
    import okg.substrate.mcp.server as server_mod
    from tests.substrate.mcp.test_principal_mapping import (
        DEPLOYMENT,
        PRINCIPAL_A,
        PRINCIPAL_B,
        PRINCIPAL_HEADER as MAPPED_HEADER,
        SUBJECT_A,
        SUBJECT_B,
        _activate_two_principal_policy,
        _configure_header_identity,
        _decision_rows,
        _multi_user_server,
        _publish_two_source_graph,
    )

    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime,
        principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
        drive_subjects=(SUBJECT_A,),
    )
    _activate_two_principal_policy(clean_runtime, b_effect_on_drive="deny")
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:process-wide-operator")
    json_client_sessions(monkeypatch)

    resolutions: list[tuple[str, str]] = []
    original_resolve = server_mod._resolve_principal_to_subject

    def observed_resolve(*args: Any, **kwargs: Any) -> Any:
        resolutions.append((kwargs.get("surface", "?"), threading.current_thread().name))
        return original_resolve(*args, **kwargs)

    monkeypatch.setattr(server_mod, "_resolve_principal_to_subject", observed_resolve)

    def headers(principal: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {_HTTP_TOKEN}", MAPPED_HEADER: principal}

    filter_args = {
        "input": {"kind": "nodes"}, "predicate": {},
        "select": ["node_id"], "limit": 50,
    }

    async def scenario() -> tuple[dict, dict, dict]:
        async with AsyncExitStack() as stack:
            alice, bob, nobody = await _open_sessions(stack, url, [
                headers(PRINCIPAL_A), headers(PRINCIPAL_B),
                headers("nobody@example.edu"),
            ])
            await alice.list_tools()
            a, b = await asyncio.gather(
                alice.call_tool("filter", filter_args),
                bob.call_tool("filter", filter_args),
            )
            refused = await nobody.call_tool("filter", filter_args)
            # A resource read resolves the principal too (its own gate).
            digest = await alice.read_resource(
                f"okg://readside-metadata/{DEPLOYMENT}"
            )
            assert digest.contents
            return payload(a), payload(b), payload(refused)

    with _multi_user_server(clean_runtime, auth_token=_HTTP_TOKEN) as mcp:
        _barrier_tool(mcp, "filter", 2)
        with _serve_http(mcp) as url:
            a_rows, b_rows, refused = asyncio.run(scenario())

    assert {row["node_id"] for row in a_rows["items"]} == {
        "node:private-drive", "node:public-docs",
    }, a_rows
    assert {row["node_id"] for row in b_rows["items"]} == {"node:public-docs"}, b_rows
    assert refused["status"] == "refused", refused
    assert "items" not in refused
    assert refused["error"]["code"] == "nomos_principal_unidentified", refused

    surfaces = {surface for surface, _thread in resolutions}
    assert {"mcp.tool.call", "mcp.resource.read"} <= surfaces, resolutions
    off_worker = [
        (surface, thread) for surface, thread in resolutions
        if not thread.startswith(CALL_WORKER_PREFIX)
    ]
    assert not off_worker, (
        f"{len(off_worker)} principal resolutions ran outside a request "
        f"worker: {off_worker[:4]}"
    )

    request_subjects = {
        row["subject"]
        for row in _decision_rows(clean_runtime)
        if row["action"] in {"graph.read.mcp", "mcp.tool.call"}
    }
    assert {SUBJECT_A, SUBJECT_B} <= request_subjects
    assert "subject:process-wide-operator" not in request_subjects
