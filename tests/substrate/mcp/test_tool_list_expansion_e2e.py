"""In-session tool-list expansion, measured against real server processes.

Issue #1458 refused to call in-session expansion done until a REAL client had
been observed re-listing and using a newly exposed tool. Every test here
spawns `_expansion_e2e_server.py` as its own process and talks to it with the
SDK's own `ClientSession` over a real transport. Nothing is mocked, stubbed,
or monkeypatched: an in-process `FastMCP` object would prove the projection's
Python semantics while proving nothing about the frames on the wire, which is
the part that was unverified.

Three sections, for three different claims:

* **stdio** — one session against `_expansion_e2e_server.py`: capability
  advertisement, minimal starting surface, refusal of a hidden tool,
  expansion, the `notifications/tools/list_changed` frame, re-listing, and
  calling the newly exposed tool.
* **streamable-http** — two concurrent sessions against ONE process, which is
  the claim the SDK cannot make on its own: `ToolManager._tools` is
  process-wide, so an expansion for one caller must be shown NOT to widen the
  other.
* **the production server** — the substrate's own `build_server`, served over
  a real streamable-http listener and widened by a real `ClientSession`. The
  two sections above use a synthetic two-tool server, so they prove the
  projection and the frames but nothing about launch admission, the byte
  contract, or the instructions the fleet doctor hashes.
"""
from __future__ import annotations

from tests.substrate.mcp._result_payload import (
    result_text as _result_text,
)

import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import uvicorn

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import ToolListChangedNotification

from tests.pytest.child_pythonpath import child_pythonpath
from tests.substrate.mcp import test_tool_profiles as _tool_profile_tests
from tests.substrate.mcp._result_payload import (
    json_client_sessions,
)

#: The production `build_server` fixture, reused rather than re-declared so
#: the launch this file drives over the wire is the same launch the profile
#: tests assert on in-process. Bound under a new name because `probe` is
#: already a local name here, and pytest registers a fixture under the module
#: attribute it is bound to.
build_production_server = _tool_profile_tests.probe



@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch):
    """Program-side test: every call here reads the JSON result format
    (issue #3059), through request `_meta` or the in-process scope."""
    json_client_sessions(monkeypatch)


SERVER_SCRIPT = Path(__file__).parent / "_expansion_e2e_server.py"
#: Generous on purpose. A tight bound would turn a slow shared runner into a
#: red build about a feature that works; the tests fail on the assertions
#: below long before they fail on time.
STARTUP_TIMEOUT_S = 30.0
CALL_TIMEOUT_S = 30.0


def _subprocess_env() -> dict[str, str]:
    """Give the child the repo's import roots, not the parent's `sys.path`
    (see `tests.pytest.child_pythonpath`)."""
    env = dict(os.environ)
    env["PYTHONPATH"] = child_pythonpath()
    env["PYTHONUNBUFFERED"] = "1"
    return env


class _Notifications:
    """Collects `tools/list_changed` frames for one client session."""

    def __init__(self) -> None:
        self.tool_list_changed = 0

    async def handle(self, message: Any) -> None:
        if isinstance(getattr(message, "root", None), ToolListChangedNotification):
            self.tool_list_changed += 1


def _session(read, write, notifications: _Notifications) -> ClientSession:
    return ClientSession(
        read,
        write,
        read_timeout_seconds=timedelta(seconds=CALL_TIMEOUT_S),
        message_handler=notifications.handle,
    )


def _tool_names(result: Any) -> set[str]:
    return {tool.name for tool in result.tools}


def _text(result: Any) -> str:
    return _result_text(result)


# ---------- stdio: the whole round trip in one session ----------


async def _stdio_scenario() -> dict[str, Any]:
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVER_SCRIPT), "stdio"],
        env=_subprocess_env(),
    )
    observed: dict[str, Any] = {}
    async with stdio_client(params) as (read, write):
        notifications = _Notifications()
        async with _session(read, write, notifications) as session:
            init = await session.initialize()
            observed["list_changed_advertised"] = bool(
                init.capabilities.tools and init.capabilities.tools.listChanged
            )
            observed["initial_tools"] = _tool_names(await session.list_tools())

            hidden = await session.call_tool("wide_only", {})
            observed["hidden_call_is_error"] = bool(hidden.isError)
            observed["hidden_call_text"] = _text(hidden)

            observed["notifications_before"] = notifications.tool_list_changed
            expanded = await session.call_tool(
                "inspect", {"target": {"action": "expand"}},
            )
            observed["expand_text"] = _text(expanded)
            observed["notifications_after"] = notifications.tool_list_changed

            observed["relisted_tools"] = _tool_names(await session.list_tools())
            widened = await session.call_tool("wide_only", {})
            observed["widened_call_is_error"] = bool(widened.isError)
            observed["widened_call_text"] = _text(widened)
    return observed


def test_a_real_stdio_session_widens_itself_and_uses_the_new_tool() -> None:
    observed = asyncio.run(asyncio.wait_for(_stdio_scenario(), STARTUP_TIMEOUT_S))

    # 1. The capability is advertised truthfully. Before this change FastMCP
    #    called create_initialization_options() with no NotificationOptions,
    #    so every session was told listChanged: false.
    assert observed["list_changed_advertised"] is True

    # 2. The session starts minimal, and the hidden tool is REFUSED rather
    #    than merely unlisted.
    assert observed["initial_tools"] == {"inspect"}
    assert observed["hidden_call_is_error"] is True
    # The refusal names the tool and says the surface is the reason, so a
    # caller can tell it apart from a tool that does not exist. The typed
    # code `mcp_tool_not_in_session_surface` reaches the substrate server's
    # own callers through its byte-bounded refusal envelope, which
    # `test_session_tool_surface.py` covers; a bare FastMCP has no such
    # envelope and surfaces the message text only.
    assert "wide_only" in observed["hidden_call_text"]
    assert "not in this session's tool surface" in observed["hidden_call_text"]

    # 3. Expansion emits exactly one notification.
    assert observed["notifications_before"] == 0
    assert observed["notifications_after"] == 1
    assert json.loads(observed["expand_text"])["status"] == "expanded"

    # 4. Re-listing shows the wider set, and the new tool actually runs.
    assert observed["relisted_tools"] == {"inspect", "wide_only"}
    assert observed["widened_call_is_error"] is False
    assert "wide_only" in observed["widened_call_text"]


async def _stdio_idempotence_scenario() -> dict[str, Any]:
    params = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVER_SCRIPT), "stdio"],
        env=_subprocess_env(),
    )
    async with stdio_client(params) as (read, write):
        notifications = _Notifications()
        async with _session(read, write, notifications) as session:
            await session.initialize()
            await session.call_tool("inspect", {"target": {"action": "expand"}})
            second = await session.call_tool(
                "inspect", {"target": {"action": "expand"}},
            )
            return {
                "second_text": _text(second),
                "notifications": notifications.tool_list_changed,
            }


def test_a_repeat_expansion_sends_no_second_notification() -> None:
    """A frame for a list that did not change trains clients to ignore it."""
    observed = asyncio.run(
        asyncio.wait_for(_stdio_idempotence_scenario(), STARTUP_TIMEOUT_S)
    )
    assert '"status": "already_expanded"' in observed["second_text"]
    assert observed["notifications"] == 1


# ---------- streamable-http: two sessions, one process ----------


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture
def http_server(tmp_path):
    """One real streamable-http server process, ready to accept sessions.

    Three deliberate choices, each against a way tests like this go flaky on
    a shared runner:

    * Readiness is a real TCP connect, not a sleep, which is wrong in both
      directions under load.
    * Server output goes to a FILE, not a pipe. A pipe nobody drains blocks
      the server once it fills, which would look like a hang in the test
      rather than a full buffer.
    * A port chosen by bind-and-close can be taken before the server binds
      it, so a start that dies is retried on a fresh port a few times before
      the test is called failed. The log is quoted on the last attempt.
    """
    log_path = tmp_path / "streamable-http-server.log"
    process = None
    failures: list[str] = []
    for attempt in range(3):
        port = _free_port()
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [sys.executable, str(SERVER_SCRIPT), "streamable-http", str(port)],
                env=_subprocess_env(),
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                # The child leads its own process group, so a signal it sends
                # or receives can never reach the pytest-xdist worker whose
                # stdin is the controller channel (#703). Its stdio is
                # detached from the worker for the same reason.
                start_new_session=True,
            )
        ready = False
        end = time.monotonic() + STARTUP_TIMEOUT_S
        while time.monotonic() < end:
            if process.poll() is not None:
                break
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    ready = True
                    break
            except OSError:
                time.sleep(0.1)
        if ready:
            break
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        failures.append(
            f"attempt {attempt + 1} on port {port}: "
            f"rc={process.returncode}"
        )
        process = None
    if process is None:
        raise AssertionError(
            "the streamable-http server never accepted connections "
            f"({'; '.join(failures)}). Server log:\n"
            + log_path.read_text(encoding="utf-8", errors="replace")
        )
    try:
        yield f"http://127.0.0.1:{port}/mcp"
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            process.kill()


async def _two_session_scenario(url: str) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    async with streamable_http_client(url) as (read_a, write_a, _a):
        notes_a = _Notifications()
        async with _session(read_a, write_a, notes_a) as session_a:
            await session_a.initialize()
            async with streamable_http_client(url) as (read_b, write_b, _b):
                notes_b = _Notifications()
                async with _session(read_b, write_b, notes_b) as session_b:
                    await session_b.initialize()

                    observed["a_before"] = _tool_names(await session_a.list_tools())
                    observed["b_before"] = _tool_names(await session_b.list_tools())

                    await session_a.call_tool(
                        "inspect", {"target": {"action": "expand"}},
                    )

                    observed["a_after"] = _tool_names(await session_a.list_tools())
                    observed["b_after"] = _tool_names(await session_b.list_tools())
                    observed["notes_a"] = notes_a.tool_list_changed
                    observed["notes_b"] = notes_b.tool_list_changed

                    b_hidden = await session_b.call_tool("wide_only", {})
                    observed["b_hidden_is_error"] = bool(b_hidden.isError)
                    a_wide = await session_a.call_tool("wide_only", {})
                    observed["a_wide_is_error"] = bool(a_wide.isError)
    return observed


def test_one_sessions_expansion_does_not_widen_another(http_server: str) -> None:
    """The claim `ToolManager._tools` alone cannot support.

    Both sessions live in ONE process against ONE registry. Session A widens;
    session B must still list and be able to call only the base surface, and
    must not be told its own list changed.
    """
    observed = asyncio.run(
        asyncio.wait_for(_two_session_scenario(http_server), 120.0)
    )

    assert observed["a_before"] == {"inspect"}
    assert observed["b_before"] == {"inspect"}
    assert observed["a_after"] == {"inspect", "wide_only"}
    assert observed["b_after"] == {"inspect"}, (
        "session B's tool list widened when session A expanded; the "
        "projection is not per-session"
    )
    assert observed["notes_a"] == 1
    assert observed["notes_b"] == 0, (
        "session B was told its tool list changed when it had not"
    )
    assert observed["b_hidden_is_error"] is True
    assert observed["a_wide_is_error"] is False


# ---------- the PRODUCTION server, over a real HTTP transport ----------
#
# Everything above runs `_expansion_e2e_server.py`: a real process on a real
# transport, but a synthetic two-tool server. Everything the substrate's own
# `build_server` contributes -- launch admission, the byte-bounded refusal
# envelope, the rendered instructions the fleet doctor hashes -- was proved
# only by calling `list_tools()` / `call_tool()` as Python methods, which
# says nothing about the frames a client actually receives.
#
# This section closes that gap: the real `build_server`, served over a real
# streamable-http listener, driven by the SDK's own `ClientSession`.
#
# The server is built in-process by the `probe` fixture because the launch
# contract under test is expressed in `build_server`'s keyword arguments and
# the surrounding environment; a subprocess would need a deployment root, a
# reachable DSN and an argv round trip to say the same thing. The TRANSPORT
# is real either way, which is the part that was unmeasured. No database is
# required: `OKG_MCP_SURFACE_PROBE` builds both pools unopened.


def _write_probe_deployment(root: Path, name: str) -> None:
    """A resolvable deployment that declares no `mcp:` block.

    Written to disk rather than monkeypatched so `build_server` and the fleet
    doctor's `expected_surface` resolve the SAME deployment by the same code
    path; a stubbed resolver on one side would make the hash comparison below
    compare two stubs.
    """
    directory = root / name
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text("", encoding="utf-8")
    (directory / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: production-surface e2e probe\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )


@asynccontextmanager
async def _served_over_http(mcp):
    """Serve a real FastMCP app on a test-owned, already-bound port.

    The socket is bound here and handed to uvicorn already listening, so
    there is no window between choosing a port and binding it for another
    process on a shared runner to take.
    """
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen()
        port = int(sock.getsockname()[1])
        server = uvicorn.Server(
            uvicorn.Config(
                mcp.streamable_http_app(),
                log_level="error",
                lifespan="on",
                timeout_graceful_shutdown=1,
            )
        )
        task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            async with asyncio.timeout(STARTUP_TIMEOUT_S):
                while not server.started:
                    if task.done():
                        task.result()
                        raise AssertionError(
                            "the production server exited before startup"
                        )
                    await asyncio.sleep(0.01)
            yield f"http://127.0.0.1:{port}/mcp"
        finally:
            server.should_exit = True
            await asyncio.wait_for(task, STARTUP_TIMEOUT_S)


async def _production_scenario(mcp) -> dict[str, Any]:
    observed: dict[str, Any] = {}
    async with _served_over_http(mcp) as url:
        async with streamable_http_client(url) as (read, write, _):
            notifications = _Notifications()
            async with _session(read, write, notifications) as session:
                init = await session.initialize()
                observed["list_changed_advertised"] = bool(
                    init.capabilities.tools and init.capabilities.tools.listChanged
                )
                observed["instructions"] = init.instructions or ""
                observed["base_tools"] = _tool_names(await session.list_tools())

                # Withheld at LAUNCH: `record` is a write tool and this
                # launch set no `--allow-writes`, so it was never registered
                # and no ceiling can reach it.
                withheld = await session.call_tool("record", {"payload": {}})
                observed["withheld_before_is_error"] = bool(withheld.isError)
                observed["withheld_before_bytes"] = len(
                    withheld.model_dump_json().encode("utf-8")
                )

                # Registered but not visible: refused with the typed code,
                # through the substrate's own byte-bounded envelope.
                hidden = await session.call_tool("query", {"sql": "SELECT 1"})
                observed["hidden_is_error"] = bool(hidden.isError)
                observed["hidden_text"] = _text(hidden)

                expanded = await session.call_tool(
                    "inspect", {"target": {"kind": "surface", "action": "expand"}},
                )
                observed["expand_is_error"] = bool(expanded.isError)
                observed["expand_text"] = _text(expanded)
                observed["notifications"] = notifications.tool_list_changed
                observed["wide_tools"] = _tool_names(await session.list_tools())

                # Launch admission after the widening: still absent, still
                # refused. A ceiling widens what a session SEES, never what
                # the launch admitted.
                withheld_after = await session.call_tool("record", {"payload": {}})
                observed["withheld_after_is_error"] = bool(withheld_after.isError)

                # The byte bound still wraps a tool this session could not
                # call a moment ago: an invalid budget is refused rather
                # than silently clamped, and the refusal is small. (Over the
                # ceiling: under the floor is raised, #3153.)
                budget = await session.call_tool(
                    "query", {"sql": "SELECT 1", "max_bytes": 10_000_000},
                )
                observed["budget_text"] = _text(budget)
                observed["budget_bytes"] = len(
                    budget.model_dump_json().encode("utf-8")
                )

                surface = await session.call_tool(
                    "inspect", {"target": {"kind": "surface"}},
                )
                observed["surface_payload"] = json.loads(_text(surface))
    return observed


def test_the_production_server_holds_its_launch_contract_over_the_wire(
    build_production_server, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real `build_server`, widened by a real client over a real socket.

    Three claims that only the production server can make, and that only the
    wire can prove:

    1. **Launch admission survives widening.** `record` is withheld at launch
       (no `--allow-writes`), so it is absent from every listing and refused
       before and after the expansion. The ceiling is a projection over what
       the launch registered, never a second admission decision.
    2. **The byte bound still applies after widening.** A newly exposed tool
       is still wrapped by the response-byte contract: an out-of-range
       `max_bytes` is refused with `invalid_response_byte_budget`, and the
       refusal itself stays small.
    3. **The fleet doctor's expectation matches.** The `instructions_hash`
       this running process reports over the wire equals the sha256 of the
       instructions the client received at `initialize`; its authored part
       (`authored_instructions_hash`, #3151) equals what
       `surface_fleet.expected_surface` computes for this registration --
       so a healthy ceiling server is not reported `stale` with a restart
       hint that could never fix it.
    """
    from okg.substrate.mcp.response_bounds import MIN_RESPONSE_BYTES
    from okg.substrate.mcp.surface_fleet import expected_surface

    root = tmp_path / "deployments"
    _write_probe_deployment(root, "e2e-probe")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    mcp = build_production_server(
        deployment_name="e2e-probe",
        tool_profile="discovery",
        tool_profile_ceiling="reader",
    )
    observed = asyncio.run(
        asyncio.wait_for(_production_scenario(mcp), 180.0)
    )

    # The advertisement and the starting surface.
    assert observed["list_changed_advertised"] is True
    assert observed["base_tools"] == {"inspect", "search", "aggregate"}

    # 1. Launch admission, before and after.
    assert observed["withheld_before_is_error"] is True
    assert observed["withheld_after_is_error"] is True
    assert "record" not in observed["wide_tools"], (
        "a tool withheld at launch became visible through an in-session "
        "expansion; the ceiling is admitting, not projecting"
    )

    # The hidden-but-registered tool is a typed refusal naming the way up.
    assert observed["hidden_is_error"] is True
    assert "mcp_tool_not_in_session_surface" in observed["hidden_text"]

    # The widening itself: one frame, one wider list, on the wire.
    assert observed["expand_is_error"] is False
    assert json.loads(observed["expand_text"])["status"] == "expanded"
    assert observed["notifications"] == 1
    assert observed["wide_tools"] == {
        "inspect", "search", "aggregate", "expand", "filter", "map", "query",
        "run",
    }

    # 2. The byte contract still wraps the newly exposed tool.
    assert json.loads(observed["budget_text"])["status"] == "refused"
    assert "invalid_response_byte_budget" in observed["budget_text"]
    # Refused, not silently clamped -- and the refusal is far smaller than
    # the smallest budget a caller may ask for.
    assert observed["budget_bytes"] < MIN_RESPONSE_BYTES
    assert observed["withheld_before_bytes"] < MIN_RESPONSE_BYTES

    # 3. The hash the fleet doctor expects is the hash the process serves.
    payload = observed["surface_payload"]
    served_hash = payload["instructions_hash"]
    # Not vacuous: both sides are a real digest of real text.
    assert observed["instructions"].strip()
    assert str(served_hash).startswith("sha256:")
    assert served_hash == "sha256:" + hashlib.sha256(
        observed["instructions"].encode("utf-8")
    ).hexdigest(), (
        "the surface readback hashes text the client was never sent"
    )
    # #3151 slice 1: the served text ends with generated capability lines
    # that depend on the database (here: the unprobed hermetic record), which
    # the doctor cannot recompute. It compares the AUTHORED part, reported
    # separately, and the served text must still be the authored text plus
    # exactly that generated block.
    from okg.substrate.mcp.capability_probe import split_generated_instructions

    from okg.substrate.mcp.dataset_card import split_dataset_card

    # PACT dataset-card: the dataset card comes first and depends on the
    # database too, so it is outside the authored part as well.
    card, after_card = split_dataset_card(observed["instructions"])
    assert card, "the served instructions start with the dataset card"
    authored, generated = split_generated_instructions(after_card)
    assert generated, "a bare hermetic server serves the generated block"
    assert payload["authored_instructions_hash"] == "sha256:" + hashlib.sha256(
        authored.encode("utf-8")
    ).hexdigest()
    assert payload["authored_instructions_hash"] == expected_surface(
        "e2e-probe", tool_profile="discovery", tool_profile_ceiling="reader",
    )["instructions_hash"], (
        "the fleet doctor would report this healthy server stale"
    )
    # The readback is answered for THIS session, which has widened.
    assert payload["tool_surface"]["expansion"]["expanded"] is True
    assert payload["tool_surface"]["expansion"]["expandable_tools"] == []
