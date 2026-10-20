"""Manifest-owned ports, and a supervisor whose status is observed (PACT
``chat-instance-lifecycle``,
``req.chat-instance-lifecycle.ports-and-supervision``).

Both halves of design.md section 8 risk 3 ("The manifest owns ports; process
supervision is manual in the first slices").

**Ports.** Every occupancy arm binds a REAL socket rather than simulating
occupancy, and every refusal arm is paired with a free-port positive control
that starts — a check that refuses everything would satisfy the refusals
alone. The two drift arms are the point of the requirement: after a conflict
the command must not have quietly moved the site to ``port + 1`` or to an
ephemeral port, so those arms assert on the owned runtime's launch log and instance record,
not on unrelated processes' sockets or the error message.

**Supervision.** The restart arm kills a real child process with ``SIGKILL``;
the socket genuinely goes away and ``GET /api/version`` genuinely fails. The
status arm reads ``down`` at that moment and ``up`` only after the instance
itself answers again. Both assertions are bracketed by an independent probe,
so a status surface that reported from the supervisor's launch record instead
of from the instance would fail here.

**The manifest key.** ``chat.instance_port`` is admitted by ``chat_block.py``
as of PACT ``chat-layer-lifecycle``, so the undeclared case now closes as
``instance_port_undeclared`` rather than as "the schema does not admit the
key". :func:`manifest_admits_instance_port` reads the sibling's own census
rather than restating the answer, and the arm below asserts the right refusal
either way — the invariant under test is "refuses rather than defaults", which
holds across the flip. Precedence between the key and ``--instance-port``, and
their disagreement, are proved in
``tests/substrate/test_chat_lifecycle_receipts.py``.
"""

from __future__ import annotations

import socket
import threading
from pathlib import Path

import pytest

from okg.substrate.chat.instance import (
    MCP_ENDPOINT_DEAD,
    MCP_STATE_DOWN,
    MCP_STATE_UNOBSERVED,
    MCP_STATE_UP,
    OCCUPANCY_FREE,
    PINNED_IMAGE_REF,
    PINNED_VERSION,
    STATE_DOWN,
    STATE_UP,
    SUPERVISION_RESTARTED,
    ChatInstanceError,
    InstanceRecord,
    container_name_for,
    instance_status,
    manifest_admits_instance_port,
    mcp_endpoint_for,
    normalize_dsn,
    port_occupancy,
    probe_mcp_endpoint,
    probe_version,
    read_instance,
    record_instance,
    resolve_instance_port,
    stand_up,
    supervise_once,
)

from .instance_harness import (
    ProcessRuntime,
    chat_block,
    env,
    free_port,
    reachable_probe,
    wait_until,
)

DEPLOYMENT = "fixture-deployment"
GRAPH_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_graph"
APP_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_chat_app"
APP_ENV = "OKG_CHAT_APP_DATABASE_URL"
SECRET_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"


@pytest.fixture
def runtime():
    made = ProcessRuntime()
    try:
        yield made
    finally:
        made.shutdown()


def _stand_up(runtime, tmp_path, *, port, block=None, deployment=DEPLOYMENT):
    return stand_up(
        deployment,
        chat_block=chat_block() if block is None else block,
        graph_dsn=GRAPH_DSN,
        runtime=runtime,
        environ=env(**{APP_ENV: APP_DSN, SECRET_ENV: "fixture-key"}),
        instance_port_override=port,
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        ready_timeout=20.0,
        ready_interval=0.05,
    )


def _hold(port: int) -> socket.socket:
    """Bind and listen. A REAL occupied port, not a recorded belief in one."""
    held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    held.bind(("127.0.0.1", port))
    held.listen(1)
    return held


def _record_peer(tmp_path, *, deployment: str, port: int) -> None:
    record_instance(
        InstanceRecord(
            deployment=deployment,
            container=container_name_for(deployment),
            host="127.0.0.1",
            port=port,
            image=PINNED_IMAGE_REF,
            app_database=normalize_dsn(
                "postgresql://okg:pw@127.0.0.1:5432/other_app", label="app",
            ),
            port_source="operator_flag",
            recorded_at="2026-08-10T00:00:00+00:00",
        ),
        state_root=tmp_path,
    )


# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------

def test_conflicting_instance_port_refuses_and_names_the_holder(
    runtime, tmp_path,
):
    """A genuinely bound port refuses, naming the port and the deployment."""
    port = free_port()
    held = _hold(port)
    _record_peer(tmp_path, deployment="first-deployment", port=port)
    try:
        assert port_occupancy(port) != OCCUPANCY_FREE

        with pytest.raises(ChatInstanceError) as caught:
            _stand_up(runtime, tmp_path, port=port)

        assert caught.value.data["reason"] == "instance_port_conflict"
        assert caught.value.data["port"] == port
        assert caught.value.data["holder"] == "first-deployment"
        assert str(port) in str(caught.value)
        assert "first-deployment" in str(caught.value)
    finally:
        held.close()


def test_free_port_starts_as_positive_control(runtime, tmp_path):
    """The control for every refusal above: a free port stands up and answers."""
    port = free_port()
    assert port_occupancy(port) == OCCUPANCY_FREE

    report = _stand_up(runtime, tmp_path, port=port)

    assert report.version == PINNED_VERSION
    assert probe_version(report.base_url) == PINNED_VERSION
    assert report.port_plan.instance_port == port


def _server_side_time_wait() -> int:
    """A port whose only trace is a closed connection the SERVER closed
    first: the TIME_WAIT record `down` leaves behind (#2876 item 3)."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = listener.getsockname()[1]
    client = socket.create_connection(("127.0.0.1", port))
    accepted, _ = listener.accept()
    accepted.close()  # the server's active close puts ITS side in TIME_WAIT
    wait_until(lambda: client.recv(1) == b"", timeout=2.0)
    client.close()
    listener.close()
    return port


def test_a_time_wait_record_on_the_port_reads_free():
    """Fail-before: a bind without SO_REUSEADDR hits EADDRINUSE on a
    TIME_WAIT record, so `down` waited and `up` refused a port nothing held."""
    port = _server_side_time_wait()

    assert port_occupancy(port) == OCCUPANCY_FREE


def test_a_wildcard_listener_still_refuses_up(runtime, tmp_path):
    """SO_REUSEADDR must not let a 0.0.0.0 listener through."""
    held = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    held.bind(("0.0.0.0", 0))
    # A backlog the probes' unaccepted connections cannot fill: with
    # listen(1), the first probe's connection occupies the queue and a later
    # connect times out, which would read "free" for the wrong reason.
    held.listen(16)
    port = held.getsockname()[1]
    try:
        assert port_occupancy(port) != OCCUPANCY_FREE
        with pytest.raises(ChatInstanceError) as caught:
            _stand_up(runtime, tmp_path, port=port)
        assert caught.value.data["reason"] == "instance_port_conflict"
        assert runtime.run_calls == []
    finally:
        held.close()


@pytest.mark.parametrize(
    "family, address", [(socket.AF_INET, "0.0.0.0"), (socket.AF_INET6, "::")],
    ids=["ipv4-wildcard", "ipv6-wildcard"],
)
def test_a_wildcard_bind_without_a_listener_reads_bound(family, address):
    """Fail-before (at 6a9b62dcbd, on macOS): with SO_REUSEADDR alone the
    127.0.0.1 bind went through beside a bound-but-not-listening wildcard
    socket, so the port read free."""
    v6only = Path("/proc/sys/net/ipv6/bindv6only")
    if family == socket.AF_INET6 and v6only.is_file() and v6only.read_text().strip() == "1":
        pytest.skip("net.ipv6.bindv6only=1: a :: socket does not hold the IPv4 port")
    try:
        held = socket.socket(family, socket.SOCK_STREAM)
    except OSError:
        pytest.skip(f"no {address} socket on this host")
    try:
        try:
            held.bind((address, 0))
        except OSError:
            pytest.skip(f"cannot bind {address} on this host")
        port = held.getsockname()[1]
        assert port_occupancy(port) != OCCUPANCY_FREE
    finally:
        held.close()


def _hold_adjacent_ports() -> tuple[socket.socket, socket.socket]:
    """Reserve two real sockets; allocation retries never rerun the test."""
    for _ in range(100):
        held = _hold(0)
        port = held.getsockname()[1]
        if port == 65535:
            held.close()
            continue
        try:
            neighbor = _hold(port + 1)
        except OSError:
            held.close()
            continue
        return held, neighbor
    raise AssertionError("could not reserve adjacent sockets for conflict isolation proof")


def test_conflicting_port_is_not_incremented(runtime, tmp_path):
    """An unrelated occupied neighbor cannot be mistaken for our fallback.

    Any fallback through this runtime must leave a launch call, even if its
    process subsequently exits. Check that evidence and the instance record
    instead of assuming other processes leave the neighboring ports free.
    """
    held, neighbor = _hold_adjacent_ports()
    port = held.getsockname()[1]
    try:
        assert port_occupancy(port + 1) != OCCUPANCY_FREE
        with pytest.raises(ChatInstanceError) as caught:
            _stand_up(runtime, tmp_path, port=port)

        assert caught.value.data["reason"] == "instance_port_conflict"
        assert runtime.run_calls == [], "conflict unexpectedly launched an instance"
        assert runtime.start_calls == []
        assert runtime.containers == {}
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is None
    finally:
        neighbor.close()
        held.close()


def test_conflict_proof_detects_actual_fallback(runtime, tmp_path, monkeypatch):
    """Negative control: a real fallback launch must fail the conflict proof."""
    original = _stand_up

    def faulty_fallback(runtime, tmp_path, *, port):
        try:
            return original(runtime, tmp_path, port=port)
        except ChatInstanceError:
            original(runtime, tmp_path, port=free_port())
            raise

    monkeypatch.setitem(globals(), "_stand_up", faulty_fallback)
    with pytest.raises(AssertionError, match="conflict unexpectedly launched"):
        test_conflicting_port_is_not_incremented(runtime, tmp_path)
    assert len(runtime.run_calls) == 1
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is not None


def test_conflicting_port_does_not_fall_back_to_an_ephemeral_port(
    runtime, tmp_path,
):
    """No launch happened at all, so no ephemeral port could have been chosen.

    The runtime's launch log is the strongest form of this assertion: an
    ephemeral fallback would necessarily appear there with a port that is not
    the declared one.
    """
    port = free_port()
    held = _hold(port)
    try:
        with pytest.raises(ChatInstanceError):
            _stand_up(runtime, tmp_path, port=port)

        assert runtime.run_calls == []
        assert runtime.containers == {}
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is None
    finally:
        held.close()

    # Control: with the holder gone, the SAME declared port starts. This is
    # what shows the refusal was about occupancy and not about the port value.
    report = _stand_up(runtime, tmp_path, port=port)
    assert report.port_plan.instance_port == port
    assert len(runtime.run_calls) == 1


def test_instance_port_equal_to_mcp_port_refuses():
    """A deployment cannot serve chat and its own mcp-serve on one port."""
    block = chat_block()
    mcp_port = block.mcp.port
    assert mcp_port, "the fixture must declare chat.mcp.port for this arm"

    with pytest.raises(ChatInstanceError) as caught:
        resolve_instance_port(DEPLOYMENT, block, override=mcp_port)
    assert caught.value.data["reason"] == "instance_port_equals_mcp_port"
    assert caught.value.data["mcp_port"] == mcp_port

    # Control: any other port resolves.
    plan = resolve_instance_port(DEPLOYMENT, block, override=mcp_port + 1)
    assert plan.instance_port == mcp_port + 1


def test_undeclared_instance_port_refuses_rather_than_defaulting():
    """No manifest key and no flag is a refusal, never a default.

    The refusal REASON depends on whether ``chat-manifest-block`` has admitted
    the key yet, and this test asserts the right one either way rather than
    pinning today's state — the requirement ("refuses rather than defaults")
    is the invariant across the flip.
    """
    block = chat_block()
    with pytest.raises(ChatInstanceError) as caught:
        resolve_instance_port(DEPLOYMENT, block, override=None)

    data = caught.value.data
    if manifest_admits_instance_port():
        assert data["reason"] == "instance_port_undeclared"
        assert data["manifest_admits_key"] is True
    else:
        assert data["reason"] == "instance_port_key_not_admitted"
        assert data["manifest_admits_key"] is False
        assert data["blocked_on"] == "chat-manifest-block"
    assert data["manifest_key"] == "chat.instance_port"
    # Whatever the arm, no port was chosen: a refusal that had settled on a
    # value would carry it, and that value would be the silent default.
    assert "port" not in data
    assert "instance_port" not in data


# ---------------------------------------------------------------------------
# Supervision
# ---------------------------------------------------------------------------

def test_killed_instance_is_restarted_and_answers_again(runtime, tmp_path):
    """SIGKILL a real process; the supervisor brings the instance back."""
    port = free_port()
    report = _stand_up(runtime, tmp_path, port=port)
    assert probe_version(report.base_url) == PINNED_VERSION

    runtime.kill_instance(container_name_for(DEPLOYMENT))
    assert wait_until(lambda: probe_version(report.base_url) is None), (
        "the killed instance is still answering; the arm would be vacuous"
    )

    event = supervise_once(
        DEPLOYMENT,
        runtime=runtime,
        state_root=tmp_path,
        restart_timeout=20.0,
        restart_interval=0.05,
    )

    assert event.before.state == STATE_DOWN
    assert event.action == SUPERVISION_RESTARTED
    assert event.after.state == STATE_UP
    assert probe_version(report.base_url) == PINNED_VERSION
    assert runtime.start_calls == [container_name_for(DEPLOYMENT)]


def test_status_reports_down_only_while_the_instance_fails_to_answer(
    runtime, tmp_path,
):
    """Three observations, each bracketed by an INDEPENDENT probe.

    Up while the instance answers, down at a moment when ``GET /api/version``
    genuinely fails, and up again only after it genuinely succeeds. The
    independent ``probe_version`` call beside each assertion is what makes the
    "down" reading non-vacuous: a status derived from the supervisor's launch
    record would still read up at the middle step, and a status that always
    read down would fail the first and third.
    """
    port = free_port()
    report = _stand_up(runtime, tmp_path, port=port)

    assert probe_version(report.base_url) == PINNED_VERSION
    up = instance_status(DEPLOYMENT, state_root=tmp_path)
    assert (up.state, up.status, up.version) == (STATE_UP, "healthy", PINNED_VERSION)
    assert up.observed is True

    runtime.kill_instance(container_name_for(DEPLOYMENT))
    assert wait_until(lambda: probe_version(report.base_url) is None)
    down = instance_status(DEPLOYMENT, state_root=tmp_path)
    assert (down.state, down.status, down.version) == (STATE_DOWN, "stopped", None)
    assert down.base_url == report.base_url  # it knew where to look, and asked

    runtime.start(container_name_for(DEPLOYMENT))
    assert wait_until(lambda: probe_version(report.base_url) == PINNED_VERSION)
    again = instance_status(DEPLOYMENT, state_root=tmp_path)
    assert (again.state, again.status) == (STATE_UP, "healthy")


def test_status_of_an_unknown_deployment_is_absent_not_healthy(tmp_path):
    """Nothing recorded is `absent`/`missing` — never a default-healthy."""
    status = instance_status("never-stood-up", state_root=tmp_path)
    assert (status.state, status.status) == ("absent", "missing")
    assert status.observed is False


# ---------------------------------------------------------------------------
# The deployment's mcp-serve endpoint: reported, never assumed, never restarted
# ---------------------------------------------------------------------------

def test_a_dead_mcp_endpoint_is_named_on_an_otherwise_healthy_status(
    runtime, tmp_path,
):
    """The site is up, its tool endpoint is not, and the status says which.

    This is the live shape: the instance runs as a container this module owns
    while mcp-serve is a separate process on the declared port, so an instance
    answering with a dead endpoint behind it is ordinary. Reported as `up` with
    nothing else said, the operator opens a chat where every tool call fails.
    """
    port = free_port()
    _stand_up(runtime, tmp_path, port=port)

    # A port nothing is on. Bound and released, so the refusal is real rather
    # than a name that happens not to resolve.
    dead_endpoint = f"http://127.0.0.1:{free_port()}/mcp"
    status = instance_status(
        DEPLOYMENT, state_root=tmp_path, mcp_endpoint=dead_endpoint,
    )

    assert (status.state, status.status) == (STATE_UP, "healthy")
    assert status.mcp.state == MCP_STATE_DOWN
    assert status.mcp.reason == MCP_ENDPOINT_DEAD
    assert status.mcp.endpoint == dead_endpoint
    assert MCP_ENDPOINT_DEAD in status.detail
    assert status.as_dict()["mcp"]["reason"] == MCP_ENDPOINT_DEAD


def test_a_live_mcp_endpoint_reads_up_and_says_what_was_actually_asked(
    runtime, tmp_path,
):
    """The positive control, and the limit of the claim it makes.

    Any completed HTTP exchange means a listener is there — including the 4xx
    mcp-serve's streamable-http transport answers a bare GET with. The
    observation therefore carries its KIND, so nothing downstream can read a
    transport probe as the initialize + tools/list readback `okg chat sync`
    performs.
    """
    port = free_port()
    report = _stand_up(runtime, tmp_path, port=port)

    # The stub answers 404 on an unknown path: a live listener declining a
    # request, which is exactly the case that must not read as dead.
    live_endpoint = f"{report.base_url}/mcp"
    observation = probe_mcp_endpoint(live_endpoint)

    assert observation.state == MCP_STATE_UP
    assert observation.reason is None
    assert observation.kind == "transport"
    assert "not an initialize + tools/list readback" in observation.detail


def test_an_undeclared_mcp_endpoint_is_unobserved_and_not_reachable(
    runtime, tmp_path,
):
    """Not asked is its own answer. `unobserved` is never `up`."""
    port = free_port()
    _stand_up(runtime, tmp_path, port=port)

    status = instance_status(DEPLOYMENT, state_root=tmp_path)

    assert status.state == STATE_UP
    assert status.mcp.state == MCP_STATE_UNOBSERVED
    assert status.mcp.state != MCP_STATE_UP
    assert status.mcp.dead is False
    assert "nothing was asked" in status.mcp.detail

    # And the endpoint is derived from the manifest, not invented: a block
    # declaring chat.mcp.port yields one, a block without it yields None.
    assert mcp_endpoint_for(chat_block()) == (
        f"http://127.0.0.1:{chat_block().mcp.port}/mcp"
    )
    portless = chat_block(mcp={"tools": ["inspect", "search"]})
    assert portless.mcp.port is None
    assert mcp_endpoint_for(portless) is None


def test_a_listener_that_does_not_speak_http_reads_down_rather_than_raising(
    runtime, tmp_path,
):
    """The condition arriving as a crash is worse than the silence it replaced.

    Something bound to the port that accepts a connection and then does not
    speak HTTP — a TLS socket, a stale non-HTTP process, a half-open proxy —
    raises ``http.client.BadStatusLine``, which is an ``HTTPException`` and
    NOT an ``OSError``. Uncaught it escapes ``instance_status`` entirely, so
    ``okg chat-instance status`` prints a traceback instead of naming the
    condition. The same hole existed in ``probe_version``, where it would take
    out the instance's own status too, so both are asserted here.
    """
    port = free_port()
    _stand_up(runtime, tmp_path, port=port)

    garbage_port = free_port()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", garbage_port))
    listener.listen(8)
    stop = threading.Event()

    def babble() -> None:
        """Accept, answer with something that is not a status line, close."""
        while not stop.is_set():
            try:
                conn, _addr = listener.accept()
            except OSError:
                return
            with conn:
                try:
                    conn.recv(4096)
                    conn.sendall(b"NOT-HTTP\r\n\r\n")
                except OSError:
                    # The probe under test may reset the connection mid-write;
                    # the babbler's only job is to emit garbage, so a torn
                    # connection is not a failure of this fixture.
                    pass

    thread = threading.Thread(target=babble, daemon=True)
    thread.start()
    try:
        observation = probe_mcp_endpoint(f"http://127.0.0.1:{garbage_port}/mcp")
        assert observation.state == MCP_STATE_DOWN
        assert observation.reason == MCP_ENDPOINT_DEAD

        # It reaches the status surface as a reading, not as an exception —
        # including for a deployment that was never stood up, where the probe
        # runs before there is any record to read.
        status = instance_status(
            DEPLOYMENT,
            state_root=tmp_path,
            mcp_endpoint=f"http://127.0.0.1:{garbage_port}/mcp",
        )
        assert status.state == STATE_UP
        assert status.mcp.reason == MCP_ENDPOINT_DEAD

        absent = instance_status(
            "never-stood-up",
            state_root=tmp_path,
            mcp_endpoint=f"http://127.0.0.1:{garbage_port}/mcp",
        )
        assert absent.state == "absent"
        assert absent.mcp.reason == MCP_ENDPOINT_DEAD

        # And the instance's OWN probe survives the same listener, which is
        # what keeps a port held by something else reading as `down` rather
        # than raising out of the status surface.
        assert probe_version(f"http://127.0.0.1:{garbage_port}") is None
    finally:
        stop.set()
        listener.close()
        thread.join(timeout=5)


def test_supervision_reports_a_dead_mcp_endpoint_and_restarts_only_the_instance(
    runtime, tmp_path,
):
    """The condition is carried by a pass that was otherwise a clean restart.

    Two claims, and the second is why this arm exists: the supervisor names
    what it saw, and it does not pretend to have fixed something it does not
    manage. Its only restart target is the instance's container.
    """
    port = free_port()
    report = _stand_up(runtime, tmp_path, port=port)
    dead_endpoint = f"http://127.0.0.1:{free_port()}/mcp"

    runtime.kill_instance(container_name_for(DEPLOYMENT))
    assert wait_until(lambda: probe_version(report.base_url) is None)

    event = supervise_once(
        DEPLOYMENT,
        runtime=runtime,
        state_root=tmp_path,
        mcp_endpoint=dead_endpoint,
        restart_timeout=20.0,
        restart_interval=0.05,
    )

    assert event.action == SUPERVISION_RESTARTED
    assert event.after.state == STATE_UP
    assert event.conditions == (MCP_ENDPOINT_DEAD,)
    assert event.as_dict()["conditions"] == [MCP_ENDPOINT_DEAD]
    assert event.as_dict()["unmanaged"] == ["mcp-serve"]
    # The instance's container, and nothing else, was started.
    assert runtime.start_calls == [container_name_for(DEPLOYMENT)]

    # Control: with a LIVE endpoint the same pass carries no condition, so the
    # list above is not something this event always says.
    healthy = supervise_once(
        DEPLOYMENT,
        runtime=runtime,
        state_root=tmp_path,
        mcp_endpoint=f"{report.base_url}/mcp",
    )
    assert healthy.conditions == ()
