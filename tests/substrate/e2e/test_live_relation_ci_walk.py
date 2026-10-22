"""The live-relation path, end to end, against a REAL third-party HTTP service.

PACT ``laptop-monitor-live-example`` (issue #2164), req.lmle.live. The PACT
id keeps its original name; the change was re-scoped from a user-facing
example to this CI test, and renaming a change directory is churn.

The hermetic tests prove everything up to the adapter boundary with a fake
adapter. This proves the rest, and it proves it against software this
repository did not write: a real provision into a real database, a published
generation, a real MCP server built from the fixture manifest, and live calls
that dial a third-party HTTP server and come back with its actual output.

**This test starts and stops the service itself, and takes no opt-in flag.**
Glances is a pinned dev dependency (``glances[web]==4.5.6``); the test binds a
free loopback port, launches ``glances -w`` on it, waits for
``/api/4/percpu`` to answer within a bounded timeout, runs the walk, and kills
the process in a ``finally``. So it runs in hosted CI on every pull request
that touches the substrate, which is the point: a "generic" adapter proved
only against the system it was written for is not proved.

**It FAILS if Glances cannot start.** It never skips. A skipped arm reads as
a passed one (#2078), and the whole value of this file is that something
outside our control is actually answering.

Why Glances: it differs from OpenSearch -- the system the HTTP adapter was
written against -- in every place that matters. GET instead of POST, no
request body at all, and a top-level JSON array instead of an object rooted
at ``hits.hits``. Pointing the shipped adapter at it found three refusals no
stub would have produced.

A FREE PORT, not a fixed one. Two of these running on one runner must not
race for 61208, and a developer's own Glances must not be dialled by a test.
The fixture manifest carries Glances' default port as a placeholder and the
staged copy is rewritten to the port this process bound.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main

from .._deployment_repo import commit_deployment_tree

pytestmark = pytest.mark.postgres


@pytest.fixture(autouse=True)
def _json_result_format(monkeypatch: pytest.MonkeyPatch):
    """These tests assert on the result dictionary, so every call asks for
    the JSON result format (issue #3059: the default is one text block).
    Client sessions send the request _meta key; in-process
    FastMCP.call_tool calls run inside result_format_scope."""
    from okg.substrate.mcp.result_text import result_format_scope
    from tests.substrate.mcp._result_payload import json_client_sessions

    json_client_sessions(monkeypatch)
    with result_format_scope("json"):
        yield

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = Path(__file__).resolve().parent / "fixtures/live-http-ci"
#: One fixed database name for the whole file, which is SAFE ONLY WHILE THIS
#: FILE RUNS ON ONE WORKER. The `glances` fixture is module-scoped and the
#: lane runs `--dist=loadscope`, which keeps a module together, so the three
#: tests here share one server and one database. Under `--dist=load` pytest
#: would spread them across workers, two of which would then drop and
#: recreate this database underneath each other -- and `_recreate_database`
#: terminates the other's backends by design. If this file is ever split or
#: the lane's distribution mode changes, make the name worker-scoped (append
#: `PYTEST_XDIST_WORKER`) rather than hoping the ordering holds.
DATABASE = "okg_live_http_ci"

#: How long Glances gets to bind and answer. Generous for a loaded CI runner
#: and still bounded: an unbounded wait turns "the service never came up"
#: into a lane that hangs until its own timeout kills it with no diagnosis.
GLANCES_START_TIMEOUT_S = 90.0


def _free_port() -> int:
    """Bind port 0, read the port, release it.

    A race remains between release and Glances binding it, and it is accepted
    rather than papered over: the alternative is a fixed port, which races
    with every other run on the same runner and with the developer's own
    Glances. If the bind loses the race Glances fails to start and this test
    FAILS with that message, which is a diagnosable outcome.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _Glances:
    """A Glances web server on loopback, owned by one test session."""

    def __init__(self, port: int, proc: subprocess.Popen) -> None:
        self.port = port
        self.proc = proc
        self.base_url = f"http://127.0.0.1:{port}"
        self.host = f"127.0.0.1:{port}"

    def get(self, path: str):
        with urllib.request.urlopen(
            f"{self.base_url}{path}", timeout=10,
        ) as fh:
            return json.load(fh)


@pytest.fixture(scope="module")
def glances(tmp_path_factory: pytest.TempPathFactory):
    """Start Glances, wait for it to answer, and always kill its group.

    `-w` is web-server mode. `--disable-webui` serves the REST API without
    the browser bundle, which is all this test reads.
    `--sort-processes cpu_percent` PINS THE ORDER: `/processlist/top/N`
    returns whatever Glances is currently sorting by and the default is
    `auto` -- CPU when a core is loaded, memory otherwise. Both orders were
    observed on one machine minutes apart, so without this flag a relation
    named `top_processes` means something different from run to run.

    Started with `sys.executable -m glances`, not a bare `glances`, so it is
    THIS environment's pinned interpreter and package rather than whatever
    the runner happens to have on PATH.

    THREE THINGS ISOLATE IT FROM THE TEST RUNNER, and all three are required
    by `test_xdist_spawn_isolation_lint` (#703). A long-lived child spawned
    without them lands in the pytest-xdist worker's own session and inherits
    the worker's stdin -- which under xdist is the execnet control channel to
    the controller -- so a signal to or from the child reaches the worker
    group and surfaces as an intermittent
    `<WorkerController gwN> received keyboard-interrupt`.

    * `start_new_session=True` makes Glances its own session and process
      group leader, so nothing it does can signal the runner.
    * stdout and stderr go to a FILE, not a `PIPE`. A pipe would be another
      descriptor shared with the worker, and a module-scoped server that
      filled a pipe nobody was draining would block forever. The file is
      still read, in full, for the death report below.
    * `stdin=DEVNULL` so the control channel is never inherited.

    Because it leads its own group, `proc.terminate()` alone signals only
    the leader; the teardown below signals the GROUP, so a Glances that
    forked a helper leaves nothing behind.
    """
    port = _free_port()
    log_path = tmp_path_factory.mktemp("glances") / "glances.log"
    log = log_path.open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "glances", "-w",
                "--bind", "127.0.0.1", "--port", str(port),
                "--disable-webui", "--sort-processes", "cpu_percent",
            ],
            stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
    finally:
        # The child holds its own duplicate of the descriptor; this process
        # does not need it and must not keep the file open for the life of
        # the module.
        log.close()

    server = _Glances(port, proc)
    try:
        _wait_until_serving(server, proc, port, log_path)
        # YIELD, not return. A `return` inside a `try/finally` runs the
        # finally immediately, which killed Glances before the first test
        # ran and produced three "Connection refused" failures from a
        # fixture that had reported success.
        yield server
    finally:
        _terminate_group(proc)


def _terminate_group(proc: subprocess.Popen) -> None:
    """Signal the child's whole process group, then reap it.

    `start_new_session=True` makes the child a group leader, so its pid is
    also its process-group id and `killpg` reaches anything it forked.
    `proc.terminate()` would signal the leader alone and leave a helper
    running past the test -- on a CI runner that is a process nobody owns
    holding a port.

    `ProcessLookupError` is tolerated because the group can exit between the
    `poll()` and the signal; nothing else is swallowed.
    """
    if proc.poll() is not None:
        return
    for signal_number in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proc.pid, signal_number)
        except ProcessLookupError:
            break
        try:
            proc.wait(timeout=15)
            return
        except subprocess.TimeoutExpired:
            continue
    # SIGKILL to a group that still has not been reaped is a broken runner,
    # not something to pass over quietly.
    assert proc.poll() is not None, (
        f"glances (pid {proc.pid}) survived SIGKILL to its process group"
    )


def _wait_until_serving(
    server: _Glances, proc: subprocess.Popen, port: int, log_path: Path,
) -> None:
    """Block until the service answers the endpoint the binding uses.

    READY means "answers `/api/4/percpu` with the shape the binding
    expects", not "the port accepts a socket". A half-started server that
    accepts connections and 500s would otherwise be called ready and fail
    later, somewhere less informative.

    Bounded, and a death is detected rather than waited out: an exited
    process is reported with its own output immediately instead of costing
    the lane the full timeout for a diagnosis the exit code already had.
    That output comes from the log FILE, since stdio is not piped here.
    """
    deadline = time.monotonic() + GLANCES_START_TIMEOUT_S
    last: Exception | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out = log_path.read_text(encoding="utf-8", errors="replace")
            pytest.fail(
                f"glances exited with {proc.returncode} before serving on "
                f"127.0.0.1:{port}. This is a FAILURE, not a skip: the "
                f"point of this test is that a real HTTP service "
                f"answers.\n{out[-2000:]}"
            )
        try:
            rows = server.get("/api/4/percpu")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            last = exc
            time.sleep(0.5)
            continue
        if isinstance(rows, list) and rows:
            return
        last = ValueError(f"/api/4/percpu returned {type(rows).__name__}")
        time.sleep(0.5)
    out = log_path.read_text(encoding="utf-8", errors="replace")
    pytest.fail(
        f"glances did not answer /api/4/percpu on 127.0.0.1:{port} within "
        f"{GLANCES_START_TIMEOUT_S:.0f}s; last error: {last!r}\n"
        f"{out[-2000:]}"
    )


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    return json.loads((out.out or out.err).strip().splitlines()[-1])


def _with_database(dsn: str, database: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit((
        parts.scheme, parts.netloc, f"/{database}", parts.query,
        parts.fragment,
    ))


def _recreate_database(dsn: str, database: str) -> None:
    """Drop and recreate this deployment's own database, clean.

    Recreated rather than reused: the catalog records an ownership claim
    bound to the manifest's fingerprint, so a database left over from a run
    against an older manifest refuses the next publish as drift -- correctly,
    and uselessly for a proof that is supposed to run from nothing.

    Guarded to this deployment's own name on a loopback host, so it can only
    ever drop the disposable database this test made.
    """
    parts = urlsplit(dsn)
    assert database == DATABASE, database
    assert parts.hostname in ("127.0.0.1", "localhost"), parts.hostname

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()", (database,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{database}"')
        cur.execute(f'CREATE DATABASE "{database}"')


def _stage(tmp_path: Path, name: str, server: _Glances) -> Path:
    """Copy the fixture tree, renamed and pointed at THIS Glances.

    Copied rather than mocked: this has to exercise the manifest, the
    registry entry, the deployment-local schema and the module list as they
    are committed, or it proves something about a mock.

    Three edits, and no more. The name, so the run owns its own database;
    the DSN, which the test supplies; and the binding's `base_url` and
    `allowed_hosts`, which have to name the port this process just bound.
    Everything the test is actually about -- the method, the path, the root
    `rows_path`, `max_rows`, the relation declarations and the gate -- is
    used exactly as committed.
    """
    target = tmp_path / "deployments" / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(FIXTURE, target)

    manifest = yaml.safe_load((target / "deployment.yaml").read_text())
    manifest["name"] = name
    manifest["postgres"] = {"dsn": "${OKG_DSN}"}
    for relation, binding in manifest["agent_surface"]["live_bindings"].items():
        config = binding["config"]
        # Asserted before rewriting: if the committed fixture ever stopped
        # naming loopback, silently repointing it at the test's own port
        # would hide that.
        assert config["base_url"].startswith("http://127.0.0.1:"), relation
        config["base_url"] = server.base_url
        config["allowed_hosts"] = [server.host]
    (target / "deployment.yaml").write_text(yaml.safe_dump(manifest))

    registry = yaml.safe_load((target / "source_registry.yaml").read_text())
    entry = registry["sources"]["local-env"]
    entry["ownership_id"] = f"{name}.local-env"
    entry["admission_policy"]["producer_id"] = f"{name}.local-env"
    (target / "source_registry.yaml").write_text(yaml.safe_dump(registry))

    # The live gate reads `nomos_external_probe.json` by DEPLOYMENT NAME, and
    # the bundle's own `scope.deployment` must agree with it or the rule
    # matches nothing and every call comes back `nomos.default_deny` -- a
    # denial that would look exactly like a working gate.
    bundle_path = target / "nomos_external_probe.json"
    bundle = json.loads(bundle_path.read_text())
    bundle["scope"]["deployment"] = name
    bundle["rules"][0]["match"]["deployment"] = name
    bundle_path.write_text(json.dumps(bundle, indent=2))

    commit_deployment_tree(target)
    return target


def _sample_rows(dsn: str, table: str = "call_sample_hot") -> list[dict]:
    """Read the samples from the table the RETENTION CLASS routes them to.

    `retention_class: hot` writes `okg.call_sample_hot`, not
    `call_sample_warm`. Reading the wrong table returns zero rows, which is
    indistinguishable from "no call was sampled" -- the exact confusion this
    file exists to rule out, and what a first run of this test against the
    `warm` table reported.
    """
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT relation, decision, status, attrs, payload "
            f"  FROM okg.{table} ORDER BY sample_id"
        )
        return [
            {
                "relation": r[0], "decision": r[1], "status": r[2],
                "attrs": r[3], "payload": r[4],
            }
            for r in cur.fetchall()
        ]


def test_the_service_started_and_is_sorting_by_cpu(glances: _Glances) -> None:
    """The precondition, checked first and by itself.

    That a THIRD-PARTY service came up under this test's own control is the
    first claim; everything below depends on it. The version is pinned in
    `pyproject.toml`, so a mismatch here means the environment is not the one
    the assertions were written against.

    `top/N` returns Glances' CURRENT sort, and the default is `auto` -- CPU
    when a core is loaded, memory otherwise. Both orders were observed on one
    machine minutes apart, so the fixture passes
    `--sort-processes cpu_percent` and this asserts the server took it. A
    relation called `top_processes` whose meaning changes between runs is
    worse than one that refuses.
    """
    assert glances.get("/api/4/status")["version"] == "4.5.6"
    assert glances.get("/api/4/args")["sort_processes_key"] == "cpu_percent"


def test_both_endpoints_still_return_a_top_level_array(
    glances: _Glances,
) -> None:
    """The shape this whole example exists to exercise.

    Asserted against the running server because it is an assumption about
    somebody else's software: if Glances ever wraps these in an object, the
    bindings' root `rows_path` stops matching and every live call returns
    `response_unusable`. Better to fail here, naming the shape.
    """
    percpu = glances.get("/api/4/percpu")
    assert isinstance(percpu, list) and percpu
    assert "cpu_number" in percpu[0] and "total" in percpu[0]

    # THE PATH THE BINDING ACTUALLY USES. An earlier version of this probe
    # asked for `top/20` and checked `<= 20`, which would have passed on a
    # server that ignored the number entirely -- it proved nothing about the
    # bound the deployment relies on.
    top = glances.get("/api/4/processlist/top/5")
    assert isinstance(top, list)
    # The path literal is the SERVER-side bound. If Glances stopped honouring
    # it the binding's `max_rows` would still cut, but the cost would have
    # moved to the upstream, so it is checked where it is claimed.
    assert 0 < len(top) <= 5
    assert {"pid", "name", "cpu_percent", "memory_percent"} <= set(top[0])


def test_the_walk_reaches_the_live_service_through_mcp(
    glances: _Glances,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """req.lmle.live, driven through `build_server` + `call_tool`.

    Every step is the REGISTERED tool over a real client session. A
    helper-level test cannot tell "the branches are correct" from "the
    branches are reached", and what is being claimed here is that the
    question is answerable through the tools an agent actually has.

    The deny step's "zero adapter calls" is asserted from the call sample's
    `adapter_ran` flag, not from an upstream counter: Glances publishes no
    request count. The stronger form of that claim -- counted outside the
    substrate -- is the hermetic `_RefusingAdapter` test, which fails if the
    adapter is entered at all.
    """
    # `clean_runtime` is resolved here rather than in the signature so the
    # `glances` fixture above is set up FIRST. That ordering is deliberate:
    # if the third-party service cannot start, this test must fail with that
    # message, not with a database one.
    clean_runtime: str = request.getfixturevalue("clean_runtime")

    import anyio
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.deployment.testing import mock_dbos_context
    from okg.substrate.external_backed.live_dispatch import build_live_gate
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.mcp.server import build_server
    from okg.substrate.runtime import admission_workflows

    def payload_of(result):
        structured = getattr(result, "structuredContent", None)
        if structured is not None:
            return structured
        return json.loads(result.content[0].text)

    name = "live-http-ci"
    staged = _stage(tmp_path, name, glances)
    deployment_dsn = _with_database(clean_runtime, DATABASE)
    _recreate_database(clean_runtime, DATABASE)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(staged.parent))
    monkeypatch.setenv("OKG_DSN", deployment_dsn)

    # 1. Provision: mint the machine node and publish a generation.
    monkeypatch.delenv("OKG_MCP_SURFACE_PROBE", raising=False)
    # `provision` IS the bootstrap path: it migrates the empty database,
    # claims ownership, applies the catalog, ingests and publishes. A
    # separate ownership claim ahead of it fails on a fresh database,
    # because there is no `okg` schema yet to hold the claim.
    rc = main([
        "provision", "--deployment", name, "--dsn", deployment_dsn, "--json",
    ])
    envelope = _json_out(capsys)
    rendered = json.dumps(envelope, indent=2, default=str)
    assert rc == 0, rendered
    # A published generation with rows in it. A source can emit a perfectly
    # good machine node and still have none of it reach a generation, and an
    # empty graph would make every `search` below fail for the wrong reason.
    proof = envelope["publish_proof"]
    assert proof["generation_id"], rendered
    assert proof["nodes_added"] >= 1, rendered
    assert proof["dlq_count"] == 0, rendered

    observed: dict[str, object] = {}

    # Warm the process-level Chronos registry OUTSIDE any transaction: the
    # admission workflow's own bootstrap is forbidden inside the psycopg
    # transaction `mock_dbos_context` opens.
    with psycopg.connect(deployment_dsn, autocommit=True) as warm:
        ChronosGraphStore.from_connection(warm)

    # The staged manifest runs `runtime_enforcement: audit`, and a live call
    # is refused while access rules are on (audit or enforce; issue #3923).
    # This walk is about reaching the live service, so the rules are turned
    # off for the served calls only, by the operator override; provisioning
    # above ran under the manifest's own posture. The refusal itself is
    # proved in tests/substrate/external_backed/test_live_call_access_rules.py.
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    with mock_dbos_context():
        monkeypatch.setattr(
            admission_workflows, "_resolve_dsn",
            lambda _deployment: deployment_dsn,
        )
        admission_workflows.register_admission_workflows(deployment=name)
        server = build_server(
            dsn=deployment_dsn, deployment_name=name, allow_writes=False,
        )
        # NO `live_adapters`. The runner is the real HTTP adapter, resolved
        # from the binding, so these calls dial Glances.
        assert sorted(server._okg_live_bindings) == [
            "cpu_per_core", "top_processes",
        ]
        assert sorted(server._okg_live_relation_runners) == [
            "cpu_per_core", "top_processes",
        ]

        async def walk() -> None:
            async with create_connected_server_and_client_session(
                server,
            ) as client:
                # 2. Find the host by its name, the way an agent would.
                hostname = glances.get("/api/4/system")["hostname"]
                result = await client.call_tool(
                    "search", {"query": hostname.split(".")[0], "limit": 10},
                )
                hits = payload_of(result)["results"]
                machine = next(
                    h["node_id"] for h in hits if h["subtype"] == "machine"
                )
                observed["machine"] = machine

                # 3. What it can be ASKED.
                result = await client.call_tool("inspect", {
                    "target": {"kind": "node", "node_id": machine},
                })
                observed["relations"] = payload_of(result)["live_relations"]

                # 4/5. The two live calls.
                for relation in ("cpu_per_core", "top_processes"):
                    result = await client.call_tool("expand", {
                        "input": {"node_id": machine},
                        "edge_type": "contains", "direction": "out",
                        "limit": 5,
                        "live": {"relation": relation, "args": {}},
                    })
                    body = payload_of(result)
                    observed[relation] = body["external_live"]
                    # Kept so the DENIED call below has something to be
                    # compared against. The claim is that a refused live
                    # call costs the caller nothing on the graph side, and
                    # that is only checkable against the count the same
                    # expand returns when the call is allowed.
                    observed[f"{relation}_neighbors"] = len(body["neighbors"])

                # 6. DENIED: the same gate, pointed at a directory with no
                #    bundle. An absent bundle must deny, not default open.
                bundle_less = tmp_path / "no-bundle"
                (bundle_less / name).mkdir(parents=True)
                saved = server._okg_live_gate
                server._okg_live_gate = build_live_gate(
                    name, deployments_root=bundle_less,
                )
                result = await client.call_tool("expand", {
                    "input": {"node_id": machine},
                    "edge_type": "contains", "direction": "out",
                    "limit": 5,
                    "live": {"relation": "cpu_per_core", "args": {}},
                })
                body = payload_of(result)
                observed["denied"] = body["external_live"]
                # The graph rows are a complete answer to the GRAPH question
                # and survive a refused live call.
                observed["denied_neighbors"] = len(body["neighbors"])
                server._okg_live_gate = saved

        anyio.run(walk)

    # `inspect` advertised both relations, with schemas.
    relations = observed["relations"]
    assert sorted(relations) == ["cpu_per_core", "top_processes"]
    for body in relations.values():
        assert body["action"] == "external.host.read_telemetry"
        assert body["args"] == {}

    # Per-core rows, from the real cores of the machine running the test.
    cpu = observed["cpu_per_core"]
    assert cpu["state"] == "ok", cpu
    assert cpu["trust"] == "untrusted"
    assert len(cpu["rows"]) == os.cpu_count(), cpu["rows"]
    numbers = sorted(row["cpu_number"] for row in cpu["rows"])
    assert numbers == list(range(len(numbers)))
    assert all(isinstance(row["total"], (int, float)) for row in cpu["rows"])

    # The process table, cut by the path literal and again by `max_rows`.
    procs = observed["top_processes"]
    assert procs["state"] == "ok", json.dumps(procs, default=str)[:2000]
    # NOT degraded to a digest. A shaped payload over 16 KiB loses its rows
    # and the caller gets a sha256 instead of a process list -- reported,
    # never silent, and still the wrong answer to "what is my machine doing".
    # Twenty rows measured 57,109 fenced bytes on a 12-core host, which is how
    # the path literal came to be five. Asserted because the number that
    # keeps it under the cap depends on the command lines of whatever is
    # busy, so it can regress without anything in this repository changing.
    assert procs["policy"] == "inline", json.dumps(procs, default=str)[:2000]
    assert procs["inline_bytes"] < 16_384, procs["inline_bytes"]
    assert 0 < len(procs["rows"]) <= 8, json.dumps(procs, default=str)[:2000]
    assert all(
        isinstance(row["pid"], int) and isinstance(row["name"], str)
        for row in procs["rows"]
    )
    # Sorted by CPU, descending: that is what `top_processes` claims to mean,
    # and the README's `--sort-processes cpu_percent` is what makes it true.
    cpu_percents = [row["cpu_percent"] for row in procs["rows"]]
    assert cpu_percents == sorted(cpu_percents, reverse=True), cpu_percents

    denied = observed["denied"]
    assert denied["state"] == "denied", denied
    assert denied["error"]["reason"] == "nomos.default_deny.no_active_bundle"
    # The graph half of the answer SURVIVES a refused live call, whole. The
    # same expand, the same node, the same edge type: the only difference is
    # that the live call was denied, so the neighbour count must not move.
    # An earlier `>= 0` here asserted nothing at all -- it is true of any
    # list, including an empty one from a request that failed outright.
    assert observed["cpu_per_core_neighbors"] >= 1, observed
    assert observed["denied_neighbors"] == observed["cpu_per_core_neighbors"]

    # One sample row per call, allowed and denied alike: a rollback cannot
    # un-call an external system, it can only erase the evidence.
    samples = _sample_rows(deployment_dsn)
    by_decision = [(s["relation"], s["decision"]) for s in samples]
    assert by_decision == [
        ("cpu_per_core", "allow"),
        ("top_processes", "allow"),
        ("cpu_per_core", "deny"),
    ], by_decision
    assert samples[0]["attrs"]["adapter_ran"] is True
    assert samples[2]["attrs"]["adapter_ran"] is False
    # The retention class is not decoration: `hot` is 7 days and `warm` is
    # 90, and it is the only thing bounding how long a copy of the host's
    # command lines is kept. Asserted by where the rows LANDED, because a
    # declaration the writer ignored would look identical in the manifest.
    assert _sample_rows(deployment_dsn, "call_sample_warm") == []
    # `sample_policy: inline` keeps the rows WITH the sample, so a finding
    # about a busy machine can cite what was actually read.
    assert samples[0]["payload"], samples[0]
