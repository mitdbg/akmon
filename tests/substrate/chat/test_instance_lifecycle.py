"""The stand-up command: it answers, it is idempotent, and it refuses (PACT
``chat-instance-lifecycle``, ``req.chat-instance-lifecycle.instance-stood-up``).

**This suite does not close its requirement.** The PACT is explicit: because
this child is the producer every other live proof consumes, no arm of
``instance-stood-up`` may be closed by a fixture. What is proved here is that
the lifecycle CODE is correct — the order of its refusals, the post-state of a
refused run, the idempotence of a second invocation, and that "running" is
decided by the instance's own ``GET /api/version`` and by nothing else. That
the code stands up a real Open WebUI container is the LIVE arm, recorded
separately as an ``operator_report``. A green run of this file is a
precondition for that arm, not a substitute for it.

Everything here is real except the container runtime: real child processes,
real listening sockets, real HTTP, real refusals.
"""

from __future__ import annotations

import socket
import subprocess
import threading
import time

import pytest

from okg.substrate.chat.instance import (
    InstanceRecord,
    OUTCOME_ALREADY_RUNNING,
    OUTCOME_STARTED,
    PINNED_VERSION,
    ChatInstanceError,
    DockerRuntime,
    container_name_for,
    normalize_dsn,
    peer_instances,
    probe_version,
    record_instance,
    read_instance,
    stand_up,
    tear_down,
)

from .instance_harness import (
    ProcessRuntime,
    chat_block,
    env,
    free_port,
    reachable_probe,
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


#: `block=None` is a MEANINGFUL argument here (a deployment with no chat
#: block), so the "use the default" case needs its own sentinel.
_DEFAULT_BLOCK = object()


def _stand_up(runtime, tmp_path, *, port, environ=None, block=_DEFAULT_BLOCK, **kwargs):
    return stand_up(
        DEPLOYMENT,
        chat_block=chat_block() if block is _DEFAULT_BLOCK else block,
        graph_dsn=GRAPH_DSN,
        runtime=runtime,
        environ=environ if environ is not None
        else env(**{APP_ENV: APP_DSN, SECRET_ENV: "a-long-random-fixture-key"}),
        instance_port_override=port,
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        ready_timeout=20.0,
        ready_interval=0.05,
        **kwargs,
    )


def _socket_listening(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# The positive arm
# ---------------------------------------------------------------------------

def test_default_chat_state_path_does_not_shadow_deployment(tmp_path, monkeypatch):
    """Default chat state lives outside the deployment lookup root."""
    from okg.substrate.deployments import find_deployment_dir

    user_home = tmp_path / "user-home"
    repo_root = tmp_path / "repo"
    deployment_dir = repo_root / "deployments" / DEPLOYMENT
    deployment_dir.mkdir(parents=True)
    (repo_root / "pyproject.toml").write_text("[project]\n")
    (deployment_dir / "deployment.yaml").write_text(
        "name: fixture-deployment\n"
    )
    monkeypatch.setenv("OKG_USER_HOME", str(user_home))
    monkeypatch.chdir(repo_root)

    path = record_instance(
        InstanceRecord(
            deployment=DEPLOYMENT,
            container=container_name_for(DEPLOYMENT),
            host="127.0.0.1",
            port=8091,
            image="ghcr.io/open-webui/open-webui:v0.11.0",
            app_database=normalize_dsn(APP_DSN, label="app"),
            port_source="operator_flag",
            recorded_at="2026-08-13T00:00:00+00:00",
        )
    )

    assert path == (
        user_home / "chat-instances" / DEPLOYMENT / "chat-instance.json"
    )
    assert not (user_home / "deployments" / DEPLOYMENT).exists()
    assert find_deployment_dir(DEPLOYMENT) == deployment_dir


def test_legacy_default_chat_state_remains_readable_after_path_migration(
    tmp_path, monkeypatch,
):
    """Existing records remain visible while new records use chat-instances."""
    from okg.substrate.deployments import user_state_deployments_root

    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-home"))
    record = InstanceRecord(
        deployment=DEPLOYMENT,
        container=container_name_for(DEPLOYMENT),
        host="127.0.0.1",
        port=8091,
        image="ghcr.io/open-webui/open-webui:v0.11.0",
        app_database=normalize_dsn(APP_DSN, label="app"),
        port_source="operator_flag",
        recorded_at="2026-08-13T00:00:00+00:00",
    )
    record_instance(record, state_root=user_state_deployments_root())

    assert read_instance(DEPLOYMENT) == record


def test_both_current_and_legacy_chat_state_files_refuse_as_ambiguous(
    tmp_path, monkeypatch,
):
    """Two records cannot silently choose a possibly stale instance."""
    from okg.substrate.deployments import (
        user_state_chat_instances_root,
        user_state_deployments_root,
    )

    monkeypatch.setenv("OKG_USER_HOME", str(tmp_path / "user-home"))
    record = InstanceRecord(
        deployment=DEPLOYMENT,
        container=container_name_for(DEPLOYMENT),
        host="127.0.0.1",
        port=8091,
        image="ghcr.io/open-webui/open-webui:v0.11.0",
        app_database=normalize_dsn(APP_DSN, label="app"),
        port_source="operator_flag",
        recorded_at="2026-08-13T00:00:00+00:00",
    )
    record_instance(record, state_root=user_state_deployments_root())
    record_instance(record, state_root=user_state_chat_instances_root())

    with pytest.raises(ChatInstanceError) as caught:
        read_instance(DEPLOYMENT)
    assert caught.value.data["reason"] == "instance_state_ambiguous"

def test_standup_returns_pinned_version_from_the_instance(runtime, tmp_path):
    """The version comes off the instance, not out of our own request.

    The assertion is deliberately made TWICE from two directions: the report's
    version, and an independent ``GET /api/version`` issued after the fact.
    A report that carried the pinned constant through without ever asking
    would pass the first and fail the second.
    """
    port = free_port()
    report = _stand_up(runtime, tmp_path, port=port)

    assert report.outcome == OUTCOME_STARTED
    assert report.version == PINNED_VERSION
    assert report.base_url == f"http://127.0.0.1:{port}"
    assert probe_version(report.base_url) == PINNED_VERSION
    assert _socket_listening(port)

    # The launch argv carries variable NAMES; the DSN and the key ride in the
    # child environment. A secret in an argv is visible in `ps`.
    argv = " ".join(runtime.run_calls[0].run_argv("docker"))
    assert APP_DSN not in argv
    assert "a-long-random-fixture-key" not in argv
    assert "--env DATABASE_URL" in argv
    assert "--env WEBUI_SECRET_KEY" in argv


def test_second_run_reports_already_running_and_creates_no_second_instance(
    runtime, tmp_path,
):
    """Idempotence, decided by an observation rather than by the state file."""
    port = free_port()
    first = _stand_up(runtime, tmp_path, port=port)
    second = _stand_up(runtime, tmp_path, port=port)

    assert first.outcome == OUTCOME_STARTED
    assert second.outcome == OUTCOME_ALREADY_RUNNING
    assert second.version == PINNED_VERSION
    assert second.created_container is False
    assert len(runtime.run_calls) == 1
    assert list(runtime.containers) == [container_name_for(DEPLOYMENT)]


def test_second_run_creates_no_second_app_database(runtime, tmp_path):
    """One deployment, one app-database record, however often the verb runs."""
    port = free_port()
    _stand_up(runtime, tmp_path, port=port)
    before = read_instance(DEPLOYMENT, state_root=tmp_path)

    second = _stand_up(runtime, tmp_path, port=port)

    after = read_instance(DEPLOYMENT, state_root=tmp_path)
    assert second.created_app_database_record is False
    assert before is not None and after is not None
    assert before.app_database == after.app_database
    assert [record.deployment for record in peer_instances(state_root=tmp_path)] == [
        DEPLOYMENT
    ]


# ---------------------------------------------------------------------------
# The refusal arms, with their post-state
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "missing, reason",
    [
        (APP_ENV, "app_database_env_unset"),
        (SECRET_ENV, "secret_key_env_unset"),
    ],
)
def test_missing_operator_state_input_refuses_and_leaves_no_container(
    runtime, tmp_path, missing, reason,
):
    """Each operator-state input, removed one at a time, refuses BY NAME.

    Parameterized over both variables rather than testing one, because a
    refusal that only fires for the first input read is a refusal with a hole
    in it.
    """
    supplied = {APP_ENV: APP_DSN, SECRET_ENV: "a-long-random-fixture-key"}
    supplied.pop(missing)
    port = free_port()

    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=port, environ=env(**supplied))

    assert caught.value.data["reason"] == reason
    assert caught.value.data["env_var"] == missing
    assert missing in str(caught.value)
    # Post-state: nothing created, nothing recorded.
    assert runtime.run_calls == []
    assert runtime.containers == {}
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None


def test_missing_operator_state_input_leaves_no_listening_socket(
    runtime, tmp_path,
):
    """The port the refused run would have bound is still free afterwards.

    Asserted by BINDING it, not by asking the refusal what it thinks it did.
    """
    port = free_port()

    with pytest.raises(ChatInstanceError):
        _stand_up(
            runtime, tmp_path, port=port,
            environ=env(**{SECRET_ENV: "a-long-random-fixture-key"}),
        )

    assert not _socket_listening(port)
    claimer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        claimer.bind(("127.0.0.1", port))  # would raise if anything held it
    finally:
        claimer.close()


def test_disabled_chat_block_refuses(runtime, tmp_path):
    """`enabled: false` is an explicit decision, not a gap to fill in."""
    disabled = chat_block(
        enabled=False, ui=None, models=None, preset=None,
        search_profile=None, mcp=None, deep_links=None,
    )
    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=free_port(), block=disabled)

    assert caught.value.data["reason"] == "chat_block_disabled"
    assert runtime.run_calls == []


def test_no_chat_block_refuses(runtime, tmp_path):
    """A deployment that declares no chat surface gets no instance."""
    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=free_port(), block=None)

    assert caught.value.data["reason"] == "no_chat_block"
    assert runtime.run_calls == []


def test_instance_serving_another_deployment_is_not_adopted(runtime, tmp_path):
    """A container under our name but another deployment's label is refused.

    Adoption is the failure that would put two deployments' chat history and
    app databases in one instance. The observation is the LABEL: a name match
    is not evidence of ownership.
    """
    runtime.adopt_foreign(container_name_for(DEPLOYMENT), owner="someone-else")

    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=free_port())

    assert caught.value.data["reason"] == "foreign_instance_not_adopted"
    assert caught.value.data["owner"] == "someone-else"
    assert runtime.run_calls == []
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None


# ---------------------------------------------------------------------------
# Teardown: the record is forgotten only once the container is proved gone
# ---------------------------------------------------------------------------

def _completed(returncode: int, *, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["docker"], returncode=returncode, stdout=stdout, stderr=stderr,
    )


class _UnreachableRuntime(ProcessRuntime):
    """A runtime whose daemon is down: it cannot answer, so it refuses."""

    def inspect(self, container: str):
        raise ChatInstanceError(
            "docker could not be asked",
            data={"reason": "container_runtime_unavailable", "container": container},
        )


class _RemoveDoesNothingRuntime(ProcessRuntime):
    """A runtime that accepts `remove` and leaves the container running.

    The shape of a `docker rm` that raced a restart policy, or of a runtime
    that reported success it did not achieve. Teardown must not believe it.
    """

    def remove(self, container: str) -> None:
        self.remove_calls.append(container)


def test_teardown_refuses_when_the_runtime_cannot_be_asked_and_keeps_the_record(
    tmp_path,
):
    """A daemon that cannot answer is not a container that is not there.

    Collapsing the two is how ``down`` reported success against a stopped
    Docker: ``inspect`` answered ``None``, ``removed: false`` read as "nothing
    to do", and the record was dropped while the instance kept running and
    kept its port. The record is what the next stand-up would have used to
    name the holder.
    """
    runtime = ProcessRuntime()
    try:
        _stand_up(runtime, tmp_path, port=free_port())
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is not None

        unreachable = _UnreachableRuntime()
        with pytest.raises(ChatInstanceError) as caught:
            tear_down(DEPLOYMENT, runtime=unreachable, state_root=tmp_path)

        assert caught.value.data["reason"] == "container_runtime_unavailable"
        # The record survives: forgetting it is what makes a running instance
        # invisible to everything that would otherwise find it.
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is not None
    finally:
        runtime.shutdown()


def test_teardown_refuses_when_the_container_outlives_its_removal(tmp_path):
    """Removal is confirmed by a second observation, not by an exit code."""
    runtime = _RemoveDoesNothingRuntime()
    try:
        _stand_up(runtime, tmp_path, port=free_port())

        with pytest.raises(ChatInstanceError) as caught:
            tear_down(DEPLOYMENT, runtime=runtime, state_root=tmp_path)

        assert caught.value.data["reason"] == "container_still_present_after_teardown"
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is not None
    finally:
        runtime.shutdown()


def test_teardown_forgets_the_record_when_the_container_is_proved_gone(
    runtime, tmp_path,
):
    """The positive control. Without it the two refusals above could be a
    teardown that never works."""
    _stand_up(runtime, tmp_path, port=free_port())

    result = tear_down(DEPLOYMENT, runtime=runtime, state_root=tmp_path)

    assert result["removed"] is True
    assert result["app_database_dropped"] is False
    assert runtime.inspect(container_name_for(DEPLOYMENT)) is None
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None


class _LingeringPortRuntime(ProcessRuntime):
    """Removal succeeds, but a network helper keeps LISTENING on the port for
    a while, as rootless podman's does (issue #2876 item 3)."""

    def __init__(self, *, linger: float | None) -> None:
        super().__init__()
        self.linger = linger
        self.holders: list[socket.socket] = []
        self.released_at: list[float] = []

    def remove(self, container: str) -> None:
        entry = self.containers.get(container)
        port = entry.spec.host_port if entry is not None else None
        super().remove(container)
        if port is None:
            return
        holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        holder.bind(("127.0.0.1", port))
        holder.listen(1)
        self.holders.append(holder)
        if self.linger is not None:
            def release() -> None:
                # Stamped BEFORE the close, so a teardown that returns the
                # instant the port frees can never look early.
                self.released_at.append(time.monotonic())
                holder.close()

            threading.Timer(self.linger, release).start()

    def shutdown(self) -> None:
        super().shutdown()
        for holder in self.holders:
            holder.close()


def test_teardown_returns_only_once_the_port_is_released(tmp_path):
    """Fail-before: ``down`` returned while the port was still held, so an
    immediate ``up`` refused with ``instance_port_conflict``."""
    from okg.substrate.chat.instance import OCCUPANCY_FREE, port_occupancy

    runtime = _LingeringPortRuntime(linger=0.6)
    try:
        port = free_port()
        _stand_up(runtime, tmp_path, port=port)

        result = tear_down(DEPLOYMENT, runtime=runtime, state_root=tmp_path)
        returned_at = time.monotonic()

        assert port_occupancy(port) == OCCUPANCY_FREE
        assert runtime.released_at and runtime.released_at[0] <= returned_at
        assert result["port_released"] is True
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is None
    finally:
        runtime.shutdown()


def test_teardown_refuses_when_the_port_is_never_released(tmp_path):
    """The wait is bounded, the error names the port, and the record stays."""
    runtime = _LingeringPortRuntime(linger=None)
    try:
        port = free_port()
        _stand_up(runtime, tmp_path, port=port)

        with pytest.raises(ChatInstanceError) as caught:
            tear_down(
                DEPLOYMENT, runtime=runtime, state_root=tmp_path,
                port_release_timeout=0.3, port_release_interval=0.05,
            )

        assert caught.value.data["reason"] == "instance_port_not_released"
        assert caught.value.data["port"] == port
        assert str(port) in str(caught.value)
        assert runtime.inspect(container_name_for(DEPLOYMENT)) is None
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is not None
    finally:
        runtime.shutdown()


# ---------------------------------------------------------------------------
# Issue #2875: the site name reaches the container
# ---------------------------------------------------------------------------

def test_the_site_name_reaches_the_container_as_webui_name(runtime, tmp_path):
    """Fail-before: only DATABASE_URL and WEBUI_SECRET_KEY were passed, so
    the vendor served its default name and readback failed on site_name."""
    _stand_up(runtime, tmp_path, port=free_port())

    (spec,) = runtime.run_calls
    assert spec.env["WEBUI_NAME"] == "Fixture Chat"
    # Names only on the runtime argv; the value rides the child environment.
    assert "Fixture Chat" not in " ".join(spec.run_argv("docker"))


def test_a_changed_site_name_is_an_instance_settings_change(runtime, tmp_path):
    """A running container never re-reads WEBUI_NAME, so 'already running'
    after a rename would report a site name that is not applied."""
    from .instance_harness import VALID_CHAT

    port = free_port()
    _stand_up(runtime, tmp_path, port=port)
    renamed = chat_block(ui={**VALID_CHAT["ui"], "site_name": "Renamed Chat"})

    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=port, block=renamed)

    assert caught.value.data["reason"] == "instance_settings_changed"
    assert "site_name" in str(caught.value)
    assert len(runtime.run_calls) == 1


def test_no_declared_site_name_sets_no_webui_name(runtime, tmp_path):
    """Control: an undeclared site name leaves the vendor default alone."""
    from .instance_harness import VALID_CHAT

    ui = {k: v for k, v in VALID_CHAT["ui"].items() if k != "site_name"}
    _stand_up(runtime, tmp_path, port=free_port(), block=chat_block(ui=ui))

    (spec,) = runtime.run_calls
    assert "WEBUI_NAME" not in spec.env


def test_docker_inspect_tells_an_absent_container_from_an_unreachable_daemon():
    """``docker inspect`` exits non-zero for both; only the message differs.

    Absence is matched POSITIVELY. Anything else — a daemon that is not
    running, a socket we lack permission on, a client/server mismatch — is
    "could not ask" and refuses, because the reading that fails closed is the
    only one that cannot silently forget a live container.
    """
    absent = DockerRuntime(runner=lambda argv, env: _completed(
        1, stderr="Error: No such object: okg-chat-fixture-deployment",
    ))
    assert absent.inspect("okg-chat-fixture-deployment") is None

    daemon_down = DockerRuntime(runner=lambda argv, env: _completed(
        1,
        stderr=(
            "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. "
            "Is the docker daemon running?"
        ),
    ))
    with pytest.raises(ChatInstanceError) as caught:
        daemon_down.inspect("okg-chat-fixture-deployment")
    assert caught.value.data["reason"] == "container_runtime_unavailable"
    assert "Cannot connect" in caught.value.data["detail"]

    # And a runtime binary that is not installed at all.
    def _missing(argv, env):
        raise FileNotFoundError(2, "No such file or directory: 'docker'")

    with pytest.raises(ChatInstanceError) as missing:
        DockerRuntime(runner=_missing).inspect("okg-chat-fixture-deployment")
    assert missing.value.data["reason"] == "container_runtime_unavailable"


def test_docker_stop_and_remove_raise_rather_than_discarding_their_exit_codes():
    """Their exit codes are the only evidence the container is gone."""
    failing = DockerRuntime(runner=lambda argv, env: _completed(
        1, stderr="Error response from daemon: cannot remove container: in use",
    ))
    for verb, call in (("stop", failing.stop), ("remove", failing.remove)):
        with pytest.raises(ChatInstanceError) as caught:
            call("okg-chat-fixture-deployment")
        assert caught.value.data["reason"] == f"container_{verb}_failed"
        assert caught.value.data["exit_code"] == 1

    # Control: a container the runtime says is already gone is not a failure,
    # or an idempotent `down` could never complete.
    gone = DockerRuntime(runner=lambda argv, env: _completed(
        1, stderr="Error response from daemon: No such container: okg-chat-x",
    ))
    gone.stop("okg-chat-x")
    gone.remove("okg-chat-x")


def test_a_wrong_version_tears_down_rather_than_reporting_success(tmp_path):
    """An instance that answers the WRONG version is not a stood-up instance.

    Every measured fact in this program was taken at the pinned version, so an
    instance on another one silently invalidates them. The container is
    removed before the refusal is raised, which is what keeps the "a refused
    run leaves nothing" post-state true for the two failures that can only be
    detected after a launch.
    """
    runtime = ProcessRuntime(version="0.10.0")
    try:
        with pytest.raises(ChatInstanceError) as caught:
            _stand_up(runtime, tmp_path, port=free_port())
        assert caught.value.data["reason"] == "version_not_pinned"
        assert caught.value.data["observed"] == "0.10.0"
        assert runtime.remove_calls == [container_name_for(DEPLOYMENT)]
        assert read_instance(DEPLOYMENT, state_root=tmp_path) is None
    finally:
        runtime.shutdown()


def _existing_instance(
    runtime, tmp_path, *, port, running, image=None, tested_images=None
):
    """Create a live stub or an owned stopped container on a never-bound port."""
    from dataclasses import replace

    container = container_name_for(DEPLOYMENT)
    if running:
        _stand_up(
            runtime, tmp_path, port=port, image=image, tested_images=tested_images
        )
    else:
        # A just-killed HTTP server can leave TIME_WAIT and obscure the guard.
        runtime.adopt_foreign(container, owner=DEPLOYMENT)
        runtime.containers[container].spec = replace(
            runtime.containers[container].spec,
            host_port=port,
            image=image or runtime.containers[container].spec.image,
            env={"DATABASE_URL": APP_DSN, "WEBUI_SECRET_KEY": "fixture-key"},
        )
        record_instance(
            InstanceRecord(
                deployment=DEPLOYMENT,
                container=container,
                host="127.0.0.1",
                port=port,
                image=runtime.containers[container].spec.image,
                app_database=normalize_dsn(APP_DSN, label="app"),
                port_source="operator_flag",
                recorded_at="2026-08-10T00:00:00Z",
            ),
            state_root=tmp_path,
        )
    return container, runtime.containers[container]


_MISSING_IMAGE = object()
_OTHER_TESTED_IMAGE = "registry.example/other-webui:v0.11.0"


@pytest.mark.parametrize("running", [True, False], ids=["running", "stopped"])
@pytest.mark.parametrize(
    "observed, reason",
    [
        (_OTHER_TESTED_IMAGE, "existing_instance_image_mismatch"),
        ("registry.example/untested:v9", "existing_instance_image_mismatch"),
        (_MISSING_IMAGE, "existing_instance_image_unmeasured"),
        (None, "existing_instance_image_unmeasured"),
        ("", "existing_instance_image_unmeasured"),
        ("  ", "existing_instance_image_unmeasured"),
        (17, "existing_instance_image_unmeasured"),
    ],
    ids=["other-tested", "untested", "missing", "null", "empty", "blank", "non-string"],
)
def test_existing_other_image_refuses_before_reuse_or_restart(
    runtime,
    tmp_path,
    monkeypatch,
    running,
    observed,
    reason,
):
    """The same HTTP version cannot substitute for the inspected image."""
    from okg.substrate.chat import instance as instance_module
    from okg.substrate.chat.instance import PINNED_IMAGE_REF

    port = free_port()
    container, entry = _existing_instance(runtime, tmp_path, port=port, running=running)
    process = entry.process
    state_before = {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
    inspect = runtime.inspect

    def other_image(name):
        facts = inspect(name)
        if facts is not None:
            if observed is _MISSING_IMAGE:
                facts.pop("image")
            else:
                facts["image"] = observed
        return facts

    calls = []

    def unexpected(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("image refusal must precede HTTP and lifecycle actions")

    with monkeypatch.context() as patch:
        patch.setattr(runtime, "inspect", other_image)
        patch.setattr(instance_module, "probe_version", unexpected)
        for operation in ("run", "start", "stop", "remove"):
            patch.setattr(runtime, operation, unexpected)
        with pytest.raises(ChatInstanceError) as caught:
            _stand_up(
                runtime,
                tmp_path,
                port=port,
                tested_images={
                    PINNED_IMAGE_REF: PINNED_VERSION,
                    _OTHER_TESTED_IMAGE: PINNED_VERSION,
                },
            )
    assert caught.value.data["reason"] == reason
    assert caught.value.data["expected_image"] == PINNED_IMAGE_REF
    assert caught.value.data["observed_image"] == (
        observed if isinstance(observed, str) else None
    )
    assert calls == []
    assert entry.process is process
    assert entry.running is running
    assert runtime.containers[container] is entry
    assert {p: p.read_bytes() for p in tmp_path.rglob("*.json")} == state_before


@pytest.mark.parametrize("observed", [_MISSING_IMAGE, _OTHER_TESTED_IMAGE])
def test_foreign_owner_refusal_precedes_existing_image_guard(
    runtime,
    tmp_path,
    monkeypatch,
    observed,
):
    runtime.adopt_foreign(container_name_for(DEPLOYMENT), owner="someone-else")
    inspect = runtime.inspect

    def foreign_image(name):
        facts = inspect(name)
        if observed is _MISSING_IMAGE:
            facts.pop("image")
        else:
            facts["image"] = observed
        return facts

    monkeypatch.setattr(runtime, "inspect", foreign_image)
    with pytest.raises(ChatInstanceError) as caught:
        _stand_up(runtime, tmp_path, port=free_port())
    assert caught.value.data["reason"] == "foreign_instance_not_adopted"
    assert caught.value.data["owner"] == "someone-else"
    assert runtime.run_calls == runtime.start_calls == runtime.remove_calls == []
    assert read_instance(DEPLOYMENT, state_root=tmp_path) is None


@pytest.mark.parametrize("running", [True, False], ids=["reuse", "restart"])
@pytest.mark.parametrize(
    "selected", [None, _OTHER_TESTED_IMAGE], ids=["default", "selected"]
)
def test_matching_existing_image_preserves_reuse_and_restart(
    runtime,
    tmp_path,
    running,
    selected,
):
    from okg.substrate.chat.instance import PINNED_IMAGE_REF

    port = free_port()
    tested = {PINNED_IMAGE_REF: PINNED_VERSION, _OTHER_TESTED_IMAGE: PINNED_VERSION}
    container, entry = _existing_instance(
        runtime,
        tmp_path,
        port=port,
        running=running,
        image=selected,
        tested_images=tested,
    )
    process = entry.process
    report = _stand_up(
        runtime, tmp_path, port=port, image=selected, tested_images=tested
    )
    assert report.outcome == (OUTCOME_ALREADY_RUNNING if running else OUTCOME_STARTED)
    assert report.image == (selected or PINNED_IMAGE_REF)
    assert report.version == probe_version(report.base_url) == PINNED_VERSION
    assert report.created_container is False
    assert runtime.start_calls == ([] if running else [container])
    assert len(runtime.run_calls) == int(running)
    assert entry.running is True
    assert (entry.process is process) is running


def test_docker_inspect_observes_config_image_without_a_fallback():
    import json

    for config, expected in (
        ({"Image": _OTHER_TESTED_IMAGE}, _OTHER_TESTED_IMAGE),
        ({}, ""),
    ):
        runtime = DockerRuntime(
            runner=lambda argv, env: _completed(
                0,
                stdout=json.dumps([{"Id": "owned", "Config": config, "State": {}}]),
            )
        )
        assert runtime.inspect("owned")["image"] == expected
