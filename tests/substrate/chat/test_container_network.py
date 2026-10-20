"""How the chat container reaches the host's loopback (issue #2876 item 2,
PACT ``chat-linux-loopback-forwarding``).

Every launch here goes through the real :func:`stand_up` or the real
:class:`DockerRuntime` with the command line captured, never a container: the
claims are about the exact ``podman``/``docker`` arguments per platform and
runtime, and about the environment the container starts with.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pytest

from okg.substrate.chat.container_network import (
    NETWORK_GATEWAY,
    NETWORK_HOST,
    NETWORK_PASTA,
    loopback_ports,
    resolve_network_mode,
)
from okg.substrate.chat.instance import (
    PINNED_VERSION,
    ChatInstanceError,
    DockerRuntime,
    InstanceRecord,
    container_name_for,
    normalize_dsn,
    read_instance,
    record_instance,
    settings_fingerprint,
    stand_up,
)
from okg.substrate.chat.setup import SetupOptions, resolve_sync_instance_url

from .instance_harness import chat_block, env, free_port, reachable_probe
from .test_chat_setup import DEPLOYMENT as SETUP_DEPLOYMENT
from .test_chat_setup import Seams, _env, _instance, _run

DEPLOYMENT = "fixture-deployment"
GRAPH_DSN = "postgresql://okg:pw@127.0.0.1:5432/okg_fixture_graph"
#: Never dialled: stand-up's reachability probe is faked. Only its host and
#: port matter (the app database port pasta must forward). Port 5439 keeps
#: it off the shared okg-pg port 5433 the DSN guard watches for.
APP_DSN = "postgresql://okg:pw@127.0.0.1:5439/okg_fixture_chat_app"
APP_ENV = "OKG_CHAT_APP_DATABASE_URL"
SECRET_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"
SECRET = "a-long-random-fixture-key"
OLLAMA = {"base_url": "http://host.docker.internal:11434/v1"}
#: The site-cut variables every spec carries (instance.CONTAINER_FIXED_ENV),
#: in the sorted position the runtime passes them.
_FIXED_ENV_ARGV = (
    "--env", "ENABLE_EASTER_EGGS", "--env", "ENABLE_VERSION_UPDATE_CHECK",
)


PODMAN_VERSION = "podman version 5.8.2"
DOCKER_VERSION = "Docker version 27.3.1, build ce12230"
ROOTFUL_OPTIONS = "[name=seccomp,profile=builtin name=cgroupns]"
ROOTLESS_OPTIONS = "[name=seccomp,profile=builtin name=rootless name=cgroupns]"


class CapturingRunner:
    """A command runner that records argv and child env, answers the runtime
    questions (``--version``, ``info``) as configured (``None``: the command
    fails), and answers like a runtime that has no such container and
    launches one."""

    def __init__(
        self, *, version: str | None = PODMAN_VERSION, remote: str | None = "false",
        security: str | None = ROOTFUL_OPTIONS,
    ) -> None:
        self.calls: list[tuple[list[str], dict[str, str] | None]] = []
        self.answers = {
            ("--version",): version,
            ("info", "--format", "{{.Host.ServiceIsRemote}}"): remote,
            ("info", "--format", "{{.SecurityOptions}}"): security,
        }

    def __call__(
        self, argv: Sequence[str], env: Mapping[str, str] | None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append((list(argv), dict(env) if env is not None else None))
        question = tuple(argv[1:])
        if question in self.answers:
            answer = self.answers[question]
            if answer is None:
                return subprocess.CompletedProcess(argv, 125, "", "cannot connect")
            return subprocess.CompletedProcess(argv, 0, answer + "\n", "")
        if argv[1] == "inspect":
            return subprocess.CompletedProcess(argv, 1, "", "Error: no such container")
        return subprocess.CompletedProcess(argv, 0, "cid\n", "")

    def run_call(self) -> tuple[list[str], dict[str, str]]:
        (argv, child_env), = [c for c in self.calls if c[0][1] == "run"]
        assert child_env is not None
        return argv, child_env


def _version(_url: str, _timeout: float) -> tuple[int, str]:
    return 200, json.dumps({"version": PINNED_VERSION})


def _up(
    tmp_path: Path, *, binary: str, platform: str, network: str = "auto",
    block: Any = None, host: str = "127.0.0.1", port: int | None = None,
    app_dsn: str = APP_DSN, runner: CapturingRunner | None = None,
) -> tuple[Any, CapturingRunner, int]:
    if runner is None:
        runner = CapturingRunner(
            version=PODMAN_VERSION if "podman" in binary else DOCKER_VERSION,
        )
    port = port or free_port()
    report = stand_up(
        DEPLOYMENT,
        chat_block=block if block is not None else chat_block(),
        graph_dsn=GRAPH_DSN,
        runtime=DockerRuntime(binary=binary, runner=runner),
        environ=env(**{APP_ENV: app_dsn, SECRET_ENV: SECRET}),
        instance_port_override=port,
        host=host,
        state_root=tmp_path,
        reachability_probe=reachable_probe,
        http_get=_version,
        ready_timeout=1.0,
        ready_interval=0.01,
        sleep=lambda _s: None,
        container_network=network,
        platform=platform,
    )
    return report, runner, port


def _strip_labels(argv: list[str]) -> list[str]:
    out: list[str] = []
    skip = False
    for arg in argv:
        if skip:
            skip = False
            continue
        if arg == "--label":
            skip = True
            continue
        out.append(arg)
    return out


# ---------------------------------------------------------------------------
# Linux + podman: pasta forwarding of each host-loopback port
# ---------------------------------------------------------------------------

def test_linux_podman_forwards_each_loopback_port_through_pasta(tmp_path: Path) -> None:
    """Fail-before: podman on Linux got --add-host host.docker.internal:host-gateway,
    which resolves to the host's external address, where mcp-serve refuses."""
    block = chat_block(provider=OLLAMA)
    report, runner, port = _up(tmp_path, binary="podman", platform="linux", block=block)
    argv, child_env = runner.run_call()

    assert _strip_labels(argv) == [
        "podman", "run", "--detach", "--name", container_name_for(DEPLOYMENT),
        # graph server 8101, app database 5439, Ollama through the alias 11434
        "--network", "pasta:-T,5439,-T,8101,-T,11434",
        "--publish", f"127.0.0.1:{port}:8080",
        "--add-host", "host.docker.internal:127.0.0.1",
        "--env", "DATABASE_URL", *_FIXED_ENV_ARGV,
        "--env", "WEBUI_NAME", "--env", "WEBUI_SECRET_KEY",
        report.image,
    ]
    # The container dials the app database on 127.0.0.1 itself: no rewrite.
    assert child_env["DATABASE_URL"] == APP_DSN
    assert "HOST" not in argv and "PORT" not in argv
    assert report.network_mode == NETWORK_PASTA
    assert report.forwarded_ports == (5439, 8101, 11434)
    recorded = read_instance(DEPLOYMENT, state_root=tmp_path)
    assert recorded.network_mode == NETWORK_PASTA
    assert recorded.forwarded_ports == (5439, 8101, 11434)


def test_a_remote_provider_and_app_database_are_not_forwarded(tmp_path: Path) -> None:
    block = chat_block(provider={"base_url": "http://gpu.example.invalid:8000/v1"})
    report, runner, _port = _up(
        tmp_path, binary="/usr/bin/podman", platform="linux", block=block,
        app_dsn="postgresql://okg:pw@db.example.invalid:5432/okg_app",
    )
    argv, _env_ = runner.run_call()
    assert argv[argv.index("--network") + 1] == "pasta:-T,8101"
    assert report.forwarded_ports == (8101,)


def test_a_forwarded_port_equal_to_the_container_port_refuses(tmp_path: Path) -> None:
    block = chat_block(provider={"base_url": "http://127.0.0.1:8080/v1"})
    with pytest.raises(ChatInstanceError) as caught:
        _up(tmp_path, binary="podman", platform="linux", block=block)
    assert caught.value.data["reason"] == "container_network_port_clash"


def test_podmans_docker_wrapper_is_podman(tmp_path: Path) -> None:
    """Review item 4, fail-before: the runtime was classed by its file name,
    so podman's `docker` wrapper was docker. `docker --version` answers
    `podman version ...`, and that decides."""
    report, runner, _port = _up(
        tmp_path, binary="/usr/bin/docker", platform="linux",
        runner=CapturingRunner(version=PODMAN_VERSION),
    )
    argv, _env_ = runner.run_call()
    assert argv[argv.index("--network") + 1] == "pasta:-T,5439,-T,8101"
    assert report.network_mode == NETWORK_PASTA


@pytest.mark.parametrize(
    ("version", "remote", "reason"),
    [
        (PODMAN_VERSION, "true", "container_network_choice_required"),
        ("podman-remote version 5.8.2", None, "container_network_choice_required"),
    ],
)
def test_auto_refuses_a_remote_or_unidentified_podman_service(
    tmp_path: Path, version: str, remote: str | None, reason: str,
) -> None:
    """Review item 4: pasta forwards the SERVICE host's loopback."""
    with pytest.raises(ChatInstanceError) as caught:
        _up(
            tmp_path, binary="podman", platform="linux",
            runner=CapturingRunner(version=version, remote=remote),
        )
    assert caught.value.data["reason"] == reason
    assert caught.value.data["podman_service_remote"] is True


def test_a_local_podman_whose_info_is_unanswered_still_gets_pasta(tmp_path: Path) -> None:
    report, _runner, _port = _up(
        tmp_path, binary="podman", platform="linux",
        runner=CapturingRunner(version=PODMAN_VERSION, remote=None),
    )
    assert report.network_mode == NETWORK_PASTA


def test_live_tools_url_on_loopback_is_forwarded(tmp_path: Path) -> None:
    """Review item 3, fail-before: chat.mcp.live_tools.url was not forwarded."""
    block = chat_block(mcp={
        "port": 8101, "tools": ["inspect", "search"],
        "live_tools": {"names": ["now"], "url": "http://host.docker.internal:8301/mcp"},
    })
    report, _runner, _port = _up(tmp_path, binary="podman", platform="linux", block=block)
    assert report.forwarded_ports == (5439, 8101, 8301)


# ---------------------------------------------------------------------------
# Linux + docker: host network only when asked, Open WebUI on 127.0.0.1
# ---------------------------------------------------------------------------

def test_linux_docker_auto_refuses_and_asks_for_a_choice(tmp_path: Path) -> None:
    """Review item 1, fail-before: auto picked the host network for docker,
    from where the container reaches every host-loopback service."""
    with pytest.raises(ChatInstanceError) as caught:
        _up(tmp_path, binary="docker", platform="linux")
    assert caught.value.data["reason"] == "container_network_choice_required"
    assert "EVERY service" in caught.value.message
    assert "--container-network host" in caught.value.fix_hint


def test_rootless_docker_host_network_refuses(tmp_path: Path) -> None:
    """Review item 2, fail-before: rootless docker's --network host is
    rootlesskit's namespace; the launch would wait 120 s and fail."""
    with pytest.raises(ChatInstanceError) as caught:
        _up(
            tmp_path, binary="docker", platform="linux", network="host",
            runner=CapturingRunner(version=DOCKER_VERSION, security=ROOTLESS_OPTIONS),
        )
    assert caught.value.data["reason"] == "host_network_rootless_docker"


def test_host_network_refuses_when_docker_info_does_not_answer(tmp_path: Path) -> None:
    with pytest.raises(ChatInstanceError) as caught:
        _up(
            tmp_path, binary="docker", platform="linux", network="host",
            runner=CapturingRunner(version=DOCKER_VERSION, security=None),
        )
    assert caught.value.data["reason"] == "host_network_rootless_unknown"


def test_explicit_host_network_on_rootful_docker_binds_loopback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HOST=127.0.0.1 is set even when the operator's shell exports HOST=0.0.0.0:
    the image's start.sh defaults HOST to 0.0.0.0, which would publish the
    site on every interface of the host. The note states the full reach."""
    monkeypatch.setenv("HOST", "0.0.0.0")
    monkeypatch.setenv("PORT", "9")
    report, runner, port = _up(
        tmp_path, binary="docker", platform="linux", network="host",
    )
    argv, child_env = runner.run_call()
    assert any("EVERY service" in note for note in report.notes)

    assert _strip_labels(argv) == [
        "docker", "run", "--detach", "--name", container_name_for(DEPLOYMENT),
        "--network", "host",
        "--add-host", "host.docker.internal:127.0.0.1",
        "--env", "DATABASE_URL", *_FIXED_ENV_ARGV,
        "--env", "HOST", "--env", "PORT",
        "--env", "WEBUI_NAME", "--env", "WEBUI_SECRET_KEY",
        report.image,
    ]
    assert "--publish" not in argv
    assert child_env["HOST"] == "127.0.0.1"
    assert child_env["PORT"] == str(port)
    assert child_env["DATABASE_URL"] == APP_DSN
    assert report.base_url == f"http://127.0.0.1:{port}"
    assert report.network_mode == NETWORK_HOST
    assert read_instance(DEPLOYMENT, state_root=tmp_path).network_mode == NETWORK_HOST


@pytest.mark.parametrize("host", ["0.0.0.0", "10.0.0.5"])
def test_host_network_refuses_a_non_loopback_bind(tmp_path: Path, host: str) -> None:
    with pytest.raises(ChatInstanceError) as caught:
        _up(tmp_path, binary="docker", platform="linux", host=host, network="host")
    assert caught.value.data["reason"] == "host_network_needs_loopback"


def test_explicit_host_mode_on_linux_podman_also_sets_host(tmp_path: Path) -> None:
    _report, runner, port = _up(
        tmp_path, binary="podman", platform="linux", network="host",
    )
    argv, child_env = runner.run_call()
    assert argv[argv.index("--network") + 1] == "host"
    assert (child_env["HOST"], child_env["PORT"]) == ("127.0.0.1", str(port))


# ---------------------------------------------------------------------------
# macOS: unchanged
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("binary", ["docker", "podman"])
def test_macos_keeps_the_gateway_alias(tmp_path: Path, binary: str) -> None:
    report, runner, port = _up(tmp_path, binary=binary, platform="darwin")
    argv, child_env = runner.run_call()

    assert _strip_labels(argv) == [
        binary, "run", "--detach", "--name", container_name_for(DEPLOYMENT),
        "--publish", f"127.0.0.1:{port}:8080",
        "--add-host", "host.docker.internal:host-gateway",
        "--env", "DATABASE_URL", *_FIXED_ENV_ARGV,
        "--env", "WEBUI_NAME", "--env", "WEBUI_SECRET_KEY",
        report.image,
    ]
    assert "--network" not in argv
    assert child_env["DATABASE_URL"] == APP_DSN.replace("127.0.0.1", "host.docker.internal")
    assert report.network_mode == NETWORK_GATEWAY


@pytest.mark.parametrize("mode", ["pasta", "host"])
def test_macos_refuses_the_linux_modes(tmp_path: Path, mode: str) -> None:
    with pytest.raises(ChatInstanceError) as caught:
        _up(tmp_path, binary="podman", platform="darwin", network=mode)
    assert caught.value.data["reason"] == "container_network_unsupported"


def test_pasta_needs_podman() -> None:
    with pytest.raises(ChatInstanceError) as caught:
        resolve_network_mode("pasta", runtime_binary="docker", platform="linux")
    assert caught.value.data["reason"] == "container_network_unsupported"


def test_an_unknown_runtime_on_linux_says_it_used_the_gateway() -> None:
    mode, note = resolve_network_mode("auto", runtime_binary="nerdctl", platform="linux")
    assert mode == NETWORK_GATEWAY
    assert note and "--container-network" in note


# ---------------------------------------------------------------------------
# The fingerprint, the record and the port set
# ---------------------------------------------------------------------------

def test_the_network_mode_changes_the_fingerprint_and_gateway_keeps_the_old_one() -> None:
    launch_env = {"DATABASE_URL": "x", "WEBUI_SECRET_KEY": "y"}
    legacy = settings_fingerprint(launch_env)
    assert settings_fingerprint(launch_env, network=None) == legacy
    assert settings_fingerprint(launch_env, network="pasta:8101") != legacy
    assert settings_fingerprint(launch_env, network="pasta:8101") != settings_fingerprint(
        launch_env, network="pasta:8101,5439",
    )


def test_a_gateway_container_is_refused_as_changed_under_pasta(tmp_path: Path) -> None:
    """A running container started in another mode is not answered
    `already_running`: its network is fixed at creation."""
    _report, runner, port = _up(tmp_path, binary="podman", platform="darwin")
    argv, _ = runner.run_call()
    labels = dict(
        argv[i + 1].split("=", 1) for i, a in enumerate(argv) if a == "--label"
    )

    class Existing(CapturingRunner):
        def __call__(self, argv, env):  # noqa: ANN001
            if argv[1] == "inspect":
                payload = [{
                    "Id": "cid", "Config": {"Labels": labels, "Image": _report.image},
                    "State": {"Running": True},
                }]
                return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")
            return super().__call__(argv, env)

    with pytest.raises(ChatInstanceError) as caught:
        stand_up(
            DEPLOYMENT, chat_block=chat_block(), graph_dsn=GRAPH_DSN,
            runtime=DockerRuntime(binary="podman", runner=Existing()),
            environ=env(**{APP_ENV: APP_DSN, SECRET_ENV: SECRET}),
            instance_port_override=port, state_root=tmp_path,
            reachability_probe=reachable_probe, http_get=_version,
            container_network="pasta", platform="linux",
        )
    assert caught.value.data["reason"] == "instance_settings_changed"
    # Review item 6: both modes named, and keeping the instance is offered.
    assert caught.value.data["recorded_network_mode"] == NETWORK_GATEWAY
    assert caught.value.data["desired_network_mode"] == NETWORK_PASTA
    assert "--container-network gateway" in caught.value.fix_hint


def _existing_without_fingerprint(tmp_path: Path) -> tuple[int, Any]:
    """A running container from before the settings fingerprint existed:
    its labels carry the deployment and owner only."""
    _report, runner, port = _up(tmp_path, binary="podman", platform="darwin")
    argv, _ = runner.run_call()
    labels = {
        key: value for key, value in (
            argv[i + 1].split("=", 1) for i, a in enumerate(argv) if a == "--label"
        ) if key != "okg.settings-fingerprint"
    }

    class Existing(CapturingRunner):
        def __call__(self, argv, env):  # noqa: ANN001
            if argv[1] == "inspect":
                payload = [{
                    "Id": "cid", "Config": {"Labels": labels, "Image": _report.image},
                    "State": {"Running": True},
                }]
                return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")
            return super().__call__(argv, env)

    return port, Existing()


@pytest.mark.parametrize(
    ("network", "platform", "refused"),
    [("pasta", "linux", True), ("gateway", "linux", False), ("auto", "darwin", False)],
)
def test_a_container_without_a_fingerprint_is_refused_outside_gateway(
    tmp_path: Path, network: str, platform: str, refused: bool,
) -> None:
    """Review item 5, fail-before: a pre-fingerprint container was answered
    `already_running` under pasta and recorded as pasta, a mode it does not
    run. It predates the modes, so it runs gateway: kept only there."""
    port, runner = _existing_without_fingerprint(tmp_path)

    def run() -> Any:
        return stand_up(
            DEPLOYMENT, chat_block=chat_block(), graph_dsn=GRAPH_DSN,
            runtime=DockerRuntime(binary="podman", runner=runner),
            environ=env(**{APP_ENV: APP_DSN, SECRET_ENV: SECRET}),
            instance_port_override=port, state_root=tmp_path,
            reachability_probe=reachable_probe, http_get=_version,
            container_network=network, platform=platform,
        )

    if refused:
        with pytest.raises(ChatInstanceError) as caught:
            run()
        assert caught.value.data["reason"] == "instance_settings_changed"
        assert caught.value.data["fingerprint_recorded"] is False
        assert caught.value.data["recorded_network_mode"] == NETWORK_GATEWAY
        assert "--container-network gateway" in caught.value.fix_hint
    else:
        assert run().outcome == "already_running"


def test_a_record_from_before_the_mode_reads_as_gateway(tmp_path: Path) -> None:
    record_instance(
        InstanceRecord(
            deployment=DEPLOYMENT, container=container_name_for(DEPLOYMENT),
            host="127.0.0.1", port=3000, image="img",
            app_database=normalize_dsn(APP_DSN, label="app"),
            port_source="operator_flag", recorded_at="2026-09-28T00:00:00Z",
        ),
        state_root=tmp_path,
    )
    path = tmp_path / DEPLOYMENT / "chat-instance.json"
    raw = json.loads(path.read_text())
    raw.pop("network_mode")
    path.write_text(json.dumps(raw))
    assert read_instance(DEPLOYMENT, state_root=tmp_path).network_mode == NETWORK_GATEWAY


def test_loopback_ports_reads_the_provider_through_the_alias() -> None:
    block = chat_block(provider={"base_url": "https://localhost/v1"})
    assert loopback_ports(
        block, app_database_host="127.0.0.1", app_database_port=5432,
        alias="host.docker.internal", extra=(9000,),
    ) == (443, 5432, 8101, 9000)


# ---------------------------------------------------------------------------
# setup and sync register the same 127.0.0.1 graph URL
# ---------------------------------------------------------------------------

class _PastaSeams(Seams):
    def __init__(self, state_root: Path) -> None:
        super().__init__()
        self.state_root = state_root

    def stand_up(self, deployment: str, **kwargs: Any) -> Any:
        _record(self.state_root, network_mode=NETWORK_PASTA, forwarded=(5432, 8101))
        return SimpleNamespace(
            **{
                **vars(super().stand_up(deployment, **kwargs)),
                "base_url": "http://127.0.0.1:3000",
                "network_mode": NETWORK_PASTA,
            },
        )


def _record(state_root: Path, *, network_mode: str, forwarded: tuple[int, ...]) -> None:
    record_instance(
        InstanceRecord(
            deployment=SETUP_DEPLOYMENT, container=container_name_for(SETUP_DEPLOYMENT),
            host="127.0.0.1", port=3000, image="img",
            app_database=normalize_dsn(APP_DSN, label="app"),
            port_source="operator_flag", recorded_at="2026-09-28T00:00:00Z",
            network_mode=network_mode, forwarded_ports=forwarded,
        ),
        state_root=state_root,
    )


def test_setup_registers_the_loopback_graph_url_under_pasta(tmp_path: Path) -> None:
    """Fail-before: setup registered http://host.docker.internal:8101/mcp,
    which a Linux container cannot reach on a loopback-bound mcp-serve."""
    state_root = tmp_path / "state"
    report, seams = _run(
        _instance(), _env(), tmp_path, state_root=state_root,
        seams=_PastaSeams(state_root),
        options=SetupOptions(container_network="auto"),
    )
    assert report.ok, report.to_dict()
    assert seams.stand_up_calls[0]["container_network"] == "auto"
    (inputs, _client), = seams.sync_calls
    assert inputs.mcp_instance_url == "http://127.0.0.1:8101/mcp"
    saved = json.loads((state_root / SETUP_DEPLOYMENT / "graph-endpoint.json").read_text())
    assert saved["instance_url"] == "http://127.0.0.1:8101/mcp"
    # mcp-serve is told only about the loopback Host header.
    assert "host.docker.internal:*" not in report.steps["graph"].data["command"]


def test_setup_forwards_a_loopback_probe_url_port(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    _report, seams = _run(
        _instance(), _env(), tmp_path, state_root=state_root,
        seams=_PastaSeams(state_root),
        options=SetupOptions(mcp_probe_url="http://127.0.0.1:8201/mcp"),
    )
    assert seams.stand_up_calls[0]["extra_forward_ports"] == (8201,)


@pytest.mark.parametrize(
    ("instance_url", "forwarded"),
    [
        ("http://host.docker.internal:8301/mcp", (8301,)),
        ("http://127.0.0.1:8302/mcp", (8302,)),
        ("http://graph.example.invalid:8303/mcp", ()),
    ],
)
def test_setup_forwards_a_loopback_mcp_instance_url_port(
    tmp_path: Path, instance_url: str, forwarded: tuple[int, ...],
) -> None:
    """Review item 3, fail-before: a declared loopback --mcp-instance-url
    was registered but its port was not forwarded."""
    state_root = tmp_path / "state"
    _report, seams = _run(
        _instance(), _env(), tmp_path, state_root=state_root,
        seams=_PastaSeams(state_root),
        options=SetupOptions(mcp_instance_url=instance_url),
    )
    assert seams.stand_up_calls[0]["extra_forward_ports"] == forwarded


def test_plain_sync_reuses_the_loopback_url_for_a_forwarded_port(tmp_path: Path) -> None:
    """No graph-endpoint record: under pasta a forwarded loopback port is
    reachable, so sync registers the probe URL instead of refusing."""
    _record(tmp_path, network_mode=NETWORK_PASTA, forwarded=(8101,))
    url, note = resolve_sync_instance_url(
        SETUP_DEPLOYMENT, probe_url="http://127.0.0.1:8101/mcp",
        instance_base_url=None, state_root=tmp_path,
    )
    assert url == "http://127.0.0.1:8101/mcp"
    assert note and "pasta" in note


def test_plain_sync_still_refuses_a_port_pasta_does_not_forward(tmp_path: Path) -> None:
    from okg.substrate.chat.sync import ChatSyncError

    _record(tmp_path, network_mode=NETWORK_PASTA, forwarded=(5432,))
    with pytest.raises(ChatSyncError) as caught:
        resolve_sync_instance_url(
            SETUP_DEPLOYMENT, probe_url="http://127.0.0.1:8101/mcp",
            instance_base_url=None, state_root=tmp_path,
        )
    assert caught.value.data["reason"] == "mcp_instance_url_unresolved"


def test_plain_sync_under_host_network_reuses_any_loopback_url(tmp_path: Path) -> None:
    _record(tmp_path, network_mode=NETWORK_HOST, forwarded=())
    url, _note = resolve_sync_instance_url(
        SETUP_DEPLOYMENT, probe_url="http://127.0.0.1:8101/mcp",
        instance_base_url="http://127.0.0.1:3000", state_root=tmp_path,
    )
    assert url == "http://127.0.0.1:8101/mcp"
