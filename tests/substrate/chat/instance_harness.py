"""Shared harness for the chat-instance-lifecycle suites (PACT
``chat-instance-lifecycle``).

The seam this harness fills is the CONTAINER RUNTIME, and only that one.
Everything else in these suites is real: real listening sockets, real child
processes, real HTTP round trips, real port-conflict refusals, real
``SIGKILL``. :class:`ProcessRuntime` implements
:class:`okg.substrate.chat.instance.ContainerRuntime` by launching
``openwebui_stub.py`` as a child process instead of a container, so the
lifecycle code under test is byte-for-byte the code the live arm drives —
they differ in one constructor argument.

What that buys, and what it does not
------------------------------------
It buys: idempotence, refusal ordering, port conflict, supervision-after-kill,
status-from-observation, and front-door routing, all proved against something
that genuinely binds, genuinely answers and genuinely dies.

It does not buy: any claim about Open WebUI's own behaviour. The stub does not
have a SQLite fallback, an app database, or the vendor's index bytes. Every
arm that needs those is a live arm recorded as an operator_report. Per the
PACT, ``req.chat-instance-lifecycle.instance-stood-up`` is NOT closed by
anything in this directory.

The one modelled behaviour, and why it is honest
------------------------------------------------
:meth:`ProcessRuntime.execute` answers ``test -f /app/backend/data/webui.db``
by MODELLING F1: the store exists exactly when the launch spec carried no
``DATABASE_URL``. That models the vendor rule measured at ``env.py:267``
rather than hardcoding "absent", so the assertion would fail if the lifecycle
ever launched without the variable. It is a model, and the measurement of the
real store is the live arm's.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate.chat.instance import (
    CONTAINER_APP_DATABASE_VAR,
    LABEL_DEPLOYMENT,
    LABEL_MANAGED_BY,
    MANAGED_BY,
    PINNED_IMAGE_REF,
    PINNED_VERSION,
    InstanceSpec,
    probe_version,
)
from okg.substrate.deployments.chat_block import parse_chat_block

STUB = Path(__file__).parent / "openwebui_stub.py"

#: A deployment manifest that loads, with a chat block that validates. Built
#: through the real ``parse_chat_block`` so a schema change in
#: ``chat-manifest-block`` breaks these suites loudly rather than letting them
#: run against a hand-rolled stand-in.
VALID_CHAT: dict[str, Any] = {
    "enabled": True,
    "ui": {
        "site_name": "Fixture Chat",
        "logo_ref": "chat/logo.png",
        "banner": "Fixture deployment.",
        "features": {"context_compaction": True, "arena": True},
    },
    "models": {
        "builtin": ["anthropic/claude-sonnet-5"],
        "default": "anthropic/claude-sonnet-5",
    },
    "preset": {
        "display_name": "Fixture Chat",
        "description": "Ask the fixture graph",
        "system_prompt_ref": "chat/system_prompt.md",
        "suggestions": ["What changed?"],
    },
    "search_profile": "analyst",
    "mcp": {"port": 8101, "tools": ["inspect", "search"]},
    "deep_links": {
        "operator_ui_base": "http://127.0.0.1:5173",
        "chat_site_base": "http://chat.fixture.invalid:8080",
    },
}


def chat_block(**overrides: Any):
    """A real, parsed :class:`ChatBlock`. ``None`` in an override deletes a key."""
    raw = {key: value for key, value in VALID_CHAT.items()}
    for key, value in overrides.items():
        if value is None:
            raw.pop(key, None)
        else:
            raw[key] = value
    manifest = {
        "name": "fixture-deployment",
        "search": {
            "default_profile": "analyst",
            "profiles": {"analyst": {"subtypes": ["chunk"]}},
        },
        "chat": raw,
    }
    return parse_chat_block(manifest, manifest_path=None)


def free_port(host: str = "127.0.0.1") -> int:
    """A port the kernel says is free right now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])


def wait_until(predicate, *, timeout: float = 10.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class _Container:
    """One launched stub, with the facts ``inspect`` reports about it."""

    def __init__(self, spec: InstanceSpec, version: str, marker: str) -> None:
        self.spec = spec
        self.version = version
        self.marker = marker
        self.labels = dict(spec.labels)
        self.env_var_names = spec.env_var_names()
        self.had_database_url = CONTAINER_APP_DATABASE_VAR in spec.env
        self.process: subprocess.Popen[bytes] | None = None

    def launch(self) -> None:
        self.process = subprocess.Popen(  # noqa: S603 - argv built here
            [
                sys.executable,
                str(STUB),
                str(self.spec.host_port),
                self.version,
                self.marker,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # Own session (#703): without it a signal aimed at this stub can
            # reach the whole pytest-xdist worker group and make the lane
            # nondeterministic. stdio is already detached above.
            start_new_session=True,
        )

    @property
    def running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def kill(self) -> None:
        """SIGKILL. The instance dies without a chance to close its socket."""
        if self.process is not None and self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=10)


class ProcessRuntime:
    """A :class:`ContainerRuntime` whose containers are child processes."""

    binary = "process-runtime"

    def __init__(self, *, version: str = PINNED_VERSION) -> None:
        self.version = version
        self.containers: dict[str, _Container] = {}
        self.run_calls: list[InstanceSpec] = []
        self.start_calls: list[str] = []
        self.remove_calls: list[str] = []

    # -- ContainerRuntime --------------------------------------------------

    def inspect(self, container: str) -> dict[str, Any] | None:
        entry = self.containers.get(container)
        if entry is None:
            return None
        return {
            "id": container,
            "labels": dict(entry.labels),
            "image": entry.spec.image,
            "running": entry.running,
            "exit_code": None if entry.running else 137,
        }

    def run(self, spec: InstanceSpec) -> str:
        self.run_calls.append(spec)
        entry = _Container(spec, self.version, marker=spec.deployment)
        self.containers[spec.container] = entry
        entry.launch()
        return spec.container

    def start(self, container: str) -> None:
        self.start_calls.append(container)
        entry = self.containers.get(container)
        if entry is not None and not entry.running:
            entry.launch()

    def stop(self, container: str) -> None:
        entry = self.containers.get(container)
        if entry is not None:
            entry.kill()

    def remove(self, container: str) -> None:
        self.remove_calls.append(container)
        entry = self.containers.pop(container, None)
        if entry is not None:
            entry.kill()

    def execute(
        self, container: str, argv: Sequence[str],
    ) -> tuple[int, str, str]:
        entry = self.containers.get(container)
        if entry is None:
            return 1, "", "no such container"
        if list(argv[:2]) == ["test", "-f"]:
            # F1, modelled: the vendor default store exists exactly when the
            # instance was launched without DATABASE_URL.
            return (1 if entry.had_database_url else 0), "", ""
        return 0, "", ""

    # -- harness affordances ----------------------------------------------

    def kill_instance(self, container: str) -> None:
        """Kill the real child process holding the port."""
        entry = self.containers[container]
        entry.kill()

    def adopt_foreign(self, container: str, *, owner: str) -> None:
        """Register a container labelled for a DIFFERENT deployment.

        Nothing is launched: the point is the label, which is what makes
        "serving another deployment" an observation rather than a name clash.
        """
        spec = InstanceSpec(
            deployment=owner,
            container=container,
            image=PINNED_IMAGE_REF,
            host="127.0.0.1",
            host_port=0,
            container_port=8080,
            env={CONTAINER_APP_DATABASE_VAR: "postgresql://x@127.0.0.1:5432/other"},
            labels={LABEL_DEPLOYMENT: owner, LABEL_MANAGED_BY: MANAGED_BY},
        )
        self.containers[container] = _Container(spec, self.version, marker=owner)

    def shutdown(self) -> None:
        for container in list(self.containers):
            self.remove(container)


def reachable_probe(*_args: Any) -> None:
    """A probe that succeeds. Paired in every suite with a real closed port."""
    return None


def await_up(base_url: str, *, timeout: float = 15.0) -> str | None:
    """Poll the instance's own version endpoint until it answers."""
    result: dict[str, str | None] = {"version": None}

    def answered() -> bool:
        result["version"] = probe_version(base_url)
        return result["version"] is not None

    wait_until(answered, timeout=timeout)
    return result["version"]


def env(**pairs: str) -> Mapping[str, str]:
    """An explicit operator environment. Never ``os.environ``.

    Every suite passes one of these so a variable that happens to be set on
    the developer's machine can never satisfy a refusal arm.
    """
    return dict(pairs)
