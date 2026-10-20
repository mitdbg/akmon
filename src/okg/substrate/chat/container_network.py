"""How the chat container reaches services on the host's loopback (#2876 item 2).

The instance container dials host services: the graph server (``okg
mcp-serve``, which must stay loopback-only because it exposes raw SQL), the
chat app database when it runs on the host, a local model provider, and a
live-tools endpoint. Inside a container ``127.0.0.1`` is the container
itself, and on Linux ``host.docker.internal`` resolves to the host's EXTERNAL
address, where a loopback-bound service refuses. So the network mode is
chosen by platform and runtime:

* ``gateway`` -- macOS (Docker Desktop, podman machine) and the explicit
  choice anywhere: loopback URLs are rewritten to the gateway alias, and the
  alias maps to the runtime's ``host-gateway``. Unchanged from before.
* ``pasta`` -- Linux + a local podman (``auto``): ``--network
  pasta:-T,<port>,...`` forwards each named host-loopback port into the
  container, where it is dialled as ``127.0.0.1:<port>``. Only the named ports
  are reachable and nothing new listens on the host (measured on a shared
  Linux build host, podman 5.8.2).
* ``host`` -- explicit only: ``--network host``, with Open WebUI forced onto
  ``127.0.0.1`` through ``HOST`` and ``PORT``. The image's ``start.sh``
  defaults ``HOST`` to ``0.0.0.0``, which on the host network would publish
  the site on every interface, so ``HOST`` is ALWAYS set in this mode. The
  container can then reach EVERY host-loopback service, so ``auto`` never
  picks it: docker on Linux refuses ``container_network_choice_required``.

In both Linux modes the gateway alias is mapped to ``127.0.0.1`` inside the
container, so a provider declared as ``http://host.docker.internal:11434/v1``
(the Ollama preset) reaches the host's loopback too; its port is forwarded.
"""

from __future__ import annotations

import sys
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import urlsplit

NETWORK_AUTO = "auto"
NETWORK_GATEWAY = "gateway"
NETWORK_PASTA = "pasta"
NETWORK_HOST = "host"
NETWORK_MODES = (NETWORK_AUTO, NETWORK_GATEWAY, NETWORK_PASTA, NETWORK_HOST)

#: The modes in which the container dials the host's loopback as 127.0.0.1.
LOOPBACK_MODES = (NETWORK_PASTA, NETWORK_HOST)

#: Open WebUI's ``start.sh`` reads these for the uvicorn bind.
LISTEN_HOST_VAR = "HOST"
LISTEN_PORT_VAR = "PORT"
LISTEN_HOST = "127.0.0.1"

#: What the host network lets the container reach, stated wherever it is chosen.
HOST_NETWORK_REACH = (
    "the container shares this host's network, so it can reach EVERY service "
    "on this host's 127.0.0.1 (other deployments' graph servers, databases, "
    "model servers), not only the ports it needs"
)

_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", "[::1]"})
_DEFAULT_PORTS = {"http": 80, "https": 443}

#: Runs ``<runtime binary> <args...>`` and returns ``(exit code, stdout,
#: stderr)``: :meth:`okg.substrate.chat.instance.DockerRuntime.query`.
RuntimeQuery = Callable[[Sequence[str]], "tuple[int, str, str]"]


def _ask(query: RuntimeQuery | None, args: Sequence[str]) -> str | None:
    """The runtime's stdout, or ``None`` when it could not be asked."""
    if query is None:
        return None
    try:
        code, out, _err = query(list(args))
    except OSError:
        return None
    return out.strip() if code == 0 else None


def runtime_kind(query: RuntimeQuery | None) -> tuple[str, str]:
    """``(kind, version line)`` from ``<binary> --version``, never the file
    name: podman's ``docker`` wrapper answers ``podman version ...``."""
    lines = (_ask(query, ["--version"]) or "").splitlines()
    line = lines[0] if lines else ""
    lowered = line.lower()
    for kind in ("podman", "docker"):
        if kind in lowered:
            return kind, line
    return "", line


def podman_is_remote(query: RuntimeQuery | None, version: str) -> bool | None:
    """``podman info``'s ``ServiceIsRemote``; ``None`` when it cannot be told.

    pasta forwards the SERVICE host's loopback, which for a remote service is
    another machine's. An unanswered ``podman-remote`` counts as remote; an
    unanswered plain ``podman`` is ``None`` and is treated as local.
    """
    answer = _ask(query, ["info", "--format", "{{.Host.ServiceIsRemote}}"])
    if answer in ("true", "false"):
        return answer == "true"
    return True if "podman-remote" in version.lower() else None


def _refuse(message: str, *, reason: str, fix_hint: str, **data: Any) -> Exception:
    from okg.substrate.chat.instance import ChatInstanceError

    return ChatInstanceError(
        message, data={"reason": reason, **data}, fix_hint=fix_hint,
    )


def resolve_network_mode(
    requested: str, *, runtime_binary: str, platform: str | None = None,
    query: RuntimeQuery | None = None,
) -> tuple[str, str | None]:
    """``(mode, note)``. ``auto`` picks by platform and runtime and says so.

    The runtime is identified by ``query`` (``<binary> --version``), not by
    its file name. On Linux ``auto`` picks ``pasta`` for a LOCAL podman
    service and nothing else it could refuse: it never picks ``host``, which
    reaches every host-loopback service, so docker and a podman service that
    is (or may be) remote refuse ``container_network_choice_required``. An
    explicit mode the platform or runtime cannot honour refuses: ``pasta``
    needs a local podman on Linux; ``host`` needs Linux and not a rootless
    docker, whose host network is rootlesskit's namespace.
    """
    plat = sys.platform if platform is None else platform
    if requested not in NETWORK_MODES:
        raise _refuse(
            f"unknown container network mode {requested!r}",
            reason="container_network_unknown",
            fix_hint=f"pass --container-network one of {', '.join(NETWORK_MODES)}",
            requested=requested,
        )
    if requested == NETWORK_GATEWAY:
        return NETWORK_GATEWAY, None
    if not plat.startswith("linux"):
        if requested == NETWORK_AUTO:
            return NETWORK_GATEWAY, None
        raise _refuse(
            f"--container-network {requested} needs Linux; on {plat} the "
            "runtime runs in a VM, whose loopback and host network are not "
            "this machine's",
            reason="container_network_unsupported",
            fix_hint="use --container-network gateway (or auto) on this platform",
            requested=requested, platform=plat, runtime=runtime_binary,
        )
    kind, version = runtime_kind(query)
    data = {
        "requested": requested, "platform": plat, "runtime": runtime_binary,
        "runtime_version": version,
    }
    if kind == "podman" and requested in (NETWORK_AUTO, NETWORK_PASTA):
        remote = podman_is_remote(query, version)
        if remote:
            # A local `podman` whose info did not answer (None) stays podman:
            # its service is this host. `podman-remote` unanswered is True.
            state = "a remote (or unconfirmed podman-remote)"
            raise _refuse(
                f"{runtime_binary!r} talks to {state} podman service; pasta "
                "would forward that service host's loopback ports, which may "
                "not be this host's",
                reason=(
                    "container_network_choice_required"
                    if requested == NETWORK_AUTO
                    else "container_network_unsupported"
                ),
                fix_hint=(
                    "run the instance with a local podman, or pass "
                    "--container-network gateway"
                ),
                podman_service_remote=remote, **data,
            )
        if requested == NETWORK_AUTO:
            return NETWORK_PASTA, (
                "Linux + local podman: the container reaches only the named "
                "host-loopback ports, through pasta forwarding "
                "(--container-network pasta)"
            )
        return NETWORK_PASTA, None
    if requested == NETWORK_PASTA:
        raise _refuse(
            f"--container-network pasta needs a local podman on Linux; "
            f"{runtime_binary!r} answers {version or 'nothing'!r} to --version",
            reason="container_network_unsupported",
            fix_hint=(
                "use podman, or --container-network host (the container can "
                "then reach every host-loopback service), or gateway"
            ),
            **data,
        )
    if requested == NETWORK_AUTO:
        if kind == "docker":
            raise _refuse(
                "--container-network auto does not choose the host network "
                f"for docker on Linux: {HOST_NETWORK_REACH}",
                reason="container_network_choice_required",
                fix_hint=(
                    "use podman (auto then forwards only the needed ports), "
                    "pass --container-network host to accept that reach, or "
                    "pass --container-network gateway"
                ),
                **data,
            )
        return NETWORK_GATEWAY, (
            f"--container-network auto knows docker and podman, not "
            f"{runtime_binary!r} ({version or 'no --version answer'}); the "
            "gateway alias is used, which does not reach a service bound to "
            "the host's 127.0.0.1 on Linux. Pass --container-network pasta or "
            "host to choose"
        )
    # An explicit --container-network host on Linux.
    if kind == "docker":
        options = _ask(query, ["info", "--format", "{{.SecurityOptions}}"])
        if options is None:
            raise _refuse(
                f"{runtime_binary!r} did not answer `info`, so whether it is "
                "rootless cannot be told; a rootless docker's host network is "
                "rootlesskit's namespace, where the instance never answers",
                reason="host_network_rootless_unknown",
                fix_hint=f"make `{runtime_binary} info` answer, then re-run",
                **data,
            )
        if "name=rootless" in options:
            raise _refuse(
                f"{runtime_binary!r} is a rootless docker: --network host joins "
                "rootlesskit's network namespace, not this host's, so the "
                "instance would never answer on this host's 127.0.0.1",
                reason="host_network_rootless_docker",
                fix_hint=(
                    "use podman (pasta), a rootful docker, or "
                    "--container-network gateway"
                ),
                **data,
            )
    return NETWORK_HOST, None


def _url_port(url: str) -> tuple[str, int | None]:
    split = urlsplit(url.strip())
    host = (split.hostname or "").lower()
    try:
        port = split.port
    except ValueError:
        return host, None
    return host, port or _DEFAULT_PORTS.get(split.scheme.lower())


def names_host_loopback(host: str, alias: str | None) -> bool:
    """The host part names the host's loopback (directly, or via the alias)."""
    host = (host or "").lower()
    return host in _LOOPBACK_HOSTS or bool(alias) and host == alias.lower()


def loopback_url_ports(urls: Iterable[str | None], alias: str | None) -> tuple[int, ...]:
    """The ports of the URLs that name the host's loopback or the alias."""
    ports: set[int] = set()
    for url in urls:
        if not isinstance(url, str) or not url.strip():
            continue
        host, port = _url_port(url)
        if port and names_host_loopback(host, alias):
            ports.add(port)
    return tuple(sorted(ports))


def loopback_ports(
    chat_block: Any,
    *,
    app_database_host: str,
    app_database_port: int,
    alias: str | None,
    extra: Iterable[int] = (),
) -> tuple[int, ...]:
    """The host-loopback ports the container must reach, sorted.

    The graph server's declared port always (``mcp-serve`` binds loopback);
    the app database's when it is on the host; the provider's and
    ``chat.mcp.live_tools.url``'s when they name the loopback or the gateway
    alias; and any ``extra`` the caller knows (setup's ``--mcp-probe-url``
    and ``--mcp-instance-url`` ports).
    """
    ports: set[int] = {int(p) for p in extra if p}
    mcp = getattr(chat_block, "mcp", None)
    mcp_port = getattr(mcp, "port", None) if mcp is not None else None
    if mcp_port:
        ports.add(int(mcp_port))
    if names_host_loopback(app_database_host, alias):
        ports.add(int(app_database_port))
    provider = getattr(chat_block, "provider", None)
    live_tools = getattr(mcp, "live_tools", None) if mcp is not None else None
    ports.update(loopback_url_ports(
        (
            getattr(provider, "base_url", None) if provider is not None else None,
            getattr(live_tools, "url", None) if live_tools is not None else None,
        ),
        alias,
    ))
    return tuple(sorted(ports))


def pasta_network_arg(ports: Sequence[int]) -> str:
    """``pasta:-T,5795,-T,5797``; plain ``pasta`` when nothing is forwarded."""
    if not ports:
        return "pasta"
    parts: list[str] = []
    for port in ports:
        parts += ["-T", str(int(port))]
    return "pasta:" + ",".join(parts)


def network_fingerprint_part(mode: str, ports: Sequence[int]) -> str | None:
    """What the settings fingerprint adds for the mode; ``None`` for gateway,
    so a container started before the mode existed keeps its fingerprint."""
    if mode == NETWORK_GATEWAY:
        return None
    return f"{mode}:{','.join(str(int(p)) for p in ports)}"


def require_forwardable(
    deployment: str, mode: str, ports: Sequence[int], container_port: int,
) -> None:
    """A forwarded port cannot be the container's own listening port."""
    if mode == NETWORK_PASTA and container_port in ports:
        raise _refuse(
            f"deployment {deployment!r} needs host-loopback port "
            f"{container_port} forwarded into the container, where Open WebUI "
            "itself listens on that port",
            reason="container_network_port_clash",
            fix_hint=(
                f"move the graph server, app database or provider off port "
                f"{container_port}, or use --container-network host"
            ),
            deployment=deployment, port=container_port,
        )


__all__ = [
    "HOST_NETWORK_REACH",
    "LISTEN_HOST",
    "LISTEN_HOST_VAR",
    "LISTEN_PORT_VAR",
    "LOOPBACK_MODES",
    "NETWORK_AUTO",
    "NETWORK_GATEWAY",
    "NETWORK_HOST",
    "NETWORK_MODES",
    "NETWORK_PASTA",
    "RuntimeQuery",
    "loopback_ports",
    "loopback_url_ports",
    "names_host_loopback",
    "network_fingerprint_part",
    "pasta_network_arg",
    "podman_is_remote",
    "require_forwardable",
    "resolve_network_mode",
    "runtime_kind",
]
