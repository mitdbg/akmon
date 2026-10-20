"""`okg ui`: serve the built operator UI and its read API from one process.

PACT okg-ui-command. Before this command an operator ran two processes: the
read API (``python -m okg.viz``) and a Vite server (``npm run
operator-ui:dev`` or ``operator-ui:preview``) that proxied ``/viz/api`` to it.
``okg ui`` resolves the deployment the way the sibling read commands do,
starts the same read API, and serves the BUILT frontend bundle beside it on
one port, so the browser talks to one origin and no proxy is needed.

Four refusals keep it honest:

* No built bundle: the wheel does not ship the operator UI's TypeScript, so
  a bundle exists only where ``npm run operator-ui:build`` ran. The command
  refuses with ``operator_ui_bundle_missing`` and the build command; it never
  starts the API alone, which would leave the browser on a JSON 410 page.
* No deployment name: the read API pins one deployment's published graph.
* Any bind that is not loopback, or a port outside 1-65535, refuses with
  ``operator_ui_bind_refused``. The command serves loopback only, with or
  without ``OKG_VIZ_OPERATOR_TOKEN``: that token guards only the read API's
  POST routes, which the front door already refuses, so it would gate
  nothing. Remote operators use an SSH tunnel.

The front door answers GET, HEAD and OPTIONS. Any other method gets HTTP 405
with ``operator_ui_read_only`` before it reaches the read API, so the API's
legacy write routes are unreachable through this command. That matches the
operator UI contract: it issues only reads. OPTIONS passes through so the read
API's CORS preflight works; the API answers a non-preflight OPTIONS with its own
405, and no handler runs. HEAD is served for the static bundle; the read API's
routes are GET-only and answer HEAD with their own 405.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import sys
from collections.abc import Awaitable, Callable, Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from okg.substrate.errors import (
    OperatorUiBindRefused,
    OperatorUiBundleMissing,
    OperatorUiReadOnly,
    SubstrateError,
)

#: Environment variable naming a built bundle directory; ``--dist`` wins.
BUNDLE_ENV = "OKG_OPERATOR_UI_DIST"
#: Where ``npm run operator-ui:build`` writes, relative to the checkout root.
CHECKOUT_BUNDLE = Path(".tmp") / "operator-ui-dist"
#: A file only a source checkout has, next to ``src/``.
_CHECKOUT_MARKER = "vite.operator-ui.config.ts"
#: The only methods the front door passes on. OPTIONS reaches the read API
#: for its CORS preflight; no route there handles OPTIONS as a write.
READ_METHODS: tuple[str, ...] = ("GET", "HEAD", "OPTIONS")
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
_TUNNEL_FIX = (
    "okg ui serves loopback only; bind --host 127.0.0.1 (or localhost, ::1) and "
    "reach it remotely through an SSH tunnel, e.g. "
    "`ssh -L 8765:127.0.0.1:8765 <host>`"
)
_ALLOW_HEADER = ", ".join(READ_METHODS)

_BUILD_FIX = (
    "run `npm install` once and `npm run operator-ui:build` at the root of a "
    "source checkout"
)

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]
AsgiApp = Callable[[Scope, Receive, Send], Awaitable[None]]


@dataclass(frozen=True)
class UiLaunch:
    """Everything ``serve`` needs, resolved and checked before any I/O."""

    dsn: str
    deployment: str
    generation: str | None
    host: str
    port: int
    bundle_dir: Path
    #: Read token from ``--auth-token-env`` (issue #3278); None = open.
    api_token: str | None = None


def checkout_bundle_dir() -> Path | None:
    """The default bundle path when this package runs from a source checkout.

    ``src/okg/viz/ui_server.py`` sits three levels below the checkout root. An
    installed wheel has no ``vite.operator-ui.config.ts`` there, so it gets
    None and must be told where a bundle is.
    """
    root = Path(__file__).resolve().parents[3]
    if (root / _CHECKOUT_MARKER).is_file():
        return root / CHECKOUT_BUNDLE
    return None


def resolve_bundle_dir(
    explicit: str | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Return a built bundle directory, or refuse naming where it looked."""
    env = os.environ if environ is None else environ
    candidate: Path | None
    if explicit:
        candidate, source = Path(explicit).expanduser(), "--dist"
    elif env.get(BUNDLE_ENV):
        candidate, source = Path(env[BUNDLE_ENV]).expanduser(), BUNDLE_ENV
    else:
        candidate, source = checkout_bundle_dir(), "source checkout default"
    if candidate is None:
        raise OperatorUiBundleMissing(
            "no built operator UI bundle: this okg is an installed package, "
            "not a source checkout, and the package does not ship the frontend",
            data={"bundle_dir": None, "source": "installed package"},
            fix_hint=(
                f"{_BUILD_FIX}, then pass --dist "
                f"<checkout>/{CHECKOUT_BUNDLE.as_posix()} or set {BUNDLE_ENV}"
            ),
        )
    if not (candidate / "index.html").is_file():
        raise OperatorUiBundleMissing(
            f"no built operator UI bundle at {candidate}: index.html is "
            f"missing (location from {source})",
            data={"bundle_dir": str(candidate), "source": source},
            fix_hint=f"{_BUILD_FIX}, or pass --dist <a built bundle directory>",
        )
    return candidate.resolve()


def is_loopback_host(host: str) -> bool:
    """True only for ``localhost``, an address in 127.0.0.0/8, or ``::1``.

    Stricter than ``ipaddress``'s ``is_loopback``: an IPv4-mapped IPv6 address
    such as ``::ffff:127.0.0.1`` is refused, because whether it binds only the
    loopback interface depends on the platform's dual-stack settings.
    """
    if host == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv4Address):
        return address in ipaddress.IPv4Network("127.0.0.0/8")
    return address == ipaddress.IPv6Address("::1")


def resolve_bind(
    host_arg: str | None,
    port_arg: int | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, int]:
    """Return a loopback host and a usable port, or refuse by name."""
    env = os.environ if environ is None else environ
    host = host_arg or env.get("OKG_VIZ_HOST") or DEFAULT_HOST
    if not is_loopback_host(host):
        raise OperatorUiBindRefused(
            f"okg ui refuses to bind {host}: it serves loopback only "
            "(localhost, 127.0.0.0/8, ::1), with or without an operator token",
            data={"host": host, "port": port_arg, "reason": "non_loopback_host"},
            fix_hint=_TUNNEL_FIX,
        )
    if port_arg is not None:
        port: int = port_arg
        source = "--port"
    elif env.get("OKG_VIZ_PORT"):
        raw = env["OKG_VIZ_PORT"]
        source = "OKG_VIZ_PORT"
        try:
            port = int(raw)
        except ValueError:
            raise OperatorUiBindRefused(
                f"okg ui refuses OKG_VIZ_PORT={raw!r}: not an integer port",
                data={"host": host, "port": raw, "reason": "port_not_integer"},
                fix_hint="set OKG_VIZ_PORT to a port from 1 to 65535, or pass --port",
            ) from None
    else:
        port, source = DEFAULT_PORT, "default"
    if not 1 <= port <= 65535:
        raise OperatorUiBindRefused(
            f"okg ui refuses port {port} (from {source}): a port must be "
            "from 1 to 65535",
            data={"host": host, "port": port, "reason": "port_out_of_range"},
            fix_hint="pass --port <1-65535>",
        )
    return host, port


def _is_api_path(path: str) -> bool:
    return path == "/viz" or path.startswith("/viz/")


def _authorization_header(scope: Scope) -> str | None:
    for name, value in scope.get("headers") or ():
        if name.lower() == b"authorization":
            return value.decode("latin-1")
    return None


def build_ui_app(
    api_app: AsgiApp,
    bundle_dir: Path,
    *,
    api_token: str | None = None,
) -> AsgiApp:
    """One ASGI app: ``/viz/*`` to the read API, everything else to the bundle.

    Methods outside ``READ_METHODS`` are refused here, before either side sees
    them. Lifespan and any non-HTTP scope go to the read API, which owns
    startup.

    With ``api_token`` (issue #3278) every HTTP request, page and assets
    included, must carry the token, or it gets 401 with an HTTP Basic
    challenge. The browser then shows its own sign-in prompt (any user name,
    the token as the password) and resends the credential on every later
    request to this origin, so the UI itself needs no change. Scripts may send
    ``Authorization: Bearer <token>`` instead.
    """
    from starlette.responses import JSONResponse
    from starlette.staticfiles import StaticFiles

    from okg.viz.auth import api_request_authorized

    static = StaticFiles(directory=str(bundle_dir), html=True)

    async def front_door(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await api_app(scope, receive, send)
            return
        method = str(scope.get("method") or "").upper()
        path = str(scope.get("path") or "")
        if api_token is not None and not api_request_authorized(
            _authorization_header(scope), api_token
        ):
            denied = JSONResponse(
                {"ok": False, "error": "authentication required", "path": path},
                status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="okg ui", charset="UTF-8"'},
            )
            await denied(scope, receive, send)
            return
        if method not in READ_METHODS:
            refusal = OperatorUiReadOnly(
                f"okg ui serves reads only; {method} {path} was refused",
                data={"method": method, "path": path},
                fix_hint=(
                    "the operator UI is read-only; change deployment state with the CLI"
                ),
            )
            response = JSONResponse(
                refusal.to_json(),
                status_code=405,
                headers={"Allow": _ALLOW_HEADER},
            )
            await response(scope, receive, send)
            return
        if _is_api_path(path):
            await api_app(scope, receive, send)
            return
        await static(scope, receive, send)

    return front_door


def prepare(args: argparse.Namespace) -> UiLaunch:
    """Resolve and check every input; raise ``SubstrateError`` on a refusal.

    Order: bind and bundle first (decidable without the database), then the
    deployment name, then its DSN through the shared read authority.
    """
    from okg.substrate.cli import dsn_authority

    from okg.viz.auth import resolve_api_token

    host, port = resolve_bind(args.host, args.port)
    # Fail closed before any I/O: a named but unusable variable refuses.
    api_token = resolve_api_token(getattr(args, "auth_token_env", None))
    bundle_dir = resolve_bundle_dir(args.dist)
    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        raise SubstrateError(
            "okg ui needs a deployment name: the read API pins one "
            "deployment's published graph",
            data={"deployment": None},
            fix_hint="pass --deployment <name> or set OKG_DEPLOYMENT",
        )
    resolution = dsn_authority.resolve_read_dsn(args, surface="okg ui")
    return UiLaunch(
        dsn=resolution.dsn,
        deployment=resolution.deployment or deployment,
        generation=args.generation,
        host=host,
        port=port,
        bundle_dir=bundle_dir,
        api_token=api_token,
    )


def serve(launch: UiLaunch) -> int:
    """Start the background graph load and serve until interrupted."""
    import uvicorn

    from okg.viz import boot
    from okg.viz.app import build_app

    # The lazy loader and the app read these, as under `python -m okg.viz`.
    os.environ["OKG_DSN"] = launch.dsn
    os.environ["OKG_DEPLOYMENT"] = launch.deployment
    os.environ["OKG_VIZ_HOST"] = launch.host
    os.environ["OKG_VIZ_PORT"] = str(launch.port)

    provider, is_ready = boot.get_ctx_lazy()
    boot.preload_in_background(
        dsn=launch.dsn,
        deployment_name=launch.deployment,
        generation_id=launch.generation,
    )
    # okg ui passes no operator token. build_app may still read
    # OKG_VIZ_OPERATOR_TOKEN for its POST routes, but the front door refuses
    # every POST, so the token gates nothing here and grants no network bind.
    # With --auth-token-env the token is checked twice: at the front door
    # (every request, page included) and again inside the read API.
    api_app = build_app(provider, is_ready=is_ready, api_token=launch.api_token)
    app = build_ui_app(api_app, launch.bundle_dir, api_token=launch.api_token)
    auth_note = (
        "sign in with the token as the password"
        if launch.api_token else "no sign-in"
    )
    print(
        f"okg ui: http://{launch.host}:{launch.port}/ "
        f"(deployment={launch.deployment}, bundle={launch.bundle_dir}, "
        f"read-only, {auth_note}; the graph loads in the background; "
        "ctrl-c to stop)",
        file=sys.stderr,
    )
    # proxy_headers=False for the same reason as `python -m okg.viz`: the
    # loopback check trusts the real TCP peer, never X-Forwarded-For.
    uvicorn.run(
        app,
        host=launch.host,
        port=launch.port,
        log_level="warning",
        access_log=False,
        proxy_headers=False,
    )
    return 0


__all__ = [
    "BUNDLE_ENV",
    "READ_METHODS",
    "UiLaunch",
    "build_ui_app",
    "checkout_bundle_dir",
    "is_loopback_host",
    "prepare",
    "resolve_bind",
    "resolve_bundle_dir",
    "serve",
]
