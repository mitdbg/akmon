"""Standalone viz server entry point: ``python -m okg.viz``.

Substrate-native. Requires a substrate DSN and a published
generation for the named deployment.

CLI::

    python -m okg.viz \\
        --dsn postgres://.../okg \\
        --deployment cms \\
        [--generation N] \\
        [--host H] [--port P]

Falls back to env vars: ``OKG_DSN``, ``OKG_DEPLOYMENT``,
``OKG_VIZ_HOST``, ``OKG_VIZ_PORT``.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys


def _register_auth_gated_bind(
    host: str, port: int, token: str | None, *, api_token: str | None = None,
) -> None:
    """When the viz server binds non-loopback, declare the bind's auth posture
    to the listener-exposure doctor check (secure-viz-operator-routes).

    With an operator token configured, add the port to OKG_AUTH_GATED_PORTS so
    the doctor recognizes the authenticated bind. WITHOUT a token, a non-loopback
    bind is REFUSED at startup (fail closed): exposing unauthenticated mutating
    routes to the network is the rejected silent-insecure-default, so the
    operator must set OKG_VIZ_OPERATOR_TOKEN (or bind loopback). A non-loopback
    bind also needs the read-API token (``--auth-token-env``, issue #3278):
    without it every /viz/api/* read is open to the network. This is the
    launcher half; the app middleware also restricts mutating routes to loopback
    peers whenever no token is configured."""
    from okg.substrate.cli.connector_security import (
        AUTH_GATED_PORTS_ENV,
        _is_loopback,
    )
    if _is_loopback(host):
        return
    log = logging.getLogger(__name__)
    if token and not api_token:
        log.error(
            "viz: REFUSING to bind non-loopback (%s:%s) WITHOUT a read-API "
            "token -- every /viz/api/* read would be open to the network. "
            "Pass --auth-token-env VARIABLE_NAME, or bind 127.0.0.1.",
            host, port,
        )
        raise SystemExit(
            f"viz refused: non-loopback bind {host}:{port} requires "
            "--auth-token-env VARIABLE_NAME, or bind 127.0.0.1"
        )
    if token:
        existing = (os.environ.get(AUTH_GATED_PORTS_ENV) or "").strip()
        ports = {p.strip() for p in existing.split(",") if p.strip()}
        ports.add(str(port))
        os.environ[AUTH_GATED_PORTS_ENV] = ",".join(sorted(ports))
        log.info(
            "viz: non-loopback bind %s:%s is operator-token auth-gated", host, port,
        )
    else:
        log.error(
            "viz: REFUSING to bind non-loopback (%s:%s) WITHOUT an operator "
            "token — unauthenticated mutating routes must not be exposed to the "
            "network. Set OKG_VIZ_OPERATOR_TOKEN, or bind 127.0.0.1.",
            host, port,
        )
        raise SystemExit(
            f"viz refused: non-loopback bind {host}:{port} requires "
            "OKG_VIZ_OPERATOR_TOKEN (fail-closed; secure-viz-operator-routes)"
        )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="okg.viz",
        description="OKG visualization server (substrate-backed)",
    )
    p.add_argument(
        "--dsn", default=None,
        help="Substrate Postgres DSN (default: $OKG_DSN)",
    )
    p.add_argument(
        "--deployment", default=None,
        help="Deployment name to pin against (default: $OKG_DEPLOYMENT or 'default')",
    )
    p.add_argument(
        "--generation", default=None,
        help=(
            "Generation checkpoint id or compatibility integer to pin "
            "(default: latest published)"
        ),
    )
    p.add_argument(
        "--host", default=None,
        help="Bind host (default: $OKG_VIZ_HOST or 127.0.0.1)",
    )
    p.add_argument(
        "--port", type=int, default=None,
        help="Bind port (default: $OKG_VIZ_PORT or 8765)",
    )
    p.add_argument(
        "--coordination-setup-config",
        default=None,
        help=(
            "Private operator-owned first-chief setup authority file "
            "(default: $OKG_COORDINATION_SETUP_CONFIG; setup stays unavailable "
            "when omitted)"
        ),
    )
    p.add_argument(
        "--auth-token-env",
        dest="auth_token_env",
        default=None,
        metavar="VARIABLE_NAME",
        help=(
            "Require a bearer token on every /viz/api/* request, read from "
            "this environment variable (pass the NAME, not the value). An "
            "unset, empty or padded variable refuses to start. Required for a "
            "non-loopback --host"
        ),
    )
    return p


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("OKG_VIZ_LOG", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )

    args = _build_parser().parse_args()

    dsn = args.dsn or os.environ.get("OKG_DSN")
    deployment = (
        args.deployment
        or os.environ.get("OKG_DEPLOYMENT")
        or "default"
    )
    host = args.host or os.environ.get("OKG_VIZ_HOST") or "127.0.0.1"
    port_raw = args.port or os.environ.get("OKG_VIZ_PORT") or "8765"
    port = int(port_raw)

    if not dsn:
        print(
            "Error: no substrate DSN — pass --dsn or set OKG_DSN",
            file=sys.stderr,
        )
        return 1

    # Resolve the read-API token before any graph load starts: a named but
    # unusable variable refuses here, never serves unauthenticated.
    from okg.substrate.errors import SubstrateError
    from okg.viz.auth import resolve_api_token

    try:
        api_token = resolve_api_token(args.auth_token_env)
    except SubstrateError as exc:
        # The message names the variable, never its value.
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    import uvicorn

    from okg.viz.app import build_app
    from okg.viz.boot import get_ctx_lazy, preload_in_background

    # Prime the lazy loader's deployment + DSN before kicking off
    # the background thread, so the env-var defaults inside `load`
    # see the values from CLI flags.
    if args.dsn:
        os.environ["OKG_DSN"] = args.dsn
    if args.deployment:
        os.environ["OKG_DEPLOYMENT"] = args.deployment
    os.environ["OKG_VIZ_HOST"] = host
    os.environ["OKG_VIZ_PORT"] = str(port)

    provider, is_ready = get_ctx_lazy()

    preload_in_background(
        dsn=dsn,
        deployment_name=deployment,
        generation_id=args.generation,
    )

    from okg.viz.auth import resolve_operator_token
    from okg.viz.coordination_services import build_native_coordination_services

    operator_token = resolve_operator_token()
    _register_auth_gated_bind(host, port, operator_token, api_token=api_token)
    setup_config_raw = args.coordination_setup_config or os.environ.get(
        "OKG_COORDINATION_SETUP_CONFIG"
    )
    setup_kwargs: dict[str, object] = {}
    if setup_config_raw:
        from okg.viz.coordination_adapters import (
            ProductionSetupUnavailable,
            build_production_setup_adapters,
        )

        try:
            setup_adapters = build_production_setup_adapters(
                dsn=dsn,
                deployment_name=deployment,
                repo_root=Path.cwd(),
                config_path=Path(setup_config_raw),
            )
        except ProductionSetupUnavailable:
            print(
                "Error: coordination setup authority is unavailable or unsafe",
                file=sys.stderr,
            )
            return 1
        setup_kwargs = {
            "repo_root": Path.cwd(),
            "setup_probes": setup_adapters.setup_probes,
            "chief_resolver": setup_adapters.chief_resolver,
            "pair_request_factory": setup_adapters.pair_request_factory,
            "lifecycle_factory": setup_adapters.lifecycle_factory,
        }
    native_services = build_native_coordination_services(
        dsn=dsn,
        deployment_name=deployment,
        **setup_kwargs,
    )
    app = build_app(
        provider,
        is_ready=is_ready,
        operator_token=operator_token,
        operator_control_service=native_services.operator_control_service,
        approval_control_service=native_services.approval_control_service,
        setup_plan_service=native_services.setup_plan_service,
        api_token=api_token,
    )
    auth_note = (
        f"read API needs the token in ${args.auth_token_env}"
        if api_token else "read API unauthenticated"
    )
    print(
        f"OKG viz: http://{host}:{port}/viz/  "
        f"(deployment={deployment}, {auth_note}, loading in background; "
        "ctrl-c to stop)",
        file=sys.stderr,
    )
    # proxy_headers=False: the operator-auth boundary trusts request.client.host
    # to decide loopback-vs-network when no token is configured. Honoring
    # X-Forwarded-For (esp. under FORWARDED_ALLOW_IPS=*) would let a remote peer
    # spoof a loopback peer and reach mutating routes. The real TCP peer is the
    # only trustworthy signal here, so do NOT let proxy headers rewrite it.
    uvicorn.run(
        app, host=host, port=port,
        log_level="warning", access_log=False,
        proxy_headers=False,
    )
    return 0


if __name__ == "__main__":
    # Process entry point: keep the bytecode of deployment modules the viz
    # server imports out of the deployment repository
    # (okg.substrate.bytecode_cache). main() itself leaves the interpreter
    # alone for in-process callers.
    from okg.substrate.bytecode_cache import enter_user_bytecode_cache

    enter_user_bytecode_cache()
    raise SystemExit(main())
