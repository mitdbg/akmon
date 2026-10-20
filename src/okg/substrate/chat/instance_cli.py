"""``okg chat-instance`` — the operator verbs for one deployment's Open WebUI
instance (PACT ``chat-instance-lifecycle``).

Five subcommands, all thin. Every decision lives in
:mod:`okg.substrate.chat.instance` and :mod:`okg.substrate.chat.front_door`;
this file resolves the manifest, turns flags into arguments, and prints.

``okg chat-instance up``
    Stand the instance up at the pinned version and prove it by reading
    ``GET /api/version`` off the instance itself. Idempotent: a second run
    against a running instance reports ``already_running``, creates no second
    container and no second app-database record. Refuses, naming the missing
    input, on: no chat block, a disabled chat block, an undeclared instance
    port, an unset app-database or secret-key variable, a graph DSN that does
    not resolve (so the isolation check cannot be made), an app database that
    resolves to the deployment's graph database or to another deployment's,
    an unreachable app database, an occupied port, and a container that
    belongs to a different deployment.

``okg chat-instance status``
    Observe the instance and report in the deployment status vocabulary. The
    state is derived from the version response every time; the operator-state
    record supplies only the address to ask at. The deployment's mcp-serve
    endpoint is observed SEPARATELY and reported beside it: a site that loads
    while its tool endpoint is dead is an ordinary condition and a named one
    (``mcp_endpoint_dead``), not a silence. The exit code stays the
    instance's.

``okg chat-instance supervise``
    One pass, or ``--iterations N`` passes: observe, restart what does not
    answer, observe again. Both observations are version responses. mcp-serve
    is NOT supervised — it is not a process this command runs — so a dead MCP
    endpoint appears in the pass's ``conditions`` and is never restarted and
    never omitted.

``okg chat-instance down``
    Stop and remove the container and forget the record. The app DATABASE is
    left alone — it holds the chat history.

``okg chat-instance front-door``
    Render the single-operator reverse-proxy configuration for one deployment
    from its declared instance port and ``chat.deep_links.chat_site_base``.
    An ``https`` site base REQUIRES ``--tls-certificate`` and
    ``--tls-private-key`` (paths, never key material) and renders a complete
    TLS server block; without them it refuses rather than emitting a listener
    that reads as TLS-terminated and is not. The ``/console`` path answers
    not-configured until the console bundle is measured or declared base-aware
    (``--console-bundle-index`` / ``--console-base-aware``), and routes to
    ``--console-upstream`` once it is; a permitted bundle with no upstream, and
    an upstream for a bundle that is not permitted, are both refusals. With
    ``--serve`` it also runs the reference front door so the routes can be
    exercised without an nginx binary; that server speaks plain HTTP and
    refuses a TLS plan.

Secrets are named, never passed. ``--app-database-url-env`` and
``--admin-secret-env`` take variable NAMES; the values are read from the
environment and never appear in an argv, a state file, a report or a log.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import pathlib
import sys
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from okg.substrate.errors import SubstrateError

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance at runtime
    from okg.substrate.chat.instance import McpObservation


def _emit(payload: dict[str, Any], *, as_json: bool, headline: str) -> None:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(payload, sort_keys=True, default=str))
        return
    print(headline)


def _emit_error(err: SubstrateError, *, as_json: bool) -> int:
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(err.to_json(), sort_keys=True), file=sys.stderr)
        return 1
    fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
    print(f"error [{err.code}]: {err.message}{fix}", file=sys.stderr)
    if err.data:
        print(f"  data: {json.dumps(err.data, sort_keys=True, default=str)}",
              file=sys.stderr)
    return 1


def _load_manifest(
    deployment: str, *, require_graph_dsn: bool,
) -> tuple[Any, str | None]:
    """The deployment's chat block and graph DSN, from ONE manifest read.

    Read together because the app-database isolation check compares them: two
    separate reads could straddle an edit and compare an app DSN against a
    graph DSN the deployment no longer has.

    ``require_graph_dsn`` is True for every verb that runs that check, which
    is ``up``. An unresolvable graph DSN then REFUSES, naming the variable to
    export. It must not degrade to ``None``: the check is guarded on having a
    graph DSN, ``None`` is what the manifest loader returns for the
    ``dsn: ${OKG_DSN}`` shape this repo uses whenever the variable is
    unexported, and the two together would silently retire the one refusal
    standing between an app DSN and the deployment's graph database.
    ``front-door`` passes False because it never consults the graph DSN.
    """
    from okg.substrate import fastyaml
    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.deployments.chat_block import parse_chat_block

    dep_dir = find_deployment_dir(deployment)
    manifest = (dep_dir / "deployment.yaml") if dep_dir else None
    if manifest is None or not manifest.is_file():
        raise SubstrateError(
            f"deployment {deployment!r} has no deployment.yaml",
            data={"deployment": deployment, "reason": "manifest_absent"},
            fix_hint="check the deployment name, or run `okg install`",
        )
    raw = fastyaml.safe_load(manifest.read_text()) or {}
    chat = parse_chat_block(raw, manifest_path=manifest)
    if getattr(chat, "oidc_login", None) is not None:
        # PACT chat-oidc-login: the instance's login and mcp-serve's token
        # verification must describe one provider and one client.
        from okg.substrate.mcp.oidc_auth import (
            oidc_auth_from_raw_manifest,
            require_login_blocks_agree,
        )

        require_login_blocks_agree(
            oidc_auth_from_raw_manifest(raw), chat.oidc_login, deployment=deployment,
        )
    if not require_graph_dsn:
        return chat, None

    # The graph DSN as the MANIFEST declares it, env interpolation included.
    # Resolved through the substrate's own accessor rather than re-parsed here,
    # so the isolation check compares against the same string the worker
    # connects on and an unresolvable one is a refusal rather than a None.
    from okg.substrate.deployments import (
        DeploymentManifest,
        DeploymentResolutionError,
        require_postgres_dsn,
    )

    try:
        loaded = DeploymentManifest.from_yaml(manifest)
        graph_dsn = require_postgres_dsn(loaded, deployment=deployment)
    except DeploymentResolutionError as err:
        # Translated, not swallowed: DeploymentResolutionError is not a
        # SubstrateError, so it would leave this CLI as a traceback rather
        # than as the named refusal the operator can act on.
        raise SubstrateError(
            f"deployment {deployment!r}'s graph database DSN is unresolvable, "
            f"so the app database cannot be proved distinct from it: {err}",
            data={
                "deployment": deployment,
                "reason": "graph_dsn_unresolvable",
                "manifest": str(manifest),
                "manifest_key": "postgres.dsn",
            },
            fix_hint=(
                "export the variable the manifest's postgres.dsn interpolates "
                "(OKG_DSN for the `dsn: ${OKG_DSN}` shape) and run the command "
                "again. Standing the instance up without it would skip the "
                "check that keeps Open WebUI's tables out of this deployment's "
                "graph database"
            ),
        ) from err
    return chat, graph_dsn


def add_container_network_arg(parser: Any) -> None:
    """``--container-network``, shared by ``chat-instance up`` and ``chat setup``."""
    from okg.substrate.chat.container_network import NETWORK_AUTO, NETWORK_MODES

    parser.add_argument(
        "--container-network",
        choices=NETWORK_MODES,
        default=NETWORK_AUTO,
        help=(
            "how the container reaches the graph server, app database and "
            "model provider on this host's loopback. auto (default): Linux + "
            "a local podman uses pasta (forwards only those ports, dialled as "
            "127.0.0.1); macOS uses gateway (host.docker.internal); Linux + "
            "docker refuses and asks for a choice. host (explicit only): the "
            "host network with the site bound to 127.0.0.1, and the container "
            "can reach EVERY service on this host's loopback"
        ),
    )


def _runtime(args: argparse.Namespace):
    from okg.substrate.chat.instance import DockerRuntime

    return DockerRuntime(binary=getattr(args, "container_runtime", "docker"))


def _cmd_up(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.instance import stand_up

    chat, graph_dsn = _load_manifest(args.deployment, require_graph_dsn=True)
    report = stand_up(
        args.deployment,
        chat_block=chat,
        install_plan=getattr(args, "install_plan", None),
        graph_dsn=graph_dsn,
        runtime=_runtime(args),
        app_database_env=args.app_database_url_env,
        secret_env=args.admin_secret_env,
        instance_port_override=args.instance_port,
        image=args.image,
        host=args.host,
        host_gateway_alias=args.host_gateway_alias or None,
        ready_timeout=args.ready_timeout,
        container_network=args.container_network,
    )
    payload = {"ok": True, **report.as_dict()}
    _emit(
        payload,
        as_json=as_json,
        headline=(
            f"chat instance {report.outcome}: {args.deployment} at "
            f"{report.base_url} serving version {report.version} "
            f"(app database {report.app_database.target.describe()}, port "
            f"from {report.port_plan.source})"
        ),
    )
    for note in report.notes:
        if not (as_json or os.environ.get("OKG_AGENT") == "1"):
            print(f"  note: {note}")
    return 0


def _mcp_endpoint(
    args: argparse.Namespace,
) -> "tuple[str | None, McpObservation | None]":
    """The deployment's mcp-serve endpoint, and the reason there isn't one.

    Read out of the manifest rather than taken as a flag, so the endpoint the
    status surface asks at is the one the deployment declares.

    Returns a second value for the case the reason matters: a manifest that
    cannot be READ is not a deployment that declares no MCP endpoint, and the
    two must not collapse. ``status`` used to answer from the operator-state
    record alone and must keep doing so — refusing outright would make an
    unreadable manifest hide a running instance — so the observation carries
    ``mcp_endpoint_undetermined`` with the loader's own message instead. What
    is never done is report it as ``undeclared``, or as reachable.
    """
    from okg.substrate.chat.instance import (
        MCP_STATE_UNOBSERVED,
        McpObservation,
        mcp_endpoint_for,
    )

    try:
        chat, _graph_dsn = _load_manifest(args.deployment, require_graph_dsn=False)
    except SubstrateError as err:
        return None, McpObservation(
            state=MCP_STATE_UNOBSERVED,
            endpoint=None,
            detail=(
                f"the deployment's manifest could not be read, so its mcp "
                f"endpoint is UNDETERMINED, not undeclared: {err.message}"
            ),
            reason="mcp_endpoint_undetermined",
        )
    return mcp_endpoint_for(chat, host=args.host), None


def _cmd_status(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.instance import STATE_UP, instance_status

    endpoint, undetermined = _mcp_endpoint(args)
    status = instance_status(args.deployment, mcp_endpoint=endpoint)
    if undetermined is not None:
        status = replace(status, mcp=undetermined)
    payload = {"ok": status.state == STATE_UP, **status.as_dict()}
    # The exit code stays the INSTANCE's, which is what this verb is
    # documented to answer and what every caller keys on. The MCP endpoint is
    # a separate process with a separate lifetime, so it is reported in the
    # payload and the headline rather than silently changing what a non-zero
    # exit from this verb means.
    _emit(
        payload,
        as_json=as_json,
        headline=(
            f"chat instance {status.state} ({status.status}): "
            f"{args.deployment} — {status.detail}\n"
            f"  mcp endpoint {status.mcp.state}: {status.mcp.detail}"
        ),
    )
    return 0 if status.state == STATE_UP else 1


def _cmd_supervise(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.instance import (
        SUPERVISION_RESTART_FAILED,
        supervise_once,
    )

    runtime = _runtime(args)
    endpoint, _undetermined = _mcp_endpoint(args)
    events = []
    for index in range(max(1, int(args.iterations))):
        if index:
            time.sleep(max(0.0, float(args.interval)))
        events.append(
            supervise_once(args.deployment, runtime=runtime, mcp_endpoint=endpoint)
        )
    failed = [e for e in events if e.action == SUPERVISION_RESTART_FAILED]
    payload = {
        "ok": not failed,
        "deployment": args.deployment,
        "events": [event.as_dict() for event in events],
    }
    last = events[-1]
    # A pass that restarted nothing and found the tool endpoint dead has to
    # SAY so: "healthy" on its own is the reading this line exists to prevent.
    conditions = (
        f"; conditions {list(last.conditions)} (mcp-serve is not supervised "
        f"by this command)"
        if last.conditions
        else ""
    )
    _emit(
        payload,
        as_json=as_json,
        headline=(
            f"chat instance supervision: {args.deployment} — "
            f"{len(events)} pass(es), last action {last.action}, now "
            f"{last.after.state} ({last.after.status}){conditions}"
        ),
    )
    return 1 if failed else 0


def _cmd_down(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.instance import tear_down

    result = tear_down(args.deployment, runtime=_runtime(args))
    _emit(
        {"ok": True, **result},
        as_json=as_json,
        headline=(
            f"chat instance down: {args.deployment} — container "
            f"{'removed' if result['removed'] else 'was not present'}; the "
            f"app database was NOT dropped"
        ),
    )
    return 0


def _cmd_front_door(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.chat.front_door import (
        TlsMaterial,
        front_door_report,
        plan_front_door,
        render_nginx_front_door,
        routes_agree,
        serve_front_door,
    )
    from okg.substrate.chat.instance import read_instance, resolve_instance_port
    from okg.substrate.chat.sso import ConsoleBuild

    chat, _graph_dsn = _load_manifest(args.deployment, require_graph_dsn=False)

    # The port the instance ACTUALLY bound, when one is recorded; the declared
    # port otherwise. A front door generated against a port the instance is
    # not on is a file that proves nothing, so the running fact wins.
    record = read_instance(args.deployment)
    if record is not None and record.port:
        instance_port = record.port
    else:
        instance_port = resolve_instance_port(
            args.deployment, chat, override=args.instance_port,
        ).instance_port

    # Both or neither. One alone is refused here rather than at nginx, which
    # would report it as a broken configuration on the serving host instead of
    # as a missing operator input at the moment the file was generated.
    if bool(args.tls_certificate) != bool(args.tls_private_key):
        raise SubstrateError(
            "a TLS front door needs both --tls-certificate and "
            "--tls-private-key",
            data={
                "deployment": args.deployment,
                "reason": "tls_material_incomplete",
                "certificate": bool(args.tls_certificate),
                "private_key": bool(args.tls_private_key),
            },
            fix_hint="pass both paths, or neither",
        )
    tls = (
        TlsMaterial(
            certificate=args.tls_certificate, private_key=args.tls_private_key,
        )
        if args.tls_certificate
        else None
    )

    plan = plan_front_door(
        args.deployment,
        chat_block=chat,
        instance_port=instance_port,
        instance_host=args.host,
        tls=tls,
        console_build=ConsoleBuild(
            declared_base_aware=bool(args.console_base_aware),
            index_path=args.console_bundle_index,
        ) if (args.console_base_aware or args.console_bundle_index) else None,
        console_upstream=args.console_upstream,
    )
    configuration = render_nginx_front_door(plan)
    disagreements = routes_agree(plan, configuration)
    if disagreements:  # pragma: no cover - a renderer regression, not an input
        raise SubstrateError(
            "the rendered front door disagrees with its own route table",
            data={"deployment": args.deployment, "problems": list(disagreements)},
        )

    if args.output:
        from pathlib import Path

        Path(args.output).write_text(configuration)

    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(
            {"ok": True, **front_door_report(plan, configuration)},
            sort_keys=True, default=str,
        ))
    elif not args.serve:
        print(configuration, end="")

    if args.serve:
        with contextlib.closing(
            serve_front_door(plan, host=args.host, port=args.serve_port)
        ) as server:
            print(
                f"front door for {args.deployment} serving chat at "
                f"{server.base_url}/ -> {plan.chat_upstream} "
                f"({'single-operator local auth'}); ctrl-c to stop",
                file=sys.stderr,
            )
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:  # pragma: no cover - operator interrupt
                pass
    return 0


def _receipt_context(args: argparse.Namespace) -> tuple[Any, Any, Any]:
    """The manifest's chat block, the deployment's app-database target, and
    the sync inputs — resolved exactly the way ``up`` and ``sync`` resolve
    them, so a receipt names the same database and the same ids they do."""
    from okg.substrate.chat.instance import resolve_app_database
    from okg.substrate.chat.sync import load_inputs

    chat, graph_dsn = _load_manifest(args.deployment, require_graph_dsn=True)
    app_database = resolve_app_database(
        args.deployment,
        env_var=args.app_database_url_env,
        environ=os.environ,
        graph_dsn=graph_dsn,
    )
    return chat, app_database.target, load_inputs(args.deployment)


def _cmd_lifecycle_receipt(args: argparse.Namespace, *, as_json: bool) -> int:
    """Thin wrapper. Every decision lives in ``chat.lifecycle``; this resolves
    the manifest, reads the documents named on the command line and prints.

    It starts and stops nothing, for any kind."""
    from okg.substrate.chat.instance import read_instance, resolve_instance_image
    from okg.substrate.chat.lifecycle import (
        KIND_RETIRE,
        KIND_ROLLBACK,
        load_lifecycle_receipt,
        retire_receipt,
        rollback_receipt,
        upgrade_receipt,
        write_lifecycle_receipt,
    )
    from okg.substrate.chat.plan_apply import load_chat_plan

    _require_receipt_flags(args)
    chat, app_target, inputs = _receipt_context(args)
    record = read_instance(args.deployment)
    if args.kind == KIND_RETIRE:
        from okg.substrate.chat.sync import InstanceClient, resolve_operator_environment

        base_url, token = resolve_operator_environment()
        document = retire_receipt(
            args.deployment,
            InstanceClient(base_url, token),
            preset_id=inputs.preset_id,
            mcp_server_id=inputs.mcp_server_id,
            app_database=app_target,
        )
    elif args.kind == KIND_ROLLBACK:
        document = rollback_receipt(
            args.deployment,
            upgrade=load_lifecycle_receipt(args.upgrade_receipt),
            current_app_database=app_target,
        )
    else:
        # The baseline is read from the RECORD, both for the image and for the
        # app database. Without one there is no baseline at all, and falling
        # back to the current values on either side would compare a value
        # against itself and report an unchanged database that was never
        # checked.
        baseline_image = _recorded_image(args.deployment, record)
        document = upgrade_receipt(
            args.deployment,
            baseline_image=baseline_image,
            candidate_image=resolve_instance_image(args.deployment, chat).image,
            baseline_app_database=record.app_database,
            candidate_app_database=app_target,
            plan_digest=load_chat_plan(args.plan)["plan_digest"],
            readback=json.loads(
                pathlib.Path(args.readback).read_text(encoding="utf-8")
            ),
        )
    written = write_lifecycle_receipt(document, args.out)
    if as_json or os.environ.get("OKG_AGENT") == "1":
        print(json.dumps(document, sort_keys=True, default=str))
    else:
        print(
            f"chat {document['kind']} receipt -> {written['receipt_path']} "
            f"({written['receipt_digest'][:12]}); no container was started or "
            "stopped"
        )
    return 0


#: Which flags each kind cannot proceed without. argparse cannot express
#: "required for this choice", and defaulting a missing document would make
#: the run compare against nothing.
_RECEIPT_REQUIRED_FLAGS = {
    "upgrade": ("plan", "readback"),
    "rollback": ("upgrade_receipt",),
    "retire": (),
}


def _require_receipt_flags(args: argparse.Namespace) -> None:
    from okg.substrate.chat.lifecycle import ChatLifecycleError

    missing = [
        "--" + name.replace("_", "-")
        for name in _RECEIPT_REQUIRED_FLAGS[args.kind]
        if not getattr(args, name, None)
    ]
    if missing:
        raise ChatLifecycleError(
            f"a {args.kind} receipt needs {missing}",
            data={
                "reason": "receipt_input_missing",
                "kind": args.kind,
                "missing": missing,
            },
            fix_hint=f"pass {missing}",
        )


def _recorded_image(deployment: str, record: Any) -> str:
    """The image the instance is RECORDED as running.

    Not the manifest's: an upgrade's baseline is what is running, and reading
    it from the same file the candidate comes from would compare a value
    against itself and call every upgrade a no-op.
    """
    from okg.substrate.chat.instance import ChatInstanceError

    image = getattr(record, "image", None)
    if not image:
        raise ChatInstanceError(
            f"deployment {deployment!r} has no recorded instance image, so "
            "there is no baseline to upgrade from",
            data={"reason": "no_recorded_image", "deployment": deployment},
            fix_hint="stand the instance up first, or record the rollback pair",
        )
    return str(image)


_SUBCOMMANDS = {
    "up": _cmd_up,
    "status": _cmd_status,
    "supervise": _cmd_supervise,
    "down": _cmd_down,
    "front-door": _cmd_front_door,
    "lifecycle-receipt": _cmd_lifecycle_receipt,
}


def _cmd_chat_instance(args: argparse.Namespace, *, as_json: bool) -> int:
    handler = _SUBCOMMANDS.get(args.chat_instance_subcmd)
    if handler is None:  # pragma: no cover - argparse constrains the choices
        raise SystemExit(
            f"unknown chat-instance subcommand: {args.chat_instance_subcmd!r}"
        )
    try:
        return handler(args, as_json=as_json)
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)


def add_chat_instance_parser(sub: Any, *, add_json_flag: Any,
                             add_deployment_arg: Any) -> Any:
    """Build the ``chat-instance`` parser.

    Lives here rather than in the dispatcher so the CLI addition in
    ``cli/main.py`` stays three lines: an import, a subcommand-set entry and a
    dispatch entry.
    """
    from okg.substrate.chat.instance import (
        DEFAULT_APP_DATABASE_ENV,
        DEFAULT_SECRET_KEY_ENV,
        IMAGE_KEY,
        INSTANCE_PORT_KEY,
        PINNED_IMAGE_REF,
    )

    parser = sub.add_parser(
        "chat-instance",
        help=(
            "Stand up, observe, supervise and tear down a deployment's Open "
            "WebUI instance, and render its single-operator front door (PACT "
            "chat-instance-lifecycle). The app database DSN and the instance "
            "secret key are operator state, named by environment variable and "
            "never passed as values"
        ),
    )
    add_json_flag(parser)
    inner = parser.add_subparsers(dest="chat_instance_subcmd", required=True)

    def _common(target: Any, *, runtime: bool = True) -> None:
        add_json_flag(target)
        add_deployment_arg(target, required=True)
        target.add_argument(
            "--host",
            default="127.0.0.1",
            help="host interface the instance publishes on (default: 127.0.0.1)",
        )
        if runtime:
            target.add_argument(
                "--container-runtime",
                default="docker",
                help="container runtime binary: docker or podman (default: docker)",
            )

    p_up = inner.add_parser(
        "up",
        help=(
            "Stand the instance up at the pinned version and prove it with "
            "GET /api/version off the instance. Idempotent; refuses by name "
            "on any missing operator-state input"
        ),
    )
    _common(p_up)
    p_up.add_argument(
        "--app-database-url-env",
        default=DEFAULT_APP_DATABASE_ENV,
        help=(
            "NAME of the environment variable holding the app database DSN "
            f"(default: {DEFAULT_APP_DATABASE_ENV}). The DSN itself is never "
            "a command-line argument and the manifest cannot carry it"
        ),
    )
    p_up.add_argument(
        "--admin-secret-env",
        default=DEFAULT_SECRET_KEY_ENV,
        help=(
            "NAME of the environment variable holding the instance secret key "
            f"(default: {DEFAULT_SECRET_KEY_ENV})"
        ),
    )
    p_up.add_argument(
        "--instance-port",
        type=int,
        help=(
            f"explicit host port, for use until chat-manifest-block admits "
            f"chat.{INSTANCE_PORT_KEY}. This is an operator input, not a "
            "default: with neither the manifest key nor this flag, stand-up "
            "REFUSES rather than choosing a port"
        ),
    )
    p_up.add_argument(
        "--image", default=None,
        help=(
            f"Open WebUI image. Defaults to chat.{IMAGE_KEY} when the "
            f"manifest declares one and to {PINNED_IMAGE_REF} when it does "
            "not; an image outside the tested set refuses "
            "chat_image_not_tested, and a flag disagreeing with the manifest "
            "refuses rather than winning"
        ),
    )
    p_up.add_argument(
        "--host-gateway-alias",
        default="host.docker.internal",
        help=(
            "hostname the container reaches the host's loopback on. Used only "
            "when the app database is on 127.0.0.1, and reported as a note "
            "when it is; pass '' to disable"
        ),
    )
    add_container_network_arg(p_up)
    p_up.add_argument(
        "--ready-timeout", type=float, default=120.0,
        help="seconds to wait for GET /api/version (default: 120)",
    )
    p_up.add_argument(
        "--install-plan",
        help=(
            "directory holding the plan.json an install verb wrote; the "
            "bundle's site name applies when chat.ui.site_name is not "
            "declared, as it does for okg chat setup, plan and readback. "
            "Pass it on every up: without it a bundle-named instance refuses "
            "instance_settings_changed, naming the recorded plan"
        ),
    )

    p_status = inner.add_parser(
        "status",
        help=(
            "Report the instance's state, derived from GET /api/version and "
            "never from the launch record. Exits non-zero when it is not up"
        ),
    )
    _common(p_status, runtime=False)

    p_sup = inner.add_parser(
        "supervise",
        help="Observe, restart what does not answer, observe again",
    )
    _common(p_sup)
    p_sup.add_argument("--iterations", type=int, default=1)
    p_sup.add_argument("--interval", type=float, default=5.0)

    p_down = inner.add_parser(
        "down",
        help=(
            "Stop and remove the container and forget the record. The app "
            "database is NOT dropped"
        ),
    )
    _common(p_down)

    p_receipt = inner.add_parser(
        "lifecycle-receipt",
        help=(
            "Write an inert upgrade, rollback or retire receipt for the chat "
            "layer (PACT chat-layer-lifecycle). Starts and stops NOTHING: it "
            "records the image change or the retirement, proves the app "
            "database is the same database, and refuses an untested image, a "
            "baseline that deviates from its plan, or a retirement while an "
            "OKG-owned surface is still configured"
        ),
    )
    _common(p_receipt, runtime=False)
    p_receipt.add_argument(
        "--kind", required=True, choices=["upgrade", "rollback", "retire"],
    )
    p_receipt.add_argument(
        "--out", required=True,
        help="output DIRECTORY, which must not already exist",
    )
    p_receipt.add_argument(
        "--app-database-url-env",
        default=DEFAULT_APP_DATABASE_ENV,
        help=(
            "NAME of the variable holding the app database DSN (default: "
            f"{DEFAULT_APP_DATABASE_ENV}). Only the host, port and database "
            "name it resolves to reach a receipt"
        ),
    )
    p_receipt.add_argument(
        "--plan", help="upgrade: the chat-plan.json the readback was taken against",
    )
    p_receipt.add_argument(
        "--readback", help="upgrade: the chat-readback.json proving the baseline",
    )
    p_receipt.add_argument(
        "--upgrade-receipt",
        help="rollback: the upgrade receipt this run reverses",
    )

    p_fd = inner.add_parser(
        "front-door",
        help=(
            "Render the single-operator reverse-proxy configuration: one "
            "hostname, chat at /, and an explicit not-configured response at "
            "/console until operator-ui-base-aware-build lands"
        ),
    )
    _common(p_fd, runtime=False)
    p_fd.add_argument(
        "--instance-port", type=int,
        help="instance port, when none is recorded and none is declared",
    )
    p_fd.add_argument("--output", help="write the configuration to this path")
    p_fd.add_argument(
        "--tls-certificate",
        help=(
            "PATH, on the proxy host, of the certificate chain this front "
            "serves. REQUIRED when chat.deep_links.chat_site_base is https: "
            "without it the site base's scheme would render `listen 443;` with "
            "no TLS at all, so an https base refuses instead"
        ),
    )
    p_fd.add_argument(
        "--tls-private-key",
        help=(
            "PATH, on the proxy host, of the private key. A path, never the "
            "key: nginx reads the file and okg never does"
        ),
    )
    p_fd.add_argument(
        "--serve", action="store_true",
        help=(
            "also run the reference front door over the same plan, for local "
            "bring-up and proof on a machine with no nginx"
        ),
    )
    p_fd.add_argument("--serve-port", type=int, default=0)
    p_fd.add_argument(
        "--console-bundle-index",
        help="built console index.html, measured for base-awareness",
    )
    p_fd.add_argument(
        "--console-base-aware", action="store_true",
        help="declare the console bundle base-aware (a capability gate, not a proof)",
    )
    p_fd.add_argument(
        "--console-upstream",
        help=(
            "origin serving the built console, e.g. http://127.0.0.1:5173. "
            "REQUIRED once the bundle is measured or declared base-aware, and "
            "refused when it is not: it is never inferred from "
            "chat.deep_links.operator_ui_base, which is the console's PUBLIC "
            "base and on a fronted deployment is this very hostname"
        ),
    )
    return parser
