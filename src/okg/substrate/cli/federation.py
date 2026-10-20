"""CLI helpers for the OKG federation contract."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.error import URLError

from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    open_validated_url,
)

from okg.substrate.cli._options import (
    add_deployment_arg,
    add_dsn_arg,
)
from okg.substrate.cli._output import emit_result
from okg.substrate.federation import (
    DetachedObjectPointer,
    DiscoveryDocument,
    FederationValidationError,
    LocalFederationRegistry,
    PostgresFederationRegistry,
    RouteRequest,
    assess_exposure_readiness,
    authority_id_from_okg_uri,
    board_address,
    default_action_for_target,
    pull_surface_address,
    validate_pull_packet,
)
from okg.substrate.federation.mailbox import verify_pointer_bytes
from okg.substrate.federation.mailbox.models import (
    MAILBOX_PAYLOAD_DETACHED_POINTER,
    MAILBOX_PAYLOAD_INLINE,
    canonical_mailbox_json_bytes,
)
from okg.substrate.federation.registry import (
    FEDERATION_GRAPH_PULL,
    FEDERATION_MESSAGE_SEND,
)


JsonFlagAdder = Callable[[argparse.ArgumentParser], None]


def register_subparsers(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    _register_connect_subparser(sub, add_json_flag=add_json_flag)
    _register_send_subparser(sub, add_json_flag=add_json_flag)
    _register_pull_subparser(sub, add_json_flag=add_json_flag)
    _register_inbox_subparser(sub, add_json_flag=add_json_flag)
    _register_proof_subparser(sub, add_json_flag=add_json_flag)

    p_fed = sub.add_parser(
        "federation",
        help="OKG federation discovery, route, and pull helpers",
    )
    add_json_flag(p_fed)
    fed_sub = p_fed.add_subparsers(dest="federation_subcmd", required=True)

    p_identity = fed_sub.add_parser("identity", help="Inspect local identity")
    add_json_flag(p_identity)
    identity_sub = p_identity.add_subparsers(dest="identity_subcmd", required=True)
    p_identity_show = identity_sub.add_parser("show")
    add_json_flag(p_identity_show)
    p_identity_show.add_argument("--authority", required=True)

    p_discover = fed_sub.add_parser(
        "discover",
        help="Validate a public federation discovery document",
    )
    add_json_flag(p_discover)
    p_discover.add_argument("locator", help="Local path to discovery JSON")

    p_candidate = fed_sub.add_parser("candidate", help="Inspect candidates")
    add_json_flag(p_candidate)
    candidate_sub = p_candidate.add_subparsers(
        dest="candidate_subcmd",
        required=True,
    )
    p_candidate_inspect = candidate_sub.add_parser("inspect")
    add_json_flag(p_candidate_inspect)
    p_candidate_inspect.add_argument("--discovery", required=True)

    p_pull_surface = fed_sub.add_parser(
        "pull-surface",
        help="Inspect exposed pull surfaces",
    )
    add_json_flag(p_pull_surface)
    pull_surface_sub = p_pull_surface.add_subparsers(
        dest="pull_surface_subcmd",
        required=True,
    )
    p_pull_surface_list = pull_surface_sub.add_parser("list")
    add_json_flag(p_pull_surface_list)
    p_pull_surface_list.add_argument("--discovery", required=True)

    p_mailbox = fed_sub.add_parser("mailbox", help="Inspect mailbox locators")
    add_json_flag(p_mailbox)
    mailbox_sub = p_mailbox.add_subparsers(dest="mailbox_subcmd", required=True)
    p_mailbox_list = mailbox_sub.add_parser("list")
    add_json_flag(p_mailbox_list)
    p_mailbox_list.add_argument("--discovery", required=True)
    p_mailbox_pull = mailbox_sub.add_parser(
        "pull",
        help="Poll a mailbox and submit messages through receiver-owned Poros",
    )
    add_json_flag(p_mailbox_pull)
    p_mailbox_pull.add_argument("--mailbox-url", required=True)
    p_mailbox_pull.add_argument("--receiver", required=True)
    p_mailbox_pull.add_argument("--trust-pins", required=True)
    p_mailbox_pull.add_argument("--receiver-keys", required=True)
    add_dsn_arg(p_mailbox_pull)
    add_deployment_arg(p_mailbox_pull, default="default")
    p_mailbox_pull.add_argument("--nomos-bundle")
    p_mailbox_pull.add_argument(
        "--admission-mode",
        default="fast_track",
        choices=("fast_track", "review", "quarantine"),
    )
    p_mailbox_pull.add_argument("--limit", type=int, default=10)
    p_mailbox_pull.add_argument("--lease-seconds", type=int, default=60)
    p_mailbox_pull.add_argument("--apply", action="store_true")
    p_mailbox_pull.add_argument("--publish", action="store_true")
    p_mailbox_pull.add_argument("--object-store-root")
    p_mailbox_pull.add_argument(
        "--allow-http-detached",
        action="store_true",
        help="Allow HTTP(S) fetches for detached mailbox packet objects",
    )
    p_mailbox_serve = mailbox_sub.add_parser(
        "serve",
        help="Run the Poros mailbox HTTP service",
    )
    add_json_flag(p_mailbox_serve)
    add_dsn_arg(p_mailbox_serve)
    p_mailbox_serve.add_argument("--host", default="127.0.0.1")
    p_mailbox_serve.add_argument("--port", type=int, default=8788)
    p_mailbox_serve.add_argument("--object-store-root")
    p_mailbox_serve.add_argument("--discovery")
    p_mailbox_serve.add_argument(
        "--principals",
        help=(
            "JSON file mapping recipient authority id -> shared-secret token for "
            "mailbox read/finalize authentication. Without it (and without "
            "OKG_MAILBOX_PRINCIPALS) the mailbox serves fail-closed: every "
            "read/finalize request is rejected with 401."
        ),
    )
    p_mailbox_demo = mailbox_sub.add_parser(
        "demo",
        help="Run a local receiver-owned mailbox incorporation demo",
    )
    add_json_flag(p_mailbox_demo)
    add_dsn_arg(p_mailbox_demo)
    p_mailbox_demo.add_argument("--policy-dir")
    p_mailbox_demo.add_argument("--output")

    p_route = fed_sub.add_parser("route", help="Check a federation route")
    add_json_flag(p_route)
    route_sub = p_route.add_subparsers(dest="route_subcmd", required=True)
    p_route_check = route_sub.add_parser("check")
    add_json_flag(p_route_check)
    p_route_check.add_argument("--local-authority", required=True)
    p_route_check.add_argument("--discovery", required=True)
    p_route_check.add_argument("--key-fingerprint", required=True)
    p_route_check.add_argument("--action", required=True)
    p_route_check.add_argument("--target", required=True)
    p_route_check.add_argument("--subject", required=True)
    p_route_check.add_argument("--nomos-decision-id", required=True)
    p_route_check.add_argument("--visibility", default="public")

    p_pull = fed_sub.add_parser("pull", help="Validate a pulled Poros packet")
    add_json_flag(p_pull)
    pull_sub = p_pull.add_subparsers(dest="pull_subcmd", required=True)
    p_pull_validate = pull_sub.add_parser("validate")
    add_json_flag(p_pull_validate)
    p_pull_validate.add_argument("--packet", required=True)
    p_pull_validate.add_argument("--public-key", required=True)
    p_pull_validate.add_argument("--origin-authority", required=True)
    p_pull_validate.add_argument("--recipient-authority", default=None)
    p_pull_validate.add_argument("--surface", required=True)
    p_pull_validate.add_argument("--generation", type=int, default=None)
    p_pull_validate.add_argument("--catalog-reference", default=None)


def _register_connect_subparser(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_connect = sub.add_parser(
        "connect",
        help="Discover, request, approve, and audit OKG connections",
    )
    add_json_flag(p_connect)
    connect_sub = p_connect.add_subparsers(dest="connect_subcmd", required=True)

    p_discover = connect_sub.add_parser(
        "discover",
        help="Discover an OKG and store a pending local candidate",
    )
    add_json_flag(p_discover)
    p_discover.add_argument("locator")
    add_dsn_arg(p_discover)
    p_discover.add_argument("--local-authority")

    p_request = connect_sub.add_parser(
        "request",
        help="Create a scoped connection request",
    )
    add_json_flag(p_request)
    add_dsn_arg(p_request, required=True)
    p_request.add_argument("--local-authority", required=True)
    p_request.add_argument("--to", required=True)
    target = p_request.add_mutually_exclusive_group(required=True)
    target.add_argument("--board")
    target.add_argument("--surface")
    p_request.add_argument("--requester-authority")
    p_request.add_argument("--target-authority")
    p_request.add_argument("--key-fingerprint")
    p_request.add_argument("--action")
    p_request.add_argument("--visibility", default="public")

    p_approve = connect_sub.add_parser(
        "approve",
        help="Approve a scoped connection request and create a trust pin",
    )
    add_json_flag(p_approve)
    p_approve.add_argument("request_id")
    add_dsn_arg(p_approve, required=True)
    p_approve.add_argument("--actor", required=True)
    p_approve.add_argument("--nomos-decision-id", required=True)
    p_approve.add_argument("--evidence-json")

    p_deny = connect_sub.add_parser("deny", help="Deny a connection request")
    add_json_flag(p_deny)
    p_deny.add_argument("request_id")
    add_dsn_arg(p_deny, required=True)
    p_deny.add_argument("--actor", required=True)
    p_deny.add_argument("--reason-code", default="connection_request_denied")
    p_deny.add_argument("--evidence-json")

    p_readiness = connect_sub.add_parser(
        "readiness",
        help="Check whether mailbox/direct exposure is production-ready",
    )
    add_json_flag(p_readiness)
    p_readiness.add_argument(
        "--exposure",
        choices=("pilot", "world-open"),
        default="pilot",
    )
    p_readiness.add_argument("--dns-tls", action="store_true")
    p_readiness.add_argument("--auth-or-rate-limit", action="store_true")
    p_readiness.add_argument("--quotas", action="store_true")
    p_readiness.add_argument("--retention", action="store_true")
    p_readiness.add_argument("--object-store-encryption", action="store_true")
    p_readiness.add_argument("--key-rotation", action="store_true")
    p_readiness.add_argument("--audit-logs", action="store_true")
    p_readiness.add_argument("--backups", action="store_true")
    p_readiness.add_argument("--monitoring", action="store_true")
    p_readiness.add_argument("--nomos-receive-policy", action="store_true")
    p_readiness.add_argument("--network-restricted", action="store_true")

    p_demo = connect_sub.add_parser(
        "demo",
        help="Run a local end-to-end OKG connection lifecycle demo",
    )
    add_json_flag(p_demo)
    add_dsn_arg(p_demo)
    p_demo.add_argument(
        "--mailbox-url",
        help="Use an HTTP mailbox service instead of the local Postgres mailbox",
    )
    p_demo.add_argument("--policy-dir")
    p_demo.add_argument("--output")


def _register_send_subparser(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_send = sub.add_parser(
        "send",
        help="Send or prepare a routed Poros graph message",
    )
    add_json_flag(p_send)
    add_dsn_arg(p_send, required=True)
    p_send.add_argument("--local-authority", required=True)
    p_send.add_argument("--to", required=True)
    p_send.add_argument("--board", required=True)
    p_send.add_argument("--graph", required=True)
    p_send.add_argument("--subject")
    p_send.add_argument("--visibility", default="public")
    p_send.add_argument("--classification", default="public")
    p_send.add_argument("--graph-profile", default="context")
    p_send.add_argument("--origin-generation-id", type=int, default=1)
    p_send.add_argument("--message-id")
    p_send.add_argument("--ttl-seconds", type=int, default=15 * 60)
    p_send.add_argument("--signing-keys")
    p_send.add_argument("--envelope-out")
    p_send.add_argument("--mailbox-url")
    p_send.add_argument("--submit-mailbox", action="store_true")


def _register_pull_subparser(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_pull = sub.add_parser(
        "pull",
        help="Route-check and optionally validate a one-shot graph pull packet",
    )
    add_json_flag(p_pull)
    add_dsn_arg(p_pull, required=True)
    p_pull.add_argument("--local-authority", required=True)
    p_pull.add_argument("--from", dest="from_okg", required=True)
    p_pull.add_argument("--surface", required=True)
    p_pull.add_argument("--subject")
    p_pull.add_argument("--visibility", default="public")
    p_pull.add_argument("--packet")
    p_pull.add_argument("--public-key")
    p_pull.add_argument("--recipient-authority")
    p_pull.add_argument("--generation", type=int)
    p_pull.add_argument("--catalog-reference")


def _register_inbox_subparser(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_inbox = sub.add_parser(
        "inbox",
        help="Inspect and act on receiver-owned Poros inbox state",
    )
    add_json_flag(p_inbox)
    inbox_sub = p_inbox.add_subparsers(dest="inbox_subcmd", required=True)

    p_list = inbox_sub.add_parser("list")
    add_json_flag(p_list)
    add_dsn_arg(p_list, required=True)
    p_list.add_argument("--receiver")
    p_list.add_argument("--limit", type=int, default=20)

    p_review = inbox_sub.add_parser("review")
    add_json_flag(p_review)
    p_review.add_argument("message_id")
    add_dsn_arg(p_review, required=True)

    p_promote = inbox_sub.add_parser("promote")
    add_json_flag(p_promote)
    p_promote.add_argument("message_id")
    add_dsn_arg(p_promote, required=True)
    add_deployment_arg(p_promote, default="default")
    p_promote.add_argument("--actor", required=True)
    p_promote.add_argument("--nomos-bundle", required=True)
    p_promote.add_argument("--publish", action="store_true")


def _register_proof_subparser(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_proof = sub.add_parser(
        "proof",
        help="Explain Poros/federation evidence for a message",
    )
    add_json_flag(p_proof)
    p_proof.add_argument("message_id")
    add_dsn_arg(p_proof, required=True)
    p_proof.add_argument("--sender-dsn")
    p_proof.add_argument("--mode", choices=("public", "audit"), default="public")


def cmd_federation(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        if args.federation_subcmd == "identity":
            return _cmd_identity(args, as_json=as_json)
        if args.federation_subcmd == "discover":
            return _cmd_discover(args, as_json=as_json)
        if args.federation_subcmd == "candidate":
            return _cmd_candidate(args, as_json=as_json)
        if args.federation_subcmd == "pull-surface":
            return _cmd_pull_surface(args, as_json=as_json)
        if args.federation_subcmd == "mailbox":
            return _cmd_mailbox(args, as_json=as_json)
        if args.federation_subcmd == "route":
            return _cmd_route(args, as_json=as_json)
        if args.federation_subcmd == "pull":
            return _cmd_pull(args, as_json=as_json)
    except (
        FederationValidationError,
        OSError,
        ValueError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1
    print(f"unknown federation subcommand: {args.federation_subcmd}", file=sys.stderr)
    return 2


def cmd_connect(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        if args.connect_subcmd == "discover":
            return _cmd_connect_discover(args, as_json=as_json)
        if args.connect_subcmd == "request":
            return _cmd_connect_request(args, as_json=as_json)
        if args.connect_subcmd == "approve":
            return _cmd_connect_approve(args, as_json=as_json)
        if args.connect_subcmd == "deny":
            return _cmd_connect_deny(args, as_json=as_json)
        if args.connect_subcmd == "readiness":
            return _cmd_connect_readiness(args, as_json=as_json)
        if args.connect_subcmd == "demo":
            return _cmd_connect_demo(args, as_json=as_json)
    except (
        FederationValidationError,
        OSError,
        ValueError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1
    return 2


def cmd_send(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        store = PostgresFederationRegistry(args.dsn)
        target_address = board_address(args.to, args.board)
        target_authority = _resolve_target_authority(
            store,
            local_authority_id=args.local_authority,
            okg_uri=args.to,
        )
        route = store.route_check(
            local_authority_id=args.local_authority,
            remote_authority_id=target_authority,
            subject=args.subject or args.local_authority,
            action=FEDERATION_MESSAGE_SEND,
            target_address=target_address,
            visibility=args.visibility,
        )
        payload: dict[str, Any] = {
            "status": "route_allowed" if route.allowed else "route_denied",
            "route": route.to_dict(),
            "transport": "not_attempted",
            "live_row_delta": dict(route.live_row_delta),
        }
        if not route.allowed:
            emit_result(payload, as_json=as_json)
            return 1

        graph_payload = _load_json(Path(args.graph).expanduser())
        envelope = None
        if args.signing_keys:
            envelope = _build_signed_graph_envelope(
                graph_payload,
                sender=args.local_authority,
                recipient=target_authority,
                signing_keys=_load_json(Path(args.signing_keys).expanduser()),
                message_id=args.message_id,
                ttl_seconds=args.ttl_seconds,
                origin_generation_id=args.origin_generation_id,
                classification=args.classification,
                graph_profile=args.graph_profile,
            )
            payload["envelope"] = _envelope_summary(envelope)
            if args.envelope_out:
                output = Path(args.envelope_out).expanduser().resolve()
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(
                    json.dumps(envelope, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                payload["envelope_out"] = str(output)
            if args.submit_mailbox:
                mailbox_url = args.mailbox_url or _resolved_mailbox_url(
                    store,
                    local_authority_id=args.local_authority,
                    remote_authority_id=target_authority,
                )
                if not mailbox_url:
                    raise FederationValidationError(
                        "mailbox submission requires --mailbox-url or a discovered mailbox endpoint"
                    )
                from okg.substrate.federation.mailbox import MailboxPollingClient

                delivery = MailboxPollingClient(mailbox_url).submit_envelope(envelope)
                payload["transport"] = "mailbox"
                payload["mailbox_url"] = mailbox_url
                payload["delivery"] = delivery
        else:
            payload["status"] = "route_allowed_unsigned"
            payload["next_step"] = "pass --signing-keys to create a Poros envelope"
        emit_result(payload, as_json=as_json)
        return 0
    except (
        FederationValidationError,
        OSError,
        ValueError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1


def cmd_pull(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        store = PostgresFederationRegistry(args.dsn)
        target_address = pull_surface_address(args.from_okg, args.surface)
        remote_authority = _resolve_target_authority(
            store,
            local_authority_id=args.local_authority,
            okg_uri=args.from_okg,
        )
        route = store.route_check(
            local_authority_id=args.local_authority,
            remote_authority_id=remote_authority,
            subject=args.subject or args.local_authority,
            action=FEDERATION_GRAPH_PULL,
            target_address=target_address,
            visibility=args.visibility,
        )
        payload: dict[str, Any] = {
            "status": "route_allowed" if route.allowed else "route_denied",
            "route": route.to_dict(),
            "live_row_delta": dict(route.live_row_delta),
        }
        if not route.allowed:
            emit_result(payload, as_json=as_json)
            return 1
        if args.packet:
            candidate = store.get_candidate(
                local_authority_id=args.local_authority,
                remote_authority_id=remote_authority,
            )
            public_key = (
                _read_public_key_arg(args.public_key)
                if args.public_key
                else _first_public_key(candidate)
            )
            validation = validate_pull_packet(
                _load_json(Path(args.packet).expanduser()),
                public_key=public_key,
                expected_origin_authority_id=remote_authority,
                expected_recipient_authority_id=args.recipient_authority
                or args.local_authority,
                expected_pull_surface_address=target_address,
                expected_remote_generation_id=args.generation,
                expected_catalog_reference=args.catalog_reference,
            )
            payload["packet"] = validation.to_dict()
            payload["status"] = "packet_validated"
        emit_result(payload, as_json=as_json)
        return 0
    except (
        FederationValidationError,
        OSError,
        ValueError,
        RuntimeError,
        json.JSONDecodeError,
    ) as exc:
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1


def cmd_inbox(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        if args.inbox_subcmd == "list":
            return _cmd_inbox_list(args, as_json=as_json)
        if args.inbox_subcmd == "review":
            return _cmd_inbox_review(args, as_json=as_json)
        if args.inbox_subcmd == "promote":
            return _cmd_inbox_promote(args, as_json=as_json)
    except Exception as exc:  # noqa: BLE001 - CLI returns structured errors.
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1
    return 2


def cmd_proof(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        proof = _poros_proof(
            dsn=args.dsn,
            message_id=args.message_id,
            sender_dsn=args.sender_dsn,
            mode=args.mode,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured errors.
        emit_result({"ok": False, "error": str(exc)}, as_json=as_json, stream=sys.stderr)
        return 1
    emit_result(proof, as_json=as_json)
    return 0


def _cmd_connect_discover(args: argparse.Namespace, *, as_json: bool) -> int:
    doc = _load_discovery_locator(args.locator)
    summary = _discovery_summary(doc)
    if args.dsn or args.local_authority:
        if not args.dsn or not args.local_authority:
            raise FederationValidationError(
                "persisted discovery requires both --dsn and --local-authority"
            )
        candidate = PostgresFederationRegistry(args.dsn).discover(
            local_authority_id=args.local_authority,
            document=doc,
        )
        summary["candidate"] = candidate.to_dict()
        summary["status"] = "candidate_pending"
    else:
        summary["status"] = "discovered_not_persisted"
    emit_result(summary, as_json=as_json)
    return 0


def _cmd_connect_request(args: argparse.Namespace, *, as_json: bool) -> int:
    store = PostgresFederationRegistry(args.dsn)
    target_authority = args.target_authority
    candidate = None
    if target_authority is None:
        candidate = store.find_candidate_for_uri(
            local_authority_id=args.local_authority,
            okg_uri=args.to,
        )
        target_authority = (
            candidate.remote_authority_id
            if candidate is not None
            else authority_id_from_okg_uri(args.to)
        )
    action = args.action or default_action_for_target(
        board=args.board,
        surface=args.surface,
    )
    target_address = (
        board_address(args.to, args.board)
        if args.board
        else pull_surface_address(args.to, args.surface)
    )
    key_fingerprint = args.key_fingerprint
    if not key_fingerprint and candidate is not None and candidate.document.keys:
        key_fingerprint = candidate.document.keys[0].fingerprint
    if not key_fingerprint:
        raise FederationValidationError(
            "connection request requires --key-fingerprint unless discovery provides a peer key"
        )
    request = store.create_request(
        local_authority_id=args.local_authority,
        requester_authority_id=args.requester_authority or args.local_authority,
        target_authority_id=target_authority,
        requested_action=action,
        target_address=target_address,
        requested_visibility=args.visibility,
        key_fingerprint=key_fingerprint,
        discovery_hash=candidate.discovery_hash if candidate is not None else None,
    )
    emit_result(
        {
            "status": request.status,
            "request": request.to_dict(),
            "trust_granted": False,
            "live_row_delta": {"nodes_live": 0, "edges_live": 0},
        },
        as_json=as_json,
    )
    return 0


def _cmd_connect_approve(args: argparse.Namespace, *, as_json: bool) -> int:
    evidence = _optional_json_argument(args.evidence_json)
    request, pin = PostgresFederationRegistry(args.dsn).approve_request(
        request_id=args.request_id,
        reviewer_identity=args.actor,
        nomos_decision_id=args.nomos_decision_id,
        decision_evidence=evidence,
    )
    emit_result(
        {
            "status": "approved",
            "request": request.to_dict(),
            "trust_pin": pin.to_dict(),
            "live_row_delta": {"nodes_live": 0, "edges_live": 0},
        },
        as_json=as_json,
    )
    return 0


def _cmd_connect_deny(args: argparse.Namespace, *, as_json: bool) -> int:
    request = PostgresFederationRegistry(args.dsn).deny_request(
        request_id=args.request_id,
        reviewer_identity=args.actor,
        reason_code=args.reason_code,
        decision_evidence=_optional_json_argument(args.evidence_json),
    )
    emit_result(
        {
            "status": "denied",
            "request": request.to_dict(),
            "trust_granted": False,
            "live_row_delta": {"nodes_live": 0, "edges_live": 0},
        },
        as_json=as_json,
    )
    return 0


def _cmd_connect_readiness(args: argparse.Namespace, *, as_json: bool) -> int:
    readiness = assess_exposure_readiness(
        exposure=args.exposure,
        dns_tls=args.dns_tls,
        auth_or_rate_limit=args.auth_or_rate_limit,
        quotas=args.quotas,
        retention=args.retention,
        object_store_encryption=args.object_store_encryption,
        key_rotation=args.key_rotation,
        audit_logs=args.audit_logs,
        backups=args.backups,
        monitoring=args.monitoring,
        nomos_receive_policy=args.nomos_receive_policy,
        network_restricted=args.network_restricted,
    )
    emit_result(readiness.to_dict(), as_json=as_json)
    return 0 if readiness.ready else 1


def _cmd_connect_demo(args: argparse.Namespace, *, as_json: bool) -> int:
    if not args.dsn:
        raise FederationValidationError(
            "--dsn is required unless OKG_DSN is set"
        )
    from okg.substrate.federation.connection_demo import (
        run_connection_lifecycle_demo,
    )

    evidence = run_connection_lifecycle_demo(
        str(args.dsn),
        policy_dir=Path(args.policy_dir).expanduser().resolve()
        if args.policy_dir
        else None,
        mailbox_url=args.mailbox_url,
    )
    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        evidence = {"evidence_path": str(output), **evidence}
    emit_result(evidence, as_json=as_json)
    return 0


def _cmd_identity(args: argparse.Namespace, *, as_json: bool) -> int:
    emit_result(
        {
            "authority_id": args.authority,
            "runtime_source_of_truth": "postgres_registry",
            "inbound_exposure_required": False,
        },
        as_json=as_json,
    )
    return 0


def _cmd_discover(args: argparse.Namespace, *, as_json: bool) -> int:
    doc = _load_discovery(Path(args.locator))
    emit_result(_discovery_summary(doc), as_json=as_json)
    return 0


def _cmd_candidate(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.candidate_subcmd != "inspect":
        return 2
    doc = _load_discovery(Path(args.discovery))
    emit_result(
        {
            "candidate": {
                "authority_id": doc.authority_id,
                "status": "pending",
                "discovery_hash": doc.discovery_hash,
                "trust_granted": False,
            }
        },
        as_json=as_json,
    )
    return 0


def _cmd_pull_surface(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.pull_surface_subcmd != "list":
        return 2
    doc = _load_discovery(Path(args.discovery))
    emit_result(
        {
            "authority_id": doc.authority_id,
            "pull_surfaces": [surface.to_dict() for surface in doc.pull_surfaces],
        },
        as_json=as_json,
    )
    return 0


def _cmd_mailbox(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.mailbox_subcmd == "list":
        doc = _load_discovery(Path(args.discovery))
        emit_result(
            {
                "authority_id": doc.authority_id,
                "mailboxes": [mailbox.to_dict() for mailbox in doc.mailboxes],
                "inbound_exposure_required": False,
            },
            as_json=as_json,
        )
        return 0
    if args.mailbox_subcmd == "demo":
        return _cmd_mailbox_demo(args, as_json=as_json)
    if args.mailbox_subcmd == "pull":
        return _cmd_mailbox_pull(args, as_json=as_json)
    if args.mailbox_subcmd == "serve":
        return _cmd_mailbox_serve(args, as_json=as_json)
    return 2


def _cmd_mailbox_demo(args: argparse.Namespace, *, as_json: bool) -> int:
    if not args.dsn:
        raise ValueError("--dsn is required unless OKG_DSN is set")
    from okg.substrate.federation.mailbox.demo import run_mailbox_incorporation_demo

    evidence = run_mailbox_incorporation_demo(
        str(args.dsn),
        policy_dir=Path(args.policy_dir).expanduser().resolve()
        if args.policy_dir
        else None,
    )
    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        evidence = {"evidence_path": str(output), **evidence}
    emit_result(evidence, as_json=as_json)
    return 0


def _cmd_mailbox_pull(args: argparse.Namespace, *, as_json: bool) -> int:
    if not args.dsn:
        raise ValueError("--dsn is required unless OKG_DSN is set")
    trust_pins = _load_json(Path(args.trust_pins).expanduser())
    receiver_keys = _load_json(Path(args.receiver_keys).expanduser())
    if not isinstance(trust_pins, Mapping):
        raise FederationValidationError("trust pins file must contain an object")
    if not isinstance(receiver_keys, Mapping):
        raise FederationValidationError("receiver keys file must contain an object")
    from okg.substrate.federation.mailbox import MailboxPollingClient

    client = MailboxPollingClient(args.mailbox_url)
    result = pull_mailbox_messages(
        client=client,
        receiver=args.receiver,
        trust_pins=trust_pins,
        receiver_keys=receiver_keys,
        dsn=str(args.dsn),
        deployment=args.deployment,
        nomos_bundle=Path(args.nomos_bundle).expanduser().resolve()
        if args.nomos_bundle
        else None,
        admission_mode=args.admission_mode,
        limit=args.limit,
        lease_seconds=args.lease_seconds,
        apply=args.apply or args.publish,
        publish=args.publish,
        object_store_root=Path(args.object_store_root).expanduser().resolve()
        if args.object_store_root
        else None,
        allow_http_detached=args.allow_http_detached,
    )
    emit_result(result, as_json=as_json)
    return 0 if result["failed"] == 0 else 1


def _cmd_mailbox_serve(args: argparse.Namespace, *, as_json: bool) -> int:
    if not args.dsn:
        raise ValueError("--dsn is required unless OKG_DSN is set")
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover - optional extra.
        raise RuntimeError("uvicorn is required for mailbox serve") from exc
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.federation.mailbox import (
        FileObjectStore,
        MailboxService,
        PostgresMailboxStore,
        build_mailbox_app,
    )

    authenticator = _load_mailbox_authenticator(args)
    conn = db_connect(str(args.dsn), autocommit=True)
    try:
        PostgresMailboxStore.install_schema(conn)
        object_store = (
            FileObjectStore(Path(args.object_store_root).expanduser().resolve())
            if args.object_store_root
            else None
        )
        service = MailboxService(
            store=PostgresMailboxStore(conn),
            object_store=object_store,
        )
        discovery = (
            _load_json(Path(args.discovery).expanduser().resolve())
            if args.discovery
            else None
        )
        if as_json:
            emit_result(
                {
                    "status": "starting",
                    "host": args.host,
                    "port": args.port,
                    "object_store_configured": object_store is not None,
                    "authenticated": authenticator is not None,
                    "fail_closed": authenticator is None,
                },
                as_json=True,
            )
        uvicorn.run(
            build_mailbox_app(
                service,
                discovery_document=discovery,
                authenticator=authenticator,
            ),
            host=args.host,
            port=args.port,
        )
    finally:
        conn.close()
    return 0


def _load_mailbox_authenticator(args: argparse.Namespace) -> Any:
    """Build a mailbox authenticator from `--principals` (a JSON file) or the
    OKG_MAILBOX_PRINCIPALS env var (JSON). Returns None when neither is set, in
    which case the mailbox serves fail-closed (deny all read/finalize)."""
    from okg.substrate.federation.mailbox.auth import static_token_authenticator

    principals: Any = None
    if getattr(args, "principals", None):
        principals = _load_json(Path(args.principals).expanduser().resolve())
    else:
        raw = os.environ.get("OKG_MAILBOX_PRINCIPALS")
        if raw:
            principals = json.loads(raw)
    if not principals:
        return None
    if not isinstance(principals, Mapping):
        raise ValueError(
            "mailbox principals must be a JSON object mapping authority id -> token"
        )
    return static_token_authenticator(
        {str(k): str(v) for k, v in principals.items()}
    )


def pull_mailbox_messages(
    *,
    client: Any,
    receiver: str,
    trust_pins: Mapping[str, Any],
    receiver_keys: Mapping[str, Any],
    dsn: str,
    deployment: str,
    nomos_bundle: Path | None,
    admission_mode: str,
    limit: int,
    lease_seconds: int,
    apply: bool,
    publish: bool,
    object_store_root: Path | None = None,
    allow_http_detached: bool = False,
) -> dict[str, Any]:
    messages = client.poll(receiver, limit=limit, lease_seconds=lease_seconds)
    result: dict[str, Any] = {
        "status": "processed",
        "receiver": receiver,
        "polled": len(messages),
        "acknowledged": 0,
        "denied": 0,
        "left_unacknowledged": 0,
        "failed": 0,
        "messages": [],
    }
    for message in messages:
        item = _process_mailbox_message(
            client=client,
            message=message,
            receiver=receiver,
            trust_pins=trust_pins,
            receiver_keys=receiver_keys,
            dsn=dsn,
            deployment=deployment,
            nomos_bundle=nomos_bundle,
            admission_mode=admission_mode,
            apply=apply,
            publish=publish,
            object_store_root=object_store_root,
            allow_http_detached=allow_http_detached,
        )
        result["messages"].append(item)
        status = item["mailbox_action"]
        if status == "acknowledged":
            result["acknowledged"] += 1
        elif status == "denied":
            result["denied"] += 1
        elif status == "failed":
            result["failed"] += 1
        else:
            result["left_unacknowledged"] += 1
    return result


def _process_mailbox_message(
    *,
    client: Any,
    message: Mapping[str, Any],
    receiver: str,
    trust_pins: Mapping[str, Any],
    receiver_keys: Mapping[str, Any],
    dsn: str,
    deployment: str,
    nomos_bundle: Path | None,
    admission_mode: str,
    apply: bool,
    publish: bool,
    object_store_root: Path | None,
    allow_http_detached: bool,
) -> dict[str, Any]:
    message_id = _required_message_str(message, "message_id")
    lease_token = _required_message_str(message, "lease_token")
    try:
        raw = _mailbox_message_bytes(
            message,
            mailbox_client=client,
            object_store_root=object_store_root,
            allow_http_detached=allow_http_detached,
        )
    except _MailboxDeny as exc:
        client.deny(
            message_id,
            lease_token=lease_token,
            reason_code=exc.reason_code,
            denial_evidence=exc.evidence,
        )
        return {
            "message_id": message_id,
            "mailbox_action": "denied",
            "reason_code": exc.reason_code,
            "poros_status": None,
            "nomos_effect": None,
            "admission_status": None,
            "publish_status": None,
            "receiver_generation_id": None,
            "live_counts": None,
        }
    except Exception as exc:  # noqa: BLE001 - leave item leased for retry.
        return {
            "message_id": message_id,
            "mailbox_action": "failed",
            "error": str(exc),
            "poros_status": None,
            "nomos_effect": None,
            "admission_status": None,
            "publish_status": None,
            "receiver_generation_id": None,
            "live_counts": None,
        }

    from okg.substrate.exchange.receiver import submit_poros_envelope_bytes

    outcome = submit_poros_envelope_bytes(
        raw,
        trust_pins=dict(trust_pins),
        receiver_keys=dict(receiver_keys),
        receiver=receiver,
        dsn=dsn,
        deployment=deployment,
        admission_mode=admission_mode,
        persist_preflight=True,
        apply=apply,
        publish=publish,
        nomos_bundle=nomos_bundle,
    )
    payload = outcome.payload
    if _mailbox_pull_should_deny(payload):
        reason_code = _mailbox_denial_reason(payload)
        client.deny(
            message_id,
            lease_token=lease_token,
            reason_code=reason_code,
            denial_evidence=_denial_evidence(payload),
        )
        mailbox_action = "denied"
    elif _has_durable_receipt(payload):
        client.ack(
            message_id,
            lease_token=lease_token,
            receipt_evidence=_receipt_evidence(payload),
        )
        mailbox_action = "acknowledged"
    else:
        mailbox_action = "left_unacknowledged"
    return {
        "message_id": message_id,
        "mailbox_action": mailbox_action,
        "reason_code": (
            _mailbox_denial_reason(payload) if mailbox_action == "denied" else None
        ),
        "poros_status": payload.get("status"),
        "nomos_effect": (
            payload.get("nomos_decision", {}).get("effect")
            if isinstance(payload.get("nomos_decision"), Mapping)
            else None
        ),
        "admission_status": (
            payload.get("admission", {}).get("status")
            if isinstance(payload.get("admission"), Mapping)
            else None
        ),
        "publish_status": (
            payload.get("publish", {}).get("status")
            if isinstance(payload.get("publish"), Mapping)
            else None
        ),
        "receiver_generation_id": (
            payload.get("publish", {}).get("generation_id")
            if isinstance(payload.get("publish"), Mapping)
            else None
        ),
        "live_counts": payload.get("live_counts"),
    }


class _MailboxDeny(Exception):
    def __init__(self, reason_code: str, evidence: Mapping[str, Any]) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.evidence = dict(evidence)


def _mailbox_message_bytes(
    message: Mapping[str, Any],
    *,
    mailbox_client: Any | None,
    object_store_root: Path | None,
    allow_http_detached: bool,
) -> bytes:
    payload_mode = str(message.get("payload_mode") or "")
    envelope = message.get("envelope")
    if payload_mode == MAILBOX_PAYLOAD_INLINE or isinstance(envelope, Mapping):
        if not isinstance(envelope, Mapping):
            raise _MailboxDeny(
                "mailbox_inline_payload_missing",
                {"message_id": message.get("message_id")},
            )
        return canonical_mailbox_json_bytes(dict(envelope))
    if payload_mode == MAILBOX_PAYLOAD_DETACHED_POINTER or isinstance(
        message.get("pointer"),
        Mapping,
    ):
        pointer_raw = message.get("pointer")
        if not isinstance(pointer_raw, Mapping):
            raise _MailboxDeny(
                "mailbox_detached_pointer_missing",
                {"message_id": message.get("message_id")},
            )
        pointer = DetachedObjectPointer.from_mapping(pointer_raw)
        return _fetch_detached_pointer_bytes(
            pointer,
            mailbox_client=mailbox_client,
            object_store_root=object_store_root,
            allow_http_detached=allow_http_detached,
        )
    raise _MailboxDeny(
        "mailbox_payload_mode_unsupported",
        {
            "message_id": message.get("message_id"),
            "payload_mode": message.get("payload_mode"),
        },
    )


def _fetch_detached_pointer_bytes(
    pointer: DetachedObjectPointer,
    *,
    mailbox_client: Any | None,
    object_store_root: Path | None,
    allow_http_detached: bool,
) -> bytes:
    if object_store_root is not None:
        from okg.substrate.federation.mailbox import (
            FileObjectStore,
            fetch_and_verify_pointer,
        )

        return fetch_and_verify_pointer(pointer, FileObjectStore(object_store_root))
    fetch_object = getattr(mailbox_client, "fetch_object", None)
    if callable(fetch_object):
        data = fetch_object(pointer.message_id)
        verify_pointer_bytes(pointer, data)
        return data
    if pointer.object_uri.startswith(("http://", "https://")) and allow_http_detached:
        # SSRF guard: object_uri comes from a remote, attacker-influenceable
        # mailbox pointer. Validate it is a public http(s) target and re-validate
        # every redirect hop before any request leaves the host (the response is
        # hash-verified afterwards, but the SSRF request itself must be blocked).
        try:
            with open_validated_url(pointer.object_uri, timeout=30) as response:
                data = response.read()
        except UnsafeFetchURL as exc:
            raise RuntimeError(f"detached object fetch refused (unsafe URL): {exc}") from exc
        except (OSError, URLError) as exc:
            raise RuntimeError(f"detached object fetch failed: {exc}") from exc
        verify_pointer_bytes(pointer, data)
        return data
    raise _MailboxDeny(
        "mailbox_detached_object_fetch_not_configured",
        {
            "message_id": pointer.message_id,
            "object_uri": pointer.object_uri,
        },
    )


def _mailbox_pull_should_deny(payload: Mapping[str, Any]) -> bool:
    decision = payload.get("nomos_decision")
    if isinstance(decision, Mapping) and decision.get("effect") == "deny":
        return True
    if payload.get("status") == "error" and not _has_durable_receipt(payload):
        return True
    if payload.get("reason_code") in {"duplicate"}:
        return False
    return False


def _has_durable_receipt(payload: Mapping[str, Any]) -> bool:
    preflight = payload.get("preflight")
    if not isinstance(preflight, Mapping):
        return False
    return bool(preflight.get("inbox_id")) and str(preflight.get("status") or "") in {
        "accepted_for_admission",
        "duplicate",
    }


def _mailbox_denial_reason(payload: Mapping[str, Any]) -> str:
    decision = payload.get("nomos_decision")
    if isinstance(decision, Mapping) and decision.get("reason_code"):
        return str(decision["reason_code"])
    if payload.get("reason_code"):
        return str(payload["reason_code"])
    return "poros_mailbox_pull_denied"


def _receipt_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    preflight = payload.get("preflight") if isinstance(payload.get("preflight"), Mapping) else {}
    decision = (
        payload.get("nomos_decision")
        if isinstance(payload.get("nomos_decision"), Mapping)
        else {}
    )
    admission = (
        payload.get("admission") if isinstance(payload.get("admission"), Mapping) else {}
    )
    publish = payload.get("publish") if isinstance(payload.get("publish"), Mapping) else {}
    return {
        "poros_status": payload.get("status"),
        "poros_inbox_id": preflight.get("inbox_id"),
        "receipt_sha256": preflight.get("receipt_sha256"),
        "nomos_decision_id": decision.get("decision_id"),
        "nomos_effect": decision.get("effect"),
        "admission_id": admission.get("admission_id"),
        "receiver_generation_id": publish.get("generation_id"),
        "live_counts": payload.get("live_counts"),
    }


def _denial_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    decision = (
        payload.get("nomos_decision")
        if isinstance(payload.get("nomos_decision"), Mapping)
        else {}
    )
    preflight = (
        payload.get("preflight") if isinstance(payload.get("preflight"), Mapping) else {}
    )
    return {
        "poros_status": payload.get("status"),
        "reason_code": _mailbox_denial_reason(payload),
        "preflight": dict(preflight),
        "nomos_decision": {
            "decision_id": decision.get("decision_id"),
            "effect": decision.get("effect"),
            "reason_code": decision.get("reason_code"),
        },
        "error": payload.get("error") or payload.get("mutation_error"),
    }


def _required_message_str(message: Mapping[str, Any], key: str) -> str:
    value = message.get(key)
    if not isinstance(value, str) or not value:
        raise FederationValidationError(f"mailbox message missing {key}")
    return value


def _cmd_route(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.route_subcmd != "check":
        return 2
    doc = _load_discovery(Path(args.discovery))
    registry = LocalFederationRegistry(local_authority_id=args.local_authority)
    registry.discover(doc)
    registry.accept_peer(
        authority_id=doc.authority_id,
        key_fingerprint=args.key_fingerprint,
        allowed_actions=(args.action,),
        allowed_boards=tuple(board.address for board in doc.boards),
        allowed_pull_surfaces=tuple(surface.address for surface in doc.pull_surfaces),
        visibility_tiers=doc.visibility_tiers,
        nomos_decision_id=args.nomos_decision_id,
    )
    event = registry.route_check(
        RouteRequest(
            local_authority_id=args.local_authority,
            remote_authority_id=doc.authority_id,
            subject=args.subject,
            action=args.action,
            target_address=args.target,
            visibility=args.visibility,
        )
    )
    emit_result(event.to_dict(), as_json=as_json)
    return 0 if event.allowed else 1


def _cmd_pull(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.pull_subcmd != "validate":
        return 2
    packet = _load_json(Path(args.packet))
    public_key = _read_public_key_arg(args.public_key)
    validation = validate_pull_packet(
        packet,
        public_key=public_key,
        expected_origin_authority_id=args.origin_authority,
        expected_recipient_authority_id=args.recipient_authority,
        expected_pull_surface_address=args.surface,
        expected_remote_generation_id=args.generation,
        expected_catalog_reference=args.catalog_reference,
    )
    emit_result(validation.to_dict(), as_json=as_json)
    return 0


def _cmd_inbox_list(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.db.connection import connect as db_connect

    query = """
        SELECT message_id,
               sender_authority_id,
               receiver_authority_id,
               envelope->'body'->>'graph_id' AS graph_id,
               body_sha256,
               envelope->'body'->>'classification' AS classification,
               envelope->'body'->>'graph_profile' AS graph_profile,
               status,
               received_at,
               updated_at
          FROM okg.poros_inbox_messages
    """
    params: list[Any] = []
    if args.receiver:
        query += " WHERE receiver_authority_id = %s"
        params.append(args.receiver)
    query += " ORDER BY received_at DESC LIMIT %s"
    params.append(args.limit)
    with db_connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    messages = [
        {
            "message_id": str(row[0]),
            "sender_authority_id": str(row[1]),
            "receiver_authority_id": str(row[2]),
            "graph_id": str(row[3]),
            "body_sha256": str(row[4]),
            "classification": str(row[5]),
            "graph_profile": str(row[6]),
            "status": str(row[7]),
            "received_at": row[8].isoformat() if row[8] else None,
            "updated_at": row[9].isoformat() if row[9] else None,
        }
        for row in rows
    ]
    emit_result({"messages": messages, "count": len(messages)}, as_json=as_json)
    return 0


def _cmd_inbox_review(args: argparse.Namespace, *, as_json: bool) -> int:
    proof = _poros_proof(
        dsn=args.dsn,
        message_id=args.message_id,
        sender_dsn=None,
        mode="audit",
    )
    emit_result({"status": "review", "message": proof}, as_json=as_json)
    return 0


def _cmd_inbox_promote(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.receiver import promote_poros_message

    outcome = promote_poros_message(
        dsn=args.dsn,
        deployment=args.deployment,
        actor=args.actor,
        nomos_bundle=args.nomos_bundle,
        message_id=args.message_id,
        publish=args.publish,
    )
    emit_result(outcome.payload, as_json=as_json)
    return outcome.exit_code


def _resolve_target_authority(
    store: PostgresFederationRegistry,
    *,
    local_authority_id: str,
    okg_uri: str,
) -> str:
    candidate = store.find_candidate_for_uri(
        local_authority_id=local_authority_id,
        okg_uri=okg_uri,
    )
    if candidate is not None:
        return candidate.remote_authority_id
    return authority_id_from_okg_uri(okg_uri)


def _resolved_mailbox_url(
    store: PostgresFederationRegistry,
    *,
    local_authority_id: str,
    remote_authority_id: str,
) -> str | None:
    candidate = store.get_candidate(
        local_authority_id=local_authority_id,
        remote_authority_id=remote_authority_id,
    )
    if candidate is None:
        return None
    endpoint = candidate.document.endpoints.get("mailbox")
    if endpoint:
        return endpoint
    for mailbox in candidate.document.mailboxes:
        if mailbox.queue_ref.startswith(("http://", "https://")):
            return mailbox.queue_ref
    return None


def _first_public_key(candidate: Any) -> str:
    if candidate is None or not candidate.document.keys:
        raise FederationValidationError(
            "--public-key is required when no discovery candidate is stored"
        )
    return candidate.document.keys[0].public_key


def _optional_json_argument(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    raw = value.strip()
    payload: Any
    if raw.startswith("{"):
        payload = json.loads(raw)
    else:
        payload = _load_json(Path(raw).expanduser())
    if not isinstance(payload, Mapping):
        raise FederationValidationError("evidence JSON must be an object")
    return dict(payload)


def _build_signed_graph_envelope(
    graph_payload: Any,
    *,
    sender: str,
    recipient: str,
    signing_keys: Any,
    message_id: str | None,
    ttl_seconds: int,
    origin_generation_id: int,
    classification: str,
    graph_profile: str,
) -> dict[str, Any]:
    if not isinstance(signing_keys, Mapping):
        raise FederationValidationError("signing keys must be a JSON object")
    if not isinstance(signing_keys.get("private_key"), str):
        raise FederationValidationError("signing keys require private_key")
    kid = signing_keys.get("kid")
    if not isinstance(kid, str) or not kid:
        raise FederationValidationError("signing keys require kid")
    from okg.substrate.exchange import build_unsigned_envelope, sign_envelope

    body = _graph_body_from_payload(
        graph_payload,
        origin_authority_id=sender,
        origin_generation_id=origin_generation_id,
        classification=classification,
        graph_profile=graph_profile,
    )
    unsigned = build_unsigned_envelope(
        sender=sender,
        recipient=recipient,
        body=body,
        message_id=message_id,
        ttl_seconds=ttl_seconds,
    )
    return sign_envelope(unsigned, signing_keys["private_key"], kid=kid)


def _graph_body_from_payload(
    payload: Any,
    *,
    origin_authority_id: str,
    origin_generation_id: int,
    classification: str,
    graph_profile: str,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise FederationValidationError("graph payload must be a JSON object")
    if isinstance(payload.get("body"), Mapping):
        body = dict(payload["body"])
    else:
        nodes = payload.get("nodes") or []
        edges = payload.get("edges") or []
        if not isinstance(nodes, list) or not isinstance(edges, list):
            raise FederationValidationError("graph payload nodes/edges must be lists")
        root_node_ids = payload.get("root_node_ids")
        if root_node_ids is None:
            root_node_ids = [
                str(nodes[0]["node_id"])
            ] if nodes and isinstance(nodes[0], Mapping) and nodes[0].get("node_id") else []
        body = {
            "graph_id": str(payload.get("graph_id") or "poros:graph:connection-send"),
            "origin_authority_id": origin_authority_id,
            "origin_generation_id": origin_generation_id,
            "classification": str(payload.get("classification") or classification),
            "graph_profile": str(payload.get("graph_profile") or graph_profile),
            "root_node_ids": root_node_ids,
            "nodes": nodes,
            "edges": edges,
        }
    if not body.get("origin_authority_id"):
        body["origin_authority_id"] = origin_authority_id
    if not body.get("origin_generation_id"):
        body["origin_generation_id"] = origin_generation_id
    if not body.get("classification"):
        body["classification"] = classification
    if not body.get("graph_profile"):
        body["graph_profile"] = graph_profile
    return body


def _envelope_summary(envelope: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "message_id": envelope.get("message_id"),
        "sender": envelope.get("sender"),
        "recipient": envelope.get("recipient"),
        "body_sha256": envelope.get("body_sha256"),
        "signing_key_id": envelope.get("signing_key_id"),
        "expires_at": envelope.get("expires_at"),
    }


def _poros_proof(
    *,
    dsn: str,
    message_id: str,
    sender_dsn: str | None,
    mode: str,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.cli.poros import (
        _load_poros_proof,
        _load_sender_delivery_proof,
    )

    with db_connect(dsn, autocommit=True) as conn:
        proof = _load_poros_proof(
            conn,
            message_id=message_id,
            graph_id=None,
            body_sha256=None,
            mode=mode,
        )
    if sender_dsn:
        proof["sender_delivery"] = _load_sender_delivery_proof(
            sender_dsn,
            message_id=str(proof["message_id"]),
        )
    return proof


def _load_discovery(path: Path) -> DiscoveryDocument:
    value = _load_json(path)
    if not isinstance(value, Mapping):
        raise FederationValidationError("discovery JSON must be an object")
    return DiscoveryDocument.from_mapping(value)


def _load_discovery_locator(locator: str) -> DiscoveryDocument:
    value = _load_json_locator(locator)
    if not isinstance(value, Mapping):
        raise FederationValidationError("discovery JSON must be an object")
    return DiscoveryDocument.from_mapping(value)


def _load_json_locator(locator: str) -> Any:
    if locator.startswith(("http://", "https://")):
        # SSRF guard on the discovery locator (operator/remote-provided URL);
        # redirect hops are re-validated, never auto-followed.
        with open_validated_url(locator, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    return _load_json(Path(locator).expanduser())


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_public_key_arg(value: str) -> str:
    path = Path(value)
    if path.exists():
        raw = _load_json(path)
        if not isinstance(raw, Mapping) or not isinstance(raw.get("public_key"), str):
            raise FederationValidationError(
                "public-key file must contain a public_key string"
            )
        return raw["public_key"]
    return value


def _discovery_summary(doc: DiscoveryDocument) -> dict[str, Any]:
    return {
        "authority_id": doc.authority_id,
        "display_label": doc.display_label,
        "discovery_hash": doc.discovery_hash,
        "capabilities": list(doc.capabilities),
        "visibility_tiers": list(doc.visibility_tiers),
        "boards": [board.to_dict() for board in doc.boards],
        "pull_surfaces": [surface.to_dict() for surface in doc.pull_surfaces],
        "mailboxes": [mailbox.to_dict() for mailbox in doc.mailboxes],
        "latest_public_generation_id": doc.latest_public_generation_id,
        "catalog_reference": doc.catalog_reference,
        "secret_free": True,
        "trust_granted": False,
    }


