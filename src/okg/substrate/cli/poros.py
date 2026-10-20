"""Usability commands for the Poros exchange protocol."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from okg.substrate.cli._options import (
    add_deployment_arg,
    add_dsn_arg,
)
from okg.substrate.cli._output import emit_result
from okg.substrate.exchange import (
    KeyDescriptor,
    PorosInbox,
    TrustPinRegistry,
    build_authority_document,
    build_unsigned_envelope,
    generate_ed25519_keypair,
    new_message_id,
    record_delivery_attempt,
    record_poros_incorporation_lifecycle,
    record_poros_nomos_decision,
    sign_envelope,
    verify_receipt,
)
from okg.substrate.exchange.canonical_json import (
    canonical_json_bytes,
)
from okg.substrate.exchange.errors import PorosError
from okg.substrate.exchange.receiver import submit_poros_envelope_bytes


JsonFlagAdder = Callable[[argparse.ArgumentParser], None]


def register_subparsers(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_poros = sub.add_parser(
        "poros",
        help="Poros exchange demo and setup utilities",
    )
    add_json_flag(p_poros)
    p_poros_sub = p_poros.add_subparsers(dest="poros_subcmd", required=True)

    p_demo = p_poros_sub.add_parser(
        "demo",
        help="Run a local Poros sender-to-receiver demo",
    )
    add_json_flag(p_demo)
    p_demo.add_argument(
        "--out",
        default=".poros-demo",
        help="Directory for generated demo keys, envelope, receipt, and notes",
    )
    p_demo.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing demo artifact files in --out",
    )
    p_demo.add_argument("--sender", default="okg:lab-a")
    p_demo.add_argument("--receiver", default="okg:mit-demo")
    p_demo.add_argument("--sender-kid", default="lab-a-sign-demo")
    p_demo.add_argument("--receiver-kid", default="mit-demo-sign")
    p_demo.add_argument(
        "--graph-id",
        default=None,
        help="Graph id to place in the signed Poros body",
    )
    p_demo.add_argument(
        "--bundle-id",
        default=None,
        help="Legacy alias for --graph-id",
    )
    p_demo.add_argument(
        "--message-id",
        default=None,
        help="Optional fixed message id for repeatable demos",
    )
    p_demo.add_argument(
        "--message",
        default=(
            "Poros demo context packet: signed by a sender OKG, "
            "verified by a receiver OKG, then turned into an admission "
            "candidate with a signed receipt."
        ),
        help="Human-readable context text to include in the demo graph",
    )
    p_demo.add_argument(
        "--classification",
        default="public",
        choices=["public", "internal", "restricted"],
        help="Demo graph classification",
    )

    p_init = p_poros_sub.add_parser(
        "init",
        help="Initialize a deployment-local Poros readiness manifest",
    )
    add_json_flag(p_init)
    p_init.add_argument("--manifest", default="poros.manifest.json")
    p_init.add_argument("--authority", required=True)
    p_init.add_argument(
        "--role",
        action="append",
        choices=["sender", "receiver"],
        required=True,
        help="Enable a Poros role; repeat for sender+receiver",
    )
    p_init.add_argument("--signing-kid", default="poros-signing")
    p_init.add_argument(
        "--keys-in",
        default=None,
        help="Existing keypair JSON with kid/public_key/private_key",
    )
    p_init.add_argument(
        "--key-out",
        default=None,
        help="Where to write a generated keypair; defaults near manifest",
    )
    p_init.add_argument("--receiver-endpoint", default=None)
    p_init.add_argument("--object-store-root", default=None)
    p_init.add_argument("--nomos-bundle", default=None)
    p_init.add_argument(
        "--formal-level",
        choices=["none", "local-demo", "pilot", "production"],
        default="none",
    )
    p_init.add_argument(
        "--redaction-mode",
        choices=["public", "audit"],
        default="public",
    )
    p_init.add_argument("--overwrite", action="store_true")

    p_peer = p_poros_sub.add_parser(
        "peer",
        help="Manage local Poros peer pins in the readiness manifest",
    )
    add_json_flag(p_peer)
    p_peer_sub = p_peer.add_subparsers(dest="peer_subcmd", required=True)

    def _add_manifest_arg(parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--manifest", default="poros.manifest.json")

    p_peer_add = p_peer_sub.add_parser("add", help="Pin a Poros peer key")
    add_json_flag(p_peer_add)
    _add_manifest_arg(p_peer_add)
    p_peer_add.add_argument("--authority", required=True)
    p_peer_add.add_argument("--kid", required=True)
    p_peer_add.add_argument(
        "--public-key",
        required=True,
        help="Peer Ed25519 public key or JSON key file with public_key",
    )
    p_peer_add.add_argument("--endpoint", default=None)
    p_peer_add.add_argument("--display-name", default=None)
    p_peer_add.add_argument(
        "--allow-replace",
        action="store_true",
        help="Replace key material for the same kid without rotation evidence",
    )

    p_peer_list = p_peer_sub.add_parser("list", help="List pinned Poros peers")
    add_json_flag(p_peer_list)
    _add_manifest_arg(p_peer_list)
    p_peer_list.add_argument(
        "--include-disabled",
        action="store_true",
        help="Include disabled peers in output",
    )

    p_peer_inspect = p_peer_sub.add_parser("inspect", help="Inspect one peer")
    add_json_flag(p_peer_inspect)
    _add_manifest_arg(p_peer_inspect)
    p_peer_inspect.add_argument("--authority", required=True)

    p_peer_disable = p_peer_sub.add_parser("disable", help="Disable one peer")
    add_json_flag(p_peer_disable)
    _add_manifest_arg(p_peer_disable)
    p_peer_disable.add_argument("--authority", required=True)

    p_peer_rotate = p_peer_sub.add_parser(
        "rotate-key",
        help="Rotate a pinned peer signing key with old/new evidence",
    )
    add_json_flag(p_peer_rotate)
    _add_manifest_arg(p_peer_rotate)
    p_peer_rotate.add_argument("--authority", required=True)
    p_peer_rotate.add_argument("--old-kid", required=True)
    p_peer_rotate.add_argument("--new-kid", required=True)
    p_peer_rotate.add_argument(
        "--public-key",
        required=True,
        help="New peer Ed25519 public key or JSON key file with public_key",
    )

    p_readiness = p_poros_sub.add_parser(
        "readiness",
        help="Evaluate local Poros deployment readiness",
    )
    add_json_flag(p_readiness)
    p_readiness.add_argument("--manifest", default="poros.manifest.json")
    p_readiness.add_argument(
        "--level",
        choices=["local-demo", "pilot", "production"],
        default="local-demo",
    )
    p_readiness.add_argument(
        "--require-transfer-proof",
        action="store_true",
        default=None,
        help="Require manifest evidence for a successful transfer proof",
    )

    p_send = p_poros_sub.add_parser(
        "send",
        help="Export and deliver a published subgraph using a Poros manifest peer",
    )
    add_json_flag(p_send)
    p_send.add_argument("--manifest", default="poros.manifest.json")
    add_dsn_arg(p_send, required=True)
    p_send.add_argument(
        "--peer",
        required=True,
        help="Registered recipient authority id",
    )
    p_send.add_argument("--sender-generation-id", type=str, required=True)
    p_send.add_argument("--graph-id", required=True)
    p_send.add_argument(
        "--root-node-id",
        action="append",
        required=True,
        help="Root node id to include in the exported subgraph; repeatable",
    )
    p_send.add_argument("--classification", default="public")
    p_send.add_argument("--graph-profile", default="context")
    p_send.add_argument("--message-id", default=None)
    p_send.add_argument("--ttl-seconds", type=int, default=15 * 60)
    p_send.add_argument("--detach", action="store_true")
    p_send.add_argument("--object-store-root", default=None)
    p_send.add_argument(
        "--detached-chunk-size-bytes",
        type=int,
        default=64 * 1024,
    )
    p_send.add_argument(
        "--deliver",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deliver after enqueueing the outbox message",
    )
    p_send.add_argument(
        "--receiver-inbox-url",
        default=None,
        help="Override the peer manifest endpoint for delivery",
    )
    p_send.add_argument("--timeout-seconds", type=float, default=10.0)
    p_send.add_argument("--stale-after-seconds", type=int, default=300)
    p_send.add_argument("--max-attempts", type=int, default=3)
    p_send.add_argument("--retry-delay-seconds", type=int, default=60)

    p_receive = p_poros_sub.add_parser(
        "receive",
        help="Prepare receiver service configuration from a Poros manifest",
    )
    add_json_flag(p_receive)
    p_receive.add_argument("--manifest", default="poros.manifest.json")
    # `<receiver-dsn>` was a placeholder default that bypassed the
    # OKG_DSN env fallback; align with the shared helper.
    add_dsn_arg(p_receive)
    add_deployment_arg(p_receive, default="default")
    p_receive.add_argument(
        "--level",
        choices=["local-demo", "pilot", "production"],
        default="local-demo",
    )
    p_receive.add_argument(
        "--trust-pins-out",
        default=None,
        help="Where to write active peer trust pins; defaults beside manifest",
    )
    p_receive.add_argument("--host", default="127.0.0.1")
    p_receive.add_argument("--port", type=int, default=8765)
    p_receive.add_argument("--apply", action="store_true")
    p_receive.add_argument("--publish", action="store_true")

    p_submit = p_poros_sub.add_parser(
        "submit",
        help="Verify a Poros envelope and optionally admit/publish it",
    )
    add_json_flag(p_submit)
    p_submit.add_argument(
        "--envelope",
        required=True,
        help="Path to canonical Poros envelope bytes",
    )
    p_submit.add_argument(
        "--trust-pins",
        required=True,
        help="Path to local receiver trust-pin JSON",
    )
    p_submit.add_argument(
        "--receiver-keys",
        required=True,
        help="Path to receiver keypair JSON used to sign receipts",
    )
    p_submit.add_argument(
        "--receiver",
        default=None,
        help="Receiver authority id; defaults to envelope.recipient",
    )
    add_dsn_arg(p_submit)
    p_submit.add_argument(
        "--sender-dsn",
        default=None,
        help="Optional sender DB DSN for recording delivery attempts",
    )
    add_deployment_arg(p_submit, default="default")
    p_submit.add_argument(
        "--admission-mode",
        choices=["fast_track", "review", "quarantine"],
        default="fast_track",
    )
    p_submit.add_argument(
        "--apply",
        action="store_true",
        help="Actually write admission/fact rows when --dsn is supplied",
    )
    p_submit.add_argument(
        "--persist-preflight",
        action="store_true",
        help="Persist receiver inbox evidence and signed receipt without admission",
    )
    p_submit.add_argument(
        "--publish",
        action="store_true",
        help="Run one normal publisher cycle after admission",
    )
    p_submit.add_argument(
        "--nomos-bundle",
        default=None,
        help="Optional compiled Nomos policy bundle to evaluate before admission",
    )
    p_submit.add_argument(
        "--nomos-identity-claim",
        default=None,
        help=(
            "Optional JSON file containing an institutional Nomos identity "
            "claim for Poros admission"
        ),
    )
    p_submit.add_argument(
        "--nomos-decision-out",
        default=None,
        help="Optional path for the Nomos decision JSON",
    )

    p_proof = p_poros_sub.add_parser(
        "proof",
        help="Show receiver-side Poros incorporation proof",
    )
    add_json_flag(p_proof)
    add_dsn_arg(p_proof, required=True)
    p_proof.add_argument(
        "--sender-dsn",
        default=None,
        help="Optional sender DB DSN for outbox/delivery-attempt evidence",
    )
    lookup = p_proof.add_mutually_exclusive_group(required=True)
    lookup.add_argument("--message-id")
    lookup.add_argument("--graph-id")
    lookup.add_argument("--body-sha256")
    p_proof.add_argument(
        "--mode",
        choices=["public", "audit"],
        default="public",
        help="Proof visibility mode",
    )

    p_deliver = p_poros_sub.add_parser(
        "deliver",
        help="Deliver sender outbox messages to a receiver HTTP inbox",
    )
    add_json_flag(p_deliver)
    add_dsn_arg(p_deliver, required=True)
    p_deliver.add_argument("--receiver-inbox-url", required=True)
    p_deliver.add_argument(
        "--receiver-public-key",
        required=True,
        help="Receiver Ed25519 public key or JSON key file with public_key",
    )
    p_deliver.add_argument("--message-id", default=None)
    p_deliver.add_argument("--limit", type=int, default=1)
    p_deliver.add_argument("--timeout-seconds", type=float, default=10.0)
    p_deliver.add_argument("--stale-after-seconds", type=int, default=300)
    p_deliver.add_argument("--max-attempts", type=int, default=3)
    p_deliver.add_argument("--retry-delay-seconds", type=int, default=60)

    p_status = p_poros_sub.add_parser(
        "status",
        help="Show durable Poros sender or receiver transport status",
    )
    add_json_flag(p_status)
    add_dsn_arg(p_status, required=True)
    p_status.add_argument("--side", choices=["sender", "receiver"], required=True)
    p_status.add_argument("--message-id", default=None)
    p_status.add_argument("--graph-id", default=None)
    p_status.add_argument("--limit", type=int, default=20)

    p_promote = p_poros_sub.add_parser(
        "promote",
        help="Promote a reviewed/quarantined receiver message",
    )
    add_json_flag(p_promote)
    add_dsn_arg(p_promote, required=True)
    add_deployment_arg(p_promote, default="default")
    p_promote.add_argument("--actor", required=True)
    p_promote.add_argument("--nomos-bundle", required=True)
    p_promote.add_argument(
        "--envelope",
        default=None,
        help="Optional envelope copy to prove the promoted body was not mutated",
    )
    promote_lookup = p_promote.add_mutually_exclusive_group(required=True)
    promote_lookup.add_argument("--message-id")
    promote_lookup.add_argument("--graph-id")
    promote_lookup.add_argument("--body-sha256")
    p_promote.add_argument(
        "--publish",
        action="store_true",
        help="Run the normal publisher after promotion admission",
    )

    p_serve = p_poros_sub.add_parser(
        "serve",
        help="Run a local Poros receiver HTTP ingress service",
    )
    add_json_flag(p_serve)
    add_dsn_arg(p_serve, required=True)
    p_serve.add_argument(
        "--sender-dsn",
        default=None,
        help="Optional sender DB DSN used to record sender-side delivery callbacks",
    )
    p_serve.add_argument("--trust-pins", required=True)
    p_serve.add_argument("--receiver-keys", required=True)
    p_serve.add_argument("--receiver", required=True)
    add_deployment_arg(p_serve, default="default")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8765)
    p_serve.add_argument("--base-url", default=None)
    p_serve.add_argument("--nomos-bundle", default=None)
    p_serve.add_argument("--object-store-root", default=None)
    p_serve.add_argument("--apply", action="store_true")
    p_serve.add_argument("--publish", action="store_true")
    p_serve.add_argument(
        "--no-persist-preflight",
        action="store_true",
        help="Do not persist inbox evidence before returning receipts",
    )

    p_harness = p_poros_sub.add_parser(
        "harness-init",
        help="Initialize a two-OKG Poros sender/receiver harness",
    )
    add_json_flag(p_harness)
    p_harness.add_argument("--sender-dsn", required=True)
    p_harness.add_argument("--receiver-dsn", required=True)
    p_harness.add_argument("--sender-deployment", default="poros-sender")
    p_harness.add_argument("--receiver-deployment", default="poros-receiver")
    p_harness.add_argument(
        "--skip-migrate",
        action="store_true",
        help="Assume both DSNs are already migrated",
    )

    p_hardening = p_poros_sub.add_parser(
        "hardening-harness",
        help="Run the repeatable Poros hardening sender/receiver harness",
    )
    add_json_flag(p_hardening)
    p_hardening.add_argument("--sender-dsn", required=True)
    p_hardening.add_argument("--receiver-dsn", required=True)
    p_hardening.add_argument("--sender-deployment", default="poros-sender")
    p_hardening.add_argument("--receiver-deployment", default="poros-receiver")
    p_hardening.add_argument(
        "--out",
        default=".scratch/poros-hardening-harness",
        help="Directory for generated keys, policy, objects, and result JSON",
    )
    p_hardening.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing hardening harness artifacts in --out",
    )
    p_hardening.add_argument(
        "--skip-migrate",
        action="store_true",
        help="Assume both DSNs are already migrated",
    )
    p_hardening.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Allow existing Poros rows in the sender/receiver databases",
    )

    p_readiness_harness = p_poros_sub.add_parser(
        "readiness-harness",
        help="Run the Poros deployment-readiness sender/receiver harness",
    )
    add_json_flag(p_readiness_harness)
    p_readiness_harness.add_argument("--sender-dsn", required=True)
    p_readiness_harness.add_argument("--receiver-dsn", required=True)
    p_readiness_harness.add_argument("--sender-deployment", default="poros-sender")
    p_readiness_harness.add_argument("--receiver-deployment", default="poros-receiver")
    p_readiness_harness.add_argument(
        "--out",
        default=".scratch/poros-readiness-harness",
        help="Directory for generated manifests, keys, policy, objects, and reports",
    )
    p_readiness_harness.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing readiness harness artifacts in --out",
    )
    p_readiness_harness.add_argument(
        "--skip-migrate",
        action="store_true",
        help="Assume both DSNs are already migrated",
    )
    p_readiness_harness.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Allow existing Poros rows in the sender/receiver databases",
    )

    p_export = p_poros_sub.add_parser(
        "outbox-export",
        help="Export a published subgraph into a durable Poros outbox message",
    )
    add_json_flag(p_export)
    add_dsn_arg(p_export, required=True)
    p_export.add_argument("--sender", required=True)
    p_export.add_argument("--recipient", required=True)
    p_export.add_argument("--sender-generation-id", type=str, required=True)
    p_export.add_argument("--graph-id", required=True)
    p_export.add_argument(
        "--root-node-id",
        action="append",
        required=True,
        help="Root node id to include in the exported subgraph; repeatable",
    )
    p_export.add_argument(
        "--signing-keys",
        required=True,
        help="JSON keypair with kid/private_key/public_key fields",
    )
    p_export.add_argument("--classification", default="public")
    p_export.add_argument("--graph-profile", default="context")
    p_export.add_argument("--message-id", default=None)
    p_export.add_argument("--ttl-seconds", type=int, default=15 * 60)
    p_export.add_argument(
        "--detach",
        action="store_true",
        help=(
            "write the exported graph body to --object-store-root "
            "and sign a detached manifest"
        ),
    )
    p_export.add_argument(
        "--object-store-root",
        default=None,
        help="local object-store root for detached Poros object export",
    )
    p_export.add_argument(
        "--detached-chunk-size-bytes",
        type=int,
        default=64 * 1024,
        help="chunk size for detached object manifests",
    )
    p_export.add_argument(
        "--envelope-out",
        default=None,
        help="Optional path for canonical signed envelope bytes",
    )


def cmd_poros(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.poros_subcmd == "demo":
        return _cmd_demo(args, as_json=as_json)
    if args.poros_subcmd == "init":
        return _cmd_readiness_init(args, as_json=as_json)
    if args.poros_subcmd == "peer":
        return _cmd_peer(args, as_json=as_json)
    if args.poros_subcmd == "readiness":
        return _cmd_readiness(args, as_json=as_json)
    if args.poros_subcmd == "send":
        return _cmd_send(args, as_json=as_json)
    if args.poros_subcmd == "receive":
        return _cmd_receive(args, as_json=as_json)
    if args.poros_subcmd == "submit":
        return _cmd_submit(args, as_json=as_json)
    if args.poros_subcmd == "proof":
        return _cmd_proof(args, as_json=as_json)
    if args.poros_subcmd == "deliver":
        return _cmd_deliver(args, as_json=as_json)
    if args.poros_subcmd == "status":
        return _cmd_status(args, as_json=as_json)
    if args.poros_subcmd == "promote":
        return _cmd_promote(args, as_json=as_json)
    if args.poros_subcmd == "serve":
        return _cmd_serve(args, as_json=as_json)
    if args.poros_subcmd == "harness-init":
        return _cmd_harness_init(args, as_json=as_json)
    if args.poros_subcmd in {"hardening-harness", "readiness-harness"}:
        return _cmd_hardening_harness(args, as_json=as_json)
    if args.poros_subcmd == "outbox-export":
        return _cmd_outbox_export(args, as_json=as_json)
    print(f"unknown poros subcommand: {args.poros_subcmd}", file=sys.stderr)
    return 2


def _cmd_readiness_init(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.readiness import (
        PorosReadinessError,
        create_manifest,
        readiness_report,
        save_manifest,
        signing_key_ref,
    )

    manifest_path = Path(args.manifest).expanduser().resolve()
    try:
        if manifest_path.exists() and not args.overwrite:
            raise PorosReadinessError(
                "Poros manifest already exists",
                reason_code="manifest_exists",
                data={"path": str(manifest_path)},
            )
        roles = sorted(set(args.role or []))
        if args.keys_in:
            key_path = Path(args.keys_in).expanduser().resolve()
            key_payload = _read_json(key_path, "Poros signing keys")
            if not isinstance(key_payload, dict):
                raise ValueError("Poros signing keys must be a JSON object")
            kid = str(key_payload.get("kid") or args.signing_kid)
            public_key = str(key_payload["public_key"])
        else:
            key_pair = generate_ed25519_keypair(args.signing_kid)
            key_path = (
                Path(args.key_out).expanduser().resolve()
                if args.key_out
                else manifest_path.parent
                / f"{_authority_slug(args.authority)}-{args.signing_kid}.keys.json"
            )
            if key_path.exists() and not args.overwrite:
                raise PorosReadinessError(
                    "Poros key file already exists",
                    reason_code="key_file_exists",
                    data={"path": str(key_path)},
                )
            key_path.parent.mkdir(parents=True, exist_ok=True)
            _write_json(key_path, key_pair.__dict__, secret=True)
            kid = key_pair.kid
            public_key = key_pair.public_key
        object_store_root = (
            Path(args.object_store_root).expanduser().resolve()
            if args.object_store_root
            else manifest_path.parent / "poros-objects"
        )
        object_store_root.mkdir(parents=True, exist_ok=True)
        manifest = create_manifest(
            authority_id=args.authority,
            roles=roles,
            signing_key=signing_key_ref(
                kid=kid,
                public_key=public_key,
                private_key_path=str(key_path),
            ),
            receiver_endpoint=args.receiver_endpoint,
            object_store_root=str(object_store_root),
            nomos_bundle_path=(
                str(Path(args.nomos_bundle).expanduser().resolve())
                if args.nomos_bundle
                else None
            ),
            formal_level=args.formal_level,
            redaction_mode=args.redaction_mode,
        )
        save_manifest(manifest, manifest_path, overwrite=args.overwrite)
        report = readiness_report(manifest, level="local-demo")
    except (KeyError, OSError, ValueError, PorosReadinessError) as exc:
        _emit_readiness_error(exc, as_json=as_json)
        return 1
    payload = {
        "status": "ok",
        "manifest_path": str(manifest_path),
        "authority_id": manifest.authority_id,
        "roles": manifest.roles,
        "signing_key": manifest.signing_keys[0].to_dict(public=True),
        "key_path": str(key_path),
        "object_store": dict(manifest.object_store),
        "readiness": {
            "level": report["level"],
            "ready": report["ready"],
            "failed_checks": [
                check for check in report["checks"] if not check["ok"]
            ],
        },
        "manifest": manifest.to_dict(public=True),
    }
    emit_result(payload, as_json=as_json)
    return 0


def _cmd_peer(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.readiness import (
        PorosReadinessError,
        add_peer,
        disable_peer,
        load_manifest,
        readiness_report,
        rotate_peer_key,
        save_manifest,
        trust_pins_payload,
    )

    manifest_path = Path(args.manifest).expanduser().resolve()
    try:
        manifest = load_manifest(manifest_path)
        if args.peer_subcmd == "add":
            public_key = _read_public_key_arg(args.public_key)
            peer = add_peer(
                manifest,
                authority_id=args.authority,
                kid=args.kid,
                public_key=public_key,
                endpoint=args.endpoint,
                display_name=args.display_name,
                allow_replace=args.allow_replace,
            )
            save_manifest(manifest, manifest_path)
            payload = {
                "status": "ok",
                "action": "add",
                "manifest_path": str(manifest_path),
                "peer": peer.to_dict(public=True),
                "trust_pins": trust_pins_payload(manifest),
                "readiness": _readiness_summary(readiness_report(manifest)),
            }
        elif args.peer_subcmd == "list":
            peers = [
                peer.to_dict(public=True)
                for peer in manifest.peers
                if args.include_disabled or peer.status != "disabled"
            ]
            payload = {
                "status": "ok",
                "manifest_path": str(manifest_path),
                "peers": peers,
                "trust_pins": trust_pins_payload(manifest),
            }
        elif args.peer_subcmd == "inspect":
            peer = manifest.peer(args.authority)
            if peer is None:
                raise PorosReadinessError(
                    "Poros peer is not registered",
                    reason_code="peer_not_found",
                    data={"authority_id": args.authority},
                )
            payload = {
                "status": "ok",
                "manifest_path": str(manifest_path),
                "peer": peer.to_dict(public=True),
                "readiness": _readiness_summary(readiness_report(manifest)),
            }
        elif args.peer_subcmd == "disable":
            peer = disable_peer(manifest, authority_id=args.authority)
            save_manifest(manifest, manifest_path)
            payload = {
                "status": "ok",
                "action": "disable",
                "manifest_path": str(manifest_path),
                "peer": peer.to_dict(public=True),
                "trust_pins": trust_pins_payload(manifest),
                "readiness": _readiness_summary(readiness_report(manifest)),
            }
        elif args.peer_subcmd == "rotate-key":
            public_key = _read_public_key_arg(args.public_key)
            peer = rotate_peer_key(
                manifest,
                authority_id=args.authority,
                old_kid=args.old_kid,
                new_kid=args.new_kid,
                public_key=public_key,
            )
            save_manifest(manifest, manifest_path)
            payload = {
                "status": "ok",
                "action": "rotate-key",
                "manifest_path": str(manifest_path),
                "peer": peer.to_dict(public=True),
                "trust_pins": trust_pins_payload(manifest),
                "readiness": _readiness_summary(readiness_report(manifest)),
            }
        else:
            raise PorosReadinessError(
                "unknown Poros peer subcommand",
                reason_code="unknown_peer_subcommand",
                data={"peer_subcmd": args.peer_subcmd},
            )
    except (OSError, ValueError, PorosReadinessError) as exc:
        _emit_readiness_error(exc, as_json=as_json)
        return 1
    emit_result(payload, as_json=as_json)
    return 0


def _cmd_readiness(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.readiness import (
        PorosReadinessError,
        load_manifest,
        readiness_report,
    )

    try:
        manifest = load_manifest(Path(args.manifest).expanduser().resolve())
        payload = readiness_report(
            manifest,
            level=args.level,
            require_transfer_proof=args.require_transfer_proof,
        )
    except (OSError, ValueError, PorosReadinessError) as exc:
        _emit_readiness_error(exc, as_json=as_json)
        return 1
    emit_result(payload, as_json=as_json)
    return 0 if payload["ready"] else 1


def _cmd_send(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.outbox import (
        PorosOutboxError,
        authorize_poros_signing_key_use,
        export_published_subgraph_to_outbox,
    )
    from okg.substrate.exchange.readiness import (
        PorosReadinessError,
        load_manifest,
        require_active_peer,
        require_sender_manifest,
        require_single_active_peer_key,
        require_single_active_signing_key,
    )
    from okg.substrate.exchange.sender_delivery import (
        deliver_next_outbox_messages,
    )

    try:
        manifest_path = Path(args.manifest).expanduser().resolve()
        manifest = load_manifest(manifest_path)
        require_sender_manifest(manifest)
        local_key = require_single_active_signing_key(manifest)
        peer = require_active_peer(manifest, args.peer)
        peer_key = require_single_active_peer_key(peer)
        authorize_poros_signing_key_use(
            dsn=args.dsn,
            sender_authority_id=manifest.authority_id,
            sender_generation_id=args.sender_generation_id,
            signing_key_id=local_key.kid,
            signing_key_reference=local_key.private_key_path,
            nomos_subject=manifest.authority_id,
            use="poros.read_signing_key_file",
        )
        key_payload = _read_json(Path(local_key.private_key_path), "Poros signing keys")
        if not isinstance(key_payload, dict):
            raise ValueError("Poros signing keys must be a JSON object")
        private_key = str(key_payload["private_key"])
        key_public = str(key_payload.get("public_key") or "")
        if key_public and key_public != local_key.public_key:
            raise PorosReadinessError(
                "Poros signing key file public key does not match manifest",
                reason_code="local_signing_key_mismatch",
                data={
                    "kid": local_key.kid,
                    "private_key_path": local_key.private_key_path,
                },
            )
        receiver_inbox_url = args.receiver_inbox_url or peer.endpoint
        if args.deliver and not receiver_inbox_url:
            raise PorosReadinessError(
                "Poros peer has no receiver endpoint for delivery",
                reason_code="missing_peer_endpoint",
                data={"authority_id": peer.authority_id},
            )
        object_store_root = (
            Path(args.object_store_root).expanduser().resolve()
            if args.object_store_root
            else Path(str(manifest.object_store["root"])).expanduser().resolve()
        )
        export = export_published_subgraph_to_outbox(
            dsn=args.dsn,
            sender_authority_id=manifest.authority_id,
            recipient_authority_id=peer.authority_id,
            sender_generation_id=args.sender_generation_id,
            graph_id=args.graph_id,
            root_node_ids=args.root_node_id,
            signing_private_key=private_key,
            signing_key_id=local_key.kid,
            classification=args.classification,
            graph_profile=args.graph_profile,
            message_id=args.message_id,
            ttl_seconds=args.ttl_seconds,
            object_store_root=object_store_root,
            detach_payload=args.detach,
            detached_chunk_size_bytes=args.detached_chunk_size_bytes,
            nomos_subject=manifest.authority_id,
            signing_key_reference=local_key.private_key_path,
        )
        delivery_results = []
        if args.deliver:
            delivery_results = deliver_next_outbox_messages(
                dsn=args.dsn,
                receiver_inbox_url=receiver_inbox_url,
                receiver_public_key=peer_key.public_key,
                message_id=export.message_id,
                limit=1,
                timeout_seconds=args.timeout_seconds,
                stale_after_seconds=args.stale_after_seconds,
                max_attempts=args.max_attempts,
                retry_delay_seconds=args.retry_delay_seconds,
            )
    except (
        KeyError,
        OSError,
        ValueError,
        PorosError,
        PorosOutboxError,
        PorosReadinessError,
    ) as exc:
        _emit_readiness_error(exc, as_json=as_json)
        return 1
    payload = {
        "status": "ok",
        "manifest_path": str(manifest_path),
        "sender_authority_id": manifest.authority_id,
        "recipient_authority_id": peer.authority_id,
        "peer": {
            "authority_id": peer.authority_id,
            "endpoint": receiver_inbox_url,
            "signing_key_id": peer_key.kid,
            "signing_key_fingerprint": peer_key.fingerprint,
        },
        "export": export.to_dict(),
        "delivery": [result.to_dict() for result in delivery_results],
    }
    emit_result(payload, as_json=as_json)
    return (
        0
        if all(result.outbox_status != "terminal_failed" for result in delivery_results)
        else 1
    )


def _cmd_receive(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.readiness import (
        PorosReadinessError,
        load_manifest,
        readiness_report,
        require_receiver_manifest,
        require_single_active_signing_key,
        trust_pins_payload,
    )

    try:
        manifest_path = Path(args.manifest).expanduser().resolve()
        manifest = load_manifest(manifest_path)
        require_receiver_manifest(manifest)
        local_key = require_single_active_signing_key(manifest)
        report = readiness_report(
            manifest,
            level=args.level,
            require_transfer_proof=False,
        )
        if not report["ready"]:
            emit_result(report, as_json=as_json)
            return 1
        trust_pins_path = (
            Path(args.trust_pins_out).expanduser().resolve()
            if args.trust_pins_out
            else manifest_path.with_name("poros.trust-pins.json")
        )
        _write_json(trust_pins_path, trust_pins_payload(manifest))
        serve_command = [
            "okg",
            "poros",
            "serve",
            "--dsn",
            args.dsn,
            "--trust-pins",
            str(trust_pins_path),
            "--receiver-keys",
            str(local_key.private_key_path),
            "--receiver",
            manifest.authority_id,
            "--deployment",
            args.deployment,
            "--host",
            args.host,
            "--port",
            str(args.port),
            "--object-store-root",
            str(manifest.object_store["root"]),
        ]
        bundle_path = manifest.nomos.get("bundle_path")
        if bundle_path:
            serve_command.extend(["--nomos-bundle", str(bundle_path)])
        if args.apply:
            serve_command.append("--apply")
        if args.publish:
            serve_command.append("--publish")
    except (OSError, ValueError, PorosReadinessError) as exc:
        _emit_readiness_error(exc, as_json=as_json)
        return 1
    payload = {
        "status": "ok",
        "ready": True,
        "level": args.level,
        "manifest_path": str(manifest_path),
        "authority_id": manifest.authority_id,
        "receiver_endpoint": manifest.receiver_endpoint,
        "trust_pins_out": str(trust_pins_path),
        "receiver_keys": str(local_key.private_key_path),
        "object_store": dict(manifest.object_store),
        "nomos": dict(manifest.nomos),
        "serve_command": serve_command,
        "readiness": _readiness_summary(report),
    }
    emit_result(payload, as_json=as_json)
    return 0


def _readiness_summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "level": report["level"],
        "ready": report["ready"],
        "failed_checks": [check for check in report["checks"] if not check["ok"]],
    }


def _emit_readiness_error(exc: Exception, *, as_json: bool) -> None:
    if hasattr(exc, "to_dict"):
        payload = exc.to_dict()  # type: ignore[no-any-return, attr-defined]
    else:
        payload = {
            "status": "error",
            "error": str(exc),
            "reason_code": exc.__class__.__name__,
        }
    emit_result(payload, as_json=as_json)


def _cmd_deliver(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.sender_delivery import deliver_next_outbox_messages

    try:
        receiver_public_key = _read_public_key_arg(args.receiver_public_key)
        results = deliver_next_outbox_messages(
            dsn=args.dsn,
            receiver_inbox_url=args.receiver_inbox_url,
            receiver_public_key=receiver_public_key,
            message_id=args.message_id,
            limit=args.limit,
            timeout_seconds=args.timeout_seconds,
            stale_after_seconds=args.stale_after_seconds,
            max_attempts=args.max_attempts,
            retry_delay_seconds=args.retry_delay_seconds,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured error
        emit_result({"status": "error", "error": str(exc)}, as_json=as_json)
        return 1
    payload = {
        "status": "ok",
        "delivered_count": len(results),
        "results": [result.to_dict() for result in results],
    }
    emit_result(payload, as_json=as_json)
    return 0 if all(result.status != "failed" for result in results) else 1


def _cmd_status(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        if args.side == "sender":
            payload = _sender_status(
                args.dsn,
                message_id=args.message_id,
                graph_id=args.graph_id,
                limit=args.limit,
            )
        else:
            payload = _receiver_status(
                args.dsn,
                message_id=args.message_id,
                graph_id=args.graph_id,
                limit=args.limit,
            )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured error
        emit_result({"status": "error", "error": str(exc)}, as_json=as_json)
        return 1
    emit_result(payload, as_json=as_json)
    return 0


def _cmd_promote(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.receiver import promote_poros_message

    try:
        promotion_envelope = (
            _read_json(Path(args.envelope), "promotion envelope")
            if args.envelope
            else None
        )
        outcome = promote_poros_message(
            dsn=args.dsn,
            deployment=args.deployment,
            actor=args.actor,
            nomos_bundle=args.nomos_bundle,
            message_id=args.message_id,
            graph_id=args.graph_id,
            body_sha256=args.body_sha256,
            promotion_envelope=promotion_envelope,
            publish=args.publish,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured error
        emit_result({"status": "error", "error": str(exc)}, as_json=as_json)
        return 1
    emit_result(outcome.payload, as_json=as_json)
    return outcome.exit_code


def _cmd_serve(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        import uvicorn
        from fastapi import FastAPI

        from okg.substrate.exchange.http import build_poros_router

        receiver_keys = _read_json(Path(args.receiver_keys), "receiver keys")
        trust_pins = _read_json(Path(args.trust_pins), "trust pins")
        public_key = str(receiver_keys["public_key"])
        authority = build_authority_document(
            authority_id=args.receiver,
            base_url=(
                args.base_url
                or f"http://{args.host}:{args.port}"
            ),
            signing_keys=[
                KeyDescriptor(
                    kid=str(receiver_keys["kid"]),
                    use="sign",
                    alg="Ed25519",
                    public_key=public_key,
                )
            ],
            latest_public_generation={},
        )
        app = FastAPI(title=f"{args.receiver} Poros ingress")
        app.include_router(
            build_poros_router(
                authority_document=authority,
                trust_pins=trust_pins,
                receiver_keys=receiver_keys,
                receiver=args.receiver,
                dsn=args.dsn,
                sender_dsn=args.sender_dsn,
                deployment=args.deployment,
                persist_preflight=not args.no_persist_preflight,
                apply=args.apply,
                publish=args.publish,
                nomos_bundle=args.nomos_bundle,
                object_store_root=args.object_store_root,
            )
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured error
        emit_result({"status": "error", "error": str(exc)}, as_json=as_json)
        return 1
    if as_json:
        print(
            json.dumps(
                {
                    "status": "serving",
                    "url": f"http://{args.host}:{args.port}",
                    "inbox": f"http://{args.host}:{args.port}/poros/v1/inbox",
                },
                sort_keys=True,
            )
        )
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


def _cmd_outbox_export(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.outbox import (
        PorosOutboxError,
        authorize_poros_signing_key_use,
        export_published_subgraph_to_outbox,
    )

    try:
        signing_keys_path = Path(args.signing_keys).expanduser().resolve()
        authorize_poros_signing_key_use(
            dsn=args.dsn,
            sender_authority_id=args.sender,
            sender_generation_id=args.sender_generation_id,
            signing_key_reference=str(signing_keys_path),
            nomos_subject=args.sender,
            use="poros.read_signing_key_file",
        )
        signing_keys = _read_json(signing_keys_path, "signing keys")
        signing_key_id = str(signing_keys["kid"])
        result = export_published_subgraph_to_outbox(
            dsn=args.dsn,
            sender_authority_id=args.sender,
            recipient_authority_id=args.recipient,
            sender_generation_id=args.sender_generation_id,
            graph_id=args.graph_id,
            root_node_ids=args.root_node_id,
            signing_private_key=str(signing_keys["private_key"]),
            signing_key_id=signing_key_id,
            classification=args.classification,
            graph_profile=args.graph_profile,
            message_id=args.message_id,
            ttl_seconds=args.ttl_seconds,
            object_store_root=args.object_store_root,
            detach_payload=args.detach,
            detached_chunk_size_bytes=args.detached_chunk_size_bytes,
            nomos_subject=args.sender,
            signing_key_reference=str(signing_keys_path),
        )
        envelope_out = None
        if args.envelope_out:
            envelope_path = Path(args.envelope_out).expanduser().resolve()
            envelope_path.parent.mkdir(parents=True, exist_ok=True)
            envelope_path.write_bytes(canonical_json_bytes(result.envelope))
            envelope_out = str(envelope_path)
    except (
        KeyError,
        OSError,
        ValueError,
        PorosError,
        PorosOutboxError,
    ) as exc:
        emit_result(
            {"status": "error", "error": str(exc)},
            as_json=as_json,
        )
        return 1
    payload = result.to_dict()
    if envelope_out is not None:
        payload["envelope_out"] = envelope_out
    emit_result(payload, as_json=as_json)
    return 0


def _cmd_harness_init(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.harness import (
        PorosHarnessError,
        initialize_two_okg_harness,
    )

    try:
        result = initialize_two_okg_harness(
            sender_dsn=args.sender_dsn,
            receiver_dsn=args.receiver_dsn,
            sender_deployment=args.sender_deployment,
            receiver_deployment=args.receiver_deployment,
            migrate_schema=not args.skip_migrate,
        )
    except (PorosHarnessError, OSError, ValueError) as exc:
        emit_result(
            {"status": "error", "error": str(exc)},
            as_json=as_json,
        )
        return 1
    emit_result(result.to_dict(), as_json=as_json)
    return 0


def _cmd_hardening_harness(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.exchange.hardening_harness import (
        run_poros_hardening_harness,
    )

    try:
        payload = run_poros_hardening_harness(
            sender_dsn=args.sender_dsn,
            receiver_dsn=args.receiver_dsn,
            out_dir=args.out,
            sender_deployment=args.sender_deployment,
            receiver_deployment=args.receiver_deployment,
            migrate_schema=not args.skip_migrate,
            overwrite=args.overwrite,
            allow_dirty=args.allow_dirty,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns structured error
        emit_result(
            {"status": "error", "error": str(exc)},
            as_json=as_json,
        )
        return 1
    emit_result(payload, as_json=as_json)
    return 0


def _cmd_proof(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(args.dsn, autocommit=True) as conn:
            proof = _load_poros_proof(
                conn,
                message_id=args.message_id,
                graph_id=args.graph_id,
                body_sha256=args.body_sha256,
                mode=args.mode,
            )
            if args.sender_dsn:
                proof["sender_delivery"] = _load_sender_delivery_proof(
                    args.sender_dsn,
                    message_id=str(proof["message_id"]),
                )
    except (LookupError, ValueError) as exc:
        emit_result({"status": "error", "error": str(exc)}, as_json=as_json)
        return 1
    emit_result(proof, as_json=as_json)
    return 0


def _load_poros_proof(
    conn,
    *,
    message_id: str | None,
    graph_id: str | None,
    body_sha256: str | None,
    mode: str,
) -> dict[str, Any]:
    if mode not in {"public", "audit"}:
        raise ValueError(f"unsupported proof mode: {mode}")
    inbox = _load_proof_inbox_row(
        conn,
        message_id=message_id,
        graph_id=graph_id,
        body_sha256=body_sha256,
    )
    events = _load_proof_events(conn, message_id=str(inbox["message_id"]))
    _authorize_poros_proof_with_active_policy(
        conn,
        inbox=inbox,
        events=events,
        mode=mode,
    )
    nodes = _load_proof_nodes(conn, message_id=str(inbox["message_id"]))
    edges = _load_proof_edges(conn, message_id=str(inbox["message_id"]))
    envelope = inbox["envelope"] if isinstance(inbox["envelope"], dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    protected = _proof_body_is_protected(body)
    allowed_for_audit = _proof_has_allowed_audit_decision(events)
    redact_protected = protected and (mode == "public" or not allowed_for_audit)
    proof: dict[str, Any] = {
        "status": "ok",
        "mode": mode,
        "state": _proof_state(inbox, events),
        "message_id": inbox["message_id"],
        "body_sha256": inbox["body_sha256"],
        "sender_authority_id": inbox["sender_authority_id"],
        "receiver_authority_id": inbox["receiver_authority_id"],
        "signing_key_id": inbox["signing_key_id"],
        "preflight": {
            "status": inbox["status"],
            "reason_code": inbox["reason_code"],
            "receipt_sha256": inbox["receipt_sha256"],
        },
        "graph": _proof_graph_summary(
            body,
            redact_objects=redact_protected,
        ),
        "detached_objects": _load_proof_detached_objects(
            conn,
            message_id=str(inbox["message_id"]),
            redact=redact_protected,
        ),
        "lifecycle": _proof_lifecycle_summary(events),
        "events": events,
        "imported_nodes": nodes,
        "imported_edges": edges,
        "redaction": {
            "applied": mode == "public" and protected,
            "protected": protected,
            "audit_allowed": allowed_for_audit,
        },
    }
    if mode == "audit" and protected and not allowed_for_audit:
        proof["redaction"]["audit_denied_reason"] = (
            "missing_allowed_nomos_decision"
        )
    elif mode == "audit":
        proof["body"] = body
    return proof


def _authorize_poros_proof_with_active_policy(
    conn,
    *,
    inbox: dict[str, Any],
    events: list[dict[str, Any]],
    mode: str,
) -> None:
    deployment_name = _proof_deployment_from_events(events)
    if not deployment_name:
        deployment_name = _proof_deployment_from_inbox(inbox)
    if not deployment_name:
        return
    from okg.substrate.nomos import (
        NomosRuntimeRequest,
        deployment_requires_nomos_runtime,
        evaluate_runtime_boundary_with_active_policy,
    )

    if not deployment_requires_nomos_runtime(deployment_name):
        return
    envelope = inbox.get("envelope") if isinstance(inbox.get("envelope"), dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    if not isinstance(body, dict):
        body = {}
    performed = ["audit"]
    if mode == "public":
        performed.append("redaction")
    outcome = evaluate_runtime_boundary_with_active_policy(
        conn,
        deployment_name=deployment_name,
        scope=f"deployment:{deployment_name}",
        request=NomosRuntimeRequest(
            surface="graph.read.export_like",
            subject=str(inbox["receiver_authority_id"]),
            classification=str(body.get("classification") or "internal"),
            resource={
                "use": "poros.proof",
                "message_id": inbox["message_id"],
                "body_sha256": inbox["body_sha256"],
                "proof_mode": mode,
                "sender_authority_id": inbox["sender_authority_id"],
                "receiver_authority_id": inbox["receiver_authority_id"],
                "graph_id": body.get("graph_id"),
            },
        ),
        target={
            "kind": "poros_proof",
            "message_id": inbox["message_id"],
            "proof_mode": mode,
        },
        performed_obligations=tuple(performed),
    )
    conn.commit()
    if outcome is not None and not outcome.allowed:
        raise ValueError(
            "Nomos denied graph.read.export_like: "
            f"{outcome.decision.reason_code or outcome.final_effect}"
        )


def _proof_deployment_from_events(events: list[dict[str, Any]]) -> str | None:
    for event in events:
        generation_id = event.get("receiver_generation_id")
        deployment = _deployment_from_generation_id(str(generation_id or ""))
        if deployment:
            return deployment
    return None


def _proof_deployment_from_inbox(inbox: dict[str, Any]) -> str | None:
    envelope = inbox.get("envelope") if isinstance(inbox.get("envelope"), dict) else {}
    body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
    if not isinstance(body, dict):
        return None
    for key in ("receiver_generation_id", "origin_generation_id"):
        deployment = _deployment_from_generation_id(str(body.get(key) or ""))
        if deployment:
            return deployment
    return None


def _deployment_from_generation_id(generation_id: str) -> str | None:
    parts = str(generation_id).split(":", 3)
    if len(parts) >= 2 and parts[0] == "okg" and parts[1]:
        return parts[1]
    return None


def _load_proof_detached_objects(
    conn,
    *,
    message_id: str,
    redact: bool,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT side, authority_id, object_id, media_type, size_bytes,
                   object_sha256, chunk_count, classification, encrypted,
                   manifest
              FROM okg.poros_detached_object_manifests
             WHERE message_id = %s
             ORDER BY side, authority_id, object_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        _proof_detached_object_summary(
            {
                "side": row[0],
                "authority_id": row[1],
                "object_id": row[2],
                "media_type": row[3],
                "size_bytes": row[4],
                "object_sha256": row[5],
                "chunk_count": row[6],
                "classification": row[7],
                "encrypted": row[8],
                "manifest": row[9] if isinstance(row[9], dict) else {},
            },
            redact=redact,
        )
        for row in rows
    ]


def _proof_detached_object_summary(
    row: dict[str, Any],
    *,
    redact: bool,
) -> dict[str, Any]:
    manifest = row["manifest"] if isinstance(row.get("manifest"), dict) else {}
    if redact:
        return {
            "side": row.get("side"),
            "authority_id": row.get("authority_id"),
            "classification": row.get("classification"),
            "encrypted": bool(row.get("encrypted")),
            "metadata_redacted": True,
            "locations_redacted": bool(manifest.get("locations")),
            "bytes_redacted": bool(manifest.get("inline_bytes_b64url")),
            "manifest_redacted": True,
        }
    return {
        "side": row.get("side"),
        "authority_id": row.get("authority_id"),
        "object_id": row.get("object_id"),
        "media_type": row.get("media_type"),
        "size_bytes": int(row.get("size_bytes") or 0),
        "object_sha256": row.get("object_sha256"),
        "chunk_count": int(row.get("chunk_count") or 0),
        "classification": row.get("classification"),
        "encrypted": bool(row.get("encrypted")),
        "locations_redacted": bool(manifest.get("locations")),
        "bytes_redacted": bool(manifest.get("inline_bytes_b64url")),
    }


def _load_proof_inbox_row(
    conn,
    *,
    message_id: str | None,
    graph_id: str | None,
    body_sha256: str | None,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[str] = []
    if message_id is not None:
        clauses.append("message_id = %s")
        params.append(message_id)
    if graph_id is not None:
        clauses.append("envelope->'body'->>'graph_id' = %s")
        params.append(graph_id)
    if body_sha256 is not None:
        clauses.append("body_sha256 = %s")
        params.append(body_sha256)
    if not clauses:
        raise ValueError("one proof lookup key is required")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT receiver_authority_id, sender_authority_id, message_id,
                   status, reason_code, body_sha256, signing_key_id,
                   receipt_sha256, envelope
              FROM okg.poros_inbox_messages
             WHERE {' AND '.join(clauses)}
             ORDER BY received_at DESC
             LIMIT 1
            """,
            params,
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError("no Poros inbox evidence matched the lookup")
    return {
        "receiver_authority_id": row[0],
        "sender_authority_id": row[1],
        "message_id": row[2],
        "status": row[3],
        "reason_code": row[4],
        "body_sha256": row[5],
        "signing_key_id": row[6],
        "receipt_sha256": row[7],
        "envelope": row[8],
    }


def _load_proof_events(conn, *, message_id: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT event_type, status, reason_code, nomos_decision_id,
                   policy_bundle_id, policy_fingerprint, nomos_input_hash,
                   admission_id::text, receiver_generation_id, payload,
                   created_at
              FROM okg.poros_incorporation_events
             WHERE message_id = %s
             ORDER BY event_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "event_type": row[0],
            "status": row[1],
            "reason_code": row[2],
            "nomos_decision_id": row[3],
            "policy_bundle_id": row[4],
            "policy_fingerprint": row[5],
            "nomos_input_hash": row[6],
            "admission_id": row[7],
            "receiver_generation_id": row[8],
            "payload": _proof_event_payload(row[9]),
            "created_at": row[10].isoformat() if row[10] else None,
        }
        for row in rows
    ]


def _load_proof_nodes(conn, *, message_id: str) -> list[dict[str, Any]]:
    # Chronos cutover: the proof bundle lists the node facts a Poros
    # message imported. The legacy per-generation visibility window
    # (`nodes_live.valid_from_gen` / `valid_to_gen`) has no Chronos
    # equivalent and is dropped.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT nf.node_id, nf.fact_id, nf.source_record_id->'poros'
              FROM okg.node_facts nf
             WHERE nf.source_record_id->'poros'->>'message_id' = %s
             ORDER BY nf.fact_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "node_id": row[0],
            "fact_id": row[1],
            "poros": _proof_sensitive_poros(row[2]),
        }
        for row in rows
    ]


def _load_proof_edges(conn, *, message_id: str) -> list[dict[str, Any]]:
    # Chronos cutover: see `_load_proof_nodes` — the bitemporal
    # window columns are dropped.
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ef.src, ef.edge_type, ef.dst, ef.fact_id,
                   ef.source_record_id->'poros'
              FROM okg.edge_facts ef
             WHERE ef.source_record_id->'poros'->>'message_id' = %s
             ORDER BY ef.fact_id
            """,
            (message_id,),
        )
        rows = cur.fetchall()
    return [
        {
            "src": row[0],
            "edge_type": row[1],
            "dst": row[2],
            "fact_id": row[3],
            "poros": _proof_sensitive_poros(row[4]),
        }
        for row in rows
    ]


def _load_sender_delivery_proof(
    sender_dsn: str,
    *,
    message_id: str,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(sender_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT sender_authority_id, recipient_authority_id,
                   sender_generation_id, graph_id, signing_key_id,
                   body_sha256, envelope_sha256, status,
                   max_delivery_attempts, next_retry_at, last_error,
                   terminal_failure_reason, created_at, updated_at
              FROM okg.poros_outbox_messages
             WHERE message_id = %s
            """,
            (message_id,),
        )
        outbox = cur.fetchone()
        cur.execute(
            """
            SELECT attempt_id, transport, status, reason_code,
                   receipt_sha256, attempted_at, completed_at
              FROM okg.poros_delivery_attempts
             WHERE message_id = %s
             ORDER BY attempt_id
            """,
            (message_id,),
        )
        attempts = cur.fetchall()
    return {
        "outbox": (
            {
                "message_id": message_id,
                "sender_authority_id": outbox[0],
                "recipient_authority_id": outbox[1],
                "sender_generation_id": outbox[2],
                "graph_id": outbox[3],
                "signing_key_id": outbox[4],
                "body_sha256": outbox[5],
                "envelope_sha256": outbox[6],
                "state": outbox[7],
                "max_delivery_attempts": outbox[8],
                "next_retry_at": outbox[9].isoformat() if outbox[9] else None,
                "last_error": outbox[10],
                "terminal_failure_reason": outbox[11],
                "receiver_publication_claim": (
                    "not_proven_by_sender_delivery"
                    if outbox[7] != "delivered"
                    else "not_proven_without_receiver_proof"
                ),
                "created_at": outbox[12].isoformat() if outbox[12] else None,
                "updated_at": outbox[13].isoformat() if outbox[13] else None,
            }
            if outbox is not None
            else None
        ),
        "delivery_attempts": [
            {
                "attempt_id": row[0],
                "transport": row[1],
                "status": row[2],
                "reason_code": row[3],
                "receipt_sha256": row[4],
                "attempted_at": row[5].isoformat() if row[5] else None,
                "completed_at": row[6].isoformat() if row[6] else None,
            }
            for row in attempts
        ],
    }


def _proof_event_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    redacted = dict(payload)
    if "receipt" in redacted:
        redacted["receipt"] = "<redacted>"
    return redacted


def _proof_lifecycle_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    def latest(event_type: str) -> dict[str, Any] | None:
        return next(
            (
                _proof_lifecycle_event_summary(event)
                for event in reversed(events)
                if event["event_type"] == event_type
            ),
            None,
        )

    summary: dict[str, Any] = {
        "preflight_accepted": latest("preflight_accepted"),
        "preflight_rejected": latest("preflight_rejected"),
        "duplicate": latest("duplicate"),
        "nomos_decision": latest("nomos_decided"),
        "review": latest("review"),
        "quarantine": latest("quarantined"),
        "admission": latest("admitted"),
        "publication": latest("published"),
        "promotion_decision": latest("promotion_decided"),
        "promotion_result": latest("promoted") or latest("promotion_denied"),
    }
    return {key: value for key, value in summary.items() if value is not None}


def _proof_lifecycle_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_type": event["event_type"],
        "status": event["status"],
        "reason_code": event["reason_code"],
        "nomos_decision_id": event["nomos_decision_id"],
        "policy_bundle_id": event["policy_bundle_id"],
        "policy_fingerprint": event["policy_fingerprint"],
        "nomos_input_hash": event["nomos_input_hash"],
        "admission_id": event["admission_id"],
        "receiver_generation_id": event["receiver_generation_id"],
        "created_at": event["created_at"],
    }


def _proof_graph_summary(
    body: Any,
    *,
    redact_objects: bool = False,
) -> dict[str, Any]:
    body = body if isinstance(body, dict) else {}
    nodes = body.get("nodes") or body.get("node_facts") or []
    edges = body.get("edges") or body.get("edge_facts") or []
    objects = body.get("objects") if isinstance(body.get("objects"), list) else []
    return {
        "graph_id": body.get("graph_id"),
        "classification": body.get("classification"),
        "graph_profile": body.get("graph_profile"),
        "origin_generation_id": body.get("origin_generation_id"),
        "root_node_ids": body.get("root_node_ids") or [],
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
        "edge_count": len(edges) if isinstance(edges, list) else 0,
        "objects": [
            _proof_object_summary(obj, redact=redact_objects)
            for obj in objects
        ],
    }


def _proof_object_summary(obj: Any, *, redact: bool = False) -> dict[str, Any]:
    if not isinstance(obj, dict):
        return {"invalid": True}
    object_hash = (
        obj.get("cleartext_sha256")
        or obj.get("ciphertext_sha256")
        or obj.get("sha256")
    )
    if redact:
        return {
            "redacted": True,
            "metadata_redacted": True,
            "classification": obj.get("classification"),
            "encrypted": bool("encryption" in obj or "ciphertext_sha256" in obj),
            "locations_redacted": "locations" in obj,
            "bytes_redacted": "inline_bytes_b64url" in obj,
        }
    return {
        "object_id": obj.get("object_id"),
        "media_type": obj.get("media_type"),
        "size_bytes": obj.get("size_bytes") or obj.get("size"),
        "object_sha256": object_hash,
        "chunk_count": len(obj.get("chunks") or []),
        "classification": obj.get("classification"),
        "encrypted": bool("encryption" in obj or "ciphertext_sha256" in obj),
        "locations_redacted": "locations" in obj,
        "bytes_redacted": "inline_bytes_b64url" in obj,
    }


def _proof_sensitive_poros(value: Any) -> dict[str, Any]:
    poros = dict(value or {}) if isinstance(value, dict) else {}
    for key in ["private_context", "embedding", "object_locator"]:
        poros.pop(key, None)
    return poros


def _proof_body_is_protected(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    classification = str(body.get("classification") or "public")
    if classification not in {"public", "unspecified"}:
        return True
    objects = body.get("objects")
    if isinstance(objects, list):
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            if obj.get("classification") not in {None, "public"}:
                return True
            if "encryption" in obj or "ciphertext_sha256" in obj:
                return True
            if "inline_bytes_b64url" in obj or "locations" in obj:
                return True
    return False


def _proof_has_allowed_audit_decision(events: list[dict[str, Any]]) -> bool:
    return any(
        event["event_type"] == "nomos_decided"
        and event["status"] in {"allow", "redact", "declassify"}
        for event in events
    )


def _proof_state(
    inbox: dict[str, Any],
    events: list[dict[str, Any]],
) -> str:
    if inbox["status"] == "rejected_preflight":
        return "preflight-rejected"
    event_types = [event["event_type"] for event in events]
    if "promoted" in event_types:
        return "promoted"
    if "rejected" in event_types:
        return "rejected"
    if "rejection_denied" in event_types:
        return "rejection-denied"
    if "promotion_denied" in event_types:
        return "promotion-denied"
    if "published" in event_types:
        return "published"
    if "admitted" in event_types:
        return "admitted-unpublished"
    latest_nomos = next(
        (
            event for event in reversed(events)
            if event["event_type"] == "nomos_decided"
        ),
        None,
    )
    if latest_nomos is not None:
        if latest_nomos["status"] == "quarantine":
            return "quarantined"
        if latest_nomos["status"] == "review":
            return "review"
        if latest_nomos["status"] == "deny":
            return "blocked"
    if "review" in event_types:
        return "review"
    if "quarantined" in event_types:
        return "quarantined"
    if inbox["status"] == "duplicate" or "duplicate" in event_types:
        return "duplicate"
    return "preflight-accepted"


def _cmd_submit(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        envelope_path = Path(args.envelope).expanduser().resolve()
        trust_pins = _read_json(Path(args.trust_pins), "trust pins")
        receiver_keys = _read_json(Path(args.receiver_keys), "receiver keys")
        raw = envelope_path.read_bytes()
    except (OSError, KeyError, ValueError, PorosError) as exc:
        payload = {"status": "error", "error": str(exc)}
        emit_result(payload, as_json=as_json)
        return 1
    nomos_identity_claim = None
    if args.nomos_identity_claim:
        try:
            claim = _read_json(Path(args.nomos_identity_claim), "Nomos identity claim")
            if not isinstance(claim, dict):
                raise ValueError("Nomos identity claim file must contain an object")
            nomos_identity_claim = claim
        except (OSError, ValueError) as exc:
            payload = {"status": "error", "error": str(exc)}
            emit_result(payload, as_json=as_json)
            return 1
    outcome = submit_poros_envelope_bytes(
        raw,
        trust_pins=trust_pins,
        receiver_keys=receiver_keys,
        receiver=args.receiver,
        dsn=args.dsn,
        sender_dsn=args.sender_dsn,
        deployment=args.deployment,
        admission_mode=args.admission_mode,
        persist_preflight=args.persist_preflight,
        apply=args.apply,
        publish=args.publish,
        nomos_bundle=Path(args.nomos_bundle) if args.nomos_bundle else None,
        nomos_identity_claim=nomos_identity_claim,
    )
    if args.nomos_decision_out and outcome.payload.get("nomos_decision"):
        from okg.substrate.nomos.core import write_json

        write_json(Path(args.nomos_decision_out), outcome.payload["nomos_decision"])
    emit_result(outcome.payload, as_json=as_json)
    return outcome.exit_code


def _cmd_demo(args: argparse.Namespace, *, as_json: bool) -> int:
    out_dir = Path(args.out).expanduser().resolve()
    if out_dir.exists() and any(out_dir.iterdir()) and not args.overwrite:
        message = (
            f"{out_dir} already contains files; pass --overwrite or choose "
            "a different --out directory"
        )
        if as_json:
            print(json.dumps({"status": "error", "error": message}))
        else:
            print(f"error: {message}", file=sys.stderr)
        return 1
    out_dir.mkdir(parents=True, exist_ok=True)

    sender = generate_ed25519_keypair(args.sender_kid)
    receiver = generate_ed25519_keypair(args.receiver_kid)

    authority = build_authority_document(
        authority_id=args.receiver,
        base_url=f"https://example.invalid/{_authority_slug(args.receiver)}",
        display_name=f"{args.receiver} Poros demo receiver",
        signing_keys=[
            KeyDescriptor(
                kid=receiver.kid,
                use="sign",
                alg="Ed25519",
                public_key=receiver.public_key,
            )
        ],
        latest_public_generation={"generation_id": 0, "demo": True},
    )
    trust_pins = {
        "pins": [
            {
                "authority_id": args.sender,
                "kid": sender.kid,
                "use": "sign",
                "alg": "Ed25519",
                "public_key": sender.public_key,
            }
        ]
    }
    registry = TrustPinRegistry.from_mapping(trust_pins)
    inbox = PorosInbox(
        receiver_authority_id=args.receiver,
        trust_registry=registry,
        receiver_private_key=receiver.private_key,
        receiver_signing_kid=receiver.kid,
    )

    message_id = args.message_id or new_message_id()
    body = _demo_body(
        sender=args.sender,
        graph_id=args.graph_id or args.bundle_id or "poros:graph:poros-demo",
        classification=args.classification,
        message=args.message,
        message_id=message_id,
    )
    unsigned = build_unsigned_envelope(
        sender=args.sender,
        recipient=args.receiver,
        message_id=message_id,
        created_at=datetime.now(UTC),
        ttl_seconds=15 * 60,
        body=body,
    )
    envelope = sign_envelope(unsigned, sender.private_key, kid=sender.kid)
    envelope_wire = canonical_json_bytes(envelope)
    result = inbox.handle_bytes(envelope_wire)
    receipt_verified = False
    if result.receipt is not None:
        verify_receipt(result.receipt, receiver.public_key)
        receipt_verified = True

    artifacts = {
        "sender_private_key": out_dir / "sender.keys.json",
        "receiver_private_key": out_dir / "receiver.keys.json",
        "receiver_authority": out_dir / "receiver.authority.json",
        "trust_pins": out_dir / "trust-pins.json",
        "envelope_wire": out_dir / "envelope.wire.json",
        "envelope_pretty": out_dir / "envelope.pretty.json",
        "receipt_wire": out_dir / "receipt.wire.json",
        "receipt_pretty": out_dir / "receipt.pretty.json",
        "admission_candidate": out_dir / "admission-candidate.json",
        "readme": out_dir / "README.md",
    }
    _write_json(artifacts["sender_private_key"], sender.__dict__, secret=True)
    _write_json(artifacts["receiver_private_key"], receiver.__dict__, secret=True)
    _write_json(artifacts["receiver_authority"], authority.to_dict())
    _write_json(artifacts["trust_pins"], trust_pins)
    artifacts["envelope_wire"].write_bytes(envelope_wire)
    _write_json(artifacts["envelope_pretty"], envelope)
    if result.receipt is not None:
        artifacts["receipt_wire"].write_bytes(canonical_json_bytes(result.receipt))
        _write_json(artifacts["receipt_pretty"], result.receipt)
    if result.admission_candidate is not None:
        _write_json(
            artifacts["admission_candidate"],
            {
                "message_id": result.admission_candidate.message_id,
                "sender": result.admission_candidate.sender,
                "recipient": result.admission_candidate.recipient,
                "body_sha256": result.admission_candidate.body_sha256,
                "received_at": result.admission_candidate.received_at,
                "body": result.admission_candidate.body,
            },
        )
    else:
        _write_json(artifacts["admission_candidate"], None)
    artifacts["readme"].write_text(
        _artifact_readme(args, result.status),
        encoding="utf-8",
    )

    payload = {
        "status": result.status,
        "reason_code": result.reason_code,
        "receipt_verified": receipt_verified,
        "out_dir": str(out_dir),
        "sender": args.sender,
        "receiver": args.receiver,
        "message_id": envelope["message_id"],
        "body_sha256": envelope["body_sha256"],
        "admission_candidate_created": result.admission_candidate is not None,
        "artifacts": {name: str(path) for name, path in artifacts.items()},
    }
    if as_json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(f"poros demo: {result.status}")
        if result.reason_code:
            print(f"reason_code: {result.reason_code}")
        print(f"out_dir: {out_dir}")
        print(f"sender: {args.sender}")
        print(f"receiver: {args.receiver}")
        print(f"message_id: {envelope['message_id']}")
        print(f"receipt_verified: {receipt_verified}")
        print("artifacts:")
        for name, path in artifacts.items():
            print(f"  {name}: {path}")
    return 0 if result.status == "accepted_for_admission" else 1


def _demo_body(
    *,
    sender: str,
    graph_id: str,
    classification: str,
    message: str,
    message_id: str,
) -> dict[str, Any]:
    node_id = f"poros-demo:context:{message_id}"
    return {
        "graph_id": graph_id,
        "origin_authority_id": sender,
        "origin_generation_id": 0,
        "classification": classification,
        "graph_profile": "context",
        "root_node_ids": [node_id],
        "nodes": [
            {
                "node_id": node_id,
                "subtype": "context_packet",
                "attrs": {
                    "title": "Poros demo context packet",
                    "text": message,
                    "created_for": "poros-demo",
                },
            }
        ],
        "edges": [],
        "metadata": {
            "demo": True,
            "created_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        },
    }


def _submit_payload(
    result,
    *,
    receipt_verified: bool,
    admission: dict[str, Any] | None,
    publish: dict[str, Any] | None,
    live_counts: dict[str, int] | None,
    nomos_decision: dict[str, Any] | None = None,
    preflight: dict[str, Any] | None = None,
    delivery: dict[str, Any] | None = None,
    mutation_error: str | None = None,
) -> dict[str, Any]:
    candidate = result.admission_candidate
    payload: dict[str, Any] = {
        "status": result.status,
        "reason_code": result.reason_code,
        "receipt_verified": receipt_verified,
        "message_id": (
            candidate.message_id if candidate is not None else None
        ),
        "sender": candidate.sender if candidate is not None else None,
        "recipient": candidate.recipient if candidate is not None else None,
        "body_sha256": (
            candidate.body_sha256 if candidate is not None else None
        ),
        "admission": admission,
        "publish": publish,
        "live_counts": live_counts,
        "nomos_decision": nomos_decision,
        "preflight": preflight,
        "delivery": delivery,
    }
    if mutation_error:
        payload["mutation_error"] = mutation_error
    return payload


def _apply_preflight_status_override(
    payload: dict[str, Any],
    preflight: dict[str, Any] | None,
) -> None:
    if not preflight:
        return
    if preflight.get("status") != "duplicate":
        return
    payload["status"] = "duplicate"
    payload["reason_code"] = "duplicate"
    payload["message_id"] = preflight.get("message_id")
    payload["sender"] = preflight.get("sender_authority_id")
    payload["recipient"] = preflight.get("receiver_authority_id")
    payload["body_sha256"] = preflight.get("body_sha256")


def _record_delivery_attempt_payload(
    sender_dsn: str,
    envelope: dict[str, Any],
    *,
    status: str,
    reason_code: str | None,
    receipt_sha256: str | None,
) -> dict[str, Any]:
    result = record_delivery_attempt(
        dsn=sender_dsn,
        message_id=str(envelope["message_id"]),
        body_sha256=str(envelope["body_sha256"]),
        envelope_sha256=_sha256_canonical(envelope),
        status=status,
        reason_code=reason_code,
        receipt_sha256=receipt_sha256,
        response={
            "status": status,
            "reason_code": reason_code,
            "receipt_sha256": receipt_sha256,
        },
    )
    return result.to_dict()


def _record_nomos_decision_event(
    conn,
    *,
    receiver: str,
    envelope: dict[str, Any],
    nomos_decision: dict[str, Any],
) -> None:
    record_poros_nomos_decision(
        conn,
        receiver_authority_id=receiver,
        sender_authority_id=str(envelope["sender"]),
        message_id=str(envelope["message_id"]),
        body_sha256=(
            str(envelope["body_sha256"])
            if envelope.get("body_sha256") is not None
            else None
        ),
        decision=nomos_decision,
    )


def _record_lifecycle_event(
    conn,
    *,
    receiver: str,
    envelope: dict[str, Any],
    event_type: str,
    status: str,
    reason_code: str | None,
    admission_id: str | None = None,
    receiver_generation_id: int | str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    record_poros_incorporation_lifecycle(
        conn,
        receiver_authority_id=receiver,
        sender_authority_id=str(envelope["sender"]),
        message_id=str(envelope["message_id"]),
        body_sha256=(
            str(envelope["body_sha256"])
            if envelope.get("body_sha256") is not None
            else None
        ),
        event_type=event_type,
        status=status,
        reason_code=reason_code,
        admission_id=admission_id,
        receiver_generation_id=receiver_generation_id,
        payload=payload,
    )


def _record_publish_lifecycle_event(
    dsn: str,
    *,
    receiver: str,
    envelope: dict[str, Any],
    admission: dict[str, Any],
    publish: dict[str, Any],
    live_counts: dict[str, int] | None,
) -> None:
    from okg.substrate.db.connection import connect as db_connect

    status = str(publish.get("status") or "unknown")
    # receiver_generation_id carries the chronos-native published generation
    # (a string checkpoint id) in the TEXT column.
    generation_id = publish.get("generation_id")
    with db_connect(dsn) as conn:
        with conn.transaction():
            _record_lifecycle_event(
                conn,
                receiver=receiver,
                envelope=envelope,
                event_type=("published" if status == "published" else "blocked"),
                status=status,
                reason_code=(
                    "poros_published"
                    if status == "published"
                    else str(publish.get("blocked_reason") or status)
                ),
                admission_id=str(admission["admission_id"]),
                receiver_generation_id=generation_id,
                payload={
                    "admission_id": admission["admission_id"],
                    "publish": publish,
                    "live_counts": live_counts,
                },
            )


def _admission_lifecycle_event_type(
    admission: dict[str, Any],
    *,
    admission_mode: str,
) -> str:
    if admission.get("status") == "accepted":
        return "admitted"
    if admission_mode == "review":
        return "review"
    if admission_mode == "quarantine":
        return "quarantined"
    return "blocked"


def _nomos_poros_provenance(
    nomos_decision: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if nomos_decision is None:
        return None
    return {
        "nomos_decision_id": nomos_decision.get("decision_id"),
        "policy_bundle_id": nomos_decision.get("policy_bundle_id"),
        "policy_fingerprint": nomos_decision.get("policy_fingerprint"),
        "nomos_input_hash": nomos_decision.get("input_hash"),
        "nomos_effect": nomos_decision.get("effect"),
        "nomos_reason_code": nomos_decision.get("reason_code"),
    }


def _sha256_canonical(value: dict[str, Any]) -> str:
    from okg.substrate.exchange.hashing import sha256_b64url

    return sha256_b64url(canonical_json_bytes(value))


def _admission_result_payload(admission) -> dict[str, Any]:
    return {
        "deployment": admission.deployment,
        "admission_id": admission.admission_id,
        "status": admission.status,
        "inserted": admission.inserted,
        "idempotency_key": admission.idempotency_key,
        "source": admission.source,
        "run_id": admission.run_id,
        "payload_ref_inserted": admission.payload_ref_inserted,
        "emitted": admission.emitted,
        "fact_ref_count": admission.fact_ref_count,
        "payload_rejected": admission.payload_rejected,
    }


def _nomos_decision_for_candidate(
    candidate,
    *,
    bundle_path: Path,
    signing_key_id: str,
    catalog_version_id: int | None,
    requested_incorporation_mode: str = "fast_track",
):
    from okg.substrate.nomos import DecisionInput, evaluate, load_bundle
    from okg.substrate.exchange.graph_shape import classify_graph_body

    body = candidate.body
    graph_shape = classify_graph_body(body)
    sender_profile = body.get("graph_profile")
    decision_input = DecisionInput(
        subject=candidate.sender,
        action="poros.graph.submit",
        classification=str(body.get("classification") or "internal"),
        resource={
            "graph_id": body.get("graph_id"),
            **graph_shape.to_resource_fields(),
            "sender_authority_id": candidate.sender,
            "graph_classification": str(body.get("classification") or "internal"),
            "requested_incorporation_mode": requested_incorporation_mode,
            "sender_graph_profile": sender_profile,
            "recipient": candidate.recipient,
            "signing_key_id": signing_key_id,
        },
        context={
            "message_id": candidate.message_id,
            "body_sha256": candidate.body_sha256,
            "catalog_version_id": catalog_version_id,
        },
    )
    return evaluate(
        load_bundle(bundle_path),
        decision_input,
        enforcement_point="poros_incorporation",
    )


def _current_catalog_version_id(dsn: str) -> int | None:
    from okg.substrate.db.connection import connect as db_connect

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT max(catalog_version_id)
              FROM okg.catalog_versions
             WHERE status = 'applied'
            """
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def _read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.expanduser().read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} file is not valid JSON: {path}") from exc


def _read_public_key_arg(value: str) -> str:
    path = Path(value).expanduser()
    if path.is_file():
        payload = _read_json(path, "receiver public key")
        if isinstance(payload, dict) and isinstance(payload.get("public_key"), str):
            return str(payload["public_key"])
        if isinstance(payload, str):
            return payload
        raise ValueError(
            "receiver public key file must be a string or contain public_key"
        )
    return value


def _sender_status(
    dsn: str,
    *,
    message_id: str | None,
    graph_id: str | None,
    limit: int,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    clauses: list[str] = []
    params: list[Any] = []
    if message_id:
        clauses.append("o.message_id = %s")
        params.append(message_id)
    if graph_id:
        clauses.append("o.graph_id = %s")
        params.append(graph_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT o.message_id, o.recipient_authority_id, o.graph_id,
                   o.status, o.body_sha256, o.created_at, o.updated_at,
                   o.max_delivery_attempts, o.next_retry_at, o.last_error,
                   o.terminal_failure_reason,
                   COALESCE(attempt_counts.attempts, 0) AS attempts,
                   latest.completed_at AS last_attempt_at,
                   latest.attempt_id AS latest_attempt_id,
                   latest.status AS latest_attempt_status,
                   latest.reason_code AS latest_attempt_reason_code,
                   latest.receipt_sha256 AS latest_attempt_receipt_sha256,
                   latest.transport AS latest_attempt_transport
              FROM okg.poros_outbox_messages o
              LEFT JOIN LATERAL (
                SELECT count(*) AS attempts
                  FROM okg.poros_delivery_attempts a
                 WHERE a.message_id = o.message_id
              ) attempt_counts ON true
              LEFT JOIN LATERAL (
                SELECT a.attempt_id, a.status, a.reason_code,
                       a.receipt_sha256, a.transport, a.completed_at,
                       a.attempted_at
                  FROM okg.poros_delivery_attempts a
                 WHERE a.message_id = o.message_id
                 ORDER BY a.attempted_at DESC, a.attempt_id DESC
                 LIMIT 1
              ) latest ON true
             {where}
             ORDER BY o.created_at DESC
             LIMIT %s
            """,
            params,
        )
        messages = [
            {
                "message_id": row[0],
                "recipient_authority_id": row[1],
                "graph_id": row[2],
                "state": row[3],
                "body_sha256": row[4],
                "created_at": row[5].isoformat() if row[5] else None,
                "updated_at": row[6].isoformat() if row[6] else None,
                "max_delivery_attempts": int(row[7] or 0),
                "next_retry_at": row[8].isoformat() if row[8] else None,
                "last_error": row[9],
                "terminal_failure_reason": row[10],
                "attempt_count": int(row[11] or 0),
                "last_attempt_at": row[12].isoformat() if row[12] else None,
                "latest_attempt": (
                    {
                        "attempt_id": row[13],
                        "status": row[14],
                        "reason_code": row[15],
                        "receipt_sha256": row[16],
                        "transport": row[17],
                        "completed_at": row[12].isoformat() if row[12] else None,
                    }
                    if row[13] is not None
                    else None
                ),
            }
            for row in cur.fetchall()
        ]
        attempts = []
        message_ids = [str(message["message_id"]) for message in messages]
        if message_ids:
            cur.execute(
                """
                SELECT message_id, attempt_id, status, reason_code,
                       receipt_sha256, transport, attempted_at, completed_at
                  FROM okg.poros_delivery_attempts
                 WHERE message_id = ANY(%s)
                 ORDER BY message_id, attempted_at DESC, attempt_id DESC
                """,
                (message_ids,),
            )
            attempts = [
                {
                    "message_id": row[0],
                    "attempt_id": row[1],
                    "status": row[2],
                    "reason_code": row[3],
                    "receipt_sha256": row[4],
                    "transport": row[5],
                    "attempted_at": row[6].isoformat() if row[6] else None,
                    "completed_at": row[7].isoformat() if row[7] else None,
                }
                for row in cur.fetchall()
            ]
    return {"status": "ok", "side": "sender", "messages": messages, "attempts": attempts}


def _receiver_status(
    dsn: str,
    *,
    message_id: str | None,
    graph_id: str | None,
    limit: int,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    clauses: list[str] = []
    params: list[Any] = []
    if message_id:
        clauses.append("i.message_id = %s")
        params.append(message_id)
    if graph_id:
        clauses.append("i.envelope->'body'->>'graph_id' = %s")
        params.append(graph_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT i.message_id, i.sender_authority_id, i.receiver_authority_id,
                   i.status, i.reason_code, i.body_sha256, i.received_at,
                   latest.event_type AS latest_event,
                   latest.receiver_generation_id AS receiver_generation_id,
                   nomos.status AS nomos_effect,
                   nomos.reason_code AS nomos_reason_code,
                   nomos.nomos_decision_id,
                   nomos.policy_bundle_id,
                   nomos.policy_fingerprint,
                   nomos.nomos_input_hash,
                   promotion.status AS promotion_effect,
                   promotion.reason_code AS promotion_reason_code,
                   promotion.nomos_decision_id AS promotion_decision_id,
                   promotion.policy_bundle_id AS promotion_policy_bundle_id,
                   promotion.policy_fingerprint AS promotion_policy_fingerprint,
                   promotion.nomos_input_hash AS promotion_input_hash,
                   history.event_types,
                   i.envelope
              FROM okg.poros_inbox_messages i
              LEFT JOIN LATERAL (
                SELECT e.event_type, e.receiver_generation_id
                  FROM okg.poros_incorporation_events e
                 WHERE e.message_id = i.message_id
                   AND e.event_type IN (
                     'published', 'admitted', 'review', 'quarantined',
                     'duplicate', 'preflight_rejected', 'promotion_denied',
                     'promoted', 'rejection_denied', 'rejected'
                   )
                 ORDER BY e.event_id DESC
                 LIMIT 1
              ) latest ON true
              LEFT JOIN LATERAL (
                SELECT e.status, e.reason_code, e.nomos_decision_id,
                       e.policy_bundle_id, e.policy_fingerprint,
                       e.nomos_input_hash
                  FROM okg.poros_incorporation_events e
                 WHERE e.message_id = i.message_id
                   AND e.event_type = 'nomos_decided'
                 ORDER BY e.event_id DESC
                 LIMIT 1
              ) nomos ON true
              LEFT JOIN LATERAL (
                SELECT e.status, e.reason_code, e.nomos_decision_id,
                       e.policy_bundle_id, e.policy_fingerprint,
                       e.nomos_input_hash
                  FROM okg.poros_incorporation_events e
                 WHERE e.message_id = i.message_id
                   AND e.event_type = 'promotion_decided'
                 ORDER BY e.event_id DESC
                 LIMIT 1
              ) promotion ON true
              LEFT JOIN LATERAL (
                SELECT array_agg(e.event_type ORDER BY e.event_id) AS event_types
                  FROM okg.poros_incorporation_events e
                 WHERE e.message_id = i.message_id
              ) history ON true
             {where}
             ORDER BY i.received_at DESC
             LIMIT %s
            """,
            params,
        )
        messages = []
        for row in cur.fetchall():
            inbox = {
                "message_id": row[0],
                "sender_authority_id": row[1],
                "receiver_authority_id": row[2],
                "preflight_status": row[3],
                "reason_code": row[4],
                "body_sha256": row[5],
                "received_at": row[6].isoformat() if row[6] else None,
                "latest_event": row[7] or None,
                "receiver_generation_id": row[8],
                "nomos_effect": row[9],
                "nomos_reason_code": row[10],
                "nomos_decision_id": row[11],
                "policy_bundle_id": row[12],
                "policy_fingerprint": row[13],
                "nomos_input_hash": row[14],
                "promotion_effect": row[15],
                "promotion_reason_code": row[16],
                "promotion_decision_id": row[17],
                "promotion_policy_bundle_id": row[18],
                "promotion_policy_fingerprint": row[19],
                "promotion_input_hash": row[20],
                "event_types": list(row[21] or []),
            }
            envelope = row[22] if isinstance(row[22], dict) else {}
            body = envelope.get("body") if isinstance(envelope.get("body"), dict) else {}
            inbox["graph"] = _proof_graph_summary(
                body,
                redact_objects=_proof_body_is_protected(body),
            )
            inbox["state"] = _receiver_status_state(inbox)
            messages.append(inbox)
    return {"status": "ok", "side": "receiver", "messages": messages}


def _receiver_status_state(row: dict[str, Any]) -> str:
    if row["preflight_status"] == "rejected_preflight":
        return "preflight-rejected"
    event_types = list(row.get("event_types") or [])
    if "promoted" in event_types:
        return "promoted"
    if "rejected" in event_types:
        return "rejected"
    if "rejection_denied" in event_types:
        return "rejection-denied"
    if "promotion_denied" in event_types:
        return "promotion-denied"
    if "published" in event_types:
        return "published"
    if "admitted" in event_types:
        return "admitted-unpublished"
    if "review" in event_types:
        return "review"
    if "quarantined" in event_types:
        return "quarantined"
    if row.get("nomos_effect") == "review":
        return "review"
    if row.get("nomos_effect") == "quarantine":
        return "quarantined"
    if row.get("nomos_effect") == "deny":
        return "blocked"
    if row["preflight_status"] == "duplicate" or "duplicate" in event_types:
        return "duplicate"
    return "preflight-accepted"




def _write_json(path: Path, value: Any, *, secret: bool = False) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if secret:
        os.chmod(path, 0o600)


def _authority_slug(authority_id: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in authority_id).strip("-")


def _artifact_readme(args: argparse.Namespace, status: str) -> str:
    return f"""# Poros Demo Artifacts

This directory was created by:

```bash
okg poros demo --out {args.out}
```

The demo created a sender identity, receiver identity, receiver authority
document, local sender trust pin, signed Poros graph envelope, preflight
admission candidate, and signed receiver receipt.

Status: `{status}`

Important files:

- `receiver.authority.json`: public discovery document for the receiver.
- `trust-pins.json`: local receiver trust pin for the sender signing key.
- `envelope.wire.json`: canonical signed envelope bytes suitable for POST.
- `envelope.pretty.json`: readable copy of the same envelope.
- `admission-candidate.json`: what the receiver would pass to admission.
- `receipt.wire.json`: canonical signed receipt bytes.
- `receipt.pretty.json`: readable copy of the signed receipt.
- `sender.keys.json` and `receiver.keys.json`: demo private keys. Do not
  use them outside this local demo.

This proves the Poros protocol boundary: export, sign, transmit, verify
trust pins, reject replay/expiry/tamper, and return a signed receipt.
Use `okg poros submit --apply --publish` with a migrated test database
to route the same graph through admission and the normal publisher.
"""
