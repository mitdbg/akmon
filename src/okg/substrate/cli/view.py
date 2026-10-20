"""CLI handlers for `okg view`.

Extracted from ``cli/main.py`` by ``extract-cli-view`` (2026-05).
Subcommands: list / show / inspect. cli.main helpers loaded
lazily via the shared ``_load_main_helpers()`` cache (per Wave
D-1).
"""
from __future__ import annotations

import argparse
from collections.abc import Mapping
import json
import os
import sys
from typing import Any


# Module-level helper bindings, populated by _load_main_helpers().
_emit_error: Any = None
_resolve_deployment_dsn: Any = None
SubstrateError: Any = None


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError``."""
    global _emit_error
    global _resolve_deployment_dsn, SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _resolve_deployment_dsn as _rd,
    )
    from okg.substrate.errors import SubstrateError as _se
    _emit_error = _e
    _resolve_deployment_dsn = _rd
    SubstrateError = _se


def _cmd_view(args: argparse.Namespace, *, as_json: bool) -> int:
    """Dispatcher for `okg view <subcmd>`."""
    _load_main_helpers()
    if args.subcmd == "list":
        return _cmd_view_list(args, as_json=as_json)
    if args.subcmd == "show":
        return _cmd_view_show(args, as_json=as_json)
    if args.subcmd == "inspect":
        return _cmd_view_inspect(args, as_json=as_json)
    return _emit_error(
        SubstrateError(
            f"unknown view subcommand {args.subcmd!r}",
            fix_hint="okg view --help",
        ),
        as_json=as_json,
    )


def _cmd_view_list(args: argparse.Namespace, *, as_json: bool) -> int:
    """List canonical native view artifacts for a deployment."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.derived_artifacts import list_view_artifacts

    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg view list requires --deployment",
                fix_hint="okg view list --deployment <name>",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_deployment_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    with db_connect(dsn, autocommit=True) as conn:
        items = list_view_artifacts(
            conn,
            deployment_name=deployment,
            generation_id=args.generation,
        )

    payload = {
        "items": [item.to_json() for item in items],
        "meta": {
            "deployment": deployment,
            "generation": args.generation,
            "count": len(items),
        },
    }
    if as_json:
        print(json.dumps(payload, sort_keys=True))
        return 0

    if not items:
        sys.stdout.write("no native view artifacts found\n")
        return 0

    view_w = max(len("view_id"), *(len(item.view_id) for item in items))
    gen_w = max(
        len("generation"),
        *(len(str(item.target_generation_id)) for item in items),
    )
    fresh_w = max(len("freshness"), *(len(item.freshness) for item in items))
    header = (
        f"{'view_id'.ljust(view_w)}  "
        f"{'generation'.rjust(gen_w)}  "
        f"{'freshness'.ljust(fresh_w)}  "
        "slots"
    )
    sys.stdout.write(header + "\n")
    sys.stdout.write("-" * len(header) + "\n")
    for item in items:
        slots = ",".join(item.slot_names or item.required_slots)
        sys.stdout.write(
            f"{item.view_id.ljust(view_w)}  "
            f"{str(item.target_generation_id).rjust(gen_w)}  "
            f"{item.freshness.ljust(fresh_w)}  "
            f"{slots}\n"
        )
    return 0


def _cmd_view_show(args: argparse.Namespace, *, as_json: bool) -> int:
    """Read one canonical native view output through its policy authority."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.deployments import find_deployment_dir
    from okg.substrate.derived_artifacts import ViewArtifactReadError
    from okg.substrate.views.read_policy import (
        ReadSurfacePolicyError,
        read_governed_native_view,
    )
    from okg.substrate.views.registry import (
        adapt_views_registry_entries,
        load_views_registry,
    )

    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg view show requires --deployment",
                fix_hint="okg view show --deployment <name> --view <view_id>",
            ),
            as_json=as_json,
        )
    audience = args.audience or os.environ.get("OKG_READ_AUDIENCE")
    tenant = args.tenant or os.environ.get("OKG_READ_TENANT")
    if not audience or not tenant:
        return _emit_error(
            SubstrateError(
                "okg view show requires explicit --audience and --tenant policy identities",
                fix_hint=(
                    "okg view show --deployment <name> --view <view_id> "
                    "--audience <id> --tenant <id>"
                ),
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_deployment_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    try:
        deployment_root = find_deployment_dir(deployment)
        if deployment_root is None:
            raise ReadSurfacePolicyError(
                "native_read_deployment_missing",
                "deployment definition is unavailable for governed native read",
            )
        resolution = adapt_views_registry_entries(
            load_views_registry(deployment_root)
        )
        contract = next(
            (
                item
                for item in resolution.contracts
                if item.asset.asset_id == args.view
            ),
            None,
        )
        if contract is None:
            raise ReadSurfacePolicyError(
                "native_read_asset_missing",
                "view is not selected by the deployment's canonical read registry",
            )
        if args.definition_id not in (None, f"view:{args.view}"):
            raise ReadSurfacePolicyError(
                "native_read_definition_mismatch",
                "definition override is outside the canonical read asset identity",
            )
        with db_connect(dsn, autocommit=True) as conn:
            result = read_governed_native_view(
                conn,
                contract=contract,
                execution_token=resolution.execution_token(contract.qualified_id),
                deployment_name=deployment,
                view_id=args.view,
                slot_name=args.slot,
                audience=audience,
                tenant=tenant,
                generation_id=args.generation,
            )
    except (ViewArtifactReadError, ReadSurfacePolicyError, ValueError) as exc:
        refusal_reason = (
            exc.code if isinstance(exc, ReadSurfacePolicyError) else None
        )
        return _emit_error(
            SubstrateError(
                str(exc),
                data={
                    **({"reason": refusal_reason} if refusal_reason else {}),
                    "deployment": deployment,
                    "view": args.view,
                    "slot": args.slot,
                    "generation": args.generation,
                    "definition_id": args.definition_id,
                },
                fix_hint=(
                    exc.fix_hint
                    if isinstance(exc, ReadSurfacePolicyError) and exc.fix_hint
                    else (
                        "check the view id, slot name, generation, and "
                        "whether the generation has a completed native "
                        "view manifest"
                    )
                ),
            ),
            as_json=as_json,
        )

    value = result.payload
    if as_json:
        print(json.dumps({
            "asset_id": result.asset_id,
            "asset_digest": result.asset_digest,
            "generation_id": result.generation_id,
            "visible_fields": list(result.visible_fields),
            "lineage_sources": list(result.lineage_sources),
            "decision_ids": list(result.decision_ids),
            "payload": dict(value) if isinstance(value, Mapping) else value,
        }, sort_keys=True, default=str))
        return 0

    if isinstance(value, str):
        sys.stdout.write(value)
        if not value.endswith("\n"):
            sys.stdout.write("\n")
    else:
        print(json.dumps(value, indent=2, sort_keys=True, default=str))
    return 0


def _cmd_view_inspect(args: argparse.Namespace, *, as_json: bool) -> int:
    """Inspect native-view metadata without bypassing governed payload reads."""
    _load_main_helpers()
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.derived_artifacts import (
        ViewArtifactReadError,
        inspect_view_slot,
    )

    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg view inspect requires --deployment",
                fix_hint=(
                    "okg view inspect --deployment <name> --view <view_id>"
                ),
            ),
            as_json=as_json,
        )

    if args.preview_limit:
        return _emit_error(
            SubstrateError(
                "okg view inspect cannot preview policy-visible relation rows",
                fix_hint=(
                    "use okg view show with explicit --audience and --tenant "
                    "to read a governed output slot"
                ),
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_deployment_dsn(args)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    try:
        with db_connect(dsn, autocommit=True) as conn:
            payload = inspect_view_slot(
                conn,
                deployment_name=deployment,
                view_id=args.view,
                slot_name=args.slot,
                generation_id=args.generation,
                definition_id=args.definition_id,
                preview_limit=0,
            )
        slot_metadata = dict(payload["slot"])
        for private_key in ("inline_payload", "payload", "storage_ref"):
            slot_metadata.pop(private_key, None)
        payload = {**payload, "slot": slot_metadata}
    except ViewArtifactReadError as exc:
        return _emit_error(
            SubstrateError(
                str(exc),
                data={
                    "deployment": deployment,
                    "view": args.view,
                    "slot": args.slot,
                    "generation": args.generation,
                    "definition_id": args.definition_id,
                },
                fix_hint=(
                    "check the view id, slot name, generation, and "
                    "whether the generation has a completed native "
                    "view manifest"
                ),
            ),
            as_json=as_json,
        )

    if as_json:
        print(json.dumps(payload, sort_keys=True, default=str))
        return 0

    _emit_view_inspection_text(payload)
    return 0


def _emit_view_inspection_text(payload: dict[str, Any]) -> None:
    artifact = payload["artifact"]
    slot = payload["slot"]
    sys.stdout.write(f"view: {artifact['view_id']}\n")
    sys.stdout.write(f"definition_id: {artifact['definition_id']}\n")
    sys.stdout.write(f"manifest_id: {artifact['manifest_id']}\n")
    sys.stdout.write(f"generation: {artifact['target_generation_id']}\n")
    sys.stdout.write(f"slot: {slot['slot_name']}\n")
    sys.stdout.write(f"storage_kind: {slot['storage_kind']}\n")
    sys.stdout.write(f"media_type: {slot['media_type']}\n")
    sys.stdout.write(f"payload_hash: {slot['payload_hash']}\n")

    relation = payload.get("relation")
    if not relation:
        return

    sys.stdout.write("\nrelation:\n")
    sys.stdout.write(f"  available: {relation['available']}\n")
    sys.stdout.write(
        f"  logical_relation_id: {relation.get('logical_relation_id')}\n"
    )
    sys.stdout.write(f"  physical_relation: {relation.get('physical_relation')}\n")
    sys.stdout.write(
        f"  upstream_definition_id: "
        f"{relation.get('upstream_definition_id')}\n"
    )
    sys.stdout.write(
        f"  upstream_manifest_id: {relation.get('upstream_manifest_id')}\n"
    )
    sys.stdout.write(
        f"  target_generation_id: {relation.get('target_generation_id')}\n"
    )
    if not relation["available"]:
        sys.stdout.write(f"  reason: {relation.get('reason')}\n")
        return
    sys.stdout.write(f"  rows: {relation['row_count']}\n")
    sys.stdout.write(f"  scopes: {relation['scope_count']}\n")
    bytes_ = relation["bytes"]
    sys.stdout.write(
        f"  bytes: total={bytes_['total']} "
        f"table={bytes_['table']} indexes={bytes_['indexes']}\n"
    )
    bound_hashes = relation.get("bound_hashes") or {}
    sys.stdout.write("  bound_hashes:\n")
    for key in (
        "compute_hash",
        "output_schema_hash",
        "renderer_hash",
        "dirty_rule_hash",
    ):
        sys.stdout.write(f"    {key}: {bound_hashes.get(key)}\n")
    registry = relation.get("registry")
    if registry:
        sys.stdout.write("  registry:\n")
        sys.stdout.write(f"    status: {registry.get('status')}\n")
        sys.stdout.write(
            f"    output_schema_hash: "
            f"{registry.get('output_schema_hash')}\n"
        )
        sys.stdout.write(f"    ddl_hash: {registry.get('ddl_hash')}\n")
    preview = relation.get("preview") or {}
    rows = preview.get("rows") or []
    if rows:
        sys.stdout.write("  preview:\n")
        for row in rows:
            sys.stdout.write(
                "    "
                + json.dumps(row, sort_keys=True, default=str)
                + "\n"
            )


# `_cmd_catalog` moved to `cli/catalog.py` by `extract-cli-catalog`.

# `_cmd_catalog_history` moved to `cli/catalog.py` by `extract-cli-catalog`.

# `_cmd_runtime` moved to `cli/runtime.py` by `extract-cli-runtime`.

# `_cmd_runtime_escalation` moved to `cli/runtime.py` by `extract-cli-runtime`.

def _current_user() -> str:
    """Operator identifier for CLI-stamped decisions. Falls back to
    ``unknown`` when $USER is unset (e.g. some CI environments)."""
    _load_main_helpers()
    import os
    return os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"
