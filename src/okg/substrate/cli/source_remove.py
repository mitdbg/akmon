"""`okg deployment remove-source` (issue #2477).

The counterpart of `okg deployment add-source`: without `--apply` it shows
what removing a source would retract and which registry entry goes; with
`--apply` it retracts the records, publishes, removes the entry, and
re-claims the deployment identity. The flow lives in
`okg.substrate.sources.guided_remove`; this module parses, prints, prompts.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable

from okg.substrate.errors import RemoveSourceError, SubstrateError


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    from okg.substrate.cli._output import emit_result

    if as_json:
        emit_result(payload, as_json=True)
    else:
        print(render_text(payload))


def _emit_error(err: Exception, *, as_json: bool) -> int:
    from okg.substrate.cli.main import _emit_error as emit

    return emit(err, as_json=as_json)


def confirm(prompt: str, *, read: Callable[[str], str] | None = None) -> bool:
    """Ask once; EOF or Ctrl-C reads as "no" instead of a traceback."""
    try:
        answer = (read or input)(prompt)
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer.strip().lower() in ("y", "yes")


def cmd_remove_source(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.sources.guided_remove import (
        apply_remove_source,
        plan_remove_source,
    )

    try:
        plan = plan_remove_source(
            deployment=args.deployment,
            name=args.name,
            deployments_root=getattr(args, "deployments_root", None),
            dsn=getattr(args, "dsn", None),
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    plan_payload = plan.as_dict()
    if not getattr(args, "apply", False):
        plan_payload["apply"] = False
        plan_payload["next"] = _next_step(plan.action, plan.name)
        _emit(plan_payload, as_json=as_json)
        return 0
    if not getattr(args, "yes", False):
        if as_json or not sys.stdin.isatty():
            # Removal retracts records and deletes configuration; with no one
            # at a terminal to answer, refuse rather than assume a yes.
            return _emit_error(
                RemoveSourceError(
                    "--apply needs a confirmation: "
                    + ("--json output cannot prompt" if as_json
                       else "standard input is not a terminal"),
                    data={
                        "deployment": plan.deployment_name,
                        "source": plan.name,
                        "cause": "no confirmation",
                    },
                    fix_hint="add --yes to confirm the removal",
                ),
                as_json=as_json,
            )
        print(render_text(plan_payload))
        if not confirm("Remove this source now? [y/N] "):
            print("Nothing was changed.")
            return 0
    try:
        outcome = apply_remove_source(plan)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit(outcome.as_dict(), as_json=as_json)
    return 0 if outcome.status == "removed" else 1


def _next_step(action: str, name: str) -> str:
    from okg.substrate.sources.guided_remove import (
        FINISH,
        PUBLISH_PENDING,
        REGISTRY_ONLY,
    )

    if action == FINISH:
        return (
            f"the records and registry entry of {name!r} are gone; --apply "
            "re-claims the deployment identity and updates the access policy "
            "(safe to repeat)"
        )
    if action == REGISTRY_ONLY:
        return (
            f"re-run with --apply to remove {name!r}'s registry entry and "
            "re-claim the deployment identity"
        )
    if action == PUBLISH_PENDING:
        return (
            "re-run with --apply to publish the retraction a previous run "
            f"applied, then remove {name!r}'s registry entry"
        )
    return (
        f"re-run with --apply to remove {name!r} (disables it, retracts its "
        "records, publishes, removes its registry entry, and re-claims the "
        "deployment identity)"
    )


def render_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    schema = str(payload.get("schema") or "")
    if schema.endswith("plan/v1"):
        lines.append(
            f"Remove source {payload['name']!r} ({payload.get('ownership_id')}) "
            f"from deployment {payload['deployment']!r}"
        )
        retract = payload.get("retract") or {}
        lines.append(
            f"Records to retract: {retract.get('records', 0)}, relationships "
            f"to retract: {retract.get('relationships', 0)}"
        )
        for rec in retract.get("sample") or ():
            lines.append(f"  - [{rec['subtype']}] {rec['label']}")
        registry = payload.get("registry") or {}
        if registry.get("entry_to_remove"):
            lines.append(
                f"Registry entry to remove: {registry['entry_to_remove']} "
                f"({registry.get('entry_type') or 'unknown reader'}) from "
                f"{registry.get('path')}"
            )
        if payload.get("note"):
            lines.append(f"Note: {payload['note']}")
        if payload.get("next"):
            lines.append(f"Next: {payload['next']}")
        return "\n".join(lines)
    if schema.endswith("outcome/v1"):
        lines.append(f"Source {payload['name']!r}: {payload['status']}")
        for step in payload.get("steps") or ():
            mark = {"ok": "done", "failed": "FAILED", "skipped": "skipped"}.get(
                step["status"], step["status"],
            )
            lines.append(f"  {step['step']}: {mark}")
            if step.get("cause"):
                lines.append(f"    cause: {step['cause']}")
            if step.get("fix_hint"):
                lines.append(f"    fix: {step['fix_hint']}")
        counts = payload.get("counts") or {}
        if payload.get("generation_id"):
            lines.append(f"Published graph version: {payload['generation_id']}")
        if payload.get("status") == "removed":
            lines.append(
                f"Records removed: {counts.get('records_removed')}, "
                f"relationships removed: {counts.get('relationships_removed')}"
            )
        if payload.get("note"):
            lines.append(f"Note: {payload['note']}")
        if payload.get("retry"):
            lines.append(payload["retry"])
        return "\n".join(lines)
    return json.dumps(payload, indent=2, default=str)


def register_remove_source_subparser(
    p_dep_sub: Any, *, add_json_flag: Any, add_dsn_arg: Any,
) -> None:
    p_remove = p_dep_sub.add_parser(
        "remove-source",
        help=(
            "Remove a source: preview, then --apply to retract its records, "
            "publish, and remove its registry entry"
        ),
    )
    add_json_flag(p_remove)
    p_remove.add_argument("deployment", help="Deployment name or directory")
    p_remove.add_argument(
        "name", help="The source's registry name or its ownership id",
    )
    p_remove.add_argument("--deployments-root", help="Directory holding deployments")
    add_dsn_arg(p_remove)
    p_remove.add_argument(
        "--apply", action="store_true",
        help="Confirm: retract, publish, remove the entry, re-claim",
    )
    p_remove.add_argument(
        "--yes", action="store_true",
        help=(
            "Confirm without the interactive prompt (required when input "
            "is not a terminal or with --json)"
        ),
    )


__all__ = [
    "cmd_remove_source",
    "confirm",
    "register_remove_source_subparser",
    "render_text",
]
