"""`okg deployment add-source` and `okg deployment source-types` (issue #1802).

One guided entry point: choose a supported source type, give its location or
scope and only the missing access reference, see a preview, confirm once with
`--apply`, and finish with a searchable published sample. The flow logic
lives in `okg.substrate.sources.guided_add`; this module only parses
arguments, prints, and prompts.
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from okg.substrate.errors import SubstrateError


def _emit(payload: dict[str, Any], *, as_json: bool) -> None:
    from okg.substrate.cli._output import emit_result

    if as_json:
        emit_result(payload, as_json=True)
    else:
        print(_render_text(payload))


def _emit_error(err: Exception, *, as_json: bool) -> int:
    from okg.substrate.cli.main import _emit_error as emit

    return emit(err, as_json=as_json)


def parse_settings(raw: list[str] | None) -> dict[str, Any]:
    """`name=value` pairs; a JSON value is decoded, anything else is a string."""
    settings: dict[str, Any] = {}
    for item in raw or ():
        key, sep, value = str(item).partition("=")
        if not sep or not key.strip():
            raise SubstrateError(
                f"--setting expects name=value, got {item!r}",
                fix_hint="example: --setting root=/data/notes",
            )
        text = value.strip()
        decoded: Any = text
        if text[:1] in "[{\"" or text in ("true", "false", "null") or (
            text.replace(".", "", 1).isdigit()
        ):
            try:
                decoded = json.loads(text)
            except ValueError:
                decoded = text
        settings[key.strip()] = decoded
    return settings


def cmd_source_types(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.sources.guided_catalog import SUPPORTED_SOURCE_TYPES

    payload = {
        "schema": "okg.deployment.source_types/v1",
        "types": [t.describe() for t in SUPPORTED_SOURCE_TYPES.values()],
        "unsupported": (
            "A format or service not listed here needs a new reader before it "
            "can be added; see the developer guide, 'Add a source adapter'."
        ),
    }
    if as_json:
        _emit(payload, as_json=True)
        return 0
    for item in payload["types"]:
        print(f"{item['type']}: {item['title']}")
        print(f"  {item['summary']}")
        for setting in item["settings"]:
            flag = "required" if setting["required"] else "optional"
            print(f"  --setting {setting['name']}=...  ({flag}) {setting['description']}")
        print(f"  Access: {item['access']}")
        print(f"  Updates: {item['update_behaviour']}")
        print(f"  Removals: {item['removal_behaviour']}")
    print(payload["unsupported"])
    return 0


def cmd_add_source(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.sources.guided_add import (
        AddSourceError,
        apply_add_source,
        plan_add_source,
    )

    manifest = getattr(args, "manifest", None)
    if manifest is not None and (
        getattr(args, "type", None) is not None
        or getattr(args, "registration", None) is not None
        or getattr(args, "setting", None)
        or getattr(args, "credential", None)
    ):
        return _emit_error(
            SubstrateError(
                "--manifest describes the whole source; it cannot be combined "
                "with --type, --registration, --setting or --credential",
                fix_hint="put everything about the source in the manifest file",
            ),
            as_json=as_json,
        )
    if (
        getattr(args, "type", None) is None
        and getattr(args, "registration", None) is None
        and manifest is None
    ):
        return _emit_error(
            SubstrateError(
                "choose a source type with --type, or describe the source with --manifest",
                fix_hint="run `okg deployment source-types` to list them",
            ),
            as_json=as_json,
        )
    try:
        settings = parse_settings(getattr(args, "setting", None))
        plan = plan_add_source(
            deployment=args.deployment,
            name=args.name,
            type_id=getattr(args, "type", None),
            settings=settings,
            credential_refs=tuple(getattr(args, "credential", None) or ()),
            registration_path=getattr(args, "registration", None),
            deployments_root=getattr(args, "deployments_root", None),
            manifest_path=manifest,
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    plan_payload = plan.as_dict()
    if not plan.ok:
        failed = plan.failed_step
        _emit(plan_payload, as_json=as_json)
        if not as_json and failed is not None:
            print(f"Cannot add {args.name!r}: {failed.cause}")
            if failed.fix_hint:
                print(f"  Fix: {failed.fix_hint}")
        return 1
    if not getattr(args, "apply", False):
        plan_payload["apply"] = False
        plan_payload["next"] = (
            "re-run with --apply to add this source (saves the definition, "
            "imports, checks, publishes, and shows a searchable sample)"
        )
        _emit(plan_payload, as_json=as_json)
        return 0
    if not as_json and not getattr(args, "yes", False) and sys.stdin.isatty():
        print(_render_text(plan_payload))
        try:
            answer = input("Add this source now? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            # #2480: end of input (Ctrl-D) or Ctrl-C at the prompt is not
            # a yes; say so instead of printing a traceback.
            print()
            print("Nothing was saved.")
            return 1
        if answer not in ("y", "yes"):
            print("Nothing was saved.")
            return 0
    try:
        outcome = apply_add_source(plan, dsn=getattr(args, "dsn", None))
    except AddSourceError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit(outcome.as_dict(), as_json=as_json)
    return 0 if outcome.status in ("published", "no_change") else 1


def _render_text(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    schema = str(payload.get("schema") or "")
    if schema.endswith("plan/v1"):
        lines.append(
            f"Add source {payload['name']!r} ({payload.get('type')}) to "
            f"deployment {payload['deployment']!r}"
        )
        preview = payload.get("preview") or {}
        scope = preview.get("scope") or {}
        if scope:
            lines.append("Scope to read: " + ", ".join(
                f"{k}={v}" for k, v in scope.items()
            ))
        lines.extend(_render_manifest_preview(preview.get("manifest")))
        if preview.get("records"):
            lines.append("Representative records:")
            for rec in preview["records"]:
                lines.append(f"  - [{rec['subtype']}] {rec['label']}")
        if preview.get("relationships"):
            lines.append("Representative relationships:")
            for rel in preview["relationships"]:
                lines.append(f"  - {rel['from']} -{rel['type']}-> {rel['to']}")
        for decision in payload.get("decisions") or ():
            lines.append(f"Decision ({decision['kind']}): {decision['why']}")
            if decision.get("add"):
                lines.append(f"  add modules: {', '.join(decision['add'])}")
            for item in decision.get("install") or ():
                origin = f"from {item['from']}" if item.get("from") else item.get("action")
                lines.append(f"  install schema: {item['to']} ({origin})")
            for change in decision.get("changes") or ():
                lines.append(f"  {change}")
        behaviour = payload.get("behaviour") or {}
        for key in ("update", "removal", "refresh"):
            if behaviour.get(key):
                lines.append(f"{key.title()}: {behaviour[key]}")
        for step in payload.get("steps") or ():
            if step["status"] == "failed":
                lines.append(f"Problem at {step['step']}: {step.get('cause')}")
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
        if payload.get("status") == "no_change":
            lines.append(
                "No change: this source's records are already in graph "
                f"version {payload.get('generation_id')}, which stays "
                "current. The source stays enabled."
            )
        elif payload.get("generation_id"):
            lines.append(f"Published graph version: {payload['generation_id']}")
            counts = payload.get("counts") or {}
            lines.append(
                f"Records added: {counts.get('records_added')}, relationships "
                f"added: {counts.get('relationships_added')}"
            )
            if counts.get("records_removed") or counts.get("relationships_removed"):
                lines.append(
                    f"Records removed: {counts.get('records_removed')}, "
                    f"relationships removed: {counts.get('relationships_removed')}"
                )
        for step in payload.get("steps") or ():
            note = (step.get("detail") or {}).get("note") if isinstance(step, dict) else None
            if step.get("step") == "import" and note:
                lines.append(f"Note: {note}")
        sample = payload.get("sample")
        if sample:
            lines.append(
                f"Sample searchable result for {sample['query']!r}: "
                f"[{sample['subtype']}] {sample['label']} (from source "
                f"{sample['source']}; origin {sample.get('origin') or 'n/a'})"
            )
        if payload.get("retry"):
            lines.append(payload["retry"])
        return "\n".join(lines)
    return json.dumps(payload, indent=2, default=str)


def _render_manifest_preview(section: Any) -> list[str]:
    """The `--manifest` preview: files, records per type, samples, problems."""
    if not isinstance(section, dict):
        return []
    lines: list[str] = []
    reads = section.get("reads") or ()
    if any(r.get("kind") == "http" for r in reads):
        lines.append("Reads:")
        for read in reads:
            state = "complete" if read.get("complete") else f"INCOMPLETE: {read.get('error')}"
            size = (
                f"{read.get('pages')} page(s)" if read.get("kind") == "http"
                else f"{read.get('files')} file(s)"
            )
            lines.append(f"  - {read.get('type')}: {read.get('what')}; {size}, "
                         f"{read.get('records')} record(s); {state}")
        if section.get("files_matched"):
            lines.append(f"Files matched: {section.get('files_matched')}")
    else:
        lines.append(f"Files matched: {section.get('files_matched')}")
    for path in section.get("files") or ():
        lines.append(f"  - {path}")
    counts = section.get("records_per_type") or {}
    lines.append("Records per type: " + (
        ", ".join(f"{k}={v}" for k, v in counts.items()) or "none"
    ))
    lines.append("Types it will create: " + ", ".join(section.get("types") or ()))
    if section.get("links"):
        lines.append("Links it will create: " + "; ".join(section["links"]))
    for type_name, samples in (section.get("samples") or {}).items():
        lines.append(f"Sample {type_name} records:")
        for sample in samples:
            lines.append(f"  - {sample['key']}: {json.dumps(sample['attrs'], default=str)}")
            for link in sample.get("links") or ():
                lines.append(f"      {link}")
    if section.get("max_deleted_fraction") is not None:
        lines.append(
            "A run that would remove more than "
            f"{section['max_deleted_fraction']:g} of the earlier records removes "
            "nothing (max_deleted_fraction)."
        )
    lines.append(
        f"Deletion: {section.get('deletion')}; rejected records: "
        f"{section.get('rejected_records')}; links dropped (target not in "
        f"this read): {section.get('dropped_links')}"
    )
    for problem in section.get("problems") or ():
        where = " ".join(
            str(problem[k]) for k in ("path", "file", "record") if problem.get(k)
        )
        mark = "STOPS IMPORT" if problem.get("blocking") else "note"
        lines.append(f"  [{mark}] {problem['message']} ({where})")
        lines.append(f"      fix: {problem['fix']}")
    admission = section.get("admission") or {}
    lines.append(
        "Admission would accept the records: "
        + ("yes" if admission.get("accepted") else "no")
    )
    return lines


def register_deployment_subparsers(
    p_dep_sub: Any, *, add_json_flag: Any, add_dsn_arg: Any,
) -> None:
    p_types = p_dep_sub.add_parser(
        "source-types",
        help="List the source types `add-source` can add",
    )
    add_json_flag(p_types)

    p_add = p_dep_sub.add_parser(
        "add-source",
        help=(
            "Add a supported data source: preview, then --apply to save, "
            "import, publish, and show a searchable sample"
        ),
    )
    add_json_flag(p_add)
    p_add.add_argument("deployment", help="Deployment name or directory")
    p_add.add_argument("name", help="Name for the new source (letters, digits, -, _)")
    p_add.add_argument("--type", help="Source type from `source-types`")
    p_add.add_argument(
        "--registration",
        help="Packaged connector asset JSON to use instead of --type",
    )
    p_add.add_argument(
        "--manifest", metavar="FILE",
        help=(
            "A source manifest (okg.source/v1) describing local JSON, JSONL "
            "or CSV files or JSON APIs; replaces --type"
        ),
    )
    p_add.add_argument(
        "--setting", action="append", default=[], metavar="NAME=VALUE",
        help="A source setting (repeatable); see `source-types`",
    )
    p_add.add_argument(
        "--credential", action="append", default=[], metavar="ENV_NAME",
        help="Name of an environment variable holding a credential (never its value)",
    )
    p_add.add_argument("--deployments-root", help="Directory holding deployments")
    add_dsn_arg(p_add)
    p_add.add_argument(
        "--apply", action="store_true",
        help="Confirm: save the definition, import, check, publish",
    )
    p_add.add_argument(
        "--yes", action="store_true",
        help="Skip the interactive confirmation prompt",
    )


__all__ = [
    "cmd_add_source",
    "cmd_source_types",
    "parse_settings",
    "register_deployment_subparsers",
]
