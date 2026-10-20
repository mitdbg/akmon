#!/usr/bin/env python3
"""Validate the schema migration applicability manifest."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = ROOT / "src" / "okg" / "substrate" / "db" / "migrations"
DEFAULT_MANIFEST = MIGRATIONS_DIR / "manifest.yaml"

ALLOWED_APPLICABILITY = {
    "always",
    "legacy_live_runtime",
    "operator_confirmed_legacy_retirement",
}
ALLOWED_DESTRUCTIVE_APPROVAL = {
    "not_required",
    "requires_explicit_operator_confirm",
}
ALLOWED_SCHEMA_DIFF_MODES = {
    "required",
    "advisory",
    "not_applicable",
}


def _repo_relative(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def migration_sql_files(migrations_dir: Path = MIGRATIONS_DIR) -> list[Path]:
    return sorted(
        path
        for path in migrations_dir.glob("*.sql")
        if path.is_file() and not path.name.endswith(".down.sql")
    )


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("manifest root must be a mapping")
    return data


def _validate_string_list(
    *,
    errors: list[str],
    migration_id: str,
    field: str,
    value: Any,
) -> None:
    if not isinstance(value, list) or not value:
        errors.append(f"{migration_id}: {field} must be a non-empty list")
        return
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            errors.append(f"{migration_id}: {field}[{index}] must be text")


def _validate_squawk(
    *,
    errors: list[str],
    migration_id: str,
    entry: dict[str, Any],
) -> None:
    squawk = entry.get("squawk")
    if not isinstance(squawk, dict):
        errors.append(f"{migration_id}: squawk must be a mapping")
        return
    findings = squawk.get("expected_findings")
    if not isinstance(findings, list):
        errors.append(f"{migration_id}: squawk.expected_findings must be a list")
        return
    for index, finding in enumerate(findings):
        if not isinstance(finding, dict):
            errors.append(
                f"{migration_id}: squawk.expected_findings[{index}] "
                "must be a mapping"
            )
            continue
        if not str(finding.get("rule") or "").strip():
            errors.append(
                f"{migration_id}: squawk.expected_findings[{index}].rule "
                "is required"
            )
        if not str(finding.get("reason") or "").strip():
            errors.append(
                f"{migration_id}: squawk.expected_findings[{index}].reason "
                "is required"
            )


def _validate_schema_diff(
    *,
    errors: list[str],
    migration_id: str,
    entry: dict[str, Any],
) -> None:
    schema_diff = entry.get("schema_diff")
    if not isinstance(schema_diff, dict):
        errors.append(f"{migration_id}: schema_diff must be a mapping")
        return
    mode = schema_diff.get("mode")
    if mode not in ALLOWED_SCHEMA_DIFF_MODES:
        errors.append(
            f"{migration_id}: schema_diff.mode must be one of "
            f"{sorted(ALLOWED_SCHEMA_DIFF_MODES)}"
        )
    if not str(schema_diff.get("reason") or "").strip():
        errors.append(f"{migration_id}: schema_diff.reason is required")


def validate_manifest(
    manifest: dict[str, Any],
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    sql_files: list[Path] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    if manifest.get("version") != 1:
        errors.append("version must be 1")

    entries = manifest.get("migrations")
    if not isinstance(entries, list):
        errors.append("migrations must be a list")
        entries = []

    sql_files = sql_files or migration_sql_files(manifest_path.parent)
    sql_by_name = {path.name: path for path in sql_files}
    entry_by_file: dict[str, dict[str, Any]] = {}
    seen_ids: set[str] = set()

    for index, raw_entry in enumerate(entries):
        if not isinstance(raw_entry, dict):
            errors.append(f"migrations[{index}] must be a mapping")
            continue
        migration_id = str(raw_entry.get("migration_id") or "").strip()
        if not migration_id:
            migration_id = f"migrations[{index}]"
            errors.append(f"{migration_id}: migration_id is required")
        elif migration_id in seen_ids:
            errors.append(f"{migration_id}: duplicate migration_id")
        seen_ids.add(migration_id)

        file_name = str(raw_entry.get("file") or "").strip()
        if not file_name:
            errors.append(f"{migration_id}: file is required")
        elif file_name in entry_by_file:
            errors.append(f"{migration_id}: duplicate file {file_name}")
        elif file_name not in sql_by_name:
            errors.append(f"{migration_id}: file does not exist: {file_name}")
        else:
            entry_by_file[file_name] = raw_entry
            expected_checksum = str(raw_entry.get("checksum_sha256") or "")
            observed_checksum = _sha256(sql_by_name[file_name])
            if expected_checksum != observed_checksum:
                errors.append(
                    f"{migration_id}: checksum mismatch for {file_name}: "
                    f"expected {expected_checksum}, observed {observed_checksum}"
                )

        if not str(raw_entry.get("title") or "").strip():
            errors.append(f"{migration_id}: title is required")
        if raw_entry.get("applicability") not in ALLOWED_APPLICABILITY:
            errors.append(
                f"{migration_id}: applicability must be one of "
                f"{sorted(ALLOWED_APPLICABILITY)}"
            )
        if not isinstance(raw_entry.get("destructive"), bool):
            errors.append(f"{migration_id}: destructive must be boolean")
        if raw_entry.get("destructive_approval") not in ALLOWED_DESTRUCTIVE_APPROVAL:
            errors.append(
                f"{migration_id}: destructive_approval must be one of "
                f"{sorted(ALLOWED_DESTRUCTIVE_APPROVAL)}"
            )
        if raw_entry.get("destructive") is True and (
            raw_entry.get("destructive_approval") == "not_required"
        ):
            errors.append(
                f"{migration_id}: destructive migrations require explicit approval"
            )
        declared_class = raw_entry.get("class")
        if declared_class is not None and declared_class not in (
            "single_transaction",
            "multi_phase",
        ):
            # Mirror the apply-time refusal statically so a typo'd class
            # (which would otherwise silently classify as
            # single_transaction at status-read surfaces) reds CI.
            errors.append(
                f"{migration_id}: class must be single_transaction or "
                f"multi_phase, got {declared_class!r}"
            )
        if declared_class == "multi_phase" and raw_entry.get(
            "destructive"
        ) is True:
            errors.append(
                f"{migration_id}: multi_phase migrations cannot also be "
                "destructive"
            )
        _validate_string_list(
            errors=errors,
            migration_id=migration_id,
            field="prerequisites",
            value=raw_entry.get("prerequisites"),
        )
        _validate_squawk(errors=errors, migration_id=migration_id, entry=raw_entry)
        _validate_schema_diff(
            errors=errors,
            migration_id=migration_id,
            entry=raw_entry,
        )

    missing = sorted(set(sql_by_name) - set(entry_by_file))
    for file_name in missing:
        errors.append(f"missing manifest entry for {file_name}")

    return {
        "ok": not errors,
        "errors": errors,
        "migration_count": len(entries),
        "sql_files": [_repo_relative(path) for path in sql_files],
        "manifest": _repo_relative(manifest_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate src/okg/substrate/db/migrations/manifest.yaml."
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST),
        help="Path to the migration manifest.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit machine-readable output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    manifest_path = Path(args.manifest)
    if not manifest_path.is_absolute():
        manifest_path = ROOT / manifest_path
    try:
        manifest = load_manifest(manifest_path)
        result = validate_manifest(manifest, manifest_path=manifest_path)
    except Exception as exc:  # noqa: BLE001 - command should report parser errors.
        result = {
            "ok": False,
            "errors": [f"{type(exc).__name__}: {exc}"],
            "migration_count": 0,
            "sql_files": [],
            "manifest": _repo_relative(manifest_path),
        }

    if args.json_output:
        print(json.dumps(result, indent=2, sort_keys=True))
    elif result["ok"]:
        print(
            "schema migration manifest: "
            f"ok {result['migration_count']} migration(s)"
        )
    else:
        print("schema migration manifest: FAIL", file=sys.stderr)
        for error in result["errors"]:
            print(f"  - {error}", file=sys.stderr)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
