"""Deployment bootstrap workflow.

This module keeps the bootstrapper as an authoring orchestrator over
existing substrate primitives. It owns file sampling, plan validation,
staged artifact generation, and the small proof-publish loop; it does
not introduce a second graph runtime.
"""
from __future__ import annotations

import csv
import hashlib
import inspect
import importlib
import json
import os
import re
import shlex
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import jsonschema
import yaml
from okg.substrate import fastyaml
from okg.substrate.schema_resources import authored_file_for_schema, schemas_dir
from okg.substrate.source_run_modes import (
    WHOLE_SCOPE_REQUEST,
    resolve_whole_scope_request,
)

from okg.substrate.errors import (
    BootstrapCatalogRejected,
    BootstrapDraftInvalid,
    BootstrapPlanInvalid,
    BootstrapPublishBlocked,
    BootstrapSourceSampleFailed,
    BootstrapUnverifiedApply,
    SubstrateError,
)


PLAN_FILENAME = "bootstrap_plan.yaml"
STAGED_DIRNAME = ".okg_bootstrap/staged"
SUPPORTED_SUFFIXES = {
    ".json": "local_json",
    ".csv": "local_csv",
    ".md": "local_markdown",
    ".markdown": "local_markdown",
}


@dataclass(frozen=True)
class SourceDescriptor:
    source_id: str
    path: Path
    kind: str


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def plan_path(deployment: Path | str) -> Path:
    return Path(deployment).expanduser().resolve() / PLAN_FILENAME


def staged_dir(deployment: Path | str) -> Path:
    return Path(deployment).expanduser().resolve() / STAGED_DIRNAME


def load_plan(deployment: Path | str) -> dict[str, Any]:
    path = plan_path(deployment)
    if not path.is_file():
        raise BootstrapPlanInvalid(
            f"bootstrap plan not found: {path}",
            data={"path": str(path), "errors": ["file_not_found"]},
            fix_hint=f"okg deployment bootstrap plan --deployment {path.parent} --brief <file> --apply",
        )
    try:
        doc = fastyaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise BootstrapPlanInvalid(
            f"bootstrap_plan.yaml YAML parse failed: {exc}",
            data={"path": str(path), "errors": [str(exc)]},
            fix_hint="fix the YAML and rerun the bootstrap command",
        ) from exc
    _refuse_plan_source_schedule(doc, path=path)
    validate_against_schema(
        doc,
        "bootstrap_plan.schema.json",
        path=path,
        error_cls=BootstrapPlanInvalid,
    )
    return doc


def _refuse_plan_source_schedule(doc: Any, *, path: Path) -> None:
    """Refuse a plan source that still authors a per-source `schedule`.

    Earlier plans could carry `sources[].schedule`, which the draft copied
    into `source_registry.yaml`. The strict_v1 registry contract refuses that
    key (a scheduled refresh is the entry's `sync.poll` block), so the first
    `okg deployment add-source` failed on the drafted registry (issue #2952).
    No code ever read the key, so this names it instead of letting the
    schema's generic "additional property" message stand alone.
    """
    if not isinstance(doc, dict):
        return
    sources = doc.get("sources")
    if not isinstance(sources, list):
        return
    offending = [
        str(source.get("id") or index)
        for index, source in enumerate(sources)
        if isinstance(source, dict) and "schedule" in source
    ]
    if offending:
        raise BootstrapPlanInvalid(
            "bootstrap_plan.yaml sources carry a per-source `schedule`, which "
            "the source registry does not accept and nothing ever read: "
            + ", ".join(offending),
            data={
                "path": str(path),
                "errors": [f"sources[{name}].schedule" for name in offending],
            },
            fix_hint=(
                "delete `schedule:` from each named source in "
                "bootstrap_plan.yaml; a scheduled refresh is set by the "
                "source's `sync.poll` block in source_registry.yaml with "
                "`runtime.enabled: true` in deployment.yaml"
            ),
        )


def save_plan(deployment: Path | str, plan: dict[str, Any]) -> None:
    validate_against_schema(
        plan,
        "bootstrap_plan.schema.json",
        path=plan_path(deployment),
        error_cls=BootstrapPlanInvalid,
    )
    p = plan_path(deployment)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_dump_yaml(plan), encoding="utf-8")


def validate_against_schema(
    doc: Any,
    schema_name: str,
    *,
    path: Path | str,
    error_cls: type[SubstrateError] = BootstrapDraftInvalid,
) -> None:
    schema_path = schemas_dir() / schema_name
    schema = json.loads(schema_path.read_text())
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(doc), key=lambda e: list(e.path))
    if not errors:
        return
    formatted = [
        {
            "path": "/".join(str(p) for p in err.path),
            "message": err.message,
        }
        for err in errors[:20]
    ]
    raise error_cls(
        f"{path} failed {schema_name} validation",
        data={
            "path": str(path),
            "schema_name": schema_name,
            "errors": formatted,
        },
        fix_hint=(
            f"run `okg schema show {authored_file_for_schema(schema_name)}` "
            "and fix the authored file"
        ),
    )


def create_plan(
    *,
    deployment: Path | str,
    brief_file: Path | str,
    sources: list[str] | None = None,
    name: str | None = None,
    template_base: str = "empty",
    dsn_env: str = "OKG_DSN",
    apply: bool = False,
) -> dict[str, Any]:
    dep = Path(deployment).expanduser().resolve()
    brief_path = Path(brief_file).expanduser().resolve()
    brief_text = brief_path.read_text(encoding="utf-8")
    deployment_name = name or _slug(dep.name or "bootstrap_deployment")
    descriptors = [_parse_source_descriptor(s, base=dep) for s in (sources or [])]
    plan = _empty_plan(
        deployment_name=deployment_name,
        deployment_path=dep,
        template_base=template_base,
        dsn_env=dsn_env,
        brief_text=brief_text,
        sources=descriptors,
    )
    validate_against_schema(
        plan,
        "bootstrap_plan.schema.json",
        path=dep / PLAN_FILENAME,
        error_cls=BootstrapPlanInvalid,
    )
    report = {
        "ok": True,
        "command": "deployment bootstrap plan",
        "applied": apply,
        "plan_path": str(dep / PLAN_FILENAME),
        "proposed_writes": [str(dep / PLAN_FILENAME)],
        "status": _compute_status(plan),
    }
    lookup = name_lookup_advice(dep, deployment_name)
    report["name_lookup"] = lookup
    if not lookup["resolves_by_name"]:
        # Warn, never refuse: the bootstrap commands take the folder, and a
        # deployment kept elsewhere still works through `--deployment <folder>`.
        report["warnings"] = [lookup["warning"]]
        print(f"warning: {lookup['warning']}", file=sys.stderr)
    if apply:
        dep.mkdir(parents=True, exist_ok=True)
        save_plan(dep, plan)
    return report


def name_lookup_advice(dep: Path, deployment_name: str) -> dict[str, Any]:
    """Whether later `--deployment <name>` commands reach this folder (#2951).

    `okg run`, `okg ingest`, `okg deployment add-source` and the other
    deployment commands find a deployment by name only under the deployments
    roots (`OKG_DEPLOYMENTS_DIR`, the user-state directory, a checkout's
    `deployments/`). Bootstrap takes any folder, so say up front when the name
    will not reach it and what makes it reachable.
    """
    from okg.substrate.deployments import deployment_name_lookup

    looked_up = deployment_name_lookup(deployment_name, dep)
    advice: dict[str, Any] = {
        "deployment": deployment_name,
        "deployment_dir": str(dep),
        "resolves_by_name": bool(looked_up["resolves"]),
        "searched": looked_up["searched"],
    }
    if looked_up["resolves"]:
        return advice
    if dep.name == deployment_name:
        export = f"export OKG_DEPLOYMENTS_DIR={shlex.quote(str(dep.parent))}"
        advice["export"] = export
        fix = f"run `{export}` in every shell that uses the deployment by name"
    else:
        fix = (
            f"keep the deployment at <root>/{deployment_name} and export "
            "OKG_DEPLOYMENTS_DIR=<root>"
        )
    if looked_up.get("found"):
        where = f"reaches a different deployment at {looked_up['found']}"
    else:
        where = "reaches no deployment"
    advice["warning"] = (
        f"later commands that take `--deployment {deployment_name}` will not "
        f"find {dep}: the name {where}. To use the name, {fix}; otherwise pass "
        f"`--deployment {dep}`"
    )
    return advice


def inspect_source(
    *,
    deployment: Path | str,
    source_id: str | None = None,
    apply: bool = False,
    max_records: int = 100,
) -> dict[str, Any]:
    plan = load_plan(deployment)
    selected = [
        src for src in plan.get("sources", [])
        if source_id is None or src.get("id") == source_id
    ]
    if source_id is not None and not selected:
        raise BootstrapSourceSampleFailed(
            f"source {source_id!r} is not present in bootstrap_plan.yaml",
            data={
                "source_id": source_id,
                "sample_path": None,
                "reason": "source_not_found",
            },
            fix_hint="add the source in bootstrap_plan.yaml or rerun bootstrap plan --source id=path",
        )

    updates: list[dict[str, Any]] = []
    for src in selected:
        sample = _inspect_one_source(
            plan_path(deployment).parent,
            src,
            max_records=max_records,
        )
        updates.append(sample)
        src.update({
            "sample_format": sample["sample_format"],
            "sample_fingerprint": sample["sample_fingerprint"],
            "record_count": sample["record_count"],
        })
        _replace_record_shape(plan, sample)

    report = {
        "ok": True,
        "command": "deployment bootstrap inspect-source",
        "applied": apply,
        "plan_path": str(plan_path(deployment)),
        "inspected": [u["source_id"] for u in updates],
        "record_shapes": updates,
        "status": _compute_status(plan),
    }
    if apply:
        save_plan(deployment, plan)
    return report


def draft(
    *,
    deployment: Path | str,
    apply: bool = False,
) -> dict[str, Any]:
    dep = Path(deployment).expanduser().resolve()
    plan = load_plan(dep)
    _ensure_default_authoring_plan(plan)
    artifacts = _build_artifacts(dep, plan)
    _validate_artifact_payloads(artifacts)
    writes = [str(staged_dir(dep) / rel) for rel in sorted(artifacts)]
    report = {
        "ok": True,
        "command": "deployment bootstrap draft",
        "applied": apply,
        "staged_dir": str(staged_dir(dep)),
        "proposed_writes": writes,
        "status": _compute_status(plan),
    }
    if apply:
        sdir = staged_dir(dep)
        if sdir.exists():
            shutil.rmtree(sdir)
        for rel, content in artifacts.items():
            target = sdir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                target.write_bytes(content)
            else:
                target.write_text(content, encoding="utf-8")
        hashes = _hash_tree(sdir)
        plan.setdefault("verification", {})["status"] = "unverified"
        plan["verification"]["staged_hashes"] = hashes
        plan["verification"]["checks"] = [
            {
                "name": "draft_artifacts",
                "status": "passed",
                "stage": "read-side surfacing",
                "summary": {"file_count": len(hashes)},
            }
        ]
        save_plan(dep, plan)
        report["staged_hashes"] = hashes
        report["status"] = _compute_status(plan)
    return report


def verify(
    *,
    deployment: Path | str,
    dsn: str | None = None,
    apply: bool = False,
    structural_only: bool = False,
) -> dict[str, Any]:
    dep = Path(deployment).expanduser().resolve()
    plan = load_plan(dep)
    sdir = staged_dir(dep)
    if not sdir.is_dir():
        raise BootstrapDraftInvalid(
            f"staged deployment artifacts not found: {sdir}",
            data={"path": str(sdir), "stage": "read-side surfacing", "errors": ["staged_dir_missing"]},
            fix_hint="run `okg deployment bootstrap draft --apply` first",
        )

    checks: list[dict[str, Any]] = []
    _validate_staged_artifacts(sdir)
    checks.append({
        "name": "draft_artifacts",
        "status": "passed",
        "stage": "read-side surfacing",
        "summary": {"staged_dir": str(sdir)},
    })

    from okg.substrate.sources.registry import validate_source_registry

    try:
        source_summary = validate_source_registry(sdir / "source_registry.yaml")
        bootstrap_source_summary = validate_source_registry(
            sdir / "source_registry.bootstrap.yaml",
        )
    except Exception as exc:
        raise BootstrapDraftInvalid(
            f"source_registry.yaml is invalid: {exc}",
            data={
                "path": str(sdir),
                "stage": "source coverage",
                "errors": [str(exc)],
            },
            fix_hint="fix source_registry.yaml or rerun bootstrap draft",
        ) from exc
    checks.append({
        "name": "source_registry",
        "status": "passed",
        "stage": "source coverage",
        "summary": {
            "deployment": source_summary,
            "bootstrap": bootstrap_source_summary,
        },
    })

    try:
        from okg.substrate.library.extraction.registry import load_extractors
        extractors = load_extractors(sdir)
    except Exception as exc:
        raise BootstrapDraftInvalid(
            f"extractors.yaml is invalid: {exc}",
            data={
                "path": str(sdir / "extractors.yaml"),
                "stage": "projection",
                "errors": [f"{type(exc).__name__}: {exc}"],
            },
            fix_hint="fix extractors.yaml or rerun bootstrap draft",
        ) from exc
    checks.append({
        "name": "extractors",
        "status": "passed",
        "stage": "projection",
        "summary": {
            "parser_count": len(extractors.parsers),
            "chunker_count": len(extractors.chunkers_by_format),
            "ner_route_count": len(extractors.ner_routes),
        },
    })

    try:
        from okg.substrate.catalog.modules_compose import compose_catalog
        composed = compose_catalog(sdir)
        _validate_linker_routes_against_catalog(sdir, composed)
    except SubstrateError as exc:
        stage = str(exc.data.get("stage") or "narrowing-trigger acceptance")
        raise BootstrapCatalogRejected(
            f"catalog dry-run rejected staged deployment: {exc}",
            data={
                "stage": stage,
                "path": str(sdir),
                "error": exc.to_json()["error"],
            },
            fix_hint="fix staged schemas/bridges or deployment.yaml modules",
        ) from exc
    except Exception as exc:
        raise BootstrapCatalogRejected(
            f"catalog dry-run rejected staged deployment: {type(exc).__name__}: {exc}",
            data={
                "stage": "narrowing-trigger acceptance",
                "path": str(sdir),
                "error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint="fix staged schemas/bridges, aliases, or deployment.yaml modules",
        ) from exc
    checks.append({
        "name": "catalog_dry_run",
        "status": "passed",
        "stage": "narrowing-trigger acceptance",
        "summary": {
            "subtype_count": len(composed.subtypes),
            "narrowing_count": len(composed.narrowings),
        },
    })

    if structural_only:
        summary = {
            "status": "structural_passed",
            "checks": checks,
            "staged_hashes": _hash_tree(sdir),
            "plan_hash": _hash_authoring_plan(plan),
        }
        if apply:
            plan["verification"].update(summary)
            save_plan(dep, plan)
        return {
            "ok": True,
            "command": "deployment bootstrap verify",
            "applied": apply,
            "structural_only": True,
            "verification": summary,
            "status": _compute_status(plan),
        }

    if not dsn:
        raise BootstrapPublishBlocked(
            "bootstrap verify requires a DSN for the real publish proof",
            data={
                "stage": "projection",
                "generation_id": None,
                "error": "dsn_missing",
            },
            fix_hint="pass --dsn <test-or-dev-dsn> or use --structural-only for a non-verifying dry run",
        )

    publish_summary = _run_real_publish_proof(
        dsn=dsn,
        deployment_name=str(plan["deployment"]["name"]),
        staged=sdir,
        composed=composed,
    )
    checks.extend(publish_summary.pop("checks"))

    verification = {
        "status": "verified",
        "checks": checks,
        "staged_hashes": _hash_tree(sdir),
        "plan_hash": _hash_authoring_plan(plan),
        # Which database the proof published to (host, port, name; never the
        # credentials), so apply hands the catalog over in that same one.
        "database": _database_target(dsn),
        **publish_summary,
    }
    if apply:
        plan["verification"].update(verification)
        save_plan(dep, plan)

    return {
        "ok": True,
        "command": "deployment bootstrap verify",
        "applied": apply,
        "verification": verification,
        "status": _compute_status({**plan, "verification": verification}),
    }


def apply_staged(
    *,
    deployment: Path | str,
    apply: bool = False,
    dsn: str | None = None,
) -> dict[str, Any]:
    """Copy the verified staged files into the deployment and hand it the catalog.

    Verify publishes from `.okg_bootstrap/staged`, so that copy is the
    recorded catalog writer. Apply copies the files, then claims catalog
    ownership for the deployment directory and applies its catalog, so later
    publishes and forks from the deployment directory are not refused as a
    foreign writer or an older catalog (issue #2867). The plan is marked
    `applied` only after the handover succeeds; a failed handover leaves it
    `verified`, so the same command can be run again.
    """
    dep = Path(deployment).expanduser().resolve()
    plan = load_plan(dep)
    sdir = staged_dir(dep)
    if plan.get("verification", {}).get("status") == "applied":
        raise BootstrapUnverifiedApply(
            "this deployment was already applied at "
            f"{plan['verification'].get('applied_at') or 'an earlier run'}; "
            "there is nothing new to apply",
            data={
                "status": "applied",
                "applied_at": plan["verification"].get("applied_at"),
                "changed_files": [],
                "missing_files": [],
            },
            fix_hint=(
                "to apply new staged changes, rerun `okg deployment bootstrap "
                "draft --apply` and `verify --apply` first"
            ),
        )
    if plan.get("verification", {}).get("status") != "verified":
        raise BootstrapUnverifiedApply(
            "staged deployment artifacts have not passed bootstrap verification",
            data={"changed_files": [], "missing_files": ["verification.status"]},
            fix_hint="run `okg deployment bootstrap verify --apply --dsn <dev-dsn>` first",
        )
    expected = plan.get("verification", {}).get("staged_hashes") or {}
    expected_plan_hash = plan.get("verification", {}).get("plan_hash")
    observed_plan_hash = _hash_authoring_plan(plan)
    observed = _hash_tree(sdir) if sdir.is_dir() else {}
    changed = sorted(
        rel for rel, digest in expected.items()
        if rel in observed and observed[rel] != digest
    )
    missing = sorted(rel for rel in expected if rel not in observed)
    extra_missing = [] if sdir.is_dir() else [str(sdir)]
    if changed or missing or extra_missing:
        raise BootstrapUnverifiedApply(
            "staged deployment artifacts changed after verification",
            data={
                "changed_files": changed,
                "missing_files": missing + extra_missing,
            },
            fix_hint="rerun bootstrap verify so staged hashes match the plan",
        )
    if expected_plan_hash and observed_plan_hash != expected_plan_hash:
        raise BootstrapUnverifiedApply(
            "bootstrap_plan.yaml changed after verification",
            data={
                "changed_files": ["bootstrap_plan.yaml"],
                "missing_files": [],
            },
            fix_hint="rerun bootstrap verify so the verified plan hash matches the authored plan",
        )

    writes = [str(dep / rel) for rel in sorted(expected)]
    report = {
        "ok": True,
        "command": "deployment bootstrap apply",
        "applied": apply,
        "proposed_writes": writes,
        "catalog_handover": {"planned": True, "deployment_root": str(dep)},
        "status": _compute_status(plan),
    }
    if apply:
        # Settle the database before writing anything: a DSN that is missing,
        # names a different database than verify published to, or cannot be
        # reached leaves the deployment directory untouched.
        handover_dsn = dsn or _staged_manifest_dsn(sdir)
        target = _database_target(handover_dsn)
        verified_database = plan["verification"].get("database")
        if dsn is None and verified_database and verified_database != target:
            raise BootstrapPublishBlocked(
                "bootstrap apply would hand the catalog over in a different "
                "database than verify published to",
                data={
                    "stage": "catalog handover",
                    "generation_id": None,
                    "error": "database_mismatch",
                    "verify_database": verified_database,
                    "apply_database": target,
                },
                fix_hint=(
                    "pass --dsn <the DSN verify used>; pass a different --dsn "
                    "only to hand the catalog over in that database on purpose"
                ),
            )
        _require_reachable_database(handover_dsn, target=target)
        for rel in sorted(expected):
            src = sdir / rel
            dst = dep / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
        report["catalog_handover"] = {
            **_hand_over_catalog_or_refuse(dep=dep, dsn=handover_dsn),
            "database": target,
        }
        plan["verification"]["status"] = "applied"
        plan["verification"]["applied_at"] = datetime.now(timezone.utc).isoformat()
        save_plan(dep, plan)
        report["status"] = _compute_status(plan)
    deployment_name = str((plan.get("deployment") or {}).get("name") or dep.name)
    lookup = name_lookup_advice(dep, deployment_name)
    report["name_lookup"] = lookup
    if not lookup["resolves_by_name"]:
        report["warnings"] = [lookup["warning"]]
        print(f"warning: {lookup['warning']}", file=sys.stderr)
    return report


def _staged_manifest_dsn(sdir: Path) -> str:
    """The database the staged manifest names, `${VAR}` expanded."""
    from okg.substrate.db import dsn as dsn_module

    manifest_path = sdir / "deployment.yaml"
    manifest = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8")) or {}
    raw = (manifest.get("postgres") or {}).get("dsn") if isinstance(manifest, dict) else None
    value = (
        dsn_module.expand(raw.strip(), os.environ).value
        if isinstance(raw, str) and raw.strip()
        else None
    )
    if not value:
        fix_hint = "pass --dsn <the DSN verify used>"
        if isinstance(raw, str) and raw.strip():
            fix_hint += f", or set the environment variable {raw.strip()} refers to"
        raise BootstrapPublishBlocked(
            "bootstrap apply needs the deployment's database to hand the "
            "catalog over from the verified staged copy",
            data={
                "stage": "catalog handover",
                "generation_id": None,
                "error": "dsn_missing",
                "manifest_dsn": raw,
            },
            fix_hint=fix_hint,
        )
    return value


def _database_target(dsn: str) -> dict[str, str | None]:
    """Host, port and database name a DSN names; never its credentials."""
    from okg.substrate.db import dsn as dsn_module

    params = dsn_module.parse(dsn).params
    return {
        "host": params.get("host") or params.get("hostaddr"),
        "port": params.get("port"),
        "dbname": params.get("dbname"),
    }


def _require_reachable_database(dsn: str, *, target: dict[str, Any]) -> None:
    """Refuse, before any file is copied, when the handover database is unusable."""
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(dsn, connect_timeout=5) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.graph_generations')")
            migrated = cur.fetchone()[0] is not None
    except Exception as exc:  # noqa: BLE001 - every cause is reported, coded
        raise BootstrapPublishBlocked(
            "bootstrap apply cannot reach the database to hand the catalog "
            f"over: {type(exc).__name__}",
            data={
                "stage": "catalog handover",
                "generation_id": None,
                "error": "database_unreachable",
                "database": target,
                "detail": _redacted_error(exc, dsn),
            },
            fix_hint=(
                "start the database or pass --dsn <the DSN verify used>, then "
                "rerun the same apply; nothing was copied"
            ),
        ) from None
    if not migrated:
        raise BootstrapPublishBlocked(
            "bootstrap apply cannot hand the catalog over: the database has "
            "no okg schema",
            data={
                "stage": "catalog handover",
                "generation_id": None,
                "error": "schema_not_migrated",
                "database": target,
            },
            fix_hint="pass --dsn <the DSN verify used>; nothing was copied",
        )


def _redacted_error(exc: BaseException, dsn: str) -> str:
    from okg.substrate.db import dsn as dsn_module

    text = f"{type(exc).__name__}: {exc}"
    return dsn_module.redact(text.replace(dsn, dsn_module.redact(dsn)))


def _hand_over_catalog_or_refuse(*, dep: Path, dsn: str) -> dict[str, Any]:
    """Run the handover; a database failure is a coded refusal, not a traceback.

    Ownership refusals already carry their own `catalog_ownership_*` code and
    pass through unchanged. The files are copied by now, and the plan stays
    `verified`, so the same apply can be run again after the cause is fixed.
    """
    import psycopg

    try:
        return _hand_over_catalog(dep=dep, dsn=dsn)
    except psycopg.Error as exc:
        raise BootstrapCatalogRejected(
            "bootstrap apply copied the files but could not hand the catalog "
            f"over: {type(exc).__name__}",
            data={
                "stage": "catalog handover",
                "error": _redacted_error(exc, dsn),
                "files_copied": True,
                "plan_status": "verified",
            },
            fix_hint="fix the cause, then rerun the same apply",
        ) from None


def _hand_over_catalog(*, dep: Path, dsn: str) -> dict[str, Any]:
    """Make the applied deployment directory the catalog writer (issue #2867).

    Verify published from `.okg_bootstrap/staged`, so the recorded owner is
    that staged copy. When the recorded owner is exactly this deployment's
    own staged copy, apply takes ownership over from it explicitly: those
    are the files apply just copied into place, so the applied directory is
    the same writer moving to its final location, and the report says so
    (`previous_owner.is_this_deployments_staged_copy`). Any other owner goes through
    the ordinary claim rules and still refuses with its own
    `catalog_ownership_*` error; the plan then stays `verified` so apply can
    be run again. The catalog is then applied as this directory, which
    records the catalog version its publishes build on.
    """
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
        diagnose_catalog_ownership,
        ensure_catalog_write_allowed,
        owner_is_stale,
    )
    from okg.substrate.db.connection import connect as db_connect

    composed = compose_catalog(dep)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=dep,
        command="okg deployment bootstrap apply",
    )
    staged = staged_dir(dep).resolve()
    with db_connect(dsn, autocommit=True) as conn:
        before = diagnose_catalog_ownership(conn, identity=identity)
        previous = before.get("owner") or {}
        previous_root = (previous.get("metadata") or {}).get("deployment_root")
        # The one owner apply takes over by design: this deployment's own
        # verified staged copy. Anything else goes through the normal rules.
        # (The row is looked up by this deployment's name, so the name
        # already matches; the staged path is what identifies the owner.)
        from_own_staged_copy = (
            isinstance(previous_root, str)
            and Path(previous_root).resolve() == staged
        )
        claimed = claim_catalog_ownership(
            conn, identity=identity, force=from_own_staged_copy,
        )
        ensure_catalog_write_allowed(conn, identity=identity)
        catalog_version_id = apply_composed_catalog(
            conn, composed, identity=identity,
        )
    owner = claimed.get("owner") or {}
    previous_owner = None
    if previous:
        stale, stale_reason = owner_is_stale(previous)
        previous_owner = {
            "deployment_root": previous_root,
            "is_this_deployments_staged_copy": from_own_staged_copy,
            "claimed_by_command": previous.get("claimed_by_command"),
            "stale": stale,
            "stale_reason": stale_reason,
        }
    return {
        "planned": True,
        "claimed": True,
        "deployment_root": str(dep),
        "owner_root": (owner.get("metadata") or {}).get("deployment_root"),
        "previous_owner": previous_owner,
        "catalog_version_id": catalog_version_id,
    }


def status(*, deployment: Path | str) -> dict[str, Any]:
    dep = Path(deployment).expanduser().resolve()
    try:
        plan = load_plan(dep)
    except BootstrapPlanInvalid as exc:
        scorecard = _deployment_artifact_scorecard(dep)
        if (
            scorecard["artifacts"]["deployment_yaml"]
            and not plan_path(dep).is_file()
        ):
            return {
                "ok": True,
                "stage": "needs_bootstrap_plan",
                "blocking_stage": "source coverage",
                "last_failure_code": exc.code,
                "next_action": {
                    "edit": str(plan_path(dep)),
                    "command": "okg deployment bootstrap plan --deployment <path> --brief <file> --apply",
                },
                "scorecard": scorecard,
            }
        return {
            "ok": False,
            "stage": "needs_plan",
            "blocking_stage": "source coverage",
            "last_failure_code": exc.code,
            "next_action": {
                "edit": str(plan_path(deployment)),
                "command": "okg deployment bootstrap plan --deployment <path> --brief <file> --apply",
            },
            "scorecard": scorecard,
        }
    return {"ok": True, **_compute_status(plan, deployment_dir=dep)}


def fixture_run_mode(entry: Any, strict_binding: Any = None) -> str:
    """The mode a bootstrap fixture sample runs in: the profile's whole scope.

    A strict source runs under its bound executable profile, and the runner
    refuses a mode that profile has no policy for, so a `reference_catalog`,
    `mutable_api` or `append_feed` source handed `scope_complete` failed
    verification (issue #2486). Other sources resolve by their declared
    profile, exactly as `okg ingest` does.
    """
    profile = (
        strict_binding.executable_profile
        if strict_binding is not None
        else getattr(entry, "source_class", None)
    )
    return resolve_whole_scope_request(profile, WHOLE_SCOPE_REQUEST)


def _run_real_publish_proof(
    *,
    dsn: str,
    deployment_name: str,
    staged: Path,
    composed: Any,
) -> dict[str, Any]:
    """Run the fixture sources and publish while holding the deployment's
    source-sync signal, so no other publish sees a partial run (#3003)."""
    from okg.substrate.runtime.source_sync_priority import ingest_run_guard

    with ingest_run_guard(
        dsn=dsn,
        deployment=deployment_name,
        deployment_dir=staged,
        hold=True,
        wait_seconds=None,
    ):
        return _run_real_publish_proof_held(
            dsn=dsn,
            deployment_name=deployment_name,
            staged=staged,
            composed=composed,
        )


def _run_real_publish_proof_held(
    *,
    dsn: str,
    deployment_name: str,
    staged: Path,
    composed: Any,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        ensure_catalog_write_allowed,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.ingest.adapter_factory import (
        bind_source_adapter_identity,
        construct_source_adapter_from_plan,
        plan_source_registry_bindings,
    )
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.sources.registry import load_source_registry_admission

    admission = load_source_registry_admission(
        staged / "source_registry.bootstrap.yaml"
    )
    strict_bindings = {}
    if admission.strict:
        plans = plan_source_registry_bindings(
            admission,
            dsn=dsn,
            deployment=deployment_name,
        ).plans
        strict_bindings = {
            name: construct_source_adapter_from_plan(plan)
            for name, plan in plans.items()
        }

    checks: list[dict[str, Any]] = []
    doctor = _doctor_connectivity_check(dsn)
    checks.append({
        "name": "doctor",
        "status": "passed",
        "stage": "source coverage",
        "summary": doctor,
    })
    store = ChronosGraphStore.connect(dsn)
    try:
        graph = store.checkout()
        before_nodes = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        before_edges = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    finally:
        store.close()
    catalog_identity = catalog_identity_for(
        composed=composed,
        deployment_dir=staged,
        command="okg deployment bootstrap verify",
    )
    with db_connect(dsn, autocommit=True) as conn:
        ensure_catalog_write_allowed(conn, identity=catalog_identity)
        catalog_version_id = apply_composed_catalog(
            conn,
            composed,
            identity=catalog_identity,
        )
    checks.append({
        "name": "catalog_apply",
        "status": "passed",
        "stage": "narrowing-trigger acceptance",
        "summary": {"catalog_version_id": catalog_version_id},
    })

    source_runs: list[dict[str, Any]] = []
    registry = admission.entries
    # PACT first-class-images (#3046): fixture ingest renders file image
    # values at the staged manifest's `agent_surface.images` settings.
    from okg.substrate.images.render import images_config_for_manifest

    images_config = images_config_for_manifest(
        fastyaml.safe_load(
            (staged / "deployment.yaml").read_text(encoding="utf-8")
        ) or {},
        manifest_path=staged / "deployment.yaml",
    )
    for name, entry in registry.items():
        if entry.legacy or entry.is_registry_only:
            continue
        raw = dict(entry.raw)
        params = dict(raw.get("params") or {})
        fixture_path = params.get("fixture_path")
        if fixture_path:
            fpath = Path(str(fixture_path))
            fallback = staged / "fixtures" / fpath.name
            if fallback.exists():
                params["fixture_path"] = str(fallback)
        try:
            strict_binding = strict_bindings.get(name)
            from okg.substrate.admission import (
                graph_branch_for_admission_mode,
            )

            admission_mode = (
                entry.admission_policy.admission_mode
                if entry.admission_policy is not None
                else None
            )
            graph_branch = graph_branch_for_admission_mode(admission_mode)
            if strict_binding is not None:
                adapter = strict_binding.adapter
                graph_branch = strict_binding.graph_branch
                registry_binding_fingerprint = (
                    strict_binding.registry_binding_fingerprint
                )
            else:
                module = importlib.import_module(str(raw["module"]))
                cls = getattr(module, str(raw["class"]))
                try:
                    sig = inspect.signature(cls)
                    if (
                        "source_name" in sig.parameters
                        and "source_name" not in params
                    ):
                        params["source_name"] = entry.ownership_id
                    if (
                        "graph_branch" in sig.parameters
                        and "graph_branch" not in params
                    ):
                        params["graph_branch"] = graph_branch
                except (TypeError, ValueError):
                    # Some adapter factories are not introspectable.
                    pass
                adapter = cls(**params)
                registry_binding_fingerprint = bind_source_adapter_identity(
                    adapter,
                    entry,
                )

            # `bind-at-rest-anonymization`: a not-store-raw source's bootstrap
            # fixture sample is transformed at admission too. Its generated,
            # bounded deferral retains compatibility only after portfolio DSN audit.
            from okg.substrate.ingest.pii_posture import (
                resolve_deployment_source_pii_posture,
            )
            from okg.substrate.nomos.rollout import runtime_enforce_mode_dsn_keys

            result = SourceRunner(
                dsn,
                source_name=entry.ownership_id,
                deployment_name=deployment_name,
                graph_branch=graph_branch,
                pii_posture=resolve_deployment_source_pii_posture(
                    manifest=fastyaml.safe_load(
                        (staged / "deployment.yaml").read_text(encoding="utf-8")
                    ) or {},
                    source_name=name,
                    source_entry=entry.raw,
                    enforce_dsn_keys=runtime_enforce_mode_dsn_keys(staged),
                ),
                deletion_semantics=entry.deletion_semantics,
                executable_profile=(
                    strict_binding.executable_profile
                    if strict_binding is not None else None
                ),
                registry_name=(
                    strict_binding.registry_name
                    if strict_binding is not None else None
                ),
                ownership_id=(
                    strict_binding.ownership_id
                    if strict_binding is not None else None
                ),
                registry_binding_fingerprint=(
                    registry_binding_fingerprint
                ),
                images_config=images_config,
            ).run(adapter, mode=fixture_run_mode(entry, strict_binding))
        except Exception as exc:
            raise BootstrapSourceSampleFailed(
                f"bootstrap source {name!r} failed during fixture ingest: {exc}",
                data={
                    "stage": "source coverage",
                    "source_id": name,
                    "sample_path": str(params.get("fixture_path")),
                    "reason": f"{type(exc).__name__}: {exc}",
                },
                fix_hint="fix the generated fixture source or rerun inspect-source/draft",
            ) from exc
        source_runs.append(result.as_dict())
    checks.append({
        "name": "fixture_ingest",
        "status": "passed",
        "stage": "source coverage",
        "summary": {"source_count": len(source_runs)},
    })

    try:
        publish = build_deployment_publisher(
            dsn=dsn,
            deployment_name=deployment_name,
            deployment_dir=staged,
            command="okg deployment bootstrap verify",
        ).cycle()
    except SubstrateError as exc:
        exc_json = exc.to_json()["error"]
        stage = (
            "narrowing-trigger acceptance"
            if exc_json.get("code") == "narrowing_violation"
            else "projection"
        )
        raise BootstrapPublishBlocked(
            f"bootstrap publish failed: {exc}",
            data={
                "stage": stage,
                "generation_id": None,
                "error": exc_json,
            },
            fix_hint="inspect the source facts and staged narrowings, then rerun bootstrap verify",
        ) from exc
    except Exception as exc:
        message = str(exc)
        stage = (
            "narrowing-trigger acceptance"
            if "narrowing_violation" in message
            else "projection"
        )
        raise BootstrapPublishBlocked(
            f"bootstrap publish failed: {type(exc).__name__}: {exc}",
            data={
                "stage": stage,
                "generation_id": None,
                "error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint="inspect the source facts and staged narrowings, then rerun bootstrap verify",
        ) from exc

    if publish.get("status") != "published":
        raise BootstrapPublishBlocked(
            f"bootstrap generation {publish.get('generation_id')} was not published",
            data={
                "stage": "gate evaluation",
                "generation_id": publish.get("generation_id"),
                "error": publish,
            },
            fix_hint="run `okg generation describe <g>` and fix the blocking invariant",
        )
    checks.append({
        "name": "publish",
        "status": "passed",
        "stage": "projection",
        "summary": publish,
    })

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        published_graph = store.checkout_generation(publish["generation_id"])
        after_nodes = int(
            published_graph.query(
                "SELECT count(*) AS n FROM okg.graph_nodes"
            )[0]["n"]
        )
        after_edges = int(
            published_graph.query(
                "SELECT count(*) AS n FROM okg.graph_edges"
            )[0]["n"]
        )
    finally:
        store.close()
    from okg.substrate.publish_proof import dlq_count as _dlq_count

    dlq_count, queue_depth = _dlq_count(dsn)
    if after_nodes <= before_nodes or after_edges <= before_edges:
        raise BootstrapPublishBlocked(
            "bootstrap publish did not increase live node and edge rows",
            data={
                "stage": "projection",
                "generation_id": publish.get("generation_id"),
                "error": {
                    "before_nodes": before_nodes,
                    "after_nodes": after_nodes,
                    "before_edges": before_edges,
                    "after_edges": after_edges,
                },
            },
            fix_hint="check that fixture facts use subtypes and edge types accepted by the staged catalog",
        )
    if dlq_count:
        raise BootstrapPublishBlocked(
            "bootstrap verification found dead-letter runtime work",
            data={
                "stage": "read-side surfacing",
                "generation_id": publish.get("generation_id"),
                "error": {"queue_depth": queue_depth},
            },
            fix_hint="inspect DBOS workflow failures and fix the source or runtime configuration",
        )

    checks.append({
        "name": "metrics",
        "status": "passed",
        "stage": "read-side surfacing",
        "summary": {"queue_depth": queue_depth, "dlq_count": dlq_count},
    })
    return {
        "checks": checks,
        "generation_id": str(publish["generation_id"]),
        "catalog_version_id": int(publish["catalog_version_id"]),
        "source_runs": source_runs,
        "node_row_delta": after_nodes - before_nodes,
        "edge_row_delta": after_edges - before_edges,
        "metrics_snapshot": {
            "queue_depth": queue_depth,
            "dlq_count": dlq_count,
        },
        "doctor": doctor,
    }


def _doctor_connectivity_check(dsn: str) -> dict[str, Any]:
    """Small bootstrap doctor check.

    The full `okg doctor` host check covers deployment-local services
    that are outside this bootstrap proof. The proof only needs a DB
    that is migrated enough to publish, so this check verifies
    connectivity and the core ledger table before the destructive parts
    of the proof run.
    """
    from okg.substrate.db.connection import connect as db_connect

    try:
        with db_connect(dsn, connect_timeout=2) as conn, conn.cursor() as cur:
            cur.execute("SELECT to_regclass('okg.graph_generations')")
            graph_generations = cur.fetchone()[0]
    except Exception as exc:
        raise BootstrapPublishBlocked(
            f"bootstrap doctor check failed: {type(exc).__name__}: {exc}",
            data={
                "stage": "source coverage",
                "generation_id": None,
                "error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint="run `okg migrate --apply` against the bootstrap DSN, then rerun verify",
        ) from exc
    if graph_generations is None:
        raise BootstrapPublishBlocked(
            "bootstrap doctor check failed: okg.graph_generations is missing",
            data={
                "stage": "source coverage",
                "generation_id": None,
                "error": "schema_not_migrated",
            },
            fix_hint="run `okg migrate --apply` against the bootstrap DSN, then rerun verify",
        )
    return {"ok": True, "check": "dsn_connectivity", "core_tables": ["okg.graph_generations"]}


def _empty_plan(
    *,
    deployment_name: str,
    deployment_path: Path,
    template_base: str,
    dsn_env: str,
    brief_text: str,
    sources: list[SourceDescriptor],
) -> dict[str, Any]:
    return {
        "version": 1,
        "deployment": {
            "name": deployment_name,
            "path": str(deployment_path),
            "template_base": template_base,
            "description": _first_sentence(brief_text),
            "postgres": {"dsn": f"${{{dsn_env}}}"},
        },
        "domain_brief": {
            "text": brief_text,
            "example_questions": _extract_questions(brief_text),
        },
        "sources": [
            {
                "id": d.source_id,
                "kind": d.kind,
                "sample_path": _display_path(d.path, deployment_path),
                "allow_live_sampling": False,
            }
            for d in sources
        ],
        "record_shapes": [],
        "ontology_plan": {"library_modules": [], "subtypes": []},
        "narrowing_plan": [],
        "extractor_plan": {
            "entity_patterns": [],
            "linker_routes": [],
            "aliases": [],
        },
        "view_plan": {"views": []},
        "quality_plan": {"invariants": [], "fixture_assertions": []},
        "verification": {"status": "unverified", "checks": []},
    }


def _inspect_one_source(
    deployment_dir: Path,
    src: dict[str, Any],
    *,
    max_records: int,
) -> dict[str, Any]:
    source_id = str(src["id"])
    kind = str(src.get("kind") or "")
    raw_path = src.get("sample_path")
    if kind in {"api", "database"} and not src.get("allow_live_sampling"):
        raise BootstrapSourceSampleFailed(
            f"source {source_id!r} has no bounded sample fixture",
            data={
                "source_id": source_id,
                "sample_path": raw_path,
                "reason": "live_sampling_not_enabled",
            },
            fix_hint="provide sample_path or set allow_live_sampling only after confirming a bounded sampler",
        )
    if not raw_path:
        raise BootstrapSourceSampleFailed(
            f"source {source_id!r} has no sample_path",
            data={
                "source_id": source_id,
                "sample_path": None,
                "reason": "sample_path_missing",
            },
            fix_hint="add sample_path to bootstrap_plan.yaml",
        )
    path = _resolve_sample_path(deployment_dir, str(raw_path))
    records, sample_format, fingerprint = _read_records(path, kind, max_records=max_records)
    fields, ids, texts = _summarize_records(records)
    return {
        "source_id": source_id,
        "sample_format": sample_format,
        "sample_fingerprint": fingerprint,
        "record_count": len(records),
        "fields": fields,
        "candidate_identifier_fields": ids,
        "candidate_text_fields": texts,
        "examples": records[:3],
    }


def _read_records(
    path: Path,
    kind: str,
    *,
    max_records: int,
) -> tuple[list[dict[str, Any]], str, str]:
    if not path.exists():
        raise BootstrapSourceSampleFailed(
            f"sample path does not exist: {path}",
            data={
                "source_id": "",
                "sample_path": str(path),
                "reason": "sample_path_not_found",
            },
            fix_hint="point sample_path at an existing bounded fixture",
        )
    if path.is_dir():
        records: list[dict[str, Any]] = []
        for child in sorted(path.rglob("*")):
            if len(records) >= max_records:
                break
            if not child.is_file() or child.suffix.lower() not in SUPPORTED_SUFFIXES:
                continue
            child_records, _, _ = _read_records(
                child,
                SUPPORTED_SUFFIXES[child.suffix.lower()],
                max_records=max_records - len(records),
            )
            for record in child_records:
                record.setdefault("path", str(child.relative_to(path)))
                records.append(record)
                if len(records) >= max_records:
                    break
        return records, "directory", _hash_directory(path)

    suffix = path.suffix.lower()
    effective_kind = kind if kind != "local_directory" else SUPPORTED_SUFFIXES.get(suffix, "")
    if effective_kind == "local_json" or suffix == ".json":
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise BootstrapSourceSampleFailed(
                f"JSON sample parse failed: {exc}",
                data={
                    "source_id": "",
                    "sample_path": str(path),
                    "reason": "json_parse_failed",
                },
                fix_hint="fix the JSON sample or point at a valid fixture",
            ) from exc
        return _records_from_json(data, max_records), "json", _hash_file(path)
    if effective_kind == "local_csv" or suffix == ".csv":
        with path.open(newline="", encoding="utf-8") as f:
            rows = [dict(row) for _, row in zip(range(max_records), csv.DictReader(f))]
        return rows, "csv", _hash_file(path)
    if effective_kind == "local_markdown" or suffix in {".md", ".markdown"}:
        text = path.read_text(encoding="utf-8")
        title = _first_markdown_heading(text) or path.stem
        return [{"path": path.name, "title": title, "text": text}], "markdown", _hash_file(path)
    raise BootstrapSourceSampleFailed(
        f"unsupported sample format for {path}",
        data={
            "source_id": "",
            "sample_path": str(path),
            "reason": "unsupported_sample_format",
        },
        fix_hint="use JSON, CSV, Markdown, or a directory containing those files",
    )


def _records_from_json(data: Any, max_records: int) -> list[dict[str, Any]]:
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = None
        for key in ("records", "items", "rows", "nodes"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break
        if rows is None:
            rows = [data]
    else:
        rows = [{"value": data}]
    out: list[dict[str, Any]] = []
    for row in rows[:max_records]:
        if isinstance(row, dict):
            out.append(dict(row))
        else:
            out.append({"value": row})
    return out


def _summarize_records(records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    counts: dict[str, int] = defaultdict(int)
    types: dict[str, set[str]] = defaultdict(set)
    examples: dict[str, Any] = {}
    uniques: dict[str, set[str]] = defaultdict(set)
    for record in records:
        for key, value in record.items():
            counts[key] += 1
            types[key].add(_json_type(value))
            examples.setdefault(key, value)
            if isinstance(value, (str, int, float, bool)) or value is None:
                uniques[key].add(str(value))
    denom = max(len(records), 1)
    fields = [
        {
            "name": key,
            "types": sorted(types[key]),
            "count": counts[key],
            "frequency": round(counts[key] / denom, 4),
            "unique_count": len(uniques.get(key, set())),
            "example": examples.get(key),
        }
        for key in sorted(counts)
    ]
    identifier_fields = [
        f["name"] for f in fields
        if _looks_like_identifier(f["name"], f.get("unique_count", 0), len(records))
    ]
    text_fields = [
        f["name"] for f in fields
        if _looks_like_text_field(f["name"], f.get("example"))
    ]
    return fields, identifier_fields[:8], text_fields[:8]


def _deployment_postgres_block(plan_postgres: Any) -> dict[str, Any]:
    """The deployment.yaml `postgres` block for a plan's `postgres` block.

    A plan may name the DSN's environment variable (`dsn_env`); the loader
    reads only `dsn`, and refuses any other key (#2634), so it becomes a
    `${NAME}` reference.
    """
    if not plan_postgres:
        return {"dsn": "${OKG_DSN}"}
    block = dict(plan_postgres)
    dsn_env = block.pop("dsn_env", None)
    if dsn_env is not None:
        block["dsn"] = f"${{{dsn_env}}}"
    return block


def _build_artifacts(dep: Path, plan: dict[str, Any]) -> dict[str, str | bytes]:
    from okg.substrate.sources.guided_defaults import EXTRACTORS_YAML

    name = str(plan["deployment"]["name"])
    review_by = (datetime.now(timezone.utc).date() + timedelta(days=30)).isoformat()
    modules = list(dict.fromkeys(plan["ontology_plan"].get("library_modules") or []))
    for required in (
        "git_graph",
        "openspec",
        "extraction",
        "agent_memory",
        "reasoning",
    ):
        if required not in modules:
            modules.append(required)
    (
        source_registry,
        bootstrap_source_registry,
        fixture_payloads,
    ) = _source_registries_and_fixtures(dep, plan)
    invariants_doc = {"invariants": plan.get("quality_plan", {}).get("invariants") or []}
    artifacts: dict[str, str | bytes] = {
        "deployment.yaml": _dump_yaml({
            "name": name,
            "description": plan["deployment"].get("description") or plan["domain_brief"]["text"][:120],
            "postgres": _deployment_postgres_block(plan["deployment"].get("postgres")),
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "modules": modules,
            # Free-disk floors, in percent available, as the install profiles
            # write them: without them doctor reports its own disk check as
            # misconfigured (issue #3578).
            "host_storage": {"warn_available_pct": 15, "blocker_available_pct": 6},
            # Bootstrap-authored deployments start in an explicit compatibility
            # rollout. Their fixture sources retain store-raw behavior until an
            # operator authors a deployment posture and per-source policy, rather
            # than relying on an implicit missing-policy fallback.
            "nomos": {
                "rollout": {
                    "status": "deferred",
                    "reason": (
                        "Bootstrap fixture sources retain raw compatibility until "
                        "the operator authors deployment and per-source posture."
                    ),
                    "owner": {
                        "name": f"{name}-bootstrap",
                        "contact": f"{name}-owner@okg.example",
                    },
                    "review_by": review_by,
                }
            },
        }),
        "source_registry.yaml": _dump_yaml(source_registry),
        "source_registry.bootstrap.yaml": _dump_yaml(bootstrap_source_registry),
        "schemas/bootstrap_domain.yaml": _dump_yaml(_domain_schema(plan)),
        "schemas/bridges/bootstrap_bridges.yaml": _dump_yaml(_bridge_schema(plan)),
        # The packaged file readers (Markdown, text, JSON, YAML), so a folder
        # or upload source added later reads file contents, not only names
        # (#2974). The bootstrap fixture source itself reads no extractors.
        "extractors.yaml": EXTRACTORS_YAML.read_text(encoding="utf-8"),
        "linker_config.yaml": _dump_yaml(_linker_config(plan)),
        "identifier_patterns.yaml": _dump_yaml(_identifier_patterns(plan)),
        "aliases.json": json.dumps(
            _aliases_json(plan),
            indent=2,
            sort_keys=True,
        ) + "\n",
        "invariants.yaml": _dump_yaml(invariants_doc),
        "invariants/bootstrap.yaml": _dump_yaml(invariants_doc),
        "views_registry.yaml": _dump_yaml({"deployment": name, "views": plan.get("view_plan", {}).get("views") or []}),
    }
    for source_id, payload in fixture_payloads.items():
        artifacts[f"fixtures/{source_id}.json"] = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    return artifacts


def _source_registries_and_fixtures(
    dep: Path,
    plan: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    name = str(plan["deployment"]["name"])
    record_subtype = _snake(_primary_subtype_names(plan)[0])
    finding_subtype = _snake(_primary_subtype_names(plan)[1])
    sources: dict[str, Any] = {}
    bootstrap_sources: dict[str, Any] = {}
    fixtures: dict[str, dict[str, Any]] = {}
    for src in plan.get("sources", []):
        source_id = str(src["id"])
        fixture_path = dep / "fixtures" / f"{source_id}.json"
        sources[source_id] = _production_source_registry_entry(
            deployment_name=name,
            source_id=source_id,
            source=src,
            fixture_path=fixture_path,
        )
        bootstrap_sources[source_id] = {
            "module": "okg.substrate.library.sources.bootstrap_fixture",
            "class": "BootstrapFixtureSource",
            "ownership_id": f"{name}.{source_id}",
            "params": {
                "fixture_path": str(fixture_path),
            },
            "source_class": "discovery_crawl",
            "record_identity_kind": "scoped_locator",
            "record_identity_fields": ["source_id", "record_id"],
            "source_revision_kind": "content_hash",
            "deletion_semantics": "missing_from_completed_scope",
            "publication_mode": "published_generation",
        }
        records, _, _ = _read_records(
            _resolve_sample_path(dep, str(src["sample_path"])),
            str(src["kind"]),
            max_records=100,
        )
        fixtures[source_id] = _fact_fixture_for_records(
            source_id=f"{name}:{source_id}",
            records=records,
            record_subtype=record_subtype,
            finding_subtype=finding_subtype,
        )
    return {"sources": sources}, {"sources": bootstrap_sources}, fixtures


def _production_source_registry_entry(
    *,
    deployment_name: str,
    source_id: str,
    source: dict[str, Any],
    fixture_path: Path,
) -> dict[str, Any]:
    adapter = source.get("adapter") or {}
    profile = source.get("profile") or {}
    if adapter:
        module = str(adapter.get("module") or "")
        class_name = str(adapter.get("class") or "")
        params = dict(source.get("params") or {})
    else:
        module = "okg.substrate.library.sources.bootstrap_fixture"
        class_name = "BootstrapFixtureSource"
        params = {"fixture_path": str(fixture_path)}

    entry = {
        "module": module,
        "class": class_name,
        "ownership_id": str(source.get("ownership_id") or f"{deployment_name}.{source_id}"),
        **_default_source_profile(source_id=source_id, profile=profile),
    }
    if params:
        entry["params"] = params
    # No per-source `schedule` (issue #2952): the strict_v1 registry contract
    # refuses it (a scheduled refresh is the entry's `sync.poll` block run by
    # the deployment runtime), and no runtime code ever read it. `load_plan` refuses a plan
    # that still authors one rather than dropping it here unannounced.
    return entry


def _default_source_profile(*, source_id: str, profile: dict[str, Any]) -> dict[str, Any]:
    if profile:
        return dict(profile)
    return {
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["source_id", "record_id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }


def _linker_config(plan: dict[str, Any]) -> dict[str, Any]:
    routes: dict[str, Any] = {}
    for route in plan.get("extractor_plan", {}).get("linker_routes") or []:
        entity_type = str(route["entity_type"])
        simple = {
            "edge_type": str(route["edge_type"]),
            "dst_template": str(route["dst_template"]),
        }
        if "confidence" in route:
            simple["confidence"] = float(route["confidence"])
        src_subtype = route.get("src_subtype")
        if src_subtype:
            existing = routes.setdefault(entity_type, {})
            if not isinstance(existing, dict) or "edge_type" in existing:
                raise BootstrapDraftInvalid(
                    f"linker route {entity_type!r} mixes simple and subtype-scoped routes",
                    data={
                        "stage": "enrichment",
                        "entity_type": entity_type,
                    },
                    fix_hint=(
                        "keep either one default route or only "
                        "src_subtype-specific routes for each entity_type"
                    ),
                )
            existing[_subtype_class_name(str(src_subtype))] = simple
        else:
            if entity_type in routes:
                raise BootstrapDraftInvalid(
                    f"duplicate linker route for entity_type {entity_type!r}",
                    data={
                        "stage": "enrichment",
                        "entity_type": entity_type,
                    },
                    fix_hint="merge duplicate linker routes or set src_subtype",
                )
            routes[entity_type] = simple
    return {"routes": routes}


def _aliases_json(plan: dict[str, Any]) -> dict[str, Any]:
    aliases: dict[str, Any] = {}
    for entry in plan.get("extractor_plan", {}).get("aliases") or []:
        alias = str(entry["alias"])
        canonical = str(entry["canonical"])
        scope = entry.get("scope")
        if scope:
            aliases[alias] = {"canonical": canonical, "scope": str(scope)}
        else:
            aliases[alias] = canonical
    return aliases


def _identifier_patterns(plan: dict[str, Any]) -> dict[str, Any]:
    patterns: list[dict[str, Any]] = []
    for entry in plan.get("extractor_plan", {}).get("entity_patterns") or []:
        pattern = {
            "id_type": str(entry["id_type"]),
            "regex": str(entry["regex"]),
        }
        for key in ("validator_kind", "description"):
            if entry.get(key) is not None:
                pattern[key] = str(entry[key])
        patterns.append(pattern)
    return {"patterns": patterns}


def _fact_fixture_for_records(
    *,
    source_id: str,
    records: list[dict[str, Any]],
    record_subtype: str,
    finding_subtype: str,
) -> dict[str, list[dict[str, Any]]]:
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for idx, record in enumerate(records or [{"id": "empty", "text": "empty bootstrap record"}], start=1):
        rid = _record_identity(record, idx)
        text = _record_text(record)
        title = _record_title(record, rid)
        # Issue #2991: the record body goes into `body`, which the default
        # record and finding subtypes declare searchable beside the title
        # or label. It is not also written into `text`: the stamper would
        # keep a second copy of it, and `text` is the searchable rollup the
        # stamper builds from `title` and `body`.
        body = _record_body(record, title)
        record_node = f"{source_id}:record:{_slug(rid)}"
        finding_node = f"{source_id}:finding:{_slug(rid)}"
        doc_node = f"doc:{source_id}:{_slug(rid)}"
        chunk_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        chunk_node = f"chunk:{chunk_digest[:16]}"
        nodes.extend([
            {
                "node_id": record_node,
                "subtype": record_subtype,
                "attrs": {
                    "record_id": rid,
                    "title": title,
                    "body": body,
                    "source_id": source_id,
                    "raw": record,
                },
                "source_record_id": {"source": source_id, "record_id": rid, "kind": "record"},
                "source_revision": {"content_sha256": _hash_json(record)},
            },
            {
                "node_id": finding_node,
                "subtype": finding_subtype,
                "attrs": {
                    "finding_id": f"{rid}:shape",
                    "label": f"{source_id} sample record",
                    "body": body[:500],
                    "source_id": source_id,
                },
                "source_record_id": {"source": source_id, "record_id": rid, "kind": "finding"},
                "source_revision": {"content_sha256": _hash_json(record)},
            },
            {
                "node_id": doc_node,
                "subtype": "document",
                "attrs": {
                    "doc_id": doc_node,
                    "repo": source_id,
                    "path": str(record.get("path") or f"{rid}.sample"),
                    "format": "sample",
                    "byte_count": len(text.encode("utf-8")),
                    "content_sha256": chunk_digest,
                    "title": title,
                    "text": text,
                },
                "source_record_id": {"source": source_id, "record_id": rid, "kind": "document"},
                "source_revision": {"content_sha256": chunk_digest},
            },
            {
                "node_id": chunk_node,
                "subtype": "document_chunk",
                "attrs": {
                    "chunk_id": chunk_node,
                    "content_sha256": chunk_digest,
                    "text": text,
                    "char_offset": 0,
                    "char_length": len(text),
                    "chunker_name": "bootstrap_sample",
                },
                "source_record_id": {"source": source_id, "record_id": rid, "kind": "chunk"},
                "source_revision": {"content_sha256": chunk_digest},
            },
        ])
        edges.extend([
            {"src": record_node, "edge_type": "contains", "dst": finding_node},
            {"src": record_node, "edge_type": "references", "dst": doc_node},
            {"src": doc_node, "edge_type": "references", "dst": record_node},
            {"src": doc_node, "edge_type": "contains", "dst": chunk_node},
            {"src": chunk_node, "edge_type": "member_of", "dst": doc_node},
        ])
    return {"nodes": nodes, "edges": edges}


def _domain_schema(plan: dict[str, Any]) -> dict[str, Any]:
    classes: dict[str, Any] = {}
    for subtype in _planned_subtype_defs(plan):
        name = _subtype_class_name(str(subtype["name"]))
        attrs = {
            str(attr): ({"required": True} if _looks_required_attr(str(attr)) else {})
            for attr in subtype.get("attrs", [])
        }
        annotations = {
            "archetype": str(subtype.get("archetype") or "artifact"),
            "transient": "false",
        }
        text_fields = subtype.get("searchable_text_fields") or [
            a for a in attrs
            if _looks_like_text_field(a, a)
        ]
        if text_fields:
            annotations["okg_searchable_text_fields"] = ",".join(
                str(f) for f in text_fields
            )
        alias_fields = subtype.get("alias_fields") or []
        if alias_fields:
            annotations["okg_alias_fields"] = ",".join(
                str(f) for f in alias_fields
            )
        classes[name] = {
            "description": (
                subtype.get("description")
                or "Bootstrap-generated deployment subtype."
            ),
            "attributes": attrs,
            "annotations": annotations,
        }
    return {
        "id": f"https://palimpzest.mit.edu/okg/deployments/{plan['deployment']['name']}/bootstrap_domain",
        "name": f"{plan['deployment']['name']}_bootstrap_domain",
        "prefixes": {
            "linkml": "https://w3id.org/linkml/",
            "okg": "https://palimpzest.mit.edu/okg/",
        },
        "default_range": "string",
        "imports": ["linkml:types"],
        "classes": classes,
    }


def _bridge_schema(plan: dict[str, Any]) -> dict[str, Any]:
    attrs: dict[str, Any] = {}
    seen: set[str] = set()
    for idx, narrowing in enumerate(_planned_narrowings(plan), start=1):
        attr_name = _narrowing_attr_name(narrowing)
        if attr_name in seen:
            attr_name = f"{attr_name}_{idx}"
        seen.add(attr_name)
        attrs[attr_name] = {
            "annotations": {
                "edge_archetype": str(narrowing["edge"]),
                "src_subtypes": _bridge_subtype_label(plan, narrowing["parent"]),
                "dst_subtypes": _bridge_subtype_label(plan, narrowing["child"]),
            }
        }
    return {
        "id": f"https://palimpzest.mit.edu/okg/deployments/{plan['deployment']['name']}/bootstrap_bridges",
        "name": f"{plan['deployment']['name']}_bootstrap_bridges",
        "prefixes": {
            "linkml": "https://w3id.org/linkml/",
            "okg": "https://palimpzest.mit.edu/okg/",
        },
        "default_range": "string",
        "imports": ["linkml:types"],
        "classes": {
            "EdgeNarrowings": {
                "annotations": {"okg_edge_narrowings": "true"},
                "attributes": attrs,
            }
        },
    }


def _ensure_default_authoring_plan(plan: dict[str, Any]) -> None:
    record, finding = _default_subtype_names(plan)
    modules = plan.setdefault("ontology_plan", {}).setdefault("library_modules", [])
    for required in (
        "git_graph",
        "openspec",
        "extraction",
        "agent_memory",
        "reasoning",
    ):
        if required not in modules:
            modules.append(required)
    subtypes = plan["ontology_plan"].setdefault("subtypes", [])
    if not subtypes:
        subtypes.extend([
            {
                "name": record,
                "description": "Bootstrap-generated source record subtype.",
                "archetype": "artifact",
                "attrs": ["record_id", "title", "body", "source_id"],
                "searchable_text_fields": ["title", "body"],
                "alias_fields": ["record_id", "title"],
            },
            {
                "name": finding,
                "description": "Bootstrap-generated observation subtype.",
                "archetype": "observation",
                "attrs": ["finding_id", "label", "body", "source_id"],
                "searchable_text_fields": ["label", "body"],
            },
        ])
    elif len(subtypes) == 1:
        subtypes.append({
            "name": finding,
            "description": "Bootstrap-generated observation subtype.",
            "archetype": "observation",
            "attrs": ["finding_id", "label", "body", "source_id"],
            "searchable_text_fields": ["label", "body"],
        })
    if not plan.get("narrowing_plan"):
        record, finding = _primary_subtype_names(plan)
        plan["narrowing_plan"] = [
            {"parent": record, "edge": "contains", "child": finding},
            {"parent": record, "edge": "references", "child": "Document"},
            {"parent": "Document", "edge": "references", "child": record},
        ]
    assertions = plan.setdefault("quality_plan", {}).setdefault("fixture_assertions", [])
    if not assertions:
        assertions.append({"name": "non_empty_graph", "min_nodes": 1, "min_edges": 1})


def _planned_subtype_defs(plan: dict[str, Any]) -> list[dict[str, Any]]:
    subtypes = [
        dict(s) for s in plan.get("ontology_plan", {}).get("subtypes") or []
        if s.get("name")
    ]
    if len(subtypes) >= 2:
        return subtypes
    default_record, default_finding = _default_subtype_names(plan)
    if not subtypes:
        return [
            {
                "name": default_record,
                "description": "Bootstrap-generated source record subtype.",
                "archetype": "artifact",
                "attrs": ["record_id", "title", "body", "source_id"],
                "searchable_text_fields": ["title", "body"],
                "alias_fields": ["record_id", "title"],
            },
            {
                "name": default_finding,
                "description": "Bootstrap-generated observation subtype.",
                "archetype": "observation",
                "attrs": ["finding_id", "label", "body", "source_id"],
                "searchable_text_fields": ["label", "body"],
            },
        ]
    default = {
        "name": default_finding,
        "description": "Bootstrap-generated observation subtype.",
        "archetype": "observation",
        "attrs": ["finding_id", "label", "body", "source_id"],
        "searchable_text_fields": ["label", "body"],
    }
    return [subtypes[0], default]


def _primary_subtype_names(plan: dict[str, Any]) -> tuple[str, str]:
    names = [str(s["name"]) for s in _planned_subtype_defs(plan)]
    if len(names) >= 2:
        return names[0], names[1]
    return _default_subtype_names(plan)


def _default_subtype_names(plan: dict[str, Any]) -> tuple[str, str]:
    base = _pascal(str(plan.get("deployment", {}).get("name") or "Bootstrap"))
    return f"{base}Record", f"{base}Finding"


def _planned_narrowings(plan: dict[str, Any]) -> list[dict[str, Any]]:
    narrowings = [
        dict(n) for n in plan.get("narrowing_plan") or []
        if n.get("parent") and n.get("edge") and n.get("child")
    ]
    if narrowings:
        return narrowings
    record, finding = _primary_subtype_names(plan)
    return [
        {"parent": record, "edge": "contains", "child": finding},
        {"parent": record, "edge": "references", "child": "Document"},
        {"parent": "Document", "edge": "references", "child": record},
    ]


def _narrowing_attr_name(narrowing: dict[str, Any]) -> str:
    raw = "_".join([
        str(narrowing.get("parent") or "parent"),
        str(narrowing.get("edge") or "edge"),
        str(narrowing.get("child") or "child"),
    ])
    return re.sub(r"[^a-z0-9_]+", "_", _snake(raw)).strip("_") or "bootstrap_narrowing"


def _bridge_subtype_label(plan: dict[str, Any], value: Any) -> str:
    raw = str(value)
    for subtype in _planned_subtype_defs(plan):
        if str(subtype["name"]) == raw:
            return _subtype_class_name(raw)
    return raw


def _subtype_class_name(value: str) -> str:
    if re.fullmatch(r"[A-Z][A-Za-z0-9_]*", value):
        return value
    return _pascal(value)


def _looks_required_attr(name: str) -> bool:
    lowered = name.lower()
    return lowered == "id" or lowered.endswith("_id")


def _validate_linker_routes_against_catalog(staged: Path, composed: Any) -> None:
    from okg.substrate.catalog.linker_config import load_linker_config

    routes = load_linker_config(staged / "linker_config.yaml")
    for route in routes:
        if route.src_subtype in (None, "_default"):
            continue
        normalized = _snake(str(route.src_subtype))
        if normalized in composed.subtypes:
            continue
        raise SubstrateError(
            f"linker_config route {route.entity_type!r} references unknown source subtype {route.src_subtype!r}",
            data={
                "stage": "enrichment",
                "path": str(staged / "linker_config.yaml"),
                "entity_type": route.entity_type,
                "src_subtype": route.src_subtype,
                "known_subtypes": sorted(composed.subtypes),
            },
            fix_hint="use a subtype declared by ontology_plan or move the route under _default",
        )


def _validate_artifact_payloads(artifacts: dict[str, str | bytes]) -> None:
    for rel, content in artifacts.items():
        if rel.endswith(".json"):
            doc = json.loads(str(content))
        elif rel.endswith(".yaml") or rel.endswith(".yml"):
            doc = fastyaml.safe_load(str(content)) or {}
        else:
            continue
        if rel == "deployment.yaml":
            validate_against_schema(doc, "deployment.schema.json", path=rel)
        elif rel == "source_registry.yaml":
            validate_against_schema(doc, "source_registry.schema.json", path=rel)
        elif rel == "source_registry.bootstrap.yaml":
            validate_against_schema(doc, "source_registry.schema.json", path=rel)
        elif rel == "linker_config.yaml":
            validate_against_schema(doc, "linker_config.schema.json", path=rel)
        elif rel == "identifier_patterns.yaml":
            from okg.substrate.catalog.identifier_patterns import (
                VALID_VALIDATOR_KINDS,
            )
            patterns = doc.get("patterns")
            if not isinstance(patterns, list):
                raise BootstrapDraftInvalid(
                    "identifier_patterns.yaml requires a patterns list",
                    data={"path": rel},
                )
            for i, pattern in enumerate(patterns):
                if not isinstance(pattern, dict):
                    raise BootstrapDraftInvalid(
                        "identifier_patterns.yaml pattern must be a mapping",
                        data={"path": rel, "index": i},
                    )
                for key in ("id_type", "regex"):
                    if key not in pattern:
                        raise BootstrapDraftInvalid(
                            f"identifier_patterns.yaml pattern missing {key}",
                            data={"path": rel, "index": i, "missing": key},
                        )
                try:
                    re.compile(str(pattern["regex"]))
                except re.error as exc:
                    raise BootstrapDraftInvalid(
                        f"identifier_patterns.yaml pattern has invalid regex: {exc}",
                        data={
                            "path": rel,
                            "index": i,
                            "regex_error": str(exc),
                        },
                    ) from exc
                validator_kind = pattern.get("validator_kind")
                if (
                    validator_kind is not None
                    and validator_kind not in VALID_VALIDATOR_KINDS
                ):
                    raise BootstrapDraftInvalid(
                        "identifier_patterns.yaml pattern has unknown validator_kind",
                        data={
                            "path": rel,
                            "index": i,
                            "validator_kind": validator_kind,
                            "valid_kinds": sorted(VALID_VALIDATOR_KINDS),
                        },
                    )
        elif rel == "aliases.json":
            validate_against_schema(doc, "aliases.schema.json", path=rel)
        elif rel == "invariants.yaml" or rel.startswith("invariants/"):
            validate_against_schema(doc, "invariants.schema.json", path=rel)
        elif rel.startswith("schemas/") and rel.endswith(".yaml"):
            validate_against_schema(doc, "classes.schema.json", path=rel)


def _validate_staged_artifacts(staged: Path) -> None:
    required = _required_staged_files()
    missing = [rel for rel in required if not (staged / rel).is_file()]
    if missing:
        raise BootstrapDraftInvalid(
            "staged deployment artifacts are incomplete",
            data={"path": str(staged), "stage": "read-side surfacing", "errors": missing},
            fix_hint="rerun `okg deployment bootstrap draft --apply`",
        )
    _validate_artifact_payloads({
        rel: (staged / rel).read_text(encoding="utf-8")
        for rel in required
    })
    from okg.substrate.catalog.identifier_patterns import parse_yaml
    parse_yaml(staged / "identifier_patterns.yaml")
    from okg.substrate.views.registry import load_views_registry
    load_views_registry(staged)


def _required_staged_files() -> list[str]:
    return [
        "deployment.yaml",
        "source_registry.yaml",
        "source_registry.bootstrap.yaml",
        "schemas/bootstrap_domain.yaml",
        "schemas/bridges/bootstrap_bridges.yaml",
        "extractors.yaml",
        "linker_config.yaml",
        "identifier_patterns.yaml",
        "aliases.json",
        "invariants.yaml",
        "invariants/bootstrap.yaml",
        "views_registry.yaml",
    ]


def _staged_artifact_presence(
    plan: dict[str, Any],
    *,
    deployment_dir: Path | None,
) -> dict[str, list[str]]:
    required = _required_staged_files()
    verification = plan.get("verification") or {}
    staged_hashes = verification.get("staged_hashes") or {}
    expected = sorted(str(rel) for rel in staged_hashes)
    if deployment_dir is None:
        return {
            "existing": expected,
            "missing_required": [
                rel for rel in required
                if rel not in staged_hashes
            ],
            "missing_verified": [],
        }

    sdir = staged_dir(deployment_dir)
    tracked = sorted(set(required) | set(expected))
    existing = [
        rel for rel in tracked
        if (sdir / rel).is_file()
    ]
    return {
        "existing": existing,
        "missing_required": [
            rel for rel in required
            if not (sdir / rel).is_file()
        ],
        "missing_verified": [
            rel for rel in expected
            if not (sdir / rel).is_file()
        ],
    }


def _plan_scorecard(
    plan: dict[str, Any],
    *,
    deployment_dir: Path | None = None,
) -> dict[str, Any]:
    sources = list(plan.get("sources") or [])
    source_ids = [str(src.get("id")) for src in sources if src.get("id")]
    inspected_ids = {
        str(shape.get("source_id"))
        for shape in plan.get("record_shapes") or []
        if shape.get("source_id")
    }
    bounded_sample_ids = [
        str(src.get("id")) for src in sources
        if src.get("sample_path")
    ]
    adapter_ids = [
        str(src.get("id")) for src in sources
        if src.get("adapter")
    ]
    ontology = plan.get("ontology_plan") or {}
    extractor = plan.get("extractor_plan") or {}
    quality = plan.get("quality_plan") or {}
    view_plan = plan.get("view_plan") or {}
    verification = plan.get("verification") or {}
    staged_hashes = verification.get("staged_hashes") or {}
    staged_presence = _staged_artifact_presence(
        plan,
        deployment_dir=deployment_dir,
    )
    dlq_count = None
    metrics = verification.get("metrics_snapshot") or {}
    if isinstance(metrics, dict):
        dlq_count = metrics.get("dlq_count")
    return {
        "sources": {
            "count": len(sources),
            "inspected_count": len([
                sid for sid in source_ids
                if sid in inspected_ids
            ]),
            "bounded_sample_count": len(bounded_sample_ids),
            "adapter_count": len(adapter_ids),
            "missing_sample_ids": [
                sid for sid in source_ids
                if sid not in bounded_sample_ids
            ],
            "uninspected_ids": [
                sid for sid in source_ids
                if sid not in inspected_ids
            ],
        },
        "ontology": {
            "library_module_count": len(ontology.get("library_modules") or []),
            "subtype_count": len(ontology.get("subtypes") or []),
        },
        "narrowings": {
            "count": len(plan.get("narrowing_plan") or []),
        },
        "extraction": {
            "identifier_pattern_count": len(extractor.get("entity_patterns") or []),
            "linker_route_count": len(extractor.get("linker_routes") or []),
            "alias_count": len(extractor.get("aliases") or []),
        },
        "quality": {
            "invariant_count": len(quality.get("invariants") or []),
            "fixture_assertion_count": len(quality.get("fixture_assertions") or []),
        },
        "views": {
            "count": len(view_plan.get("views") or []),
        },
        "artifacts": {
            "staged_file_count": len(staged_presence["existing"]),
            "verified_staged_file_count": len(staged_hashes),
            "required_staged_count": len(_required_staged_files()),
            "missing_required_staged": staged_presence["missing_required"],
            "missing_verified_staged": staged_presence["missing_verified"],
        },
        "verification": {
            "status": verification.get("status"),
            "generation_id": verification.get("generation_id"),
            "catalog_version_id": verification.get("catalog_version_id"),
            "node_row_delta": verification.get("node_row_delta"),
            "edge_row_delta": verification.get("edge_row_delta"),
            "dlq_count": dlq_count,
        },
    }


def _deployment_artifact_scorecard(deployment_dir: Path) -> dict[str, Any]:
    deployment_yaml = deployment_dir / "deployment.yaml"
    deployment_doc = _safe_yaml(deployment_yaml)
    schema_dir = deployment_dir / str(deployment_doc.get("schema_dir") or "schemas")
    source_registry_name = str(
        deployment_doc.get("source_registry") or "source_registry.yaml"
    )
    source_registry_path = deployment_dir / source_registry_name
    source_registry_doc = _safe_yaml(source_registry_path)
    sources = source_registry_doc.get("sources") or {}
    source_count = len(sources) if isinstance(sources, dict) else 0
    schemas = sorted(schema_dir.glob("*.yaml")) if schema_dir.is_dir() else []
    bridge_schemas = (
        sorted((schema_dir / "bridges").glob("*.yaml"))
        if (schema_dir / "bridges").is_dir()
        else []
    )
    views_doc = _safe_yaml(deployment_dir / "views_registry.yaml")
    views = views_doc.get("views") or []
    from okg.substrate.catalog.invariants import discover_invariant_files

    invariant_files = discover_invariant_files(deployment_dir)
    return {
        "sources": {
            "count": source_count,
            "registry_path": (
                str(source_registry_path)
                if source_registry_path.is_file()
                else None
            ),
        },
        "ontology": {
            "schema_file_count": len(schemas),
            "bridge_schema_count": len(bridge_schemas),
            "schema_dir": str(schema_dir) if schema_dir.is_dir() else None,
        },
        "extraction": {
            "has_extractors": (deployment_dir / "extractors.yaml").is_file(),
            "has_linker_config": (deployment_dir / "linker_config.yaml").is_file(),
            "has_identifier_patterns": (
                deployment_dir / "identifier_patterns.yaml"
            ).is_file(),
            "has_aliases": (deployment_dir / "aliases.json").is_file(),
        },
        "quality": {
            "invariant_file_count": len(invariant_files),
        },
        "views": {
            "has_views_registry": (deployment_dir / "views_registry.yaml").is_file(),
            "count": len(views) if isinstance(views, list) else 0,
        },
        "artifacts": {
            "deployment_yaml": deployment_yaml.is_file(),
            "bootstrap_plan": plan_path(deployment_dir).is_file(),
        },
        "verification": {
            "status": None,
            "generation_id": None,
        },
    }


def _safe_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        doc = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
    return doc if isinstance(doc, dict) else {}


def _compute_status(
    plan: dict[str, Any],
    *,
    deployment_dir: Path | None = None,
) -> dict[str, Any]:
    scorecard = _plan_scorecard(plan, deployment_dir=deployment_dir)
    verification = plan.get("verification") or {}
    if not plan.get("sources"):
        return {
            "stage": "needs_sources",
            "blocking_stage": "source coverage",
            "last_failure_code": None,
            "next_action": {
                "edit": "bootstrap_plan.yaml:sources",
                "command": "okg deployment bootstrap plan --source <id=path> --apply",
            },
            "scorecard": scorecard,
        }
    if not plan.get("record_shapes"):
        return {
            "stage": "needs_source_inspection",
            "blocking_stage": "source coverage",
            "last_failure_code": _last_failure_code(verification),
            "next_action": {
                "edit": "bootstrap_plan.yaml:sources[*].sample_path",
                "command": "okg deployment bootstrap inspect-source --apply",
            },
            "scorecard": scorecard,
        }
    if not plan.get("ontology_plan", {}).get("subtypes"):
        return {
            "stage": "needs_ontology_plan",
            "blocking_stage": "narrowing-trigger acceptance",
            "last_failure_code": _last_failure_code(verification),
            "next_action": {
                "edit": "bootstrap_plan.yaml:ontology_plan",
                "command": "okg deployment bootstrap draft --apply",
            },
            "scorecard": scorecard,
        }
    if not verification.get("staged_hashes"):
        return {
            "stage": "needs_draft",
            "blocking_stage": "read-side surfacing",
            "last_failure_code": _last_failure_code(verification),
            "next_action": {
                "edit": "staged deployment artifacts",
                "command": "okg deployment bootstrap draft --apply",
            },
            "scorecard": scorecard,
        }
    if verification.get("status") == "applied":
        return {
            "stage": "applied",
            "blocking_stage": None,
            "last_failure_code": None,
            "next_action": None,
            "scorecard": scorecard,
        }
    artifacts = scorecard["artifacts"]
    if (
        deployment_dir is not None
        and (
            artifacts["missing_required_staged"]
            or artifacts["missing_verified_staged"]
        )
    ):
        return {
            "stage": "needs_staged_artifacts",
            "blocking_stage": "read-side surfacing",
            "last_failure_code": _last_failure_code(verification),
            "next_action": {
                "edit": "staged deployment artifacts",
                "command": "okg deployment bootstrap draft --apply",
            },
            "scorecard": scorecard,
        }
    if verification.get("status") not in {"verified", "applied"}:
        return {
            "stage": "needs_verification",
            "blocking_stage": "projection",
            "last_failure_code": _last_failure_code(verification),
            "next_action": {
                "edit": "staged deployment artifacts",
                "command": "okg deployment bootstrap verify --dsn <dev-dsn> --apply",
            },
            "scorecard": scorecard,
        }
    return {
        "stage": "ready_to_apply",
        "blocking_stage": None,
        "last_failure_code": None,
        "next_action": {
            "edit": None,
            "command": "okg deployment bootstrap apply --apply",
        },
        "scorecard": scorecard,
    }


def _replace_record_shape(plan: dict[str, Any], sample: dict[str, Any]) -> None:
    source_id = sample["source_id"]
    shapes = [
        s for s in plan.setdefault("record_shapes", [])
        if s.get("source_id") != source_id
    ]
    shapes.append(sample)
    plan["record_shapes"] = sorted(shapes, key=lambda s: s["source_id"])


def _parse_source_descriptor(raw: str, *, base: Path) -> SourceDescriptor:
    if "=" not in raw:
        raise BootstrapPlanInvalid(
            f"--source must be id=path; got {raw!r}",
            data={"path": str(base / PLAN_FILENAME), "errors": [f"bad_source_descriptor:{raw}"]},
            fix_hint="use --source records=fixtures/records.json",
        )
    source_id, raw_path = raw.split("=", 1)
    source_id = _slug(source_id.strip())
    path = Path(raw_path.strip()).expanduser()
    if not path.is_absolute():
        path = (base / path).resolve()
    kind = "local_directory" if path.is_dir() else SUPPORTED_SUFFIXES.get(path.suffix.lower(), "local_json")
    return SourceDescriptor(source_id=source_id, path=path, kind=kind)


def _resolve_sample_path(deployment_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (deployment_dir / path).resolve()


def _display_path(path: Path, base: Path) -> str:
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def _hash_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return f"sha256:{h.hexdigest()}"


def _hash_directory(path: Path) -> str:
    h = hashlib.sha256()
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        h.update(str(child.relative_to(path)).encode("utf-8"))
        h.update(child.read_bytes())
    return f"sha256:{h.hexdigest()}"


def _hash_tree(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        rel = str(child.relative_to(path))
        out[rel] = _hash_file(child)
    return out


def _hash_authoring_plan(plan: dict[str, Any]) -> str:
    authoring_plan = {
        key: value for key, value in plan.items()
        if key != "verification"
    }
    normalized = json.dumps(authoring_plan, sort_keys=True, default=str)
    return f"sha256:{hashlib.sha256(normalized.encode('utf-8')).hexdigest()}"


def _hash_json(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _dump_yaml(value: Any) -> str:
    return yaml.safe_dump(value, sort_keys=False, allow_unicode=False)


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _looks_like_identifier(name: str, unique_count: int, record_count: int) -> bool:
    lowered = name.lower()
    name_hit = bool(re.search(r"(^id$|_id$|uuid|key|code|number|path|slug|^name$)", lowered))
    uniqueness_ok = record_count <= 1 or unique_count >= max(1, int(record_count * 0.8))
    return name_hit and uniqueness_ok


def _looks_like_text_field(name: str, example: Any) -> bool:
    lowered = name.lower()
    if any(tok in lowered for tok in ("title", "text", "body", "summary", "description", "content", "note", "abstract")):
        return True
    return isinstance(example, str) and len(example) >= 40


def _record_identity(record: dict[str, Any], idx: int) -> str:
    for key in ("id", "record_id", "uuid", "key", "code", "number", "path", "name", "title"):
        if record.get(key) not in (None, ""):
            return str(record[key])
    return str(idx)


def _record_text(record: dict[str, Any]) -> str:
    parts = []
    for key in ("title", "summary", "description", "body", "content", "text", "note", "abstract"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    if parts:
        return "\n\n".join(parts)
    return json.dumps(record, sort_keys=True, default=str)


def _record_body(record: dict[str, Any], title: str) -> str:
    """The record's text fields without the one used as its title.

    The title is its own searchable part; repeating it at the head of the
    body only inflates its term frequency. Falls back to `_record_text`
    when the title is the record's only text.
    """
    parts = []
    for key in ("title", "summary", "description", "body", "content", "text", "note", "abstract"):
        value = record.get(key)
        if isinstance(value, str) and value.strip() and value.strip() != title:
            parts.append(value.strip())
    if parts:
        return "\n\n".join(parts)
    return _record_text(record)


def _record_title(record: dict[str, Any], fallback: str) -> str:
    for key in ("title", "summary", "name", "path", "id"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:120]
    return fallback


def _slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_-]+", "-", value)
    value = value.strip("-_")
    return value or "bootstrap"


def _pascal(value: str) -> str:
    parts = re.split(r"[^A-Za-z0-9]+", value)
    out = "".join(p[:1].upper() + p[1:] for p in parts if p)
    return out or "Bootstrap"


def _snake(value: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
    snake = re.sub(r"[^a-z0-9_]+", "_", snake)
    return re.sub(r"_+", "_", snake).strip("_") or "bootstrap"


def _first_sentence(text: str) -> str:
    compact = " ".join(text.strip().split())
    if not compact:
        return "Bootstrap deployment"
    match = re.search(r"(.{1,160}?[.!?])\s", compact + " ")
    return match.group(1) if match else compact[:160]


def _extract_questions(text: str) -> list[str]:
    questions = []
    for line in text.splitlines():
        line = line.strip("-* \t")
        if line.endswith("?"):
            questions.append(line)
    return questions[:10]


def _first_markdown_heading(text: str) -> str | None:
    for line in text.splitlines():
        if line.startswith("# "):
            return line[2:].strip()
    return None


def _last_failure_code(verification: dict[str, Any]) -> str | None:
    failure = verification.get("last_failure") or {}
    return failure.get("code")
