"""Deployment environment authority diagnostics.

The builder in this module is side-effect free: it reads manifest/local env
files and a supplied process environment mapping, but it never calls
``load_local_env`` and never mutates ``os.environ``.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Mapping

from okg.substrate import fastyaml
from okg.substrate.db.dsn import redact_target
from okg.substrate.deployment_authority import (
    resolved_target,
    worktree_main_local_env_dsn,
)
from okg.substrate.deployment_lint import DiagnosticFinding, _sort_finding_dicts
from okg.substrate.local_env import configured_local_env_path, parse_local_env
from okg.substrate.support.env_provenance import interpreter_provenance_report
from okg.substrate.cli.command_reference import Masked, okg_command
from okg.substrate.support.user_hints import hint_command


ENV_DOCTOR_SCHEMA = "okg.deployment_env_doctor.v1"

_ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_SECRET_RE = re.compile(
    r"(SECRET|TOKEN|PASSWORD|PASS|PWD|PRIVATE|KEY|DSN|DATABASE_URL)",
    re.IGNORECASE,
)
_DISALLOWED_LOCAL_KEYS = {
    "OKG_TEST_DSN",
    "OKG_TEST_DSN_TEMPLATE",
    "OKG_TEST_DSN_TRUST_AS_EPHEMERAL",
}
_ALLOWED_NON_OKG_LOCAL_KEYS = {
    "CHRONOS_INTERVAL_CONTINUATION_PERCENT",
}


def build_env_doctor_report(
    deployment: str | Path,
    *,
    deployments_root: str | Path | None = None,
    explicit_dsn: str | None = None,
    local_env_path: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    for_command: str = "read",
    show_values: bool = False,
    local_profile_authoritative: bool | None = None,
    include_worktree_main_fallback: bool = True,
) -> dict[str, Any]:
    """Build the env doctor report envelope for one deployment."""

    from okg.substrate.deployment_lint import resolve_deployment_manifest_path

    process_env = os.environ if env is None else env
    command_risk = classify_for_command(for_command)
    manifest_path = resolve_deployment_manifest_path(
        deployment,
        deployments_root=deployments_root,
    )
    findings: list[DiagnosticFinding] = []

    if manifest_path is None:
        findings.append(_finding(
            code="deployment.env.manifest_not_found",
            severity="blocker",
            message=f"deployment {deployment!r} was not found",
            why="Env authority is rooted in deployment.yaml.",
            next_action="check deployments/<name>/ or OKG_DEPLOYMENTS_DIR",
        ))
        return _report(
            deployment=str(deployment),
            manifest_path=None,
            local_env_path=None,
            for_command=for_command,
            command_risk=command_risk,
            findings=findings,
        )

    deployment_ref = str(deployment)
    deployment_dir = manifest_path.parent
    deployment_name = str(deployment)
    raw_manifest: dict[str, Any] = {}
    try:
        raw = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8"),
        ) or {}
        if not isinstance(raw, dict):
            raise ValueError("deployment.yaml must be a mapping")
        raw_manifest = raw
        deployment_name = str(raw.get("name") or deployment_dir.name)
    except Exception as exc:  # noqa: BLE001 - normalized.
        findings.append(_finding(
            code="deployment.env.manifest_unreadable",
            severity="blocker",
            message="deployment manifest could not be read",
            why="Env doctor cannot resolve authority without the manifest.",
            location=_loc(manifest_path),
            observed=str(exc),
            expected="valid YAML mapping",
            next_action="fix deployment.yaml",
        ))
        return _report(
            deployment=deployment_name,
            manifest_path=manifest_path,
            local_env_path=None,
            for_command=for_command,
            command_risk=command_risk,
            findings=findings,
        )

    if local_profile_authoritative is None:
        # Default authority posture comes from the manifest's
        # postgres.self_hosted_authoritative flag. Read it from the manifest
        # ALREADY resolved+parsed above (which honors a Path / custom
        # deployments_root) rather than re-resolving by name, so the doctor's
        # verdict matches the deployment it actually inspected. Mirrors
        # deployment_self_hosted_authoritative's contract: only a literal True
        # opts in.
        _pg = raw_manifest.get("postgres") if isinstance(raw_manifest, dict) else None
        local_profile_authoritative = (
            isinstance(_pg, dict) and _pg.get("self_hosted_authoritative") is True
        )
    else:
        local_profile_authoritative = bool(local_profile_authoritative)
    local_path = _configured_local_env_path(
        local_env_path=local_env_path,
        process_env=process_env,
    )
    local_values: dict[str, str] = {}
    local_parse_error: str | None = None
    if local_path.is_file():
        try:
            local_values = dict(parse_local_env(local_path))
        except Exception as exc:  # noqa: BLE001
            local_parse_error = str(exc)
            findings.append(_finding(
                code="deployment.env.local_env_parse_error",
                severity="blocker",
                message="local env file could not be parsed",
                why="Invalid local env syntax makes authority ambiguous.",
                location=_loc(local_path),
                observed=str(exc),
                expected="KEY=VALUE local env file",
                next_action="fix the local env file",
            ))

    pg_block = raw_manifest.get("postgres")
    raw_dsn = pg_block.get("dsn") if isinstance(pg_block, Mapping) else None
    if raw_dsn is None:
        raw_dsn = raw_manifest.get("postgres_dsn")
    expected_database = (
        pg_block.get("database")
        if isinstance(pg_block, Mapping)
        and isinstance(pg_block.get("database"), str)
        else None
    )
    required_env = _env_refs(raw_dsn) if isinstance(raw_dsn, str) else []

    authority_order = _authority_order(
        explicit=explicit_dsn is not None,
        local_profile_authoritative=local_profile_authoritative,
        include_worktree_main_fallback=include_worktree_main_fallback,
    )
    local_load = _would_load_local_env(
        local_values,
        process_env=process_env,
        override=local_profile_authoritative,
    )

    variables: list[dict[str, Any]] = []
    values_for_resolution: dict[str, str] = {}
    conflicts: list[dict[str, Any]] = []
    unresolved: list[str] = []
    worktree_fallback = _worktree_fallback_dsn(
        include=include_worktree_main_fallback,
        env=process_env,
    )

    for name in required_env:
        row = _variable_authority_row(
            name,
            process_env=process_env,
            local_values=local_values,
            worktree_fallback=worktree_fallback,
            local_profile_authoritative=local_profile_authoritative,
            show_values=show_values,
        )
        variables.append(row)
        winner = row.get("winner") or {}
        winner_value = winner.get("raw_value")
        if isinstance(winner_value, str) and winner_value:
            values_for_resolution[name] = winner_value
        else:
            unresolved.append(name)
        if row.get("conflict"):
            conflict = {
                "name": name,
                "winner": winner.get("source"),
                "reason": row.get("conflict_reason"),
            }
            conflicts.append(conflict)
            severity = _conflict_severity(
                command_risk,
                local_profile_authoritative=local_profile_authoritative,
            )
            findings.append(_finding(
                code="deployment.env.authority_conflict",
                severity=severity,
                message=f"multiple sources set {name}",
                why="The selected command risk determines whether this is safe.",
                location=_loc(manifest_path, pointer="/postgres/dsn"),
                observed={
                    "winner": winner.get("source"),
                    "risk": command_risk,
                },
                expected="one clear authority source",
                next_action="unset the losing value or update local.env",
                details=conflict,
            ))

    resolved_dsn: str | None = None
    authority_mode = "unresolved"
    if explicit_dsn:
        resolved_dsn = explicit_dsn
        authority_mode = "explicit"
    elif isinstance(raw_dsn, str):
        resolved = raw_dsn
        for name, value in values_for_resolution.items():
            resolved = resolved.replace(f"${{{name}}}", value)
        if not _env_refs(resolved):
            resolved_dsn = resolved
            authority_mode = (
                "deployment_manifest_literal"
                if not required_env
                else (
                    "dogfood_local_profile"
                    if local_profile_authoritative else "deployment_manifest"
                )
            )

    if explicit_dsn is None:
        for name in unresolved:
            findings.append(_finding(
                code="deployment.env.required_missing",
                severity="blocker",
                message=f"required environment variable {name} is not set",
                why="The deployment DSN cannot be resolved.",
                location=_loc(manifest_path, pointer="/postgres/dsn"),
                observed="missing",
                expected=name,
                next_action=hint_command(okg_command(
                    "deployment env", "doctor", deployment_name, json=True,
                )),
            ))
    if raw_dsn is None and explicit_dsn is None:
        findings.append(_finding(
            code="deployment.env.dsn_not_declared",
            severity="blocker",
            message="deployment does not declare a DSN",
            why="Deployment-scoped commands need a DSN authority.",
            location=_loc(manifest_path, pointer="/postgres/dsn"),
            expected="postgres.dsn",
            next_action="add postgres.dsn to deployment.yaml",
        ))
    elif not isinstance(raw_dsn, str):
        findings.append(_finding(
            code="deployment.env.dsn_not_string",
            severity="blocker",
            message="deployment DSN is not a string",
            why="Env interpolation works on string DSN declarations.",
            location=_loc(manifest_path, pointer="/postgres/dsn"),
            observed=type(raw_dsn).__name__,
            expected="string",
            next_action="replace postgres.dsn with a string",
        ))

    target = (
        resolved_target(
            resolved_dsn,
            deployment=deployment_name,
            authority_mode=authority_mode,
            expected_database=expected_database,
        )
        if resolved_dsn
        else None
    )
    if target and target.get("identity") == "mismatch":
        severity = "blocker" if command_risk in {"write", "runtime"} else "warning"
        findings.append(_finding(
            code="deployment.env.dsn_target_mismatch",
            severity=severity,
            message="resolved DSN points at a different database",
            why=(
                "Reading the wrong database is confusing; writing or running "
                "workers against it is unsafe."
            ),
            location=_loc(manifest_path, pointer="/postgres/database"),
            observed=target.get("database"),
            expected=target.get("expected_database"),
            next_action="fix OKG_DSN/local.env or the manifest database",
            details={"risk": command_risk},
        ))

    public_variables = _public_variables(variables)
    environment = [
        {
            "name": row["name"],
            "required": row["required"],
            "set": bool(row.get("winner", {}).get("present")),
            "source": row.get("winner", {}).get("source") or "missing",
            "value": row.get("winner", {}).get("value"),
        }
        for row in variables
    ]
    authority = {
        "mode": authority_mode,
        "winner": _winner_summary(variables, explicit_dsn=explicit_dsn),
        "authority_order": list(authority_order),
        "local_profile_authoritative": local_profile_authoritative,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "missing": list(unresolved),
    }
    if target is not None:
        authority["target"] = target

    from okg.substrate.catalog.ownership import suggested_worktree_dsn

    suggested_dsn = suggested_worktree_dsn(
        deployment_name=deployment_name,
        start=deployment_dir,
    )
    recommended = _recommended_commands(
        deployment_ref,
        unresolved=unresolved,
        resolved=resolved_dsn is not None,
        suggested_dsn=suggested_dsn,
    )
    report = _report(
        deployment=deployment_name,
        manifest_path=manifest_path,
        local_env_path=local_path,
        for_command=for_command,
        command_risk=command_risk,
        findings=findings,
        local_env={
            "path": str(local_path),
            "exists": local_path.is_file(),
            "parse_error": local_parse_error,
            "loaded": local_load["loaded"],
            "skipped": local_load["skipped"],
            "values": [
                {
                    "name": key,
                    "set": True,
                    "value": redact_env_value(
                        value,
                        key=key,
                        show_values=show_values,
                    ),
                }
                for key, value in sorted(local_values.items())
            ],
        },
        postgres={
            "declared": isinstance(raw_dsn, str),
            "raw_dsn": redact_env_value(
                raw_dsn if isinstance(raw_dsn, str) else None,
                key="OKG_DSN",
                show_values=show_values,
            ),
            "required_env": list(required_env),
            "resolved": resolved_dsn is not None,
            "resolved_dsn": redact_env_value(
                resolved_dsn,
                key="OKG_DSN",
                show_values=show_values,
            ),
            "target": target,
        },
        variables=public_variables,
        environment=environment,
        authority=authority,
        recommended_commands=recommended,
    )
    return report


def classify_for_command(command: str | None) -> str:
    """Normalize a command hint into read/write/runtime risk."""

    value = (command or "read").strip().lower()
    if value in {"read", "write", "runtime"}:
        return value
    runtime_terms = ("runtime", "worker", "dogfood", "watch", "webhook")
    write_terms = (
        "write",
        "ingest",
        "publish",
        "sync",
        "catalog load",
        "catalog reconcile",
        "deployment up",
        "deployment fork",
        "deployment gc",
        "upgrade",
        "migrate",
        "record",
    )
    if any(term in value for term in runtime_terms):
        return "runtime"
    if any(term in value for term in write_terms):
        return "write"
    return "read"


def redact_env_value(
    value: str | None,
    *,
    key: str | None = None,
    show_values: bool = False,
) -> str | None:
    """Redact secrets while preserving presence and DSN target metadata."""

    if value is None:
        return None
    if show_values:
        return value
    if value.startswith("${") and value.endswith("}"):
        return value
    if "://" in value:
        return redact_target(value)
    if key and _SECRET_RE.search(key):
        return "<redacted>"
    return "<set>"


def format_env_doctor_summary(report: Mapping[str, Any]) -> str:
    findings = list(report.get("findings") or [])
    status = "ok" if report.get("ok") else "blocked"
    lines = [
        (
            f"deployment env doctor: {status}; "
            f"risk={report.get('command_risk')}; "
            f"findings={len(findings)}"
        )
    ]
    authority = report.get("authority") or {}
    if isinstance(authority, Mapping):
        lines.append(f"authority: {authority.get('mode')}")
    interpreter = report.get("interpreter") or {}
    if isinstance(interpreter, Mapping):
        lines.append(
            f"interpreter: {interpreter.get('executable')} "
            f"({interpreter.get('environment_kind')}; "
            f"okg {interpreter.get('okg_version') or 'version unknown'})"
        )
        command_authority = interpreter.get("repo_command_authority") or {}
        if isinstance(command_authority, Mapping):
            expected = command_authority.get("expected_command")
            if not command_authority.get("in_checkout"):
                lines.append(
                    "repo command authority: n/a "
                    "(not run from an okg checkout)"
                )
            elif command_authority.get("matches"):
                lines.append(
                    f"repo command authority: matches `{expected}` "
                    f"({command_authority.get('checkout_venv')})"
                )
            else:
                lines.append(
                    "repo command authority: MISMATCH - interpreter is "
                    f"not the checkout venv "
                    f"{command_authority.get('checkout_venv')}; run "
                    f"`{expected}` from "
                    f"{command_authority.get('checkout_root')}"
                )
    for finding in findings:
        lines.append(
            f"- {finding.get('severity')} {finding.get('code')}: "
            f"{finding.get('message')}"
        )
        if finding.get("why"):
            lines.append(f"  why: {finding.get('why')}")
        if finding.get("next_action"):
            lines.append(f"  next: {finding.get('next_action')}")
    return "\n".join(lines)


def _report(
    *,
    deployment: str,
    manifest_path: Path | None,
    local_env_path: Path | None,
    for_command: str,
    command_risk: str,
    findings: list[DiagnosticFinding],
    local_env: dict[str, Any] | None = None,
    postgres: dict[str, Any] | None = None,
    variables: list[dict[str, Any]] | None = None,
    environment: list[dict[str, Any]] | None = None,
    authority: dict[str, Any] | None = None,
    recommended_commands: list[str] | None = None,
) -> dict[str, Any]:
    normalized = _sort_finding_dicts([f.as_dict() for f in findings])
    blockers = [f for f in normalized if f.get("severity") == "blocker"]
    payload = {
        "schema": ENV_DOCTOR_SCHEMA,
        "deployment": deployment,
        "manifest_path": str(manifest_path) if manifest_path else None,
        "deployment_dir": (
            str(manifest_path.parent) if manifest_path is not None else None
        ),
        "local_env_path": str(local_env_path) if local_env_path else None,
        "local_env_exists": (
            local_env_path.is_file() if local_env_path is not None else False
        ),
        "for_command": for_command,
        "command_risk": command_risk,
        "ok": not blockers,
        "finding_count": len(normalized),
        "findings": normalized,
        "local_env": local_env or {},
        "postgres": postgres or {},
        "variables": list(variables or []),
        "environment": list(environment or []),
        "authority": authority or {},
        # Interpreter provenance (issue #639): which Python environment this
        # doctor run actually resolved to — uv tool shim vs checkout venv vs
        # system interpreter — so shim-vs-venv mismatch is visible BEFORE a
        # source fails. Describes the live process, deliberately not the
        # injected `env` mapping.
        "interpreter": interpreter_provenance_report(),
        "recommended_commands": list(recommended_commands or []),
        "command_contract": {
            "env_command": "deployment env <deployment>",
            "doctor_command": "deployment env doctor <deployment>",
            "write_flags_mutate_local_env": False,
            "local_env_mutation": "not_performed",
            "process_env_wins_by_default": True,
            "dogfood_local_profile_can_override_process_env": True,
        },
    }
    return payload


def _variable_authority_row(
    name: str,
    *,
    process_env: Mapping[str, str],
    local_values: Mapping[str, str],
    worktree_fallback: str | None,
    local_profile_authoritative: bool,
    show_values: bool,
) -> dict[str, Any]:
    process_raw = process_env.get(name)
    local_raw = local_values.get(name)
    sources = [
        _source_row(
            "process_env",
            process_raw,
            key=name,
            show_values=show_values,
        ),
        _source_row(
            "local_env",
            local_raw,
            key=name,
            show_values=show_values,
        ),
    ]
    if name == "OKG_DSN":
        sources.append(_source_row(
            "worktree_main_local_env",
            worktree_fallback,
            key=name,
            show_values=show_values,
        ))

    winner_source = "missing"
    winner_value: str | None = None
    conflict_reason = None
    if local_profile_authoritative and local_raw:
        winner_source = "local_env"
        winner_value = local_raw
        if process_raw and process_raw != local_raw:
            conflict_reason = "dogfood_local_profile_authority"
    elif process_raw:
        winner_source = "process_env"
        winner_value = process_raw
        if local_raw and local_raw != process_raw:
            conflict_reason = "process_env_wins"
    elif local_raw:
        winner_source = "local_env"
        winner_value = local_raw
    elif name == "OKG_DSN" and worktree_fallback:
        winner_source = "worktree_main_local_env"
        winner_value = worktree_fallback

    for source in sources:
        source["wins"] = source["source"] == winner_source
    return {
        "name": name,
        "required": True,
        "sources": sources,
        "winner": {
            "source": winner_source,
            "present": bool(winner_value),
            "value": redact_env_value(
                winner_value,
                key=name,
                show_values=show_values,
            ),
            "raw_value": winner_value,
        },
        "conflict": conflict_reason is not None,
        "conflict_reason": conflict_reason,
    }


def _source_row(
    source: str,
    raw_value: str | None,
    *,
    key: str,
    show_values: bool,
) -> dict[str, Any]:
    return {
        "source": source,
        "present": bool(raw_value),
        "value": redact_env_value(
            raw_value,
            key=key,
            show_values=show_values,
        ),
    }


def _would_load_local_env(
    local_values: Mapping[str, str],
    *,
    process_env: Mapping[str, str],
    override: bool,
) -> dict[str, Any]:
    loaded: list[str] = []
    skipped: dict[str, str] = {}
    for key in sorted(local_values):
        if key in _DISALLOWED_LOCAL_KEYS:
            skipped[key] = "test_db_key_not_loaded_from_local_profile"
        elif (
            not key.startswith("OKG_")
            and key not in _ALLOWED_NON_OKG_LOCAL_KEYS
        ):
            skipped[key] = "not_an_okg_key"
        elif process_env.get(key) and not override:
            skipped[key] = "process_env_wins"
        else:
            loaded.append(key)
    return {"loaded": loaded, "skipped": skipped}


def _authority_order(
    *,
    explicit: bool,
    local_profile_authoritative: bool,
    include_worktree_main_fallback: bool,
) -> list[str]:
    if explicit:
        return ["explicit"]
    order = ["deployment_manifest"]
    if local_profile_authoritative:
        order.extend(["local_env", "process_env"])
    else:
        order.extend(["process_env", "local_env"])
    if include_worktree_main_fallback:
        order.append("worktree_main_local_env")
    return order


def _configured_local_env_path(
    *,
    local_env_path: str | Path | None,
    process_env: Mapping[str, str],
) -> Path:
    if local_env_path is not None:
        return Path(local_env_path).expanduser().resolve()
    override = process_env.get("OKG_LOCAL_ENV")
    if override:
        return Path(override).expanduser().resolve()
    return configured_local_env_path()


def _winner_summary(
    variables: list[dict[str, Any]],
    *,
    explicit_dsn: str | None,
) -> dict[str, Any]:
    if explicit_dsn:
        return {"source": "explicit", "name": "OKG_DSN", "present": True}
    winners = []
    for row in variables:
        winner = dict(row.get("winner") or {})
        winner.pop("raw_value", None)
        winners.append({"name": row.get("name"), **winner})
    return {"variables": winners}


def _public_variables(
    variables: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    public: list[dict[str, Any]] = []
    for row in variables:
        next_row = dict(row)
        winner = dict(next_row.get("winner") or {})
        winner.pop("raw_value", None)
        next_row["winner"] = winner
        public.append(next_row)
    return public


def _recommended_commands(
    deployment: str,
    *,
    unresolved: list[str],
    resolved: bool,
    suggested_dsn: str,
) -> list[str]:
    deployment_ref = str(deployment)
    env_doctor = hint_command(
        okg_command("deployment env", "doctor", deployment_ref, json=True)
    )
    if unresolved:
        # Redacted for the same reason as the catalog-ownership recommendation
        # (#2213): this list is operator-pasted JSON, not a connection string.
        # The hint is only shown, never run, so both forms are the redacted one.
        dsn = redact_target(suggested_dsn)
        return [
            env_doctor,
            hint_command(okg_command(
                "deployment env", deployment_ref, write_local=True,
                env_name=str(unresolved[0]), dsn=Masked(dsn, dsn),
            )),
        ]
    if resolved:
        return [hint_command(
            okg_command("deployment show", deployment_ref, json=True)
        )]
    return [env_doctor]


def _worktree_fallback_dsn(
    *,
    include: bool,
    env: Mapping[str, str],
) -> str | None:
    if not include:
        return None
    if (
        env.get("PYTEST_CURRENT_TEST")
        and env.get("OKG_ENABLE_LOCAL_ENV_FOR_TESTS") != "1"
    ):
        return None
    try:
        return worktree_main_local_env_dsn()
    except Exception:  # noqa: BLE001 - absence of fallback is not fatal.
        return None


def _conflict_severity(
    command_risk: str,
    *,
    local_profile_authoritative: bool,
) -> str:
    if local_profile_authoritative:
        return "warning"
    return "blocker" if command_risk in {"write", "runtime"} else "warning"


def _env_refs(value: str | None) -> list[str]:
    if not isinstance(value, str):
        return []
    return sorted(set(_ENV_REF_RE.findall(value)))


def _finding(
    *,
    code: str,
    severity: str,
    message: str,
    why: str,
    location: dict[str, Any] | None = None,
    observed: Any = None,
    expected: Any = None,
    next_action: str | None = None,
    details: dict[str, Any] | None = None,
) -> DiagnosticFinding:
    return DiagnosticFinding(
        code=code,
        severity=severity,
        stage="environment",
        message=message,
        why=why,
        location=location,
        observed=observed,
        expected=expected,
        next_action=next_action,
        details=dict(details or {}),
    )


def _loc(path: Path, *, pointer: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"path": str(path)}
    if pointer:
        out["pointer"] = pointer
    return out


__all__ = [
    "ENV_DOCTOR_SCHEMA",
    "build_env_doctor_report",
    "classify_for_command",
    "format_env_doctor_summary",
    "redact_env_value",
]
