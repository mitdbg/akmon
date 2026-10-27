"""okg CLI — substrate-era dispatcher.

Routes substrate subcommands (migrate, doctor, run, status, ...)
to okg.substrate.cli.<module>. Falls through to the legacy agent
CLI (okg ask) for unknown first-argument subcommands so existing
scripts keep working.

Under `OKG_AGENT=1`, default output is JSON and no TTY prompts
appear (per docs/reference/security.md + AGENTS.md).
"""
from __future__ import annotations
from okg.substrate import fastyaml

import argparse
import functools
import json
import os
import shlex
import shutil
import subprocess
import sys
import textwrap
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from okg.substrate.cli._options import (
    add_deployment_arg,
    add_dsn_arg,
    install_actionable_errors,
)
from okg.substrate.cli import entry_roles
from okg.substrate.cli.renamed_flags import add_renamed_argument, warn_renamed
from okg.substrate.unknown import Unknown, read_ok
from okg.substrate.cli.admission import _cmd_admission
from okg.substrate.cli.branch import _cmd_branch
from okg.substrate.cli.catalog import _cmd_catalog
from okg.substrate.cli.deployment import _cmd_deployment, _cmd_deployments
from okg.substrate.cli.environment import _cmd_environment
from okg.substrate.cli.external_actions import _cmd_external_action
from okg.substrate.cli.doctor import _cmd_doctor
from okg.substrate.cli.dogfood import cmd_dogfood as _cmd_dogfood
from okg.substrate.cli.init import (
    # `_cmd_init` is no longer dispatched directly — `init` is an alias of
    # `install`, which routes here for the scaffold path — but it stays
    # imported: this module is its established import path and both tests
    # and tooling reach it as `cli.main._cmd_init`.
    _cmd_init,
    _cmd_new_module,
    _cmd_new_source,
)
from okg.substrate.chat.instance_cli import (
    _cmd_chat_instance,
    add_chat_instance_parser,
)
from okg.substrate.cli.chat import _cmd_chat
from okg.substrate.cli.generation import _cmd_generation
from okg.substrate.cli.jobs import _cmd_jobs
from okg.substrate.cli.metrics import _cmd_metrics
from okg.substrate.cli.parser_groups import (
    register_db_cleanup_parsers,
    register_read_admin_parsers,
)
from okg.substrate.cli.provision import _cmd_provision
from okg.substrate.cli.runtime import _cmd_runtime
from okg.substrate.cli.run_summary import (
    BLOCKED_STATUSES as _CYCLE_BLOCKED_STATUSES,
    EXIT_CODES as _RUN_EXIT_CODES,
    publish_outcome as _run_publish_outcome,
)
from okg.substrate.cli.telemetry import (
    _cmd_telemetry,
    register_subparsers as register_telemetry_subparsers,
)
from okg.substrate.cli.view import _cmd_view
from okg.substrate.errors import SubstrateError
from okg.substrate.support.user_hints import okg_prefix

def _microbench_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _microbench_mapping_float(
    mapping: Any,
    *keys: str,
) -> float | None:
    if not isinstance(mapping, Mapping):
        return None
    for key in keys:
        value = _microbench_float(mapping.get(key))
        if value is not None:
            return value
    return None


def _microbench_publish_lock_wait_p95(row: Mapping[str, Any]) -> float | None:
    publish_stage = row.get("publish_stage_p95_ms")
    detail_stage = row.get("publish_timing_details_p95_ms")
    return _microbench_mapping_float(
        publish_stage,
        "locked_connection",
    ) or _microbench_mapping_float(
        detail_stage,
        "lock.acquire",
        "stage.locked_connection",
    )


def _microbench_gate_result(
    report: Mapping[str, Any],
    *,
    max_p95_ms: float,
) -> dict[str, Any]:
    rows = list(report.get("rows") or [])
    violations: list[dict[str, Any]] = []
    checked_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []
    for blocker in report.get("environment_blockers") or ():
        if not isinstance(blocker, Mapping):
            continue
        violations.append({
            "reason": str(blocker.get("reason") or "environment_blocker"),
            "environment": True,
            "details": dict(blocker),
        })
    for row in rows:
        source_name = str(row.get("source_name") or "")
        pipeline_mode = str(row.get("pipeline_mode") or "")
        if row.get("skipped"):
            skipped_rows.append({
                "source_name": source_name,
                "pipeline_mode": pipeline_mode,
                "reason": row.get("skip_reason"),
            })
            continue
        latency_ms = row.get("latency_ms") or {}
        p95 = _microbench_float(
            latency_ms.get("p95") if isinstance(latency_ms, Mapping) else None
        )
        sample_count = row.get("sample_count")
        checked_rows.append({
            "source_name": source_name,
            "pipeline_mode": pipeline_mode,
            "p95_ms": p95,
            "sample_count": sample_count,
        })
        if row.get("no_event_noop") or not sample_count:
            event_plan = row.get("event_plan") or {}
            violations.append({
                "source_name": source_name,
                "pipeline_mode": pipeline_mode,
                "reason": "no_real_samples",
                "sample_count": sample_count,
                "scope_status": (
                    event_plan.get("scope_status")
                    if isinstance(event_plan, Mapping) else None
                ),
            })
        elif isinstance(
            row_ok := read_ok(row, source="microbench_row"), Unknown,
        ):
            # A row with no verdict is unknown, never a pass (#2573).
            violations.append({
                "source_name": source_name,
                "pipeline_mode": pipeline_mode,
                "reason": "row_ok_unknown",
                "unknown_reason": row_ok.reason,
                "p95_ms": p95,
            })
        elif not row_ok:
            violations.append({
                "source_name": source_name,
                "pipeline_mode": pipeline_mode,
                "reason": "row_not_ok",
                "p95_ms": p95,
                "error_count": row.get("error_count"),
            })
        elif p95 is None:
            violations.append({
                "source_name": source_name,
                "pipeline_mode": pipeline_mode,
                "reason": "missing_p95_ms",
            })
        elif (
            observability_violation := _microbench_observability_violation(row)
        ) is not None:
            violations.append(observability_violation)
        elif p95 > max_p95_ms:
            lock_wait_p95 = _microbench_publish_lock_wait_p95(row)
            if (
                pipeline_mode == "full-pipeline"
                and lock_wait_p95 is not None
                and lock_wait_p95 >= min(250.0, max_p95_ms * 0.25)
            ):
                violations.append({
                    "source_name": source_name,
                    "pipeline_mode": pipeline_mode,
                    "reason": "publisher_lock_contention",
                    "environment": True,
                    "p95_ms": p95,
                    "max_p95_ms": max_p95_ms,
                    "lock_wait_p95_ms": lock_wait_p95,
                })
            else:
                violations.append({
                    "source_name": source_name,
                    "pipeline_mode": pipeline_mode,
                    "reason": "p95_budget_exceeded",
                    "p95_ms": p95,
                    "max_p95_ms": max_p95_ms,
                })
    report_ok = read_ok(report, source="microbench_report")
    if isinstance(report_ok, Unknown):
        # A report with no verdict is unknown, never a pass (#2573). It is
        # named even beside environment blockers: those do not explain it.
        violations.append({
            "reason": "report_ok_unknown",
            "unknown_reason": report_ok.reason,
        })
    elif not report_ok and not report.get("environment_blockers"):
        violations.append({
            "reason": "report_not_ok",
            "error_count": report.get("error_count"),
        })
    out = dict(report)
    out["gate"] = {
        "schema": "okg.source_sync.microbench.gate.v1",
        "ok": not violations,
        "max_p95_ms": max_p95_ms,
        "checked_row_count": len(checked_rows),
        "skipped_row_count": len(skipped_rows),
        "checked_rows": checked_rows,
        "skipped_rows": skipped_rows,
        "violation_count": len(violations),
        "violations": violations,
    }
    out["ok"] = report_ok is True and not violations
    return out


def _microbench_observability_violation(
    row: Mapping[str, Any],
) -> dict[str, Any] | None:
    pipeline_mode = str(row.get("pipeline_mode") or "")
    if pipeline_mode != "full-pipeline":
        return None
    status = row.get("observability_status")
    if status == "pass":
        return None
    gaps = _microbench_observability_gaps(row.get("observability_gaps"))
    reason = (
        gaps[0]
        if gaps
        else (
            "missing_full_pipeline_observability"
            if status is None
            else "full_pipeline_observability_failed"
        )
    )
    return {
        "source_name": str(row.get("source_name") or ""),
        "pipeline_mode": pipeline_mode,
        "reason": reason,
        "observability_status": status,
        "observability_gaps": gaps,
        "expected_delta_source": row.get("expected_delta_source"),
        "expected_delta_units": row.get("expected_delta_units"),
        "observed_delta_units": row.get("observed_delta_units"),
        "source_work_units": row.get("source_work_units"),
        "work_amplification_ratio": row.get("work_amplification_ratio"),
        "work_amplification_budget": row.get("work_amplification_budget"),
    }


def _microbench_observability_gaps(raw: Any) -> list[str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return [raw]
    if isinstance(raw, (list, tuple, set)):
        return [str(gap) for gap in raw if gap]
    return []


SUBSTRATE_SUBCOMMANDS = {
    "migrate",
    "doctor",
    "dogfood",
    "db",
    "run",
    "enricher",
    "ingest",
    "sync",
    "status",
    "schema",
    "worker",
    "jobs",
    "chat",
    "chat-instance",
    "cron",
    "mcp-serve",
    "ui",
    "agent-ssh-run",
    "agent-ssh-client",
    "agent-runtime-launch",
    "init",
    # Layer 2A: forker-audience module scaffolder; `new-source` is retired
    # (issue #2479) but still routes here so it can refuse.
    "new-module",
    "new-source",
    "deployment",
    "library",
    # `okg source draft` (PACT no-code-source-manifest-http).
    "source",
    "trace",
    "generation",
    "admission",
    "consistency-rerun",
    "consistency-scope-report",
    "bench",
    "view",
    "catalog",
    "alias",
    "remediate",
    "snapshot-load",
    "perf",
    # Deprecated alias of `perf` (decision 310); see entry_roles.RENAMED_VERBS.
    "profile",
    "poros",
    "nomos",
    "federation",
    "connect",
    "send",
    "pull",
    "inbox",
    "proof",
    "agent",
    "pact",
    # Profile-shipped verbs are NOT enumerated here. Each profile's
    # `library/profiles/<name>/cli/__init__.py` declares its own
    # verbs; the substrate discovers them at parser-build time via
    # `cli.profile_cli_loader.register_all_profile_clis`. Per
    # `retire-pi-prefix-commands`: the substrate dispatcher SHALL
    # NOT name any specific profile or profile-shipped verb.
}


def _publisher_kwargs_from_env() -> dict:
    """Operator env overrides shared by publisher-backed commands."""
    from okg.substrate.publisher.deployment_stack import (
        publisher_kwargs_from_env,
    )

    return publisher_kwargs_from_env()


def _is_agent_mode() -> bool:
    return os.environ.get("OKG_AGENT", "") == "1"


def _agent_apply_fix_hint(args: argparse.Namespace) -> str:
    """Return the concrete --apply placement for an agent-gated write."""

    command = getattr(args, "command", None)
    return f"okg {command} --apply ..."


def _migrate_apply_fix_hint(
    deployment_name: str | None, *, views_only: bool,
) -> str:
    """Return the exact `okg migrate` invocation that performs the write."""

    parts = ["okg migrate"]
    if deployment_name:
        parts.append(f"--deployment {deployment_name}")
    parts.append("--apply")
    if views_only:
        parts.append("--views-only")
    return (
        f"re-run as `{' '.join(parts)}`; to inspect the ledger without "
        "writing, use `okg db migrations"
        + (f" --deployment {deployment_name}" if deployment_name else "")
        + "`"
    )


def _normalize_cli_error(err: Exception) -> SubstrateError:
    """Return the stable CLI error shape for typed and ordinary exceptions.

    A few command boundaries intentionally catch stdlib exceptions alongside
    ``SubstrateError`` (for example catalog composition catches
    ``FileNotFoundError``).  Error rendering is the last-resort boundary: it
    must never raise a second exception and mask the failure it was asked to
    report.
    """
    if isinstance(err, SubstrateError):
        return err
    return SubstrateError(
        str(err),
        data={"exception_type": type(err).__name__},
    )


def _json_error(err: Exception) -> int:
    """Print a structured error envelope and return exit code."""
    err = _normalize_cli_error(err)
    from okg.substrate.db.connection import redact_value

    # Error rendering is the final failure boundary. Diagnostics routinely
    # contain datetimes, Paths, enums, and nested database details; rendering
    # must preserve the original actionable error rather than raise a second
    # serialization exception. Redact after normalization so every nested
    # string, including values produced from Path/Enum objects, is covered.
    payload = redact_value(_json_safe(err.to_json()))
    print(json.dumps(payload, sort_keys=True), file=sys.stderr)
    return 2 if err.code.startswith("extension_") else 1


def _text_error(err: Exception) -> int:
    err = _normalize_cli_error(err)
    fix = f"\n  fix_hint: {err.fix_hint}" if err.fix_hint else ""
    print(f"error [{err.code}]: {err.message}{fix}", file=sys.stderr)
    if err.data:
        print(f"  data: {err.data}", file=sys.stderr)
    return 2 if err.code.startswith("extension_") else 1


def _emit_error(err: Exception, *, as_json: bool) -> int:
    return _json_error(err) if as_json else _text_error(err)


def _emit_ok(payload: dict, *, as_json: bool) -> None:
    """Backwards-compatible thin wrapper around `emit_result`.

    Per `extract-cli-shared-options-and-output` / Wave D, the
    canonical CLI output helper is `cli._output.emit_result`.
    `_emit_ok` predates that helper; keeping it as a wrapper lets
    the 53+ existing call sites in this module (and the catalog/
    doctor/generation/init extracted modules that depend on it
    via the deferred-import shim) converge on the canonical
    envelope shape (sort_keys + default=str + nested-dict/list
    text rendering) without touching every call site.
    """
    from okg.substrate.cli._output import emit_result
    emit_result(payload, as_json=as_json)


def _report_pending_migrations(
    readiness,
    *,
    surface: str,
    deployment: str | None,
) -> dict | None:
    """Say the database is behind, and hand the caller the JSON block.

    The decision of 2026-09-21 for issue #2128: when the schema is
    applied but migrations are pending, a command REPORTS it and
    continues; refusal is reserved for an absent schema or ledger. Both
    readiness call sites originally discarded the returned readiness, so
    nothing reached the operator on any surface, in text or in JSON.

    The line goes to stderr so it can never corrupt a `--json` payload
    on stdout, and it is printed in both modes because `okg mcp-serve`
    has no success payload to carry the block.
    """
    from okg.substrate.db.readiness import (
        pending_migration_report,
        pending_migration_warning,
    )

    block = pending_migration_report(readiness, deployment=deployment)
    if block is None:
        return None
    line = pending_migration_warning(
        readiness, surface=surface, deployment=deployment
    )
    print(f"warning: {line}", file=sys.stderr)
    return block


def _report_database_behind(
    readiness,
    release_report: dict | None,
    *,
    surface: str,
    deployment: str | None,
) -> None:
    """Say how far behind the database is, once, after the release verdict.

    PACT unknown-s7-release-verdict. Call it only after
    `enforce_release_contract_if_declared` has returned, so a surface that is
    about to refuse never first prints "continuing". With no declared
    release there is no verdict, and the #2128 rule stands: report the
    pending migrations and continue. With a declared release that serves,
    the verdict's own sentence (for example a pending destructive cleanup)
    replaces the readiness line for migrations. The verdict is built from
    the migration ledger and says nothing about missing tool indexes
    (#3311), so those are still reported here.
    """
    if release_report is None:
        _report_pending_migrations(
            readiness, surface=surface, deployment=deployment
        )
        return
    message = release_report.get("verdict_message")
    if message:
        print(f"note: {surface}: {message}", file=sys.stderr)
    if readiness is not None and getattr(readiness, "missing_schema_objects", ()):
        import dataclasses

        _report_pending_migrations(
            dataclasses.replace(
                readiness, pending_migrations=(), pending_migration_count=0,
            ),
            surface=surface,
            deployment=deployment,
        )


def _json_safe(value: Any) -> Any:
    from enum import Enum
    from os import PathLike

    if isinstance(value, Mapping):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(v) for v in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, PathLike):
        return os.fspath(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _profiling_policy_for_args(args: argparse.Namespace):
    """Resolve profiling policy from deployment manifest + CLI override."""
    from okg.substrate.deployments import DeploymentManifest, find_deployment_dir
    from okg.substrate.profiling.policy import resolve_policy

    raw = None
    deployment = getattr(args, "deployment", None)
    if deployment:
        dep_dir = find_deployment_dir(deployment)
        manifest = dep_dir / "deployment.yaml" if dep_dir else None
        if manifest and manifest.is_file():
            try:
                raw = DeploymentManifest.from_yaml(manifest).profiling
            except Exception as exc:  # noqa: BLE001
                raise SubstrateError(
                    "could not resolve deployment profiling policy",
                    data={
                        "deployment": deployment,
                        "manifest": str(manifest),
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    fix_hint=(
                        "fix deployment.yaml or remove the deployment "
                        "profiling block before retrying"
                    ),
                ) from exc
    return resolve_policy(
        raw,
        profile=getattr(args, "profile", None),
        collectors=getattr(args, "profile_collector", ()) or (),
    )


def _profile_force_deep(args: argparse.Namespace) -> bool:
    if getattr(args, "profile", None) == "off":
        return False
    return (
        getattr(args, "profile", None) == "deep"
        or bool(getattr(args, "profile_collector", None))
    )


# ---------- per-deployment DSN resolution ----------
#
# Per `one-db-per-deployment` (2026-04-26): commands that
# take `--deployment <name>` resolve the DSN from the
# manifest's `postgres.dsn` field (with `${VAR}`
# interpolation). `OKG_DSN` is consumed only via explicit
# `${OKG_DSN}` interpolation; it is NOT a silent fallback.
# This kills the dual-source-of-truth bug that caused the
# 2026-04-26 test-destroys-graph incident.

_VAR_INTERP_RE = None


def _interpolate_env(value: str) -> str:
    """Expand `${VAR}` references in `value` against
    `os.environ`. Unset vars are left as-is so a structured
    error can surface them downstream."""
    import re
    global _VAR_INTERP_RE
    if _VAR_INTERP_RE is None:
        _VAR_INTERP_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

    def _sub(m):
        name = m.group(1)
        value = os.environ.get(name)
        return value if value else m.group(0)

    return _VAR_INTERP_RE.sub(_sub, value)


def _refuse_keyword_form_dsn(source: str, dsn: str) -> None:
    """Refuse a libpq keyword/value DSN, naming the setting it came from.

    libpq accepts `host=... dbname=...` as an equal spelling of the URL, so
    an operator who writes it is doing nothing wrong. Chronos, though,
    selects its storage backend by URL SCHEME: the keyword form has none, so
    it used to be routed to SQLite and the publish path wrote a FILE named
    after the whole connection string (issue #1971).

    `connect_sql_database` refuses that string for every caller, but it
    refuses in Chronos' terms and only once a write is attempted. Refusing
    here instead puts the message at the variable or manifest key the
    operator actually edited, before the command does any work (issue
    #2001). The URL is offered, never substituted: the two spellings are not
    interchangeable here, and rewriting one into the other would hide that.
    """
    from okg.substrate.dsn_form import (
        is_keyword_form_dsn,
        keyword_form_refusal,
    )

    if not is_keyword_form_dsn(dsn):
        return
    raise SubstrateError(
        keyword_form_refusal(source, dsn),
        data={"dsn_form": "libpq_keyword", "dsn_source": source},
        fix_hint=(
            "write the DSN in URL form: "
            "postgresql://user:password@host:port/dbname"
        ),
    )


def _manifest_dsn_source(raw_dsn: str, manifest_path) -> str:
    """Name what an operator would edit to change this manifest's DSN.

    An interpolated manifest (`dsn: ${OKG_DSN}`) sends them to the env var;
    a literal one sends them to the manifest key and file.
    """
    import re as _re

    refs = _re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", raw_dsn)
    where = f"`postgres.dsn` in {manifest_path}"
    if refs:
        return f"{', '.join(refs)} (interpolated into {where})"
    return where


def _resolve_deployment_dsn(args: argparse.Namespace) -> str:
    """Resolve the Postgres DSN for a deployment-scoped
    command. Order:

      1. `--dsn` flag
      2. `<deployment_dir>/deployment.yaml.postgres.dsn`
         (with `${VAR}` interpolation)

    `OKG_DSN` is NOT a silent fallback. It is consumed only
    when a manifest declares `postgres.dsn: ${OKG_DSN}`
    explicitly.

    Raises `SubstrateError` when no source yields a value.
    """
    cli_dsn = getattr(args, "dsn", None)
    if cli_dsn:
        _refuse_keyword_form_dsn("the --dsn value", cli_dsn)
        return cli_dsn

    deployment = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment:
        raise SubstrateError(
            "no deployment specified; pass --deployment "
            "<name> or set OKG_DEPLOYMENT (or use --dsn for "
            "a non-deployment-scoped operation)",
            fix_hint=(
                "--deployment <name> or --dsn "
                "postgres://..."
            ),
        )

    from okg.substrate.deployments import find_deployment_dir
    deployment_dir = find_deployment_dir(deployment)
    if deployment_dir is None:
        raise SubstrateError(
            f"deployment dir for {deployment!r} not found",
            data={"deployment": deployment},
            fix_hint=(
                "check the deployment exists under "
                "deployments/<name>/"
            ),
        )

    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        raise SubstrateError(
            f"deployment {deployment!r} has no deployment.yaml",
            data={
                "deployment": deployment,
                "expected_path": str(manifest_path),
            },
            fix_hint=(
                f"create {manifest_path} with a "
                f"`postgres: {{dsn: ...}}` block"
            ),
        )

    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}

    raw_dsn = (manifest.get("postgres") or {}).get("dsn")
    if not isinstance(raw_dsn, str) or not raw_dsn.strip():
        raise SubstrateError(
            f"deployment {deployment!r}'s manifest declares "
            f"no `postgres.dsn` field; per "
            f"`one-db-per-deployment` (2026-04-26) the "
            f"manifest is the only source of truth for "
            f"deployment-scoped DSNs (OKG_DSN is no longer a "
            f"silent fallback)",
            data={
                "deployment": deployment,
                "manifest_path": str(manifest_path),
            },
            fix_hint=(
                f"add to {manifest_path}:\n"
                f"postgres:\n"
                f"  dsn: ${{OKG_DSN}}      # or a literal "
                f"postgres://... DSN"
            ),
        )

    resolved = _interpolate_env(raw_dsn.strip())
    if "${" in resolved:
        # Some env var the manifest references is unset. Tier R
        # (worktree-access-tiers req.tiers.read-default): before failing,
        # run the full authority chain - it ends in the main checkout's
        # .okg/local.env, so cold worktrees resolve with zero setup.
        from okg.substrate.deployment_authority import (
            resolve_deployment_dsn_authority,
        )

        resolution = resolve_deployment_dsn_authority(
            deployment,
            local_profile_authoritative=True,
        )
        if (resolution.get("authority") or {}).get("dsn_form") == (
            "libpq_keyword"
        ):
            # The chain DID find a value and refused it for its spelling.
            # Falling through to the unset-variable branch below would tell
            # the operator to export a variable they have already set, and
            # would drop the reason that says what is actually wrong.
            raise SubstrateError(
                str(resolution.get("reason")),
                data={
                    "deployment": deployment,
                    "manifest_path": str(manifest_path),
                    "dsn_form": "libpq_keyword",
                    "authority": resolution.get("authority"),
                },
                fix_hint=(
                    "write the DSN in URL form: "
                    "postgresql://user:password@host:port/dbname"
                ),
            )
        if resolution.get("dsn"):
            return resolution["dsn"]
        import re
        missing = re.findall(
            r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", resolved,
        )
        raise SubstrateError(
            f"deployment {deployment!r}'s `postgres.dsn` "
            f"references unset env var(s): {missing}, and the "
            f"authority chain (incl. the main checkout's "
            f".okg/local.env) resolved nothing",
            data={
                "deployment": deployment,
                "manifest_path": str(manifest_path),
                "unresolved": missing,
                "authority": resolution.get("authority"),
            },
            fix_hint=(
                f"okg deployment env doctor {deployment} --json  # shows "
                f"deployment DSN authority; or export {missing[0]}=..."
            ),
        )
    _refuse_keyword_form_dsn(
        _manifest_dsn_source(raw_dsn, manifest_path), resolved,
    )
    return resolved


def _resolve_deployment_dsn_from_dir(
    args: argparse.Namespace,
    deployment_dir,
) -> str:
    """Path-based variant of `_resolve_deployment_dsn` for
    commands that take `--deployment <path>` (e.g.
    `catalog load --deployment ./deployments/foo`).

    Same priority: `--dsn` wins; otherwise read
    `<deployment_dir>/deployment.yaml.postgres.dsn` with
    `${VAR}` interpolation; otherwise error. No silent
    OKG_DSN fallback.
    """
    cli_dsn = getattr(args, "dsn", None)
    if cli_dsn:
        _refuse_keyword_form_dsn("the --dsn value", cli_dsn)
        return cli_dsn

    from pathlib import Path
    deployment_dir = Path(deployment_dir)
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        raise SubstrateError(
            f"deployment dir {deployment_dir} has no "
            f"deployment.yaml",
            data={"manifest_path": str(manifest_path)},
            fix_hint=(
                f"create {manifest_path} with a "
                f"`postgres: {{dsn: ...}}` block"
            ),
        )

    with manifest_path.open() as f:
        manifest = fastyaml.safe_load(f) or {}

    raw_dsn = (manifest.get("postgres") or {}).get("dsn")
    if not isinstance(raw_dsn, str) or not raw_dsn.strip():
        raise SubstrateError(
            f"deployment manifest at {manifest_path} declares "
            f"no `postgres.dsn` field; per "
            f"`one-db-per-deployment` (2026-04-26) the "
            f"manifest is the only source of truth for "
            f"deployment-scoped DSNs",
            data={"manifest_path": str(manifest_path)},
            fix_hint=(
                f"add to {manifest_path}:\n"
                f"postgres:\n"
                f"  dsn: ${{OKG_DSN}}      # or a literal "
                f"postgres://... DSN"
            ),
        )

    resolved = _interpolate_env(raw_dsn.strip())
    if "${" in resolved:
        import re
        missing = re.findall(
            r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", resolved,
        )
        raise SubstrateError(
            f"deployment manifest at {manifest_path}'s "
            f"`postgres.dsn` references unset env var(s): "
            f"{missing}",
            data={
                "manifest_path": str(manifest_path),
                "unresolved": missing,
            },
            fix_hint=(
                f"export {missing[0]}=... or replace the "
                f"interpolation with a literal DSN in the "
                f"manifest"
            ),
        )
    _refuse_keyword_form_dsn(
        _manifest_dsn_source(raw_dsn, manifest_path), resolved,
    )
    return resolved


def _load_projection_extractor(deployment_dir):
    """Read `<deployment>/deployment.yaml`'s `extractor:` block
    and instantiate it as the Publisher's projection-time
    EntityExtractor. Returns None when no block is declared.

    Block shape:

        extractor:
          class: okg.substrate.projection.extractor.RegexEntityExtractor
          config:
            rules:
              - [af_pub_number, '\\b...\\b']

    The class is instantiated with `**config`; nothing more.
    Sources that need a per-conn extractor (e.g. one that holds
    a psycopg connection) should be loaded by per-batch
    factories elsewhere.
    """
    from okg.substrate.publisher.deployment_stack import (
        load_projection_extractor,
    )

    return load_projection_extractor(deployment_dir)


def _deployment_embeddings_enabled(deployment_dir) -> bool:
    """Read the admitted typed embedding configuration."""

    if deployment_dir is None:
        return False
    from okg.substrate.publisher.deployment_stack import embeddings_enabled

    return embeddings_enabled(deployment_dir)


def _resolve_dsn(args: argparse.Namespace) -> str:
    """DSN resolver for commands that may or may not be
    deployment-scoped.

    Per `one-db-per-deployment` (2026-04-26):
      - In a sealed launch (`OKG_ENVIRONMENT_DESCRIPTOR` is set), the
        launcher-bound `OKG_DSN` is authoritative. Caller flags and shared
        deployment manifests cannot redirect the process to another DB.
      - When `args.deployment` is set: manifest is
        authoritative (via `_resolve_deployment_dsn`).
        OKG_DSN is consumed only via explicit
        `${OKG_DSN}` interpolation inside the manifest.
      - When `args.deployment` is unset: DBA mode — fall
        back to `--dsn` flag, then `OKG_DSN` env, else
        error.
    """
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        active_deployment = os.environ.get("OKG_DEPLOYMENT")
        requested_deployment = getattr(args, "deployment", None)
        if not active_deployment:
            raise SubstrateError(
                "sealed environment did not provide its active OKG_DEPLOYMENT",
                fix_hint="repair the active environment descriptor and launcher",
            )
        if requested_deployment and str(requested_deployment) != active_deployment:
            raise SubstrateError(
                "sealed process cannot use a deployment outside its active environment: "
                f"requested={requested_deployment!r} active={active_deployment!r}",
                fix_hint=f"use the active deployment {active_deployment!r}",
            )
        sealed_dsn = os.environ.get("OKG_DSN")
        if sealed_dsn:
            _refuse_keyword_form_dsn(
                "OKG_DSN (the sealed environment's launcher)", sealed_dsn,
            )
            return sealed_dsn
        raise SubstrateError(
            "sealed environment did not provide its authoritative OKG_DSN",
            fix_hint="repair the active environment descriptor and launcher",
        )
    if getattr(args, "deployment", None) or os.environ.get(
        "OKG_DEPLOYMENT",
    ):
        return _resolve_deployment_dsn(args)
    cli_dsn = getattr(args, "dsn", None)
    if cli_dsn:
        _refuse_keyword_form_dsn("the --dsn value", cli_dsn)
        return cli_dsn
    env_dsn = os.environ.get("OKG_DSN")
    if env_dsn:
        _refuse_keyword_form_dsn("OKG_DSN", env_dsn)
        return env_dsn
    raise SubstrateError(
        "no DSN: pass --deployment <name> (manifest "
        "authoritative), --dsn postgres://..., or set "
        "OKG_DSN for DBA-level operations",
        fix_hint=(
            "for deployment-scoped work: --deployment "
            "<name>; for DBA: --dsn or OKG_DSN"
        ),
    )


def _resolve_run_context(
    args: argparse.Namespace,
) -> tuple[str, str, Path | None, bool]:
    """Resolve `okg run` DSN and canonical durable deployment identity."""
    deployment_ref = (
        getattr(args, "deployment", None)
        or os.environ.get("OKG_DEPLOYMENT")
    )
    if not deployment_ref:
        return (
            _resolve_dsn(args),
            "default",
            None,
            False,
        )

    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    from okg.substrate.deployments.admission import require_active_deployment

    try:
        require_active_deployment(str(deployment_ref))
        manifest = resolve_deployment(str(deployment_ref))
    except DeploymentResolutionError as exc:
        raise SubstrateError(
            str(exc),
            data={"deployment": str(deployment_ref)},
            fix_hint=(
                "pass a deployment name, a deployment directory, or a "
                "deployment.yaml path"
            ),
        ) from exc

    # Later lookups by the manifest name must reach the folder resolved here,
    # not search the deployments roots again (issue #2951).
    from okg.substrate.deployments import bind_resolved_deployment

    bind_resolved_deployment(manifest.name, manifest.deployment_dir)
    canonical_args = argparse.Namespace(**vars(args))
    canonical_args.deployment = manifest.name
    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        # A sealed run has two independent authorities: the active environment
        # chooses the deployment and its launcher binds the database.  Do not
        # let the run-specific compatibility path reintroduce a caller DSN
        # after the generic resolver has already made sealed launches
        # fail-closed.
        dsn = _resolve_dsn(canonical_args)
    else:
        dsn = (
            getattr(args, "dsn", None)
            or getattr(manifest, "postgres_dsn", None)
            or _resolve_deployment_dsn(canonical_args)
        )
    return (
        dsn,
        manifest.name,
        manifest.deployment_dir,
        bool(getattr(manifest.runtime, "enabled", False)),
    )


# ---------- subcommand implementations ----------

def _cmd_migrate(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg migrate` — apply the v1 schema idempotently.

    Supports an optional ``--deployment <name>``. When supplied,
    after the substrate's ``views.sql`` is loaded the migrate
    function ALSO loads ``deployments/<name>/views.sql`` if it
    exists — letting deployments register their own
    ``okg.v_<deployment>_<view>`` Postgres VIEW objects without
    forking the substrate. Per ``add-deployment-sql-views``.

    ``--apply`` is required (issue #1288). Both the full-schema and the
    ``--views-only`` branch write, so without it the command refuses
    before resolving a DSN or opening a connection — the same refusal
    under ``OKG_AGENT=1`` and outside it. Every result, success or
    refusal, reports ``mutation_requested`` and ``mutation_started``.
    """
    from okg.substrate.db.migrate import install_views, migrate

    deployment_name = getattr(args, "deployment", None)
    views_only = bool(getattr(args, "views_only", False))
    mutation_requested = bool(getattr(args, "apply", False))
    if not mutation_requested:
        # Issue #1288. Both branches below replay DDL inside a write
        # transaction, so `--apply` is the blast-radius boundary for this
        # verb and it has to hold for every caller. Enforcing it only under
        # OKG_AGENT=1 gave one command two mutation semantics: an operator
        # reaching for `okg migrate` as a read-only probe got a schema
        # write. Refuse HERE, before the DSN is resolved and before any
        # connection is opened, so the refusal cannot depend on the
        # environment it is protecting.
        return _emit_error(
            SubstrateError(
                "`okg migrate` requires --apply: it "
                + (
                    "replaces okg.v_* views and their grants"
                    if views_only
                    else "replays schema DDL, reinstalls views, and "
                    "advances the migration ledger"
                )
                + " inside a write transaction",
                data={
                    "mutation_requested": False,
                    "mutation_started": False,
                    "views_only": views_only,
                    "deployment": deployment_name,
                },
                fix_hint=_migrate_apply_fix_hint(
                    deployment_name, views_only=views_only,
                ),
            ),
            as_json=as_json,
        )
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as err:
        err.data = {
            **(err.data or {}),
            "mutation_requested": True,
            "mutation_started": False,
        }
        return _emit_error(err, as_json=as_json)
    # Flipped immediately before the write path is entered, so a failure
    # reports whether anything could already have been written rather than
    # leaving the operator to guess.
    mutation_started = False
    try:
        mutation_started = True
        if views_only:
            r = install_views(dsn, deployment_name=deployment_name)
        else:
            r = migrate(dsn, deployment_name=deployment_name)
    except SubstrateError as e:
        e.data = {
            **(e.data or {}),
            "mutation_requested": True,
            "mutation_started": mutation_started,
        }
        return _emit_error(e, as_json=as_json)

    _emit_ok(
        {
            "views_only": views_only,
            "mutation_requested": True,
            "mutation_started": True,
            "statements_executed": r.statements_executed,
            "extension_versions": r.extension_versions,
            "idempotent_second_run": r.idempotent_second_run,
            "converged_views": list(r.converged_views),
            # Multi-phase orchestration outcome
            # (phase-interval-narrow-split-migration): which declared
            # multi-phase migrations the checkpointed driver ran in this
            # invocation, and which ordinals the barrier deferred to the
            # second ordered pass. Empty on a converged database.
            "multiphase_applied": [
                dict(row) for row in getattr(r, "multiphase_applied", ())
            ],
            "deferred_post_multiphase": list(
                getattr(r, "deferred_post_multiphase", ())
            ),
            # okg#3112: the online graph read-index step; `deferred` means a
            # lock was busy and the next migrate or publish finishes it.
            "graph_read_indexes": getattr(r, "graph_read_indexes", None),
            # okg#3112 step 2: whether migrate cancelled a declared-attribute
            # index build and held the graph-index DDL lock.
            "declared_attr_index_preemption": getattr(
                r, "declared_attr_index_preemption", None
            ),
            # okg#3342: repair contracts of pre-upgrade incomplete
            # checkpoints retired once, each with its recorded reason.
            "incomplete_checkpoint_retirements": list(
                getattr(r, "incomplete_checkpoint_retirements", ())
            ),
            # PACT recovery-contract-removed-sources: contracts whose
            # required sources are gone (retired, or why not).
            "source_contract_retirements": list(
                getattr(r, "source_contract_retirements", ())
            ),
            # PACT openrouter-embeddings-hnsw: each vector set migrate
            # registered, its watermarks and its index state; None when the
            # deployment declares no set.
            "vector_sets": (
                None if getattr(r, "vector_sets", None) is None
                else [dict(item) for item in r.vector_sets]
            ),
        },
        as_json=as_json,
    )
    return 0


def _cmd_pact(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.pact.cli import cmd_pact

    return cmd_pact(args, as_json=as_json)


def _cmd_agent_ssh_run(args: argparse.Namespace, *, as_json: bool) -> int:
    """Run one server-side restricted coordination request.

    sshd invokes this as a forced command.  It intentionally bypasses the
    ordinary CLI environment loader and output/error path: all route, policy,
    transport, and database inputs are server-owned, while the handler emits
    only a small sanitized JSON receipt on stdout.
    """

    from okg.substrate.library.agent_runtimes.coordination_ssh import (
        run_forced_command,
    )

    stdin = getattr(sys.stdin, "buffer", sys.stdin)
    return run_forced_command(
        route=args.route,
        stdin=stdin,
        stdout=sys.stdout,
        environ=os.environ,
    )


def _cmd_agent_ssh_client(args: argparse.Namespace, *, as_json: bool) -> int:
    """Run the local MCP bridge over a real restricted SSH connection."""

    from okg.substrate.library.agent_runtimes.coordination_client import (
        run_client_bridge,
    )

    return run_client_bridge()


def _add_installed_upgrade_arguments(parser: argparse.ArgumentParser) -> None:
    """#3415: flags of `okg up` for a deployment run from an installed okg."""
    parser.add_argument(
        "--from-contract",
        metavar="FILE",
        help=(
            "Installed deployments: the release contract of the okg version "
            "you upgraded from, when the deployment has no installed-release "
            "receipt"
        ),
    )
    parser.add_argument(
        "--accept-unknown-compat",
        action="store_true",
        help=(
            "Installed deployments: upgrade even though compatibility with "
            "the new okg is unknown (announced, recorded in the receipt; "
            "never overrides an incompatibility)"
        ),
    )


def _cmd_up(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.cli.deployment import _cmd_deployment_up

    return _cmd_deployment_up(args, as_json=as_json)


def _db_as_json(args: argparse.Namespace, as_json: bool) -> bool:
    """Decide if `okg db <subverb>` output should be JSON.

    Top-level `OKG_AGENT=1` forces JSON via `as_json`; otherwise
    the subverb's `--json` flag (added by `_add_json_flag`) decides.
    """
    return as_json or bool(getattr(args, "json", False))


def _cmd_db(args: argparse.Namespace, *, as_json: bool) -> int:
    from okg.substrate.db.management import (
        apply_remove_job_runtime_subsystem,
        database_health_report,
        dbos_backlog_gc,
        drop_database,
        gc_sweep,
        inspect_database,
        list_databases,
        migrate_down,
        migration_status,
        prune_orphaned_source_blobs,
    )

    as_json = _db_as_json(args, as_json)
    subcmd = getattr(args, "db_subcmd", None)
    if subcmd == "attr-index":
        return _cmd_db_attr_index(args, as_json=as_json)
    try:
        if subcmd == "list":
            if getattr(args, "cluster", False) and not args.cluster_dsn:
                raise SubstrateError(
                    "cluster inventory requires --cluster-dsn",
                    fix_hint="okg db list --cluster --cluster-dsn postgres://...",
                )
            payload = list_databases(cluster_dsn=args.cluster_dsn)
        elif subcmd == "status":
            deployment = getattr(args, "deployment", None)
            if deployment:
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            payload = inspect_database(
                dsn,
                deployment_name=deployment,
                top_limit=getattr(args, "top", 20),
            )
            if getattr(args, "brief", False) and not as_json:
                _print_db_status_brief(payload)
                return 0
        elif subcmd == "gc":
            if not args.cluster_dsn:
                raise SubstrateError(
                    "database GC requires an explicit --cluster-dsn",
                    fix_hint="okg db gc --cluster-dsn postgres://... [--apply]",
                )
            if getattr(args, "dry_run", False) and getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db gc` cannot combine --dry-run with --apply",
                    fix_hint=(
                        "drop --dry-run (dry-run is the default) or drop --apply"
                    ),
                )
            payload = gc_sweep(
                cluster_dsn=args.cluster_dsn,
                apply=bool(getattr(args, "apply", False)),
            )
        elif subcmd == "clone":
            from okg.substrate.postgres_clone import (
                clone_database,
                restore_database_dump,
            )

            if not getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db clone` requires --apply",
                    fix_hint="review the DSNs, then rerun with --apply",
                )
            source_dsn = getattr(args, "source_dsn", None) or os.environ.get(
                "OKG_CLONE_SOURCE_DSN"
            )
            target_dsn = getattr(args, "target_dsn", None) or os.environ.get(
                "OKG_CLONE_TARGET_DSN"
            )
            if not source_dsn or not target_dsn:
                raise SubstrateError(
                    "database clone requires source and target DSNs",
                    fix_hint=(
                        "pass --source-dsn/--target-dsn or set "
                        "OKG_CLONE_SOURCE_DSN/OKG_CLONE_TARGET_DSN"
                    ),
                )
            if getattr(args, "restore_dump", None) and getattr(
                args, "dump_file", None
            ):
                raise SubstrateError(
                    "`okg db clone` cannot combine --restore-dump and --dump-file"
                )
            common = {
                "admin_dsn": (
                    getattr(args, "admin_dsn", None)
                    or os.environ.get("OKG_CLONE_ADMIN_DSN")
                ),
                "drain_timeout_seconds": args.drain_timeout_seconds,
            }
            if getattr(args, "restore_dump", None):
                payload = restore_database_dump(
                    source_dsn,
                    target_dsn,
                    args.restore_dump,
                    **common,
                )
            else:
                payload = clone_database(
                    source_dsn,
                    target_dsn,
                    dump_file=getattr(args, "dump_file", None),
                    keep_dump=bool(getattr(args, "dump_file", None)),
                    **common,
                )
        elif subcmd == "dbos-gc":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            if getattr(args, "confirm", False) and not getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db dbos-gc --confirm` requires --apply",
                    fix_hint=(
                        "okg db dbos-gc --deployment <name> "
                        "--confirm --apply"
                    ),
                )
            if getattr(args, "apply", False) and not getattr(args, "confirm", False):
                raise SubstrateError(
                    "`okg db dbos-gc --apply` requires --confirm",
                    fix_hint=(
                        "okg db dbos-gc --deployment <name> "
                        "--confirm --apply"
                    ),
                )
            payload = dbos_backlog_gc(
                dsn,
                stale_after_seconds=args.stale_after_seconds,
                confirm=bool(args.confirm and args.apply),
                queue_name=args.queue_name,
                workflow_name=args.workflow_name,
                statuses=tuple(args.status or ("ENQUEUED",)),
                sample_limit=args.sample_limit,
            )
        elif subcmd == "prune-source-blobs":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            if getattr(args, "confirm", False) and not getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db prune-source-blobs --confirm` requires --apply",
                    fix_hint=(
                        "okg db prune-source-blobs --deployment <name> "
                        "--confirm --apply"
                    ),
                )
            if getattr(args, "apply", False) and not getattr(args, "confirm", False):
                raise SubstrateError(
                    "`okg db prune-source-blobs --apply` requires --confirm",
                    fix_hint=(
                        "okg db prune-source-blobs --deployment <name> "
                        "--confirm --apply"
                    ),
                )
            payload = prune_orphaned_source_blobs(
                dsn,
                confirm=bool(args.confirm and args.apply),
                grace_seconds=args.grace_seconds,
            )
        elif subcmd == "prune-blobs":
            from okg.substrate.db.blob_prune import prune_blobs, require_grace_seconds

            # Refuse a grace window under the minimum before resolving or
            # connecting to anything (blob_prune_grace_too_short).
            require_grace_seconds(args.grace_seconds)
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            if bool(args.confirm) != bool(args.apply):
                missing = "--apply" if args.confirm else "--confirm"
                given = "--confirm" if args.confirm else "--apply"
                raise SubstrateError(
                    f"`okg db prune-blobs {given}` requires {missing}",
                    fix_hint=(
                        f"okg db prune-blobs --kind {args.kind} "
                        "--deployment <name> --confirm --apply"
                    ),
                )
            payload = prune_blobs(
                dsn,
                kind=args.kind,
                confirm=bool(args.confirm and args.apply),
                grace_seconds=args.grace_seconds,
            )
        elif subcmd == "remove-job-runtime-subsystem":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            if not getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db remove-job-runtime-subsystem` requires --apply",
                    fix_hint=(
                        "okg db remove-job-runtime-subsystem --dsn postgres://... "
                        "--confirm --apply"
                    ),
                )
            payload = apply_remove_job_runtime_subsystem(
                dsn,
                confirm=bool(args.confirm),
                accept_retired_ledger_rows=bool(
                    getattr(args, "accept_retired_ledger_rows", False)
                ),
                retired_rows_dump_ref=getattr(args, "retired_rows_dump_ref", None),
            )
        elif subcmd == "drop":
            if not args.name:
                raise SubstrateError(
                    "`okg db drop` requires --name",
                    fix_hint="okg db drop --name <database> --cluster-dsn ... --confirm",
                )
            if not args.cluster_dsn:
                raise SubstrateError(
                    "`okg db drop` requires --cluster-dsn",
                    fix_hint="okg db drop --name <database> --cluster-dsn ... --confirm",
                )
            payload = drop_database(
                cluster_dsn=args.cluster_dsn,
                database=args.name,
                confirm=bool(args.confirm),
                apply=bool(getattr(args, "apply", False)),
                override_deployment=args.override_deployment,
            )
        elif subcmd == "migrations":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            payload = migration_status(dsn)
        elif subcmd == "migrate-down":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            if not getattr(args, "apply", False):
                raise SubstrateError(
                    "`okg db migrate-down` requires --apply",
                    fix_hint=(
                        "okg db migrate-down <migration_id> "
                        "--dsn postgres://... --confirm --apply"
                    ),
                )
            payload = migrate_down(
                dsn,
                args.migration_id,
                confirm=bool(getattr(args, "confirm", False)),
            )
        elif subcmd == "doctor":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            payload = database_health_report(
                dsn,
                deployment_name=getattr(args, "deployment", None),
            )
        elif subcmd == "role-passwords":
            from okg.substrate.cli.role_passwords import role_passwords_payload

            payload = role_passwords_payload(args, dsn=_resolve_dsn(args))
        elif subcmd == "current-runtime-dlq":
            dsn = _resolve_deployment_dsn(args)
            from okg.substrate.db.connection import connect as db_connect
            from okg.substrate.runtime_dlq import current_runtime_dlq

            with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
                payload = current_runtime_dlq(cur)
        elif subcmd == "acknowledge-runtime-dlq":
            dsn = _resolve_deployment_dsn(args)
            from okg.substrate.db.connection import connect as db_connect
            from okg.substrate.runtime_dlq_acknowledgement import (
                apply_runtime_dlq_acknowledgements,
                load_acknowledgement_request,
                plan_runtime_dlq_acknowledgements,
            )

            request = load_acknowledgement_request(args.request)
            confirm_plan_id = str(
                getattr(args, "confirm_plan_id", None) or ""
            ).strip()
            apply = bool(getattr(args, "apply", False))
            if confirm_plan_id and not apply:
                raise SubstrateError(
                    "--confirm-plan-id requires --apply",
                )
            if apply and not confirm_plan_id:
                raise SubstrateError(
                    "runtime DLQ acknowledgement apply requires --confirm-plan-id",
                )
            with db_connect(
                dsn,
                autocommit=True,
            ) as conn:
                if apply:
                    payload = apply_runtime_dlq_acknowledgements(
                        conn,
                        deployment=str(args.deployment),
                        request=request,
                        confirm_plan_id=confirm_plan_id,
                    )
                else:
                    payload = plan_runtime_dlq_acknowledgements(
                        conn,
                        deployment=str(args.deployment),
                        request=request,
                    )
        elif subcmd == "reclaim-index-space":
            # okg#3112: the supported REINDEX for index_larger_than_table.
            from okg.substrate.db.index_reclaim import reclaim_index_space

            dsn = _resolve_deployment_dsn(args)
            payload = reclaim_index_space(
                dsn,
                deployment=str(args.deployment),
                apply=bool(getattr(args, "apply", False)),
                min_index_bytes=getattr(args, "min_index_bytes", None),
                publish_lock_wait_seconds=float(args.publish_lock_wait_seconds),
                lock_timeout_seconds=float(args.lock_timeout_seconds),
                on_progress=(
                    None if as_json
                    else lambda msg: print(f"  {msg}", file=sys.stderr)
                ),
            )
        elif subcmd == "embed-backfill":
            payload = _cmd_db_embed_backfill(args, as_json=as_json)
        elif subcmd == "embed-show":
            payload = _cmd_db_embed_show(args)
        elif subcmd == "embed-index":
            payload = _cmd_db_embed_index(args)
        elif subcmd == "semantic-index":
            payload = _cmd_db_semantic_index(args, as_json=as_json)
        elif subcmd == "cold-history":
            if getattr(args, "deployment", None):
                dsn = _resolve_deployment_dsn(args)
            elif getattr(args, "dsn", None):
                dsn = args.dsn
            else:
                dsn = _resolve_dsn(args)
            from okg.substrate.db.cold_history import (
                cold_history_report,
                rotate_hot_indexes,
            )

            do_apply = bool(
                getattr(args, "confirm", False)
                and getattr(args, "apply", False)
            )
            if do_apply:
                payload = rotate_hot_indexes(
                    dsn,
                    headroom_generations=int(args.headroom_generations),
                    logical_tables=(
                        tuple(args.logical_table)
                        if getattr(args, "logical_table", None)
                        else None
                    ),
                    suffixes=(
                        tuple(args.suffix)
                        if getattr(args, "suffix", None)
                        else None
                    ),
                    activate=bool(getattr(args, "activate", False)),
                )
                payload["applied"] = True
            else:
                payload = cold_history_report(
                    dsn,
                    headroom_generations=int(args.headroom_generations),
                )
                payload.update(
                    {
                        "applied": False,
                        "dry_run": True,
                        "requested_logical_tables": list(
                            getattr(args, "logical_table", None) or []
                        ),
                        "requested_suffixes": list(
                            getattr(args, "suffix", None) or []
                        ),
                        "requested_activate": bool(
                            getattr(args, "activate", False)
                        ),
                        "fix_hint": (
                            "build a proof with `--suffix <suffix> --confirm "
                            "--apply`; add `--activate` only after plan and "
                            "byte-oracle gates pass"
                        ),
                    }
                )
        else:
            raise SubstrateError(
                "unknown db subcommand",
                data={"subcmd": subcmd},
                fix_hint=(
                    "use one of: list, status, gc, clone, dbos-gc, "
                    "prune-source-blobs, prune-blobs, drop, "
                    "remove-job-runtime-subsystem, "
                    "migrations, migrate-down, doctor, role-passwords, "
                    "current-runtime-dlq, "
                    "acknowledge-runtime-dlq, "
                    "reclaim-index-space, embed-backfill, embed-show, "
                    "embed-index, semantic-index, cold-history"
                ),
            )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)

    if subcmd == "doctor":
        from okg.substrate.cli._output import payload_verdict_exit

        payload, code = payload_verdict_exit(payload, source="db_doctor")
        _emit_ok(payload, as_json=as_json)
        return code
    _emit_ok(payload, as_json=as_json)
    return 0


def _deployment_embedding_config(args: argparse.Namespace) -> Any:
    """The deployment's admitted embedding config, refusing a disabled one."""
    from okg.substrate.deployments import resolve_deployment

    config = resolve_deployment(str(args.deployment)).embedding_config
    if not config.enabled or not config.backend:
        err = SubstrateError(
            f"deployment {args.deployment!r} has embeddings disabled",
            data={"deployment": str(args.deployment)},
            fix_hint=(
                "set `embeddings: {enabled: true, backend: "
                "openrouter:<model>@<dims>}` in deployment.yaml"
            ),
        )
        err.code = "semantic_embedder_not_configured"
        raise err
    return config


def _cmd_db_embed_backfill(args: argparse.Namespace, *, as_json: bool) -> dict:
    """`okg db embed-backfill` (PACT openrouter-embeddings-hnsw, design v3.2).

    Fills one registered vector set; ``--dry-run`` reports what it would
    embed and cost, with no paid call and no write.
    """
    from okg.substrate.embeddings.backfill import dry_run_backfill, run_set_backfill

    config = _deployment_embedding_config(args)
    dsn = _resolve_deployment_dsn(args)
    if bool(getattr(args, "dry_run", False)):
        return dry_run_backfill(
            dsn,
            deployment=str(args.deployment),
            config=config,
            set_name=getattr(args, "set", None),
            max_usd=args.max_usd,
            window_rows=int(args.window_rows),
        )

    def progress(state: dict) -> None:
        if not as_json:
            print(
                "  window {windows}: nodes={nodes_scanned} links={links_written} "
                "embedded={documents_embedded} reused={documents_reused} "
                "cost=${run_cost_usd}".format(**state),
                file=sys.stderr,
            )

    return run_set_backfill(
        dsn,
        deployment=str(args.deployment),
        config=config,
        set_name=getattr(args, "set", None),
        max_usd=args.max_usd,
        limit=args.limit,
        window_rows=int(args.window_rows),
        max_window_chars=int(args.max_window_chars),
        on_progress=progress,
    )


def _cmd_db_embed_show(args: argparse.Namespace) -> dict:
    """`okg db embed-show` (PACT openrouter-embeddings-hnsw): the exact
    document a node renders to under the registered set. Reads only."""
    from okg.substrate.embeddings.backfill import show_node_document

    config = _deployment_embedding_config(args)
    return show_node_document(
        _resolve_deployment_dsn(args),
        deployment=str(args.deployment),
        config=config,
        set_name=getattr(args, "set", None),
        node_id=str(args.node),
    )


def _cmd_db_embed_index(args: argparse.Namespace) -> dict:
    """`okg db embed-index` (PACT openrouter-embeddings-hnsw): build a
    registered set's index under its current settings beside the old one,
    then drop the old one. Dry run unless ``--apply``."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.embeddings.backfill import resolve_registered_set
    from okg.substrate.embeddings.store import rebuild_set_index

    config = _deployment_embedding_config(args)
    dsn = _resolve_deployment_dsn(args)
    with db_connect(dsn) as conn:
        resolved = resolve_registered_set(conn, config, getattr(args, "set", None))
        conn.rollback()
    if "index" in resolved.drift:
        err = SubstrateError(
            f"vector set {resolved.configured.name!r} has index settings in "
            "deployment.yaml that okg migrate has not registered yet",
            data={"set": resolved.configured.name, "set_id": resolved.configured.set_id},
            fix_hint="run `okg migrate --deployment <name> --apply` first",
        )
        err.code = "embedding_index_config_drift"
        raise err
    return {
        "deployment": str(args.deployment),
        **rebuild_set_index(dsn, resolved.stored, apply=bool(args.apply)),
    }


def _cmd_db_semantic_index(args: argparse.Namespace, *, as_json: bool) -> dict:
    """`okg db semantic-index` (PACT openrouter-embeddings-hnsw)."""
    from okg.substrate.embeddings.config import node_column_ann_model_id
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    config = _deployment_embedding_config(args)
    model_id = node_column_ann_model_id(config)
    if model_id is None:
        return {
            "deployment": str(args.deployment),
            "model_id": config.search_vector_set.model_id,
            "action": "none",
            "status": "not_indexed",
            "reason": (
                "the search set is wider than the node column; its index is "
                "the set's own (`okg db embed-index`)"
            ),
        }
    spec = ann_spec_for_model_id(model_id)
    if spec is None:
        return {
            "deployment": str(args.deployment),
            "model_id": model_id,
            "action": "none",
            "status": "not_indexed",
            "reason": "this model fills the column, so semantic search scans it exactly",
        }
    return {
        "deployment": str(args.deployment),
        **ensure_ann_index(
            _resolve_deployment_dsn(args),
            spec,
            apply=bool(args.apply),
            maintenance_work_mem=str(args.maintenance_work_mem),
            on_progress=(
                None if as_json else lambda msg: print(f"  {msg}", file=sys.stderr)
            ),
        ),
    }


def _cmd_db_attr_index(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg db attr-index reconcile` (okg#3112 step 2).

    ``reconcile`` is a dry run unless ``--apply``; ``--apply`` is also the
    operator's opt-in that lets the scheduled maintenance workflow keep the
    indexes up to date. (``verify`` is gone: okg#3112 step 4b removed the
    okg.attr_index it compared with, and PACT lane-s-storage-drop-copies the
    effective-attribute view ``--against-graph`` compared with.)

    ``reconcile --apply`` needs ``--deployment``: its build preconditions
    (the publisher lock, active source runs) are per deployment, and a
    database does not name its deployment, so none is assumed. ``--dsn``
    may still point the run at the database.
    """
    from okg.substrate import declared_attr_indexes as dai

    action = getattr(args, "attr_index_subcmd", None)
    try:
        if action == "reconcile" and getattr(args, "apply", False) \
                and not getattr(args, "deployment", None):
            raise SubstrateError(
                "okg db attr-index reconcile --apply needs --deployment",
                data={"reason": "deployment_required"},
                fix_hint=(
                    "pass --deployment <name> (with --dsn when the manifest "
                    "does not hold the database URL); the publish lock and "
                    "source-run checks are per deployment"
                ),
            )
        if getattr(args, "deployment", None):
            dsn = _resolve_deployment_dsn(args)
        elif getattr(args, "dsn", None):
            dsn = args.dsn
        else:
            dsn = _resolve_dsn(args)
        if action == "reconcile":
            # A dry run takes no lock and checks no precondition; it only
            # names the deployment it was asked about.
            deployment = getattr(args, "deployment", None) or None
            payload = dai.reconcile(
                dsn,
                deployment=deployment,
                apply=bool(getattr(args, "apply", False)),
                enable=bool(getattr(args, "apply", False)),
                retry_failed=bool(getattr(args, "retry_failed", False)),
                lock_timeout_seconds=getattr(args, "lock_timeout_seconds", None),
                on_progress=(
                    None if as_json
                    else lambda msg: print(f"  {msg}", file=sys.stderr)
                ),
            )
            failed = bool(payload.get("failed")) or bool(payload.get("capped"))
            code = 1 if payload.get("status") in {"refused", "not_installed"} or failed else 0
        else:
            raise SubstrateError(
                "unknown attr-index subcommand",
                data={"subcmd": action},
                fix_hint="use: reconcile",
            )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    _emit_ok(json.loads(json.dumps(payload, default=str)), as_json=as_json)
    return code


def _print_db_status_brief(payload: dict[str, Any]) -> None:
    generation = payload.get("generation") or {}
    latest = generation.get("latest") or {}
    obsolete_runtime = payload.get("obsolete_job_runtime") or {}
    dbos = payload.get("dbos_workflows") or {}
    runtime_queue = payload.get("runtime_queue_health") or {}
    migration = payload.get("migration_status") or {}
    dbos_count = sum(
        int(row.get("workflow_count") or 0)
        for row in dbos.get("by_status", [])
        if row.get("status") in {"PENDING", "ENQUEUED"}
    )
    print(f"database: {payload.get('database')}")
    print(f"size_bytes: {payload.get('size_bytes')}")
    print(
        "latest_generation: "
        f"{latest.get('generation_id')} {latest.get('status')}"
    )
    print(f"execution_authority: {payload.get('execution_authority')}")
    print(
        "obsolete_job_runtime: "
        f"{'cleanup_required' if obsolete_runtime.get('cleanup_required') else 'absent'}"
    )
    print(f"dbos_backlog_count: {dbos_count}")
    queue_counts = runtime_queue.get("status_counts") or {}
    if queue_counts:
        rendered_counts = ", ".join(
            f"{status}:{count}"
            for status, count in sorted(queue_counts.items())
        )
        print(f"runtime_queue_status: {rendered_counts}")
    print(
        "migration_drift: "
        f"{'no' if migration.get('ok') else 'yes'}"
    )


# `_doctor_database` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_extraction_wrap` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_extraction` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_extractors` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_sources` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_integrity` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_admission` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_formal_assurance` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_branches` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_edge_fact_strandings` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_acceptance_criteria` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_source_auth` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_external_live_authority` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_nomos` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_lens_promotions` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_doctor_integrity_coverage` moved to `cli/doctor.py` by `extract-cli-doctor`.

# `_build_doctor_registry` moved to `cli/doctor.py` by `extract-cli-doctor`.

def _discover_materializers(
    *,
    dsn: str,
    deployment_name: str,
    deployment_root,
):
    """Open a short-lived connection and ask the views-registry
    discovery helper for the deployment's registered
    materializers.

    Returns ``[]`` (silently) when the deployment has no
    `views_registry.yaml` (no views ever registered) or the registry is
    disabled. Discovery reads the durable registry directly; native
    publication still writes completed derived-artifact manifests after
    a generation publishes.

    Raises SubstrateError with a pointed message when the
    registry IS populated but importlib resolution fails (typo
    in `view_def_factory`, deleted module, etc.) — same fail-
    loud discipline as enrichers.
    """
    from okg.substrate.publisher.deployment_stack import (
        discover_materializers,
    )

    return discover_materializers(
        dsn=dsn,
        deployment_name=deployment_name,
        deployment_root=deployment_root,
    )


def _deployment_binding_scope(handler):
    """Run a command inside `deployment_binding_scope()` (issue #2951).

    A command that resolves `--deployment <folder>` binds the manifest name to
    that folder for the rest of the command, so later lookups by name reach the
    same folder instead of reporting "not found". The binding ends with the
    command.
    """

    @functools.wraps(handler)
    def wrapper(*args: Any, **kwargs: Any) -> int:
        from okg.substrate.deployments import deployment_binding_scope

        with deployment_binding_scope():
            return handler(*args, **kwargs)

    return wrapper


def _deployment_runtime_enabled(deployment: str) -> bool:
    """True iff the deployment's manifest has ``runtime.enabled: true``.

    Runtime routing must not guess when deployment resolution fails. A
    missing or invalid manifest raises so callers can return a structured
    operator error instead of silently choosing an in-process route.
    """
    try:
        from okg.substrate.deployments import resolve_deployment
        manifest = resolve_deployment(deployment)
        return bool(getattr(manifest.runtime, "enabled", False))
    except Exception as exc:  # noqa: BLE001
        err = SubstrateError(
            "could not resolve deployment runtime configuration",
            data={
                "deployment": deployment,
                "error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=(
                "fix deployment.yaml or run "
                f"`{okg_prefix()} deployment ready <deployment> --json` "
                "for the deployment readiness report"
            ),
        )
        err.code = "deployment_runtime_config_unresolved"
        raise err from exc


def _source_sync_runtime_config_warning(
    *,
    deployment: str,
    error: Exception,
) -> dict[str, Any]:
    return {
        "code": "runtime_source_sync_config_unresolved",
        "deployment": deployment,
        "message": (
            "deployment runtime source-sync configuration could not be resolved"
        ),
        "error": f"{type(error).__name__}: {error}",
        "owner": "deployment.runtime.source_sync",
        "recovery_command": (
            "fix deployment.yaml or run "
            f"`{okg_prefix()} deployment ready <deployment> --json`"
        ),
    }


def _append_source_sync_config_warnings(
    health: dict[str, Any],
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    if not warnings:
        return health
    from okg.substrate.source_sync_status import (
        build_source_sync_status_contract,
    )

    health["config_warnings"] = [
        *list(health.get("config_warnings") or []),
        *warnings,
    ]
    health["source_sync_status"] = build_source_sync_status_contract(health)
    health["ok"] = bool(health["source_sync_status"].get("ok"))
    return health


_DBOS_ACTIVE_WORKFLOW_STATUSES = ("ENQUEUED", "PENDING", "DELAYED")


def _active_source_sync_runtime_work(
    *,
    deployment: str,
    dsn: str,
) -> dict[str, Any]:
    """Return active DBOS source-sync work using dogfood status semantics."""
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.runtime.partition_queue import source_sync_queue_name

    queue_name = source_sync_queue_name(deployment)
    with db_connect(dsn, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        cur.execute("SELECT to_regclass('dbos.workflow_status')")
        if cur.fetchone()[0] is None:
            return {
                "available": False,
                "queue_name": queue_name,
                "active_count": 0,
                "active_rows": [],
                "reason": "dbos.workflow_status missing",
            }
        cur.execute(
            """
            SELECT queue_name, status, count(*)
              FROM dbos.workflow_status
             WHERE queue_name = %s
               AND name = 'okg_source_sync'
               AND upper(status) = ANY(%s)
             GROUP BY queue_name, status
             ORDER BY queue_name, status
            """,
            (queue_name, list(_DBOS_ACTIVE_WORKFLOW_STATUSES)),
        )
        active_rows = [
            {
                "queue_name": row[0],
                "status": row[1],
                "count": int(row[2]),
            }
            for row in cur.fetchall()
        ]
    return {
        "available": True,
        "queue_name": queue_name,
        "active_count": sum(int(row["count"]) for row in active_rows),
        "active_rows": active_rows,
    }


def _wait_for_idle_source_sync_runtime(
    *,
    deployment: str,
    dsn: str,
    wait_seconds: float,
) -> dict[str, Any]:
    """Wait for the source-sync DBOS lane to be idle before benchmarking."""
    import time

    started = time.monotonic()
    observations: list[dict[str, Any]] = []
    wait_seconds = max(0.0, float(wait_seconds or 0.0))
    while True:
        active = _active_source_sync_runtime_work(
            deployment=deployment,
            dsn=dsn,
        )
        observation = {
            "available": bool(active.get("available", True)),
            "queue_name": active.get("queue_name"),
            "active_count": int(active.get("active_count") or 0),
            "active_rows": active.get("active_rows") or [],
            "reason": active.get("reason"),
        }
        observations.append(observation)
        if int(observation["active_count"]) <= 0:
            return {
                "ok": True,
                "reason": "source_sync_idle",
                "deployment": deployment,
                "wait_seconds": wait_seconds,
                "waited_ms": round((time.monotonic() - started) * 1000.0, 3),
                "latest": observation,
                "observation_count": len(observations),
            }
        elapsed = time.monotonic() - started
        if elapsed >= wait_seconds:
            return {
                "ok": False,
                "reason": "source_sync_not_idle",
                "deployment": deployment,
                "wait_seconds": wait_seconds,
                "waited_ms": round(elapsed * 1000.0, 3),
                "latest": observation,
                "observations": observations[-5:],
                "observation_count": len(observations),
            }
        time.sleep(min(1.0, max(0.05, wait_seconds - elapsed)))


def _guard_inline_publish_against_active_source_sync(
    *,
    args: argparse.Namespace,
    deployment: str,
    dsn: str,
    as_json: bool,
) -> int | None:
    if not bool(getattr(args, "inline", False)):
        return None
    if bool(getattr(args, "no_publish", False)):
        return None
    try:
        runtime_enabled = _deployment_runtime_enabled(deployment)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    if not runtime_enabled:
        return None
    try:
        active = _active_source_sync_runtime_work(
            deployment=deployment,
            dsn=dsn,
        )
    except Exception as exc:  # noqa: BLE001
        err = SubstrateError(
            "could not verify source-sync DBOS lane before inline publish",
            data={
                "deployment": deployment,
                "error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=(
                "rerun without --inline so DBOS admits the work, or check "
                "`okg sync health --deployment <name> --json` before retrying"
            ),
        )
        err.code = "runtime_source_sync_probe_failed"
        return _emit_error(err, as_json=as_json)

    active_count = int(active.get("active_count") or 0)
    if active_count <= 0:
        return None
    err = SubstrateError(
        "`okg ingest --inline` with publish enabled would bypass "
        "runtime source-sync admission while source-sync work is active",
        data={
            "deployment": deployment,
            "queue_name": active.get("queue_name"),
            "active_count": active_count,
            "active_rows": active.get("active_rows") or [],
        },
        fix_hint=(
            "wait for `okg sync health --deployment "
            f"{deployment} --json` to show active_count=0, or rerun without "
            "--inline so DBOS admits the work; if the runtime worker is "
            f"stopped, start it (`okg runtime worker --deployment {deployment}`) "
            "or cancel the stranded workflow (`okg runtime cancel-workflow "
            f"<workflow-id> --deployment {deployment}`)"
        ),
    )
    err.code = "runtime_source_sync_active"
    return _emit_error(err, as_json=as_json)


def _source_entry_requires_sync_root_for_publish(entry: Any) -> bool:
    """Whether a selected source projects checkout-derived truth.

    Tier W guards repo/file projections. Tier E append ledgers are shared
    across worktrees, but only when the registry entry has no filesystem watch
    and no path-glob authority. Unknown or broader profiles stay guarded.
    """
    if getattr(entry, "source_class", None) != "append_feed":
        return True
    sync = getattr(entry, "sync", None)
    if sync is not None and getattr(sync, "watch", None) is not None:
        return True
    policy = getattr(entry, "admission_policy", None)
    authority_scope = (
        getattr(policy, "authority_scope", None)
        if policy is not None else None
    ) or {}
    if authority_scope.get("path_globs"):
        return True
    return False


def _ingest_plan_is_file_derived(
    registry: Any,
    ordered_sources: Iterable[str],
) -> bool:
    return any(
        _source_entry_requires_sync_root_for_publish(registry[source_name])
        for source_name in ordered_sources
    )


# ---------------------------------------------------------------------------
# `okg run --once` DBOS-path reporting (issue #1311)
# ---------------------------------------------------------------------------

# Flags whose implementation lives only in the in-process branch of
# `_cmd_run`. Accepting them on the DBOS enqueue path would silently drop
# them, so the command refuses instead.
_RUN_INLINE_ONLY_FLAGS: tuple[tuple[str, str], ...] = (
    ("reconcile_count_stamp", "--reconcile-count-stamp"),
    ("include_llm_invariants", "--include-llm-invariants"),
    ("llm_invariant", "--llm-invariant"),
)

# DBOS terminal states that mean the publisher cycle raised or was stopped.
_DBOS_FAILED_STATUSES = frozenset({
    "ERROR",
    "CANCELLED",
    "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
})

# DBOS non-terminal states. Seeing one of these back from the enqueue helper
# means the poll timed out: the cycle is still queued or still running and
# this command never observed its outcome.
_DBOS_UNFINISHED_STATUSES = frozenset({"PENDING", "ENQUEUED"})

# Cycle-summary `status` values (NOT DBOS workflow states): the blocked set
# (the publisher minted a generation and then a Phase B gate refused to
# expose it) and the failed set (the cycle itself failed; never a no-op, so
# never `skipped`) are imported from `cli/run_summary.py`, which the
# in-process path classifies with, so both paths read one table (#2346).


def _run_dbos_unsupported_flags(args: argparse.Namespace) -> list[str]:
    """Return the in-process-only `okg run` flags the caller passed."""
    return [
        flag
        for attr, flag in _RUN_INLINE_ONLY_FLAGS
        if getattr(args, attr, None)
    ]


def _classify_dbos_run_outcome(result: Any) -> dict[str, Any]:
    """Separate the DBOS workflow status from the publish outcome.

    ``result.status`` is the *workflow's* terminal state, which reaches
    SUCCESS whenever the cycle function returned normally -- including when
    the cycle skipped and published nothing. `okg run --once` used to print
    that workflow state as `status=` alongside a `published_status=skipped`
    line and exit 0, so a publisher that never published read as healthy
    (#1311). Classify explicitly instead:

      published    -- the cycle committed a generation and exposed it.
      no_change    -- the cycle ran and found nothing new; the current
                      generation stays.
      blocked      -- the cycle minted a generation and then a Phase B gate
                      refused to publish it.
      skipped      -- the runtime workflow did not run the cycle (status
                      ``skipped``, for example while a source sync is
                      active) and published nothing.
      failed       -- the workflow errored, was cancelled, exhausted its
                      recovery attempts, or the cycle summary itself
                      reported a failure.
      not_awaited  -- the poll timed out; the outcome is still unknown and
                      the cycle may yet fail.
      unknown      -- terminal state we cannot map to a publish outcome
                      (including SUCCESS with no cycle summary attached).

    Once the workflow finished with a cycle summary, the summary is
    classified by ``run_summary.publish_outcome``, the same verdict the
    in-process path uses (#2346): ``published`` and ``no_change`` and
    ``skipped`` exit 0, ``blocked`` exits 3, ``failed`` and ``unknown``
    exit 1. The workflow states above that prove nothing exit 1.

    The generation id alone does NOT prove a publish. `Publisher` mints the
    id BEFORE running the Phase B gate and only then sets
    ``checkpoint_status = "blocked" if blocked_reason else "published"``
    (`publisher.py`). So a gate-blocked cycle returns workflow SUCCESS, a
    generation id, and ``status: "blocked"`` together. Classifying on the id
    would call that `published` with exit 0 -- a silent fallback over a
    generation nobody can read. Read ``status`` / ``blocked_reason`` /
    ``phase_b_blocked`` instead.
    """
    status = (getattr(result, "status", "") or "").upper()
    summary = result.summary if isinstance(result.summary, dict) else None
    generation_id = summary.get("generation_id") if summary else None
    publish_status = summary.get("status") if summary else None
    blocked_reason = summary.get("blocked_reason") if summary else None
    skip_reason = None
    if summary:
        skip_reason = summary.get("skipped") or summary.get("skip_reason")

    def _verdict(outcome: str, detail: str, published: bool,
                 exit_code: int) -> dict[str, Any]:
        return {
            "outcome": outcome,
            "detail": detail,
            "published": published,
            "exit_code": exit_code,
            "generation_id": generation_id,
            "publish_status": publish_status,
            "blocked_reason": blocked_reason,
            "skip_reason": skip_reason,
        }

    if status in _DBOS_FAILED_STATUSES:
        return _verdict(
            "failed",
            f"the enqueued publisher cycle terminated {status}; "
            "nothing was published",
            False,
            1,
        )
    if status in _DBOS_UNFINISHED_STATUSES:
        return _verdict(
            "not_awaited",
            "the cycle is still enqueued or still running; this command "
            "stopped waiting before it finished, so its outcome is unknown",
            False,
            1,
        )
    if status != "SUCCESS":
        return _verdict(
            "unknown",
            f"unrecognized workflow state {status or '<empty>'}; the "
            "publish outcome could not be determined",
            False,
            1,
        )
    if summary is None:
        return _verdict(
            "unknown",
            "the workflow finished but returned no cycle summary, so "
            "whether anything was published is unproven",
            False,
            1,
        )
    # One verdict for both `okg run` paths (#2346): the same cycle summary
    # classifies the same way, in-process or on the queue. Blocked is
    # decided before published, because the id exists on both (see above).
    outcome = _run_publish_outcome(summary)
    exit_code = _RUN_EXIT_CODES[outcome]
    published = outcome in ("published", "no_change")
    gen = generation_id or "<no generation id reported>"
    details = {
        "blocked": (
            f"the cycle minted generation {gen} and then a Phase B gate "
            f"blocked it (reason: {blocked_reason or 'not reported'}); "
            "the generation was NOT published"
        ),
        "failed": (
            f"the cycle reported status {publish_status}; nothing was "
            "published"
        ),
        "published": f"the cycle committed generation {gen}",
        "no_change": (
            f"the cycle found nothing new; generation {gen} is still "
            "current"
        ),
        "skipped": (
            "the runtime did not run the cycle and published nothing "
            f"(reason: {skip_reason or 'not reported'}); this is not a "
            "successful publish"
        ),
        "unknown": (
            f"the cycle summary reported status "
            f"{publish_status if publish_status else '<missing>'}, which "
            "no rule classifies, so whether anything was published is "
            "unproven"
        ),
    }
    return _verdict(outcome, details[outcome], published, exit_code)


def _apply_inline_publish_admission_feedback(
    *,
    dsn: str,
    deployment: str,
    summary: Mapping[str, Any],
    conn: Any | None = None,
) -> dict[str, Any] | None:
    """Bind inline publish receipts to the cycle's terminal generation."""
    generation_id = summary.get("generation_id")
    status = str(summary.get("status") or "").strip().lower()
    if generation_id is None or (
        status != "published" and status not in _CYCLE_BLOCKED_STATUSES
    ):
        return None

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.admission import apply_generation_feedback

    def apply(active_conn: Any) -> dict[str, Any]:
        with active_conn.transaction():
            return apply_generation_feedback(
                active_conn,
                deployment_name=deployment,
                generation_id=generation_id,
                workflow_ref={"workflow_name": "okg_run_once_inline"},
            )

    try:
        if conn is not None:
            return apply(conn)
        with db_connect(dsn, autocommit=True) as opened_conn:
            return apply(opened_conn)
    except Exception as exc:  # noqa: BLE001 - wrap every post-publish failure.
        raise SubstrateError(
            "inline publish created a terminal generation but admission "
            "feedback failed",
            data={
                "deployment": deployment,
                "generation_id": str(generation_id),
                "generation_status": status,
                "generation_published": status == "published",
                "stage": "admission_feedback",
                "feedback_error": f"{type(exc).__name__}: {exc}",
            },
            fix_hint=(
                "apply generation feedback to the named generation before "
                "running another publish; do not reingest the source"
            ),
        ) from exc


def _run_inline_publish_cycle(
    *,
    publisher: Any,
    dsn: str,
    deployment: str,
    force_enrichers: bool,
) -> dict[str, Any]:
    """Publish inside one publisher/branch fence, then (okg#3112) rebuild
    bloated indexes once the fence and its publisher lock are released.

    The cycle runs on the fence's connection, so ``Publisher.cycle`` leaves
    the rebuild to this caller. It never raises and does nothing after a
    blocked or no-op publish.
    """
    out = _run_inline_publish_cycle_locked(
        publisher=publisher,
        dsn=dsn,
        deployment=deployment,
        force_enrichers=force_enrichers,
    )
    if isinstance(out, dict):
        from okg.substrate.db.index_reclaim import rebuild_indexes_after_publish

        out["post_publish_index_rebuild"] = rebuild_indexes_after_publish(
            dsn,
            deployment=str(getattr(publisher, "deployment_name", deployment)),
            summary=out,
        )
    return out


def _run_inline_publish_cycle_locked(
    *,
    publisher: Any,
    dsn: str,
    deployment: str,
    force_enrichers: bool,
) -> dict[str, Any]:
    """Publish and bind receipts inside one publisher/branch fence."""
    # RESERVED: the inline publish fence is the publish path, structurally
    # the same hold as the workflow one in ingest/service.py.
    from okg.substrate.db.pool import RESERVED, pooled_connection
    from okg.substrate.graph_store import ChronosGraphStore

    with pooled_connection(dsn, purpose=RESERVED) as guarded_conn:
        graph = ChronosGraphStore.from_connection(
            guarded_conn,
            ensure_schema=False,
        )
        # Match the publisher's normal lock order. Both session locks are
        # reentrant on this connection, so Publisher.cycle can enter its
        # ordinary lock path while this outer fence prevents a source writer
        # from landing after the checkpoint but before the feedback sweep.
        with publisher._publish_lock_on_connection(
            guarded_conn,
            mode=publisher._resolved_coordination_mode(),
        ):
            if not graph._lock_branch_write_session(publisher.branch_name):
                raise SubstrateError(
                    "inline publish requires PostgreSQL branch fencing",
                    data={"branch": publisher.branch_name},
                )
            try:
                out = publisher.cycle(
                    conn=guarded_conn,
                    force_enrichers=force_enrichers,
                )
                if isinstance(out, dict):
                    feedback = _apply_inline_publish_admission_feedback(
                        dsn=dsn,
                        deployment=deployment,
                        summary=out,
                        conn=guarded_conn,
                    )
                    if feedback is not None:
                        out["admission_feedback"] = feedback
                return out
            finally:
                graph._unlock_branch_write_session(publisher.branch_name)


@_deployment_binding_scope
def _cmd_run(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg run --once` — one synchronous publish cycle.

    Two execution paths:

      * In-process (default when ``runtime.enabled: false`` or
        ``--inline``): runs ``Publisher.cycle()`` in this process.
        Uses the publisher coordination guard for mutual exclusion.
      * DBOS (default when ``runtime.enabled: true`` and no
        ``--inline``): enqueues a ``publisher_cycle_workflow`` on the
        deployment's partition queue and polls for terminal state.
        The workflow path is durable; publisher-level database locking
        provides mutual exclusion across source-sync and manual publishes.

    Issue #1311: the DBOS branch reports an explicit publish ``outcome``
    (published / blocked / skipped / failed / not_awaited / unknown)
    separately from the DBOS ``workflow_status``. A workflow reaches
    SUCCESS whenever the
    cycle function returned, including when it published nothing, so the
    two must never be printed as one ``status`` field. Flags that only the
    in-process branch implements are refused on the DBOS path rather than
    silently dropped.

    Per `wire-deployment-enrichers` (2026-04-26): when the
    deployment dir exists, its `deployment.yaml`'s
    `enrichers:` list is loaded and passed to the
    Publisher. Bad class paths / constructor failures
    surface as structured errors before any cycle work.
    """
    if not args.once:
        return _emit_error(
            SubstrateError(
                "`okg run` without --once is not yet implemented in M1",
                fix_hint="use --once for synchronous one-shot publish",
            ),
            as_json=as_json,
        )

    from okg.substrate.cli.apply_boundary import missing_apply_error

    if err := missing_apply_error(
        args,
        command="run --once",
        fix_command="okg run --once --apply ...",
    ):
        return _emit_error(err, as_json=as_json)

    from okg.substrate.deployments import inject_deployment_path
    from okg.substrate.publisher.factory import build_deployment_publisher
    from okg.substrate.profiling import profiling_context

    try:
        dsn, deployment, dep_dir, runtime_enabled = _resolve_run_context(args)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    # One readiness answer BEFORE any graph work (issue #2128). Without it a
    # scaffolded-but-unmigrated database surfaced as a bare
    # `psycopg.errors.InvalidSchemaName: schema "okg" does not exist`
    # traceback from inside the publisher's catalog-ownership probe -- no
    # code, no fix_hint, and nothing naming `okg migrate`.
    from okg.substrate.db.readiness import require_database_ready

    try:
        readiness = require_database_ready(
            dsn, surface="okg run --once", deployment=deployment,
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    pending_block = _report_pending_migrations(
        readiness, surface="okg run --once", deployment=deployment
    )
    # Phase 1 §1.2.2 of `migrate-substrate-runtime-to-dbos`: when the
    # deployment's runtime layer is enabled AND the caller didn't pass
    # --inline, enqueue the publisher workflow via DBOSClient instead
    # of running Publisher.cycle() in this process. The partition
    # queue's concurrency=1 serializes against other enqueues; we
    # poll until terminal state and return the cycle summary as if we
    # had run it inline.
    run_via_dbos = (
        not getattr(args, "inline", False)
        and runtime_enabled
    )
    if run_via_dbos:
        # #1311 defect 2: several `okg run --once` flags are implemented
        # only in the in-process branch below. They used to be accepted and
        # silently dropped here, so the operator got a cycle that ignored
        # them and a result line that still read as success. Refuse with the
        # flag names and the working alternative instead.
        unsupported = _run_dbos_unsupported_flags(args)
        if unsupported:
            return _emit_error(
                SubstrateError(
                    "these `okg run --once` flags are implemented only in "
                    "the in-process publish path and have no effect when "
                    "the cycle is enqueued on the DBOS partition queue: "
                    + ", ".join(unsupported),
                    fix_hint=(
                        "rerun with --inline (runs Publisher.cycle() in "
                        "this process and honors these flags), or drop "
                        "the flags to enqueue a plain cycle"
                    ),
                    data={
                        "deployment": deployment,
                        "path": "dbos",
                        "unsupported_flags": unsupported,
                    },
                ),
                as_json=as_json,
            )
        from okg.substrate.runtime.admin import (
            PUBLISHER_CYCLE_DEQUEUE_TIMEOUT_SECONDS,
            client_enqueue_publisher_cycle,
        )
        # --branch used to be dropped here (the helper's `branch_name`
        # default won), so `--branch experiment` silently published to
        # `default` on the DBOS path. --force-enrichers is already carried
        # into the workflow's `Publisher.cycle(force_enrichers=...)`; pass
        # both explicitly and echo them so the operator can see what the
        # enqueued cycle actually ran with.
        branch_name = getattr(args, "branch", None) or "default"
        force_enrichers = bool(getattr(args, "force_enrichers", False))
        try:
            result = client_enqueue_publisher_cycle(
                deployment=deployment,
                dsn=dsn,
                branch_name=branch_name,
                force_enrichers=force_enrichers,
                # #2502: refuse fast when no worker takes the cycle
                # instead of polling an untaken row for 600 s.
                dequeue_timeout_seconds=PUBLISHER_CYCLE_DEQUEUE_TIMEOUT_SECONDS,
            )
        except SubstrateError as e:
            # A typed refusal (e.g. publisher_cycle_not_dequeued, #2502)
            # keeps its own code, data and fix hint.
            return _emit_error(e, as_json=as_json)
        except Exception as e:  # noqa: BLE001
            return _emit_error(
                SubstrateError(
                    f"DBOS-path publisher enqueue failed: {e}; "
                    "rerun with --inline to use the explicit in-process "
                    "publish path",
                ),
                as_json=as_json,
            )
        outcome = _classify_dbos_run_outcome(result)
        from okg.substrate.cli.run_summary import (
            enricher_failure_hint,
            enricher_failure_text,
            enricher_failures,
        )
        # A swallowed enricher crash does not change the publish outcome or
        # the exit code, but it is never reported as a bare "published".
        dbos_enricher_failures = enricher_failures(
            result.summary if isinstance(result.summary, dict) else None
        )
        next_command = (
            f"okg runtime debug-workflow {result.workflow_id} "
            f"--deployment {deployment}"
        )
        # #1311: `--force-enrichers` does reach the enqueued cycle, but it
        # only bypasses the pre-enrichment noop-reuse skip. A cycle that
        # stopped for any other reason still publishes nothing, which is
        # what made the flag look like it was being ignored.
        hint = None
        if force_enrichers and not outcome["published"]:
            hint = (
                "--force-enrichers was passed to the enqueued cycle, but "
                "the cycle published nothing, so no enricher output was "
                "committed; --force-enrichers only bypasses the "
                "noop-reuse skip. Rerun with `--inline --force-enrichers` "
                "to drive the recompute in this process."
            )
        payload = {
            "path": "dbos",
            # `outcome` is the publish result. `workflow_status` is the DBOS
            # workflow's terminal state. They are NOT the same thing: a
            # workflow reaches SUCCESS whenever the cycle function returned,
            # including when it published nothing. The old payload exposed
            # only a single `status` key holding the workflow state, which is
            # how a dead publisher read as healthy (#1311).
            "outcome": outcome["outcome"],
            "outcome_detail": outcome["detail"],
            "exit_code": outcome["exit_code"],
            "published": outcome["published"],
            "workflow_id": result.workflow_id,
            "workflow_status": result.status,
            "branch_name": branch_name,
            "force_enrichers": force_enrichers,
            "elapsed_seconds": result.elapsed_seconds,
            "generation_id": outcome["generation_id"],
            "publish_status": outcome["publish_status"],
            "blocked_reason": outcome["blocked_reason"],
            "skip_reason": outcome["skip_reason"],
            "enricher_failures": dbos_enricher_failures,
            "summary": result.summary,
            "error": result.error,
            "hint": hint,
            "next_command": next_command,
        }
        if pending_block is not None:
            payload["database_readiness"] = pending_block
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            headline = (
                f"okg run --once (DBOS path): outcome="
                f"{outcome['outcome']} -- {outcome['detail']}"
            )
            if dbos_enricher_failures:
                headline += "; " + enricher_failure_text(
                    dbos_enricher_failures
                )
            print(headline)
            if dbos_enricher_failures:
                print("  " + enricher_failure_hint(
                    deployment, str(outcome["outcome"]),
                ))
            print(
                f"  workflow_id={result.workflow_id} "
                f"workflow_status={result.status} "
                f"elapsed={result.elapsed_seconds:.1f}s "
                f"branch={branch_name} "
                f"force_enrichers={force_enrichers}"
            )
            print(
                f"  generation_id={outcome['generation_id']} "
                f"publish_status={outcome['publish_status']} "
                f"skip_reason={outcome['skip_reason']}"
            )
            if outcome["blocked_reason"]:
                print(f"  blocked_reason: {outcome['blocked_reason']}")
            if result.error:
                print(f"  error: {result.error}")
            if hint:
                print(f"  {hint}")
            if not outcome["published"]:
                print(
                    "  nothing was published by this command; inspect the "
                    "cycle with:"
                )
                print(f"    {next_command}")
            if getattr(args, "verbose", False) and isinstance(
                result.summary, dict
            ):
                print()
                print("Full cycle report:")
                _emit_ok(result.summary, as_json=False)
        return int(outcome["exit_code"])

    try:
        if dep_dir is not None:
            inject_deployment_path(dep_dir)
        # Preserve operator-supplied owner metadata for publisher diagnostics.
        # The legacy `okg lease ls` table surface is retired; current publisher
        # coordination uses advisory/DBOS state plus structured status payloads.
        publisher = build_deployment_publisher(
            dsn=dsn,
            deployment_name=deployment,
            deployment_dir=dep_dir,
            command="okg run",
            branch_name=getattr(args, "branch", None) or "default",
        )
        owner_id = getattr(args, "owner_id", None) or None
        owner_meta_raw = getattr(args, "owner_meta", None) or "{}"
        try:
            owner_meta = json.loads(owner_meta_raw)
        except json.JSONDecodeError:
            return _emit_error(
                SubstrateError(
                    f"--owner-meta must be JSON; got {owner_meta_raw!r}",
                ),
                as_json=as_json,
            )
        if not isinstance(owner_meta, dict):
            return _emit_error(
                SubstrateError(
                    f"--owner-meta must be a JSON object (mapping); got "
                    f"{type(owner_meta).__name__} from {owner_meta_raw!r}",
                ),
                as_json=as_json,
            )
        owner_meta.setdefault("command", "okg run")
        publisher.set_owner(owner_id=owner_id, owner_meta=owner_meta)
        policy = _profiling_policy_for_args(args)
        with profiling_context(
            deployment=deployment,
            target_kind="publish",
            command="okg run --once",
            policy=policy,
            force_deep=_profile_force_deep(args),
        ) as prof:
            previous_count_reconcile = os.environ.get(
                "OKG_FORCE_COUNT_STAMP_RECONCILE"
            )
            if getattr(args, "reconcile_count_stamp", False):
                os.environ["OKG_FORCE_COUNT_STAMP_RECONCILE"] = "1"
            try:
                out = _run_inline_publish_cycle(
                    publisher=publisher,
                    dsn=dsn,
                    deployment=deployment,
                    force_enrichers=getattr(args, "force_enrichers", False),
                )
            finally:
                if getattr(args, "reconcile_count_stamp", False):
                    if previous_count_reconcile is None:
                        os.environ.pop("OKG_FORCE_COUNT_STAMP_RECONCILE", None)
                    else:
                        os.environ["OKG_FORCE_COUNT_STAMP_RECONCILE"] = (
                            previous_count_reconcile
                        )
            if isinstance(out, dict) and out.get("generation_id"):
                # prof.generation_id is an int telemetry token. chronos_native returns a
                # Chronos checkpoint STRING as generation_id; the int token is
                # compat_generation_id. Prefer it; fall back to the legacy int. Guard the
                # cast (int() on the string would raise ValueError, which the surrounding
                # `except SubstrateError` would NOT catch -> would crash `okg run --once`).
                gen_token = out.get("compat_generation_id")
                if gen_token is None:
                    gen_token = out.get("generation_id")
                try:
                    prof.generation_id = int(gen_token)
                except (TypeError, ValueError):
                    # Non-integer Chronos generation ids are expected here.
                    pass
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    # Per `add-semantic-llm-judge-invariants` (2026-04-29): after
    # the publish cycle commits, optionally run LLM-judge
    # invariants. They never block publish — failures land as
    # warns in the persisted invariant_summary block.
    if getattr(args, "include_llm_invariants", False):
        try:
            from okg.substrate.catalog.invariants_llm import (
                run_llm_judge_invariants,
            )
            only = (
                [args.llm_invariant]
                if getattr(args, "llm_invariant", None)
                else None
            )
            judge_out = run_llm_judge_invariants(
                dsn, deployment, only=only,
                # The judge's row source now reads the graph through a pinned
                # checkout, so it must be pinned to the branch this run
                # published -- defaulting to 'default' would read a different
                # branch's graph and report confidently wrong candidates.
                branch_name=publisher.branch_name,
            )
            # Attach to the publisher's reported output so the
            # operator sees the verdict roll-up in the same JSON.
            if isinstance(out, dict):
                out["llm_judge"] = judge_out
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)

    if pending_block is not None and isinstance(out, dict):
        out["database_readiness"] = pending_block
    return _emit_run_result(
        out,
        deployment=deployment,
        as_json=as_json,
        verbose=bool(getattr(args, "verbose", False)),
    )


def _emit_run_result(
    out: Any, *, deployment: str, as_json: bool, verbose: bool
) -> int:
    """Print one inline `okg run --once` result and return its exit code.

    Issue #2282 shortened the output; issue #2346 made the exit code follow
    the same verdict (`run_summary.publish_outcome`): 0 when the cycle
    published or found nothing new, 3 when a gate blocked the graph version
    it built, 1 when the cycle reported a failure or a status nothing
    classifies. Warnings on a published cycle keep exit 0.

    The default text is a short summary: outcome, graph version, record and
    relationship deltas, graph totals, LLM-invariant counts when they ran,
    and every warning with a fix line that inspects the result. `--verbose` adds the full cycle summary that used to be the default
    (about 200 lines of stage timings and receipts). `--json` prints the
    unchanged cycle summary plus the same summary under `run_summary`.
    """
    from okg.substrate.cli.run_summary import (
        build_run_summary,
        render_run_summary,
    )

    if not isinstance(out, dict):
        return _emit_error(
            SubstrateError(
                "the publish cycle returned no cycle summary, so whether it "
                "published is unproven",
                data={
                    "deployment": deployment,
                    "returned_type": type(out).__name__,
                },
                fix_hint=(
                    "the publisher always returns a cycle summary; report "
                    "this as a publisher defect and inspect the deployment "
                    "with okg doctor"
                ),
            ),
            as_json=as_json,
        )
    summary = build_run_summary(out, deployment=deployment)
    if as_json:
        _emit_ok({**out, "run_summary": summary}, as_json=True)
        return int(summary["exit_code"])
    for line in render_run_summary(summary):
        print(line)
    if verbose:
        print()
        print("Full cycle report:")
        _emit_ok(out, as_json=False)
    return int(summary["exit_code"])


def _compose_runtime_catalog(*, deployment: str, deployment_dir: Path):
    """Compose catalog identity through the active environment manifest."""

    from okg.substrate.catalog.modules_compose import compose_catalog

    if not os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        return compose_catalog(deployment_dir)

    from okg.substrate.deployments import resolve_deployment

    manifest = resolve_deployment(deployment)
    if manifest.deployment_dir.resolve() != Path(deployment_dir).resolve():
        raise SubstrateError(
            "active environment deployment root changed during publish",
            data={
                "deployment": deployment,
                "expected": str(Path(deployment_dir).resolve()),
                "observed": str(manifest.deployment_dir.resolve()),
            },
        )
    return compose_catalog(manifest)


def enable_progress_logging() -> None:
    """Stream INFO logs to stderr for `--progress` commands.

    Without this the logs go to the null root handler and the operator sees
    nothing until the final JSON report -- which is exactly how a long
    `generation compact` dry run reads as a hang (#1308).
    """
    import logging

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-5s %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ),
    )
    root = logging.getLogger()
    # Avoid stacking handlers on repeat calls within one
    # process (test runners, REPL).
    if not any(getattr(h, "_okg_progress", False) for h in root.handlers):
        handler._okg_progress = True
        root.addHandler(handler)
    root.setLevel(logging.INFO)


@_deployment_binding_scope
def _cmd_ingest(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg ingest --deployment <name>` — drive every source
    in the deployment's source_registry.yaml in topological
    order (respecting `predecessor_source` edges), inserting
    Publisher.cycle() calls between dependency groups so
    dependent sources see predecessor data in the published graph.

    Per `add-okg-ingest-cli` (2026-04-26).
    """
    from okg.substrate.deployments import DeploymentResolutionError
    from okg.substrate.deployments.admission import require_active_deployment

    try:
        require_active_deployment(args.deployment or "default")
    except DeploymentResolutionError as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    # Per `okg ingest --progress`: surface every source / runner /
    # publisher INFO-level log to stderr in real time. Sources
    # already log meaningful per-batch progress; --progress just
    # makes those visible. Without the flag the logs go to the
    # null root handler and the operator sees nothing until the
    # final JSON report.
    if getattr(args, "progress", False):
        enable_progress_logging()

    from okg.substrate.deployments import (
        find_deployment_dir, inject_deployment_path, resolve_deployment,
    )
    from okg.substrate.ingest.service import (
        IngestOptions,
        run_ingest,
    )
    from okg.substrate.ingest.planning import plan_ingest_sources
    from okg.substrate.sources.registry import load_source_registry

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    deployment = args.deployment or "default"
    dep_dir = find_deployment_dir(deployment)
    deployment_path = Path(deployment).expanduser()
    if dep_dir is None and deployment_path.is_file():
        dep_dir = deployment_path.parent.resolve()
    elif dep_dir is None and deployment_path.is_dir():
        dep_dir = deployment_path.resolve()
    if dep_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
                fix_hint="check deployments/<name>/ exists",
            ),
            as_json=as_json,
        )
    inject_deployment_path(dep_dir)

    registry_path = dep_dir / "source_registry.yaml"
    registry_repo_root_override = None
    if (
        deployment_path.is_file()
        or (dep_dir / "deployment.yaml").is_file()
        or os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    ):
        try:
            manifest = resolve_deployment(deployment)
        except Exception as exc:  # noqa: BLE001 - source authority must fail closed.
            return _emit_error(
                SubstrateError(
                    "could not resolve deployment manifest for ingest",
                    data={
                        "deployment": deployment,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    fix_hint=(
                        "fix the deployment/environment descriptor before ingest; "
                        "ingest must not silently ignore sync_root"
                    ),
                ),
                as_json=as_json,
            )
        # `resolve_deployment` accepts both names and filesystem paths. Once
        # the manifest has been resolved, the manifest name is the durable
        # deployment identity used by source admission, Nomos scopes, and
        # publish state. Keeping the raw path here creates a second identity:
        # baseline seeding uses `manifest.name`, while path-addressed ingest
        # would otherwise query `deployment:<absolute-path>` and default-deny.
        deployment = manifest.name
        dep_dir = manifest.deployment_dir
        registry_path = manifest.source_registry
        # Every later lookup by `deployment` (runtime routing, publish
        # provenance, the runner's privacy posture, ...) must reach this same
        # folder, also when it was given as a path outside the deployments
        # roots (issue #2951).
        from okg.substrate.deployments import bind_resolved_deployment

        bind_resolved_deployment(deployment, dep_dir)
        # An unresolvable `${VAR}` sync_root would read as None here and
        # silently resolve file-derived sources against a default repo root
        # instead of the registered checkout (issue #1014).
        from okg.substrate.deployments.sync_root import (
            require_resolved_sync_root,
            require_usable_source_configuration_authority,
        )

        try:
            require_resolved_sync_root(manifest, command="ingest")
            registry_repo_root_override = (
                require_usable_source_configuration_authority(
                    manifest.sync_root,
                    command="ingest",
                )
            )
        except SubstrateError as exc:
            return _emit_error(exc, as_json=as_json)

    # Phase 2 §2.2 of `migrate-substrate-runtime-to-dbos`: when the
    # deployment's runtime layer is enabled AND no --inline override,
    # enqueue the source-sync workflow via DBOSClient instead of
    # running `run_ingest` in this process. Source-sync has its own
    # DBOS lane; publisher-level mutual exclusion is enforced by the
    # Publisher itself, not by assuming this queue serializes with
    # publisher-cycle workflows.
    try:
        run_via_dbos = (
            not getattr(args, "inline", False)
            and _deployment_runtime_enabled(deployment)
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    if run_via_dbos and getattr(args, "reset_cursor", False):
        return _emit_error(
            SubstrateError(
                "--reset-cursor repair requires --inline",
                fix_hint=(
                    "rerun as `okg ingest --inline --reset-cursor ...` "
                    "so the operator intentionally bypasses the queued "
                    "incremental path"
                ),
            ),
            as_json=as_json,
        )

    if run_via_dbos and getattr(args, "allow_full_retraction", None):
        return _emit_error(
            SubstrateError(
                "--allow-full-retraction requires --inline",
                data={"sources": list(args.allow_full_retraction)},
                fix_hint=(
                    "rerun as `okg ingest --inline --allow-full-retraction "
                    "<source> ...` so the override applies to this one run"
                ),
            ),
            as_json=as_json,
        )

    include = (
        set(args.include.split(","))
        if getattr(args, "include", None) else None
    )
    exclude = (
        set(args.exclude.split(","))
        if getattr(args, "exclude", None) else set()
    )
    run_mode = getattr(args, "mode", None) or "scope_complete"
    include_predecessors = not bool(getattr(args, "no_predecessors", False))
    include_dependents = bool(getattr(args, "include_dependents", False))
    dependent_fanout = (
        getattr(args, "dependent_fanout", None)
        or ("closure" if include_dependents else "none")
    )
    try:
        provenance_registry = load_source_registry(
            registry_path,
            **(
                {"repo_root_override": registry_repo_root_override}
                if registry_repo_root_override is not None
                else {}
            ),
        )
        provenance_plan = plan_ingest_sources(
            provenance_registry,
            include=include,
            exclude=exclude,
            include_predecessors=include_predecessors,
            include_dependents=include_dependents,
            dependent_fanout=dependent_fanout,
        )
    except (ValueError, OSError) as e:
        err = _source_registry_invalid_error(e)
        return _emit_error(err, as_json=as_json)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    if not provenance_plan.ordered:
        return _emit_error(
            SubstrateError(
                "no runnable sources selected for sync enqueue",
                fix_hint="adjust --include/--exclude or source_registry.yaml",
            ),
            as_json=as_json,
        )
    file_derived_publish = _ingest_plan_is_file_derived(
        provenance_registry,
        provenance_plan.ordered,
    )

    # Tier W guard (worktree-access-tiers req.tiers.write-guard): trunk
    # projects trunk. Refuse canonical-database ingest from any checkout
    # other than the registered sync root; isolated clones stay legal. Tier E
    # append-ledger sources are shared and can publish from worktrees when the
    # selected plan contains no file-derived source.
    from okg.substrate.deployment_authority import check_publish_provenance

    provenance = check_publish_provenance(
        deployment,
        dsn,
        file_derived=file_derived_publish,
    )
    if not provenance.get("allowed"):
        return _emit_error(
            SubstrateError(
                "refusing file-derived publish: " + str(
                    provenance.get("reason")
                ),
                data=provenance,
                fix_hint="; ".join(provenance.get("legal_moves") or []) or None,
            ),
            as_json=as_json,
        )
    if provenance.get("warning"):
        import logging as _logging

        _logging.getLogger(__name__).warning(provenance["warning"])

    if run_via_dbos:
        from okg.substrate.errors import SourceSyncAlreadyRunning
        from okg.substrate.runtime.admin import (
            client_enqueue_source_sync,
        )
        from okg.substrate.ingest.planning import plan_ingest_sources
        sources_list = (
            list(args.include.split(","))
            if getattr(args, "include", None) else None
        )
        requested_mode = getattr(args, "mode", None) or "scope_complete"
        include_predecessors = not bool(
            getattr(args, "no_predecessors", False)
        )
        include_dependents = bool(
            getattr(args, "include_dependents", False)
        )
        dependent_fanout = getattr(args, "dependent_fanout", None)
        run_batches = [
            {
                "sources": sources_list,
                "run_mode": requested_mode,
                "include_predecessors": include_predecessors,
                "include_dependents": include_dependents,
                "dependent_fanout": dependent_fanout,
            }
        ]
        if requested_mode == "reconcile":
            include = set(sources_list) if sources_list else None
            exclude = (
                set(args.exclude.split(","))
                if getattr(args, "exclude", None) else set()
            )
            try:
                registry = load_source_registry(
                    registry_path,
                    **(
                        {"repo_root_override": registry_repo_root_override}
                        if registry_repo_root_override is not None
                        else {}
                    ),
                )
                plan = plan_ingest_sources(
                    registry,
                    include=include,
                    exclude=exclude,
                    include_predecessors=include_predecessors,
                    include_dependents=include_dependents,
                    dependent_fanout=dependent_fanout or "none",
                )
            except (ValueError, OSError) as e:
                err = _source_registry_invalid_error(e)
                return _emit_error(err, as_json=as_json)
            except SubstrateError as e:
                return _emit_error(e, as_json=as_json)
            if not plan.ordered:
                return _emit_error(
                    SubstrateError(
                        "no runnable sources selected for sync enqueue",
                        fix_hint=(
                            "adjust --include/--exclude or "
                            "source_registry.yaml"
                        ),
                    ),
                    as_json=as_json,
                )
            run_batches = _sync_source_batches_by_run_mode(
                registry,
                plan.ordered,
                requested_mode=requested_mode,
            )
        try:
            results = []
            for batch in run_batches:
                result = client_enqueue_source_sync(
                    deployment=deployment,
                    dsn=dsn,
                    sources=batch["sources"],
                    run_mode=batch["run_mode"],
                    include_predecessors=batch["include_predecessors"],
                    include_dependents=batch["include_dependents"],
                    dependent_fanout=batch["dependent_fanout"],
                    publish=not bool(getattr(args, "no_publish", False)),
                    profile_force_deep=_profile_force_deep(args),
                    profile_collectors=(
                        getattr(args, "profile_collector", ()) or ()
                    ),
                    force_enrichers=bool(
                        getattr(args, "force_enrichers", False)
                    ),
                    # Typed only while the batch still carries the typed
                    # mode. `--mode reconcile` is split per source into its
                    # registry reconcile mode, or `scope_complete` for a
                    # source with no `sync:` block; that is a whole-scope
                    # request the workflow must resolve per profile (#2486).
                    run_mode_explicit=(
                        getattr(args, "mode", None) is not None
                        and batch["run_mode"] == args.mode
                    ),
                    wait_for_result=not bool(getattr(args, "enqueue", False)),
                    sync_wait_seconds=getattr(args, "sync_wait_seconds", None),
                )
                results.append(result)
                if (
                    not bool(getattr(args, "enqueue", False))
                    and (result.status != "SUCCESS" or result.any_failed)
                ):
                    break
        except SourceSyncAlreadyRunning as e:
            # #3003: a command-line run holds the source-sync signal.
            return _emit_error(e, as_json=as_json)
        except Exception as e:  # noqa: BLE001
            return _emit_error(
                SubstrateError(
                    f"DBOS-path source-sync enqueue failed: {e}; "
                    "rerun with --inline to use the explicit in-process "
                    "ingest path",
                ),
                as_json=as_json,
            )
        result = results[-1]
        queued_rc = 0 if getattr(args, "enqueue", False) or all(
            item.status == "SUCCESS" and not item.any_failed
            for item in results
        ) else 1
        # Each enqueued batch returns its own ingest report; an enricher that
        # raised in any of their publishes is named, as on the inline path,
        # including before a later batch's typed refusal below.
        failure_rows, failure_lines = _ingest_enricher_failure_lines(
            [getattr(item, "report", None) for item in results], deployment,
            command_exit_code=queued_rc,
        )
        if getattr(result, "error_code", None):
            queued_error = SubstrateError(
                result.error or "queued source-sync workflow refused",
                data={
                    **(getattr(result, "error_data", None) or {}),
                    "enricher_failures": failure_rows,
                    "workflow_id": result.workflow_id,
                    "workflow_status": result.status,
                },
                fix_hint=getattr(result, "error_fix_hint", None),
            )
            queued_error.code = result.error_code
            return _emit_error(queued_error, as_json=as_json)
        payload = {
            "workflow_id": result.workflow_id,
            "status": result.status,
            "any_failed": result.any_failed,
            "elapsed_seconds": result.elapsed_seconds,
            "report": result.report,
            "error": result.error,
            "enricher_failures": failure_rows,
        }
        if len(results) > 1:
            payload["workflows"] = [
                {
                    "workflow_id": item.workflow_id,
                    "status": item.status,
                    "any_failed": item.any_failed,
                    "elapsed_seconds": item.elapsed_seconds,
                    "report": item.report,
                    "error": item.error,
                }
                for item in results
            ]
        if as_json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(
                f"okg ingest (DBOS path): workflow_id={result.workflow_id} "
                f"status={result.status} "
                f"any_failed={result.any_failed} "
                f"elapsed={result.elapsed_seconds:.1f}s"
            )
            if result.error:
                print(f"  error: {result.error}")
            for line in failure_lines:
                print(line)
        return queued_rc

    guard_rc = _guard_inline_publish_against_active_source_sync(
        args=args,
        deployment=deployment,
        dsn=dsn,
        as_json=as_json,
    )
    if guard_rc is not None:
        return guard_rc

    owner_meta_raw = getattr(args, "owner_meta", None) or "{}"
    # Convention point (g): malformed --owner-meta is a load-time error, not a
    # benign default. Silently substituting {} discarded the operator's
    # provenance metadata and reported success (#694).
    try:
        owner_meta = json.loads(owner_meta_raw)
    except json.JSONDecodeError as exc:
        return _emit_error(
            SubstrateError(
                f"--owner-meta must be valid JSON; got {owner_meta_raw!r} "
                f"({exc})",
            ),
            as_json=as_json,
        )
    if not isinstance(owner_meta, dict):
        return _emit_error(
            SubstrateError(
                f"--owner-meta must be a JSON object (mapping); got "
                f"{type(owner_meta).__name__} from {owner_meta_raw!r}",
            ),
            as_json=as_json,
        )

    if getattr(args, "enqueue", False):
        err = SubstrateError(
            "`okg ingest --enqueue` requires an enabled DBOS runtime",
            data={"deployment": deployment, "runtime_enabled": False},
            fix_hint=(
                "set runtime.enabled: true and run `okg runtime worker "
                "--deployment <name>`, or omit --enqueue to run inline"
            ),
        )
        err.code = "runtime_unavailable"
        return _emit_error(err, as_json=as_json)

    try:
        outcome = run_ingest(
            IngestOptions(
                dsn=dsn,
                deployment=deployment,
                deployment_dir=dep_dir,
                include=include,
                exclude=exclude,
                include_predecessors=include_predecessors,
                include_dependents=include_dependents,
                dependent_fanout=dependent_fanout,
                run_mode=run_mode,
                run_mode_explicit=getattr(args, "mode", None) is not None,
                publish=not args.no_publish,
                owner_id=getattr(args, "owner_id", None) or None,
                owner_meta=owner_meta,
                profiling_policy=_profiling_policy_for_args(args),
                profile_force_deep=_profile_force_deep(args),
                command=f"okg ingest --deployment {deployment}",
                reset_cursor=bool(getattr(args, "reset_cursor", False)),
                allow_full_retraction=tuple(
                    getattr(args, "allow_full_retraction", None) or ()
                ),
                force_enrichers=bool(
                    getattr(args, "force_enrichers", False)
                ),
                source_registry_repo_root_override=(
                    registry_repo_root_override
                ),
                sync_wait_seconds=getattr(args, "sync_wait_seconds", None),
            ),
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    report = outcome.report
    failure_rows, failure_lines = _ingest_enricher_failure_lines(
        report, deployment, command_exit_code=1 if outcome.any_failed else 0,
    )
    from okg.substrate.ingest.failure_followup import (
        REPORT_KEY as OTHER_SOURCES_KEY,
        other_sources_after_failure,
        text_lines,
    )

    if isinstance(report, dict):
        report["enricher_failures"] = failure_rows
        # #3927: the failed source's line names one source; say what became
        # of the others. Built here, in the in-process path, where `report`
        # covers every selected source. The queued path above reports one
        # batch at a time and does not carry this note.
        others = other_sources_after_failure(
            deployment=deployment,
            completeness=report.get("completeness"),
            publishes=report.get("publishes"),
            inline_flag=bool(getattr(args, "inline", False)),
        )
        if others is not None:
            report[OTHER_SOURCES_KEY] = others
    _emit_ok(report, as_json=as_json)
    if not as_json:
        for line in failure_lines:
            print(line)
        for line in text_lines(report):
            print(line)
    return 1 if outcome.any_failed else 0


def _ingest_enricher_failure_lines(
    reports: Any, deployment: str, *, command_exit_code: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Name every enricher that raised during an ``okg ingest`` publish.

    ``reports`` is one ingest report or a list of them (the runtime-queue
    path returns one per enqueued batch). Every publish in them carries its
    own cycle summary. Returns the ``enricher_failures`` rows (with the
    graph version each failed in) and the text lines to print after the
    report. ``command_exit_code`` is what ``okg ingest`` will return; the
    hint states it rather than assuming 0. The exit code is not changed here.
    """
    from okg.substrate.cli.run_summary import (
        enricher_failure_hint,
        enricher_failure_text,
        enricher_failures,
        publish_outcome,
    )

    if isinstance(reports, dict) or reports is None:
        reports = [reports]
    rows_out: list[dict[str, Any]] = []
    lines: list[str] = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        for published in report.get("publishes") or []:
            rows = enricher_failures(published)
            if not rows:
                continue
            generation_id = published.get("generation_id")
            rows_out.extend(
                {**row, "generation_id": generation_id} for row in rows
            )
            outcome = publish_outcome(published)
            label = {
                "published": "published graph version",
                "no_change": "no change; graph version",
            }.get(outcome, f"NOT PUBLISHED ({outcome}); graph version")
            lines.append(
                f"{label} {generation_id}; {enricher_failure_text(rows)}"
            )
            lines.append("  " + enricher_failure_hint(
                deployment, outcome, command_exit_code=command_exit_code,
            ))
    return rows_out, lines


def _sync_source_groups_by_run_mode(
    registry: Mapping[str, Any],
    sources: tuple[str, ...],
    *,
    requested_mode: str,
) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for source_name in sources:
        entry = registry.get(source_name)
        sync = getattr(entry, "sync", None)
        run_mode = _sync_source_run_mode(
            sync,
            requested_mode=requested_mode,
        )
        groups.setdefault(run_mode, []).append(source_name)
    return groups


def _sync_source_run_mode(sync: Any, *, requested_mode: str) -> str:
    if requested_mode != "reconcile":
        return requested_mode
    if sync is not None and getattr(sync, "reconcile_mode", None):
        return sync.reconcile_mode
    return "scope_complete"


def _sync_source_batches_by_run_mode(
    registry: Mapping[str, Any],
    sources: tuple[str, ...],
    *,
    requested_mode: str,
) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for source_name in sources:
        entry = registry.get(source_name)
        sync = getattr(entry, "sync", None)
        run_mode = _sync_source_run_mode(
            sync,
            requested_mode=requested_mode,
        )
        if batches and batches[-1]["run_mode"] == run_mode:
            batches[-1]["sources"].append(source_name)
        else:
            batches.append({
                "sources": [source_name],
                "run_mode": run_mode,
                "include_predecessors": False,
                "include_dependents": False,
                "dependent_fanout": "none",
            })
    return batches


def _cmd_sync(args: argparse.Namespace, *, as_json: bool) -> int:
    """Source-sync diagnostics and producer helpers."""
    from okg.substrate.deployments import (
        find_deployment_dir,
        resolve_deployment,
    )
    from okg.substrate.source_sync_diagnostics import (
        explain_source_sync_event,
        source_frontier_report,
        sync_policy_report,
    )
    from okg.substrate.source_sync_producers import (
        handle_webhook_payload,
        run_poll_once,
        source_sync_producer_health,
    )
    from okg.substrate.sources.registry import load_source_registry_admission

    deployment = getattr(args, "deployment", None)
    if (
        args.sync_subcmd == "convergence"
        and getattr(args, "convergence_subcmd", None) == "inventory"
    ):
        from okg.substrate.source_sync_convergence import (
            build_convergence_inventory,
        )

        out = build_convergence_inventory(
            repo_root=Path.cwd(),
            deployments_root=(
                Path(args.deployments_root)
                if getattr(args, "deployments_root", None) else None
            ),
            deployment=(
                deployment
                if not getattr(args, "all_deployments", False) else None
            ),
            all_deployments=bool(getattr(args, "all_deployments", False)),
        )
        _emit_ok(out, as_json=as_json)
        return 0
    if not deployment:
        return _emit_error(
            SubstrateError("--deployment is required for this sync command"),
            as_json=as_json,
        )
    dep_dir = find_deployment_dir(deployment)
    if dep_dir is None:
        return _emit_error(
            SubstrateError(
                f"deployment dir for {deployment!r} not found",
            ),
            as_json=as_json,
        )
    manifest = None
    try:
        manifest = resolve_deployment(deployment)
    except Exception as exc:  # noqa: BLE001 - fail closed for sync-root authority.
        if args.sync_subcmd == "microbench":
            return _emit_error(
                SubstrateError(
                    "could not resolve deployment manifest for microbench",
                    data={
                        "deployment": deployment,
                        "error": f"{type(exc).__name__}: {exc}",
                    },
                    fix_hint=(
                        "fix deployment.yaml before running sync microbench; "
                        "microbench must not silently ignore sync_root"
                    ),
                ),
                as_json=as_json,
            )
    registry_path = (
        Path(getattr(manifest, "source_registry"))
        if manifest is not None and getattr(manifest, "source_registry", None)
        else dep_dir / "source_registry.yaml"
    )
    registry_repo_root_override = (
        getattr(manifest, "sync_root", None)
        if args.sync_subcmd == "microbench" and manifest is not None
        else None
    )
    try:
        registry_admission = load_source_registry_admission(
            registry_path,
            repo_root_override=registry_repo_root_override,
        )
        registry = registry_admission.entries
    except (ValueError, OSError) as e:
        err = _source_registry_invalid_error(e)
        return _emit_error(err, as_json=as_json)

    runtime_source_sync = None
    runtime_enabled = False
    runtime_config_warnings: list[dict[str, Any]] = []
    try:
        if manifest is None:
            manifest = resolve_deployment(deployment)
        from okg.substrate.runtime.config import resolve_source_sync
        runtime_source_sync = resolve_source_sync(manifest.runtime)
        runtime_enabled = bool(getattr(manifest.runtime, "enabled", False))
    except Exception as exc:  # noqa: BLE001
        runtime_source_sync = None
        runtime_enabled = False
        runtime_config_warnings.append(
            _source_sync_runtime_config_warning(
                deployment=deployment,
                error=exc,
            )
        )
    from okg.substrate.source_sync_convergence import (
        readiness_profiles_for_deployment_dir,
        readiness_source_groups_for_deployment_dir,
    )

    readiness_profiles = readiness_profiles_for_deployment_dir(dep_dir)
    readiness_source_groups = readiness_source_groups_for_deployment_dir(
        dep_dir,
        readiness_profiles,
    )

    if args.sync_subcmd == "explain":
        if getattr(args, "path", None):
            out = explain_source_sync_event(
                registry,
                path=args.path,
                trigger_kind=args.trigger,
                source=getattr(args, "source", None) or None,
            )
        else:
            out = sync_policy_report(registry)
        out["deployment"] = deployment
        _emit_ok(out, as_json=as_json)
        return 0

    if args.sync_subcmd == "frontier":
        try:
            dsn = _resolve_dsn(args)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        try:
            out = source_frontier_report(
                dsn,
                deployment=deployment,
                source=getattr(args, "source", None) or None,
                limit=int(getattr(args, "limit", 100) or 100),
            )
        except Exception as exc:  # noqa: BLE001 - normalized CLI diagnostic.
            return _emit_error(
                SubstrateError(f"source frontier diagnostics failed: {exc}"),
                as_json=as_json,
            )
        _emit_ok(out, as_json=as_json)
        return 0

    if args.sync_subcmd == "benchmark":
        from okg.substrate.source_sync_benchmark import (
            load_source_sync_benchmark,
            validate_source_sync_benchmark,
        )
        try:
            doc = load_source_sync_benchmark(args.artifact)
        except (OSError, ValueError, json.JSONDecodeError) as e:
            return _emit_error(
                SubstrateError(f"source-sync benchmark artifact is invalid: {e}"),
                as_json=as_json,
            )
        latency_budgets: dict[str, float] = {}
        if getattr(args, "max_event_to_generation_ms", None) is not None:
            latency_budgets["event_to_generation_latency_ms"] = float(
                args.max_event_to_generation_ms
            )
        if getattr(args, "max_query_visibility_ms", None) is not None:
            latency_budgets["query_visibility_latency_ms"] = float(
                args.max_query_visibility_ms
            )
        out = validate_source_sync_benchmark(
            doc,
            artifact=str(args.artifact),
            deployment=deployment,
            known_sources=tuple(registry.keys()),
            latency_budgets_ms=latency_budgets,
        )
        _emit_ok(out, as_json=as_json)
        return 0 if out["ok"] else 1

    if args.sync_subcmd == "microbench":
        if args.microbench_subcmd in {None, "gate"}:
            from okg.substrate.source_sync_microbench import (
                build_source_sync_microbenchmark_report,
            )

            try:
                dsn = _resolve_dsn(args)
                if args.microbench_subcmd == "gate":
                    raw_pipeline_modes = tuple(
                        args.pipeline_mode or ("both",)
                    )
                    full_pipeline_requested = (
                        "both" in raw_pipeline_modes
                        or "full-pipeline" in raw_pipeline_modes
                    )
                    if (
                        full_pipeline_requested
                        and _deployment_runtime_enabled(deployment)
                    ):
                        idle_state = _wait_for_idle_source_sync_runtime(
                            deployment=deployment,
                            dsn=dsn,
                            wait_seconds=float(
                                getattr(
                                    args,
                                    "idle_source_sync_wait_seconds",
                                    30.0,
                                ) or 0.0
                            ),
                        )
                        if not bool(idle_state.get("ok")):
                            out = {
                                "schema": "okg.source_sync.microbench.run.v1",
                                "deployment": deployment,
                                "deployment_dir": str(dep_dir),
                                "scenario": str(args.scenario),
                                "pipeline_modes": list(raw_pipeline_modes),
                                "source_selector": [
                                    item.strip()
                                    for item in str(
                                        args.sources or "all",
                                    ).split(",")
                                    if item.strip()
                                ],
                                "run_mode": str(
                                    args.run_mode or "scope_incomplete",
                                ),
                                "ok": False,
                                "row_count": 0,
                                "skipped_row_count": 0,
                                "sample_count": 0,
                                "rows": [],
                                "samples": [],
                                "environment_blockers": [idle_state],
                            }
                            out = _microbench_gate_result(
                                out,
                                max_p95_ms=float(
                                    getattr(args, "max_p95_ms", 1000.0)
                                    or 1000.0
                                ),
                            )
                            output_path = getattr(args, "output", None)
                            if output_path:
                                output_path = Path(output_path)
                                output_path.parent.mkdir(
                                    parents=True,
                                    exist_ok=True,
                                )
                                output_path.write_text(
                                    json.dumps(
                                        out,
                                        indent=2,
                                        sort_keys=True,
                                    ) + "\n",
                                    encoding="utf-8",
                                )
                                out["artifact"] = str(output_path)
                            _emit_ok(out, as_json=as_json)
                            return 1
                out = build_source_sync_microbenchmark_report(
                    deployment=deployment,
                    deployment_dir=dep_dir,
                    registry=registry,
                    dsn=dsn,
                    scenario=str(args.scenario),
                    sources=str(args.sources or "all"),
                    repeat=int(args.repeat or 1),
                    pipeline_modes=tuple(args.pipeline_mode or ("both",)),
                    run_mode=str(args.run_mode or "scope_incomplete"),
                    include_predecessors=bool(args.include_predecessors),
                    include_dependents=bool(args.include_dependents),
                    dependent_fanout=getattr(args, "dependent_fanout", None),
                    max_source_workers=int(args.max_source_workers or 1),
                    scope_key=getattr(args, "scope_key", None),
                    unscoped=bool(getattr(args, "unscoped", False)),
                    expected_delta=_json_mapping_arg(
                        getattr(args, "expected_delta_json", None),
                        "--expected-delta-json",
                    ),
                    output_path=getattr(args, "output", None),
                    allow_quarantined_full_pipeline=bool(
                        getattr(args, "allow_quarantined_full_pipeline", False)
                    ),
                    source_registry_repo_root_override=(
                        registry_repo_root_override
                    ),
                )
            except (ValueError, SubstrateError) as e:
                err = e if isinstance(e, SubstrateError) else SubstrateError(str(e))
                return _emit_error(err, as_json=as_json)
            if args.microbench_subcmd == "gate":
                out = _microbench_gate_result(
                    out,
                    max_p95_ms=float(
                        getattr(args, "max_p95_ms", 1000.0) or 1000.0
                    ),
                )
                output_path = getattr(args, "output", None)
                if output_path:
                    output_path = Path(output_path)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(
                        json.dumps(out, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    out["artifact"] = str(output_path)
            _emit_ok(out, as_json=as_json)
            return 0 if out["ok"] else 1
        if args.microbench_subcmd in {"inventory", "ledger"}:
            from okg.substrate.source_sync_optimization_ledger import (
                build_source_optimization_ledger,
            )
            from okg.substrate.source_sync_microbench import (
                build_microbenchmark_inventory,
            )

            dsn = None
            if not getattr(args, "no_live", False):
                try:
                    dsn = _resolve_dsn(args)
                except SubstrateError:
                    dsn = None
            out = build_microbenchmark_inventory(
                deployment=deployment,
                deployment_dir=dep_dir,
                registry=registry,
                runtime_source_sync=runtime_source_sync,
                runtime_enabled=runtime_enabled,
                dsn=dsn,
                include_live=not getattr(args, "no_live", False),
                enrichers=_deployment_enrichers(dep_dir),
                max_producer_age_seconds=int(
                    getattr(args, "max_producer_age_seconds", 15 * 60)
                    or 15 * 60
                ),
            )
            if args.microbench_subcmd == "ledger":
                out = build_source_optimization_ledger(
                    out,
                    latency_budget_ms=float(
                        getattr(args, "latency_budget_ms", 1000.0)
                        or 1000.0
                    ),
                )
            _emit_ok(out, as_json=as_json)
            return 0
        return _emit_error(
            SubstrateError(
                f"unknown sync microbench subcommand "
                f"{args.microbench_subcmd!r}",
            ),
            as_json=as_json,
        )

    if args.sync_subcmd == "benchmark-capture":
        from okg.substrate.source_sync_benchmark import (
            build_source_sync_benchmark_artifact,
            validate_source_sync_benchmark,
        )
        try:
            debug_doc = _read_json_file(args.debug_workflow_json)
            expected_delta = _json_mapping_arg(
                args.expected_delta_json,
                "--expected-delta-json",
            )
            observed_delta = _json_mapping_arg(
                args.observed_delta_json,
                "--observed-delta-json",
            )
            queue_state = _json_mapping_arg(
                args.queue_json,
                "--queue-json",
            )
        except (OSError, ValueError) as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        try:
            artifact_doc = build_source_sync_benchmark_artifact(
                debug_doc,
                deployment=deployment,
                repo=args.repo or "",
                scenario=args.scenario,
                expected_delta=expected_delta,
                observed_delta=observed_delta,
                query_visible=bool(args.query_visible),
                query_visibility_latency_ms=args.query_visibility_ms,
                queue=queue_state,
                profile_run_id=args.profile_run_id,
                readiness_ms=args.readiness_ms,
                mcp_graph_read_ms=args.mcp_graph_read_ms,
            )
        except ValueError as e:
            return _emit_error(SubstrateError(str(e)), as_json=as_json)
        validation = validate_source_sync_benchmark(
            artifact_doc,
            artifact=str(args.output or ""),
            deployment=deployment,
            known_sources=tuple(registry.keys()),
        )
        if args.output:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                json.dumps(artifact_doc, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        out = {
            "ok": validation["ok"],
            "artifact": artifact_doc,
            "validation": validation,
            "output": str(args.output or ""),
        }
        _emit_ok(out, as_json=as_json)
        return 0 if validation["ok"] else 1

    if args.sync_subcmd == "convergence":
        if args.convergence_subcmd == "status":
            try:
                dsn = _resolve_dsn(args)
            except SubstrateError as e:
                return _emit_error(e, as_json=as_json)
            out = source_sync_producer_health(
                dsn,
                deployment=deployment,
                registry=registry,
                max_producer_age_seconds=int(
                    getattr(args, "max_producer_age_seconds", 15 * 60)
                    or 15 * 60
                ),
                runtime_source_sync=runtime_source_sync,
                runtime_enabled=runtime_enabled,
                readiness_profiles=readiness_profiles,
                readiness_source_groups=readiness_source_groups,
            )
            out = _append_source_sync_config_warnings(
                out,
                runtime_config_warnings,
            )
            source_freshness = None
            try:
                from okg.substrate.db.connection import connect as db_connect
                from okg.substrate.cli.deployment import (
                    _ready_progress_freshness_sources,
                    _ready_source_aliases,
                    _ready_source_freshness_status,
                )

                progress_freshness_sources: set[str] = set()
                for profile in readiness_profiles:
                    progress_freshness_sources.update(
                        _ready_progress_freshness_sources(dep_dir, profile)
                    )
                with db_connect(
                    dsn,
                    connect_timeout=5,
                    autocommit=True,
                ) as conn:
                    source_freshness = _ready_source_freshness_status(
                        conn,
                        deployment=deployment,
                        max_source_age_seconds=int(
                            getattr(args, "max_source_age_seconds", 3600)
                            or 3600
                        ),
                        source_groups=readiness_source_groups,
                        progress_freshness_sources=progress_freshness_sources,
                        source_aliases=_ready_source_aliases(dep_dir),
                    )
            except Exception as exc:  # noqa: BLE001 - status remains readable.
                source_freshness = {
                    "ok": False,
                    "missing": [],
                    "stale": [],
                    "groups": {},
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            from okg.substrate.source_sync_status import (
                build_source_sync_status_contract,
            )

            status = build_source_sync_status_contract(
                out,
                source_freshness=source_freshness,
            )
            _emit_ok(
                {
                    "schema": "okg.source_sync.convergence.status.v1",
                    "deployment": deployment,
                    "source_sync_status": status,
                    "convergence": status.get("convergence"),
                },
                as_json=as_json,
            )
            return 0 if status.get("convergence", {}).get("ok") else 1

    if args.sync_subcmd == "webhook":
        try:
            dsn = _resolve_dsn(args)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        raw_payload = _sync_payload_bytes(args)
        try:
            payload = json.loads(raw_payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            return _emit_error(
                SubstrateError(f"webhook payload JSON is invalid: {e}"),
                as_json=as_json,
            )
        if not isinstance(payload, dict):
            return _emit_error(
                SubstrateError("webhook payload must be a JSON object"),
                as_json=as_json,
            )
        try:
            out = handle_webhook_payload(
                dsn,
                deployment=deployment,
                registry=registry,
                source_name=args.source,
                payload=payload,
                body=raw_payload,
                headers=(
                    {"X-OKG-Signature": args.signature}
                    if getattr(args, "signature", None) else {}
                ),
                publish=not args.no_publish,
            )
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        _emit_ok(out, as_json=as_json)
        return 0

    if args.sync_subcmd == "poll":
        try:
            dsn = _resolve_dsn(args)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        poll_cfg = (
            runtime_source_sync.get("poll", {})
            if isinstance(runtime_source_sync, dict) else {}
        )
        backstop_multiplier = int(poll_cfg.get("backstop_multiplier") or 5)
        backstop_dependent_fanout = poll_cfg.get(
            "backstop_dependent_fanout",
        )
        if not isinstance(backstop_dependent_fanout, str):
            backstop_dependent_fanout = None
        out = run_poll_once(
            dsn,
            deployment=deployment,
            registry=registry_admission,
            source=getattr(args, "source", None) or None,
            registry_path=registry_path,
            registry_repo_root_override=registry_repo_root_override,
            publish=not args.no_publish,
            backstop_multiplier=backstop_multiplier,
            backstop_dependent_fanout=backstop_dependent_fanout,
        )
        errors = out.get("errors") if isinstance(out, dict) else None
        if isinstance(errors, list) and errors:
            err = SubstrateError(
                "source-sync poll failed before durable DBOS enqueue completed",
                data={
                    "deployment": deployment,
                    "errors": errors,
                    "recovery_commands": {
                        "status": (
                            f"env -u OKG_DSN {okg_prefix()} dogfood status "
                            f"--deployment {deployment} --json"
                        ),
                        "worker": (
                            f"env -u OKG_DSN {okg_prefix()} runtime worker "
                            f"--deployment {deployment}"
                        ),
                    },
                },
                fix_hint=(
                    "inspect the source-sync error details, ensure the DBOS "
                    "runtime worker is healthy, then rerun okg sync poll"
                ),
            )
            err.code = "source_sync_poll_enqueue_failed"
            return _emit_error(err, as_json=as_json)
        _emit_ok(out, as_json=as_json)
        return 0

    if args.sync_subcmd == "health":
        try:
            dsn = _resolve_dsn(args)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        out = source_sync_producer_health(
            dsn,
            deployment=deployment,
            registry=registry,
            max_producer_age_seconds=int(
                getattr(args, "max_producer_age_seconds", 15 * 60) or 15 * 60
            ),
            runtime_source_sync=runtime_source_sync,
            runtime_enabled=runtime_enabled,
            readiness_profiles=readiness_profiles,
            readiness_source_groups=readiness_source_groups,
        )
        out = _append_source_sync_config_warnings(
            out,
            runtime_config_warnings,
        )
        source_freshness = None
        generation = None
        mcp = None
        readiness_sources = sorted({
            str(row.get("source_name") or "")
            for row in out.get("source_convergence_inventory") or []
            if isinstance(row, Mapping)
            and row.get("source_name")
            and isinstance(row.get("freshness_policy"), Mapping)
            and row["freshness_policy"].get("readiness_required")
        })
        if readiness_sources:
            try:
                from okg.substrate.db.connection import connect as db_connect
                from okg.substrate.cli.deployment import (
                    _ready_generation_status,
                    _ready_mcp_status,
                    _ready_progress_freshness_sources,
                    _ready_source_aliases,
                    _ready_source_freshness_status,
                )

                progress_freshness_sources: set[str] = set()
                for profile in readiness_profiles:
                    progress_freshness_sources.update(
                        _ready_progress_freshness_sources(dep_dir, profile)
                    )
                with db_connect(
                    dsn,
                    connect_timeout=5,
                    autocommit=True,
                ) as conn:
                    source_freshness = _ready_source_freshness_status(
                        conn,
                        deployment=deployment,
                        max_source_age_seconds=int(
                            getattr(args, "max_source_age_seconds", 3600)
                            or 3600
                        ),
                        source_groups=(
                            readiness_source_groups or {
                                "readiness_required": tuple(readiness_sources),
                            }
                        ),
                        progress_freshness_sources=progress_freshness_sources,
                        source_aliases=_ready_source_aliases(dep_dir),
                    )
                    generation = _ready_generation_status(conn, deployment)
                    mcp = _ready_mcp_status(conn, deployment)
            except Exception as exc:  # noqa: BLE001 - health stays readable.
                source_freshness = {
                    "ok": False,
                    "missing": [],
                    "stale": [],
                    "groups": {},
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
        from okg.substrate.source_sync_status import (
            build_source_sync_status_contract,
        )

        out["source_sync_status"] = build_source_sync_status_contract(
            out,
            source_freshness=source_freshness,
            generation=generation,
            mcp=mcp,
        )
        out["ok"] = bool(out["source_sync_status"].get("ok"))
        _emit_ok(out, as_json=as_json)
        return 0

    return _emit_error(
        SubstrateError(f"unknown sync subcommand {args.sync_subcmd!r}"),
        as_json=as_json,
    )


def _read_json_file(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _json_mapping_arg(value: str | None, label: str) -> dict[str, Any]:
    if not value:
        return {}
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return dict(payload)


def _deployment_enrichers(dep_dir: Path) -> list[dict[str, Any]]:

    manifest_path = dep_dir / "deployment.yaml"
    try:
        payload = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8"),
        ) or {}
    except OSError:
        return []
    if not isinstance(payload, dict):
        return []
    enrichers = payload.get("enrichers") or []
    if not isinstance(enrichers, list):
        return []
    return [dict(item) for item in enrichers if isinstance(item, dict)]


def _sync_payload_bytes(args: argparse.Namespace) -> bytes:
    if getattr(args, "payload_json", None):
        return args.payload_json.encode("utf-8")
    if getattr(args, "payload_file", None):
        return Path(args.payload_file).read_bytes()
    return b"{}"


def _collect_status(
    dsn: str, deployment_name: str,
    *,
    deployment_dir=None,
) -> dict:
    """Single status snapshot. Reads catalog version, latest /
    pending generation, per-source watermark + fact counts. Used
    by both single-shot `okg status` and `okg status --watch`.

    When `deployment_dir` is supplied, the source list is unioned
    with every entry from source_registry.yaml so sources that
    have not yet written any facts still appear in the dashboard
    (status `waiting`). Without it, only sources that have
    actually emitted facts show up.
    """
    import psycopg
    from okg.substrate.db.connection import connect as db_connect

    # Pre-load expected source ownership_ids from the deployment
    # registry so the dashboard shows them even before they
    # write the first fact. Best-effort: a missing/malformed
    # registry just collapses back to "show whatever we find
    # in the DB".
    expected_sources: list[str] = []
    registry_sources: dict[str, str] = {}
    # A registry that does not load still leaves the dashboard usable (the
    # scope hint is advisory), but the refusal is reported in `errors`
    # rather than swallowed.
    registry_error: dict[str, str] | None = None
    if deployment_dir is not None:
        try:
            from okg.substrate.sources.registry import (
                load_source_registry,
            )
            from pathlib import Path as _P
            reg_path = _P(deployment_dir) / "source_registry.yaml"
            if reg_path.is_file():
                registry = load_source_registry(reg_path)
                expected_sources = sorted(
                    e.ownership_id for e in registry.values()
                    if not e.legacy
                )
                registry_sources = {
                    name: e.ownership_id for name, e in registry.items()
                    if not e.legacy
                }
        except Exception as e:  # noqa: BLE001 — registry read is advisory for scope filtering; the error is reported below.
            expected_sources = []
            registry_sources = {}
            registry_error = {
                "code": str(
                    getattr(e, "code", None) or "source_registry_invalid"
                ),
                "message": f"source_registry.yaml did not load: {e}",
            }
    expected_source_set = set(expected_sources)

    def _source_in_scope(source: str | None) -> bool:
        if not source:
            return False
        if deployment_dir is None:
            return True
        if source in expected_source_set:
            return True
        if source.startswith(f"{deployment_name}."):
            return True
        # Enrichers currently write deployment-local evidence under
        # stable internal ownership ids. In the one-DB-per-deployment
        # model these are safe to show while foreign deployment source
        # prefixes are noise.
        if source.startswith("_enricher:"):
            return True
        return False

    snap: dict = {
        "deployment_name": deployment_name,
        "stage": "unknown",
        "publishing": {},
        "sources": [],
        "errors": [registry_error] if registry_error is not None else [],
    }
    from okg.substrate.db.readiness import (
        migrate_command,
        read_database_readiness,
    )

    try:
        with db_connect(dsn) as conn:
            readiness = read_database_readiness(conn)
    except Exception as e:  # noqa: BLE001 - reported, never raised: `okg status` is a report.
        snap["errors"].append(
            {"code": "database_readiness_unknown", "message": str(e)}
        )
        readiness = None
    if readiness is not None and readiness.migrated:
        # #3342: a repair_required checkpoint refuses every other source's
        # run before it starts, and nothing else records that. The hold does
        # not need the registry; without one, no source is listed.
        from okg.substrate.ingest.incomplete_recovery import source_sync_hold

        try:
            hold = source_sync_hold(
                dsn=dsn, deployment=deployment_name, sources=registry_sources,
            )
        except Exception as e:  # noqa: BLE001 - reported, never raised: `okg status` is a report.
            snap["errors"].append(
                {"code": "source_sync_hold_unknown", "message": str(e)}
            )
        else:
            if hold is not None:
                snap["source_sync_hold"] = hold
    if readiness is not None and readiness.migrated:
        # The migrated branch used to drop the readiness entirely, so a
        # database running behind its ledger looked identical to one
        # fully caught up (`"stage": "migrated"`, nothing else). Report
        # the facts here and, when something IS pending, one line on
        # stderr as well -- `okg status` reports, it never refuses.
        snap["database_readiness"] = readiness.as_dict()
        _report_pending_migrations(
            readiness, surface="okg status", deployment=deployment_name
        )
    if readiness is not None and not readiness.migrated:
        snap["stage"] = "scaffolded"
        snap["database_readiness"] = readiness.as_dict()
        snap["stage_meaning"] = (
            "this database has no migrated okg schema yet, so there are no "
            f"generations or facts to report; run `{migrate_command(deployment_name)}` "
            "before the first publish"
        )
        return snap
    try:
        with db_connect(dsn) as conn, conn.cursor() as cur:
            from okg.substrate.generation_resolver import (
                resolve_okg_generation,
            )

            latest_generation = resolve_okg_generation(
                conn,
                deployment_name=deployment_name,
                requested=None,
                status="published",
                include_counts=True,
            )
            if latest_generation is None:
                cur.execute(
                    """
                    SELECT generation_id, catalog_version_id, status
                      FROM okg.graph_generations
                     WHERE status = 'published'
                       AND deployment_name = %s
                     ORDER BY generation_id DESC
                     LIMIT 1
                    """,
                    (deployment_name,),
                )
                legacy_row = cur.fetchone()
                if legacy_row is None:
                    snap["publishing"]["latest_published_generation"] = None
                    snap["publishing"]["latest_published_target_generation"] = None
                    snap["publishing"]["latest_published_status"] = None
                    snap["publishing"]["latest_published_catalog_version"] = None
                    snap["publishing"]["latest_published_node_count"] = None
                    snap["publishing"]["latest_published_edge_count"] = None
                else:
                    snap["publishing"]["latest_published_generation"] = (
                        legacy_row[0]
                    )
                    snap["publishing"]["latest_published_target_generation"] = None
                    snap["publishing"]["latest_published_status"] = legacy_row[2]
                    snap["publishing"]["latest_published_catalog_version"] = (
                        legacy_row[1]
                    )
                    snap["publishing"]["latest_published_node_count"] = None
                    snap["publishing"]["latest_published_edge_count"] = None
            else:
                snap["publishing"]["latest_published_generation"] = (
                    latest_generation.generation_id
                )
                snap["publishing"]["latest_published_target_generation"] = (
                    latest_generation.target_generation_id
                )
                snap["publishing"]["latest_published_status"] = (
                    latest_generation.status
                )
                snap["publishing"]["latest_published_catalog_version"] = (
                    latest_generation.catalog_version_id
                )
                snap["publishing"]["latest_published_node_count"] = (
                    latest_generation.node_count
                )
                snap["publishing"]["latest_published_edge_count"] = (
                    latest_generation.edge_count
                )
            cur.execute(
                "SELECT max(catalog_version_id) "
                "FROM okg.catalog_versions WHERE status = 'applied'"
            )
            snap["publishing"]["latest_catalog_version"] = (
                cur.fetchone()[0]
            )
            if (
                snap["publishing"]["latest_catalog_version"] is None
                and latest_generation is not None
            ):
                snap["publishing"]["latest_catalog_version"] = (
                    latest_generation.catalog_version_id
                )
            snap["publishing"]["pending_generation"] = None

            # Per-source watermark + row counts in the published generation.
            from okg.substrate.graph_store import ChronosGraphStore
            from okg.substrate.source_live_counts import source_published_rows

            source_rows, counts_error = source_published_rows(
                conn,
                generation_id=snap["publishing"]["latest_published_generation"],
            )
            # Unknown counts are evidence, not a publish failure: they are
            # reported here and leave the exit code to `errors`.
            snap["source_counts"] = {
                "generation": snap["publishing"]["latest_published_generation"],
                "available": counts_error is None,
                "reason": None if counts_error is None else counts_error["message"],
            }
            seen_sources: set[str] = set()
            for row in source_rows:
                if not _source_in_scope(row["source"]):
                    continue
                seen_sources.add(row["source"])
                snap["sources"].append({
                    **row,
                    "last_reconcile_at": (
                        row["last_reconcile_at"].isoformat()
                        if row["last_reconcile_at"] else None
                    ),
                    "last_observed_at": (
                        row["last_observed_at"].isoformat()
                        if row["last_observed_at"] else None
                    ),
                })
            # Add expected sources from the registry that have
            # not yet written any facts so the dashboard shows
            # the full source list with `waiting` status. Place
            # them at the end of the list so the active sources
            # render first.
            for name in expected_sources:
                if name in seen_sources:
                    continue
                # Unknown counts stay unknown, never a false 0.
                known = counts_error is None
                snap["sources"].append({
                    "source": name,
                    "last_reconcile_at": None,
                    "projection_cursor": None,
                    "n_node_facts": 0 if known else None,
                    "n_edge_facts": 0 if known else None,
                    "last_observed_at": None,
                })

            # Live row counts (top 10 subtypes).
            graph = ChronosGraphStore.from_connection(
                conn,
                ensure_schema=False,
            ).checkout()
            snap["live_subtypes"] = [
                {"subtype": row["subtype"], "count": row["n"]}
                for row in graph.query(
                    """
                    SELECT subtype, count(*) AS n
                      FROM okg.graph_nodes
                     GROUP BY subtype
                     ORDER BY n DESC
                     LIMIT 10
                    """
                )
            ]

        if snap["publishing"]["latest_catalog_version"] is not None:
            if snap["publishing"]["latest_published_generation"]:
                snap["stage"] = "running"
            else:
                snap["stage"] = "migrated"
    except psycopg.errors.UndefinedTable:
        snap["stage"] = "scaffolded"
    except Exception as e:
        snap["errors"].append(
            {"code": "status_probe_failed", "message": str(e)},
        )

    if snap["stage"] in {"running", "migrated"}:
        try:
            with db_connect(dsn) as conn:
                attempt = _latest_publish_attempt(conn, deployment_name)
        except Exception as e:  # noqa: BLE001 - reported, never raised: `okg status` is a report.
            snap["errors"].append(
                {"code": "latest_publish_attempt_unknown", "message": str(e)},
            )
        else:
            snap["publishing"]["latest_attempt"] = attempt
            if attempt is not None and attempt["status"] not in {
                "published", "blocked",
            }:
                # Unknown is never read as healthy (#2573).
                snap["errors"].append({
                    "code": "latest_publish_attempt_unknown",
                    "message": (
                        f"checkpoint {attempt['checkpoint_id']} records "
                        f"status {attempt['status']!r}, neither published "
                        "nor blocked"
                    ),
                })
    return snap


def _latest_publish_attempt(conn, deployment_name: str) -> dict | None:
    """The newest publish attempt on the default branch, published or not.

    `latest_published_*` alone hid a wedged deployment: a publish a gate
    refused leaves a `blocked` checkpoint newer than the last published one,
    readers keep the old version, and `okg status` still looked healthy.
    Run-base checkpoints are cut at every source run's start and are never
    an attempt (issue #2993), so they are left out.
    """
    from okg.substrate.cli.run_summary import _blocked_detail
    from okg.substrate.ingest.run_base import EXCLUDE_RUN_BASE_SQL

    row = conn.execute(
        f"""
        SELECT checkpoint_id, metadata::jsonb, created_at
          FROM public._chronos_branch_interval_checkpoints
         WHERE metadata::jsonb->>'deployment_name' = %(deployment)s
           AND COALESCE(metadata::jsonb->>'branch', 'default') = 'default'
           AND {EXCLUDE_RUN_BASE_SQL}
         ORDER BY created_at DESC, checkpoint_id DESC
         LIMIT 1
        """,
        {"deployment": deployment_name},
    ).fetchone()
    if row is None:
        return None
    metadata = dict(row[1] or {})
    status = metadata.get("status")
    blocked_reason = metadata.get("blocked_reason")
    recovery = (
        blocked_reason.get("recovery") if isinstance(blocked_reason, dict)
        else None
    )
    superseded = (
        {key: recovery.get(key) for key in (
            "superseded_at", "superseded_by", "supersede_reason",
        )}
        if isinstance(recovery, dict) and recovery.get("state") == "superseded"
        else None
    )
    return {
        "checkpoint_id": str(row[0]),
        "status": status,
        # The checkpoint table stores created_at as text.
        "created_at": (
            row[2].isoformat() if hasattr(row[2], "isoformat")
            else (str(row[2]) if row[2] is not None else None)
        ),
        "blocked": status == "blocked",
        "blocked_detail": (
            _blocked_detail(blocked_reason) if status == "blocked" else None
        ),
        # `okg runtime supersede-checkpoint` retires the recovery but leaves
        # the checkpoint blocked; readers stay on the old version.
        "recovery_superseded": superseded,
    }


def _status_exit_code(snap: dict) -> int:
    """1 when a probe failed, 3 when the newest publish attempt was refused.

    3 has the meaning `okg run` gives it: a publish ran, it did not publish,
    and readers still see the previous version (or none).
    """
    from okg.substrate.cli.run_summary import EXIT_BLOCKED, EXIT_FAILED, EXIT_OK

    if snap.get("errors"):
        return EXIT_FAILED
    attempt = (snap.get("publishing") or {}).get("latest_attempt")
    if isinstance(attempt, dict) and attempt.get("blocked"):
        return EXIT_BLOCKED
    return EXIT_OK


def _format_status_dashboard(
    snap: dict,
    *,
    prev: dict | None,
    elapsed_s: float,
) -> str:
    """Render a single status snapshot as a human-readable
    dashboard. `prev` is the prior snapshot (or None on the
    first tick) for delta computation."""

    lines: list[str] = []
    pub = snap["publishing"]
    lines.append(
        f"deployment: {snap['deployment_name']:25s}  "
        f"stage: {snap['stage']:11s}  "
        f"elapsed: {int(elapsed_s)}s"
    )
    lines.append(
        f"  catalog v={pub.get('latest_catalog_version')}   "
        f"published gen={pub.get('latest_published_generation')}"
        f"   pending gen={pub.get('pending_generation')}"
    )
    attempt = pub.get("latest_attempt")
    if isinstance(attempt, dict) and attempt.get("blocked"):
        lines.append(
            f"  BLOCKED latest publish attempt {attempt['checkpoint_id']}: "
            f"{attempt.get('blocked_detail')}"
        )
    if snap.get("source_sync_hold"):
        from okg.substrate.ingest.incomplete_recovery import source_sync_hold_line

        lines.append(f"  HELD {source_sync_hold_line(snap['source_sync_hold'])}")

    # Per-source table.
    prev_facts = {
        s["source"]: s["n_node_facts"]
        for s in (prev or {}).get("sources", [])
    } if prev else {}
    if snap["sources"]:
        lines.append("")
        lines.append(
            f"  {'source':45s} {'reconciled':12s} "
            f"{'nodes':>10s} {'+nodes':>8s} {'edges':>10s}"
        )
        lines.append(f"  {'-' * 45} {'-' * 12} {'-' * 10} "
                     f"{'-' * 8} {'-' * 10}")
        for s in snap["sources"]:
            name = s["source"]
            recon = (
                "yes" if s.get("last_reconcile_at") else "no"
            )
            n = s["n_node_facts"]
            prev_n = prev_facts.get(name)
            delta = (
                n - prev_n if n is not None and prev_n is not None else 0
            )
            delta_text = f"{delta:+d}" if delta else ""
            edges = s["n_edge_facts"]
            # None: the published generation has no count rollup (the
            # reason is printed below the table).
            lines.append(
                f"  {name:45s} {recon:12s} "
                f"{'?' if n is None else format(n, ','):>10s} "
                f"{delta_text:>8s} "
                f"{'?' if edges is None else format(edges, ','):>10s}"
            )

    source_counts = snap.get("source_counts") or {}
    if source_counts.get("available") is False:
        lines.append(f"  source counts unavailable: {source_counts['reason']}")

    if snap.get("live_subtypes"):
        lines.append("")
        lines.append("  live subtypes (top 10):")
        for st in snap["live_subtypes"]:
            lines.append(
                f"    {st['subtype']:35s} {st['count']:>10,d}"
            )

    if snap.get("errors"):
        lines.append("")
        for err in snap["errors"]:
            lines.append(f"  ERROR [{err['code']}]: {err['message']}")

    return "\n".join(lines)


def _fetch_dispatcher():
    """Lazy-import wrapper for `okg fetch-deployments` (per
    `separate-framework-and-deployments`). Importing the fetch
    module pulls in yaml; defer until invoked."""
    def _dispatch(args, *, as_json):
        from okg.substrate.cli import fetch
        return fetch.cmd_fetch_deployments(args, as_json=as_json)
    return _dispatch


def _lifecycle_dispatcher(name: str):
    """Lazy import wrapper for substrate-wide lifecycle verbs
    (per `dead-simple-install`). Importing the lifecycle module
    pulls in yaml; we defer until the verb is actually invoked.
    """
    def _dispatch(args, *, as_json):
        from okg.substrate.cli import lifecycle
        return getattr(lifecycle, name)(args, as_json=as_json)
    return _dispatch


def _cmd_status(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg status` — current deployment state.

    With `--watch`, polls every `--interval` seconds and
    refreshes a dashboard on stderr until SIGINT.
    """
    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    deployment = args.deployment or "default"

    # Optional: resolve deployment dir for full-source-list view
    # in the dashboard. Best-effort — single-shot status still
    # works when no deployment dir is on disk.
    from okg.substrate.deployments import find_deployment_dir
    dep_dir = find_deployment_dir(deployment)

    if not getattr(args, "watch", False):
        snap = _collect_status(
            dsn, deployment, deployment_dir=dep_dir,
        )
        _emit_ok(snap, as_json=as_json)
        rc = _status_exit_code(snap)
        if rc == 3:
            attempt = snap["publishing"]["latest_attempt"]
            print(
                f"error: latest_publish_blocked: the newest publish attempt "
                f"({attempt['checkpoint_id']}) was blocked: "
                f"{attempt['blocked_detail']}. Readers still see "
                f"{snap['publishing'].get('latest_published_generation') or 'no published version'}.",
                file=sys.stderr,
            )
        if snap.get("source_sync_hold"):
            from okg.substrate.ingest.incomplete_recovery import (
                source_sync_hold_line,
            )

            print(
                f"error: source_sync_held: {source_sync_hold_line(snap['source_sync_hold'])}",
                file=sys.stderr,
            )
        return rc

    # Watch mode: refreshing dashboard on stderr.
    import time as _t

    interval = max(1, int(args.interval or 5))
    started = _t.monotonic()
    prev_snap: dict | None = None
    try:
        while True:
            snap = _collect_status(
                dsn, deployment, deployment_dir=dep_dir,
            )
            elapsed = _t.monotonic() - started
            dashboard = _format_status_dashboard(
                snap, prev=prev_snap, elapsed_s=elapsed,
            )
            # Clear screen + cursor home, then redraw. Fall back
            # to a separator if the terminal doesn't support ANSI.
            if sys.stderr.isatty():
                sys.stderr.write("\033[2J\033[H")
            else:
                sys.stderr.write(
                    "\n" + "=" * 60 + "\n"
                )
            sys.stderr.write(dashboard + "\n")
            sys.stderr.flush()
            _t.sleep(interval)
    except KeyboardInterrupt:
        sys.stderr.write("\n(stopped)\n")
        return 0


def _build_embedder(spec: str):
    """Parse a `--embedder` string into an Embedder instance.

    Delegates to the ONE parser in `okg.substrate.embeddings.identity`; this
    was previously a second copy that the embed workflow mirrored (PACT
    reachable-semantic-search). The surviving user-visible message is the
    shared "unknown embedder spec: ..." wording, chosen over the CLI's former
    "unknown --embedder value: ..." because the same parser now serves
    surfaces that have no `--embedder` flag.
    """
    from okg.substrate.embeddings.identity import build_embedder

    return build_embedder(spec)


def _mcp_serve_deployment_unresolvable_error(
    deployment: str,
    exc: Exception,
) -> SubstrateError:
    """Type the deployment-resolution failure `mcp-serve` cannot proceed past.

    `--deployment` defaults to the literal string ``default``, so omitting it
    does not mean "no deployment" -- it means "the deployment named default".
    The release-contract check at the call site below therefore resolves a
    manifest on EVERY invocation, including ``mcp-serve --dsn ...`` with no
    ``--deployment``. `DeploymentResolutionError` is a plain `Exception`
    (deployments/__init__.py), not a `SubstrateError`, so before PACT
    `mcp-serve-typed-deployment-error` it walked straight through the
    `except SubstrateError` boundary and out of `main()` as a five-frame
    traceback: no code, no fix hint, and no mention of the flag the operator
    actually omitted.

    The refusal is translated, never swallowed. A release contract that cannot
    be evaluated must not be silently skipped -- treating "manifest missing" as
    "no contract declared" would be a default-value-as-fallback (a P0 in this
    repo), so this returns an error to emit rather than a permission to serve.
    The original resolution message travels verbatim in both the message and
    `data["resolution_error"]`, so the searched manifest paths it already
    carries survive translation instead of being dropped.
    """
    err = SubstrateError(
        f"okg mcp-serve could not resolve deployment {deployment!r}: {exc}",
        data={
            "deployment": deployment,
            "resolution_error": str(exc),
        },
        fix_hint=(
            "pass --deployment <name> for a deployment whose manifest "
            "resolves; --dsn selects only the DATABASE, and --deployment "
            "still defaults to 'default', so supplying --dsn alone does not "
            "satisfy the deployment lookup. Check that "
            "deployments/<name>/deployment.yaml exists, or set "
            "OKG_DEPLOYMENTS_DIR to the root that holds it"
        ),
    )
    err.code = "deployment_not_found"
    return err


def _mcp_serve_embedding_plan(
    deployment: str,
    *,
    asserted_spec: str | None,
):
    """Resolve MCP semantic capability from deployment authority.

    ``mcp-serve --dsn ...`` without a resolvable manifest remains a standalone
    surface: an explicit ``--embedder`` is its authority and absence is
    disabled. Once a deployment manifest resolves, its typed ``embeddings``
    block is authoritative; the CLI flag may only repeat that value as a
    deprecated compatibility assertion and can never override it.

    Reachability caveat (PACT `mcp-serve-typed-deployment-error`): the
    unresolvable-manifest branch below is reached by PROGRAMMATIC callers and
    its own tests, not by `_cmd_mcp_serve`. The release-contract check that
    runs one step earlier in that handler resolves the manifest itself and now
    refuses by name when it cannot, so an unresolvable deployment never gets
    this far on the CLI. Whether the CLI should offer a manifest-free
    standalone mode at all is an open question for its own change; do not
    "restore" it by making the release contract skip an unevaluated manifest.
    """
    import warnings

    from okg.substrate.deployments import (
        DeploymentConfigError,
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.embeddings import (
        EmbeddingConfig,
        build_embedding_execution_plan,
    )

    try:
        manifest = resolve_deployment(deployment)
    except DeploymentResolutionError:
        config = (
            EmbeddingConfig(enabled=True, backend=asserted_spec)
            if asserted_spec is not None
            else EmbeddingConfig()
        )
        return build_embedding_execution_plan(config, paid_backend="no_probe")

    config = manifest.embedding_config
    if asserted_spec is not None:
        if not config.enabled or asserted_spec != config.backend:
            raise DeploymentConfigError(
                "--embedder conflicts with deployment.yaml's canonical "
                "embeddings backend"
            )
        warnings.warn(
            "mcp-serve --embedder is deprecated for a resolved deployment; "
            "deployment.yaml embeddings.backend already supplies this value",
            UserWarning,
            stacklevel=2,
        )
    # A paid backend makes no probe call at server start (PACT
    # openrouter-embeddings-hnsw); each semantic query embeds its text once.
    return build_embedding_execution_plan(config, paid_backend="no_probe")


def _source_registry_invalid_error(exc: BaseException) -> SubstrateError:
    """Wrap a registry load failure as `source_registry_invalid`.

    The loader's own refusal code (for example `source_registry_unknown_key`)
    is kept as `data.detail_code`, so a caller can tell a misspelled key from
    a malformed file without parsing the message.
    """
    err = SubstrateError(
        f"source_registry.yaml is invalid: {exc}",
        data=(
            {"detail_code": str(exc.code)}
            if isinstance(getattr(exc, "code", None), str)
            else None
        ),
    )
    err.code = "source_registry_invalid"
    return err


def _cmd_enricher(args: argparse.Namespace, *, as_json: bool) -> int:
    if getattr(args, "subcmd", None) != "backfill":
        return _emit_error(
            SubstrateError(f"unknown subcommand: okg enricher {args.subcmd!r}"),
            as_json=as_json,
        )
    from okg.substrate.cli.apply_boundary import missing_apply_error

    if err := missing_apply_error(
        args,
        command="enricher backfill",
        fix_command="okg enricher backfill --apply ...",
    ):
        return _emit_error(err, as_json=as_json)
    try:
        from okg.substrate.enrichers.backfill import (
            run_enricher_backfill,
            select_enricher,
        )
        from okg.substrate.publisher.deployment_stack import (
            build_deployment_publisher_stack,
        )

        dsn = _resolve_deployment_dsn(args)
        stack = build_deployment_publisher_stack(
            dsn=dsn,
            deployment_name=args.deployment,
        )
        # Validate before opening the service's write connection, so unknown or
        # ambiguous names fail with no ledger/fact/admission side effects.
        select_enricher(
            stack.enrichers,
            args.enricher_name,
            deployment=args.deployment,
        )
        result = run_enricher_backfill(
            dsn=dsn,
            deployment=args.deployment,
            enricher_name=args.enricher_name,
            enrichers=stack.enrichers,
            limit=args.limit,
            chunk_size=args.chunk_size,
            branch_name="default",
            publisher_stack=stack,
        )
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    _emit_ok(result.to_dict(), as_json=as_json)
    return 0


def _cmd_ui(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg ui` — serve the operator UI for one deployment (PACT okg-ui-command).

    Every refusal (a non-loopback host or unusable port, no built bundle, no
    deployment name, an unresolvable DSN) happens before the server starts;
    see ``okg.viz.ui_server``.
    """
    from okg.viz import ui_server

    try:
        launch = ui_server.prepare(args)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    return ui_server.serve(launch)


def _cmd_mcp_serve(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg mcp-serve` — run the substrate MCP server.

    Blocks until the MCP client disconnects. Defaults to stdio (intended for
    ``claude mcp add <name> -- okg mcp-serve`` style registrations); pass
    ``--transport streamable-http`` to serve the protocol at http://host:port/mcp
    for clients that reach the server over the network instead of as a
    subprocess.
    """
    # Credential-shape refusals come first, before the MCP import, DSN
    # resolution, or any DB work: an invocation that cannot authenticate the
    # way the operator asked must not get far enough to bind anything.
    if getattr(args, "auth_token", None) is not None:
        from okg.substrate.mcp.http_auth import AUTH_TOKEN_GUIDANCE

        return _emit_error(
            SubstrateError(
                "refusing a literal bearer token passed as a command-line "
                "argument: argv is readable by other local processes (ps) and "
                "lands in shell history",
                fix_hint=(
                    "supply the token through the environment and name the "
                    f"VARIABLE instead -- {AUTH_TOKEN_GUIDANCE}"
                ),
            ),
            as_json=as_json,
        )
    # Posture refusals stay above DSN resolution: an operator who asked for
    # multi-user principals on a transport that cannot carry them must be told
    # THAT, not told about a deployment DSN they were never going to reach.
    transport = getattr(args, "transport", "stdio")
    auth_token_env = getattr(args, "auth_token_env", None)
    multi_user_principals = bool(getattr(args, "multi_user_principals", False))
    oidc_auth = bool(getattr(args, "oidc_auth", False))
    if oidc_auth:
        # PACT mcp-oidc-auth: refuse the impossible combinations from argv
        # alone, before any DSN or network work. run_server repeats them for
        # programmatic callers.
        from okg.substrate.mcp.oidc_auth import (
            oidc_auth_exclusive_error,
            oidc_auth_stdio_error,
        )

        refusal: SubstrateError | None = None
        if transport == "stdio":
            refusal = oidc_auth_stdio_error()
        elif auth_token_env is not None:
            refusal = oidc_auth_exclusive_error("--auth-token-env")
        elif multi_user_principals:
            refusal = oidc_auth_exclusive_error("--multi-user-principals")
        elif bool(getattr(args, "allow_writes", False)) or os.environ.get(
            "OKG_MCP_ALLOW_WRITES"
        ) in {"1", "true", "TRUE", "yes", "YES"}:
            refusal = SubstrateError(
                "--oidc-auth cannot be combined with the MCP write surface: "
                "the write path still resolves identity from the "
                "caller-supplied `authority` argument, so writes would not be "
                "attributed to the verified person",
                fix_hint="drop --allow-writes and OKG_MCP_ALLOW_WRITES",
            )
        if refusal is not None:
            return _emit_error(refusal, as_json=as_json)
    if multi_user_principals:
        # Refuse the impossible combinations here so the operator gets a
        # clean error, not a stack trace, and so no DB work happens first.
        # run_server enforces the identical guards for programmatic callers.
        from okg.substrate.mcp.principal_mapping import (
            multi_user_requires_auth_error,
        )

        if transport == "stdio":
            return _emit_error(
                SubstrateError(
                    "--multi-user-principals is incompatible with "
                    "--transport stdio: stdio carries no HTTP request and "
                    "therefore no principal header, so no per-request "
                    "principal could ever be read",
                    data={"transport": "stdio"},
                    fix_hint=(
                        "serve with --transport streamable-http and "
                        "--auth-token-env VARIABLE_NAME, or drop "
                        "--multi-user-principals"
                    ),
                ),
                as_json=as_json,
            )
        if auth_token_env is None:
            return _emit_error(
                multi_user_requires_auth_error(transport=transport),
                as_json=as_json,
            )
        if bool(getattr(args, "allow_writes", False)) or os.environ.get(
            "OKG_MCP_ALLOW_WRITES"
        ) in {"1", "true", "TRUE", "yes", "YES"}:
            # build_server refuses this too; mirroring it here (including the
            # environment route, not only the flag) keeps the operator on the
            # clean error path instead of an uncaught exception, and keeps the
            # refusal above any DB work.
            return _emit_error(
                SubstrateError(
                    "--multi-user-principals cannot be combined with the MCP "
                    "write surface: the write path still resolves identity "
                    "from the caller-supplied `authority` argument and from "
                    "the process environment subject, so writes would not be "
                    "attributed to the per-request principal",
                    data={"allow_writes": True},
                    fix_hint=(
                        "drop --allow-writes (and unset "
                        "OKG_MCP_ALLOW_WRITES) for a read-only multi-user "
                        "server; write over stdio or the CLI"
                    ),
                ),
                as_json=as_json,
            )
    try:
        from okg.substrate.mcp.server import run_server
    except ImportError as e:
        return _emit_error(
            SubstrateError(
                f"MCP SDK not available: {e}",
                fix_hint="install the `mcp` package in this environment",
            ),
            as_json=as_json,
        )
    # For stdio, MCP owns stdout; emitting JSON here would corrupt the
    # protocol stream, so as_json is ignored. streamable-http leaves stdout
    # free, but keep behavior uniform and let the run loop own the process.
    host = getattr(args, "host", "127.0.0.1")
    allow_writes = bool(getattr(args, "allow_writes", False))
    # Resolve the credential before any DB work so a configured-but-
    # unresolvable one fails closed at startup rather than degrading into an
    # unauthenticated server. The CLI keeps only the yes/no answer -- the
    # variable NAME is what travels on to run_server, which re-resolves it.
    from okg.substrate.mcp.http_auth import (
        resolve_auth_token,
        stdio_auth_incompatibility_error,
    )

    auth_active = False
    if auth_token_env is not None:
        if transport == "stdio":
            return _emit_error(
                stdio_auth_incompatibility_error(auth_token_env),
                as_json=as_json,
            )
        try:
            # Resolution either raises or returns a non-empty token. The value
            # is deliberately not bound to a name here: only the yes/no answer
            # is kept, and the variable NAME travels on to run_server.
            resolve_auth_token(auth_token_env)
        except SubstrateError as e:
            return _emit_error(e, as_json=as_json)
        auth_active = True
    # Refuse enabling the write surface over a non-loopback network transport
    # ABOVE DSN resolution and the release-contract check, next to the other
    # posture refusals, because this decision is decidable from argv alone:
    # (non-loopback host) x (writes on) x (no credential). Below them, an
    # operator who asked to expose graph mutation to anyone who can reach the
    # port would be told about a DSN or a version skew instead -- the security
    # message masked by an operational one. It never opened the surface (the
    # guard still refused once reached, and run_server enforces the identical
    # guard for programmatic callers with the same auth-active value), but the
    # operator saw the wrong answer, and the live refusal capture against a
    # release-declaring deployment could not see the guard at all.
    from okg.substrate.mcp.server import _write_transport_guard

    guard = _write_transport_guard(
        allow_writes=allow_writes,
        transport=transport,
        host=host,
        auth_active=auth_active,
    )
    if guard is not None and guard[0] == "refuse":
        return _emit_error(
            SubstrateError(
                guard[1],
                fix_hint=(
                    "serve over stdio, bind --host 127.0.0.1, drop "
                    "--allow-writes, or require a bearer token with "
                    "--auth-token-env VARIABLE_NAME"
                ),
            ),
            as_json=as_json,
        )
    try:
        from okg.substrate.cli.dsn_authority import resolve_read_dsn

        dsn_resolution = resolve_read_dsn(args, surface="okg mcp-serve")
        dsn = dsn_resolution.dsn
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    # Bound OUTSIDE the try: an `except` clause is evaluated only when the
    # body raises, so importing the name inside the body would turn any import
    # failure above it into a NameError raised from the handler itself.
    from okg.substrate.deployments import DeploymentResolutionError

    # The one readiness answer, plus the login state of the role this
    # server's READ pool will actually connect as (issue #2128). Both
    # failures used to be non-refusals: an unmigrated database raised
    # `UndefinedTable` as a traceback from the release-contract read below,
    # and a NOLOGIN `okg_mcp` let the process exit 0 after logging
    # "starting anyway" while every pool connection was refused -- a server
    # that claimed to serve a graph it could never read.
    from okg.substrate.db.readiness import (
        mcp_read_roles_to_check,
        require_database_ready,
        require_mcp_read_dsn_usable,
    )

    try:
        readiness = require_database_ready(
            dsn,
            surface="okg mcp-serve",
            deployment=args.deployment,
            roles=mcp_read_roles_to_check(),
        )
    except SubstrateError as e:
        # An unresolvable deployment NAME outranks any statement about its
        # database. `--deployment` defaults to the literal string `default`,
        # which resolves nowhere, so a name typo would otherwise be reported
        # as `database_unreachable` for whatever DSN the fallback produced and
        # would never mention the flag the operator omitted. Resolved only on
        # this failure path, so a ready database pays no second lookup and the
        # launch keeps exactly one manifest resolution.
        try:
            from okg.substrate.deployments import resolve_deployment

            resolve_deployment(args.deployment)
        except DeploymentResolutionError as resolution_error:
            return _emit_error(
                _mcp_serve_deployment_unresolvable_error(
                    args.deployment, resolution_error
                ),
                as_json=as_json,
            )
        return _emit_error(e, as_json=as_json)
    # `mcp_read_roles_to_check` returns () whenever OKG_MCP_RO_DSN is set,
    # so the override suppressed the role check on its mere presence. A
    # DSN naming a NOLOGIN role, or carrying a stale password, therefore
    # passed readiness and the server started anyway. Probe it.
    try:
        require_mcp_read_dsn_usable(
            surface="okg mcp-serve", deployment=args.deployment
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    # Issue #3273: resolve the role connection strings the pools will use
    # BEFORE the server is built, so a role with no provisioned password is
    # this command's refusal (`role_password_not_provisioned`, naming
    # `okg db role-passwords --apply`) rather than a traceback from inside it.
    # A resolved password is also PROVED with one login, so a stale
    # secrets-file entry is refused here (`role_password_rejected`) instead of
    # surfacing later as a pool timeout.
    try:
        from okg.substrate.local_database_roles import require_role_dsn_usable

        require_role_dsn_usable(
            dsn, role="okg_mcp", env_var="OKG_MCP_RO_DSN",
            surface="okg mcp-serve",
        )
        if getattr(args, "allow_writes", False) or os.environ.get(
            "OKG_MCP_ALLOW_WRITES"
        ) in {"1", "true", "TRUE", "yes", "YES"}:
            require_role_dsn_usable(
                dsn, role="app_rw", env_var="OKG_MCP_RW_DSN",
                surface="okg mcp-serve",
            )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    # PACT lane-s-redesign-s1 (decision rolecheck A): a read role that may
    # use generic plans is reported, not refused.
    from okg.substrate.db.readiness import mcp_read_plan_cache_warning

    plan_cache_warning = mcp_read_plan_cache_warning(dsn, surface="okg mcp-serve")
    if plan_cache_warning is not None:
        print(f"warning: {plan_cache_warning}", file=sys.stderr)
    try:
        from okg.substrate.deployment_release import (
            enforce_release_contract_if_declared,
        )

        # `serve_read_only`: under the `read_only` verdict this server starts
        # and serves reads alone (PACT unknown-s8-mcp-readonly, #2035).
        release_report = enforce_release_contract_if_declared(
            deployment=args.deployment,
            dsn=dsn,
            process_role="mcp",
            serve_read_only=True,
        )
    except DeploymentResolutionError as e:
        # `enforce_release_contract_if_declared` resolves the manifest before
        # it can decide whether a release contract was declared at all, so an
        # unresolvable name fails HERE. Catch the one exception this call can
        # raise outside the SubstrateError hierarchy -- narrowly, not as a
        # blanket `except Exception` -- so the operator gets the CLI's normal
        # error envelope instead of a traceback.
        return _emit_error(
            _mcp_serve_deployment_unresolvable_error(args.deployment, e),
            as_json=as_json,
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    # One answer about a database behind this code (PACT
    # unknown-s7-release-verdict). With a declared release, the release
    # verdict decides, and its sentence replaces the readiness line: the old
    # order printed "behind ...; continuing" and then refused two calls later.
    # With no declared release there is no verdict, and the #2128 rule
    # stands: report the pending migrations and continue.
    _report_database_behind(
        readiness,
        release_report,
        surface="okg mcp-serve",
        deployment=args.deployment,
    )
    # PACT unknown-s8-mcp-readonly (#2035): the release verdict let this
    # server start only because every pending migration is safe to read
    # behind. It serves the read tools, stamps `schema_behind_by` on every
    # tool response, and refuses every other tool with the same reason.
    read_only_release = None
    if release_report is not None and release_report.get("verdict") == "read_only":
        from okg.substrate.mcp.server import read_only_release_from_report

        read_only_release = read_only_release_from_report(release_report)
        print(
            "note: okg mcp-serve: write and admin tools refuse until the "
            f"database is migrated ({release_report.get('fix_hint')}); every "
            "tool response carries schema_behind_by: "
            f"{read_only_release['schema_behind_by']}",
            file=sys.stderr,
        )
    try:
        embedding_plan = _mcp_serve_embedding_plan(
            args.deployment,
            asserted_spec=args.embedder,
        )
        embedder = embedding_plan if embedding_plan.enabled else None
    except Exception as e:  # normalized below for CLI error rendering
        if isinstance(e, SubstrateError):
            return _emit_error(e, as_json=as_json)
        return _emit_error(
            SubstrateError(str(e), fix_hint="fix deployment.yaml embeddings"),
            as_json=as_json,
        )
    branch_name = getattr(args, "branch", None) or "default"
    run_server(
        transport=transport,
        host=host,
        port=getattr(args, "port", 8100),
        dsn=dsn,
        dsn_authority={
            "dsn": dsn_resolution.dsn,
            "authority": dsn_resolution.authority,
            "target": dsn_resolution.target,
        },
        deployment_name=args.deployment,
        embedder=embedder,
        branch_name=branch_name,
        allow_writes=allow_writes,
        allowed_hosts=tuple(getattr(args, "allowed_host", ()) or ()),
        auth_token_env=auth_token_env,
        multi_user_principals=multi_user_principals,
        oidc_auth=oidc_auth,
        tool_profile=getattr(args, "tool_profile", None),
        tool_profile_ceiling=getattr(args, "tool_profile_ceiling", None),
        read_only_release=read_only_release,
    )
    return 0


def _cmd_agent_runtime_launch(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """`okg agent-runtime-launch` - checked launcher then exec."""
    from okg.substrate.library.agent_runtimes.launcher import (
        AgentLaunchRequest,
        AgentRuntimeBoundaryError,
        bootstrap_agent_runtime_policy,
        build_agent_launch_plan,
        ensure_agent_runtime_directories,
    )

    child_command = list(getattr(args, "child_command", ()) or ())
    if child_command and child_command[0] == "--":
        child_command = child_command[1:]
    subject = (
        getattr(args, "subject_binding_id", None)
        or f"subject:agent:{args.deployment}:{args.runtime}"
    )
    if getattr(args, "break_glass_grant_json", None):
        err = SubstrateError(
            "inline break-glass grants are not accepted by the launcher CLI",
            data={"runtime": args.runtime, "deployment": args.deployment},
            fix_hint=(
                "record an active Nomos break-glass decision and let a trusted "
                "resolver pass the matching grant into the launcher"
            ),
        )
        err.code = "agent_runtime_inline_break_glass_denied"
        return _emit_error(err, as_json=as_json)
    bundle = bootstrap_agent_runtime_policy(
        subject_binding_id=subject,
        deployment=args.deployment,
    )
    trusted_extra_env = {
        key: value
        for key in ("OKG_MCP_RO_DSN", "OKG_MCP_RW_DSN")
        if (value := os.environ.get(key))
    }
    ambient_env = dict(os.environ)
    for key in trusted_extra_env:
        ambient_env.pop(key, None)
    try:
        plan = build_agent_launch_plan(
            bundle,
            AgentLaunchRequest(
                runtime=args.runtime,
                profile_id=args.profile,
                command=child_command,
                workspace_root=args.workspace_root,
                subject_binding_id=subject,
                deployment=args.deployment,
                operator_id=getattr(args, "operator", None),
                run_id=getattr(args, "run_id", None),
                ambient_env=ambient_env,
                extra_env=trusted_extra_env,
                deployments_dir=getattr(args, "deployments_dir", None),
                local_env=getattr(args, "local_env", None),
                role_secrets_file=getattr(args, "role_secrets_file", None),
                allow_builtin_role_passwords=bool(
                    getattr(args, "allow_builtin_role_passwords", False)
                ),
            ),
        )
    except AgentRuntimeBoundaryError as exc:
        if exc.reason_code == "agent_runtime.path_binding_invalid":
            err = SubstrateError(
                str(exc),
                data={"reason_code": exc.reason_code, "trace": dict(exc.trace)},
                fix_hint=(
                    "re-run `okg agent connect --deployment "
                    f"{args.deployment}` to rewrite this registration with "
                    "the deployment's current paths"
                ),
            )
            err.code = "agent_runtime_path_binding_invalid"
            return _emit_error(err, as_json=as_json)
        err = SubstrateError(
            str(exc),
            data={
                "reason_code": exc.reason_code,
                "trace": dict(exc.trace),
            },
            fix_hint=(
                "start through an OKG profile launcher, remove unsafe flags, "
                "or provide a scoped unexpired break-glass grant"
            ),
        )
        err.code = "agent_runtime_boundary_denied"
        return _emit_error(err, as_json=as_json)

    if getattr(args, "dry_run", False):
        _emit_ok({
            "schema": "okg.agent_runtime.launch_plan.v1",
            "runtime": plan.runtime,
            "profile_id": plan.profile.profile_id,
            "agent_run_id": plan.agent_run_id,
            "subject_binding_id": plan.subject_binding_id,
            "cwd": str(plan.cwd),
            "command": dict(plan.trace.get("command") or {}),
            "env_keys": sorted(plan.env),
            "trace": dict(plan.trace),
        }, as_json=as_json)
        return 0

    ensure_agent_runtime_directories(plan)
    os.chdir(plan.cwd)
    os.execvpe(plan.command[0], plan.command, dict(plan.env))
    return 127


def _cmd_agent(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg agent connect|disconnect` (PACT agent-connect-3412)."""
    from okg.substrate.library.agent_runtimes import connect as agent_connect

    try:
        if args.agent_subcmd == "connect":
            report = agent_connect.run_connect(
                args.deployment,
                agents=getattr(args, "agents", None),
                writes=bool(getattr(args, "writes", False)),
                dry_run=bool(getattr(args, "dry_run", False)),
                replace=bool(getattr(args, "replace", False)),
                probe_timeout=float(getattr(args, "probe_timeout", 120.0)),
            )
        else:
            report = agent_connect.run_disconnect(
                args.deployment,
                agents=getattr(args, "agents", None),
                dry_run=bool(getattr(args, "dry_run", False)),
            )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    if as_json:
        print(json.dumps(report, sort_keys=True, default=str))
    else:
        print(agent_connect.render_text(report))
    return int(report["exit_code"])


def _cmd_profile(args: argparse.Namespace, *, as_json: bool) -> int:
    """Inspect profile artifacts."""
    from okg.substrate.profiling.artifacts import (
        find_profile,
        list_profiles,
        load_profile,
        profile_root,
    )

    if args.subcmd == "list":
        rows = list_profiles(getattr(args, "deployment", None))
        limit = getattr(args, "limit", 20) or 20
        items = [
            {
                "profile_run_id": r.get("profile_run_id"),
                "deployment": r.get("deployment"),
                "target_kind": r.get("target_kind"),
                "command": r.get("command"),
                "started_at": r.get("started_at"),
                "duration_ms": r.get("duration_ms"),
                "bottleneck": (r.get("summary") or {}).get("bottleneck_class"),
                "top_stage": (r.get("summary") or {}).get("top_stage"),
            }
            for r in rows[:limit]
        ]
        _emit_ok({"profiles": items, "profile_root": str(profile_root())}, as_json=as_json)
        return 0
    if args.subcmd == "show":
        path = find_profile(args.profile_id, getattr(args, "deployment", None))
        if path is None:
            return _emit_error(
                SubstrateError(
                    f"profile {args.profile_id!r} not found",
                    fix_hint="run `okg perf list` to see available captures",
                ),
                as_json=as_json,
            )
        _emit_ok(load_profile(path), as_json=as_json)
        return 0
    if args.subcmd == "compare":
        left_path = find_profile(args.left, getattr(args, "deployment", None))
        right_path = find_profile(args.right, getattr(args, "deployment", None))
        if left_path is None or right_path is None:
            return _emit_error(
                SubstrateError("one or both profiles were not found"),
                as_json=as_json,
            )
        left = load_profile(left_path)
        right = load_profile(right_path)
        if (
            left.get("deployment") != right.get("deployment")
            or left.get("target_kind") != right.get("target_kind")
        ):
            return _emit_error(
                SubstrateError(
                    "profiles are not comparable by default",
                    data={
                        "left": {
                            "deployment": left.get("deployment"),
                            "target_kind": left.get("target_kind"),
                        },
                        "right": {
                            "deployment": right.get("deployment"),
                            "target_kind": right.get("target_kind"),
                        },
                    },
                ),
                as_json=as_json,
            )
        def stage_totals(p: dict) -> dict[str, float]:
            out: dict[str, float] = {}
            for s in p.get("stages") or []:
                name = s.get("name")
                if name:
                    out[name] = out.get(name, 0.0) + float(s.get("duration_ms") or 0)
            return out
        ltot = stage_totals(left)
        rtot = stage_totals(right)
        stages = sorted(set(ltot) | set(rtot))
        deltas = [
            {
                "stage": s,
                "left_ms": ltot.get(s, 0.0),
                "right_ms": rtot.get(s, 0.0),
                "delta_ms": rtot.get(s, 0.0) - ltot.get(s, 0.0),
            }
            for s in stages
        ]
        _emit_ok({
            "left": left.get("profile_run_id"),
            "right": right.get("profile_run_id"),
            "deployment": left.get("deployment"),
            "target_kind": left.get("target_kind"),
            "stage_deltas": deltas,
        }, as_json=as_json)
        return 0
    return _emit_error(SubstrateError(f"unknown profile subcmd {args.subcmd!r}"), as_json=as_json)


_TEMPLATE_RENDER_SUFFIXES = {".yaml", ".yml", ".md", ".sh", ".py"}
_LEGACY_TEMPLATE_KEYS = {
    "__OKG_REPO_ROOT__": "repo_root",
    "__OKG_REPO_NAME__": "repo_name",
}


def _render_template_text(text: str, variables: dict[str, object]) -> str:
    import re

    def truthy(value: object) -> bool:
        if isinstance(value, str):
            return value.strip().lower() not in {"", "0", "false", "no"}
        return bool(value)

    def render_conditionals(raw: str) -> str:
        out: list[str] = []
        active_stack: list[bool] = []
        for line in raw.splitlines(keepends=True):
            stripped = line.strip()
            match = re.fullmatch(
                r"(?:#\s*)?\{\{\s*if\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}",
                stripped,
            )
            if match:
                active_stack.append(truthy(variables.get(match.group(1))))
                continue
            match = re.fullmatch(
                r"(?:#\s*)?\{\{\s*endif(?:\s+[a-zA-Z_][a-zA-Z0-9_]*)?\s*\}\}",
                stripped,
            )
            if match:
                if active_stack:
                    active_stack.pop()
                continue
            if all(active_stack):
                out.append(line)
        return "".join(out)

    def replace_match(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in variables:
            return match.group(0)
        value = variables[key]
        return "" if value is None else str(value)

    rendered = re.sub(
        r"\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}",
        replace_match,
        render_conditionals(text),
    )
    for legacy_key, variable_key in _LEGACY_TEMPLATE_KEYS.items():
        value = variables.get(variable_key, "")
        rendered = rendered.replace(
            legacy_key,
            "" if value is None else str(value),
        )
    return rendered


def _render_template_dir(src: object, target: object, variables: dict[str, object]) -> None:
    import shutil
    from pathlib import Path

    src_path = Path(src)
    target_path = Path(target)
    for src_file in src_path.rglob("*"):
        rel = src_file.relative_to(src_path)
        dst = target_path / rel
        if src_file.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_file, dst)
        if dst.suffix not in _TEMPLATE_RENDER_SUFFIXES:
            continue
        try:
            text = dst.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rendered = _render_template_text(text, variables)
        if rendered != text:
            dst.write_text(rendered, encoding="utf-8")


# `_cmd_init`, `_cmd_init_from_template`, `_cmd_init_from_profile`, `_cmd_new_module`, `_cmd_new_source` + helpers moved to `cli/init.py` by `extract-cli-init`.


def _cmd_library(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg library list [--kind <k>]` / `library show <ref>`.

    Lists or shows library refs shipped with the substrate.
    Used by agents to discover what's composable without reading
    source.
    """
    from okg.substrate.library import (
        LibraryRefMissing, available_refs, resolve,
    )
    from okg.substrate.library.resolver import KINDS

    if args.subcmd == "list":
        kinds = [args.kind] if args.kind else list(KINDS)
        result: dict = {}
        for k in kinds:
            result[k] = available_refs(k)
        _emit_ok(result, as_json=as_json)
        return 0

    # show
    ref = args.ref
    try:
        target = resolve(ref)
    except LibraryRefMissing as e:
        return _emit_error(e, as_json=as_json)
    from pathlib import Path
    if isinstance(target, Path) and target.is_file():
        if as_json:
            print(json.dumps({
                "ref": ref, "path": str(target),
                "content": target.read_text(),
            }))
        else:
            print(f"# {ref}  ({target})")
            print(target.read_text())
    elif isinstance(target, Path) and target.is_dir():
        files = sorted(
            str(p.relative_to(target))
            for p in target.rglob("*") if p.is_file()
        )
        _emit_ok(
            {"ref": ref, "path": str(target), "files": files},
            as_json=as_json,
        )
    else:
        # modules → importable dotted path
        _emit_ok(
            {"ref": ref, "import": target},
            as_json=as_json,
        )
    return 0


_TRACE_DERIVATION_EDGE_CAP = 25
_TRACE_LINEAGE_ATTRS = (
    "minted_from",
    "nomos_source_lineage",
    "linker",
    "enricher",
    "producer_id",
)


def _trace_json_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _trace_derivation_story(
    *,
    node: dict[str, Any],
    edges: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    attrs = node.get("attrs") or {}
    story: list[dict[str, Any]] = [{
        "kind": "node_origin",
        "source": node.get("source"),
        "source_record_id": node.get("source_record_id") or {},
        "source_revision": node.get("source_revision") or {},
        "run_id": node.get("run_id"),
        "observed_at": _trace_json_value(node.get("observed_at")),
        **(
            {"minted_from": attrs.get("minted_from")}
            if attrs.get("minted_from") is not None else {}
        ),
        "lineage_attrs": {
            key: attrs[key]
            for key in _TRACE_LINEAGE_ATTRS
            if key != "nomos_source_lineage" and key in attrs
        },
    }]
    lineage = attrs.get("nomos_source_lineage")
    if lineage is not None:
        story.append({
            "kind": "source_lineage",
            "nomos_source_lineage": lineage,
        })
    for edge in edges:
        edge_attrs = edge.get("attrs") or {}
        story.append({
            "kind": "adjacent_edge",
            "direction": edge["direction"],
            "edge_id": edge.get("edge_id"),
            "edge_type": edge.get("edge_type"),
            "neighbor_id": edge.get("neighbor_id"),
            "source": edge.get("source"),
            "source_record_id": edge.get("source_record_id") or {},
            "source_revision": edge.get("source_revision") or {},
            "run_id": edge.get("run_id"),
            "observed_at": _trace_json_value(edge.get("observed_at")),
            "provenance": edge.get("provenance"),
            "lineage_attrs": {
                key: edge_attrs[key]
                for key in _TRACE_LINEAGE_ATTRS
                if key in edge_attrs
            },
        })
    return [
        {"ordinal": ordinal, **step}
        for ordinal, step in enumerate(story)
    ]


def _trace_graph_node_preview(
    conn: Any,
    node_id: str,
    *,
    generation_id: int | str | None = None,
) -> dict[str, Any]:
    """Return a compact Chronos preview for trace honesty."""
    sql_node_id = node_id.replace("'", "''")
    replacement_sql = (
        "SELECT node_id, subtype, attrs "
        "FROM okg.graph_nodes "
        f"WHERE node_id = '{sql_node_id}'"
    )
    replacement_command = f"psql \"$OKG_DSN\" -c {json.dumps(replacement_sql)}"
    try:
        from okg.substrate.graph_store import ChronosGraphStore

        store = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        )
        graph = (
            store.checkout_generation(generation_id)
            if generation_id
            else store.checkout()
        )
        rows = graph.query(
            """
            SELECT node_id, subtype, attrs, source, source_record_id,
                   source_revision, run_id, observed_at
              FROM okg.graph_nodes
             WHERE node_id = :node_id
             LIMIT 1
            """,
            {"node_id": node_id},
        )
    except Exception as exc:  # noqa: BLE001 - diagnostic fallback only.
        return {
            "present": None,
            "diagnostic": (
                "chronos_generation_preview_unavailable"
                if generation_id else "chronos_preview_unavailable"
            ),
            "generation_id": generation_id,
            "error": str(exc),
            "replacement_command": replacement_command,
        }
    if not rows:
        return {
            "present": False,
            "diagnostic": (
                "chronos_generation_missing"
                if generation_id else "chronos_default_branch_missing"
            ),
            "generation_id": generation_id,
            "replacement_command": replacement_command,
        }

    row = rows[0]
    attrs = row.get("attrs") or {}
    try:
        edge_total_rows = graph.query(
            """
            SELECT count(*) AS n
              FROM okg.graph_edges
             WHERE src = :node_id OR dst = :node_id
            """,
            {"node_id": node_id},
        )
        edge_total = int(edge_total_rows[0]["n"] if edge_total_rows else 0)
        edge_rows = graph.query(
            f"""
            SELECT edge_id, src, dst, edge_type, attrs, provenance,
                   source, source_record_id, source_revision, run_id, observed_at
              FROM okg.graph_edges
             WHERE src = :node_id OR dst = :node_id
             ORDER BY CASE WHEN dst = :node_id THEN 0 ELSE 1 END,
                      edge_type,
                      CASE WHEN dst = :node_id THEN src ELSE dst END,
                      edge_id
             LIMIT {_TRACE_DERIVATION_EDGE_CAP}
            """,
            {"node_id": node_id},
        )
    except Exception as exc:  # noqa: BLE001 - diagnostic read must stay honest.
        return {
            "present": None,
            "diagnostic": (
                "chronos_generation_derivation_unavailable"
                if generation_id else "chronos_derivation_unavailable"
            ),
            "generation_id": generation_id,
            "node_id": row.get("node_id"),
            "error": str(exc),
            "replacement_command": replacement_command,
        }
    edges = [
        {
            **dict(edge),
            "direction": "in" if edge.get("dst") == node_id else "out",
            "neighbor_id": (
                edge.get("src") if edge.get("dst") == node_id
                else edge.get("dst")
            ),
        }
        for edge in edge_rows
    ]
    text = attrs.get("text")
    text_excerpt = None
    if isinstance(text, str):
        text_excerpt = text[:500]
    return {
        "present": True,
        "diagnostic": (
            "chronos_generation_present"
            if generation_id else "chronos_default_branch_present"
        ),
        "generation_id": generation_id,
        "node_id": row.get("node_id"),
        "subtype": row.get("subtype"),
        "attrs_keys": sorted(str(k) for k in attrs),
        "title": attrs.get("title") if isinstance(attrs, dict) else None,
        "text_excerpt": text_excerpt,
        "derivation_story": _trace_derivation_story(
            node=dict(row),
            edges=edges,
        ),
        "derivation_story_edge_cap": _TRACE_DERIVATION_EDGE_CAP,
        "derivation_story_edge_total": edge_total,
        "derivation_story_truncated": edge_total > len(edges),
    }


def _cmd_source(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg source draft` -- see `okg.substrate.cli.source_draft`."""
    from okg.substrate.cli.source_draft import cmd_source

    return cmd_source(args, as_json=as_json)


def _cmd_trace(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg trace node <node_id>` — dump every fact recorded for a
    node, in fact_id order. Operator tool; read-only."""
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.cli.dsn_authority import resolve_read_dsn

    try:
        dsn_resolution = resolve_read_dsn(args, surface="okg trace node")
    except SubstrateError as err:
        return _emit_error(err, as_json=as_json)
    dsn = dsn_resolution.dsn
    node_id = args.node_id
    generation_id = getattr(args, "generation_id", None)
    with db_connect(dsn, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT fact_id, subtype, source, observed_at, run_id, op,
                   source_record_id, source_revision
              FROM okg.node_facts
             WHERE node_id = %s
             ORDER BY fact_id
            """,
            (node_id,),
        )
        facts = [
            {
                "fact_id": r[0], "subtype": r[1], "source": r[2],
                "observed_at": r[3].isoformat() if r[3] else None,
                "run_id": str(r[4]), "op": r[5],
                "source_record_id": r[6],
                "source_revision": r[7],
            }
            for r in cur.fetchall()
        ]
        graph_node = None
        # `okg trace node` is the diagnostic an operator reaches for when a
        # deployment is ALREADY unhealthy -- including one whose schema
        # snapshot has not been re-applied yet. okg.entity_merge_receipts
        # (#796) is new, so probe for it rather than dying on UndefinedTable:
        # an absent table means no merge has ever been recorded here, which is
        # exactly an empty result, not a failure.
        cur.execute("SELECT to_regclass('okg.entity_merge_receipts') IS NOT NULL")
        # A probe that returns no row at all cannot confirm the table, so treat
        # that the same as absent rather than indexing into None.
        probe = cur.fetchone()
        merge_receipts_exist = bool(probe and probe[0])
        if merge_receipts_exist:
            cur.execute(
                "SELECT merge_id, deployment_name, branch_name, survivor_node_id, "
                "subtype, pinned_checkpoint, plan_digest, status, "
                "repair_generation_id, actor, applied_at "
                "FROM okg.entity_merge_receipts WHERE retired_node_id = %s "
                "AND branch_name = %s "
                "AND (CAST(%s AS TEXT) IS NULL OR deployment_name = %s) "
                "ORDER BY applied_at",
                (
                    node_id,
                    getattr(args, "branch", "default"),
                    dsn_resolution.deployment,
                    dsn_resolution.deployment,
                ),
            )
            retired_by_merges = [
                {
                    "merge_id": str(row[0]),
                    "deployment_name": str(row[1]),
                    "branch_name": str(row[2]),
                    "survivor_node_id": str(row[3]),
                    "subtype": str(row[4]),
                    "pinned_checkpoint": str(row[5]),
                    "plan_digest": str(row[6]),
                    "status": str(row[7]),
                    "repair_generation_id": row[8],
                    "actor": str(row[9]),
                    "applied_at": row[10].isoformat(),
                }
                for row in cur.fetchall()
            ]
        else:
            retired_by_merges = []
        diagnostic = "fact_log"
        if not facts:
            graph_node = _trace_graph_node_preview(
                conn, node_id, generation_id=generation_id,
            )
            if graph_node.get("present") is True:
                diagnostic = "fact_log_empty_graph_node_present"
            elif graph_node.get("present") is None:
                diagnostic = "fact_log_empty_graph_preview_unavailable"
    _emit_ok(
        {
            "node_id": node_id,
            "diagnostic": diagnostic,
            "facts": facts,
            "retired_by_merges": retired_by_merges,
            # An empty list because this deployment has no receipts table is a
            # different fact from an empty list because nothing was merged, and
            # on an unhealthy deployment it is the more useful one.
            "merge_receipts_available": merge_receipts_exist,
            "deployment": dsn_resolution.deployment,
            "authority": dsn_resolution.authority,
            **({"target": dsn_resolution.target} if dsn_resolution.target else {}),
            **({"generation_id": generation_id} if generation_id else {}),
            **({"graph_node": graph_node} if graph_node is not None else {}),
        },
        as_json=as_json,
    )
    return 0


# `_cmd_generation` moved to `cli/generation.py` by `extract-cli-generation`.

# `_admission_row`, `_print_admission_list`, `_print_admission_show`
# moved to `cli/admission.py` by `extract-cli-admission`.


# `_cmd_generation_list` moved to `cli/generation.py` by `extract-cli-generation`.

# `_cmd_branch` moved to `cli/branch.py` by `extract-cli-branch`.


# `_generation_consistency_diagnostics` moved to `cli/generation.py` by `extract-cli-generation`.

# `_consistency_gate_health_dict` moved to `cli/generation.py` by `extract-cli-generation`.

def _cmd_bench(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg bench inject|synth ...` — the shipped benchmark harness.

    ``inject``: the corpus-first injection benchmark (#749) — deterministic
    scorecard core; generation ids bind in run-binding.json. ``synth``: the
    KG-first synthetic-corpus benchmark (ring 1a-prime) — generate from the
    frozen Synthea world, corrupt, render, publish, detect, score."""
    if args.bench_cmd == "synth":
        return _cmd_bench_synth(args, as_json=as_json)
    if args.bench_cmd != "inject":  # argparse enforces membership already
        print(f"unknown bench command: {args.bench_cmd}", file=sys.stderr)
        return 2

    from pathlib import Path as _Path

    from okg.bench.injection import render_scorecard, run_injection_benchmark

    from okg.bench.injection.types import InjectionError
    from okg.substrate.errors import SubstrateError

    out_dir = _Path(args.out)
    work_dir = (
        _Path(args.work_dir)
        if args.work_dir
        else out_dir.parent / f"{out_dir.name}-working"
    )
    try:
        result = run_injection_benchmark(
            dsn=_resolve_dsn(args),
            deployment_dir=_Path(args.deployment_dir),
            corpus_dir=_Path(args.corpus) if args.corpus else None,
            out_dir=out_dir,
            work_dir=work_dir,
            seed=args.seed,
        )
    except (InjectionError, SubstrateError) as exc:
        if as_json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        else:
            print(f"bench inject failed: {exc}", file=sys.stderr)
        return 1
    core = result["scorecard"]["core"]
    if as_json:
        print(json.dumps({
            "ok": True,
            "out_dir": str(out_dir),
            "clean_generation_id": str(result["clean"]["generation_id"]),
            "mutated_generation_id": str(result["mutated"]["generation_id"]),
            "core": core,
        }, indent=2, sort_keys=True, default=str))
    else:
        print(render_scorecard(core), end="")
        print(f"artifacts: {out_dir}")
        print(
            "generations: clean="
            f"{result['clean']['generation_id']} mutated="
            f"{result['mutated']['generation_id']} (run-binding.json)"
        )
    return 0


def _cmd_bench_synth(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg bench synth --deployment-dir <dir> --out <dir> [--preset P]` —
    one-shot KG-first synthetic-corpus benchmark run."""
    from pathlib import Path as _Path

    from okg.bench.synth.config import ConfigError, load_config
    from okg.bench.synth.runner import (
        RunnerError,
        render_synth_scorecard,
        run_synth_benchmark,
    )
    from okg.substrate.errors import SubstrateError

    try:
        config = load_config(args.config) if args.config else None
        result = run_synth_benchmark(
            dsn=_resolve_dsn(args),
            deployment_dir=_Path(args.deployment_dir),
            out_dir=_Path(args.out),
            preset=args.preset,
            config=config,
            world_dir=_Path(args.world_dir) if args.world_dir else None,
        )
    except (ConfigError, RunnerError, SubstrateError) as exc:
        if as_json:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
        else:
            print(f"bench synth failed: {exc}", file=sys.stderr)
        return 1
    core = result["scorecard"]
    if as_json:
        print(json.dumps({
            "ok": True,
            "out_dir": str(args.out),
            "clean_generation_id": str(result["clean"]["generation_id"]),
            "corrupted_generation_id": str(result["corrupted"]["generation_id"]),
            "core": core,
        }, indent=2, sort_keys=True, default=str))
    else:
        print(render_synth_scorecard(core), end="")
        print(f"artifacts: {args.out}")
    return 0


def _cmd_consistency_rerun(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg consistency-rerun --deployment <name> [--generation <id>]`
    — re-run the deployment's post-publish consistency checks on
    an existing published generation.

    Each invocation creates one new terminal run per check (documented
    multiplicity, not uniqueness — no upsert, prior runs' rows are retained);
    every check's summary carries a ``runs`` block with ``runs_total`` and
    ``runs_non_terminal`` so repeated reruns stay bounded and any orphaned
    (still-``running``) run is visible in the output.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.consistency import (
        load_ir_consistency_checks,
        load_post_publish_consistency_checks,
        run_post_publish_consistency_checks,
    )
    from okg.substrate.consistency.ir_executor import IrPostPublishCheck
    from okg.substrate.deployments import (
        find_deployment_dir,
        inject_deployment_path,
    )

    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg consistency-rerun requires --deployment",
                fix_hint=(
                    "okg consistency-rerun "
                    "--deployment <name> [--generation <id>]"
                ),
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
        dep_dir = find_deployment_dir(deployment)
        if dep_dir is None:
            raise SubstrateError(
                f"deployment dir for {deployment!r} not found",
                data={"deployment": deployment},
                fix_hint="check deployments/<name>/ exists",
            )
        inject_deployment_path(dep_dir)
        # Post-publish checks = transform-IR checks (the standard) merged with
        # any transitional SQL-blob checks, mirroring the publisher stack
        # (deployment_stack.build_deployment_publisher_stack). consistency-rerun
        # otherwise diverges by loading only the SQL checks, silently ignoring a
        # deployment's ir_consistency_checks.
        checks = [
            *(IrPostPublishCheck(spec)
              for spec in load_ir_consistency_checks(dep_dir)),
            *load_post_publish_consistency_checks(dep_dir),
        ]
        scope_subject_ids = _consistency_scope_subject_ids(args)
        with db_connect(dsn, autocommit=True) as conn:
            payload = run_post_publish_consistency_checks(
                conn,
                checks,
                deployment_name=deployment,
                generation_id=args.generation,
                scope_subject_ids=scope_subject_ids,
            )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    _emit_ok(payload, as_json=as_json)
    return 0


def _consistency_scope_subject_ids(args: argparse.Namespace) -> list[str]:
    subjects: list[str] = []
    for raw in getattr(args, "scope_subject", None) or []:
        subjects.extend(part.strip() for part in str(raw).split(","))
    return list(dict.fromkeys(subject for subject in subjects if subject))


def _cmd_consistency_scope_report(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg consistency-scope-report --deployment <name> [--check-id <id>]
    [--lookback <n>]` — the locality / scope-decision observability surface.

    For each IR consistency check in the deployment, report across the last
    ``--lookback`` runs: how many ran scoped vs. a full universe sweep, the
    scope-mode distribution, the fallback-reason distribution over the FULL
    runs (WHY each went full), and the last-N raw scope decisions for
    drill-down. "Which checks never run incrementally, and why" becomes a
    measurement here, not archaeology.

    This is the prerequisite for justifying any future scope-analyzer
    extension: teaching the analyzer to scope a new shape (GROUP BY, LIMIT,
    node-joins, ...) is only worth it if this surface shows real checks
    falling back to full for that shape. Extend the analyzer against this
    measurement, never against a guess.
    """
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.consistency import (
        summarize_consistency_scope_decisions,
    )

    deployment = args.deployment or os.environ.get("OKG_DEPLOYMENT")
    if not deployment:
        return _emit_error(
            SubstrateError(
                "okg consistency-scope-report requires --deployment",
                fix_hint=(
                    "okg consistency-scope-report --deployment <name> "
                    "[--check-id <id>] [--lookback <n>]"
                ),
            ),
            as_json=as_json,
        )

    lookback = getattr(args, "lookback", None)
    lookback = 20 if lookback is None else int(lookback)
    if lookback <= 0:
        return _emit_error(
            SubstrateError(
                "okg consistency-scope-report: --lookback must be positive",
                fix_hint="--lookback <n> where n >= 1",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
        # No swallow: a failing scope-decision read (DB fault) surfaces rather
        # than reporting a default-zero summary (the P0 silent-fallback class).
        with db_connect(dsn, autocommit=True) as conn:
            payload = summarize_consistency_scope_decisions(
                conn,
                deployment_name=deployment,
                check_id=getattr(args, "check_id", None),
                lookback=lookback,
            )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_schema(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg schema` router — `show` (config JSON Schema) or `quality` (scorecard)."""
    schema_cmd = getattr(args, "schema_cmd", None)
    if schema_cmd == "quality":
        from okg.substrate.cli.schema import cmd_schema_quality

        return cmd_schema_quality(args, as_json=as_json)
    if schema_cmd in ("show", None):
        return _cmd_schema_show(args, as_json=as_json)
    return _emit_error(
        SubstrateError(
            f"unknown schema subcommand {schema_cmd!r}",
            fix_hint="use `okg schema show <file>` or `okg schema quality`",
        ),
        as_json=as_json,
    )


def _cmd_schema_show(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg schema show <file>` — emit the JSON Schema for an authored file."""
    from jsonschema import Draft202012Validator, SchemaError

    from okg.substrate.schema_resources import AUTHORED_FILE_SCHEMAS, schemas_dir

    if not getattr(args, "file", None):
        return _emit_error(
            SubstrateError(
                "okg schema show requires a file",
                fix_hint="e.g. okg schema show deployment.yaml",
            ),
            as_json=as_json,
        )

    mapping = AUTHORED_FILE_SCHEMAS
    target = args.file.strip()
    schema_file = mapping.get(target)
    if not schema_file:
        return _emit_error(
            SubstrateError(
                f"no schema known for {target!r}",
                data={"available": list(mapping.keys())},
                fix_hint=f"one of: {', '.join(mapping.keys())}",
            ),
            as_json=as_json,
        )
    try:
        path = schemas_dir() / schema_file
    except FileNotFoundError as exc:
        return _emit_error(SubstrateError(str(exc), fix_hint="reinstall okg"), as_json=as_json)
    if not path.is_file():
        return _emit_error(
            SubstrateError(
                f"schema file not found at {path}",
                fix_hint="reinstall okg or rebuild schemas/",
            ),
            as_json=as_json,
        )
    try:
        contents = path.read_text(encoding="utf-8")
        Draft202012Validator.check_schema(json.loads(contents))
    except (OSError, ValueError, SchemaError) as exc:
        return _emit_error(
            SubstrateError(
                f"schema file invalid at {path}: {exc}",
                fix_hint="reinstall okg or fix the canonical schema in schemas/",
            ),
            as_json=as_json,
        )
    print(contents)
    return 0


# `_cmd_view`, `_cmd_view_list`, `_cmd_view_show`, `_cmd_view_inspect`, `_emit_view_inspection_text` moved to `cli/view.py` by `extract-cli-view`.


def _cmd_lint(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg lint <target>` dispatcher.

    Phase 2 §6.3 ships ``okg lint deployments`` which walks every
    deployment's manifest and checks the Python modules listed in
    ``runtime.workflows`` for substrate-boundary violations.
    """
    from pathlib import Path

    if args.lint_target == "deployments":
        from okg.substrate.runtime.lint import (
            BOUNDARY_IN_TREE,
            BOUNDARY_LEGACY_ENRICHER,
            BOUNDARY_SCANNED,
            BOUNDARY_UNRESOLVED,
            CHAT_CHECKED,
            chat_block_coverage,
            lint_deployments_report,
        )

        deployments_root = args.deployments_root or "deployments"
        report = lint_deployments_report(
            deployments_root=deployments_root,
            repo_root=Path.cwd(),
        )
        findings = report.findings
        # Chat-block coverage is STATED per deployment, never inferred from an
        # empty findings list: "no chat findings" is produced equally by a
        # clean block, an absent block, and a manifest lint could not read.
        chat_coverage = chat_block_coverage(
            deployments_root=deployments_root,
            repo_root=Path.cwd(),
        )
        chat_blocking = [c for c in chat_coverage if c.blocks_exit]
        # What the boundary scan did with every declared workflow, connector
        # and enricher entry, STATED rather than inferred from the findings
        # list: an exempt in-tree entry, an enricher on the legacy surface and
        # an unscanned one all contribute zero findings.
        entry_coverage = report.entry_coverage
        entry_counts = {
            state: sum(1 for c in entry_coverage if c.state == state)
            for state in (
                BOUNDARY_SCANNED, BOUNDARY_IN_TREE, BOUNDARY_LEGACY_ENRICHER,
                BOUNDARY_UNRESOLVED,
            )
        }

        payload = [
            {
                "code": f.code,
                "path": str(f.path),
                "line": f.line,
                "importing_symbol": f.importing_symbol,
                "entry": f.entry,
                "message": f.message,
            }
            for f in findings
        ]
        chat_payload = [c.as_dict() for c in chat_coverage]
        chat_checked = sum(1 for c in chat_coverage if c.state == CHAT_CHECKED)

        if as_json:
            print(json.dumps(
                {
                    "findings": payload,
                    "count": len(payload),
                    "chat_coverage": chat_payload,
                    "chat_summary": {
                        "checked": chat_checked,
                        "blocking": len(chat_blocking),
                        "total": len(chat_coverage),
                    },
                    "entry_coverage": [c.as_dict() for c in entry_coverage],
                    "entry_summary": {
                        **entry_counts,
                        "total": len(entry_coverage),
                    },
                },
                indent=2,
            ))
        else:
            if not findings:
                print("okg lint deployments: clean (no violations)")
            else:
                print(
                    f"okg lint deployments: {len(findings)} violation(s)"
                )
                for f in findings:
                    print(f"  [{f.code}] {f.path}:{f.line}: {f.message}")
            print(
                f"boundary entries: {entry_counts[BOUNDARY_SCANNED]} scanned, "
                f"{entry_counts[BOUNDARY_IN_TREE]} in-tree (exempt), "
                f"{entry_counts[BOUNDARY_LEGACY_ENRICHER]} on the legacy "
                "enricher surface (stated, see detail), "
                f"{entry_counts[BOUNDARY_UNRESOLVED]} unresolved of "
                f"{len(entry_coverage)} declared"
            )
            for c in entry_coverage:
                if c.state == BOUNDARY_LEGACY_ENRICHER:
                    print(f"  {c.deployment}: {c.label}: {c.detail}")
            print(
                f"chat blocks: {chat_checked} checked and clean of "
                f"{len(chat_coverage)} deployment(s)"
            )
            for c in chat_coverage:
                line = f"  {c.deployment}: {c.state}"
                if c.rule:
                    line += f" [{c.rule}]"
                print(line)
                if c.blocks_exit and c.message:
                    print(f"    {c.message}")
                    if c.offending_value is not None:
                        print(f"    offending value: {c.offending_value!r}")

        return 0 if not findings and not chat_blocking else 1

    return _emit_error(
        SubstrateError(
            f"okg lint {args.lint_target!r} unrecognized; "
            "supported targets: deployments",
        ),
        as_json=as_json,
    )


# `_cmd_deployment`, `_cmd_deployment_show`, `_cmd_deployment_bug_report` moved to `cli/deployment.py` by `extract-cli-deployment`.


# `_cmd_metrics`, `_cmd_metrics_qa_snapshot`, `_parse_since`, `_SINCE_UNITS` moved to `cli/metrics.py` by `extract-cli-metrics`.


def _cmd_snapshot_load(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg snapshot-load --apply --snapshot <path> --deployment <name>`
    — load a JSON snapshot as the deployment's first published
    generation. Chronos-native: the snapshot's `nodes` / `edges`
    arrays are streamed into `NodeFact` / `EdgeFact`, applied to
    the Chronos `default` branch, and checkpointed as a published
    OKG generation (over `okg.graph_nodes` / `okg.graph_edges`).

    This is the deployment-onboarding entry point — operators run
    it once after `okg migrate --apply` (and, optionally, `okg
    catalog load --apply`) to seed the substrate from a legacy
    snapshot.

    Refuses to run against a non-empty `okg.graph_nodes`. Non-empty
    onboarding requires a separately authenticated workflow authority;
    it is deliberately unavailable as a generic CLI bypass.
    """
    from pathlib import Path

    from okg.substrate.db.bootstrap import bootstrap_snapshot

    snapshot = Path(args.snapshot).expanduser().resolve()
    if not snapshot.is_file():
        return _emit_error(
            SubstrateError(
                f"snapshot file not found: {snapshot}",
                fix_hint="pass --snapshot <path> to a JSON snapshot",
            ),
            as_json=as_json,
        )

    try:
        dsn = _resolve_dsn(args)
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    deployment_name = (
        getattr(args, "deployment_name", None)
        or getattr(args, "deployment", None)
        or "default"
    )
    if not getattr(args, "apply", False):
        _emit_ok(
            {
                "ok": True,
                "dry_run": True,
                "snapshot": str(snapshot),
                "deployment_name": deployment_name,
                "note": "pass --apply to actually load",
            },
            as_json=as_json,
        )
        return 0

    try:
        result = bootstrap_snapshot(
            dsn,
            snapshot,
            deployment_name,
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    except Exception as e:
        return _emit_error(
            SubstrateError(
                f"snapshot-load failed: {e}",
                data={"snapshot": str(snapshot),
                      "deployment_name": deployment_name},
            ),
            as_json=as_json,
        )

    _emit_ok(
        {
            "ok": True,
            "generation_id": result.generation_id,
            "nodes_appended": result.nodes_appended,
            "edges_appended": result.edges_appended,
            "elapsed_seconds": round(result.elapsed_seconds, 2),
        },
        as_json=as_json,
    )
    return 0


def _cmd_alias(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg alias <subcmd>` — manage the alias dictionary
    (refine-doc-corpus-extraction Stride F)."""
    import os

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.aliases import (
        AliasNoActiveCatalog,
        add_alias,
        list_aliases,
        list_alias_collisions,
        remove_alias,
        seed_from_subtype,
    )

    dsn = args.dsn or os.environ.get("OKG_DSN")
    if not dsn:
        return _emit_error(
            SubstrateError(
                "no DSN provided; pass --dsn or set OKG_DSN",
                fix_hint="--dsn postgres://... or export OKG_DSN=...",
            ),
            as_json=as_json,
        )
    sub = args.subcmd
    try:
        with db_connect(dsn, autocommit=True) as conn:
            if sub == "seed-from-subtype":
                result = seed_from_subtype(
                    conn,
                    subtype=args.subtype,
                    field=args.field,
                    scope=args.scope,
                    canonical_template=getattr(
                        args, "canonical_template", None,
                    ),
                    apply=getattr(args, "apply", False),
                )
                _emit_ok({
                    "subcmd": "seed-from-subtype",
                    "catalog_version_id": result.catalog_version_id,
                    "subtype": result.subtype,
                    "field": result.field,
                    "canonical_template": result.canonical_template,
                    "dry_run": result.dry_run,
                    "inserted_count": result.inserted_count,
                    "conflict_count": result.conflict_count,
                    # fix-projection-alias-identity #686: split the conflict tally
                    # and surface refused collisions at the seed output.
                    "idempotent_count": result.idempotent_count,
                    "collision_count": result.collision_count,
                    "quarantined_count": result.quarantined_count,
                    "sample_quarantined": result.sample_quarantined,
                    "collisions": [
                        {
                            "alias_value": c.alias_value,
                            "canonicals": c.canonicals,
                            "distinct_canonical_count": c.distinct_canonical_count,
                        }
                        for c in result.collisions
                    ],
                    "sample_inserted": result.sample_inserted,
                }, as_json=as_json)
                return 0
            if sub == "list":
                rows = list_aliases(
                    conn, scope=args.scope, limit=args.limit,
                )
                _emit_ok({
                    "subcmd": "list",
                    "scope": args.scope,
                    "limit": args.limit,
                    "rows": rows,
                }, as_json=as_json)
                return 0
            if sub == "collisions":
                rows = list_alias_collisions(conn, limit=args.limit)
                _emit_ok({
                    "subcmd": "collisions",
                    "limit": max(1, min(int(args.limit), 1000)),
                    "collision_count": len(rows),
                    "rows": rows,
                    "remediation": (
                        "okg remediate collapsed-aliases --subtype <type> "
                        "--field <field>"
                    ),
                }, as_json=as_json)
                return 0
            if sub == "add":
                result = add_alias(
                    conn,
                    alias=args.alias, canonical=args.canonical,
                    scope=args.scope,
                )
                _emit_ok({
                    "subcmd": "add", **result,
                }, as_json=as_json)
                return 0
            if sub == "remove":
                result = remove_alias(conn, alias=args.alias)
                _emit_ok({
                    "subcmd": "remove", **result,
                }, as_json=as_json)
                return 0
        return _emit_error(
            SubstrateError(f"unknown alias subcommand: {sub!r}"),
            as_json=as_json,
        )
    except AliasNoActiveCatalog as e:
        return _emit_error(e, as_json=as_json)


def _cmd_remediate(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg remediate <subcmd>` — bounded remediation of pre-fix
    projection-identity damage (fix-projection-alias-identity, ruling 4). Every
    subcommand is dry-run by default; --apply is the explicit mutation guard."""
    import dataclasses
    import os

    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.catalog.aliases import AliasNoActiveCatalog
    from okg.substrate.projection.remediation import (
        enumerate_multi_target_edges,
        remediate_entity_merge,
        remediate_collapsed_aliases,
        remediate_stranded_link_cache,
    )

    dsn = args.dsn or os.environ.get("OKG_DSN")
    if not dsn:
        return _emit_error(
            SubstrateError(
                "no DSN provided; pass --dsn or set OKG_DSN",
                fix_hint="--dsn postgres://... or export OKG_DSN=...",
            ),
            as_json=as_json,
        )
    sub = args.subcmd
    try:
        with db_connect(dsn, autocommit=True) as conn:
            if sub == "stranded-link-cache":
                res = remediate_stranded_link_cache(
                    conn,
                    extractor_source_subtypes=args.subtypes,
                    deployment_name=args.deployment_name,
                    branch_name=args.branch,
                    cursor=args.cursor,
                    apply=getattr(args, "apply", False),
                    limit=args.limit,
                )
                _emit_ok({"subcmd": sub, **dataclasses.asdict(res)}, as_json=as_json)
                return 0
            if sub == "link-endpoints":
                from okg.substrate.projection.remediation import (
                    backfill_link_endpoints,
                )

                res = backfill_link_endpoints(
                    conn,
                    entity_types=args.entity_types,
                    deployment_name=args.deployment_name,
                    branch_name=args.branch,
                    cursor=args.cursor,
                    apply=getattr(args, "apply", False),
                    limit=args.limit,
                )
                _emit_ok({"subcmd": sub, **dataclasses.asdict(res)}, as_json=as_json)
                return 0
            if sub == "collapsed-aliases":
                res = remediate_collapsed_aliases(
                    conn,
                    subtype=args.subtype,
                    field=args.field,
                    entity_type=getattr(args, "entity_type", None),
                    deployment_name=args.deployment_name,
                    branch_name=args.branch,
                    run_id=args.run_id,
                    cursor=args.cursor,
                    limit=args.limit,
                    apply=getattr(args, "apply", False),
                )
                _emit_ok({"subcmd": sub, **dataclasses.asdict(res)}, as_json=as_json)
                return 0
            if sub == "multi-target-report":
                if not 1 <= int(args.limit) <= 1000:
                    raise ValueError(
                        "multi-target report requires --limit between 1 and 1000"
                    )
                requested_limit = int(args.limit)
                page = enumerate_multi_target_edges(
                    conn,
                    catalog_version_id=args.catalog_version_id,
                    cursor=args.cursor,
                    limit=requested_limit + 1,
                )
                has_more = len(page) > requested_limit
                groups = page[:requested_limit]
                next_cursor = (
                    json.dumps([groups[-1].src, groups[-1].edge_type])
                    if has_more and groups else None
                )
                _emit_ok({
                    "subcmd": sub,
                    "group_count": len(groups),
                    "limit": requested_limit,
                    "input_cursor": args.cursor,
                    "next_cursor": next_cursor,
                    "groups": [dataclasses.asdict(g) for g in groups],
                }, as_json=as_json)
                return 0
            if sub == "derived-node-exposure":
                from okg.substrate.derived_node_liveness import (
                    report_derived_node_exposure,
                )

                report = report_derived_node_exposure(
                    conn,
                    deployment_name=args.deployment_name,
                    branch=args.branch,
                    generation=args.generation,
                    example_limit=args.example_limit,
                )
                _emit_ok(report, as_json=as_json)
                return 0
            if sub == "entity-merge":
                dispositions: dict[str, str] = {}
                for item in getattr(args, "merge_attr", None) or []:
                    name, _, disposition = str(item).partition("=")
                    if not name or not disposition:
                        return _emit_error(
                            SubstrateError(
                                f"--merge-attr expects NAME=DISPOSITION, got "
                                f"{item!r}"
                            ),
                            as_json=as_json,
                        )
                    dispositions[name] = disposition
                res = remediate_entity_merge(
                    conn,
                    survivor_node_id=args.survivor,
                    retired_node_id=args.retire,
                    deployment_name=args.deployment_name,
                    branch_name=args.branch,
                    expected_plan_digest=args.expected_plan_digest,
                    accept_survivor_attributes=args.accept_survivor_attrs,
                    attribute_dispositions=dispositions,
                    actor=args.actor,
                    apply=getattr(args, "apply", False),
                )
                _emit_ok(
                    {"subcmd": sub, **dataclasses.asdict(res)},
                    as_json=as_json,
                )
                return 0
            if sub == "retire-source":
                from okg.substrate.source_retirement import (
                    remediate_source_retirement,
                )

                res = remediate_source_retirement(
                    conn,
                    source_name=args.source,
                    deployment_name=args.deployment_name,
                    branch_name=args.branch,
                    expected_plan_digest=args.expected_plan_digest,
                    confirm_source=args.confirm_source,
                    actor=args.actor,
                    apply=getattr(args, "apply", False),
                )
                _emit_ok(
                    {"subcmd": sub, **dataclasses.asdict(res)},
                    as_json=as_json,
                )
                return 0
        return _emit_error(
            SubstrateError(f"unknown remediate subcommand: {sub!r}"),
            as_json=as_json,
        )
    except AliasNoActiveCatalog as e:
        return _emit_error(e, as_json=as_json)
    except ValueError as e:
        return _emit_error(SubstrateError(str(e)), as_json=as_json)


_PUBLISHER_PHASE_EVENT_NAMES: tuple[str, ...] = (
    "reserve_complete",
    "phase_a_complete",
    "phase_b_complete",
    "commit_complete",
    "post_publish_views_complete",
    "post_publish_lenses_complete",
    "post_publish_consistency_complete",
    "cycle_complete",
)


def _read_publisher_phase_events(
    *,
    dsn: str,
    workflow_id: str,
) -> list[dict[str, Any]]:
    """Read the 8 normative publisher phase events for a workflow.

    Per ``decompose-publisher-cycle-into-dbos-steps``
    (deepen-dbos-integration Phase 1). Returns events in spec-order;
    missing events (e.g. cycle blocked before that phase) are
    omitted rather than represented as None. Each entry is
    ``{"name": str, "payload": dict}``.

    Reads via ``DBOSClient.get_event`` (handles deserialization)
    with a short timeout per name so a missing-event lookup
    doesn't hang. The CLI surface tolerates partial event sets.
    """
    from okg.substrate.runtime.admin import client_get_event

    events: list[dict[str, Any]] = []
    for name in _PUBLISHER_PHASE_EVENT_NAMES:
        try:
            payload = client_get_event(
                dsn=dsn,
                workflow_id=workflow_id,
                key=name,
                timeout_seconds=0.5,
            )
        except Exception:  # noqa: BLE001
            payload = None
        if payload is not None:
            events.append({"name": name, "payload": payload})
    return events


def _coerce_dt(value):
    """Best-effort coercion of a started_at/completed_at value to
    a timezone-aware datetime. Accepts ISO strings, datetimes,
    epoch-ms ints, or None."""
    from datetime import datetime, timezone

    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        # DBOS workflow_status uses epoch-ms; if value >> seconds,
        # treat as ms.
        if value > 10_000_000_000:
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return (
                dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            )
        except ValueError:
            return None
    return None


def _find_profile_for_workflow(
    *, workflow_id: str,
) -> str | None:
    """Look up the local profile artifact whose request_path matches
    the given DBOS workflow_uuid (per
    ``restore-publisher-perf-and-profiling`` §B linkage).

    Returns the absolute path of the artifact's ``profile.json``
    when found; None otherwise. Scans `list_profiles()` (no
    deployment filter so an operator can pass any workflow_uuid
    without knowing the deployment).
    """
    try:
        from okg.substrate.profiling import list_profiles
    except Exception:  # noqa: BLE001
        return None
    try:
        profiles = list_profiles()
    except Exception:  # noqa: BLE001
        return None
    # Most-recent profiles come first; pick the first match.
    for prof in profiles:
        if prof.get("request_path") == workflow_id:
            return str(prof.get("_path") or "") or None
    return None


def _summarize_phase_event_payload(payload: dict[str, Any]) -> str:
    """One-line operator-facing summary of a phase event's payload."""
    keys = [
        ("generation_id", "gen"),
        ("status", "status"),
        ("converged", "converged"),
        ("blocked", "blocked"),
        ("blocked_reason", "blocked_reason"),
        ("n_sources", "sources"),
        ("total_dirty_nodes", "dirty_nodes"),
        ("nodes_added", "nodes_added"),
        ("edges_added", "edges_added"),
        ("n_views", "views"),
        ("n_lenses", "lenses"),
        ("n_checks", "checks"),
        ("n_errors", "errors"),
        ("n_failed", "failed"),
    ]
    parts: list[str] = []
    for src_key, label in keys:
        if src_key in payload:
            val = payload[src_key]
            if isinstance(val, dict):
                val = val.get("reason", "<dict>")
            parts.append(f"{label}={val}")
    return " ".join(parts) if parts else "<empty>"


# `_count_by_origin` moved to `cli/catalog.py` by `extract-cli-catalog`.

# ---------- top-level dispatch ----------

class _VersionAction(argparse.Action):
    """Print the installed okg distribution version and exit (issue #2275).

    The version comes only from the installed package metadata. When that
    metadata is missing (a source tree run without an install), the flag
    refuses with exit 1 instead of guessing a number.
    """

    def __init__(self, option_strings, dest=argparse.SUPPRESS, **kwargs):
        super().__init__(
            option_strings,
            dest=dest,
            default=argparse.SUPPRESS,
            nargs=0,
            help="Print the installed okg package version and exit",
        )

    def __call__(self, parser, namespace, values, option_string=None):
        import importlib.metadata

        try:
            version = importlib.metadata.version("okg")
        except importlib.metadata.PackageNotFoundError:
            parser.exit(
                1,
                "okg: version unknown: no installed package metadata for "
                "'okg'. Install okg (for example `uv sync` or `pip install "
                "okg`) to report a version.\n",
            )
        sys.stdout.write(f"okg {version}\n")
        parser.exit(0)


class _CommaListAction(argparse.Action):
    """Accumulate a repeated flag into one comma-separated string.

    ``--include a,b --include c`` stores ``"a,b,c"`` (first-seen order, exact
    duplicates dropped) so every consumer that splits on commas sees all the
    values. The default ``store`` action kept only the last repeat (#4016).
    Empty and whitespace-only items are skipped, so ``--include a,`` stores
    ``"a"``. When nothing is left the value is ``""``, which consumers read
    as no filter, exactly as a lone ``--include ""`` did before.
    """

    def __call__(self, parser, namespace, values, option_string=None):
        items: list[str] = []
        for chunk in (getattr(namespace, self.dest, None), values):
            if chunk is None:
                continue
            for item in chunk.split(","):
                if item.strip() and item not in items:
                    items.append(item)
        setattr(namespace, self.dest, ",".join(items))


def _build_parser(argv: list[str] | None = None) -> argparse.ArgumentParser:
    # --json is accepted on both the top-level parser AND every
    # subparser. On subparsers we use `default=argparse.SUPPRESS`
    # so not passing --json at the subparser level doesn't
    # clobber the top-level value (M5b fix: previously only the
    # trailing `--json` worked because the subparser's False
    # default overrode the top-level True).
    def _add_json_flag(
        p: argparse.ArgumentParser, *, is_top: bool = False,
    ) -> None:
        p.add_argument(
            "--json", action="store_true",
            default=False if is_top else argparse.SUPPRESS,
            help="Machine-readable JSON output "
                 "(default under OKG_AGENT=1)",
        )

    def _add_owner_flags(p: argparse.ArgumentParser) -> None:
        """Add diagnostic owner metadata for publisher/catalog locks.

        The old publisher lease table is retired, but these flags still
        let status/error payloads report the caller-selected identity.
        """
        p.add_argument(
            "--owner-id",
            help="lease holder identity (default: "
                 "<host>:<pid>:<uuid4>)",
        )
        p.add_argument(
            "--owner-meta",
            help="JSON object with free-form holder context "
                 "(runtime, session_id, command, …)",
            default="{}",
        )

    def _add_profile_flags(p: argparse.ArgumentParser) -> None:
        # Decision 310: `--profile` -> `--capture`, `--profile-collector`
        # -> `--capture-collector`; the old spellings warn for one release.
        add_renamed_argument(
            p, "--capture", "--profile",
            dest="profile",
            nargs="?",
            const="deep",
            choices=["deep", "off"],
            help=(
                "Override deployment performance-capture policy for this "
                "invocation. Use --capture or --capture=deep to write a "
                "deep-capture bundle; --capture=off suppresses durable capture."
            ),
        )
        add_renamed_argument(
            p, "--capture-collector", "--profile-collector",
            dest="profile_collector",
            action="append",
            choices=[
                "rootless-speedscope",
                "python-flamegraph",
                "postgres-flamegraph",
                "explain-samples",
            ],
            default=[],
            help=(
                "Enable an optional capture collector for this invocation. "
                "python-flamegraph is a deprecated alias for rootless-speedscope."
            ),
        )

    parser = argparse.ArgumentParser(
        prog="okg",
        description=(
            "OKG substrate CLI\n\n"
            f"New here? Start with `okg {entry_roles.newcomer_door()}` — "
            "it is the one documented way to bring a graph up."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=True,
    )
    _add_json_flag(parser, is_top=True)
    parser.add_argument("--version", action=_VersionAction)
    install_actionable_errors(parser, "okg <command> --help")
    sub = parser.add_subparsers(dest="command")

    # migrate
    p_mig = sub.add_parser("migrate", help="Apply the v1 schema idempotently")
    _add_json_flag(p_mig)
    add_dsn_arg(p_mig, help="Postgres DSN; defaults to $OKG_DSN")
    p_mig.add_argument(
        "--deployment",
        help=(
            "Optional deployment name. When supplied, after "
            "the substrate's views.sql is loaded, also load "
            "deployments/<name>/views.sql if it exists. Lets "
            "deployments register their own okg.v_* views. "
            "Per add-deployment-sql-views."
        ),
    )
    p_mig.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Required. `okg migrate` replays schema DDL and reinstalls "
            "views inside a write transaction; without --apply it refuses "
            "before opening a connection, in every mode"
        ),
    )
    p_mig.add_argument(
        "--views-only",
        action="store_true",
        help=(
            "Refresh only okg.v_* read-side views. Does not replay schema DDL "
            "or update the schema snapshot ledger."
        ),
    )

    # doctor
    p_doc = sub.add_parser("doctor", help="Verify extensions, roles, DSN")
    _add_json_flag(p_doc)
    install_actionable_errors(p_doc, "okg doctor --deployment <name> --json")
    add_dsn_arg(p_doc)
    p_doc.add_argument(
        "--check",
        default=None,
        help=(
            "extraction: import-check the extraction library + "
            "report spaCy/GLiNER models. "
            "extraction-wrap-invariant: lint the extraction tree. "
            "acceptance-criteria: warn on OpenSpec criteria without "
            "test evidence links. "
            "extractors: pre-instantiate every class in the "
            "deployment's extractors.yaml. "
            "integrity: audit bitemporal generation contracts. "
            "admission: audit admission-envelope conformance. "
            "agent-skills: compare installed repo-local agent skills "
            "against the bundled canon. "
            "mcp-surface-freshness: probe registered OKG MCP servers "
            "for current code/instructions. "
            "external-live-authority: validate external-live tool "
            "authority profile coverage. "
            "nomos: validate Nomos policy, identity schema, and "
            "IAM drift. "
            "source-auth: run optional source auth/cache preflight "
            "hooks from the deployment's source_registry.yaml. "
            "database: inspect database storage/runtime health. "
            "grant-registry-drift: compare the live app_ro / okg_mcp "
            "privilege surface against okg.grant_registry. "
            "pending-grants: surface grants the Chronos catch-up "
            "queued for retry instead of applying. "
            "sources: pre-instantiate every adapter in the "
            "deployment's source_registry.yaml. Deployment-specific "
            "checks are loaded from deployment.yaml."
        ),
    )
    p_doc.add_argument(
        "--warm", action="store_true",
        help="With --check extraction: eagerly preload the configured "
             "spaCy/GLiNER models for the deployment.",
    )
    p_doc.add_argument(
        "--generation", type=int, default=None,
        help="With --check integrity: generation id to audit "
             "(default: latest published for the deployment/DSN).",
    )
    p_doc.add_argument(
        "--deployment", default=None,
        help="Deployment name (used by --check extraction --warm, "
             "--check extractors, --check sources, and "
             "deployment-specific checks / source-auth / integrity)",
    )
    p_doc.add_argument(
        "--stale-after-days", type=int, default=None,
        help=(
            "With --check lens-promotions: number of days a "
            "committed_overlay row may persist before being flagged "
            "(default 30)."
        ),
    )
    p_doc.add_argument(
        "--watched-db-ports", default=None,
        help=(
            "With --check listener-exposure: comma-separated DB host "
            "ports to flag if non-loopback regardless of owning "
            "process (the graph store; the deployment's DSN port is "
            "auto-added). Also OKG_WATCHED_DB_PORTS."
        ),
    )
    p_doc.add_argument(
        "--quarantine-empty-grace-hours", type=int, default=None,
        help=(
            "With --check branches: hours a quarantine branch may "
            "exist without any routed admission envelope before "
            "being flagged as `quarantine_branch_empty` (default 24)."
        ),
    )
    p_doc.add_argument(
        "--deep", action="store_true",
        help=(
            "With --check disk: run pgstattuple_approx for ground-truth "
            "dead-tuple percentages (scans tables; slow)."
        ),
    )

    # db
    p_db = sub.add_parser(
        "db",
        help="Inspect substrate database inventory, status, and migration drift",
    )
    _add_json_flag(p_db)
    p_db_sub = p_db.add_subparsers(dest="db_subcmd", required=True)

    p_db_list = p_db_sub.add_parser("list", help="List deployment databases")
    p_db_list.add_argument("--cluster", action="store_true")
    p_db_list.add_argument("--cluster-dsn")
    _add_json_flag(p_db_list)

    p_db_status = p_db_sub.add_parser("status", help="Inspect one database")
    add_deployment_arg(p_db_status)
    add_dsn_arg(p_db_status)
    p_db_status.add_argument("--brief", action="store_true")
    p_db_status.add_argument("--top", type=int, default=20)
    _add_json_flag(p_db_status)

    p_db_gc = p_db_sub.add_parser(
        "gc",
        help="Sweep ephemeral test databases (dry-run by default)",
    )
    p_db_gc.add_argument(
        "--dry-run",
        action="store_true",
        help="Report candidates and sizes without dropping (the default)",
    )
    p_db_gc.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Drop ONLY the allowlist-prefixed test databases "
            "(okg_test_*, okg_upgrade_verify_*, okg_golden*); "
            "everything else always survives"
        ),
    )
    p_db_gc.add_argument("--cluster-dsn", required=True)
    _add_json_flag(p_db_gc)

    p_db_clone = p_db_sub.add_parser(
        "clone",
        help="Safely clone a development database with dump/restore",
    )
    p_db_clone.add_argument(
        "--source-dsn",
        help="Source DSN; defaults to OKG_CLONE_SOURCE_DSN",
    )
    p_db_clone.add_argument(
        "--target-dsn",
        help="Target DSN; defaults to OKG_CLONE_TARGET_DSN",
    )
    p_db_clone.add_argument(
        "--admin-dsn",
        help=(
            "Optional target maintenance DSN; defaults to "
            "OKG_CLONE_ADMIN_DSN, then target's postgres DB"
        ),
    )
    p_db_clone.add_argument(
        "--restore-dump",
        help="Restore this validated custom dump instead of creating a new dump",
    )
    p_db_clone.add_argument(
        "--dump-file",
        help="Path for a newly created dump; retained after success",
    )
    p_db_clone.add_argument(
        "--drain-timeout-seconds",
        type=float,
        default=30.0,
    )
    p_db_clone.add_argument("--apply", action="store_true")
    _add_json_flag(p_db_clone)

    p_dbos_gc = p_db_sub.add_parser(
        "dbos-gc",
        help=(
            "Dry-run or cancel stale DBOS ENQUEUED workflows; PENDING is "
            "inspectable but cannot be cancelled without execution proof"
        ),
    )
    add_deployment_arg(p_dbos_gc)
    add_dsn_arg(p_dbos_gc)
    p_dbos_gc.add_argument(
        "--stale-after-seconds",
        type=int,
        default=86400,
        help="Only touch rows whose updated_at/created_at is older than this",
    )
    p_dbos_gc.add_argument(
        "--status",
        action="append",
        choices=["PENDING", "ENQUEUED"],
        default=None,
    )
    p_dbos_gc.add_argument("--queue-name")
    p_dbos_gc.add_argument("--workflow-name")
    p_dbos_gc.add_argument("--sample-limit", type=int, default=20)
    p_dbos_gc.add_argument("--confirm", action="store_true")
    p_dbos_gc.add_argument("--apply", action="store_true")
    _add_json_flag(p_dbos_gc)

    p_prune_blobs = p_db_sub.add_parser(
        "prune-source-blobs",
        help=(
            "Dry-run or delete orphaned source cursor/record-set blob rows "
            "(unreferenced by any watermark rev)"
        ),
    )
    add_deployment_arg(p_prune_blobs)
    add_dsn_arg(p_prune_blobs)
    p_prune_blobs.add_argument(
        "--grace-seconds",
        type=int,
        default=86400,
        help=(
            "Never delete a blob written within this window. Keep high enough "
            "to cover a run's optimistic-write/restore window; run --confirm "
            "during an ingest lull (default 86400 = 24h)"
        ),
    )
    p_prune_blobs.add_argument("--confirm", action="store_true")
    p_prune_blobs.add_argument("--apply", action="store_true")
    _add_json_flag(p_prune_blobs)

    p_prune_kind_blobs = p_db_sub.add_parser(
        "prune-blobs",
        help=(
            "Dry-run or delete okg.blobs rows of one kind that no retained "
            "generation references and that are older than the grace window; "
            "--apply refuses when no graph is registered (blob_prune_no_graph)"
        ),
    )
    add_deployment_arg(p_prune_kind_blobs)
    add_dsn_arg(p_prune_kind_blobs)
    p_prune_kind_blobs.add_argument(
        "--kind",
        required=True,
        help=(
            "Blob kind to clean up (only `image` exists); each kind declares "
            "how its references are found, and an unknown kind is refused"
        ),
    )
    p_prune_kind_blobs.add_argument(
        "--grace-seconds",
        type=int,
        default=86400,
        help=(
            "Never delete a row used within this window, referenced or not "
            "(default 86400 = 24h; at least 3600, a smaller value is refused "
            "with blob_prune_grace_too_short)"
        ),
    )
    p_prune_kind_blobs.add_argument("--confirm", action="store_true")
    p_prune_kind_blobs.add_argument("--apply", action="store_true")
    _add_json_flag(p_prune_kind_blobs)

    register_db_cleanup_parsers(
        p_db_sub,
        add_json_flag=_add_json_flag,
    )

    p_db_drop = p_db_sub.add_parser("drop", help="Drop one database safely")
    p_db_drop.add_argument("--name", required=True)
    p_db_drop.add_argument("--cluster-dsn", required=True)
    p_db_drop.add_argument("--confirm", action="store_true")
    p_db_drop.add_argument("--override-deployment")
    p_db_drop.add_argument("--apply", action="store_true")
    _add_json_flag(p_db_drop)

    p_db_mig = p_db_sub.add_parser(
        "migrations",
        help="Report schema migration ledger status",
    )
    add_deployment_arg(p_db_mig)
    add_dsn_arg(p_db_mig)
    _add_json_flag(p_db_mig)

    p_db_migdown = p_db_sub.add_parser(
        "migrate-down",
        help=(
            "Reverse one applied migration by replaying its .down.sql "
            "inverse; refuses loudly when no inverse exists"
        ),
    )
    p_db_migdown.add_argument(
        "migration_id",
        help="Migration id to reverse (see `okg db migrations`)",
    )
    add_deployment_arg(p_db_migdown)
    add_dsn_arg(p_db_migdown)
    p_db_migdown.add_argument("--confirm", action="store_true")
    p_db_migdown.add_argument("--apply", action="store_true")
    _add_json_flag(p_db_migdown)

    p_db_doc = p_db_sub.add_parser("doctor", help="Run database health check")
    add_deployment_arg(p_db_doc)
    add_dsn_arg(p_db_doc)
    _add_json_flag(p_db_doc)

    from okg.substrate.cli.role_passwords import add_role_passwords_parser

    add_role_passwords_parser(
        p_db_sub,
        add_dsn_arg=add_dsn_arg,
        add_deployment_arg=add_deployment_arg,
        add_json_flag=_add_json_flag,
    )

    p_db_reclaim = p_db_sub.add_parser(
        "reclaim-index-space",
        help=(
            "Rebuild okg-schema indexes that outgrew their table "
            "(index_larger_than_table) with REINDEX CONCURRENTLY; dry run "
            "unless --apply"
        ),
    )
    add_deployment_arg(p_db_reclaim, required=True)
    add_dsn_arg(p_db_reclaim)
    p_db_reclaim.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Rebuild under the deployment's publisher lock and verify the "
            "authority fingerprint (default: list what would run)"
        ),
    )
    p_db_reclaim.add_argument(
        "--min-index-bytes",
        type=int,
        default=None,
        help="Index-size floor (default: the doctor disk check's floor)",
    )
    p_db_reclaim.add_argument(
        "--publish-lock-wait-seconds",
        type=float,
        default=5.0,
        help="How long to wait for the publisher lock before refusing",
    )
    p_db_reclaim.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=30.0,
        help="lock_timeout for each REINDEX TABLE CONCURRENTLY",
    )
    _add_json_flag(p_db_reclaim)

    p_db_embed_backfill = p_db_sub.add_parser(
        "embed-backfill",
        help=(
            "Embed the distinct documents of one registered vector set that "
            "have no vector (and link nodes to their documents), at the "
            "latest published generation, under a spend cap; writes no node "
            "rows; re-run to resume; --dry-run estimates without a call"
        ),
    )
    add_deployment_arg(p_db_embed_backfill, required=True)
    add_dsn_arg(p_db_embed_backfill)
    p_db_embed_backfill.add_argument(
        "--set",
        default=None,
        help=(
            "Vector set name from deployment.yaml (default: the search set; "
            "with --dry-run: every configured set)"
        ),
    )
    p_db_embed_backfill.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Count missing documents, tokens and dollars per set and subtype; "
            "no paid call, no write"
        ),
    )
    p_db_embed_backfill.add_argument(
        "--max-usd",
        type=float,
        default=None,
        help=(
            "Stop before the next request once this run plus earlier "
            "backfill runs of the set (okg.llm_calls) reach this many USD"
        ),
    )
    p_db_embed_backfill.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Embed at most this many documents (a pilot)",
    )
    p_db_embed_backfill.add_argument(
        "--window-rows",
        type=int,
        default=512,
        help="Node rows read and linked per window",
    )
    p_db_embed_backfill.add_argument(
        "--max-window-chars",
        type=int,
        default=1_000_000,
        help=(
            "Characters per embedding request batch; bounds how far one "
            "batch can pass the cap"
        ),
    )
    _add_json_flag(p_db_embed_backfill)

    p_db_embed_show = p_db_sub.add_parser(
        "embed-show",
        help=(
            "Print the exact document a node renders to under a registered "
            "vector set, its doc_hash, and whether a vector exists; reads only"
        ),
    )
    add_deployment_arg(p_db_embed_show, required=True)
    add_dsn_arg(p_db_embed_show)
    p_db_embed_show.add_argument(
        "--set",
        default=None,
        help="Vector set name from deployment.yaml (default: the search set)",
    )
    p_db_embed_show.add_argument(
        "--node", required=True, help="Node id to render",
    )
    _add_json_flag(p_db_embed_show)

    p_db_embed_index = p_db_sub.add_parser(
        "embed-index",
        help=(
            "Build a registered vector set's HNSW index under its current "
            "settings (CREATE INDEX CONCURRENTLY, beside the old one), then "
            "drop the set's other indexes; dry run unless --apply"
        ),
    )
    add_deployment_arg(p_db_embed_index, required=True)
    add_dsn_arg(p_db_embed_index)
    p_db_embed_index.add_argument(
        "--set",
        default=None,
        help="Vector set name from deployment.yaml (default: the search set)",
    )
    p_db_embed_index.add_argument(
        "--apply",
        action="store_true",
        help="Build and drop (default: report the actions)",
    )
    _add_json_flag(p_db_embed_index)

    p_db_semantic_index = p_db_sub.add_parser(
        "semantic-index",
        help=(
            "Report or build (CREATE INDEX CONCURRENTLY) the HNSW index "
            "semantic search ranks through; dry run unless --apply"
        ),
    )
    add_deployment_arg(p_db_semantic_index, required=True)
    add_dsn_arg(p_db_semantic_index)
    p_db_semantic_index.add_argument(
        "--apply",
        action="store_true",
        help="Build the index (default: report its state)",
    )
    p_db_semantic_index.add_argument(
        "--maintenance-work-mem",
        default="2GB",
        help="maintenance_work_mem for the build session",
    )
    _add_json_flag(p_db_semantic_index)

    p_db_current_dlq = p_db_sub.add_parser(
        "current-runtime-dlq",
        help="Report current DBOS failures after explicit supersession",
    )
    add_deployment_arg(p_db_current_dlq, required=True)
    _add_json_flag(p_db_current_dlq)

    p_db_ack_dlq = p_db_sub.add_parser(
        "acknowledge-runtime-dlq",
        help="Plan or apply exact, witnessed runtime DLQ acknowledgements",
    )
    add_deployment_arg(p_db_ack_dlq, required=True)
    p_db_ack_dlq.add_argument(
        "--request",
        required=True,
        help="Path to the acknowledgement request JSON",
    )
    p_db_ack_dlq.add_argument(
        "--confirm-plan-id",
        help="Exact sha256 plan_id returned by a fresh dry run",
    )
    p_db_ack_dlq.add_argument("--apply", action="store_true")
    _add_json_flag(p_db_ack_dlq)

    # okg#3112 step 2: the declared-attribute expression indexes.
    p_db_attr_index = p_db_sub.add_parser(
        "attr-index",
        help=(
            "Build and maintain the per-attribute expression indexes "
            "on the graph tables"
        ),
    )
    p_db_attr_index_sub = p_db_attr_index.add_subparsers(
        dest="attr_index_subcmd", required=True
    )
    p_db_attr_reconcile = p_db_attr_index_sub.add_parser(
        "reconcile",
        help=(
            "Build missing declared-attribute indexes and drop retired ones "
            "with CREATE/DROP INDEX CONCURRENTLY; dry run unless --apply"
        ),
    )
    add_deployment_arg(p_db_attr_reconcile)
    add_dsn_arg(p_db_attr_reconcile)
    p_db_attr_reconcile.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Run the reconciler now, and let the scheduled maintenance "
            "workflow keep the indexes up to date from then on"
        ),
    )
    p_db_attr_reconcile.add_argument(
        "--retry-failed",
        action="store_true",
        help="Also retry indexes stopped after five consecutive failures",
    )
    p_db_attr_reconcile.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=None,
        help=(
            "lock_timeout of each CREATE INDEX CONCURRENTLY (default "
            "OKG_DECLARED_ATTR_INDEX_LOCK_TIMEOUT_SECONDS or 600)"
        ),
    )
    _add_json_flag(p_db_attr_reconcile)

    p_db_cold = p_db_sub.add_parser(
        "cold-history",
        help=(
            "Inspect or concurrently rotate Chronos visibility indexes to a "
            "runtime-derived live-front floor"
        ),
    )
    add_deployment_arg(p_db_cold)
    add_dsn_arg(p_db_cold)
    p_db_cold.add_argument(
        "--headroom-generations",
        type=int,
        default=100,
        help="Recent default-lineage generations retained in hot indexes (default 100)",
    )
    p_db_cold.add_argument(
        "--logical-table",
        action="append",
        choices=["okg.graph_nodes", "okg.graph_edges"],
        default=None,
        help="Limit rotation to one logical graph table (repeatable)",
    )
    p_db_cold.add_argument(
        "--suffix",
        action="append",
        default=None,
        help="Limit rotation to one declared visibility-index suffix (repeatable)",
    )
    p_db_cold.add_argument(
        "--activate",
        action="store_true",
        help="After a valid concurrent build, make it authoritative and drop the full index concurrently",
    )
    p_db_cold.add_argument("--confirm", action="store_true")
    p_db_cold.add_argument("--apply", action="store_true")
    _add_json_flag(p_db_cold)

    # run
    p_run = sub.add_parser("run", help="Run a publish cycle")
    _add_json_flag(p_run)
    _add_owner_flags(p_run)
    _add_profile_flags(p_run)
    p_run.add_argument("--once", action="store_true",
                       help="One synchronous publish cycle (dev mode)")
    p_run.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "Print the full cycle report (stage timings, gates, views, "
            "receipts) after the short summary. Warnings always print."
        ),
    )
    add_deployment_arg(p_run, help="Deployment name")
    add_dsn_arg(p_run)
    p_run.add_argument(
        "--apply",
        action="store_true",
        help="Required to authorize the publish cycle",
    )
    # Per `migrate-substrate-runtime-to-dbos` Phase 1 §1.2.2: force
    # the in-process publisher path even when runtime.enabled: true.
    # Used by tests and by operators who need to run a cycle outside
    # the DBOS partition queue (e.g. against a deployment whose
    # worker is down).
    p_run.add_argument(
        "--inline",
        action="store_true",
        help=(
            "Run Publisher.cycle() in-process rather than enqueuing "
            "via DBOSClient. "
            "Default behavior is in-process when runtime.enabled is "
            "false; DBOS-enqueue when true."
        ),
    )
    p_run.add_argument(
        "--reconcile-count-stamp",
        action="store_true",
        help=(
            "Full-scan the graph and stamp truthful counts into a new generation; "
            "requires --inline and never rewrites prior checkpoints."
        ),
    )
    # P6 (specify-publish-commit-protocol): the sanctioned operator force
    # lever. On a zero-source-work, unchanged-catalog deployment the
    # pre-enrichment noop-reuse probe skips enrichers/gates/checkpoint;
    # --force-enrichers bypasses that skip for exactly this one cycle so an
    # operator can drive a recompute (e.g. to run a held enricher retry or
    # re-derive after a deployment-local ledger change) without fabricating
    # a fake source delta. One-shot: the next unforced cycle resumes normal
    # skip behavior with intact watermarks and reuse pins.
    p_run.add_argument(
        "--force-enrichers",
        action="store_true",
        help=(
            "Bypass the pre-enrichment noop-reuse skip for exactly this "
            "cycle: run every enricher, run gates, and cut a checkpoint "
            "even with zero graph delta. The sanctioned operator recompute "
            "lever; recorded as an 'operator_forced' reuse invalidation."
        ),
    )
    # Per `add-semantic-llm-judge-invariants` (2026-04-29): after
    # the publish cycle commits, run every kind='llm_judge'
    # invariant against the just-published generation, persist
    # verdicts + LLM-call cost roll-up into
    # graph_generations.invariant_summary, and surface counts in
    # the CLI summary. Default OFF — judges are expensive and
    # never block publish.
    p_run.add_argument(
        "--include-llm-invariants", action="store_true",
        help=(
            "After the publish cycle, run every kind='llm_judge' "
            "invariant installed at the cycle's catalog version "
            "and persist verdicts to "
            "graph_generations.invariant_summary."
        ),
    )
    p_run.add_argument(
        "--llm-invariant",
        help=(
            "Restrict --include-llm-invariants to a single "
            "invariant name (otherwise runs all kind='llm_judge' "
            "invariants for the deployment)."
        ),
    )
    # AKMON Phase A.3: target a specific branch. Default is the
    # deployment's `default` branch. Non-default branches must
    # be created via `okg branch create` before they can be
    # published to.
    p_run.add_argument(
        "--branch",
        default="default",
        help=(
            "Target branch for this publish cycle (default: "
            "'default'). Non-default branches must exist; create "
            "them with `okg branch create <name> --role "
            "(experiment|quarantine)`."
        ),
    )

    # enricher
    p_enricher = sub.add_parser(
        "enricher",
        help="Operate on deployment-registered enrichers",
    )
    _add_json_flag(p_enricher)
    p_enricher_sub = p_enricher.add_subparsers(dest="subcmd", required=True)
    p_enricher_backfill = p_enricher_sub.add_parser(
        "backfill",
        help="Backfill one registered enricher in bounded chunks",
    )
    _add_json_flag(p_enricher_backfill)
    p_enricher_backfill.add_argument("deployment", help="Deployment name")
    p_enricher_backfill.add_argument(
        "enricher_name",
        help="Registered enricher name",
    )
    p_enricher_backfill.add_argument(
        "--limit",
        type=int,
        required=True,
        help="Maximum input nodes to process in this run",
    )
    p_enricher_backfill.add_argument(
        "--chunk-size",
        type=int,
        required=True,
        help="Maximum input nodes per transaction",
    )
    add_dsn_arg(p_enricher_backfill)
    p_enricher_backfill.add_argument(
        "--apply",
        action="store_true",
        help="Required to authorize backfill writes",
    )

    # ingest
    p_ing = sub.add_parser(
        "ingest",
        help="Drive every source in a deployment's source_registry "
             "in topological order; publish between dependency groups",
    )
    _add_json_flag(p_ing)
    install_actionable_errors(
        p_ing, "okg ingest --deployment <name> --json"
    )
    _add_owner_flags(p_ing)
    _add_profile_flags(p_ing)
    add_deployment_arg(p_ing, required=True, help="Deployment name")
    add_dsn_arg(p_ing)
    p_ing.add_argument(
        "--include",
        action=_CommaListAction,
        help="comma-separated source names or ownership IDs to include "
             "(default: all; predecessors are included); "
             "repeat the flag to add more",
    )
    p_ing.add_argument(
        "--exclude",
        action=_CommaListAction,
        help="comma-separated source names to exclude; "
             "repeat the flag to add more",
    )
    p_ing.add_argument(
        "--no-predecessors",
        action="store_true",
        help="do not expand --include through predecessor_source closure",
    )
    # Per `migrate-substrate-runtime-to-dbos` Phase 2 §2.2: force the
    # in-process ingest path even when runtime.enabled: true. Used by
    # tests and by operators who need to run an ingest outside the
    # DBOS queue (e.g. against a deployment whose worker is down).
    p_ing.add_argument(
        "--inline",
        action="store_true",
        help=(
            "Run the source-sync in-process rather than enqueuing via "
            "DBOSClient. "
            "Default behavior is in-process when runtime.enabled is "
            "false; DBOS-enqueue when true."
        ),
    )
    from okg.substrate.source_sync import (
        SOURCE_SYNC_DEPENDENT_FANOUT,
        SOURCE_SYNC_RUN_MODES,
        SOURCE_SYNC_TRIGGER_KINDS,
    )
    p_ing.add_argument(
        "--mode",
        choices=sorted(SOURCE_SYNC_RUN_MODES),
        default=None,
        help="SourceRunner mode for direct or enqueued ingest. Omitted, "
             "every source reads its whole scope: scope_complete, or for a "
             "source that declares another profile that profile's own "
             "whole-scope mode (release_new, reconcile or append). Given, "
             "every selected source runs in it as typed, inline or queued.",
    )
    p_ing.add_argument(
        "--include-dependents",
        action="store_true",
        help="also include dependent sources in the source plan",
    )
    p_ing.add_argument(
        "--dependent-fanout",
        choices=sorted(SOURCE_SYNC_DEPENDENT_FANOUT),
        help="dependent source fanout for direct/enqueued ingest "
             "(default: none; --include-dependents implies closure)",
    )
    p_ing.add_argument(
        "--enqueue",
        action="store_true",
        help=(
            "enqueue source-sync through DBOS; fails when runtime.enabled is false"
        ),
    )
    p_ing.add_argument(
        "--no-publish", action="store_true",
        help="skip the inter-group publish cycles "
             "(operator runs them separately)",
    )
    p_ing.add_argument(
        "--sync-wait-seconds", type=float, default=None,
        help=(
            "how long an ingest waits for another source sync on the "
            "deployment (in-process), or for a command-line run to release "
            "it before enqueueing (runtime), before failing with "
            "source_sync_already_running "
            "(default: OKG_SOURCE_SYNC_RUN_WAIT_SECONDS, else 600)"
        ),
    )
    p_ing.add_argument(
        "--force-enrichers",
        action="store_true",
        help=(
            "Force each ingest-triggered publish to bypass no-source-work "
            "and publisher noop reuse for this invocation, run enrichers "
            "and gates, and cut a checkpoint."
        ),
    )
    p_ing.add_argument(
        "--reset-cursor",
        action="store_true",
        help=(
            "repair mode: ignore persisted source watermarks/progress for "
            "this run so selected sources re-emit source truth. Requires "
            "--inline when the deployment runtime is enabled."
        ),
    )
    p_ing.add_argument(
        "--allow-full-retraction",
        action="append",
        default=[],
        metavar="SOURCE",
        help=(
            "for this run only, let SOURCE retract all of its live rows (or "
            "empty a repository) while emitting nothing; the run records who "
            "allowed it. Repeat for several sources. Requires --inline when "
            "the deployment runtime is enabled."
        ),
    )
    p_ing.add_argument(
        "--progress", action="store_true",
        help="stream per-source logging to stderr in real time "
             "(enables INFO logs from sources / SourceRunner / "
             "Publisher)",
    )

    # sync
    p_sync = sub.add_parser(
        "sync",
        help="Inspect source-sync policy and explain source events",
    )
    _add_json_flag(p_sync)
    p_sync_sub = p_sync.add_subparsers(dest="sync_subcmd", required=True)
    p_sync_explain = p_sync_sub.add_parser(
        "explain",
        help="Explain static source-sync policy or a path event",
    )
    _add_json_flag(p_sync_explain)
    add_deployment_arg(p_sync_explain, required=True)
    p_sync_explain.add_argument(
        "--path",
        help="optional source path to match against sync.watch policy",
    )
    p_sync_explain.add_argument(
        "--trigger",
        default="fs_event",
        choices=sorted(SOURCE_SYNC_TRIGGER_KINDS),
        help="trigger kind for --path explanation (default: fs_event)",
    )
    p_sync_explain.add_argument(
        "--source",
        help="restrict explanation to one registry source",
    )
    p_sync_frontier = p_sync_sub.add_parser(
        "frontier",
        help="Show source frontier and read-visibility state",
    )
    _add_json_flag(p_sync_frontier)
    add_deployment_arg(p_sync_frontier, required=True)
    add_dsn_arg(p_sync_frontier)
    p_sync_frontier.add_argument(
        "--source",
        help="restrict diagnostics to one full or registry source name",
    )
    p_sync_frontier.add_argument(
        "--limit",
        type=int,
        default=100,
        help="maximum frontier rows to return",
    )
    p_sync_benchmark = p_sync_sub.add_parser(
        "benchmark",
        help="Validate a source-sync latency and consistency benchmark artifact",
    )
    _add_json_flag(p_sync_benchmark)
    add_deployment_arg(p_sync_benchmark, required=True)
    p_sync_benchmark.add_argument("--artifact", required=True)
    p_sync_benchmark.add_argument(
        "--max-event-to-generation-ms",
        type=float,
        help="optional event-to-generation latency budget",
    )
    p_sync_benchmark.add_argument(
        "--max-query-visibility-ms",
        type=float,
        help="optional published-generation query visibility budget",
    )
    p_sync_microbench = p_sync_sub.add_parser(
        "microbench",
        help="Run or inspect source-sync microbenchmarks",
    )
    _add_json_flag(p_sync_microbench)
    add_deployment_arg(p_sync_microbench)
    add_dsn_arg(p_sync_microbench)
    p_sync_microbench.add_argument(
        "--scenario",
        choices=["no-change", "single-delta"],
        default="no-change",
        help="Benchmark scenario for the default microbench runner",
    )
    p_sync_microbench.add_argument(
        "--sources",
        default="all",
        help="Comma-separated source names/ownership ids, or all",
    )
    p_sync_microbench.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of samples per source/pipeline mode",
    )
    p_sync_microbench.add_argument(
        "--pipeline-mode",
        action="append",
        choices=["both", "source-only", "full-pipeline"],
        default=None,
        help="Pipeline mode to benchmark; repeat for multiple modes",
    )
    p_sync_microbench.add_argument(
        "--run-mode",
        choices=sorted(SOURCE_SYNC_RUN_MODES),
        default="scope_incomplete",
        help="SourceRunner mode used by benchmark samples",
    )
    p_sync_microbench.add_argument(
        "--include-predecessors",
        action="store_true",
        help="Expand selected sources through predecessor_source closure",
    )
    p_sync_microbench.add_argument(
        "--include-dependents",
        action="store_true",
        help="Expand selected sources through dependent source closure",
    )
    p_sync_microbench.add_argument(
        "--dependent-fanout",
        choices=sorted(SOURCE_SYNC_DEPENDENT_FANOUT),
        help="Dependent fanout used when --include-dependents is set",
    )
    p_sync_microbench.add_argument(
        "--max-source-workers",
        type=int,
        default=1,
        help="Maximum per-layer source workers inside each sample",
    )
    p_sync_microbench.add_argument(
        "--scope-key",
        help=(
            "Explicit scope_key for single-delta benchmarks; if omitted, "
            "a path scope is sampled from sync.watch roots when possible"
        ),
    )
    p_sync_microbench.add_argument(
        "--unscoped",
        action="store_true",
        help=(
            "Force an unscoped source event instead of sampling a path scope; "
            "useful for full-scope backstop benchmarks"
        ),
    )
    p_sync_microbench.add_argument(
        "--expected-delta-json",
        help=(
            "Explicit expected graph delta for observability validation, "
            "for example '{\"nodes_added\":1,\"edges_added\":2}'"
        ),
    )
    p_sync_microbench.add_argument(
        "--output",
        type=Path,
        help="Write the benchmark report JSON to this path",
    )
    p_sync_microbench.add_argument(
        "--allow-quarantined-full-pipeline",
        action="store_true",
        help=(
            "When --sources all includes full-pipeline mode, also run "
            "sources known to be expensive or risky instead of recording "
            "explicit skipped rows"
        ),
    )
    p_sync_microbench_sub = p_sync_microbench.add_subparsers(
        dest="microbench_subcmd",
        required=False,
    )
    p_sync_microbench_gate = p_sync_microbench_sub.add_parser(
        "gate",
        help="Run source-sync microbenchmarks and enforce a p95 latency budget",
    )
    _add_json_flag(p_sync_microbench_gate)
    add_deployment_arg(p_sync_microbench_gate)
    add_dsn_arg(p_sync_microbench_gate)
    p_sync_microbench_gate.add_argument(
        "--scenario",
        choices=["no-change", "single-delta"],
        default="no-change",
        help="Benchmark scenario for the microbench gate",
    )
    p_sync_microbench_gate.add_argument(
        "--sources",
        default="all",
        help="Comma-separated source names/ownership ids, or all",
    )
    p_sync_microbench_gate.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Number of samples per source/pipeline mode",
    )
    p_sync_microbench_gate.add_argument(
        "--pipeline-mode",
        action="append",
        choices=["both", "source-only", "full-pipeline"],
        default=None,
        help="Pipeline mode to benchmark; repeat for multiple modes",
    )
    p_sync_microbench_gate.add_argument(
        "--run-mode",
        choices=sorted(SOURCE_SYNC_RUN_MODES),
        default="scope_incomplete",
        help="SourceRunner mode used by benchmark samples",
    )
    p_sync_microbench_gate.add_argument(
        "--include-predecessors",
        action="store_true",
        help="Expand selected sources through predecessor_source closure",
    )
    p_sync_microbench_gate.add_argument(
        "--include-dependents",
        action="store_true",
        help="Expand selected sources through dependent source closure",
    )
    p_sync_microbench_gate.add_argument(
        "--dependent-fanout",
        choices=sorted(SOURCE_SYNC_DEPENDENT_FANOUT),
        help="Dependent fanout used when --include-dependents is set",
    )
    p_sync_microbench_gate.add_argument(
        "--max-source-workers",
        type=int,
        default=1,
        help="Maximum per-layer source workers inside each sample",
    )
    p_sync_microbench_gate.add_argument(
        "--scope-key",
        help=(
            "Explicit scope_key for single-delta benchmarks; if omitted, "
            "a path scope is sampled from sync.watch roots when possible"
        ),
    )
    p_sync_microbench_gate.add_argument(
        "--unscoped",
        action="store_true",
        help=(
            "Force an unscoped source event instead of sampling a path scope; "
            "useful for full-scope backstop benchmarks"
        ),
    )
    p_sync_microbench_gate.add_argument(
        "--expected-delta-json",
        help=(
            "Explicit expected graph delta for observability validation, "
            "for example '{\"nodes_added\":1,\"edges_added\":2}'"
        ),
    )
    p_sync_microbench_gate.add_argument(
        "--output",
        type=Path,
        help="Write the benchmark report JSON to this path",
    )
    p_sync_microbench_gate.add_argument(
        "--allow-quarantined-full-pipeline",
        action="store_true",
        help=(
            "When --sources all includes full-pipeline mode, also run "
            "sources known to be expensive or risky instead of recording "
            "explicit skipped rows"
        ),
    )
    p_sync_microbench_gate.add_argument(
        "--max-p95-ms",
        type=float,
        default=1000.0,
        help="Maximum allowed row p95 latency in milliseconds",
    )
    p_sync_microbench_gate.add_argument(
        "--idle-source-sync-wait-seconds",
        type=float,
        default=30.0,
        help=(
            "Maximum time to wait for the source-sync DBOS lane to be "
            "idle before measuring full-pipeline rows"
        ),
    )
    p_sync_microbench_inventory = p_sync_microbench_sub.add_parser(
        "inventory",
        help="Build the per-source microbenchmark inventory matrix",
    )
    _add_json_flag(p_sync_microbench_inventory)
    add_deployment_arg(p_sync_microbench_inventory, required=True)
    add_dsn_arg(p_sync_microbench_inventory)
    p_sync_microbench_inventory.add_argument(
        "--no-live",
        action="store_true",
        help="do not enrich the inventory from live source-progress state",
    )
    p_sync_microbench_inventory.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=15 * 60,
        help="Maximum producer heartbeat age used in expected producer rows",
    )
    p_sync_microbench_ledger = p_sync_microbench_sub.add_parser(
        "ledger",
        help="Build the per-source differential optimization work ledger",
    )
    _add_json_flag(p_sync_microbench_ledger)
    add_deployment_arg(p_sync_microbench_ledger, required=True)
    add_dsn_arg(p_sync_microbench_ledger)
    p_sync_microbench_ledger.add_argument(
        "--no-live",
        action="store_true",
        help="do not enrich the ledger from live source-progress state",
    )
    p_sync_microbench_ledger.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=15 * 60,
        help="Maximum producer heartbeat age used in expected producer rows",
    )
    p_sync_microbench_ledger.add_argument(
        "--latency-budget-ms",
        type=float,
        default=1000.0,
        help="Latency budget used to classify source rows",
    )
    p_sync_benchmark_capture = p_sync_sub.add_parser(
        "benchmark-capture",
        help=(
            "Build a source-sync benchmark artifact from runtime "
            "debug-workflow JSON"
        ),
    )
    _add_json_flag(p_sync_benchmark_capture)
    add_deployment_arg(p_sync_benchmark_capture, required=True)
    p_sync_benchmark_capture.add_argument(
        "--debug-workflow-json",
        required=True,
        help="path to `okg runtime debug-workflow --json` output",
    )
    p_sync_benchmark_capture.add_argument(
        "--output",
        help="optional output path for the benchmark JSON artifact",
    )
    p_sync_benchmark_capture.add_argument("--repo", default="")
    p_sync_benchmark_capture.add_argument("--scenario")
    p_sync_benchmark_capture.add_argument("--profile-run-id")
    p_sync_benchmark_capture.add_argument(
        "--expected-delta-json",
        help="JSON object of expected graph deltas, e.g. '{\"nodes_added\":1}'",
    )
    p_sync_benchmark_capture.add_argument(
        "--observed-delta-json",
        help="JSON object of observed graph deltas",
    )
    p_sync_benchmark_capture.add_argument(
        "--queue-json",
        help="JSON object with queue_depth and dlq_count",
    )
    p_sync_benchmark_capture.add_argument(
        "--query-visible",
        action="store_true",
        help="mark the published generation as visible through graph queries",
    )
    p_sync_benchmark_capture.add_argument(
        "--query-visibility-ms",
        type=float,
        default=0.0,
    )
    p_sync_benchmark_capture.add_argument("--readiness-ms", type=float)
    p_sync_benchmark_capture.add_argument("--mcp-graph-read-ms", type=float)
    p_sync_convergence = p_sync_sub.add_parser(
        "convergence",
        help="Inspect source-sync readiness convergence",
    )
    _add_json_flag(p_sync_convergence)
    p_sync_convergence_sub = p_sync_convergence.add_subparsers(
        dest="convergence_subcmd",
        required=True,
    )
    p_sync_convergence_inventory = p_sync_convergence_sub.add_parser(
        "inventory",
        help="Inventory checked-in deployment/source convergence policy",
    )
    _add_json_flag(p_sync_convergence_inventory)
    add_deployment_arg(p_sync_convergence_inventory)
    p_sync_convergence_inventory.add_argument(
        "--all-deployments",
        action="store_true",
        help="classify every checked-in deployment under deployments/",
    )
    p_sync_convergence_inventory.add_argument(
        "--deployments-root",
        help="override deployments root for tests or external portfolios",
    )
    p_sync_convergence_status = p_sync_convergence_sub.add_parser(
        "status",
        help="Show live source-sync readiness convergence status",
    )
    _add_json_flag(p_sync_convergence_status)
    add_deployment_arg(p_sync_convergence_status, required=True)
    add_dsn_arg(p_sync_convergence_status)
    p_sync_convergence_status.add_argument(
        "--max-source-age-seconds",
        type=int,
        default=3600,
        help="Maximum completed-scope freshness age before stale",
    )
    p_sync_convergence_status.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=15 * 60,
        help="Maximum source-sync producer heartbeat age before stale",
    )
    p_sync_webhook = p_sync_sub.add_parser(
        "webhook",
        help="Map one webhook JSON payload to an ingest job",
    )
    _add_json_flag(p_sync_webhook)
    add_deployment_arg(p_sync_webhook, required=True)
    add_dsn_arg(p_sync_webhook)
    p_sync_webhook.add_argument("--source", required=True)
    p_sync_webhook.add_argument("--payload-json")
    p_sync_webhook.add_argument("--payload-file")
    p_sync_webhook.add_argument("--signature")
    p_sync_webhook.add_argument(
        "--no-publish",
        action="store_true",
        help="enqueue ingest without worker-side publish",
    )
    p_sync_poll = p_sync_sub.add_parser(
        "poll",
        help="Run declared cheap probes once and enqueue changed sources",
    )
    _add_json_flag(p_sync_poll)
    add_deployment_arg(p_sync_poll, required=True)
    add_dsn_arg(p_sync_poll)
    p_sync_poll.add_argument("--source")
    p_sync_poll.add_argument(
        "--no-publish",
        action="store_true",
        help="enqueue ingest without worker-side publish",
    )
    p_sync_health = p_sync_sub.add_parser(
        "health",
        help="Show source-sync producer health and queue/DLQ state",
    )
    _add_json_flag(p_sync_health)
    add_deployment_arg(p_sync_health, required=True)
    add_dsn_arg(p_sync_health)
    p_sync_health.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=15 * 60,
        help="Maximum source-sync producer heartbeat age before stale",
    )

    # metrics
    p_met = sub.add_parser(
        "metrics",
        help="Roll-up metrics (LLM token / cost spend; QA-gate snapshot)",
    )
    _add_json_flag(p_met)
    p_met.add_argument(
        "--llm", action="store_true",
        help="show okg.llm_call_stats roll-up grouped by caller × model",
    )
    p_met.add_argument(
        "--consistency", action="store_true",
        help="show the okg.work_ledger per-update fallback-cause distribution "
             "for --deployment (PACT consistency-maintenance-accounting)",
    )
    p_met.add_argument(
        "--qa-snapshot", action="store_true",
        help="emit one QAGateMetrics JSON object for --deployment "
             "(per the deployment-qa capability)",
    )
    p_met.add_argument(
        "--generation",
        help="for --qa-snapshot: target a specific generation_id "
             "(default: latest published for the deployment)",
    )
    p_met.add_argument(
        "--since", default="24h",
        help="time window for --llm (e.g. '1h', '24h', '7d'); "
             "default 24h",
    )
    p_met.add_argument(
        "--disk", action="store_true",
        help="disk-usage report: total size, top relations (heap/index), "
             "dead-tuple %%, compressed-chunk ratio, unused indexes, marginal "
             "bytes/generation (PACT bound-okg-workspace-disk-growth)",
    )
    p_met.add_argument(
        "--deep", action="store_true",
        help="for --disk: run pgstattuple_approx for ground-truth dead-tuple "
             "percentages (scans tables; slow)",
    )
    p_met.add_argument(
        "--top", type=int, default=20,
        help="for --disk: number of largest relations to report (default 20)",
    )
    add_deployment_arg(p_met)
    add_dsn_arg(p_met)

    register_telemetry_subparsers(sub, add_json_flag=_add_json_flag)

    from okg.substrate.cli.source_draft import register_source_parser

    register_source_parser(sub, add_json_flag=_add_json_flag)

    # provision (canonical-dev-okg): one-command bootstrap of a deployment's
    # database from empty to a published generation. (The legacy `hygiene`
    # sweep command was removed by the chronos cutover -- the sweeps operated
    # on the now-dropped okg.nodes_live / okg.edges_live.)
    p_prov = sub.add_parser(
        "provision",
        help="Bring a deployment's DB up from empty to a published generation "
             "(extensions -> migrate -> catalog --apply -> ingest --publish)",
        epilog="exit codes: 0 provisioned; 8 provisioned and published, but "
               "the publish held the publisher lock past the capacity bar "
               "(status capacity_warning); 1 a step or the publish proof "
               "failed; 2 usage error",
    )
    _add_json_flag(p_prov)
    p_prov.add_argument(
        "--include", nargs="*", metavar="SOURCE", action="extend",
        help="limit the ingest to these source ids (default: all repo sources)",
    )
    p_prov.add_argument(
        "--readiness-sources",
        action="store_true",
        help="use the standardized agent-context readiness source set",
    )
    p_prov.add_argument(
        "--no-predecessors",
        action="store_true",
        help="do not expand included sources through predecessor_source closure",
    )
    p_prov.add_argument(
        "--publish-once",
        action="store_true",
        help="ingest selected sources first, then run one final publish",
    )
    p_prov.add_argument(
        "--source-workers",
        type=int,
        help="maximum parallel source workers during provision ingest",
    )
    add_deployment_arg(p_prov)
    add_dsn_arg(p_prov)

    # status
    p_stat = sub.add_parser("status", help="Deployment state summary")
    _add_json_flag(p_stat)
    add_deployment_arg(p_stat)
    add_dsn_arg(p_stat)
    p_stat.add_argument(
        "--watch", action="store_true",
        help="poll every --interval seconds and refresh a "
             "dashboard on stderr until SIGINT",
    )
    p_stat.add_argument(
        "--interval", type=int, default=5,
        help="seconds between polls when --watch is set "
             "(default 5)",
    )

    # schema — subparser group: `show` (config-file JSON Schema, back-compat)
    # and `quality` (database schema-quality scorecard). A bare
    # `okg schema <file>` is rewritten to `okg schema show <file>` in main().
    p_sch = sub.add_parser(
        "schema",
        help="Database schema-quality scorecard and config-file JSON Schema",
    )
    sch_sub = p_sch.add_subparsers(dest="schema_cmd")
    p_sch_show = sch_sub.add_parser(
        "show", help="Emit JSON Schema for an authored config file"
    )
    _add_json_flag(p_sch_show)
    p_sch_show.add_argument("file", help="e.g. deployment.yaml")
    p_sch_quality = sch_sub.add_parser(
        "quality", help="Run the schema-quality scorecard (current or vs a target)"
    )
    _add_json_flag(p_sch_quality)
    p_sch_quality.add_argument(
        "--current",
        action="store_true",
        help="Score the current schema (default when --target is absent).",
    )
    p_sch_quality.add_argument(
        "--target",
        help="Path to a target/golden schema DDL for an advisory pg-schema-diff.",
    )
    p_sch_quality.add_argument(
        "--dsn",
        help="DSN of a freshly migrated DB for SQL-assertion + diff sections. "
        "Defaults to OKG_TEST_DSN. Without one, only static checks run.",
    )
    p_sch_quality.add_argument(
        "--no-lint",
        action="store_true",
        help="Skip the (warning-only) Squawk lint section.",
    )

    # ---- fetch-deployments (per `separate-framework-and-deployments`) ----
    p_fetch = sub.add_parser(
        "fetch-deployments",
        help="Clone/update the okg-deployments cache",
    )
    _add_json_flag(p_fetch)
    p_fetch.add_argument(
        "--repo",
        help=(
            "Git URL of the reference-deployments repository. "
            "Required: this copy names no default (or set "
            "OKG_DEPLOYMENTS_REPO)."
        ),
    )
    p_fetch.add_argument(
        "--branch", default="main",
        help="Branch to fetch (default: main).",
    )
    p_fetch.add_argument(
        "--source-name",
        help=(
            "Override the cache subdir name. Defaults to a "
            "slug derived from the repo URL."
        ),
    )
    p_fetch.add_argument(
        "--list", action="store_true",
        help=(
            "Don't fetch — just list the profiles currently "
            "in the cache."
        ),
    )

    # ---- substrate lifecycle verbs (`dead-simple-install`) ----
    p_start = sub.add_parser(
        "start",
        help="Start a deployment's Docker stack",
    )
    _add_json_flag(p_start)
    add_deployment_arg(p_start, required=True)

    p_stop = sub.add_parser(
        "stop",
        help="Stop a deployment's Docker stack (data persists)",
    )
    _add_json_flag(p_stop)
    add_deployment_arg(p_stop, required=True)

    p_restart = sub.add_parser(
        "restart",
        help="Restart a deployment's Docker stack",
    )
    _add_json_flag(p_restart)
    add_deployment_arg(p_restart, required=True)

    # Long-lived deployment environments. These are intentionally separate
    # from reusable deployment definitions and worktree-local profiles.
    p_environment = sub.add_parser(
        "environment",
        help="Inspect and validate long-lived staging/production environments",
    )
    _add_json_flag(p_environment)
    p_environment_sub = p_environment.add_subparsers(
        dest="environment_subcmd",
        required=True,
    )
    for command_name, command_help in (
        ("show", "Show one resolved environment with secrets redacted"),
        ("plan", "Validate one environment against the resolved host topology"),
    ):
        p_environment_command = p_environment_sub.add_parser(
            command_name,
            help=command_help,
        )
        _add_json_flag(p_environment_command)
        p_environment_command.add_argument(
            "environment",
            help="Environment id or explicit path to environment.yaml",
        )
        p_environment_command.add_argument(
            "--environments-root",
            help=(
                "Versioned environment descriptor root; defaults to "
                "OKG_ENVIRONMENTS_DIR or ops/environments"
            ),
        )

    p_environment_product_status = p_environment_sub.add_parser(
        "product-status",
        help="Read one admitted product operation from the promotion journal",
    )
    _add_json_flag(p_environment_product_status)
    p_environment_product_status.add_argument(
        "environment",
        help="Production environment id or explicit path to environment.yaml",
    )
    p_environment_product_status.add_argument(
        "--operation-id",
        required=True,
        help="Promotion operation identifier to read",
    )
    p_environment_product_status.add_argument(
        "--environments-root",
        help=(
            "Versioned environment descriptor root; defaults to "
            "OKG_ENVIRONMENTS_DIR or ops/environments"
        ),
    )

    p_environment_ready = p_environment_sub.add_parser(
        "ready",
        help="Probe environment UI, webhook, MCP, generation, and policy health",
    )
    _add_json_flag(p_environment_ready)
    p_environment_ready.add_argument(
        "environment",
        help="Environment id or explicit path to environment.yaml",
    )
    p_environment_ready.add_argument(
        "--environments-root",
        help=(
            "Versioned environment descriptor root; defaults to "
            "OKG_ENVIRONMENTS_DIR or ops/environments"
        ),
    )
    p_environment_ready.add_argument(
        "--adapter",
        choices=("launchd", "systemd"),
        required=True,
        help="Installed host service adapter",
    )
    p_environment_ready.add_argument(
        "--launcher-python",
        default="/usr/bin/python3",
        help="Absolute supervisor-owned Python used by the stable launcher",
    )
    p_environment_ready.add_argument(
        "--systemd-scope",
        choices=("system", "user"),
        default="system",
        help="Installed systemd scope",
    )
    p_environment_ready.add_argument(
        "--postgres-image",
        default="okg-pg17:local",
        help="Postgres image in the installed environment service plan",
    )
    p_environment_ready.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-endpoint and MCP probe timeout",
    )

    p_environment_prepare = p_environment_sub.add_parser(
        "prepare-release",
        help="Build one sealed release from an exact repository revision",
    )
    _add_json_flag(p_environment_prepare)
    p_environment_prepare.add_argument("environment")
    p_environment_prepare.add_argument("--environments-root")
    p_environment_prepare.add_argument("--source-repo", default=".")
    p_environment_prepare.add_argument("--revision", required=True)

    p_environment_activate = p_environment_sub.add_parser(
        "activate-release",
        help="Select a verified sealed release for one environment",
    )
    _add_json_flag(p_environment_activate)
    p_environment_activate.add_argument("environment")
    p_environment_activate.add_argument("--environments-root")
    p_environment_activate.add_argument("--source-repo", default=".")
    p_environment_activate.add_argument("--artifact", required=True)
    p_environment_activate.add_argument("--activated-by", required=True)
    p_environment_activate.add_argument(
        "--adapter",
        choices=("launchd", "systemd"),
        required=True,
        help="Host service manager that must complete the activation handoff",
    )
    p_environment_activate.add_argument(
        "--launcher-python",
        default="/usr/bin/python3",
        help="Absolute supervisor-owned Python used by the stable launcher",
    )
    p_environment_activate.add_argument(
        "--systemd-scope",
        choices=("system", "user"),
        default="system",
        help="systemd scope for the activation handoff",
    )
    p_environment_activate.add_argument(
        "--postgres-image",
        default="okg-pg17:local",
        help="Postgres image in the installed environment service plan",
    )
    p_environment_activate.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Select the release, install and restart its services, and require "
            "exact live readback; otherwise emit a validation plan"
        ),
    )

    p_environment_clone_backup = p_environment_sub.add_parser(
        "clone-backup",
        help="Create a bounded production snapshot for staging rebinding",
    )
    _add_json_flag(p_environment_clone_backup)
    p_environment_clone_backup.add_argument("source")
    p_environment_clone_backup.add_argument("target")
    p_environment_clone_backup.add_argument("--environments-root")
    p_environment_clone_backup.add_argument("--output", required=True)
    p_environment_clone_backup.add_argument("--report", required=True)
    p_environment_clone_backup.add_argument("--apply", action="store_true")

    p_environment_clone_restore = p_environment_sub.add_parser(
        "clone-restore",
        help="Restore a verified bounded snapshot into a staging database",
    )
    _add_json_flag(p_environment_clone_restore)
    p_environment_clone_restore.add_argument("environment")
    p_environment_clone_restore.add_argument("--environments-root")
    p_environment_clone_restore.add_argument("--report", required=True)
    p_environment_clone_restore.add_argument(
        "--adapter", choices=("launchd", "systemd"), default="launchd"
    )
    p_environment_clone_restore.add_argument(
        "--launcher-python", default="/usr/bin/python3"
    )
    p_environment_clone_restore.add_argument(
        "--systemd-scope", choices=("system", "user"), default="system"
    )
    p_environment_clone_restore.add_argument(
        "--postgres-image", default="okg-pg17:local"
    )
    p_environment_clone_restore.add_argument("--apply", action="store_true")

    p_environment_clone_rebind = p_environment_sub.add_parser(
        "clone-rebind",
        help="Remove production authority from a restored staging clone",
    )
    _add_json_flag(p_environment_clone_rebind)
    p_environment_clone_rebind.add_argument("environment")
    p_environment_clone_rebind.add_argument("--environments-root")
    p_environment_clone_rebind.add_argument("--report", required=True)
    p_environment_clone_rebind.add_argument("--output")
    p_environment_clone_rebind.add_argument("--apply", action="store_true")

    p_environment_claim_owner = p_environment_sub.add_parser(
        "claim-owner",
        help="Claim an unstamped environment database after resident-state checks",
    )
    _add_json_flag(p_environment_claim_owner)
    p_environment_claim_owner.add_argument("environment")
    p_environment_claim_owner.add_argument("--environments-root")
    p_environment_claim_owner.add_argument("--apply", action="store_true")

    p_environment_nomos = p_environment_sub.add_parser(
        "seed-nomos-baseline",
        help="Seed the environment-scoped Nomos baseline before first ingest",
    )
    _add_json_flag(p_environment_nomos)
    p_environment_nomos.add_argument("environment")
    p_environment_nomos.add_argument("--environments-root")
    p_environment_nomos.add_argument("--apply", action="store_true")

    p_environment_promote = p_environment_sub.add_parser(
        "promote",
        help="Promote one approved sealed artifact from staging to production",
    )
    _add_json_flag(p_environment_promote)
    p_environment_promote.add_argument("staging", help="Staging environment id")
    p_environment_promote.add_argument("production", help="Production environment id")
    p_environment_promote.add_argument(
        "--environments-root",
        help="Versioned environment descriptor root",
    )
    p_environment_promote.add_argument("--artifact", required=True)
    p_environment_promote.add_argument(
        "--transfer-archive",
        help="Authenticated staging export archive to admit before promotion",
    )
    p_environment_promote.add_argument(
        "--transfer-envelope",
        help="Authenticated staging export envelope paired with --transfer-archive",
    )
    p_environment_promote.add_argument(
        "--transfer-secret-env",
        default="OKG_CI_PROMOTION_TRANSFER_SECRET",
        help="Environment variable holding the staging/production controller authentication secret",
    )
    p_environment_promote.add_argument(
        "--candidate-revision",
        help="Exact trusted candidate revision required by a transfer envelope",
    )
    p_environment_promote.add_argument("--promotion-id", required=True)
    p_environment_promote.add_argument("--approved-by", required=True)
    p_environment_promote.add_argument("--staging-evidence")
    p_environment_promote.add_argument(
        "--supervision-evidence",
        help="Local staging supervision receipt (not used with authenticated transfer)",
    )
    p_environment_promote.add_argument(
        "--adapter", choices=("launchd", "systemd"), required=True
    )
    p_environment_promote.add_argument(
        "--launcher-python", default="/usr/bin/python3"
    )
    p_environment_promote.add_argument(
        "--systemd-scope", choices=("system", "user"), default="system"
    )
    p_environment_promote.add_argument("--postgres-image", default="okg-pg17:local")
    p_environment_promote.add_argument("--source-repo", default=".")
    p_environment_promote.add_argument("--backup-root", default=".okg/backups/promotions")
    p_environment_promote.add_argument("--drain-timeout-seconds", type=float, default=600.0)
    p_environment_promote.add_argument("--readiness-timeout-seconds", type=float, default=30.0)
    p_environment_promote.add_argument(
        "--apply",
        action="store_true",
        help="Execute the promotion; otherwise emit the validated approval plan",
    )

    p_environment_service = p_environment_sub.add_parser(
        "service",
        help="Render, install, and control environment-owned services",
    )
    _add_json_flag(p_environment_service)
    p_environment_service_sub = p_environment_service.add_subparsers(
        dest="environment_service_subcmd",
        required=True,
    )
    for action in (
        "render",
        "install",
        "start",
        "stop",
        "restart",
        "status",
        "logs",
    ):
        command = p_environment_service_sub.add_parser(
            action,
            help=f"{action.title()} one environment's owned services",
        )
        _add_json_flag(command)
        command.add_argument(
            "environment",
            help="Environment id or explicit path to environment.yaml",
        )
        command.add_argument(
            "--environments-root",
            help=(
                "Versioned environment descriptor root; defaults to "
                "OKG_ENVIRONMENTS_DIR or ops/environments"
            ),
        )
        command.add_argument(
            "--adapter",
            choices=("launchd", "systemd"),
            required=True,
            help="Host service manager to render",
        )
        command.add_argument(
            "--launcher-python",
            default="/usr/bin/python3",
            help="Absolute supervisor-owned Python used by the stable launcher",
        )
        command.add_argument(
            "--systemd-scope",
            choices=("system", "user"),
            default="system",
            help="systemd scope for lifecycle commands",
        )
        command.add_argument(
            "--postgres-image",
            default="okg-pg17:local",
            help="Postgres image for the environment-owned database container",
        )
        if action != "render":
            command.add_argument(
                "--apply",
                action="store_true",
                help=f"Execute the planned {action} operation",
            )

    # deployment portfolio audit
    #
    # `deployments` and `deployment` differ only by a trailing `s`, which is
    # a defect in a surface people type (define-newcomer-entry-path
    # req.entry.migrate-the-other-doors). Neither is a way IN — the door is
    # `okg install` — so the resolution is that each help string says what
    # it operates on and names the other, rather than leaving the reader to
    # guess from a plural.
    p_deps = sub.add_parser(
        "deployments",
        help="Audit ALL deployments as a portfolio (one: `okg deployment`)",
    )
    _add_json_flag(p_deps)
    p_deps_sub = p_deps.add_subparsers(
        dest="deployments_subcmd",
        required=True,
    )
    p_deps_audit = p_deps_sub.add_parser(
        "audit",
        help="Audit every deployment against the portfolio lifecycle contract",
    )
    _add_json_flag(p_deps_audit)
    p_deps_audit.add_argument(
        "--root",
        default=".",
        help="Repository or deployment root to scan",
    )
    p_deps_audit.add_argument(
        "--contract-level",
        choices=("current", "next"),
        default="current",
        help="Contract level to evaluate",
    )
    p_deps_audit.add_argument(
        "--include-archived",
        action="store_true",
        help="Include archived deployments in the audit output",
    )

    # deployment bootstrap
    p_dep = sub.add_parser(
        "deployment",
        help=(
            "Author, bootstrap, inspect ONE deployment (all: `okg deployments`)"
        ),
    )
    _add_json_flag(p_dep)
    install_actionable_errors(
        p_dep, "okg deployment ready <name> --checks dogfood --json"
    )
    p_dep_sub = p_dep.add_subparsers(dest="subcmd", required=True)

    p_dep_show = p_dep_sub.add_parser(
        "show",
        help="Print the resolved deployment manifest (with runtime block)",
    )
    _add_json_flag(p_dep_show)
    p_dep_show.add_argument(
        "deployment",
        help="Deployment name (or path to a deployment.yaml)",
    )

    p_dep_read_status = p_dep_sub.add_parser(
        "read-status",
        help="Read the typed status of an existing deployment lifecycle operation",
    )
    _add_json_flag(p_dep_read_status)
    p_dep_read_status.add_argument("deployment", help="Durable deployment identity")
    add_renamed_argument(
        p_dep_read_status, "--target", "--profile",
        dest="profile",
        default="dev",
        choices=["dev", "prod"],
        help="Lifecycle database target (default: dev)",
    )
    p_dep_read_status.add_argument(
        "--state-file",
        help="Existing lifecycle journal (default: .okg/up/<deployment>-<profile>.json)",
    )

    p_dep_conformance = p_dep_sub.add_parser(
        "conformance",
        help="Discover the inert external-distribution conformance wire contract",
    )
    _add_json_flag(p_dep_conformance)
    p_dep_conformance_sub = p_dep_conformance.add_subparsers(
        dest="conformance_subcmd",
        required=True,
    )
    p_dep_conformance_schema = p_dep_conformance_sub.add_parser(
        "schema",
        help="Print a public conformance request or receipt JSON Schema",
    )
    _add_json_flag(p_dep_conformance_schema)
    p_dep_conformance_schema.add_argument(
        "--kind",
        choices=("request", "receipt"),
        required=True,
        help="Wire document whose JSON Schema should be printed",
    )
    p_dep_conformance_schema.add_argument(
        "--protocol-version",
        choices=("v1", "v2"),
        default="v1",
        help="Explicit conformance protocol (default: v1)",
    )
    p_dep_conformance_validate = p_dep_conformance_sub.add_parser(
        "validate",
        help="Validate inert conformance JSON without running a distribution",
    )
    _add_json_flag(p_dep_conformance_validate)
    p_dep_conformance_validate.add_argument(
        "--request",
        required=True,
        help="External-distribution conformance request JSON",
    )
    p_dep_conformance_validate.add_argument(
        "--receipt",
        help="Optional receipt JSON; when supplied, validate the complete exchange",
    )
    p_dep_conformance_validate.add_argument(
        "--trusted-external-review",
        help=(
            "Optional acknowledgement record published out of band, read as "
            "inert data; needed for a receipt claiming a current acknowledgement"
        ),
    )

    p_dep_install_plan = p_dep_sub.add_parser(
        "install-plan",
        help="Resolve a verified package and bundle into an install plan and lock",
    )
    _add_json_flag(p_dep_install_plan)
    p_dep_install_plan.add_argument(
        "--package",
        required=True,
        help="Verified <sha256>.okgdist.tar distribution package",
    )
    p_dep_install_plan.add_argument(
        "--bundle",
        required=True,
        help="Bundle id declared by the package",
    )
    p_dep_install_plan.add_argument(
        "--instance",
        required=True,
        help="Instance config JSON/YAML carrying secret references, never values",
    )
    p_dep_install_plan.add_argument(
        "--output",
        required=True,
        help="Directory to receive plan.json and lock.json; must not already exist",
    )
    p_dep_install_plan.add_argument(
        "--replace-lock",
        action="store_true",
        help="Accept a resolution that differs from the recorded lock",
    )

    p_dep_receipt = p_dep_sub.add_parser(
        "install-receipt",
        help="Record an upgrade, rollback or drift over planned installs",
    )
    _add_json_flag(p_dep_receipt)
    p_dep_receipt.add_argument(
        "--kind",
        required=True,
        choices=("upgrade", "rollback", "drift"),
        help="Which lifecycle event to record",
    )
    p_dep_receipt.add_argument(
        "--baseline",
        required=True,
        help="Install directory holding the baseline plan.json and lock.json",
    )
    p_dep_receipt.add_argument(
        "--candidate",
        required=True,
        help="Install directory holding the candidate or re-planned documents",
    )
    p_dep_receipt.add_argument(
        "--baseline-release",
        help="Baseline sealed-release artifact id (upgrade and rollback)",
    )
    p_dep_receipt.add_argument(
        "--candidate-release",
        help="Candidate sealed-release artifact id (upgrade and rollback)",
    )
    p_dep_receipt.add_argument(
        "--baseline-package",
        help="Baseline package archive, which a rollback requires to be present",
    )
    p_dep_receipt.add_argument(
        "--okg-version",
        help="OKG version the deployment runs; defaults to the baseline lock's",
    )
    p_dep_receipt.add_argument(
        "--output",
        required=True,
        help="Directory to receive the receipt; must not already exist",
    )

    p_dep_new = p_dep_sub.add_parser(
        "new",
        help="Create a loader-valid deployment scaffold",
    )
    _add_json_flag(p_dep_new)
    p_dep_new.add_argument("name", help="Deployment name to scaffold")
    p_dep_new.add_argument(
        "--deployments-root",
        help="Directory that will receive the new deployment",
    )
    p_dep_new.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing non-empty deployment directory",
    )
    p_dep_new.add_argument(
        "--yes",
        action="store_true",
        help="Required with --force to confirm replacement",
    )
    p_dep_new.add_argument(
        "--apply",
        action="store_true",
        help="Required to authorize scaffold creation",
    )

    # `add-source` / `source-types` (issue #1802): one guided flow to add a
    # supported source; parser + handlers live in cli/source_add.py.
    from okg.substrate.cli.source_add import (
        register_deployment_subparsers as _register_add_source,
    )
    _register_add_source(
        p_dep_sub, add_json_flag=_add_json_flag, add_dsn_arg=add_dsn_arg,
    )
    # `remove-source` (issue #2477): its counterpart; cli/source_remove.py.
    from okg.substrate.cli.source_remove import (
        register_remove_source_subparser as _register_remove_source,
    )
    _register_remove_source(
        p_dep_sub, add_json_flag=_add_json_flag, add_dsn_arg=add_dsn_arg,
    )

    p_dep_list = p_dep_sub.add_parser(
        "list",
        help="List every OKG deployment on the host with DB/role/MCP/reachability",
    )
    _add_json_flag(p_dep_list)
    p_dep_list.add_argument(
        "--no-probe",
        action="store_true",
        help="Skip DB connection + node_count probing (config-only inventory)",
    )

    p_dep_fork = p_dep_sub.add_parser(
        "fork-worktree",
        help="Fork the OKG for a worktree + wire+enable its branch-pinned MCP",
    )
    _add_json_flag(p_dep_fork)
    p_dep_fork.add_argument(
        "--worktree-path",
        help="Worktree directory to fork+wire (default: current directory)",
    )
    p_dep_fork.add_argument(
        "--deployment",
        default=None,
        help=(
            "Deployment to fork (default: OKG_DEPLOYMENT, else the checkout's "
            "configured graph_projection.deployment; refuses when none is set)"
        ),
    )
    p_dep_fork.add_argument(
        "--base-branch",
        default="default",
        help="Base branch to fork the worktree branch from (default: default)",
    )

    p_dep_gc = p_dep_sub.add_parser(
        "gc-forks",
        help="Reap experiment forks whose worktree no longer exists",
    )
    _add_json_flag(p_dep_gc)
    p_dep_gc.add_argument(
        "--deployment",
        default=None,
        help=(
            "Deployment whose forks to GC (default: OKG_DEPLOYMENT, else the "
            "checkout's configured graph_projection.deployment; refuses when "
            "none is set)"
        ),
    )
    p_dep_gc.add_argument(
        "--apply",
        action="store_true",
        help="Abandon orphaned forks (default: dry-run, report only)",
    )
    p_dep_gc.add_argument(
        "--repo-path",
        help="Repo whose worktrees to check (default: current directory)",
    )

    p_dep_gc_scaffolds = p_dep_sub.add_parser(
        "gc-scaffolds",
        help=(
            "List and (with --yes) remove stale operator-state deployment "
            "scaffolds under the user-state deployments root"
        ),
    )
    _add_json_flag(p_dep_gc_scaffolds)
    p_dep_gc_scaffolds.add_argument(
        "--yes",
        action="store_true",
        help="Remove the stale scaffolds (default: dry-run, report only)",
    )
    p_dep_gc_scaffolds.add_argument(
        "--root",
        default=None,
        help=(
            "Override the user-state deployments root to scan "
            "(default: the resolved user-state root)"
        ),
    )

    p_dep_env = p_dep_sub.add_parser(
        "env",
        help="Inspect or write a deployment's local environment profile",
    )
    _add_json_flag(p_dep_env)
    p_dep_env.add_argument(
        "deployment",
        nargs="?",
        help="Deployment name (or path to a deployment.yaml)",
    )
    p_dep_env.add_argument(
        "_env_extra_arg",
        nargs="?",
        help=argparse.SUPPRESS,
    )
    p_dep_env.add_argument(
        "--local-env",
        help="Local env profile path; defaults to .okg/local.env",
    )
    p_dep_env.add_argument(
        "--write-local",
        action="store_true",
        help="Write --dsn to the local profile for the selected env var",
    )
    p_dep_env.add_argument(
        "--env-name",
        help="Env var to write with --write-local; defaults to manifest ref",
    )
    p_dep_env.add_argument(
        "--dsn",
        help="DSN value used with --write-local",
    )
    p_dep_env.add_argument(
        "--show-values",
        action="store_true",
        help="Show resolved secret values instead of redacting them",
    )
    p_dep_env.add_argument(
        "--for-command",
        default="read",
        help=(
            "With `env doctor`: classify env conflicts for read, write, "
            "runtime, or a command name"
        ),
    )

    p_dep_iso = p_dep_sub.add_parser(
        "isolated",
        help="List or garbage-collect worktree-isolated databases "
             "(standardize-deployment-authority)",
    )
    _add_json_flag(p_dep_iso)
    p_dep_iso.add_argument("iso_action", choices=["list", "gc"])
    p_dep_iso.add_argument("deployment", help="Deployment name")
    p_dep_iso.add_argument(
        "--yes", action="store_true",
        help="With gc: actually DROP orphan databases (default dry-run)",
    )
    add_dsn_arg(p_dep_iso)

    p_dep_release = p_dep_sub.add_parser(
        "release",
        help="Inspect and enforce a deployment release/version contract",
    )
    _add_json_flag(p_dep_release)
    p_dep_release.add_argument("deployment", help="Deployment name")
    p_dep_release.add_argument(
        "--process-role",
        default="cli",
        help="Process role label for the report (cli, mcp, runtime-worker)",
    )
    add_dsn_arg(p_dep_release)

    p_dep_backup = p_dep_sub.add_parser(
        "backup",
        help="Manage deployment disaster-recovery proof",
    )
    p_dep_backup_sub = p_dep_backup.add_subparsers(
        dest="backup_subcmd",
        required=True,
    )
    p_dep_backup_verify = p_dep_backup_sub.add_parser(
        "verify",
        help="Create and fully restore a current recovery archive",
    )
    _add_json_flag(p_dep_backup_verify)
    p_dep_backup_verify.add_argument("deployment", help="Deployment name")
    add_renamed_argument(
        p_dep_backup_verify, "--target", "--profile",
        dest="profile",
        choices=["dev", "prod"],
        default="prod",
        help="Database target (default: prod)",
    )
    p_dep_backup_verify.add_argument(
        "--backup-dir",
        help="Override the pg_dump backup output directory",
    )
    p_dep_backup_verify.add_argument(
        "--backup-keep",
        type=int,
        default=1,
        help="Verified recovery archives to retain (default: 1)",
    )
    p_dep_backup_verify.add_argument(
        "--state-file",
        help="Override the independent verification state file",
    )
    p_dep_backup_verify.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume a failed archive/restore verification (default: true)",
    )
    p_dep_backup_verify.add_argument(
        "--reset-state",
        action="store_true",
        help="Discard invalid independent verification state before running",
    )
    add_dsn_arg(p_dep_backup_verify)
    p_dep_backup_restore = p_dep_backup_sub.add_parser(
        "restore",
        help=(
            "Installed deployments: go back to the backup `okg up` took "
            "before an upgrade (plan by default)"
        ),
    )
    _add_json_flag(p_dep_backup_restore)
    p_dep_backup_restore.add_argument("deployment", help="Deployment name")
    p_dep_backup_restore.add_argument(
        "--archive",
        required=True,
        help="The pre-upgrade archive; its <archive>.json sidecar must sit beside it",
    )
    p_dep_backup_restore.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Stop the worker, restore into a fresh database, verify it, and "
            "swap it in by rename (the live database is kept, never dropped)"
        ),
    )

    p_dep_up = p_dep_sub.add_parser(
        "up",
        help="Idempotent deploy/upgrade: migrate -> catalog apply -> "
             "ingest+publish, target echoed, worker-restart step named",
    )
    _add_json_flag(p_dep_up)
    p_dep_up.add_argument("deployment", nargs="?", help="Deployment name")
    p_dep_up.add_argument(
        "--all",
        action="store_true",
        help="Upgrade the registered kept fleet and report each deployment",
    )
    add_renamed_argument(
        p_dep_up, "--target", "--profile",
        dest="profile", choices=["dev", "prod"], default="prod",
        help="dev = worktree-isolated database; prod = declared database",
    )
    p_dep_up.add_argument(
        "--include", nargs="*", default=None, action="extend",
        help="Limit ingest to these source ids",
    )
    p_dep_up.add_argument(
        "--skip-migrate", action="store_true",
        help="Skip pending migrations (recorded loudly in the report; for "
             "known-blocked migrations being fixed separately)",
    )
    p_dep_up.add_argument(
        "--skip-destructive-migrations",
        action="store_true",
        help="Fail if destructive migrations are pending instead of deferring them",
    )
    p_dep_up.add_argument(
        "--confirm-destructive-migrations",
        action="store_true",
        help=(
            "Apply reviewed destructive-migration adapters after backup and "
            "worker stop; unknown destructive migrations still fail closed"
        ),
    )
    p_dep_up.add_argument(
        "--skip-backup",
        action="store_true",
        help=(
            "Skip creating a backup; this never grants mutation authority and "
            "requires an exact independently verified receipt for a mutating run"
        ),
    )
    p_dep_up.add_argument(
        "--verified-backup-state",
        help="Admit an exact target-bound verified backup receipt after validation",
    )
    p_dep_up.add_argument(
        "--verified-backup-source-revision",
        help="Git revision whose run produced --verified-backup-state",
    )
    p_dep_up.add_argument(
        "--precheckout-target-revision",
        help=argparse.SUPPRESS,
    )
    p_dep_up.add_argument(
        "--lifecycle-plan",
        help="Trusted-baseline lifecycle plan receipt authorizing this candidate",
    )
    p_dep_up.add_argument(
        "--confirm-safety-plan",
        help=(
            "Exact safety-plan digest emitted by --plan; required for a "
            "substituted database target or irreversible external smoke effects"
        ),
    )
    p_dep_up.add_argument(
        "--apply-services",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Execute service stop/restart phases (default: true). "
             "--no-apply-services reports a partial run.",
    )
    p_dep_up.add_argument(
        "--sync-code",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="For prod pins, fetch and checkout release.pin.code_revision "
             "(default: true). --no-sync-code reports a partial run.",
    )
    p_dep_up.add_argument(
        "--sync-deps",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run uv sync during the dependency_sync phase (default: true). "
             "--no-sync-deps reports a partial run.",
    )
    p_dep_up.add_argument(
        "--plan",
        action="store_true",
        help="Plan-only preview: report what each mutate phase would do "
             "without applying code/dep sync or service restart.",
    )
    p_dep_up.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Resume from the persisted phase state (default: true)",
    )
    p_dep_up.add_argument(
        "--reset-state",
        action="store_true",
        help="Discard persisted phase state before running",
    )
    p_dep_up.add_argument("--runtime-fence-owner", help=argparse.SUPPRESS)
    p_dep_up.add_argument(
        "--state-file",
        help="Override the persisted upgrade state file",
    )
    p_dep_up.add_argument(
        "--state-dir",
        help="With --all, override the directory for per-deployment state files",
    )
    p_dep_up.add_argument(
        "--fleet-dispositions",
        help=(
            "With --all, JSON/YAML file recording retired or waived fleet "
            "members with reasons"
        ),
    )
    p_dep_up.add_argument(
        "--backup-dir",
        help="Override the pg_dump backup output directory",
    )
    p_dep_up.add_argument(
        "--backup-keep",
        type=int,
        default=None,
        help=(
            "Verified backup archives to keep per deployment (default: 1). "
            "Superseded archives are deleted only at the end of a fully "
            "successful upgrade; failed runs never delete backups."
        ),
    )
    add_renamed_argument(
        p_dep_up, "--checks", "--readiness-profile",
        dest="readiness_profile",
        choices=("general", "dogfood", "ci", "standard"),
        default="general",
        help="Readiness checks to run after the smoke publish",
    )
    p_dep_up.add_argument(
        "--stop-after",
        help=argparse.SUPPRESS,
    )
    _add_installed_upgrade_arguments(p_dep_up)
    add_dsn_arg(p_dep_up)

    p_dep_apply = p_dep_sub.add_parser(
        "apply",
        help=(
            "Plan or converge catalog, Nomos, source supervision, and worker "
            "configuration (dry-run by default)"
        ),
    )
    _add_json_flag(p_dep_apply)
    p_dep_apply.add_argument("deployment", help="Deployment name")
    p_dep_apply.add_argument(
        "--apply",
        action="store_true",
        help="Execute the current plan (requires --confirm <plan-id>)",
    )
    p_dep_apply.add_argument(
        "--confirm",
        help="Exact plan id emitted by the latest dry-run",
    )
    p_dep_apply.add_argument(
        "--worker-ack-timeout-seconds",
        type=float,
        default=30.0,
        help="Seconds to wait for the replacement worker acknowledgment",
    )
    add_dsn_arg(p_dep_apply)

    p_up = sub.add_parser(
        "up",
        help="Upgrade a deployment through ordered, resumable phases",
    )
    _add_json_flag(p_up)
    p_up.add_argument("deployment", nargs="?", help="Deployment name")
    p_up.add_argument(
        "--all",
        action="store_true",
        help="Upgrade the registered kept fleet and report each deployment",
    )
    add_renamed_argument(
        p_up, "--target", "--profile",
        dest="profile", choices=["dev", "prod"], default="prod",
        help="dev = worktree-isolated database; prod = declared database",
    )
    p_up.add_argument(
        "--include", nargs="*", default=None, action="extend",
        help="Limit smoke ingest to these source ids",
    )
    p_up.add_argument(
        "--skip-migrate", action="store_true",
        help="Skip pending migrations (recorded loudly in the report)",
    )
    p_up.add_argument(
        "--skip-destructive-migrations",
        action="store_true",
        help="Fail if destructive migrations are pending instead of deferring them",
    )
    p_up.add_argument(
        "--confirm-destructive-migrations",
        action="store_true",
        help=(
            "Apply reviewed destructive-migration adapters after backup and "
            "worker stop; unknown destructive migrations still fail closed"
        ),
    )
    p_up.add_argument(
        "--skip-backup",
        action="store_true",
        help=(
            "Skip creating a backup; this never grants mutation authority and "
            "requires an exact independently verified receipt for a mutating run"
        ),
    )
    p_up.add_argument(
        "--verified-backup-state",
        help="Admit an exact target-bound verified backup receipt",
    )
    p_up.add_argument(
        "--verified-backup-source-revision",
        help="Exact source commit for a pre-checkout verified backup receipt",
    )
    p_up.add_argument(
        "--precheckout-target-revision",
        help=argparse.SUPPRESS,
    )
    p_up.add_argument(
        "--lifecycle-plan",
        help="Trusted-baseline lifecycle plan receipt authorizing this candidate",
    )
    p_up.add_argument(
        "--confirm-safety-plan",
        help=(
            "Exact safety-plan digest emitted by --plan; required for a "
            "substituted database target or irreversible external smoke effects"
        ),
    )
    p_up.add_argument(
        "--apply-services",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Execute service stop/restart phases (default: true). "
             "--no-apply-services leaves the worker on stale code (reported "
             "as a partial run).",
    )
    p_up.add_argument(
        "--sync-code",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch and checkout the release pin (default: true). "
             "--no-sync-code reports a partial run.",
    )
    p_up.add_argument(
        "--sync-deps",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run uv sync (default: true). --no-sync-deps reports a "
             "partial run.",
    )
    p_up.add_argument(
        "--plan",
        action="store_true",
        help="Plan-only preview: report what each mutate phase would do "
             "without applying code/dep sync or service restart.",
    )
    p_up.add_argument(
        "--resume",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    p_up.add_argument("--reset-state", action="store_true")
    p_up.add_argument("--state-file")
    p_up.add_argument("--state-dir")
    p_up.add_argument("--fleet-dispositions")
    p_up.add_argument("--backup-dir")
    p_up.add_argument(
        "--backup-keep",
        type=int,
        default=None,
        help=(
            "Verified backup archives to keep per deployment (default: 1). "
            "Superseded archives are deleted only at the end of a fully "
            "successful upgrade; failed runs never delete backups."
        ),
    )
    add_renamed_argument(
        p_up, "--checks", "--readiness-profile",
        dest="readiness_profile",
        choices=("general", "dogfood", "ci", "standard"),
        default="general",
        help="Readiness checks to run after the smoke publish",
    )
    p_up.add_argument("--stop-after", help=argparse.SUPPRESS)
    _add_installed_upgrade_arguments(p_up)
    add_dsn_arg(p_up)

    p_dep_ready = p_dep_sub.add_parser(
        "ready",
        help="Check whether a deployment is ready for agent graph reads",
    )
    _add_json_flag(p_dep_ready)
    p_dep_ready.add_argument(
        "deployment",
        help="Deployment name (or path to a deployment.yaml)",
    )
    p_dep_ready.add_argument(
        "--local-env",
        help="Local env profile path; defaults to .okg/local.env",
    )
    add_renamed_argument(
        p_dep_ready, "--checks", "--profile",
        dest="profile",
        choices=("general", "dogfood", "ci", "standard"),
        default="general",
        help=(
            "Readiness checks; dogfood and ci add strict source freshness "
            "and sync liveness; standard adds 14-day baseline-source "
            "freshness and the runtime checks"
        ),
    )
    p_dep_prove_live = p_dep_sub.add_parser(
        "prove-live",
        help="Collect one schema-validated, generation-bound deployment proof",
    )
    _add_json_flag(p_dep_prove_live)
    p_dep_prove_live.add_argument("deployment")
    add_renamed_argument(
        p_dep_prove_live, "--checks", "--profile",
        dest="profile", choices=("ci",), default="ci",
        help="Readiness checks for the proof (only ci)",
    )
    p_dep_prove_live.add_argument(
        "--operation-receipt", required=True,
        help="Pending v2 controller receipt produced after deployment up",
    )
    p_dep_prove_live.add_argument("--max-receipt-age-seconds", type=float, default=3600.0)
    p_dep_prove_live.add_argument("--max-observation-seconds", type=float, default=120.0)
    p_dep_prove_live.add_argument("--output")
    add_dsn_arg(p_dep_prove_live)
    p_dep_ready.add_argument(
        "--max-active-queue-depth",
        type=int,
        default=0,
        help="Maximum queued/leased non-ignored jobs allowed",
    )
    p_dep_ready.add_argument(
        "--max-source-age-seconds",
        type=int,
        default=None,
        help="Maximum age for required source watermarks",
    )
    p_dep_ready.add_argument(
        "--max-producer-age-seconds",
        type=int,
        default=None,
        help="Maximum age for required source-sync producer heartbeats",
    )
    p_dep_ready.add_argument(
        "--skip-doctor",
        action="store_true",
        help="Skip the deployment doctor subprocess",
    )
    p_dep_ready.add_argument(
        "--worker-process-authority",
        choices=("local", "kubernetes-pod-readiness"),
        default="local",
        help=(
            "Where runtime.worker_processes is measured (dogfood profile "
            "only; ignored by the others). 'local' counts worker processes on "
            "this host. 'kubernetes-pod-readiness' is for a probe that runs "
            "in another Kubernetes pod than the worker (the Helm readiness "
            "test): when no worker runs here, the check reports skipped and "
            "names the runtime pod's own readiness probe, which runs this "
            "check, as its authority. Outside a Kubernetes pod it is ignored "
            "and the check is measured here"
        ),
    )
    p_dep_ready.add_argument(
        "--show-values",
        action="store_true",
        help="Show resolved secret values instead of redacting them",
    )
    p_dep_ready.add_argument(
        "--explain",
        action="store_true",
        help="Add normalized diagnostic findings without changing ready fields",
    )
    add_dsn_arg(p_dep_ready)

    p_dep_plan = p_dep_sub.add_parser(
        "plan",
        help="Inspect descriptor-only deployment wiring",
    )
    _add_json_flag(p_dep_plan)
    p_dep_plan.add_argument(
        "deployment",
        help="Deployment name, directory, or deployment.yaml path",
    )
    p_dep_plan.add_argument(
        "--deployments-root",
        help="Deployment root used when resolving a deployment name",
    )
    p_dep_plan.add_argument(
        "--no-check-imports",
        action="store_true",
        help="Skip safe import-spec checks for declared Python references",
    )

    p_dep_lint = p_dep_sub.add_parser(
        "lint",
        help="Run static deployment authoring diagnostics",
    )
    _add_json_flag(p_dep_lint)
    p_dep_lint.add_argument(
        "deployment",
        help="Deployment name, directory, or deployment.yaml path",
    )
    p_dep_lint.add_argument(
        "--deployments-root",
        help="Deployment root used when resolving a deployment name",
    )
    p_dep_lint.add_argument(
        "--live",
        action="store_true",
        help="Include live/runtime diagnostics when supported",
    )

    p_dep_compat = p_dep_sub.add_parser(
        "compat",
        help=(
            "Check, offline, whether a deployment works with this okg "
            "(exit 0 compatible, 1 incompatible, 2 unknown)"
        ),
    )
    _add_json_flag(p_dep_compat)
    from okg.substrate.deployment_compat import add_compat_arguments

    add_compat_arguments(p_dep_compat)

    p_dep_profile = p_dep_sub.add_parser(
        "profile",
        help="Inspect built-in deployment profiles",
    )
    _add_json_flag(p_dep_profile)
    p_dep_profile_sub = p_dep_profile.add_subparsers(
        dest="profile_subcmd",
        required=True,
    )
    p_dep_profile_list = p_dep_profile_sub.add_parser(
        "list",
        help="List built-in deployment profiles",
    )
    _add_json_flag(p_dep_profile_list)
    p_dep_profile_explain = p_dep_profile_sub.add_parser(
        "explain",
        help="Explain a deployment profile without writing files",
    )
    _add_json_flag(p_dep_profile_explain)
    p_dep_profile_explain.add_argument(
        "profile",
        help="Profile name, directory, or profile.yaml path",
    )

    p_dep_nomos_rollout = p_dep_sub.add_parser(
        "nomos-rollout-audit",
        help="Check the Nomos posture rollout inventory across deployments",
    )
    _add_json_flag(p_dep_nomos_rollout)
    p_dep_nomos_rollout.add_argument(
        "--deployment-dir",
        default=None,
        help=(
            "External okg-deployments directory; resolves via "
            "OKG_DEPLOYMENTS_DIR or the fetched cache, and fails loudly "
            "when unconfigured"
        ),
    )
    p_dep_nomos_rollout.add_argument(
        "--repo-deployment-dir",
        default="deployments",
        help="In-repo deployment directory to scan",
    )
    p_dep_nomos_rollout.add_argument(
        "--profile-dir",
        default="src/okg/substrate/library/profiles",
        help="Profile defaults directory to scan",
    )
    p_dep_nomos_rollout.add_argument(
        "--out",
        default=None,
        help="Write the rollout inventory JSON to this path",
    )
    p_dep_nomos_rollout.add_argument(
        "--apply-baseline",
        default=None,
        metavar="DEPLOYMENT",
        help=(
            "Apply the conservative active Nomos baseline to one deployment "
            "after reporting the inventory"
        ),
    )
    add_dsn_arg(p_dep_nomos_rollout)

    p_dep_nomos_lineage_rollout = p_dep_sub.add_parser(
        "nomos-source-lineage-rollout-audit",
        help="Check source-policy and source-scoped read coverage rollout",
    )
    _add_json_flag(p_dep_nomos_lineage_rollout)
    p_dep_nomos_lineage_rollout.add_argument(
        "--deployment-dir",
        default=None,
        help=(
            "External okg-deployments directory; resolves via "
            "OKG_DEPLOYMENTS_DIR or the fetched cache, and fails loudly "
            "when unconfigured"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--repo-deployment-dir",
        default="deployments",
        help="In-repo deployment directory to scan",
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--profile-dir",
        default="src/okg/substrate/library/profiles",
        help="Profile defaults directory to scan",
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--out",
        default=None,
        help="Write the source-lineage rollout inventory JSON to this path",
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--runtime-report",
        action="store_true",
        help=(
            "Include live Nomos source-lineage readiness where deployment "
            "DSNs resolve; blocked entries keep owner and recovery commands"
        ),
    )
    add_renamed_argument(
        p_dep_nomos_lineage_rollout, "--checks", "--readiness-profile",
        dest="readiness_profile",
        default=None,
        help=(
            "Readiness checks for runtime source-lineage checks; defaults "
            "to the entry posture's dogfood/ci/general checks"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--apply-baseline",
        default=None,
        metavar="DEPLOYMENT",
        help=(
            "Apply the source-scoped active Nomos baseline to one deployment "
            "after reporting the inventory"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--restamp-publisher-derived",
        default=None,
        metavar="DEPLOYMENT",
        help=(
            "Repair legacy _publisher_derived graph rows by stamping explicit "
            "source-lineage attrs; publish after this command before cutover"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--repair-laundered-derived",
        default=None,
        metavar="DEPLOYMENT",
        help=(
            "Re-derive root lineage for derived rows that an older okg "
            "version wrote with lineage naming the EMITTING producer "
            "instead of the root source; rows whose lineage cannot be "
            "re-derived are reported, not guessed"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--repair-laundered-derived-dry-run",
        action="store_true",
        help=(
            "Report the laundered and unrepairable counts for "
            "--repair-laundered-derived without writing any row"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--restamp-active-policy-fingerprints",
        default=None,
        metavar="DEPLOYMENT",
        help=(
            "Append the active policy fingerprint to graph rows whose existing "
            "source-lineage attrs are valid but pinned to an older policy"
        ),
    )
    p_dep_nomos_lineage_rollout.add_argument(
        "--restamp-batch-size",
        type=int,
        default=5000,
        help=(
            "Maximum graph rows per active-policy restamp chunk; lower this "
            "to reduce write pressure during bounded audit cleanup"
        ),
    )
    add_dsn_arg(p_dep_nomos_lineage_rollout)

    p_dep_bug = p_dep_sub.add_parser(
        "bug-report",
        help="Preview or create a redacted GitHub issue for a deployment bug",
    )
    _add_json_flag(p_dep_bug)
    add_deployment_arg(p_dep_bug)
    add_dsn_arg(p_dep_bug)
    p_dep_bug.add_argument(
        "--issue-repo",
        "--repo",
        dest="repo",
        help="GitHub owner/name where the issue will be opened",
    )
    p_dep_bug.add_argument(
        "--deployment-repo",
        help=(
            "GitHub owner/name for the repository that owns the deployment; "
            "defaults to the issue repo"
        ),
    )
    p_dep_bug.add_argument("--title")
    p_dep_bug.add_argument("--summary")
    p_dep_bug.add_argument(
        "--body-file",
        help="Optional UTF-8 text file with reproduction notes or log excerpt",
    )
    p_dep_bug.add_argument(
        "--label",
        action="append",
        default=[],
        help="GitHub issue label; may be repeated",
    )
    p_dep_bug.add_argument(
        "--gh-binary",
        default="gh",
        help="GitHub CLI executable to use with --apply",
    )
    p_dep_bug.add_argument(
        "--preview-json-file",
        help="Reviewed preview JSON to apply exactly with --apply",
    )
    p_dep_bug.add_argument("--apply", action="store_true")

    # Issue #2562: every bootstrap step reads `--deployment` as a path
    # (`Path(args.deployment)`), not a name the manifest resolves.
    _BOOTSTRAP_DEPLOYMENT_HELP = (
        "Deployment directory (the folder that holds or will hold "
        "deployment.yaml), not a deployment name"
    )
    p_dep_boot = p_dep_sub.add_parser(
        "bootstrap",
        help="Plan, inspect, draft, verify, and apply a new deployment",
    )
    _add_json_flag(p_dep_boot)
    p_dep_boot_sub = p_dep_boot.add_subparsers(
        dest="bootstrap_subcmd",
        required=True,
    )

    p_boot_plan = p_dep_boot_sub.add_parser(
        "plan",
        help="Create or update bootstrap_plan.yaml from a domain brief",
    )
    _add_json_flag(p_boot_plan)
    add_deployment_arg(p_boot_plan, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)
    p_boot_plan.add_argument("--brief", required=True)
    p_boot_plan.add_argument("--name")
    p_boot_plan.add_argument(
        "--template-base",
        default="empty",
        help="Template base recorded in bootstrap_plan.yaml",
    )
    p_boot_plan.add_argument(
        "--dsn-env",
        default="OKG_DSN",
        help="Environment variable referenced by postgres.dsn",
    )
    p_boot_plan.add_argument(
        "--source",
        action="append",
        default=[],
        help="Source descriptor in id=path form; may be repeated",
    )
    p_boot_plan.add_argument("--apply", action="store_true")

    p_boot_inspect = p_dep_boot_sub.add_parser(
        "inspect-source",
        help="Inspect bounded local JSON/CSV/Markdown/directory samples",
    )
    _add_json_flag(p_boot_inspect)
    add_deployment_arg(p_boot_inspect, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)
    p_boot_inspect.add_argument(
        "source_id",
        nargs="?",
        help="Source id to inspect; omitted means all sources",
    )
    p_boot_inspect.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="Maximum sampled records per source",
    )
    p_boot_inspect.add_argument("--apply", action="store_true")

    p_boot_draft = p_dep_boot_sub.add_parser(
        "draft",
        help="Materialize staged deployment artifacts from the plan",
    )
    _add_json_flag(p_boot_draft)
    add_deployment_arg(p_boot_draft, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)
    p_boot_draft.add_argument("--apply", action="store_true")

    p_boot_verify = p_dep_boot_sub.add_parser(
        "verify",
        help="Validate staged artifacts and optionally run a real publish proof",
    )
    _add_json_flag(p_boot_verify)
    add_deployment_arg(p_boot_verify, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)
    add_dsn_arg(p_boot_verify)
    p_boot_verify.add_argument(
        "--structural-only",
        action="store_true",
        help="Run schema/source/catalog checks without marking the deployment verified",
    )
    p_boot_verify.add_argument("--apply", action="store_true")

    p_boot_apply = p_dep_boot_sub.add_parser(
        "apply",
        help=(
            "Promote verified staged artifacts into the deployment directory "
            "and hand the catalog over to it"
        ),
    )
    _add_json_flag(p_boot_apply)
    add_deployment_arg(p_boot_apply, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)
    add_dsn_arg(p_boot_apply)
    p_boot_apply.add_argument("--apply", action="store_true")

    p_boot_status = p_dep_boot_sub.add_parser(
        "status",
        help="Report the single next bootstrap action for agents",
    )
    _add_json_flag(p_boot_status)
    add_deployment_arg(p_boot_status, required=True, help=_BOOTSTRAP_DEPLOYMENT_HELP)

    # jobs — DBOS workflow status and source-sync enqueue surfaces
    p_jobs = sub.add_parser(
        "jobs",
        help="Inspect DBOS workflow state or enqueue source-sync work",
    )
    _add_json_flag(p_jobs)
    p_jobs_sub = p_jobs.add_subparsers(dest="jobs_subcmd", required=True)
    p_jobs_status = p_jobs_sub.add_parser(
        "status",
        help="Report DBOS workflow status and obsolete-schema cleanup state",
    )
    _add_json_flag(p_jobs_status)
    add_deployment_arg(p_jobs_status)
    add_dsn_arg(p_jobs_status)
    p_jobs_enqueue_source = p_jobs_sub.add_parser(
        "enqueue-source",
        help="Plan scoped source-sync work through the shared runtime",
    )
    _add_json_flag(p_jobs_enqueue_source)
    add_deployment_arg(p_jobs_enqueue_source)
    add_dsn_arg(p_jobs_enqueue_source)
    p_jobs_enqueue_source.add_argument("--runtime-deployment")
    p_jobs_enqueue_source.add_argument("--source", required=True)
    p_jobs_enqueue_source.add_argument("--scope-key")
    p_jobs_enqueue_source.add_argument("--run-mode", default="scope_complete")
    p_jobs_enqueue_source.add_argument("--dependent-fanout", default="closure")
    p_jobs_enqueue_source.add_argument("--run-key")
    p_jobs_enqueue_source.add_argument("--supersedes-workflow-id")
    p_jobs_enqueue_source.add_argument("--promotion-id", help=argparse.SUPPRESS)
    p_jobs_enqueue_source.add_argument("--no-publish", action="store_true")
    p_jobs_enqueue_source.add_argument(
        "--apply",
        action="store_true",
        help="Required to authorize workflow enqueueing",
    )
    p_jobs_find_source = p_jobs_sub.add_parser(
        "find-source-run",
        help="Resolve a source-sync workflow by its durable run key",
    )
    _add_json_flag(p_jobs_find_source)
    add_deployment_arg(p_jobs_find_source)
    add_dsn_arg(p_jobs_find_source)
    p_jobs_find_source.add_argument("--runtime-deployment")
    p_jobs_find_source.add_argument("--run-key", required=True)

    # chat — chat-surface operator verbs (PACT chat-source-admin)
    p_chat = sub.add_parser(
        "chat",
        help="Chat-surface source administration and recovery affordances",
    )
    _add_json_flag(p_chat)
    p_chat_sub = p_chat.add_subparsers(dest="chat_subcmd", required=True)
    p_chat_admin = p_chat_sub.add_parser(
        "source-admin",
        help=(
            "Read a write-enabled MCP endpoint back and report the admin "
            "preset that would be projected (refuses on an "
            "unauthenticated network endpoint)"
        ),
    )
    _add_json_flag(p_chat_admin)
    add_deployment_arg(p_chat_admin, required=True)
    p_chat_admin.add_argument(
        "--endpoint",
        help=(
            "streamable-HTTP MCP endpoint, e.g. "
            "http://127.0.0.1:8101/mcp. Defaults to the deployment's "
            "chat.mcp.port on --host"
        ),
    )
    p_chat_admin.add_argument(
        "--host",
        default="127.0.0.1",
        help=(
            "host the deployment's chat.mcp.port is served on when "
            "--endpoint is not given (default: 127.0.0.1)"
        ),
    )
    p_chat_admin.add_argument(
        "--auth-token-env",
        help=(
            "NAME of the environment variable holding the bearer token "
            "(never the token itself)"
        ),
    )
    p_chat_admin.add_argument(
        "--group",
        action="append",
        help="Open WebUI group granted the admin preset (repeatable)",
    )
    p_chat_admin.add_argument(
        "--tool",
        action="append",
        help=(
            "source-admin tool the preset names (repeatable); one absent "
            "from the live tools/list is an error"
        ),
    )
    p_chat_admin.add_argument(
        "--user-tool",
        action="append",
        help=(
            "tool the NON-admin preset names (repeatable); naming a "
            "source-admin tool here is an error"
        ),
    )
    p_chat_admin.add_argument("--preset-id")
    p_chat_retry = p_chat_sub.add_parser(
        "source-retry",
        help=(
            "Retry a source's failed ingest through the DLQ supersession "
            "path; exits non-zero unless the failure actually cleared"
        ),
    )
    _add_json_flag(p_chat_retry)
    add_deployment_arg(p_chat_retry, required=True)
    add_dsn_arg(p_chat_retry)
    p_chat_retry.add_argument("--source", required=True)
    p_chat_retry.add_argument(
        "--apply",
        action="store_true",
        help=(
            "run the retry chain; without it the command only prints the "
            "commands it would run"
        ),
    )
    p_chat_sso = p_chat_sub.add_parser(
        "sso-check",
        help=(
            "Decide the deployment's identity model (PACT "
            "chat-sso-integration): refuses on a half-declared identity "
            "provider, on skew against nomos.principal_mapping, and on the "
            "falsified console path-prefix topology"
        ),
    )
    _add_json_flag(p_chat_sso)
    add_deployment_arg(p_chat_sso, required=True)
    p_chat_sso.add_argument(
        "--render",
        choices=["nginx", "oauth2-proxy"],
        help=(
            "print the production front's configuration instead of the "
            "decision. nginx needs --chat-upstream and --console-upstream "
            "(operator-state topology, never manifest content)"
        ),
    )
    p_chat_sso.add_argument("--chat-upstream")
    p_chat_sso.add_argument("--console-upstream")
    p_chat_sso.add_argument(
        "--console-bundle-index",
        help=(
            "path to the built operator-console index.html. When the console "
            "is published under a path prefix, this MEASURES whether the "
            "bundle is base-aware instead of taking anyone's word for it"
        ),
    )
    p_chat_sso.add_argument(
        "--console-base-aware",
        action="store_true",
        help=(
            "declare that the console bundle is built for its path prefix "
            "(Vite base). Use when the bundle is not reachable from here; "
            "--console-bundle-index is preferred because it is measured"
        ),
    )
    p_chat_sync = p_chat_sub.add_parser(
        "sync",
        help=(
            "Render the deployment's declared chat: block into its Open "
            "WebUI instance and prove every write by readback (PACT "
            "chat-sync-projection). Needs OKG_CHAT_INSTANCE_URL and "
            "OKG_CHAT_ADMIN_TOKEN; exits non-zero on any surface that "
            "could not be applied or read back"
        ),
    )
    _add_json_flag(p_chat_sync)
    add_deployment_arg(p_chat_sync, required=True)
    p_chat_sync.add_argument(
        "--mcp-probe-url",
        help=(
            "streamable-HTTP endpoint THIS PROCESS dials for the /mcp answer "
            "check. Defaults to http://<--mcp-host>:<chat.mcp.port>/mcp"
        ),
    )
    p_chat_sync.add_argument(
        "--mcp-instance-url",
        help=(
            "the endpoint the INSTANCE is registered with. Often a different "
            "hostname than the probe URL (a container reaches a host loopback "
            "bind through host.docker.internal). Defaults to the probe URL"
        ),
    )
    p_chat_sync.add_argument(
        "--mcp-host",
        default="127.0.0.1",
        help=(
            "host the deployment's chat.mcp.port is served on when "
            "--mcp-probe-url is not given (default: 127.0.0.1)"
        ),
    )
    p_chat_sync.add_argument(
        "--plan",
        help=(
            "a chat-plan.json from `okg chat plan`. The run refuses BEFORE "
            "dialling the instance when the plan does not describe the "
            "deployment's current chat: block (chat_plan_drift)"
        ),
    )
    p_chat_setup = p_chat_sub.add_parser(
        "setup",
        help=(
            "One guided entry point: stand the pinned Open WebUI instance up, "
            "bootstrap or sign in to the admin account, register the declared "
            "model provider, connect the graph (starting mcp-serve when it is "
            "not answering), sync, and prove a conversation on the preset "
            "invokes the graph tools. Re-run the same command "
            "after fixing whatever it names. Secrets are read from the named "
            "environment variables and never printed"
        ),
    )
    _add_json_flag(p_chat_setup)
    add_deployment_arg(p_chat_setup, required=True)
    p_chat_setup.add_argument(
        "--host", default="127.0.0.1",
        help="host interface the instance publishes on (default: 127.0.0.1)",
    )
    p_chat_setup.add_argument(
        "--container-runtime", default="docker",
        help="container runtime binary: docker or podman (default: docker)",
    )
    p_chat_setup.add_argument(
        "--app-database-url-env", default="OKG_CHAT_APP_DATABASE_URL",
        help="NAME of the variable holding the app database DSN",
    )
    p_chat_setup.add_argument(
        "--admin-secret-env", default="OKG_CHAT_WEBUI_SECRET_KEY",
        help="NAME of the variable holding the instance session-signing key",
    )
    p_chat_setup.add_argument("--instance-port", type=int)
    p_chat_setup.add_argument("--image", default=None)
    p_chat_setup.add_argument(
        "--host-gateway-alias", default="host.docker.internal",
        help="hostname the container reaches the host's loopback on; '' disables",
    )
    from okg.substrate.chat.instance_cli import add_container_network_arg

    add_container_network_arg(p_chat_setup)
    p_chat_setup.add_argument("--ready-timeout", type=float, default=120.0)
    p_chat_setup.add_argument(
        "--admin-email",
        help=(
            "email of the admin account setup creates or signs in to "
            "(default: OKG_CHAT_ADMIN_EMAIL, else okg-admin@<deployment>.invalid)"
        ),
    )
    p_chat_setup.add_argument(
        "--question",
        help=(
            "the domain question the readiness conversation asks (default: the "
            "preset's first suggestion)"
        ),
    )
    p_chat_setup.add_argument(
        "--no-start-mcp-serve", action="store_true",
        help=(
            "refuse when mcp-serve is not answering instead of starting it as "
            "a detached child of this command"
        ),
    )
    p_chat_setup.add_argument("--mcp-ready-timeout", type=float, default=60.0)
    p_chat_setup.add_argument(
        "--open-browser", action="store_true",
        help="open the site on the preset in the default browser once ready",
    )
    p_chat_setup.add_argument("--mcp-probe-url")
    p_chat_setup.add_argument("--mcp-instance-url")
    p_chat_setup.add_argument("--mcp-host", default="127.0.0.1")
    p_chat_codex = p_chat_sub.add_parser(
        "codex-shim",
        help=(
            "Serve an OpenAI-compatible endpoint on 127.0.0.1 that answers "
            "each chat completion with one `codex exec --json` run of the "
            "official Codex CLI (the operator's ChatGPT sign-in), read-only "
            "sandbox, no shell, grounded through the named MCP server. "
            "Requires a bearer token from --token-env; runs until stopped"
        ),
    )
    _add_json_flag(p_chat_codex)
    p_chat_codex.add_argument(
        "--port", type=int, default=8790,
        help="loopback port to listen on (default: 8790); the bind host is always 127.0.0.1",
    )
    p_chat_codex.add_argument(
        "--token-env", default="OKG_CODEX_SHIM_TOKEN",
        help="NAME of the variable holding the bearer token callers must send",
    )
    p_chat_codex.add_argument(
        "--codex",
        help="path to the codex executable (default: `codex` on PATH)",
    )
    p_chat_codex.add_argument(
        "--mcp-url",
        help="streamable-HTTP MCP endpoint Codex may call, e.g. http://127.0.0.1:8766/mcp",
    )
    p_chat_codex.add_argument(
        "--mcp-token-env",
        help="NAME of the variable holding the MCP bearer token (passed to Codex by name)",
    )
    p_chat_codex.add_argument(
        "--mcp-tools",
        help=(
            "the deployment's declared chat.mcp.tools, comma separated; the shim "
            "refuses to start when the MCP server exposes any other tool"
        ),
    )
    p_chat_codex.add_argument(
        "--codex-home",
        help=(
            "a Codex home dedicated to the shim (no AGENTS.md, skills or "
            "config.toml), signed in once with `CODEX_HOME=<dir> codex login`"
        ),
    )
    p_chat_codex.add_argument(
        "--accept-codex-version",
        help="accept this exact untested Codex version knowingly",
    )
    p_chat_codex.add_argument(
        "--workdir",
        help=(
            "Codex working directory; must lie outside every git repository "
            "(default: a fresh temporary directory)"
        ),
    )
    p_chat_codex.add_argument("--run-timeout", type=float, default=600.0)
    p_chat_codex.add_argument("--max-concurrent", type=int, default=4)
    # Chat-block answers (issue #2288). Given on a deployment with no chat:
    # block, they WRITE one; given on a deployment with one, they must agree
    # with it. Credentials are named by variable, never passed as values.
    p_chat_setup.add_argument(
        "--provider",
        help=(
            "model provider for a new chat: block: openai, ollama or "
            "openai-compatible (the last needs --provider-base-url)"
        ),
    )
    p_chat_setup.add_argument(
        "--provider-base-url",
        help="OpenAI-compatible API base the chat instance calls",
    )
    p_chat_setup.add_argument(
        "--provider-key-env",
        help=(
            "NAME of the variable holding the provider key (default for "
            "openai: OPENAI_API_KEY). Only the name is written"
        ),
    )
    p_chat_setup.add_argument(
        "--model", help="model id the provider serves; the site's default model",
    )
    p_chat_setup.add_argument(
        "--mcp-port", type=int,
        help=(
            "port of the deployment's mcp-serve (default: the port okg "
            "install recorded)"
        ),
    )
    add_renamed_argument(
        p_chat_setup, "--scope", "--search-profile",
        dest="search_profile",
        help="declared search scope (search profile) that scopes chat answers",
    )
    p_chat_setup.add_argument(
        "--search-subtype", action="append",
        help=(
            "subtype a NEW search profile scopes chat to (repeatable; "
            "default: document and document_chunk)"
        ),
    )
    p_chat_setup.add_argument(
        "--configure-only", action="store_true",
        help=(
            "write or check the chat: block and stop; starts no container "
            "and contacts nothing"
        ),
    )
    p_chat_plan = p_chat_sub.add_parser(
        "plan",
        help=(
            "Write the manifest-derived desired state for all six chat "
            "surfaces to <out>/chat-plan.json, contacting NO instance (PACT "
            "chat-plan-apply-readback). Byte-identical on re-run; a "
            "pre-existing plan refuses"
        ),
    )
    _add_json_flag(p_chat_plan)
    add_deployment_arg(p_chat_plan, required=True)
    p_chat_plan.add_argument("--out", required=True, help="output directory")
    p_chat_readback = p_chat_sub.add_parser(
        "readback",
        help=(
            "Read the live instance and compare it against a plan, writing "
            "<out>/chat-readback.json. Writes nothing to the instance; exits "
            "non-zero with chat_readback_deviation on any deviation"
        ),
    )
    _add_json_flag(p_chat_readback)
    add_deployment_arg(p_chat_readback, required=True)
    p_chat_readback.add_argument("--plan", required=True, help="the chat-plan.json to compare against")
    p_chat_readback.add_argument("--out", required=True, help="output directory")
    for _p in (p_chat_plan, p_chat_readback):
        _p.add_argument("--mcp-probe-url")
        _p.add_argument("--mcp-instance-url")
        _p.add_argument("--mcp-host", default="127.0.0.1")
    # The same flag on all three verbs: the plan, the apply that is gated on
    # it and the readback that is compared against it must compose from ONE
    # bundle selection, and a flag present on some of them would make the
    # difference look like drift.
    for _p in (p_chat_sync, p_chat_plan, p_chat_readback, p_chat_setup):
        _p.add_argument(
            "--install-plan",
            help=(
                "directory holding the plan.json an install verb wrote next "
                "to its package. The selected bundle's chat "
                "contribution is composed with the deployment's chat: block "
                "under bundle-defaults/manifest-narrows precedence; a "
                "manifest that widens the bundle's connections or tools "
                "refuses (chat_bundle_manifest_widens)"
            ),
        )

    # chat-instance — the chat instance's own lifecycle (PACT
    # chat-instance-lifecycle). Separate from `chat`, whose verbs all render
    # INTO an instance that already exists; this one produces it.
    add_chat_instance_parser(
        sub, add_json_flag=_add_json_flag, add_deployment_arg=add_deployment_arg,
    )

    # perf (renamed from `profile` by decision 310). `okg profile` stays a
    # deprecated alias for one release: same parser, hidden from the verb
    # listing by `entry_roles.RENAMED_VERBS`, and it warns on stderr.
    for perf_verb in ("perf", "profile"):
        p_prof = sub.add_parser(
            perf_verb,
            help="Inspect and compare performance captures",
        )
        _add_json_flag(p_prof)
        p_prof_sub = p_prof.add_subparsers(dest="subcmd", required=True)
        p_prof_list = p_prof_sub.add_parser("list")
        _add_json_flag(p_prof_list)
        add_deployment_arg(p_prof_list)
        p_prof_list.add_argument("--limit", type=int, default=20)
        p_prof_show = p_prof_sub.add_parser("show")
        _add_json_flag(p_prof_show)
        p_prof_show.add_argument("profile_id")
        add_deployment_arg(p_prof_show)
        p_prof_cmp = p_prof_sub.add_parser("compare")
        _add_json_flag(p_prof_cmp)
        p_prof_cmp.add_argument("left")
        p_prof_cmp.add_argument("right")
        add_deployment_arg(p_prof_cmp)

    # mcp-serve
    p_mcp = sub.add_parser(
        "mcp-serve",
        help="Run the substrate MCP server (stdio by default, or streamable-http)",
    )
    add_dsn_arg(p_mcp)
    add_deployment_arg(p_mcp, default="default")
    p_mcp.add_argument(
        "--embedder",
        help=(
            "standalone --dsn semantic backend, or deprecated matching "
            "assertion for a resolved deployment's embeddings.backend: "
            "'hash' (tests/dev) or 'sentence-transformers:<model>'"
        ),
    )
    p_mcp.add_argument(
        "--branch",
        default="default",
        help=(
            "AKMON branch to pin reads to (default: 'default'). "
            "One MCP server = one branch; to read a quarantine or "
            "experiment branch, start a separate mcp-serve with "
            "--branch <name>."
        ),
    )
    p_mcp.add_argument(
        "--allow-writes",
        action="store_true",
        dest="allow_writes",
        help=(
            "Enable the explicit write/admin MCP surface (record, propose, "
            "source setup/admin, connector_ingest_batch) over a separate "
            "app_rw connection. Default off: the server exposes only the "
            "AKMON read algebra plus query and connects as the least-"
            "privilege okg_mcp role, so injected content cannot drive "
            "writes. Use for operator-supervised setup/curation."
        ),
    )
    add_renamed_argument(
        p_mcp, "--tools", "--tool-profile",
        dest="tool_profile",
        default=None,
        metavar="NAME",
        help=(
            "Serve only the tools in a named profile, bounding the startup "
            "cost every session pays. Built-ins: 'discovery' (inspect, "
            "search, aggregate — the minimal find-and-read set; no `expand`, "
            "so a chunk hit cannot be traversed to its parent), 'reader' "
            "(the full seven-tool read algebra), and the reserved 'full' "
            "(no narrowing). A deployment may declare more under "
            "`mcp.tool_profiles`. A profile only NARROWS: it never widens "
            "past what --allow-writes admits, and naming a tool this launch "
            "does not serve is refused rather than dropped. Switching "
            "profiles requires restarting the server; the tool list is "
            "fixed for a session's lifetime. Incompatible with "
            "OKG_MCP_TOOL_ALLOWLIST — supplying both is refused."
        ),
    )
    add_renamed_argument(
        p_mcp, "--tools-ceiling", "--tool-profile-ceiling",
        dest="tool_profile_ceiling",
        default=None,
        metavar="NAME",
        help=(
            "Let a session widen its own tool list, at most to this profile, "
            "without restarting the server. Each session still STARTS on "
            "--tools and widens only itself by calling "
            "inspect(target={'kind':'surface','action':'expand'}); the "
            "server then sends notifications/tools/list_changed to that one "
            "session, and advertises tools.listChanged only where this flag "
            "makes it true. Requires a narrowed launch, a ceiling that is "
            "strictly wider than it, and `inspect` in the base profile "
            "(it is the tool that widens); each is refused at launch rather "
            "than started as an expansion that can never happen."
        ),
    )
    p_mcp.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help=(
            "MCP transport. 'stdio' (default) speaks the protocol over "
            "stdin/stdout for `claude mcp add -- okg mcp-serve` style "
            "subprocess registrations. 'streamable-http' serves the protocol "
            "at http://<host>:<port>/mcp so clients that reach the server over "
            "the network (e.g. a chatbot in a container) can consume it."
        ),
    )
    p_mcp.add_argument(
        "--host",
        default="127.0.0.1",
        help=(
            "Bind address for --transport streamable-http (default 127.0.0.1). "
            "Use 0.0.0.0 to accept connections from other hosts/containers. "
            "Without --auth-token-env the MCP endpoint is unauthenticated: on "
            "a sensitive graph, front a non-loopback bind with a network "
            "control (firewall/proxy), and note that --allow-writes over a "
            "non-loopback host is refused. With --auth-token-env every request "
            "must present a matching bearer token, and --allow-writes over a "
            "non-loopback host is permitted. Ignored for stdio."
        ),
    )
    p_mcp.add_argument(
        "--port",
        type=int,
        default=8100,
        help=(
            "TCP port for --transport streamable-http (default 8100). "
            "Ignored for stdio."
        ),
    )
    p_mcp.add_argument(
        "--allowed-host",
        action="append",
        default=[],
        help=(
            "Permit one exact HTTP Host header or a host with wildcard port "
            "(for example service.namespace.svc:*). Repeat for each network "
            "name. DNS-rebinding protection remains enabled."
        ),
    )
    p_mcp.add_argument(
        "--auth-token-env",
        dest="auth_token_env",
        default=None,
        metavar="VARIABLE_NAME",
        help=(
            "Require a bearer token on --transport streamable-http. Takes the "
            "NAME of an environment variable; the token VALUE is read from "
            "that variable and never appears in argv. Generate at least 32 "
            "bytes of randomness (export OKG_MCP_AUTH_TOKEN=$(openssl rand "
            "-hex 32)) and pass --auth-token-env OKG_MCP_AUTH_TOKEN. Every "
            "request without a matching `Authorization: Bearer` header is "
            "refused with HTTP 401 before any tool runs, across the whole "
            "surface (initialize, tools, resources). If the named variable is "
            "unset or empty the server refuses to start rather than serving "
            "unauthenticated. This flag is the only way to turn "
            "authentication on. Incompatible with --transport stdio."
        ),
    )
    p_mcp.add_argument(
        "--multi-user-principals",
        dest="multi_user_principals",
        action="store_true",
        default=False,
        help=(
            "Resolve each request's own Nomos principal from the "
            "deployment's declared nomos.principal_mapping header, instead "
            "of serving every caller as one process-wide subject. Requires "
            "--transport streamable-http with --auth-token-env: the bearer "
            "token authenticates the fronting application, whose SSO then "
            "names the signed-in user in the declared header. A request "
            "whose principal header is missing, empty, malformed, or bound "
            "to no subject is REFUSED, never downgraded to a shared "
            "principal. Without this flag the server behaves exactly as "
            "before."
        ),
    )
    p_mcp.add_argument(
        "--oidc-auth",
        dest="oidc_auth",
        action="store_true",
        default=False,
        help=(
            "Authenticate each person with a bearer JWT issued by the "
            "deployment's identity provider, verified by this server against "
            "the provider's published keys (declared in nomos.oidc_auth). The "
            "verified subject and roles resolve a per-request Nomos principal: "
            "a request without a valid token is refused with HTTP 401, and one "
            "whose identity binds to no subject is refused. Requires "
            "--transport streamable-http. Incompatible with --auth-token-env, "
            "--multi-user-principals and the write surface. Without this flag "
            "the server behaves exactly as before."
        ),
    )
    p_mcp.add_argument(
        "--auth-token",
        dest="auth_token",
        default=None,
        metavar="REFUSED",
        help=(
            "Always refused. A literal token on the command line is readable "
            "by other local processes (ps) and lands in shell history. Use "
            "--auth-token-env VARIABLE_NAME instead."
        ),
    )

    # ui — PACT okg-ui-command: the built operator UI and its read API on one
    # loopback port, read-only. Implementation: okg.viz.ui_server.
    p_ui = sub.add_parser(
        "ui",
        help=(
            "Serve the operator UI for one deployment: the read API plus the "
            "built frontend on one port, read-only"
        ),
    )
    add_deployment_arg(
        p_ui,
        help=(
            "Deployment whose published graph the UI reads (default: "
            "$OKG_DEPLOYMENT; required)"
        ),
    )
    add_dsn_arg(
        p_ui,
        help=(
            "Postgres DSN for that deployment; normally resolved from "
            "--deployment like the other read commands"
        ),
    )
    p_ui.add_argument(
        "--generation",
        default=None,
        help=(
            "Generation checkpoint id or compatibility integer to pin "
            "(default: latest published)"
        ),
    )
    p_ui.add_argument(
        "--host",
        default=None,
        help=(
            "Bind host (default: $OKG_VIZ_HOST or 127.0.0.1). Loopback only: "
            "localhost, 127.0.0.0/8 or ::1; anything else is refused, so use "
            "an SSH tunnel for remote access"
        ),
    )
    p_ui.add_argument(
        "--port",
        type=int,
        default=None,
        help="Bind port, 1-65535 (default: $OKG_VIZ_PORT or 8765)",
    )
    p_ui.add_argument(
        "--auth-token-env",
        dest="auth_token_env",
        default=None,
        metavar="VARIABLE_NAME",
        help=(
            "Require a token on every request, read from this environment "
            "variable (pass the NAME, not the value). The browser asks for it "
            "once: any user name, the token as the password. An unset, empty "
            "or padded variable refuses to start"
        ),
    )
    p_ui.add_argument(
        "--dist",
        default=None,
        metavar="DIR",
        help=(
            "Built frontend bundle directory (default: $OKG_OPERATOR_UI_DIST, "
            "else .tmp/operator-ui-dist in this source checkout). Build it "
            "with `npm run operator-ui:build`; a missing bundle is refused"
        ),
    )

    # agent-ssh-run — the only command installed as the coordination
    # transport's sshd ForceCommand.  Keep this grammar deliberately narrow:
    # the route is an opaque server lookup key and the event request arrives
    # as bounded JSON on stdin.  There is no deployment, DSN, identity, scope,
    # lease, or caller-time option here.
    p_agent_ssh = sub.add_parser(
        "agent-ssh-run",
        help="Process one restricted coordination event from an sshd route",
    )
    p_agent_ssh.add_argument(
        "--route",
        required=True,
        help="opaque server-owned coordination route",
    )

    # agent-ssh-client — the local MCP bridge.  It intentionally has no
    # options: route/workspace metadata comes from the assignment-local MCP
    # environment, while every host, identity, and SSH option comes from the
    # fixed operator-managed client registry.
    sub.add_parser(
        "agent-ssh-client",
        help="Run the restricted coordination SSH client MCP bridge",
    )

    # agent-runtime-launch
    p_agent_rt = sub.add_parser(
        "agent-runtime-launch",
        help="Start a child command through the profiled agent runtime boundary",
    )
    _add_json_flag(p_agent_rt)
    p_agent_rt.add_argument("--runtime", required=True)
    add_renamed_argument(
        p_agent_rt, "--permissions", "--profile",
        dest="profile",
        default="dev",
        choices=["read-only", "dev", "deploy", "security-audit"],
        help="Permission set for the agent run (default: dev)",
    )
    add_deployment_arg(p_agent_rt, default="default")
    p_agent_rt.add_argument(
        "--subject",
        dest="subject_binding_id",
        help="Nomos subject binding for this agent run",
    )
    p_agent_rt.add_argument(
        "--workspace-root",
        default=str(Path.cwd()),
        help="Workspace root exposed to the profiled agent run",
    )
    p_agent_rt.add_argument(
        "--deployments-dir",
        help=(
            "Absolute deployments folder the child resolves --deployment in "
            "(set as OKG_DEPLOYMENTS_DIR for the child only)"
        ),
    )
    p_agent_rt.add_argument(
        "--local-env",
        help=(
            "Absolute path of the deployment's .okg/local.env "
            "(set as OKG_LOCAL_ENV for the child only)"
        ),
    )
    p_agent_rt.add_argument(
        "--role-secrets-file",
        help=(
            "Absolute path of the role-password file "
            "(set as OKG_ROLE_SECRETS_FILE for the child only)"
        ),
    )
    p_agent_rt.add_argument(
        "--allow-builtin-role-passwords",
        action="store_true",
        help=(
            "Let the child use the public built-in role passwords "
            "(written by `okg agent connect` only for a fresh install, "
            "with a warning)"
        ),
    )
    p_agent_rt.add_argument("--operator")
    p_agent_rt.add_argument("--run-id")
    p_agent_rt.add_argument(
        "--break-glass-grant-json",
        help="Deprecated; inline break-glass grants are rejected by this CLI",
    )
    p_agent_rt.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print the launch plan instead of execing",
    )
    p_agent_rt.add_argument("child_command", nargs=argparse.REMAINDER)

    # agent — connect agents (Claude Code, Claude Desktop, Cursor, Codex) to
    # a deployment (PACT agent-connect-3412).
    p_agent = sub.add_parser(
        "agent",
        help="Connect Claude Code, Claude Desktop, Cursor or Codex to a deployment",
    )
    _add_json_flag(p_agent)
    p_agent_sub = p_agent.add_subparsers(dest="agent_subcmd", required=True)
    from okg.substrate.library.agent_runtimes.connect import CONNECT_AGENTS

    p_agent_connect = p_agent_sub.add_parser(
        "connect",
        help=(
            "Register the deployment's MCP server and install the okg skills "
            "for each agent"
        ),
    )
    _add_json_flag(p_agent_connect)
    add_deployment_arg(p_agent_connect, required=True)
    p_agent_connect.add_argument(
        "--agent", action="append", dest="agents", choices=CONNECT_AGENTS,
        help="Agent to connect (repeatable; default: every agent present)",
    )
    p_agent_connect.add_argument(
        "--writes", action="store_true",
        help="Register the write-enabled server and the okg-record skill",
    )
    p_agent_connect.add_argument(
        "--dry-run", action="store_true",
        help="Print each config file, entry and skill folder; write nothing",
    )
    p_agent_connect.add_argument(
        "--replace", action="store_true",
        help="Remove another entry that serves the same deployment",
    )
    p_agent_connect.add_argument(
        "--probe-timeout", type=float, default=120.0,
        help="Seconds to wait for the server check (default: 120)",
    )
    p_agent_disconnect = p_agent_sub.add_parser(
        "disconnect",
        help="Remove the deployment's entry and the skills connect installed",
    )
    _add_json_flag(p_agent_disconnect)
    add_deployment_arg(p_agent_disconnect, required=True)
    p_agent_disconnect.add_argument(
        "--agent", action="append", dest="agents", choices=CONNECT_AGENTS,
        help="Agent to disconnect (repeatable; default: every agent present)",
    )
    p_agent_disconnect.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be removed; write nothing",
    )

    # runtime — DBOS runtime layer (rebuild-substrate-on-dbos)
    p_rt = sub.add_parser(
        "runtime",
        help="DBOS runtime layer: bootstrap, worker, schedules, reset",
    )
    _add_json_flag(p_rt)
    p_rt_sub = p_rt.add_subparsers(dest="rt_subcmd", required=True)

    def _add_rt_common(parser: argparse.ArgumentParser) -> None:
        """Add the flags every runtime subcommand accepts.

        Per Phase 1 §2.10 review (SIGNIFICANT recommendation C): all
        runtime subcommands accept ``--dsn`` so an operator can target
        an environment that differs from the manifest-resolved DSN
        (e.g. a paused replica, a per-incident hot fix). Without this,
        only ``bootstrap`` had ``--dsn`` and the dispatcher's
        ``hasattr(args, "dsn")`` fallback was effectively dead code.
        """
        _add_json_flag(parser)
        add_deployment_arg(parser, required=True)
        parser.add_argument(
            "--dsn",
            default=None,
            help=(
                "Override the deployment's resolved DSN (defaults to "
                "the manifest-resolved DSN). Useful for targeting a "
                "replica or running against an alternate database."
            ),
        )

    p_rt_boot = p_rt_sub.add_parser(
        "bootstrap",
        help=(
            "Install DBOS schema + role split for a deployment "
            "(idempotent)"
        ),
    )
    _add_rt_common(p_rt_boot)

    p_rt_smoke = p_rt_sub.add_parser(
        "smoke",
        help=(
            "Run a DBOS-runtime smoke test (LISTEN/NOTIFY round-trip)"
        ),
    )
    _add_rt_common(p_rt_smoke)

    p_rt_worker = p_rt_sub.add_parser(
        "worker",
        help=(
            "Run a DBOS worker process for a deployment"
        ),
    )
    _add_rt_common(p_rt_worker)
    # Replayed from dev (commit 7fe9443e): operator mode for drain-and-exit.
    p_rt_worker.add_argument(
        "--operator",
        action="store_true",
        help=(
            "Run short-lived operator mode: suppress recurring schedules "
            "and supervised services, drain queued deployment work, and exit"
        ),
    )
    p_rt_worker.add_argument(
        "--max-wait-seconds",
        type=float,
        default=600.0,
        help="Operator mode drain timeout (default 600s)",
    )

    p_rt_reset = p_rt_sub.add_parser(
        "reset",
        help=(
            "Reset the deployment's DBOS state (dev only; refused "
            "without --confirm)"
        ),
    )
    _add_rt_common(p_rt_reset)
    p_rt_reset.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm destructive reset",
    )

    p_rt_ls = p_rt_sub.add_parser(
        "list-schedules",
        help="List DBOS scheduled workflows registered for a deployment",
    )
    _add_rt_common(p_rt_ls)

    p_rt_trig = p_rt_sub.add_parser(
        "trigger-schedule",
        help="Trigger a DBOS scheduled workflow once (out-of-cycle)",
    )
    _add_rt_common(p_rt_trig)
    p_rt_trig.add_argument("schedule_id")

    p_rt_fenced_recovery = p_rt_sub.add_parser(
        "enqueue-fenced-recovery",
        help=(
            "Enqueue one allowlisted runtime-DLQ recovery witness under an "
            "active promotion owner"
        ),
    )
    _add_rt_common(p_rt_fenced_recovery)
    p_rt_fenced_recovery.add_argument(
        "workflow_name",
        choices=(
            "doctor_workflow",
            "okg_ci_git_history_reconcile",
            "okg_ci_resource_monitor",
        ),
    )
    p_rt_fenced_recovery.add_argument("--promotion-id", required=True)
    p_rt_fenced_recovery.add_argument("--deduplication-id")

    p_rt_pause = p_rt_sub.add_parser(
        "pause-schedule",
        help="Pause a DBOS scheduled workflow before draining queued work",
    )
    _add_rt_common(p_rt_pause)
    p_rt_pause.add_argument("schedule_id")

    p_rt_resume = p_rt_sub.add_parser(
        "resume-schedule",
        help="Resume a paused DBOS scheduled workflow",
    )
    _add_rt_common(p_rt_resume)
    p_rt_resume.add_argument("schedule_id")

    p_rt_event = p_rt_sub.add_parser(
        "get-event",
        help="Read a workflow event by (workflow_id, key)",
    )
    _add_rt_common(p_rt_event)
    p_rt_event.add_argument("workflow_id")
    p_rt_event.add_argument("key")

    p_rt_measure = p_rt_sub.add_parser(
        "measure-recovery",
        help=(
            "Measure DBOS worker-recovery time (Decision 5 gate)"
        ),
    )
    _add_rt_common(p_rt_measure)
    p_rt_measure.add_argument(
        "--mode",
        choices=["kill9", "sigstop", "split-brain"],
        default="kill9",
    )

    p_rt_debug = p_rt_sub.add_parser(
        "debug-workflow",
        help="Print a structured debug report for a DBOS workflow",
    )
    _add_rt_common(p_rt_debug)
    p_rt_debug.add_argument("workflow_id")

    p_rt_cancel_wf = p_rt_sub.add_parser(
        "cancel-workflow",
        help=(
            "Cancel one stuck ENQUEUED/PENDING/DELAYED DBOS workflow so it "
            "stops holding the deployment queue slot"
        ),
    )
    _add_rt_common(p_rt_cancel_wf)
    p_rt_cancel_wf.add_argument("workflow_id")

    p_rt_supersede_ckpt = p_rt_sub.add_parser(
        "supersede-checkpoint",
        help=(
            "Retire an incomplete checkpoint's exact repair contract, with a "
            "recorded reason, so other workflows stop refusing"
        ),
    )
    _add_rt_common(p_rt_supersede_ckpt)
    p_rt_supersede_ckpt.add_argument(
        "--reason",
        required=True,
        help="Why the repair contract is being retired. Recorded on the "
             "checkpoint.",
    )
    p_rt_supersede_ckpt.add_argument(
        "--actor",
        default="operator",
        help="Who is retiring it (default: operator)",
    )
    p_rt_supersede_ckpt.add_argument(
        "--checkpoint-id",
        default=None,
        help="Refuse unless this is the deployment's latest checkpoint",
    )
    p_rt_supersede_ckpt.add_argument(
        "--apply",
        action="store_true",
        help="Perform the write. Without it, print the plan and change "
             "nothing.",
    )

    p_rt_deferred = p_rt_sub.add_parser(
        "deferred-edges",
        help=(
            "List cross-source edges waiting for another source's record, "
            "by producer, edge type, endpoint subtype and state, with the "
            "oldest age and a bounded operator-only id sample"
        ),
    )
    _add_rt_common(p_rt_deferred)

    p_rt_finish_undo = p_rt_sub.add_parser(
        "finish-undo",
        help=(
            "Retry, or with --accept-current retire, a failed source run's "
            "unfinished undo, which blocks every publish"
        ),
    )
    _add_rt_common(p_rt_finish_undo)
    p_rt_finish_undo.add_argument(
        "--run-id",
        default=None,
        help="Only this run (default: every unfinished undo)",
    )
    p_rt_finish_undo.add_argument(
        "--accept-current",
        action="store_true",
        help="Write nothing to the graph: keep the rows as they are now, "
             "mark the runs' receipts compensated and drop their run-base "
             "checkpoints. Publishing stays refused (source_rerun_required) "
             "until the source's next run succeeds or the source is retired "
             "with `okg remediate retire-source`.",
    )
    p_rt_finish_undo.add_argument(
        "--reason",
        default=None,
        help="Why the current rows are kept (required with --accept-current)",
    )
    p_rt_finish_undo.add_argument(
        "--actor",
        default="operator",
        help="Who is resolving it (default: operator)",
    )
    p_rt_finish_undo.add_argument(
        "--apply",
        action="store_true",
        help="Perform the undo or the retirement. Without it, list the "
             "unfinished undos and change nothing.",
    )

    p_rt_drain = p_rt_sub.add_parser(
        "drain",
        help=(
            "Wait for in-flight DBOS workflows on the deployment's "
            "queue to finish (used before worker restart / deploy)"
        ),
    )
    _add_rt_common(p_rt_drain)
    p_rt_drain.add_argument(
        "--max-wait-seconds",
        type=float,
        default=600.0,
        help="Timeout (default 600s)",
    )

    p_rt_quiesce = p_rt_sub.add_parser(
        "quiesce",
        help="Fence new runtime work and drain queues before deployment",
    )
    _add_rt_common(p_rt_quiesce)
    p_rt_quiesce.add_argument("--owner", required=True)
    p_rt_quiesce.add_argument("--candidate-revision", required=True)
    p_rt_quiesce.add_argument("--lifecycle-plan-digest", required=True)
    p_rt_quiesce.add_argument("--lifecycle-contract-digest", required=True)
    p_rt_quiesce.add_argument(
        "--max-wait-seconds",
        type=float,
        default=600.0,
    )
    p_rt_quiesce.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
    )
    p_rt_quiesce.add_argument(
        "--stable-zero-seconds",
        type=float,
        default=31.0,
    )
    p_rt_quiesce.add_argument("--bootstrap-quiescence", action="store_true")
    p_rt_quiesce.add_argument("--receipt")

    p_rt_fence_status = p_rt_sub.add_parser(
        "fence-status",
        help=argparse.SUPPRESS,
    )
    _add_rt_common(p_rt_fence_status)

    p_rt_openshell = p_rt_sub.add_parser(
        "openshell-policy",
        help="Generate an OpenShell sandbox policy from deployment config",
    )
    _add_rt_common(p_rt_openshell)
    p_rt_openshell.add_argument(
        "--output",
        default=None,
        help="Write the generated YAML policy to this path",
    )

    # Phase 2 §1.4.2: nested `escalation` subgroup. Currently exposes
    # `respond`; subsequent phases will add `list` and `show`.
    p_rt_esc = p_rt_sub.add_parser(
        "escalation",
        help=(
            "Operator response surface for SDK escalations "
            "(respond, list, show)"
        ),
    )
    _add_json_flag(p_rt_esc)
    add_deployment_arg(p_rt_esc, required=True)
    p_rt_esc.add_argument(
        "--dsn",
        default=None,
        help=(
            "Override the deployment's resolved DSN (defaults to "
            "the manifest-resolved DSN)."
        ),
    )
    p_rt_esc_sub = p_rt_esc.add_subparsers(dest="esc_subcmd", required=True)

    p_rt_esc_respond = p_rt_esc_sub.add_parser(
        "respond",
        help=(
            "Respond to a pending escalation with a decision JSON "
            "payload; validates against the escalation's "
            "decision_schema_ref and wakes the requesting workflow."
        ),
    )
    _add_json_flag(p_rt_esc_respond)
    p_rt_esc_respond.add_argument("escalation_id")
    p_rt_esc_respond.add_argument(
        "--decision",
        required=True,
        help=(
            "Operator decision as a JSON object. Example: "
            "--decision '{\"label\": \"stale\", \"rationale\": \"...\"}'"
        ),
    )
    p_rt_esc_respond.add_argument(
        "--decided-by",
        default=None,
        help=(
            "Operator identifier (default: cli:$USER). Stamped into "
            "the escalation's `decided_by` field for audit."
        ),
    )
    p_rt_esc_respond.add_argument(
        "--no-send-wake",
        action="store_true",
        help=(
            "Skip the DBOSClient.send wake-up call. Useful in tests "
            "and for manual operator-driven dry-runs."
        ),
    )

    # lint — Phase 2 §6.3 boundary enforcement
    p_lint = sub.add_parser(
        "lint",
        help=(
            "Boundary / cross-cutting lints. `lint deployments` walks "
            "every deployment manifest and checks runtime.workflows "
            "modules for substrate-boundary violations."
        ),
    )
    _add_json_flag(p_lint)
    p_lint_sub = p_lint.add_subparsers(
        dest="lint_target", required=True,
    )
    p_lint_dep = p_lint_sub.add_parser(
        "deployments",
        help=(
            "Walk deployment manifests and check runtime.workflows "
            "Python modules for imports of okg.substrate.*. Exits "
            "non-zero on any violation (DEPLOYMENT-001)."
        ),
    )
    _add_json_flag(p_lint_dep)
    p_lint_dep.add_argument(
        "--deployments-root",
        default=None,
        help=(
            "Override the deployments directory (default: "
            "'./deployments')."
        ),
    )

    # init — two modes:
    #   --template <name>  legacy library-template scaffold
    #   --profile <name>   Layer 2A profile-driven scaffold
    p_init = sub.add_parser(
        "init",
        help=(
            "Scaffold a deployment from a profile or library template "
            "(same as `okg install ... --scaffold-only`)"
        ),
    )
    _add_json_flag(p_init)
    p_init.add_argument(
        "--profile",
        dest="profile_name",
        help=(
            "profile name to scaffold from (Layer 2A; reads "
            "<profiles_root>/<name>/profile.yaml)"
        ),
    )
    _add_scaffold_arguments(p_init)

    # new-module (Layer 2A) — scaffold an ontology module
    # skeleton in-tree or under a deployment.
    p_new_mod = sub.add_parser(
        "new-module",
        help=(
            "Scaffold a new ontology module skeleton "
            "(module.yaml, schema.yaml, tests/, README.md)"
        ),
    )
    _add_json_flag(p_new_mod)
    p_new_mod.add_argument(
        "name",
        help="module name (snake_case, e.g. wet_lab)",
    )
    p_new_mod_loc = p_new_mod.add_mutually_exclusive_group()
    p_new_mod_loc.add_argument(
        "--in-tree",
        action="store_true",
        help=(
            "Place the module under "
            "src/okg/substrate/library/ontologies/<name>/ "
            "(default when neither --in-tree nor --deployment "
            "is supplied)"
        ),
    )
    p_new_mod_loc.add_argument(
        "--deployment",
        help=(
            "Place the module under <deployment folder>/modules/<name>/ "
            "instead of in-tree (a deployment name, folder or "
            "deployment.yaml path)"
        ),
    )
    p_new_mod.add_argument(
        "--deployments-root",
        help=(
            "Look the deployment up under this folder (default: where "
            "other okg deployment commands find it)"
        ),
    )
    p_new_mod.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing module dir",
    )

    # new-source — RETIRED (issue #2479). Its reader skeleton was not a
    # working reader. The verb stays registered so an old script gets a
    # typed refusal naming the replacement, but `entry_roles.RETIRED_VERBS`
    # keeps it out of `okg --help`. `_swallow_retired_verb_args` accepts
    # any argument list, so every old invocation reaches that refusal
    # instead of an argparse usage error; only `--json` and `--help` are
    # read.
    p_new_src = sub.add_parser(
        "new-source",
        help="Retired; refuses and names the example connector to copy",
        # Raw, hand-wrapped: the default formatter would split the example
        # path at its hyphens.
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Retired: this command wrote a reader skeleton\n"
            "that was not a working reader. It now refuses and writes\n"
            "nothing. Copy the working connector in\n"
            "examples/json-record-connector/ instead and add it with\n"
            "`okg deployment add-source <deployment> <name>\n"
            "--registration <dir>/connector.json`."
        ),
    )
    _add_json_flag(p_new_src)
    _swallow_retired_verb_args(p_new_src)

    # library
    from okg.substrate.library.resolver import KINDS as library_ref_kinds

    p_lib = sub.add_parser(
        "library",
        help=(
            "List or show substrate library refs "
            f"({', '.join(library_ref_kinds)})"
        ),
    )
    _add_json_flag(p_lib)
    p_lib_sub = p_lib.add_subparsers(dest="subcmd", required=True)
    p_lib_list = p_lib_sub.add_parser("list")
    p_lib_list.add_argument(
        "--kind",
        choices=library_ref_kinds,
    )
    p_lib_show = p_lib_sub.add_parser("show")
    p_lib_show.add_argument("ref", help="e.g. okg.modules.openspec")

    # trace
    p_trace = sub.add_parser(
        "trace", help="Dump fact log + live projection for a node",
    )
    _add_json_flag(p_trace)
    p_trace_sub = p_trace.add_subparsers(dest="subcmd", required=True)
    p_trace_node = p_trace_sub.add_parser("node")
    p_trace_node.add_argument("node_id")
    add_deployment_arg(p_trace_node, required=False)
    p_trace_node.add_argument("--branch", default="default")
    add_dsn_arg(p_trace_node)
    p_trace_node.add_argument(
        "--generation-id", "--generation",
        dest="generation_id",
        default=None,
        help="pin the Chronos graph preview to a published generation id",
    )

    register_read_admin_parsers(
        sub,
        add_json_flag=_add_json_flag,
        add_owner_flags=_add_owner_flags,
    )

    # Per define-memex-deployment-contract R4: read-side
    # subcommands (search, get-node, list-neighbors,
    # traverse-path) live in cli/_read_side.py.
    from okg.substrate.cli import _read_side
    _read_side.register_subparsers(sub, add_json_flag=_add_json_flag)
    # PACT read-commands-help-3666: name the read commands and `okg guide`
    # once under the listing (the help note on hidden verbs follows it).
    parser.epilog = _read_side._read_operators.READ_COMMANDS_EPILOG

    from okg.substrate.pact.cli import register_subparser as register_pact_cli
    register_pact_cli(sub, _add_json_flag, argv=argv)

    # Per `extend-substrate-experimental-with-compute-job-and-sources`
    # Phase 7: `experiment status` + `import-cluster-jobs` live in
    # cli/experiment.py and register themselves the same way.

    # Per `add-substrate-bibliographic-sources` Phase 6: `okg bib`
    # subcommand group for tier promotion + extraction triggering.

    # Profile-shipped CLI subcommands. Each profile under
    # `library/profiles/<name>/cli/__init__.py` exports
    # `register_subparsers(sub)` + `DISPATCH`; the loader merges
    # them in. Per `retire-pi-prefix-commands`: substrate's
    # dispatcher SHALL NOT name any specific profile.
    from okg.substrate.cli.profile_cli_loader import (
        register_all_profile_clis,
    )
    from okg.substrate.cli import dogfood as dogfood_cli
    from okg.substrate.cli import federation as federation_cli
    from okg.substrate.cli import nomos as nomos_cli
    from okg.substrate.cli import poros as poros_cli
    dogfood_cli.register_subparsers(sub, add_json_flag=_add_json_flag)
    federation_cli.register_subparsers(sub, add_json_flag=_add_json_flag)
    nomos_cli.register_subparsers(sub, add_json_flag=_add_json_flag)
    poros_cli.register_subparsers(sub, add_json_flag=_add_json_flag)

    # `install` is the one documented way in, so the SUBSTRATE owns the verb
    # (define-newcomer-entry-path task.entry.promote-install-to-substrate).
    # Profiles opt in via INSTALL_EXTENSION; nothing here names a profile.
    _build_install_parser(sub)

    # Stash the loader's dispatch so `main()` can merge it without
    # re-walking the filesystem.
    from okg.substrate.distributions.install_execution import active_bootstrap
    parser._okg_profile_dispatch = (
        {} if active_bootstrap() is not None else register_all_profile_clis(sub)
    )

    # After every registration (incl. profile-shipped verbs): wrap the whole
    # tree so leaf subparsers also print the one-line actionable correction
    # (reduce-agent-friction req.friction.actionable-cli-errors). Per-family
    # installs above win their example; everything else inherits it.
    from okg.substrate.cli._options import install_actionable_errors_tree

    install_actionable_errors_tree(parser, "okg <command> --help")

    # Order and annotate the top-level help by entry-surface role, AFTER
    # profile-shipped verbs are registered so they are covered too
    # (define-newcomer-entry-path req.entry.role-in-existing-registry).
    # This rewrites only the help pseudo-actions; dispatch stays keyed on
    # `action.choices`, so ordering can never change what a command does.
    _apply_entry_roles_to_help(sub, parser)

    return parser


#: The profile `okg install` uses when none is named. Declared as data
#: rather than imported, so the substrate never references a profile module.
#: Issue #3414 (Jason 2026-10-05, `default A`): the default is the empty
#: `minimal` starter a team grows with its own sources, not a domain profile.
DEFAULT_INSTALL_PROFILE = "minimal"



def _add_scaffold_arguments(p: argparse.ArgumentParser) -> None:
    """Add the deployment-scaffold flags shared by `init` and `install`.

    Extracted so the substrate-owned `install` verb can accept the same
    scaffold options `init` has always taken, per PACT
    `define-newcomer-entry-path`: `init` becomes an alias, so it cannot
    offer flags the door does not.

    `--profile` and `--json` are deliberately excluded — each caller owns
    them, because `init` binds --profile to `profile_name` while the
    install parser binds it to `profile`.
    """
    p.add_argument(
        "--template",
        help=(
            "library template name (legacy, e.g. devops, "
            "docs-corpus). Use --profile <name> for new "
            "deployments."
        ),
    )
    p.add_argument(
        "--target",
        help="dir to scaffold into (default: ./<template>)",
    )
    p.add_argument(
        "--repo-root",
        help=(
            "repository root for templates that scan a local codebase "
            "(required when the template's template_meta.yaml sets "
            "`requires_repo_context: true`)"
        ),
    )
    p.add_argument(
        "--openspec-root",
        help=(
            "OpenSpec root for repo-context templates "
            "(default: <repo-root>/openspec)"
        ),
    )
    p.add_argument(
        "--claude-projects-root",
        help=(
            "Claude projects root for repo-context templates "
            "(default: ~/.claude/projects)"
        ),
    )
    p.add_argument(
        "--deployment-name",
        help=(
            "deployment name (for --profile this is the "
            "deployments/<deployment_name>/ dir; for repo-context "
            "templates it defaults to the repo basename)"
        ),
    )
    p.add_argument(
        "--since",
        help=(
            "lookback window for repo-context templates "
            "(default: 90d)"
        ),
    )
    p.add_argument(
        "--notes-root",
        help="optional notes corpus root for repo-context templates",
    )
    # Layer 2A flags
    p.add_argument(
        "--deployments-root",
        help=(
            "Override the folder new deployments are written to "
            "(default: $OKG_DEPLOYMENTS_DIR when set, else "
            "$XDG_DATA_HOME/okg/deployments, i.e. "
            "~/.local/share/okg/deployments)"
        ),
    )
    p.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite an existing deployment dir (requires "
            "--yes for safety)"
        ),
    )
    p.add_argument(
        "--yes",
        action="store_true",
        help="Double-acknowledge --force; required to clobber existing files",
    )
    p.add_argument(
        "--no-publish",
        action="store_true",
        help=(
            "Skip the post-scaffold migrate + first publish "
            "(useful when Postgres isn't reachable or for tests)"
        ),
    )
    p.add_argument(
        "--non-interactive",
        action="store_true",
        help=(
            "Refuse to prompt for missing init answers; raise "
            "ProfileError instead"
        ),
    )
    # Substrate-owned, for the same reason `--json` is (see
    # `_build_install_parser`): every installable profile contributes
    # arguments to ONE parser, so a flag more than one profile wants
    # has to be owned centrally or the second contributor collides.
    # It also has to exist whether or not any profile extension was
    # discovered — a promise that appears only when some particular
    # profile happens to be installed is not a promise. Per issue #946.
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what the install would do and exit without writing "
            "anything. Reports the deployment directory, the exact "
            "files that would be created, and whether an existing "
            "deployment would be refused or overwritten. Profiles with "
            "their own installer additionally probe Docker, image "
            "presence, ports, and agent-runtime config collisions. "
            "Creates no file or directory, starts no container, and "
            "runs no schema migration or publish."
        ),
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "Print everything the install did: every file written, the "
            "module composition, the first-publish proof, and each warning, "
            "instead of the short summary. `--json` prints the same "
            "detail as one JSON document."
        ),
    )


def _build_install_parser(sub: argparse._SubParsersAction) -> None:
    """Register the substrate-owned `install` verb.

    One verb, many installable profiles. Each profile exporting an
    `INSTALL_EXTENSION` contributes its own flags to this parser, and
    `--profile` selects which one runs. This is what makes the gallery's
    advertised `okg install --profile cms` a real command instead of an
    `unrecognized arguments` error.
    """
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    from okg.substrate.distributions.install_execution import active_bootstrap
    bootstrapped = active_bootstrap() is not None
    extensions = {} if bootstrapped else discover_install_extensions()
    installable = [] if bootstrapped else _installable_profile_lines(extensions)
    p = sub.add_parser(
        "install",
        help=(
            "Set up a graph on this machine — the one documented way in. "
            "`--profile` picks what to build; with none, it installs the "
            f"empty `{DEFAULT_INSTALL_PROFILE}` starter that you grow with "
            "`okg deployment add-source`."
        ),
        description=(
            textwrap.fill(
                "Set up a graph on this machine. `--profile` picks what to "
                f"build; with none, it installs `{DEFAULT_INSTALL_PROFILE}`.",
                width=78,
            )
            + "\n\nInstallable profiles:\n"
            + ("\n".join(installable) or "  (none discovered)")
            + "\n\n"
            + textwrap.fill(
                "Each profile's setup questions are flags too: `okg install "
                "--profile <name> --dry-run --json` names the ones a profile "
                "needs.",
                width=78,
                break_on_hyphens=False,
            )
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--profile",
        default=None,
        help=(
            "What to install: one of the profiles listed above. Default: "
            f"{DEFAULT_INSTALL_PROFILE}. Reference deployments fetched with "
            "`okg fetch-deployments` are also accepted by name."
        ),
    )
    for name in sorted(extensions):
        extensions[name].add_arguments(p)
    # A profile without its own installer is still installable: the generic
    # scaffold path serves it, so `install` offers the same flags `init`
    # takes. Without these, `init` could not become an alias — it would
    # accept options the door does not.
    _add_scaffold_arguments(p)
    p.add_argument(
        "--scaffold-only",
        action="store_true",
        help=(
            "Write and build the deployment from the profile, skipping a "
            "profile's own installer (Docker provisioning, wizards). This "
            "is what `okg init` has always done, and what `okg init` now "
            "delegates to."
        ),
    )
    # Matches how profile-owned verbs declare it; SUPPRESS keeps the flag
    # out of the namespace unless passed, which the profile handlers rely on.
    p.add_argument(
        "--json", action="store_true", default=argparse.SUPPRESS,
    )
    p.add_argument("--distribution-package", help="Verified distribution package selected for a pinned install")
    p.add_argument("--bundle", help="Distribution bundle to install")
    p.add_argument("--install-dsn-ref", default="env:OKG_DSN", help="Database credential environment reference")
    p.add_argument("--source-policy", help="Explicit deployment source-policy YAML; every final registry source must be declared")
    p.add_argument("--personal-private", action="store_true", help="Require --source-policy to declare store_raw: false (redaction) for every source")
    p.add_argument("--verify-query", help="Search phrase used to verify the selected CLI reader after publication")
    p.add_argument("--search-query", help="Search a completed pinned installation using its verified release and configured reader policy")
    p.add_argument("--search-generation", help="Pin retained-entry search to a published generation; requires --search-query")
    p.add_argument("--update-source", help="Update one existing source (and its declared predecessors) in a completed pinned installation; repeat the original installation inputs")
    p.add_argument("--authorize-cluster-bootstrap", action="store_true", help="Authorize runtime bootstrap role changes on the selected PostgreSQL cluster")
    p._okg_install_extensions = extensions


def _installable_profile_lines(extensions: Mapping[str, Any]) -> list[str]:
    """One help line per installable library profile (issue #3414).

    Lists every library profile, not only those shipping their own
    installer: `okg install --help` used to name only academic-researcher,
    so neither the default nor `workspace-graph` was discoverable from it.
    The first sentence of each profile's own description is the summary.
    """
    from okg.substrate.deployment_bootstrap.profile_init import (
        installable_library_profiles,
    )

    lines: list[str] = []
    for profile in installable_library_profiles():
        summary = " ".join((profile.description or "").split())
        summary = summary.split(". ")[0].rstrip(".")
        marks = []
        if profile.name == DEFAULT_INSTALL_PROFILE:
            marks.append("default")
        if profile.name in extensions:
            marks.append("own installer")
        label = profile.name + (f" ({', '.join(marks)})" if marks else "")
        lines.append(
            textwrap.fill(
                f"{label}: {summary}.",
                width=78,
                initial_indent="  ",
                subsequent_indent="      ",
                break_on_hyphens=False,
            )
        )
    return lines


# What the general installer reads in place of a flag only a profile's own
# installer declares. The refusal names it so an operator who meant the
# routed profile -- most often the default -- learns the right spelling
# instead of only being told to switch profiles (issue #3414).
_GENERAL_INSTALLER_EQUIVALENTS: dict[str, str] = {
    "--name": "--deployment-name",
    "--postgres-port": (
        "--postgres-dsn (the general installer starts no Postgres; point it "
        "at an existing database)"
    ),
}


def _foreign_installer_flags(
    args: argparse.Namespace,
    *,
    routed_profile: str,
    extensions: Mapping[str, Any],
    routed_question_ids: Iterable[str] = (),
) -> list[dict[str, str]]:
    """Flags a profile's own installer contributed, set for another profile.

    Every installable profile adds its flags to ONE `install` parser, so
    argparse accepts `okg install --orcid X` whichever profile runs. Before
    issue #3414 a bare install ran the profile that owned those flags; now
    it runs the minimal starter, which would drop them without a word --
    the silent-fallback class this repository treats as a defect. A flag the
    scaffold path also reads (`--postgres-dsn`, `--dry-run`, ...) is shared
    and never foreign. So is a flag the routed profile consumes as the
    answer to one of its own setup questions: workspace-graph asks
    `mcp_port`, and `_maybe_inject_profile_flags` maps that answer onto the
    `--mcp-port` slot academic-researcher's installer declared, so
    `okg install --profile workspace-graph --mcp-port N` sets it on purpose.
    """
    shared = argparse.ArgumentParser(add_help=False)
    _add_scaffold_arguments(shared)
    shared_dests = {action.dest for action in shared._actions}
    shared_dests.add("postgres_dsn")
    # The scaffold path reads it too: no agent is connected (PACT
    # agent-connect-3412), and the connect commands are printed instead.
    shared_dests.add("no_claude_config")
    flag_map = getattr(args, "_profile_flag_map", None) or {}
    shared_dests.update(flag_map.values())
    for question_id in routed_question_ids:
        shared_dests.update({question_id, f"answer_{question_id}"})
    found: list[dict[str, str]] = []
    for owner, extension in sorted(extensions.items()):
        if owner == routed_profile:
            continue
        probe = argparse.ArgumentParser(add_help=False)
        extension.add_arguments(probe)
        for action in probe._actions:
            if action.dest in shared_dests or not action.option_strings:
                continue
            if getattr(args, action.dest, action.default) != action.default:
                found.append({"flag": action.option_strings[0], "profile": owner})
    return found


def _install_via_scaffold(
    args: argparse.Namespace,
    profile_name: str | None,
    *,
    as_json: bool,
) -> int:
    """Install by writing the deployment from a profile or template.

    The generic path: no profile-supplied installer runs. This is the
    common case — library profiles and every reference deployment in the
    okg-deployments cache come up this way — and it is the same code
    `okg init` has always run, which is what lets `init` alias the door.
    """
    args.profile_name = profile_name
    return _cmd_init(args, as_json=as_json)


def _cmd_install(args: argparse.Namespace, *, as_json: bool = False) -> int:
    """Route `okg install`, then record what okg installed the deployment.

    #3415: a successful install of a deployment that runs from an installed
    okg package writes its installed-release receipt, so the first `okg up`
    after a package upgrade knows which release it is upgrading from. A
    receipt that cannot be written does not undo the install; the warning
    says so on stderr, which never carries the JSON payload.
    """
    rc = _cmd_install_routed(args, as_json=as_json)
    from okg.substrate.distributions.install_execution import active_bootstrap

    if (
        rc != 0
        or bool(getattr(args, "dry_run", False))
        or active_bootstrap() is not None
        or getattr(args, "distribution_package", None)
    ):
        return rc
    from okg.substrate.deployment_installed_upgrade import record_install_receipt

    outcome = record_install_receipt(getattr(args, "deployment_name", None))
    if outcome.get("error"):
        print(
            "WARNING: okg install could not write the installed-release "
            f"receipt ({outcome['error']}); the next `okg up` for this "
            "deployment will need --from-contract or --accept-unknown-compat",
            file=sys.stderr,
        )
    elif outcome.get("warning"):
        print(f"WARNING: {outcome['warning']}", file=sys.stderr)
    return rc


def _cmd_install_routed(args: argparse.Namespace, *, as_json: bool = False) -> int:
    """Route `okg install` to the selected profile's installer."""
    from okg.substrate.cli.profile_cli_loader import (
        discover_install_extensions,
    )

    from okg.substrate.distributions.install_execution import active_bootstrap, run_install
    if active_bootstrap() is not None or getattr(args, "distribution_package", None):
        return run_install(args, as_json=as_json)
    if any(getattr(args, flag, None) is not None for flag in ("update_source", "search_query", "search_generation")):
        return _emit_error(SubstrateError("packaged updates and searches require the pinned release launcher"), as_json=as_json)
    requested = getattr(args, "profile", None)
    if requested and getattr(args, "template", None):
        # Two different sources for the same deployment. The old behaviour
        # was to take the profile and drop `--template` on the floor, which
        # is the silent-fallback class this repo treats as a defect: the
        # operator asked for a template and got something else.
        return _emit_error(
            SubstrateError(
                "--profile and --template both name what to build",
                data={
                    "profile": requested,
                    "template": getattr(args, "template", None),
                },
                fix_hint=(
                    "pass one: `--profile <name>` for a profile "
                    "(preferred), or `--template <name>` for a legacy "
                    "library template"
                ),
            ),
            as_json=as_json,
        )
    if bool(getattr(args, "scaffold_only", False)):
        # Explicitly asked for the generic path. `okg init` delegates
        # through here, so its historical meaning — scaffold the profile,
        # run no installer — is expressible in the door's own vocabulary
        # rather than being a special case in this router.
        return _install_via_scaffold(args, requested, as_json=as_json)
    if requested is None and getattr(args, "template", None):
        # A legacy library template is a scaffold job, not a profile
        # install: falling through to the default profile's installer
        # would silently ignore --template and build something else.
        return _install_via_scaffold(args, None, as_json=as_json)

    requested = requested or DEFAULT_INSTALL_PROFILE
    from okg.substrate.deployment_bootstrap.profile_init import discover_profile

    try:
        routed = discover_profile(requested)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    extensions = discover_install_extensions()
    foreign = _foreign_installer_flags(
        args,
        routed_profile=requested,
        extensions=extensions,
        routed_question_ids=[q.id for q in routed.init_questions],
    )
    if foreign:
        owners = sorted({item["profile"] for item in foreign})
        flags = ", ".join(item["flag"] for item in foreign)
        explicit = getattr(args, "profile", None) is not None
        equivalents: list[str] = []
        for item in foreign:
            general = _GENERAL_INSTALLER_EQUIVALENTS.get(item["flag"])
            if general is not None:
                item["general_installer"] = general
                equivalents.append(f"{item['flag']} with {general}")
        if equivalents:
            keep_profile = (
                f"to install profile {requested!r}, replace "
                + "; ".join(equivalents)
                + (
                    ""
                    if len(equivalents) == len(foreign)
                    else "; and drop "
                    + ", ".join(
                        item["flag"] for item in foreign
                        if "general_installer" not in item
                    )
                )
            )
        else:
            keep_profile = f"to install profile {requested!r}, drop {flags}"
        return _emit_error(
            SubstrateError(
                f"{flags} {'belongs' if len(foreign) == 1 else 'belong'} to "
                f"the {', '.join(owners)} installer, but "
                f"this install runs profile {requested!r}"
                + ("" if explicit else " (the default when --profile is not given)"),
                data={
                    "profile": requested,
                    "profile_defaulted": not explicit,
                    "foreign_flags": foreign,
                },
                fix_hint=(
                    f"to install {owners[0]}, add --profile {owners[0]}; "
                    f"{keep_profile}"
                ),
            ),
            as_json=as_json,
        )
    extension = extensions.get(requested)
    if extension is not None:
        return extension.run(args, as_json=as_json)

    # No dedicated installer for this profile. That is the common case, not
    # an error, and routing here rather than refusing is what makes
    # `okg install --profile cms` do real work.
    return _install_via_scaffold(args, requested, as_json=as_json)


def _cmd_init_alias(args: argparse.Namespace, *, as_json: bool = False) -> int:
    """`okg init` — an alias for the door, `okg install`.

    Per PACT `define-newcomer-entry-path`
    (req.entry.migrate-the-other-doors): a superseded door may not keep
    running in parallel with the documented one. `init` therefore stops
    being an independent entry point and delegates, so every existing
    script and runbook that calls it keeps working and reaches the same
    end state.

    The delegation is exact: `okg init <args>` is
    `okg install <args> --scaffold-only`.

    The notice is for a human and goes to stderr, because stdout carries
    the payload callers parse. Under `--json` (or agent mode) it is
    suppressed entirely: stderr is the STRUCTURED channel there — the
    error envelope is written to it and callers `json.loads` the whole
    stream — so a line of prose there breaks every machine consumer.
    """
    if not as_json:
        print(
            "note: `okg init` is an alias for "
            f"`okg {entry_roles.newcomer_door()}`, the one documented way "
            f"to set up a graph. Running `okg {entry_roles.newcomer_door()}"
            " --scaffold-only` with the same arguments.",
            file=sys.stderr,
        )
    if getattr(args, "profile", None) is None:
        args.profile = getattr(args, "profile_name", None)
    args.scaffold_only = True
    return _cmd_install(args, as_json=as_json)


def _swallow_retired_verb_args(parser: argparse.ArgumentParser) -> None:
    """Let a retired verb accept any argument list (issue #2479).

    A retired verb only refuses, so its arguments are never used. argparse
    would otherwise exit 2 on an extra positional, an unknown flag or a flag
    missing its value before the typed `command_retired` refusal runs. The
    subparsers action calls `parse_known_args` on the verb's parser, so this
    replaces it on that one parser instance: `-h`/`--help` still prints the
    verb's help and exits 0, `--json` still selects JSON output, and every
    other token is kept in `retired_argv` and otherwise ignored.
    """

    def parse_known_args(args=None, namespace=None):
        tokens = list(args or [])
        if "-h" in tokens or "--help" in tokens:
            parser.print_help()
            parser.exit(0)
        ns = argparse.Namespace() if namespace is None else namespace
        if "--json" in tokens:
            ns.json = True
        ns.retired_argv = tokens
        return ns, []

    parser.parse_known_args = parse_known_args


class _HiddenVerbsFormatterMixin:
    """Render a subparsers choice list without its help-hidden verbs.

    argparse builds the ``{a,b,...}`` usage metavar from every entry in
    ``action.choices``. Setting ``action.metavar`` instead would also rename
    the argument in error messages (``argument {a,b,...}: invalid choice``
    instead of ``argument command: ...``), so the hidden verbs are filtered
    here, at render time only. Dispatch never reads the formatter.
    """

    def _metavar_formatter(self, action, default_metavar):
        hidden = getattr(action, "_okg_hidden_from_help", None)
        if hidden and action.metavar is None and action.choices is not None:
            result = "{" + ",".join(
                str(v) for v in action.choices if v not in hidden
            ) + "}"

            def fmt(tuple_size):
                return (result,) * tuple_size

            return fmt
        return super()._metavar_formatter(action, default_metavar)


def _apply_entry_roles_to_help(
    sub: argparse._SubParsersAction,
    parser: argparse.ArgumentParser,
) -> None:
    """Sort, label and filter the top-level verb listing by entry role.

    A newcomer scanning 58 verbs has no way to tell which one starts the
    system. This puts the documented door first, keeps the verbs that
    operate an existing deployment near it, and sinks retired doors to the
    bottom with a marker saying what replaced them.

    It also drops verbs whose visibility is ``hidden`` (okg#3109 slice 0)
    from the listing and from the ``{a,b,...}`` usage choice list, and
    names them in one epilog note so they stay discoverable. Dispatch is
    untouched: hidden verbs stay in ``sub.choices``, so they parse, run,
    and answer ``okg <verb> --help`` as before. ``retired`` verbs (issue
    #2479) leave the listing and the usage choice list the same way but
    get no epilog mention; they still dispatch, and only refuse.
    ``unlisted`` verbs (issue #3416) leave both and get no epilog mention,
    but run exactly as before.
    """
    choices_actions = getattr(sub, "_choices_actions", None)
    if not choices_actions:
        return
    for pseudo in choices_actions:
        prefix = entry_roles.help_prefix(pseudo.dest)
        if prefix and not (pseudo.help or "").startswith(prefix):
            pseudo.help = f"{prefix}{pseudo.help or ''}"
    hidden = [
        a.dest for a in choices_actions
        if entry_roles.visibility_of(a.dest) == entry_roles.HIDDEN
    ]
    # Retired verbs (issue #2479) leave the listing too, but are not named
    # in the epilog: they only refuse and point elsewhere. Renamed verbs
    # (decision 310) leave it as well and are named only in their warning.
    retired = [
        a.dest for a in choices_actions
        if entry_roles.visibility_of(a.dest) == entry_roles.RETIRED
    ]
    renamed = [a.dest for a in choices_actions if a.dest in entry_roles.RENAMED_VERBS]
    # Unlisted verbs (issue #3416) leave the listing and are named nowhere
    # in it: this release does not support them for the public.
    unlisted = [
        a.dest for a in choices_actions
        if entry_roles.visibility_of(a.dest) == entry_roles.UNLISTED
    ]
    dropped = (
        frozenset(hidden) | frozenset(retired) | frozenset(renamed)
        | frozenset(unlisted)
    )
    if dropped:
        choices_actions[:] = [
            a for a in choices_actions if a.dest not in dropped
        ]
    # `sorted` is stable, so verbs sharing a role keep declaration order.
    choices_actions.sort(key=lambda a: entry_roles.display_rank(a.dest))
    if not dropped:
        return
    sub._okg_hidden_from_help = dropped
    base = parser.formatter_class
    if not issubclass(base, _HiddenVerbsFormatterMixin):
        parser.formatter_class = type(
            f"_HiddenVerbs{base.__name__}",
            (_HiddenVerbsFormatterMixin, base),
            {},
        )
    if not hidden:
        return
    # The top-level formatter is RawDescription, so wrap by hand.
    note = textwrap.fill(
        "Internal commands for this repository's agents, CI and "
        "benchmarks are not listed above: "
        + ", ".join(sorted(hidden))
        + ".",
        width=78,
        break_on_hyphens=False,
    ) + "\nEach still runs; see `okg <command> --help`."
    parser.epilog = f"{parser.epilog}\n\n{note}" if parser.epilog else note


#: Verbs that accept a profile's `init_questions` as `--<question-id>`
#: flags. `install` is here because it routes profiles without their own
#: installer through the same scaffold path `init` uses, so the two must
#: accept the same answers for one to alias the other.
_PROFILE_ANSWER_VERBS = frozenset({"init", "install"})


def _maybe_inject_profile_flags(
    parser: argparse.ArgumentParser, argv: list[str],
) -> dict[str, str]:
    """When `argv` looks like `<verb> --profile <name> ...`, load the
    profile and add `--<question-id>` flags to that verb's subparser
    so argparse accepts them.

    Applies to `init` and to `install`. Per PACT
    `define-newcomer-entry-path` the substrate-owned `install` routes
    profiles without their own installer through the generic scaffold
    path, so it has to accept the same profile-answer flags `init` does —
    otherwise `okg install --profile workspace-graph --postgres-dsn ...`
    dies with `unrecognized arguments` while the identical `init`
    invocation succeeds, and `init` could not become an alias.

    Returns a mapping `{question_id: argparse_dest}` for use by
    `_gather_profile_answers`. Returns `{}` when not in profile mode or
    when the profile can't be loaded (the error surfaces later via the
    normal CLI error envelope).
    """
    from okg.substrate.distributions.install_execution import active_bootstrap
    if active_bootstrap() is not None:
        return {}
    if not argv or argv[0] not in _PROFILE_ANSWER_VERBS:
        return {}
    verb = argv[0]
    profile_name: str | None = None
    i = 1
    while i < len(argv):
        a = argv[i]
        if a == "--profile" and i + 1 < len(argv):
            profile_name = argv[i + 1]
            break
        if a.startswith("--profile="):
            profile_name = a.split("=", 1)[1]
            break
        i += 1
    if not profile_name:
        return {}
    try:
        from okg.substrate.deployment_bootstrap.profile_init import (
            discover_profile,
        )
        profile = discover_profile(profile_name)
    except Exception:  # noqa: BLE001 — early arg-inject path; real error surfaces during normal dispatch.
        # Surface the error during the normal dispatch path so the
        # CLI envelope is consistent; we silently skip injection
        # here.
        return {}

    init_subparser = None
    for action in parser._actions:  # noqa: SLF001 — argparse private
        if isinstance(action, argparse._SubParsersAction):
            init_subparser = action.choices.get(verb)
            break
    if init_subparser is None:
        return {}

    flag_map: dict[str, str] = {}
    existing_dests = {
        action.dest for action in init_subparser._actions  # noqa: SLF001
    }
    existing_option_strings: set[str] = set()
    for action in init_subparser._actions:  # noqa: SLF001
        existing_option_strings.update(action.option_strings)
    for q in profile.init_questions:
        dest = f"answer_{q.id}"
        flag = "--" + q.id.replace("_", "-")
        # Skip if the profile happens to name a question that
        # collides with a known init flag (e.g. `deployment_name`
        # — already declared on the subparser). Check BOTH the
        # dest AND the flag name: a profile question whose
        # name matches a native init flag (`--deployment-name`)
        # would otherwise duplicate the option-string and crash
        # argparse at parser-build time.
        if dest in existing_dests or flag in existing_option_strings:
            # Map the question id to the native flag's dest so
            # the gatherer reads the operator's answer from the
            # right slot.
            native_action = next(
                (a for a in init_subparser._actions  # noqa: SLF001
                 if flag in a.option_strings),
                None,
            )
            if native_action is not None:
                flag_map[q.id] = native_action.dest
            else:
                flag_map[q.id] = dest
            continue
        kwargs: dict[str, Any] = {
            "dest": dest,
            "default": None,
            "help": q.prompt + (
                f" (required, type: {q.type})"
                if q.required
                else f" (optional, type: {q.type})"
            ),
        }
        # path_list accepts a comma-separated string at the CLI;
        # validation + splitting happens in validate_answer.
        init_subparser.add_argument(flag, **kwargs)
        flag_map[q.id] = dest

    # Bonus: also map deployment_name → existing --deployment-name
    # flag so the question prompts and the flag overlap.
    if profile.question("deployment_name"):
        flag_map.setdefault("deployment_name", "deployment_name")
    return flag_map


def _is_okg_checkout_root(path: Path) -> bool:
    pyproject = path / "pyproject.toml"
    marker = path / "src" / "okg" / "substrate" / "cli" / "main.py"
    if not pyproject.is_file() or not marker.is_file():
        return False
    try:
        text = pyproject.read_text(encoding="utf-8")
    except OSError:
        return False
    return "[project]" in text and 'name = "okg"' in text


def _cwd_okg_checkout_root() -> Path | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=5,
            check=True,
        )
    except Exception:  # noqa: BLE001 - no git checkout, no stale guard.
        return None
    root = Path(completed.stdout.strip()).resolve()
    return root if _is_okg_checkout_root(root) else None


def _entrypoint_package_root() -> Path | None:
    source = Path(__file__).resolve()
    marker = Path("src") / "okg" / "substrate" / "cli" / "main.py"
    for parent in source.parents:
        try:
            if (parent / marker).resolve() == source and _is_okg_checkout_root(parent):
                return parent.resolve()
        except OSError:
            continue
    return None


def _stale_entrypoint_error(argv: list[str]) -> SubstrateError | None:
    if os.environ.get("OKG_ALLOW_EXTERNAL_OKG_ENTRYPOINT") == "1":
        return None
    checkout_root = _cwd_okg_checkout_root()
    if checkout_root is None:
        return None
    package_root = _entrypoint_package_root()
    if package_root is not None and package_root == checkout_root:
        return None
    rendered_args = " ".join(shlex.quote(arg) for arg in argv)
    expected = (
        f"cd {shlex.quote(str(checkout_root))} && uv run okg"
        + (f" {rendered_args}" if rendered_args else "")
    )
    repair = f"uv tool install --force --editable {shlex.quote(str(checkout_root))}"
    return SubstrateError(
        "bare okg is not running this checkout's code",
        data={
            "executable_path": sys.argv[0],
            "path_okg": shutil.which("okg"),
            "package_path": str(Path(__file__).resolve()),
            "package_root": str(package_root) if package_root else None,
            "repo_root": str(checkout_root),
            "expected_command": expected,
            "repair_command": repair,
        },
        fix_hint=f"use `{expected}` or repair the tool with `{repair}`",
    )


def _query_redirect_error(argv: list[str]) -> SubstrateError | None:
    """`okg query` with no deployment is the most-guessed way to ask the
    graph a question in agent transcripts (per `polish-pact-agent-ux`
    req.polish.verb-guidance): a bare `okg query "recent pacts"` meant a
    text search. `okg query` is now the read-only SQL operator (issue
    3666), so a call that names a deployment -- with `--deployment`,
    through `OKG_DEPLOYMENT` or by the checkout's configured default, the
    order the command itself resolves it in -- or asks for help goes to that
    command, whose tool refuses text that is not SQL. A call that names none
    gets the pointer to the real read commands instead of a usage screen."""
    cmd_idx = next((i for i, a in enumerate(argv) if not a.startswith("-")), None)
    if cmd_idx is None or argv[cmd_idx] != "query":
        return None
    real = ("--deployment", "--help", "-h")
    if any(a == flag or a.startswith(flag + "=") for a in argv for flag in real):
        return None
    from okg.substrate.deployment_default import resolve_command_deployment

    try:
        resolve_command_deployment(None, command="okg query")
    except SubstrateError as exc:
        if getattr(exc, "code", None) != "deployment_not_specified":
            # A malformed configured default: the command says so, loudly.
            return None
    else:
        return None
    return SubstrateError(
        "`okg query` runs one read-only SQL statement and needs a deployment",
        data={"requested": " ".join(argv)},
        fix_hint=(
            "for a text search use `okg search --deployment <d> --query \"...\"`; "
            "for SQL use `okg query --deployment <d> \"select ...\"`; "
            "OKG_DEPLOYMENT=<d> stands in for the flag; the other "
            "read operators are `okg inspect`, `expand`, `filter`, `aggregate` "
            "and `map`, the same operators the MCP server serves"
        ),
    )


def _maybe_rewrite_schema_show(argv: list[str]) -> list[str]:
    """Back-compat: `okg [globals] schema <file>` -> `... schema show <file>`.

    The `schema` command grew `show`/`quality` subcommands. The command token is
    the first non-flag arg (the only top-level global, `--json`, is store_true and
    takes no value, so leading flags never consume the command). If that command
    is `schema` and the next positional is a bare file (not a subcommand, not a
    flag), insert `show` so the legacy file-dump form keeps working — including
    `okg --json schema deployment.yaml`.
    """
    cmd_idx = next((i for i, a in enumerate(argv) if not a.startswith("-")), None)
    if cmd_idx is None or argv[cmd_idx] != "schema":
        return argv
    rest = argv[cmd_idx + 1:]
    nxt_idx = next((j for j, a in enumerate(rest) if not a.startswith("-")), None)
    if nxt_idx is None or rest[nxt_idx] in ("show", "quality"):
        return argv
    return [*argv[: cmd_idx + 1], "show", *rest]


#: PACT v5 verbs handled by the self-contained `pact` package. They are
#: dispatched before argparse because their argument grammar belongs to that
#: package (its own `--json`, `--root`, `--help`), not to this parser.
_PACT_V5_VERBS = frozenset({"approve", "check", "digest", "migrate", "usage"})
#: v4 verbs whose target can be a v5 manifest; the raw argv is handed to the
#: `pact` package unchanged (flags included) once the manifest proves v5.
_PACT_V4_VERBS_WITH_CHANGE = frozenset({"view", "evidence", "gate", "decide", "new"})
#: The only `view --format` projections the v5 CLI renders. Every other
#: projection (bootstrap, next, packet, chat-review, review-queue — the ones
#: CLAUDE.md and the agent skills mandate) stays with the v4 reader, which
#: reads a v5 manifest through `okg.substrate.pact.v5_adapter.to_v4`.
_PACT_V5_VIEW_FORMATS = frozenset({"brief", "yaml"})
#: `okg pact new` flags only the v4 writer defines; any of them keeps `new`
#: on the v4 path. Without them `new` goes to the v5 writer.
_PACT_V4_ONLY_NEW_FLAGS = frozenset({
    "--template", "--shape", "--intent", "--acceptance",
    "--task-title", "--test-id", "--test-command",
    # The v4 parser's help lists every flag and the repository templates.
    "-h", "--help",
})


def _view_format(rest: list[str]) -> str | None:
    """The `--format` value in `rest`, accepting both spellings, else None."""
    for index, token in enumerate(rest):
        if token == "--format" and index + 1 < len(rest):
            return rest[index + 1]
        if token.startswith("--format="):
            return token.split("=", 1)[1]
    return None


def _pact_new_v5_argv(rest: list[str]) -> list[str] | None:
    """v5 argv for any `okg pact [--root R] [--json] new|new-change ...` form.

    `new` writes the v5 manifest that `okg pact approve` accepts, whether the
    pact-level options come before or after the verb. Only the v4 authoring
    flags keep the v4 writer, because the v5 `new` does not define them.
    """
    leading: list[str] = []
    index = 0
    while index < len(rest):
        token = rest[index]
        if token == "--root" and index + 1 < len(rest):
            leading += rest[index:index + 2]
            index += 2
        elif token.startswith("--root=") or token == "--json":
            leading.append(token)
            index += 1
        else:
            break
    if index >= len(rest) or rest[index] not in {"new", "new-change"}:
        return None
    tail = rest[index + 1:]
    if any(token.split("=", 1)[0] in _PACT_V4_ONLY_NEW_FLAGS for token in tail):
        return None
    return ["new", *tail, *leading]


def _pact_v5_reroute(rest: list[str]) -> list[str] | None:
    """argv for the v5 CLI when `okg pact <rest>` belongs to it, else None.

    v5 verbs always reroute. A v4 verb reroutes when the first non-flag token
    after the verb (and after the `evidence`/`gate` subverb) names a manifest
    under pact/changes or pact/archive whose `pact_version` is 5; `gate <hook>
    <id>` becomes `check <id> --strict` so the merge-facing hook keeps its
    strength. Options the user passed are preserved verbatim.
    """
    new_argv = _pact_new_v5_argv(rest)
    if new_argv is not None:
        return new_argv
    verb = rest[0]
    if verb in _PACT_V5_VERBS:
        return list(rest)
    if verb not in _PACT_V4_VERBS_WITH_CHANGE or verb == "new":
        return None
    if verb == "view":
        fmt = _view_format(rest)
        if fmt is not None and fmt not in _PACT_V5_VIEW_FORMATS:
            # The v4 reader owns this projection. Rerouting would hand the v5
            # parser a --format it does not define, and argparse would exit 2
            # on the exact command the agent instructions mandate.
            return None
    if rest[:2] == ["evidence", "attach"] and len(rest) > 2 and all(
        token in {"-h", "--help"} for token in rest[2:]
    ):
        # No change id, so no manifest says which parser applies. v5 is the
        # default, and the legacy v4 help (`<change> <target> --ref ...`)
        # misled readers into a form that fails on a v5 change. A v4 change
        # still gets its own help from `evidence attach <id> --help`.
        return list(rest)
    root = Path(".")
    for index, token in enumerate(rest[1:], start=1):
        if token == "--root" and index + 1 < len(rest):
            root = Path(rest[index + 1])
    positionals = [
        token for index, token in enumerate(rest[1:], start=1)
        if not token.startswith("-") and rest[index - 1] not in {"--root", "--format", "--kind", "--requirement", "--command", "--artifact", "--head", "--note", "--status", "--verdict", "--note", "--base", "--head-ref", "--approvals-json", "--pytest-report", "--formal-report", "--graph-report", "--ref", "--to", "--target"}
    ]
    subverb = None
    if verb in {"evidence", "gate"} and positionals:
        subverb = positionals.pop(0)
    if not positionals:
        return None
    change = positionals[0]
    manifest = None
    for branch in ("changes", "archive"):
        candidate = root / "pact" / branch / change / "pact.yaml"
        if candidate.is_file():
            manifest = candidate
            break
    if manifest is None:
        return None
    try:
        from pact.store import pact_version_of

        if pact_version_of(manifest) != 5:
            return None
    except Exception:
        return None
    if verb == "gate":
        tail = [token for token in rest[1:] if token != subverb]
        return ["check", *tail, "--strict"]
    return list(rest)


#: Set by a deployment that knows where its database URL is kept, for
#: example the Helm chart: "Secret okg-secrets, key okg-dsn".
DSN_SOURCE_ENV = "OKG_DSN_SOURCE"


def _dsn_variables_naming(host: str, port: str) -> list[str]:
    """Environment variables whose database URL names ``host``:``port``."""
    from okg.substrate.db.connection import connect_target

    names = []
    for key, value in sorted(os.environ.items()):
        if not key.endswith("DSN") or "://" not in value and "host=" not in value:
            continue
        target = connect_target(value)
        if target["host"] == host and target["port"] == port:
            names.append(key)
    return names


#: libpq's words for a server that answered and refused the login.
_LOGIN_REFUSED = (
    "password authentication failed",
    "no pg_hba.conf entry",
    "role \"",
)


def _database_connect_error(
    command: str, exc: BaseException, target: dict,
) -> SubstrateError:
    """One refusal for a connection failure no command caught (issue #3582).

    A server that did not answer, one that answered and refused the login,
    and a missing database each get their own hint; the hint names where to
    correct the URL when that is known.
    """
    from okg.substrate.cli._db_bootstrap_errors import missing_database_name
    from okg.substrate.db.dsn import redact_text

    host, port = str(target.get("host")), str(target.get("port"))
    user = str(target.get("user") or "")
    text = str(exc).strip()
    reason = redact_text(text.splitlines()[0] if text else type(exc).__name__)
    variables = _dsn_variables_naming(host, port)
    source = (os.environ.get(DSN_SOURCE_ENV) or "").strip()
    if source and "OKG_DSN" not in variables:
        source = ""  # the URL that failed is not the one kept there
    if source:
        where = f"the database URL in {source}"
    elif variables:
        where = " and ".join(variables)
    else:
        where = "the deployment's database URL"
    data = {
        "command": command,
        "host": host,
        "port": port,
        "database": target.get("dbname"),
        "exception_type": type(exc).__name__,
        "sqlstate": getattr(exc, "sqlstate", None),
        "dsn_source": source or None,
        "dsn_variables": variables,
    }
    missing = missing_database_name(exc)
    if missing is not None:
        dbname = missing or str(target.get("dbname"))
        createdb = " ".join(
            ["createdb", "-h", host, "-p", port, *(["-U", user] if user else []), dbname]
        )
        err = SubstrateError(
            f"okg {command}: database {dbname!r} does not exist on the server at "
            f"{host}:{port} (the server answered; only the database is missing)",
            data=data,
            fix_hint=(
                f"create it with `{createdb}`, or if the name is wrong, correct "
                f"{where}; then run the command again"
            ),
        )
        err.code = "database_missing"
        return err
    if host == "<unparseable URL>":
        err = SubstrateError(
            # libpq's own message repeats the broken host, so it is left out.
            f"okg {command}: the database URL could not be read: its host or "
            "port is not valid, which usually means a password with an "
            "unescaped @, / or :",
            data=data,
            fix_hint=(
                "a password with @, / or : must be percent-encoded in a URL "
                f"(@ is %40, / is %2F, : is %3A); correct {where}, then run "
                "the command again"
            ),
        )
        err.code = "database_unreachable"
        return err
    if any(marker in text for marker in _LOGIN_REFUSED):
        err = SubstrateError(
            f"okg {command}: the database server at {host}:{port} refused the "
            f"login: {reason}",
            data=data,
            fix_hint=(
                f"the server is up and answered; check the user and password "
                f"in {where}, then run the command again"
            ),
        )
        err.code = "database_login_refused"
        return err
    err = SubstrateError(
        f"okg {command}: cannot connect to the database at {host}:{port}: {reason}",
        data=data,
        fix_hint=(
            f"check that {host} is the right host and the server is up; if "
            f"the host is wrong, correct {where}, then run the command again"
        ),
    )
    err.code = "database_unreachable"
    return err


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    argv = _maybe_rewrite_schema_show(argv)
    if argv[:2] == ["--json", "pact"] and len(argv) > 2:
        # `okg --json pact new ...`: the global flag belongs to the v5 verb.
        new_argv = _pact_new_v5_argv(argv[2:])
        if new_argv is not None:
            from pact.cli import main as pact_v5_main

            return pact_v5_main([*new_argv, "--json"], prog="okg pact")
    if argv[:1] == ["pact"] and len(argv) > 1:
        rerouted = _pact_v5_reroute(argv[1:])
        if rerouted is not None:
            from pact.cli import main as pact_v5_main

            return pact_v5_main(rerouted, prog="okg pact")

    # A forced command has a deliberately closed argv contract.  Check it
    # before argparse so an accidental `--dsn`, identity flag, or arbitrary
    # shell argument cannot be echoed by argparse's ordinary usage error.  A
    # real sshd ForceCommand supplies exactly this three-token shape.
    # The universal CLI smoke contract invokes every verb as
    # ``okg <verb> --help``.  Permit only that exact, side-effect-free form
    # through the forced-command guard so argparse can render help; every
    # other occurrence remains subject to the closed wire grammar below.
    if "agent-ssh-run" in argv and argv != ["agent-ssh-run", "--help"]:
        if (
            argv[0:1] != ["agent-ssh-run"]
            or len(argv) != 3
            or argv[1] != "--route"
            or not argv[2]
            or argv[2].startswith("-")
        ):
            print(
                json.dumps(
                    {
                        "ok": False,
                        "status": "rejected",
                        "reason_code": "request_arguments_rejected",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 1
    if "agent-ssh-client" in argv and argv != ["agent-ssh-client", "--help"]:
        if argv[0:1] != ["agent-ssh-client"] or len(argv) != 1:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "status": "rejected",
                        "reason_code": "request_arguments_rejected",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            return 1

    stale_error = _stale_entrypoint_error(argv)
    if stale_error is not None:
        return _emit_error(
            stale_error,
            as_json=("--json" in argv or _is_agent_mode()),
        )

    query_error = _query_redirect_error(argv)
    if query_error is not None:
        return _emit_error(
            query_error,
            as_json=("--json" in argv or _is_agent_mode()),
        )

    import time

    parse_started = time.perf_counter()
    parser = _build_parser(argv)
    # Layer 2A: when invoking `okg init --profile <name>`, the
    # profile's init_questions become dynamic `--<question-id>`
    # flags. Pre-scan argv to discover the profile name + load it,
    # then re-build the parser with the flags injected so
    # argparse doesn't reject unknown args.
    profile_flag_map = _maybe_inject_profile_flags(parser, argv)
    args = parser.parse_args(argv)
    from okg.substrate.cli import _read_operators

    if getattr(args, "command", None) in _read_operators.OPERATOR_ARGUMENTS:
        # For the read commands' OKG_READ_DEBUG phase times.
        setattr(
            args, _read_operators.PARSE_SECONDS_ATTR,
            time.perf_counter() - parse_started,
        )
    if profile_flag_map:
        # `_gather_profile_answers` reads this attribute via
        # `getattr` to know which `args.*` slots came from the
        # profile's init_questions.
        setattr(args, "_profile_flag_map", profile_flag_map)

    renamed_verb = entry_roles.RENAMED_VERBS.get(getattr(args, "command", None) or "")
    if renamed_verb:
        warn_renamed(f"okg {args.command}", f"okg {renamed_verb}")

    as_json = getattr(args, "json", False) or _is_agent_mode()

    # Load the untracked local live-workspace profile before commands
    # resolve deployment `${OKG_*}` placeholders. `okg deployment env`
    # reads the profile itself so it can report process_env vs
    # local_profile sources; readiness does the same before live checks.
    # Skip auto-load for both to avoid masking which source supplied each
    # value.
    if not (
        (
            args.command == "deployment"
            and (
                getattr(args, "subcmd", None) in {"env", "ready"}
                or (
                    getattr(args, "subcmd", None) == "conformance"
                    and getattr(args, "conformance_subcmd", None)
                    in {"schema", "validate"}
                )
            )
        )
        or (
            args.command == "dogfood"
            and getattr(args, "dogfood_subcmd", None) == "status"
        )
        or args.command == "environment"
        or args.command in {"agent-ssh-run", "agent-ssh-client"}
    ):
        from okg.substrate.local_env import load_local_env_for_cli

        load_local_env_for_cli()

    # Remaining legacy agent-only write gates; migrated handlers enforce --apply themselves.
    if _is_agent_mode():
        db_write = (
            args.command == "db"
            and (
                getattr(args, "db_subcmd", None)
                in {
                    "drop",
                    "remove-job-runtime-subsystem",
                    "migrate-down",
                }
                or (
                    getattr(args, "db_subcmd", None) == "dbos-gc"
                    and bool(getattr(args, "confirm", False))
                )
            )
        )
        # `chat source-retry --apply` enqueues real source-sync work
        # through the DBOS runtime, so it is a write command like the
        # `jobs enqueue-source` it drives (PACT chat-source-admin).
        chat_write = (
            args.command == "chat"
            and getattr(args, "chat_subcmd", None) == "source-retry"
        )
        write_cmd = (
            # `migrate` is deliberately absent: it enforces --apply itself,
            # for every caller, so its refusal reads the same in both modes
            # (issue #1288).
            db_write or chat_write
        )
        if write_cmd and not getattr(args, "apply", False):
            return _emit_error(
                SubstrateError(
                    f"`okg {args.command}` requires --apply under OKG_AGENT=1",
                    fix_hint=_agent_apply_fix_hint(args),
                ),
                as_json=as_json,
            )

    # Per define-memex-deployment-contract R4: read-side
    # subcommands wrap MCP tools without duplicating logic.
    from okg.substrate.cli import _read_side
    from okg.substrate.cli import federation as federation_cli
    from okg.substrate.cli import nomos as nomos_cli
    from okg.substrate.cli import poros as poros_cli
    # Per `retire-pi-prefix-commands`: profile-shipped CLI verbs
    # come from `library/profiles/<name>/cli/__init__.py`, loaded
    # via the discovery hook. The substrate's dispatch table no
    # longer names any specific profile.
    _profile_dispatch = getattr(parser, "_okg_profile_dispatch", {})
    dispatch = {
        "migrate":   _cmd_migrate,
        "doctor":    _cmd_doctor,
        "dogfood":   _cmd_dogfood,
        "db":        _cmd_db,
        "run":       _cmd_run,
        "enricher":  _cmd_enricher,
        "ingest":    _cmd_ingest,
        "sync":      _cmd_sync,
        "metrics":   _cmd_metrics,
        "telemetry": _cmd_telemetry,
        "provision": _cmd_provision,
        "status":    _cmd_status,
        # Substrate-wide Docker lifecycle verbs (per
        # `dead-simple-install`). Imported lazily because the
        # lifecycle module imports yaml, which we don't want
        # to pull in for `okg --help`-shaped invocations.
        "start":     _lifecycle_dispatcher("cmd_start"),
        "stop":      _lifecycle_dispatcher("cmd_stop"),
        "restart":   _lifecycle_dispatcher("cmd_restart"),
        # Per `separate-framework-and-deployments`. Lazy import
        # for the same reason as lifecycle verbs.
        "fetch-deployments": _fetch_dispatcher(),
        "schema":    _cmd_schema,
        "deployment": _cmd_deployment,
        "deployments": _cmd_deployments,
        "environment": _cmd_environment,
        "external-action": _cmd_external_action,
        "up":         _cmd_up,
        "runtime":   _cmd_runtime,
        "jobs":      _cmd_jobs,
        "chat":      _cmd_chat,
        "chat-instance": _cmd_chat_instance,
        "perf":      _cmd_profile,
        "profile":   _cmd_profile,
        "federation": federation_cli.cmd_federation,
        "connect":   federation_cli.cmd_connect,
        "send":      federation_cli.cmd_send,
        "pull":      federation_cli.cmd_pull,
        "inbox":     federation_cli.cmd_inbox,
        "proof":     federation_cli.cmd_proof,
        "nomos":     nomos_cli.cmd_nomos,
        "poros":     poros_cli.cmd_poros,
        "mcp-serve": _cmd_mcp_serve,
        "ui":        _cmd_ui,
        "agent-ssh-run": _cmd_agent_ssh_run,
        "agent-ssh-client": _cmd_agent_ssh_client,
        "agent-runtime-launch": _cmd_agent_runtime_launch,
        "agent":     _cmd_agent,
        # `init` delegates to the door; see `_cmd_init_alias`.
        "init":      _cmd_init_alias,
        # Layer 2A scaffolder; `new-source` is retired and only refuses.
        "new-module": _cmd_new_module,
        "new-source": _cmd_new_source,
        "library":   _cmd_library,
        "trace":     _cmd_trace,
        "source":    _cmd_source,
        "generation": _cmd_generation,
        "branch":    _cmd_branch,
        "admission": _cmd_admission,
        "pact":      _cmd_pact,
        "consistency-rerun": _cmd_consistency_rerun,
        "consistency-scope-report": _cmd_consistency_scope_report,
        "bench":     _cmd_bench,
        "view":      _cmd_view,
        "catalog":   _cmd_catalog,
        "alias":     _cmd_alias,
        "remediate": _cmd_remediate,
        "snapshot-load": _cmd_snapshot_load,
        "lint":      _cmd_lint,
        "install":   _cmd_install,
        **_read_side.DISPATCH,
        **_profile_dispatch,
    }
    if args.command not in dispatch:
        parser.print_help()
        return 2
    try:
        return dispatch[args.command](args, as_json=as_json)
    except Exception as exc:
        target = getattr(exc, "okg_connect_target", None)
        if not isinstance(target, dict):
            raise
        return _emit_error(
            _database_connect_error(str(args.command), exc, target),
            as_json=as_json,
        )


def console_main() -> int:
    """Process entry point: the ``okg`` console script and ``python -m``.

    Moves Python's bytecode cache for this process and its Python children
    to the per-user okg cache before any deployment code is imported, so no
    ``__pycache__`` lands in a deployment repository and fails the publish's
    provenance check (``okg.substrate.bytecode_cache``). ``main`` itself
    leaves the interpreter alone, so an in-process caller keeps its settings.
    """
    from okg.substrate.bytecode_cache import enter_user_bytecode_cache

    enter_user_bytecode_cache()
    return main()


if __name__ == "__main__":
    sys.exit(console_main())
