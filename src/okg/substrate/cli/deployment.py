"""CLI handlers for `okg deployment`.

Extracted from ``cli/main.py`` by ``extract-cli-deployment``
(2026-05). Subcommands: show / bug-report. cli.main helpers
loaded lazily via the shared ``_load_main_helpers()`` cache
(per Wave D-1).
"""
from __future__ import annotations
from okg.substrate import fastyaml

import argparse
import json
import os
import shlex
import stat
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.cli.deployment_identifier import (
    require_graph_deployment_name as _require_graph_deployment_name,
)
from okg.substrate.db.connection import redact as redact_dsn
from okg.substrate.support.user_hints import okg_prefix


# Module-level helper bindings, populated by _load_main_helpers().
_emit_error: Any = None
_emit_ok: Any = None
_json_safe: Any = None
_resolve_deployment_dsn: Any = None
_resolve_deployment_dsn_from_dir: Any = None
SubstrateError: Any = None


_READY_REQUIRED_RELATIONS = (
    "okg.graph_nodes",
    "okg.graph_edges",
    "okg.v_runtime_queue_health",
    "okg.source_watermarks",
    "okg.v_nodes",
    "okg.v_edges",
)

_READY_AGENT_CONTEXT_SOURCE_GROUPS = {
    "codex_sessions": ("codex-sessions",),
    "claude_sessions": ("agent-sessions",),
    "pact": ("pact",),
    "openspec_history": ("openspec",),
    "coordination": ("coordination-config", "coordination-native"),
    "code": ("git-files", "code-structure"),
}
_READY_CI_SOURCE_GROUPS = {
    "repo": ("git-history", "github-issues", "pact"),
    "test_artifacts": ("test-reports",),
    "dependencies": ("package-manifests",),
}
AGENT_CONTEXT_READY_SOURCES = tuple(sorted({
    source
    for sources in _READY_AGENT_CONTEXT_SOURCE_GROUPS.values()
    for source in sources
}))
CI_READY_SOURCES = tuple(sorted({
    source
    for sources in _READY_CI_SOURCE_GROUPS.values()
    for source in sources
}))

_READY_QUEUE_KINDS = ("ingest", "link", "enrich", "publish", "embed")
#: The `--checks` values `deployment ready`, `deployment up` and `up` accept.
#: `general` is the smoke level and the default; the other three are the
#: declarable readiness profiles (issue #3958).
READY_CHECKS_CHOICES: tuple[str, ...] = ("general", "dogfood", "ci", "standard")
_READY_DEFAULT_MAX_SOURCE_AGE_SECONDS = 14 * 24 * 60 * 60
_READY_CI_MAX_SOURCE_AGE_SECONDS = 6 * 60 * 60
_READY_DOGFOOD_MAX_SOURCE_AGE_SECONDS = 60 * 60
_READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS = 15 * 60
_READY_MAX_WORK_AMPLIFICATION_RATIO = 3.0
_CONFORMANCE_MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
_READY_ACCEPTED_EXPECTED_DELTA_SOURCES = frozenset({
    "explicit",
    "harness_declared",
})
_READY_WORKSPACE_GRAPH_MODULES = frozenset({
    "agent_sessions",
    "code_graph",
    "git_graph",
    "openspec",
})
READY_STATE_SEMANTICS = {
    "clean": "the check ran and proved the dependency is ready",
    "unknown": (
        "the check did not run to completion within its deadline, so this "
        "state is unproven -- it is neither a pass nor a failure"
    ),
    "stale": "the check ran and found reachable runtime state outside the readiness bounds",
    "unavailable": "the check could not prove runtime state because a dependency failed",
    "command_failed": "a subprocess or helper command ran and returned a non-zero result",
}
_READY_UNAVAILABLE_BLOCKER_IDS = frozenset({
    "database.connectivity",
    "database.migrations",
    "deployment.apply",
    "generation.latest_published",
    "mcp.graph_read",
    "nomos.active_state",
    "nomos.deployment_policy",
    "runtime.worker_processes",
})


def _ready_required_relations() -> tuple[str, ...]:
    return _READY_REQUIRED_RELATIONS


def _ready_queue_kinds() -> tuple[str, ...]:
    return _READY_QUEUE_KINDS


def _ready_max_work_amplification_ratio() -> float:
    return _READY_MAX_WORK_AMPLIFICATION_RATIO


def _ready_accepted_expected_delta_sources() -> frozenset[str]:
    return _READY_ACCEPTED_EXPECTED_DELTA_SOURCES


def _ready_workspace_graph_modules() -> frozenset[str]:
    return _READY_WORKSPACE_GRAPH_MODULES


def _ready_unavailable_blocker_ids() -> frozenset[str]:
    return _READY_UNAVAILABLE_BLOCKER_IDS


# The deployment names the Nomos rollout audits require are repository
# configuration (`deployment_fleet.nomos_required` and
# `deployment_fleet.nomos_required_when_present` in .okg/coordination.yaml),
# read by `_nomos_required_deployment_names`; the package names none (#2823).


def _resolve_external_deployment_dir(explicit: str | None) -> Path:
    """Resolve the external okg-deployments directory for a rollout audit.

    Resolution order: explicit ``--deployment-dir`` → ``OKG_DEPLOYMENTS_DIR``
    → the fetched cache slot for the default repo. There is NO
    operator-specific default path: when none of these is configured we fail
    loudly with a structured error rather than silently pointing at a path
    that only exists on one machine (which would make the audit report
    migrated deployments "missing" everywhere else while passing locally)."""
    from okg.substrate.cli.fetch import DEFAULT_REPO_URL, cache_dir_for

    checked: list[str] = []
    if explicit:
        return Path(explicit)
    checked.append("--deployment-dir")
    env_dir = os.environ.get("OKG_DEPLOYMENTS_DIR")
    if env_dir:
        return Path(env_dir)
    checked.append("OKG_DEPLOYMENTS_DIR")
    repo_url = os.environ.get("OKG_DEPLOYMENTS_REPO", DEFAULT_REPO_URL)
    if repo_url:
        cache = cache_dir_for(repo_url)
        checked.append(f"fetched cache ({cache})")
        if cache.exists():
            return cache
    else:
        checked.append("OKG_DEPLOYMENTS_REPO")
    from okg.substrate.errors import ExternalDeploymentsDirNotConfigured

    raise ExternalDeploymentsDirNotConfigured(
        "external okg-deployments repo not configured: no --deployment-dir, "
        "no OKG_DEPLOYMENTS_DIR, and no fetched cache. The audit cannot tell "
        "whether migrated deployments are present.",
        data={"checked": checked},
        fix_hint=(
            "pass --deployment-dir <path>, export OKG_DEPLOYMENTS_DIR, or run "
            "`okg fetch-deployments --repo <url>` (or with OKG_DEPLOYMENTS_REPO "
            "set) to populate the cache"
        ),
    )


def _dogfood_local_profile_is_authoritative(args: argparse.Namespace) -> bool:
    """Return whether `.okg/local.env` should beat ambient process env.

    Generic deployment commands keep the older precedence rule: explicit
    process env wins. Dogfood readiness is different because the local profile
    is the deployment authority for the dedicated dogfood database, while agent
    shells can inherit stale OKG_DSN values from earlier sessions.
    """
    if str(getattr(args, "profile", "") or "") == "dogfood":
        return True
    return str(getattr(args, "dogfood_subcmd", "") or "") in {
        "status",
        "doctor",
    }


def _dogfood_local_profile_command_prefix() -> str:
    """Shell prefix for dogfood commands that must ignore ambient OKG_DSN."""
    return "env -u OKG_DSN "


def _ready_checks_flag(readiness_profile: str | None) -> str:
    """`` --checks <profile>`` for a printed `deployment ready` command.

    Every printed command re-runs the checks this run used (#3958). A name the
    CLI does not accept (an environment policy such as ``org-private`` reaches
    the live checks through the environment readiness gate) gets no flag, so
    the command runs the default ``general`` checks rather than failing to
    parse.
    """
    if readiness_profile in READY_CHECKS_CHOICES:
        return f" --checks {shlex.quote(str(readiness_profile))}"
    return ""


def _recommended_dsn_commands(
    *,
    deployment: str,
    unresolved: list[str],
    resolved_dsn: str | None,
    suggested_dsn: str,
    show_values: bool,
) -> list[str]:
    """Build the `recommended_commands` block of an env or ready report.

    The suggested DSN carries a password. `recommended_commands` is JSON an
    operator pastes into tickets and chat, and the payload redacts
    `suggested_isolated_dsn` unless `--show-values` is given, so the same
    value must not leak through the commands beside it (#2213). Without the
    flag the operator supplies the password. Every producer -- `okg
    deployment env`, `okg deployment ready` and `okg dogfood` -- builds its
    commands here, so a further copy cannot reopen the hole.
    """
    shown_suggested_dsn = (
        suggested_dsn if show_values else redact_dsn(suggested_dsn)
    )
    if unresolved:
        first_missing = str(unresolved[0])
        write_local_command = (
            f"{okg_prefix()} deployment env "
            f"{shlex.quote(deployment)} --write-local "
            f"--env-name {shlex.quote(first_missing)} "
            f"--dsn {shlex.quote(shown_suggested_dsn)}"
        )
        export_command = (
            f"export {first_missing}={shlex.quote(shown_suggested_dsn)}"
        )
        return [write_local_command, export_command]
    if resolved_dsn:
        show_command = (
            f"{okg_prefix()} deployment show {shlex.quote(deployment)} --json"
        )
        return [show_command]
    return []


def _load_main_helpers() -> None:
    """Lazily import ``cli.main`` helpers + ``SubstrateError``.

    Idempotent and self-healing: the per-slot ``is None`` guards below
    repopulate any slot a test monkeypatch reset to ``None`` on teardown,
    so this never gets locked into a half-populated table. (A one-shot
    ``_HELPERS_LOADED`` short-circuit used to strand the
    ``_resolve_deployment_dsn`` slot at ``None`` after a sibling test
    patched and restored it — surfacing later as ``'NoneType' object is
    not callable`` under xdist; see deployment-release regression.)
    """
    global _emit_error, _emit_ok, _json_safe
    global _resolve_deployment_dsn, _resolve_deployment_dsn_from_dir
    global SubstrateError
    from okg.substrate.cli.main import (
        _emit_error as _e,
        _emit_ok as _o,
        _json_safe as _j,
        _resolve_deployment_dsn as _rd,
        _resolve_deployment_dsn_from_dir as _rdfd,
    )
    from okg.substrate.errors import SubstrateError as _se
    # Only populate slots that haven't been set yet — tests
    # monkeypatch these module-level names BEFORE the first
    # handler invocation, and `_load_main_helpers()` running
    # afterwards must not clobber the test stub.
    if _emit_error is None:
        _emit_error = _e
    if _emit_ok is None:
        _emit_ok = _o
    if _json_safe is None:
        _json_safe = _j
    if _resolve_deployment_dsn is None:
        _resolve_deployment_dsn = _rd
    if _resolve_deployment_dsn_from_dir is None:
        _resolve_deployment_dsn_from_dir = _rdfd
    if SubstrateError is None:
        SubstrateError = _se


def manifest_refusal(exc: Exception, manifest: str | Path | None = None):
    """One-line CLI error for a deployment manifest the loader refused.

    The loader refuses with ``DeploymentConfigError``, with a typed
    ``SubstrateError`` from a block parser (``SearchProfileConfigInvalid``,
    ``ToolProfileConfigInvalid``), or with ``ValueError`` for a missing
    required key such as ``schema_dir``. The message already names the key
    and the manifest, so the error keeps only the message and a typed code:
    a ``data`` payload would print a second line (#2713).

    A file that is not valid YAML (``yaml.YAMLError``) or cannot be read
    (``OSError``) gets a line that names the file and the parse position or
    the operating-system error; PyYAML's own message spans several lines.
    ``manifest`` is the manifest's path or the deployment name the caller
    passed; it tells an unreadable manifest from an unreadable path the
    manifest declares (``schema_dir`` in a directory okg cannot enter), and
    names the file a non-UTF-8 byte came from.
    """
    import yaml

    from okg.substrate.cli.main import _normalize_cli_error
    from okg.substrate.errors import SubstrateError as _SubstrateError

    if isinstance(exc, yaml.YAMLError):
        refusal = _SubstrateError(_yaml_error_line(exc))
        refusal.code = "deployment_manifest_invalid_yaml"
        return refusal
    manifest_path = _manifest_path_for_refusal(manifest)
    if isinstance(exc, OSError):
        reason = exc.strerror or str(exc)
        failed = Path(exc.filename) if exc.filename else None
        if failed is None or manifest_path is None or _same_path(
            failed, manifest_path,
        ):
            where = f" {failed}" if failed else ""
            message = f"cannot read deployment manifest{where}: {reason}"
        else:
            message = (
                f"cannot read {failed} while loading deployment manifest "
                f"{manifest_path}: {reason}"
            )
        refusal = _SubstrateError(message)
        refusal.code = "deployment_manifest_unreadable"
        return refusal
    if isinstance(exc, UnicodeDecodeError):
        where = f" {manifest_path}" if manifest_path else ""
        refusal = _SubstrateError(
            f"deployment manifest{where} is not UTF-8 text: {exc.reason} "
            f"at byte {exc.start}"
        )
        refusal.code = "deployment_manifest_invalid_yaml"
        return refusal
    normalized = _normalize_cli_error(exc)
    refusal = _SubstrateError(normalized.message)
    refusal.code = normalized.code
    return refusal


def _manifest_path_for_refusal(manifest: str | Path | None) -> Path | None:
    """The manifest file behind a name or path, or None if it cannot be found.

    Only called after the loader refused the manifest, so the file was found
    once; this reads no content.
    """
    if manifest is None:
        return None
    from okg.substrate.cli._env_helpers import _deployment_manifest_path_for_env
    from okg.substrate.errors import SubstrateError as _SubstrateError

    try:
        return _deployment_manifest_path_for_env(str(manifest))
    except _SubstrateError:
        return None


def _same_path(a: Path, b: Path) -> bool:
    try:
        return a.resolve() == b.resolve()
    except OSError:
        return a == b


def _yaml_error_line(exc: Exception) -> str:
    """One line for a PyYAML parse error: file, line, column, problem."""
    mark = getattr(exc, "problem_mark", None)
    problem = getattr(exc, "problem", None)
    context = getattr(exc, "context", None)
    if mark is None or not problem:
        detail = " ".join(str(exc).split())
        return f"deployment manifest is not valid YAML: {detail}"
    context_mark = getattr(exc, "context_mark", None)
    started = (
        f" started at line {context_mark.line + 1}"
        if context_mark is not None and context_mark.line != mark.line
        else ""
    )
    during = f" ({context}{started})" if context else ""
    return (
        f"deployment manifest {mark.name} is not valid YAML at line "
        f"{mark.line + 1}, column {mark.column + 1}: {problem}{during}"
    )


def manifest_load_errors() -> tuple[type[BaseException], ...]:
    """Every exception a deployment-manifest load refuses with (#2713).

    Callers catch these around the manifest load only, never around later
    work, so an unrelated ``ValueError`` or ``OSError`` still surfaces.
    """
    import yaml

    from okg.substrate.deployments import DeploymentConfigError
    from okg.substrate.errors import SubstrateError as _SubstrateError

    return (
        DeploymentConfigError, _SubstrateError, ValueError,
        yaml.YAMLError, OSError,
    )


def _cmd_deployment_show(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment show <name>` — print the resolved manifest.

    Includes the ``runtime:`` block (DBOS configuration) which Phase 1
    of `rebuild-substrate-on-dbos` adds. The block defaults to a
    disabled-by-default RuntimeConfig when the manifest does not
    declare one.
    """
    _load_main_helpers()
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        manifest = resolve_deployment(args.deployment)
    except DeploymentResolutionError as e:
        return _emit_error(SubstrateError(str(e)), as_json=as_json)
    except manifest_load_errors() as e:
        # A refused manifest is a one-line error, not a traceback (#2713).
        return _emit_error(
            manifest_refusal(e, args.deployment), as_json=as_json,
        )

    payload = {
        "name": manifest.name,
        "description": manifest.description,
        "deployment_dir": str(manifest.deployment_dir),
        "schema_dir": str(manifest.schema_dir),
        "source_registry": str(manifest.source_registry),
        "enricher_registry": (
            str(manifest.enricher_registry)
            if manifest.enricher_registry else None
        ),
        # Redact the password in BOTH the JSON and human output —
        # `okg deployment describe` lands in scrollback, CI logs, and
        # screen-shares. Host/port/db/user stay visible for debugging.
        # Per `harden-okg-network-and-secrets`.
        "postgres_dsn": (
            redact_dsn(manifest.postgres_dsn)
            if manifest.postgres_dsn else manifest.postgres_dsn
        ),
        "modules": list(manifest.modules),
        "runtime": manifest.runtime.as_dict(),
        "chronos": dict(manifest.chronos or {}),
        "release": dict(manifest.release or {}),
        "nomos": dict(manifest.nomos or {}),
        "readiness": dict(manifest.effective_raw.get("readiness") or {}),
    }

    if as_json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"deployment: {payload['name']}")
        if payload["description"]:
            print(f"  description: {payload['description']}")
        print(f"  deployment_dir: {payload['deployment_dir']}")
        print(f"  schema_dir:     {payload['schema_dir']}")
        print(f"  source_registry: {payload['source_registry']}")
        if payload["enricher_registry"]:
            print(
                f"  enricher_registry: {payload['enricher_registry']}"
            )
        if payload["postgres_dsn"]:
            print(f"  postgres_dsn: {payload['postgres_dsn']}")
        if payload["modules"]:
            print(f"  modules: {', '.join(payload['modules'])}")
        if payload["chronos"]:
            print("  chronos:")
            for k, v in payload["chronos"].items():
                print(f"    {k}: {v!r}")
        if payload["release"]:
            print("  release:")
            for k, v in payload["release"].items():
                print(f"    {k}: {v!r}")
        print("  runtime:")
        for k, v in payload["runtime"].items():
            print(f"    {k}: {v!r}")
    return 0


def _cmd_deployment_env(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Inspect or write a deployment's local environment contract.

    Lives here rather than `cli/main.py` with the other deployment command
    handlers. The env helpers (`_env_refs` etc.) live in
    `cli/_env_helpers.py` so this handler and top-level DSN resolution can
    share them.
    """
    _load_main_helpers()
    env_mode, deployment_arg, unexpected_arg = _deployment_env_mode(args)
    if env_mode == "doctor":
        return _cmd_deployment_env_doctor(
            args,
            as_json=as_json,
            deployment=deployment_arg,
        )
    if env_mode == "extra":
        return _emit_error(
            SubstrateError(
                "`okg deployment env` accepts exactly one deployment argument",
                data={
                    "deployment": deployment_arg,
                    "unexpected": unexpected_arg,
                },
                fix_hint=(
                    f"use `{okg_prefix()} deployment env doctor <deployment> --json` "
                    "for the nested doctor report"
                ),
            ),
            as_json=as_json,
        )
    if not deployment_arg:
        return _emit_error(
            SubstrateError(
                "`okg deployment env` requires a deployment name",
                fix_hint=f"{okg_prefix()} deployment env <deployment> --json",
            ),
            as_json=as_json,
        )

    import os

    from okg.substrate.cli._env_helpers import (
        _deployment_manifest_path_for_env,
        _env_refs,
        _redact_env_value,
        _write_local_env_value,
    )
    from okg.substrate.local_env import (
        configured_local_env_path,
        load_local_env,
        parse_local_env,
    )
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        worktree_main_local_env_path,
    )

    from okg.substrate.deployments import check_manifest_keys

    try:
        manifest_path = _deployment_manifest_path_for_env(args.deployment)
        # The DSN below is read from the raw YAML so an unset `${VAR}` can be
        # reported, but the loader's refusals must still run first: a
        # misspelled key would otherwise report as a healthy manifest (#2713).
        try:
            check_manifest_keys(manifest_path)
        except manifest_load_errors() as exc:
            # One line, like show and doctor: a block parser's typed error
            # would otherwise print its fix_hint and data too, and an
            # unreadable file would get the local-env-file hint below.
            raise manifest_refusal(exc, manifest_path) from exc
        raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
        pg_block = raw_manifest.get("postgres") or {}
        raw_dsn = pg_block.get("dsn") if isinstance(pg_block, dict) else None
        if not isinstance(raw_dsn, str):
            raw_dsn = None
        required_env = _env_refs(raw_dsn)

        explicit_local_env = getattr(args, "local_env", None) or os.environ.get(
            "OKG_LOCAL_ENV"
        )
        if explicit_local_env:
            local_env_path = Path(explicit_local_env).expanduser().resolve()
        elif deployment_self_hosted_authoritative(args.deployment):
            # Self-hosted dogfood state belongs to the main checkout. A
            # worktree command may inspect or materialize the compatibility
            # projection, but must not create a competing per-worktree owner.
            local_env_path = (
                worktree_main_local_env_path() or configured_local_env_path()
            )
        else:
            local_env_path = configured_local_env_path()

        if getattr(args, "write_local", False):
            target_env = getattr(args, "env_name", None)
            if not target_env:
                if len(required_env) == 1:
                    target_env = required_env[0]
                elif "OKG_DSN" in required_env:
                    target_env = "OKG_DSN"
            if not target_env:
                raise SubstrateError(
                    "--env-name is required when the manifest does not "
                    "reference exactly one env var",
                    data={"required_env": required_env},
                    fix_hint="pass --env-name OKG_DSN",
                )
            if not getattr(args, "dsn", None):
                raise SubstrateError(
                    "--write-local requires --dsn",
                    fix_hint=(
                        "okg deployment env <deployment> --write-local "
                        "--dsn postgres://..."
                    ),
                )
            _write_local_env_value(local_env_path, target_env, args.dsn)

        local_values = (
            parse_local_env(local_env_path)
            if local_env_path.is_file()
            else {}
        )

        env_rows: list[dict[str, object]] = []
        values_for_resolution: dict[str, str] = {}
        for name in required_env:
            process_value = os.environ.get(name)
            local_value = local_values.get(name)
            if process_value:
                source = "process_env"
                value = process_value
                is_set = True
            elif local_value:
                source = "local_profile"
                value = local_value
                is_set = True
            else:
                source = "missing"
                value = ""
                is_set = False
            if is_set:
                values_for_resolution[name] = value
            env_rows.append({
                "name": name,
                "required": True,
                "set": is_set,
                "source": source,
                "value": _redact_env_value(
                    value if is_set else None,
                    show_values=args.show_values,
                ),
            })

        unresolved = [
            row["name"] for row in env_rows
            if row["required"] and not row["set"]
        ]
        if raw_dsn is None:
            resolved_dsn = None
        else:
            resolved_dsn = raw_dsn
            for name, value in values_for_resolution.items():
                resolved_dsn = resolved_dsn.replace(f"${{{name}}}", value)
            if _env_refs(resolved_dsn):
                resolved_dsn = None

        authority_resolution: dict[str, Any] | None = None
        if unresolved == ["OKG_DSN"]:
            from okg.substrate.deployment_authority import (
                deployment_self_hosted_authoritative,
                resolve_deployment_dsn_authority,
            )

            authority_resolution = resolve_deployment_dsn_authority(
                args.deployment,
                local_profile_authoritative=deployment_self_hosted_authoritative(
                    args.deployment
                ),
            )
            authority_dsn = authority_resolution.get("dsn")
            authority = authority_resolution.get("authority")
            authority_mode = (
                authority.get("mode")
                if isinstance(authority, dict)
                else "deployment_authority"
            )
            if isinstance(authority_dsn, str) and authority_dsn:
                resolved_dsn = authority_dsn
                values_for_resolution["OKG_DSN"] = authority_dsn
                unresolved = []
                for row in env_rows:
                    if row["name"] == "OKG_DSN":
                        row.update({
                            "set": True,
                            "source": authority_mode,
                            "value": _redact_env_value(
                                authority_dsn,
                                show_values=args.show_values,
                            ),
                        })

        ok = raw_dsn is not None and not unresolved and resolved_dsn is not None
        from okg.substrate.catalog.ownership import suggested_worktree_dsn
        suggested_dsn = suggested_worktree_dsn(
            deployment_name=str(raw_manifest.get("name") or args.deployment),
            start=manifest_path.parent,
        )
        recommended = _recommended_dsn_commands(
            deployment=str(args.deployment),
            unresolved=unresolved,
            resolved_dsn=resolved_dsn,
            suggested_dsn=suggested_dsn,
            show_values=bool(args.show_values),
        )

        profile_load = load_local_env(path=local_env_path)
        payload = {
            "ok": ok,
            "deployment": args.deployment,
            "manifest_path": str(manifest_path),
            "deployment_dir": str(manifest_path.parent),
            "local_env_path": str(local_env_path),
            "local_env_exists": local_env_path.is_file(),
            "local_env_loaded": sorted(profile_load.loaded),
            "local_env_skipped": profile_load.skipped,
            "postgres": {
                "declared": raw_dsn is not None,
                "raw_dsn": _redact_env_value(
                    raw_dsn, show_values=args.show_values,
                ),
                "required_env": required_env,
                "resolved": resolved_dsn is not None,
                "resolved_dsn": _redact_env_value(
                    resolved_dsn, show_values=args.show_values,
                ),
                "suggested_isolated_dsn": _redact_env_value(
                    suggested_dsn,
                    show_values=args.show_values,
                ),
            },
            "environment": env_rows,
            "unresolved": unresolved,
            "recommended_commands": recommended,
        }
        if authority_resolution:
            payload["authority"] = authority_resolution.get("authority")
            if authority_resolution.get("target"):
                payload["target"] = authority_resolution.get("target")

        if as_json:
            print(json.dumps(payload, indent=2))
        else:
            print(f"deployment: {args.deployment}")
            print(f"manifest: {manifest_path}")
            print(f"local_env: {local_env_path}")
            if raw_dsn is None:
                print("postgres.dsn: missing")
            elif resolved_dsn is None:
                print("postgres.dsn: unresolved")
            else:
                print("postgres.dsn: resolved")
            for row in env_rows:
                print(f"  {row['name']}: {row['source']}")
            if recommended:
                print("next:")
                for command in recommended:
                    print(f"  {command}")
        return 0
    except (OSError, ValueError) as exc:
        return _emit_error(
            SubstrateError(str(exc), fix_hint="check the local env file"),
            as_json=as_json,
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)


def _deployment_env_mode(
    args: argparse.Namespace,
) -> tuple[str, str | None, str | None]:
    deployment_arg = getattr(args, "deployment", None)
    extra_arg = getattr(args, "_env_extra_arg", None)
    if deployment_arg == "doctor":
        return "doctor", extra_arg, None
    if extra_arg is not None:
        return "extra", deployment_arg, extra_arg
    return "env", deployment_arg, None


def _cmd_deployment_env_doctor(
    args: argparse.Namespace,
    *,
    as_json: bool,
    deployment: str | None = None,
) -> int:
    """`okg deployment env doctor <deployment>` — env authority report."""

    from okg.substrate.deployment_env_doctor import (
        build_env_doctor_report,
        format_env_doctor_summary,
    )
    from okg.substrate.errors import SubstrateError

    if not deployment:
        return _emit_error(
            SubstrateError(
                "`okg deployment env doctor` requires a deployment name",
                fix_hint=f"{okg_prefix()} deployment env doctor <deployment> --json",
            ),
            as_json=as_json,
        )
    if getattr(args, "write_local", False) or getattr(args, "env_name", None):
        return _emit_error(
            SubstrateError(
                "`deployment env doctor` is read-only and does not write local env",
                data={
                    "deployment": deployment,
                    "write_local": bool(getattr(args, "write_local", False)),
                    "env_name": getattr(args, "env_name", None),
                },
                fix_hint=(
                    "use `okg deployment env <deployment> --write-local` "
                    "when you intentionally want to update local env"
                ),
            ),
            as_json=as_json,
        )
    try:
        payload = build_env_doctor_report(
            deployment,
            explicit_dsn=getattr(args, "dsn", None),
            local_env_path=getattr(args, "local_env", None),
            for_command=getattr(args, "for_command", "read"),
            show_values=bool(getattr(args, "show_values", False)),
        )
    except (OSError, ValueError) as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    if as_json:
        _emit_ok(payload, as_json=True)
    else:
        print(format_env_doctor_summary(payload))
    return 0 if payload.get("ok") else 1


def _cmd_deployment_new(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment new <name>` — write a loader-valid scaffold."""

    from okg.substrate.cli.apply_boundary import missing_apply_error

    if err := missing_apply_error(
        args,
        command="deployment new",
        fix_command="okg deployment new <name> --apply ...",
    ):
        return _emit_error(err, as_json=as_json)

    from okg.substrate.deployment_scaffold import create_deployment_scaffold
    from okg.substrate.deployments import DeploymentResolutionError
    from okg.substrate.errors import SubstrateError

    try:
        result = create_deployment_scaffold(
            args.name,
            deployments_root=getattr(args, "deployments_root", None),
            force=bool(getattr(args, "force", False)),
            yes=bool(getattr(args, "yes", False)),
        )
    except DeploymentResolutionError as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit_ok(result.json_payload(), as_json=as_json)
    return 0


def _cmd_deployment_plan(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment plan <deployment>` — inspect descriptor wiring."""

    from okg.substrate.deployment_lint import resolve_deployment_manifest_path
    from okg.substrate.deployment_wiring import resolve_deployment_wiring_plan
    from okg.substrate.errors import SubstrateError

    manifest_path = resolve_deployment_manifest_path(
        args.deployment,
        deployments_root=getattr(args, "deployments_root", None),
    )
    if manifest_path is None:
        return _emit_error(
            SubstrateError(
                f"deployment {args.deployment!r} not found",
                data={"deployment": args.deployment},
                fix_hint="check deployments/<name>/ or set OKG_DEPLOYMENTS_DIR",
            ),
            as_json=as_json,
        )
    plan = resolve_deployment_wiring_plan(
        manifest_path.parent,
        check_imports=not bool(getattr(args, "no_check_imports", False)),
    )
    payload = {"schema": "okg.deployment_plan.v1", **plan.as_dict()}
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _cmd_deployment_lint(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment lint <deployment>` — static authoring diagnostics."""

    from okg.substrate.deployment_lint import (
        format_diagnostic_summary,
        lint_deployment,
    )

    report = lint_deployment(
        args.deployment,
        deployments_root=getattr(args, "deployments_root", None),
        live=bool(getattr(args, "live", False)),
    )
    if as_json:
        _emit_ok(report, as_json=True)
    else:
        print(format_diagnostic_summary(report))
    return 0 if report.get("ok") else 1


def _cmd_deployment_compat(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment compat <deployment>` — offline compatibility check."""

    from okg.substrate.deployment_compat import (
        check_deployment_compat,
        format_compat_summary,
    )

    report = check_deployment_compat(
        args.deployment,
        deployments_root=getattr(args, "deployments_root", None),
        commands_file=getattr(args, "commands", None),
        against=getattr(args, "against", None),
    )
    if as_json:
        _emit_ok(report, as_json=True)
    else:
        print(format_compat_summary(report))
    return int(report["exit_code"])


def _cmd_deployments(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployments ...` — portfolio-level deployment workflows."""
    _load_main_helpers()

    from okg.substrate.deployment_contract import (
        audit_deployment_portfolio,
        format_portfolio_audit_summary,
    )
    from okg.substrate.errors import SubstrateError

    subcmd = getattr(args, "deployments_subcmd", None)
    if subcmd != "audit":
        return _emit_error(
            SubstrateError(
                "`okg deployments` requires a subcommand",
                fix_hint=f"{okg_prefix()} deployments audit --root .",
            ),
            as_json=as_json,
        )
    report = audit_deployment_portfolio(
        getattr(args, "root", "."),
        contract_level=getattr(args, "contract_level", "current"),
        include_archived=bool(getattr(args, "include_archived", False)),
    )
    if as_json:
        _emit_ok(report, as_json=True)
    else:
        print(format_portfolio_audit_summary(report))
    return 0 if report.get("ok") else 1


def _cmd_deployment_profile(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment profile ...` — inspect deployment profile packs."""

    from okg.substrate.deployment_profile_explain import (
        BUILTIN_V1_PROFILE_NAMES,
        resolve_profile_explain,
    )
    from okg.substrate.errors import SubstrateError

    subcmd = getattr(args, "profile_subcmd", None)
    if subcmd == "list":
        details = [
            resolve_profile_explain(name).to_dict()
            for name in BUILTIN_V1_PROFILE_NAMES
        ]
        _emit_ok(
            {
                "ok": True,
                "schema": "okg.deployment_profile.v1",
                "profiles": list(BUILTIN_V1_PROFILE_NAMES),
                "profile_details": [
                    {
                        "name": item["name"],
                        "version": item["version"],
                        "classification": item["classification"],
                        "classification_flags": item["classification_flags"],
                        "description": item["description"],
                    }
                    for item in details
                ],
            },
            as_json=as_json,
        )
        return 0
    if subcmd != "explain":
        return _emit_error(
            SubstrateError(
                "`okg deployment profile` requires a subcommand",
                fix_hint=f"{okg_prefix()} deployment profile explain <profile>",
            ),
            as_json=as_json,
        )
    try:
        explanation = resolve_profile_explain(args.profile)
    except Exception as exc:  # noqa: BLE001 - normalize profile loader errors.
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    if as_json:
        _emit_ok(
            {
                "ok": True,
                "schema": "okg.deployment_profile.explain.v1",
                "profile": explanation.to_dict(),
            },
            as_json=True,
        )
    else:
        print(explanation.to_text())
    return 0


#: `--worker-process-authority` values (decision 295, option a).
WORKER_PROCESS_AUTHORITY_LOCAL = "local"
WORKER_PROCESS_AUTHORITY_POD_READINESS = "kubernetes-pod-readiness"


def _ready_add_runtime_process_authority(
    payload: dict[str, Any],
    *,
    deployment: str,
    authority: str = WORKER_PROCESS_AUTHORITY_LOCAL,
) -> None:
    """Add the dogfood host's exact-one-worker process gate.

    The audit counts worker processes on THIS host, so it can only pass where
    the worker runs. A probe that runs in another pod (the Helm readiness test,
    issue #1164) passes `authority="kubernetes-pod-readiness"`: when it sees no
    worker here, the check is reported `skipped`, never `pass`, and names its
    authority, the runtime pod's own startup and readiness probes, which run
    this same check beside the worker (a Helm `--wait` install does not finish
    until that pod is Ready). Any worker it does see is still judged here, so a
    split executor stays a blocker. The authority is honoured only inside a
    Kubernetes pod (`KUBERNETES_SERVICE_HOST` set, as the kubelet sets it in
    every pod); elsewhere the check is measured here and records why.
    """
    from okg.substrate.cli.dogfood import _runtime_worker_process_audit

    process_audit = _runtime_worker_process_audit(deployment)
    payload["runtime_processes"] = process_audit
    process_status = str(process_audit.get("status") or "unavailable")
    in_pod = bool(os.environ.get("KUBERNETES_SERVICE_HOST"))
    if authority == WORKER_PROCESS_AUTHORITY_POD_READINESS and not in_pod:
        # Outside a Kubernetes pod there is no runtime pod readiness to defer
        # to, so the authority cannot hide a stopped worker on a host
        # deployment: the check is measured here as usual, and says why.
        process_audit = dict(
            process_audit,
            authority_ignored=(
                "kubernetes-pod-readiness applies only inside a Kubernetes "
                "pod (KUBERNETES_SERVICE_HOST is unset)"
            ),
        )
        payload["runtime_processes"] = process_audit
    if (
        authority == WORKER_PROCESS_AUTHORITY_POD_READINESS
        and in_pod
        and process_status == "not_observed"
    ):
        _ready_add_check(
            payload,
            "runtime.worker_processes",
            "skipped",
            (
                "not measurable from this pod: the runtime worker runs in the "
                "runtime pod, whose own readiness probe runs this check; that "
                "pod's Ready state is the authority"
            ),
            {
                "applicable": False,
                "authority": WORKER_PROCESS_AUTHORITY_POD_READINESS,
                "deferred_to": "runtime StatefulSet pod readiness probe",
                "local_observation": process_audit,
            },
        )
        return
    process_message = {
        "single_executor": "exactly one runtime worker process root is active",
        "split_executor": (
            "multiple independent runtime worker process roots are active"
        ),
        "not_observed": "no runtime worker process root is active",
        "unavailable": "runtime worker process authority could not be measured",
    }.get(
        process_status,
        "runtime worker process authority is not exactly one root",
    )
    _ready_add_check(
        payload,
        "runtime.worker_processes",
        "pass" if process_audit.get("ok") is True else "blocker",
        process_message,
        process_audit,
    )


def _cmd_deployment_ready(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment ready <name>` — agent graph readiness preflight.

    This command is intentionally diagnostic rather than a thin wrapper around
    the normal DSN resolver. If the deployment manifest references an unset
    env var, it returns a structured readiness report with a blocker instead
    of failing before agents can see the rest of the contract.
    """
    _load_main_helpers()

    # Type the identifier BEFORE any probe (#1289). An environment id here
    # would otherwise continue under a mixed identity and report policy,
    # source and generation blockers for a deployment that is healthy.
    try:
        _require_graph_deployment_name(
            str(args.deployment),
            correct_command=(
                f"{okg_prefix()} deployment ready {{deployment}} "
                "--checks dogfood --json"
            ),
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    try:
        env_payload, resolved_dsn = _deployment_ready_env_status(
            args,
            show_values=bool(getattr(args, "show_values", False)),
        )
    except (OSError, ValueError) as exc:
        return _emit_error(
            SubstrateError(str(exc), fix_hint="check the local env file"),
            as_json=as_json,
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)

    deployment_dir = Path(str(env_payload["deployment_dir"]))
    manifest = _ready_load_yaml_mapping(deployment_dir / "deployment.yaml")
    manifest_name = str(manifest.get("name") or "").strip()
    deployment_ref = str(args.deployment)
    deployment_name = manifest_name or deployment_ref

    payload: dict[str, Any] = {
        "ok": True,
        "deployment": deployment_ref,
        "deployment_name": deployment_name,
        "manifest_path": env_payload.get("manifest_path"),
        "deployment_dir": env_payload.get("deployment_dir"),
        "local_env_path": env_payload.get("local_env_path"),
        "environment": env_payload,
        "checks": [],
        "blockers": [],
        "warnings": [],
        # Issue #1290: probes whose state was never measured. Kept apart
        # from blockers so an operator is never sent to repair something
        # that was merely not read.
        "unknowns": [],
    }
    if deployment_name != deployment_ref:
        payload["deployment_ref"] = deployment_ref
    readiness_profile = getattr(args, "profile", "general") or "general"
    rollout = _ready_rollout_status(
        deployment=deployment_name,
        deployment_dir=deployment_dir,
        env_payload=env_payload,
        readiness_profile=readiness_profile,
    )
    payload["rollout"] = rollout
    _ready_add_check(
        payload,
        "rollout.workspace_graph",
        "pass" if rollout.get("ok") else "warning",
        "deployment rollout contract resolved",
        rollout,
    )
    nomos_audit = _ready_nomos_audit_status(
        deployment=deployment_name,
        deployment_dir=deployment_dir,
        readiness_profile=readiness_profile,
    )
    payload["nomos_audit"] = nomos_audit
    nomos_status = str(nomos_audit.get("status") or "")
    if nomos_status == "skipped":
        _ready_add_check(
            payload,
            "nomos.deployment_policy",
            "skipped",
            "Nomos deployment policy audit not required for this manifest",
            nomos_audit,
        )
    else:
        _ready_add_check(
            payload,
            "nomos.deployment_policy",
            "pass" if nomos_audit.get("ok") else "blocker",
            (
                "Nomos deployment policy audit passed"
                if nomos_audit.get("ok")
                else "Nomos deployment policy audit failed"
            ),
            nomos_audit,
        )
        if nomos_audit.get("ok") and nomos_audit.get("warnings"):
            _ready_add_check(
                payload,
                "nomos.deployment_policy.warnings",
                "warning",
                "Nomos deployment policy audit has warnings",
                {"warnings": nomos_audit.get("warnings")},
            )
    nomos_runtime = _ready_nomos_active_state_status(
        deployment=deployment_name,
        dsn=resolved_dsn,
        manifest=manifest,
        deployment_dir=deployment_dir,
        readiness_profile=readiness_profile,
    )
    payload["nomos_runtime"] = nomos_runtime
    if str(nomos_runtime.get("status") or "") == "skipped":
        _ready_add_check(
            payload,
            "nomos.active_state",
            "skipped",
            "Nomos runtime active state not required for this manifest",
            nomos_runtime,
        )
    else:
        _ready_add_check(
            payload,
            "nomos.active_state",
            "pass" if nomos_runtime.get("ok") else "blocker",
            _ready_nomos_active_state_message(nomos_runtime),
            nomos_runtime,
        )
    authority = (
        env_payload.get("authority")
        if isinstance(env_payload.get("authority"), dict)
        else {}
    )
    if authority:
        conflict_count = int(authority.get("conflict_count") or 0)
        mode = str(authority.get("mode") or "")
        if mode == "dogfood_local_profile":
            message = (
                "dogfood local profile is the deployment DSN authority"
            )
            if conflict_count:
                message = (
                    "dogfood local profile ignored conflicting process env"
                )
            _ready_add_check(
                payload,
                "env.authority",
                "pass",
                message,
                authority,
            )

    if env_payload.get("ok"):
        _ready_add_check(
            payload,
            "env.resolved_dsn",
            "pass",
            "deployment DSN resolved from manifest environment contract",
            {
                "required_env": env_payload.get("postgres", {}).get(
                    "required_env", []
                ),
                "source_rows": env_payload.get("environment", []),
            },
        )
    else:
        unresolved = list(env_payload.get("unresolved") or [])
        for name in unresolved:
            _ready_add_check(
                payload,
                f"env.{name}",
                "blocker",
                f"required environment variable {name} is not set",
                {"recommended_commands": env_payload.get("recommended_commands", [])},
            )
        _ready_add_check(
            payload,
            "database.live_checks",
            "skipped",
            "live readiness checks require a resolved deployment DSN",
        )

    if resolved_dsn:
        max_source_age = getattr(args, "max_source_age_seconds", None)
        if max_source_age is None:
            if readiness_profile == "dogfood":
                max_source_age = _READY_DOGFOOD_MAX_SOURCE_AGE_SECONDS
            elif readiness_profile == "ci":
                max_source_age = _READY_CI_MAX_SOURCE_AGE_SECONDS
            else:
                max_source_age = _READY_DEFAULT_MAX_SOURCE_AGE_SECONDS
        max_producer_age = getattr(args, "max_producer_age_seconds", None)
        if max_producer_age is None:
            max_producer_age = _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS
        payload["profile"] = {
            "name": readiness_profile,
            "max_source_age_seconds": int(max_source_age),
            "max_producer_age_seconds": int(max_producer_age),
        }
        live = _deployment_ready_live_checks(
            deployment=deployment_name,
            deployment_dir=deployment_dir,
            dsn=resolved_dsn,
            max_active_queue_depth=int(
                getattr(args, "max_active_queue_depth", 0) or 0
            ),
            max_source_age_seconds=int(max_source_age),
            readiness_profile=readiness_profile,
            max_producer_age_seconds=int(max_producer_age),
            skip_doctor=bool(getattr(args, "skip_doctor", False)),
        )
        if readiness_profile == "dogfood":
            live = _attach_stale_source_sync_recovery_plan(
                live=live,
                deployment=deployment_name,
                dsn=resolved_dsn,
            )
        payload["checks"].extend(live.get("checks", []))
        payload["blockers"].extend(live.get("blockers", []))
        payload["warnings"].extend(live.get("warnings", []))
        payload["unknowns"].extend(live.get("unknowns", []))
        payload["probes"] = list(live.get("probes", []))
        payload["deadline"] = live.get("deadline")
        if readiness_profile == "dogfood":
            _ready_add_runtime_process_authority(
                payload,
                deployment=deployment_name,
                authority=str(
                    getattr(args, "worker_process_authority", None) or "local"
                ),
            )
        for key in (
            "database",
            "code_drift",
            "generation",
            "queues",
            "source_freshness",
            "source_sync_status",
            "source_sync_liveness",
            "source_optimization",
            "source_sync_recovery_plan",
            "nomos_source_lineage",
            "doctor",
            "mcp",
        ):
            if key in live:
                payload[key] = live[key]
        from okg.substrate.deployment_apply import deployment_apply_health

        apply_health = deployment_apply_health(
            deployment=deployment_name,
            deployment_dir=deployment_dir,
            dsn=resolved_dsn,
        )
        payload["deployment_apply"] = apply_health
        _ready_add_check(
            payload,
            "deployment.apply",
            "pass" if apply_health.get("ok") else "blocker",
            (
                "deployment apply state is converged"
                if apply_health.get("ok")
                else apply_health.get("message")
                or "deployment apply is incomplete, failed, or stale"
            ),
            apply_health,
        )

    _ready_finalize_payload_state(payload)
    if bool(getattr(args, "explain", False)):
        from okg.substrate.deployment_lint import (
            attach_ready_explain_diagnostics,
            lint_deployment,
        )

        lint_env = dict(os.environ)
        for row in env_payload.get("environment") or []:
            if isinstance(row, Mapping) and row.get("set"):
                lint_env[str(row.get("name"))] = "<resolved>"
        lint_report = lint_deployment(deployment_dir, env=lint_env)
        payload = attach_ready_explain_diagnostics(
            payload,
            lint_report=lint_report,
        )
    payload["readiness_evidence"] = _ready_contract_evidence(
        payload,
        readiness_profile=readiness_profile,
    )
    from okg.substrate.db.connection import redact_value

    _emit_ok(redact_value(payload), as_json=as_json)
    return 0 if payload["ok"] else 1


def _ready_contract_evidence(
    payload: Mapping[str, Any],
    *,
    readiness_profile: str,
) -> dict[str, Any]:
    collected_at = datetime.now(timezone.utc)
    profile = (
        payload.get("profile")
        if isinstance(payload.get("profile"), Mapping)
        else {}
    )
    ttl_seconds = profile.get("max_source_age_seconds")
    if not isinstance(ttl_seconds, int) or ttl_seconds <= 0:
        ttl_seconds = None
    observed_at = collected_at.isoformat()
    slots = {
        "environment": _ready_contract_slot(
            payload,
            "environment",
            slot="environment",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "migrations": _ready_contract_slot(
            payload,
            "database",
            slot="migrations",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "latest_published_generation": _ready_contract_slot(
            payload,
            "generation",
            slot="latest_published_generation",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "source_freshness": _ready_contract_slot(
            payload,
            "source_freshness",
            slot="source_freshness",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "queues_runtime": _ready_contract_slot(
            payload,
            "queues",
            slot="queues_runtime",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "dlq": _ready_contract_slot(
            payload,
            "queues",
            slot="dlq",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "doctor": _ready_contract_slot(
            payload,
            "doctor",
            slot="doctor",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
        "mcp_read_alignment": _ready_contract_slot(
            payload,
            "mcp",
            slot="mcp_read_alignment",
            observed_at=observed_at,
            ttl_seconds=ttl_seconds,
        ),
    }
    return {
        "schema": "okg.deployment_readiness_evidence.v1",
        "status": _ready_contract_status(payload),
        "collected_at": collected_at.isoformat(),
        "ttl_seconds": ttl_seconds,
        "provenance": {
            "command": "okg deployment ready",
            "deployment": payload.get("deployment_name") or payload.get("deployment"),
            "profile": readiness_profile,
        },
        "slots": slots,
    }


def _ready_contract_status(payload: Mapping[str, Any]) -> str:
    if payload.get("ok") is True:
        if payload.get("warnings"):
            return "stale"
        return "clean"
    environment = payload.get("environment")
    if isinstance(environment, Mapping) and environment.get("ok") is False:
        return "unavailable"
    return "blocked"


def _ready_contract_slot(
    payload: Mapping[str, Any],
    payload_key: str,
    *,
    slot: str,
    observed_at: str,
    ttl_seconds: int | None,
) -> dict[str, Any]:
    value = payload.get(payload_key)
    if value is None:
        env = payload.get("environment")
        env_ok = (
            env.get("ok")
            if isinstance(env, Mapping)
            else payload.get("ok")
        )
        # `queues` and `database` are probed unconditionally, so their
        # absence means the probe never produced a result -- it does not mean
        # the facet was not required (issue #1515). Facets that a profile can
        # legitimately skip keep reporting `not_required`.
        always_probed = payload_key in {"queues", "database"}
        return {
            "status": (
                "unavailable"
                if env_ok is False or always_probed
                else "not_required"
            ),
            "observed_at": observed_at,
            **({"ttl_seconds": ttl_seconds} if ttl_seconds is not None else {}),
            "provenance": {"payload_key": payload_key},
        }
    if isinstance(value, Mapping):
        status, unmapped = _ready_contract_slot_status(
            payload_key, value, slot=slot,
        )
        provenance: dict[str, Any] = {"payload_key": payload_key}
        # A word outside the contract vocabulary reports "unavailable"; keep
        # what the probe actually said visible to the operator. `unmapped` is
        # non-None ONLY when this slot's verdict was read from that word, so
        # the provenance can never blame a word the verdict never consulted
        # (issue #1572).
        if unmapped is not None:
            provenance["reported_status"] = unmapped
        evidence: dict[str, Any] = {
            "status": status,
            "observed_at": observed_at,
            "provenance": provenance,
        }
        if ttl_seconds is not None:
            evidence["ttl_seconds"] = ttl_seconds
        if value.get("observed_at"):
            evidence["observed_at"] = value.get("observed_at")
        if value.get("checked_at"):
            evidence["observed_at"] = value.get("checked_at")
        return evidence
    # A payload that is present but not a mapping carries no measurement to
    # read (issue #1515). Calling that "clean" reports health from a shape
    # nothing was ever measured into; report it as unmeasured instead.
    return {
        "status": "unavailable",
        "observed_at": observed_at,
        **({"ttl_seconds": ttl_seconds} if ttl_seconds is not None else {}),
        "provenance": {
            "payload_key": payload_key,
            "reason": "payload_not_a_mapping",
        },
    }


def _ready_measured_count(value: Mapping[str, Any], key: str) -> int | None:
    """Return the measured integer at `key`, or None when it was not measured.

    A missing key and an explicit ``None`` both mean "not measured" here:
    the queue probe writes ``dlq_count: None`` when the dead-letter scan did
    not finish. Coercing either to zero would report an unread queue as
    empty (issue #1515).
    """
    if key not in value:
        return None
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    return raw


def _ready_queue_measurement_missing(
    value: Mapping[str, Any],
    *,
    slot: str,
) -> bool:
    """True when the measurement this queue slot reports on was not taken."""
    if slot == "dlq":
        if str(value.get("dlq_status") or "") == "unknown":
            return True
        return _ready_measured_count(value, "dlq_count") is None
    return (
        _ready_measured_count(value, "active_queue_depth") is None
        or _ready_measured_count(value, "max_active_queue_depth") is None
    )


def _ready_contract_slot_status(
    payload_key: str,
    value: Mapping[str, Any],
    *,
    slot: str,
) -> tuple[str, str | None]:
    """Return this slot's contract status and any word it could not map.

    The second element is the raw payload status word ONLY when this verdict
    was read from that word and the contract does not define it. Every other
    path returns None, so a caller recording it in provenance can never blame
    a word the verdict never consulted (issue #1572).
    """
    if payload_key == "generation":
        count_integrity = value.get("count_integrity")
        if not isinstance(count_integrity, Mapping):
            return "unavailable", None
        if count_integrity.get("ok") is not True:
            return (
                "unavailable"
                if count_integrity.get("status") == "unavailable"
                else "blocked"
            ), None
    if payload_key == "queues":
        # The counts this slot reports on are an INPUT to its verdict, so
        # they settle the slot before the shared summary status is read: an
        # empty or partial queues payload is unmeasured, never clean, and a
        # measured breach blocks even when the summary status is milder
        # (issue #1515). Both slots read the SAME payload, so each must
        # answer from its own measurement. This ordering is also what keeps a
        # depth breach visible now that "available" maps to clean (#1572).
        if _ready_queue_measurement_missing(value, slot=slot):
            return "unavailable", None
        if slot == "dlq":
            dlq_count = _ready_measured_count(value, "dlq_count")
            if dlq_count is not None and dlq_count > 0:
                return "blocked", None
        else:
            depth = _ready_measured_count(value, "active_queue_depth")
            bound = _ready_measured_count(value, "max_active_queue_depth")
            if depth is not None and bound is not None and depth > bound:
                return "blocked", None
    if payload_key == "database":
        # An empty `missing_relations` only means "nothing missing" when the
        # probe actually had a relation list to check against. Without
        # `required_relations` the empty list is an unrun probe, not a clean
        # database (issue #1515).
        required = value.get("required_relations")
        missing = value.get("missing_relations")
        if (
            not isinstance(required, list)
            or not required
            or not isinstance(missing, list)
        ):
            return "unavailable", None
        if missing:
            return "blocked", None
    raw_status = value.get("status")
    if isinstance(raw_status, str) and raw_status:
        return _normalize_ready_slot_status(raw_status)
    if payload_key == "environment":
        return ("clean" if value.get("ok") is True else "unavailable"), None
    if payload_key == "database":
        # Everything the database facet reports on was settled above; without
        # a summary status the measured, non-missing relation set is clean.
        return "clean", None
    if payload_key == "generation":
        # A generation id is minted before the publication gate runs, so its
        # presence says nothing about whether the generation was published.
        # Without an explicit status nothing is known: report that, never
        # "clean".
        return "unavailable", None
    if payload_key == "queues":
        # Measured, within bound, and with no summary status to report: the
        # counts above are the whole verdict.
        return "clean", None
    if payload_key == "source_freshness":
        if value.get("ok") is True:
            return "clean", None
        if value.get("stale"):
            return "stale", None
        if value.get("missing"):
            return "unavailable", None
    if payload_key in {"doctor", "mcp"}:
        if value.get("ok") is True:
            return "clean", None
        if value.get("ok") is False:
            raw_reason = str(value.get("reason") or "")
            if raw_reason.endswith("command_failed") or value.get("error"):
                return "command_failed", None
            return "blocked", None
    if value.get("ok") is True:
        return "clean", None
    if value.get("ok") is False:
        return "blocked", None
    return "unavailable", None


def _normalize_ready_slot_status(status: str) -> tuple[str, str | None]:
    """Map a producer status word onto the readiness contract vocabulary.

    Returns the contract status and, when the contract does not define the
    word, the raw word itself. The mapping lives with the auditor that reads
    it (``okg.substrate.deployment_contract``) so a producer word cannot be
    added on one side only. Fail-closed: an undefined word reports
    "unavailable", and the caller records the raw word in the slot provenance
    so the operator still sees what the probe said. Issue #1572.
    """

    from okg.substrate.deployment_contract import normalize_ready_slot_status

    return normalize_ready_slot_status(status)


def _attach_stale_source_sync_recovery_plan(
    *,
    live: dict[str, Any],
    deployment: str,
    dsn: str,
) -> dict[str, Any]:
    source_sync_status = live.get("source_sync_status")
    if not isinstance(source_sync_status, Mapping):
        return live
    from okg.substrate.source_sync_recovery import (
        plan_stale_source_sync_work_recovery,
    )

    recovery_plan = plan_stale_source_sync_work_recovery(
        deployment=deployment,
        dsn=dsn,
        source_sync_status=source_sync_status,
    )
    if recovery_plan:
        live = dict(live)
        live["source_sync_recovery_plan"] = recovery_plan
    return live


def _deployment_ready_env_status(
    args: argparse.Namespace,
    *,
    show_values: bool,
) -> tuple[dict[str, Any], str | None]:
    """Return the deployment env report plus the unredacted resolved DSN."""
    import os
    import shlex

    from okg.substrate.catalog.ownership import suggested_worktree_dsn
    from okg.substrate.cli._env_helpers import (
        _deployment_manifest_path_for_env,
        _env_refs,
        _redact_env_value,
    )
    from okg.substrate.local_env import (
        configured_local_env_path,
        load_local_env,
        parse_local_env,
    )

    explicit_dsn = getattr(args, "dsn", None)
    if explicit_dsn:
        manifest_path = _deployment_manifest_path_for_env(args.deployment)
        return {
            "ok": True,
            "deployment": args.deployment,
            "manifest_path": str(manifest_path),
            "deployment_dir": str(manifest_path.parent),
            "local_env_path": None,
            "local_env_exists": False,
            "local_env_loaded": [],
            "local_env_skipped": {},
            "postgres": {
                "declared": True,
                "raw_dsn": "<explicit>",
                "required_env": [],
                "resolved": True,
                "resolved_dsn": _redact_env_value(
                    explicit_dsn, show_values=show_values
                ),
            },
            "environment": [],
            "authority": {"mode": "explicit_dsn"},
            "unresolved": [],
            "recommended_commands": [],
        }, explicit_dsn

    if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR"):
        from okg.substrate.deployments import (
            require_postgres_dsn,
            resolve_deployment,
        )
        from okg.substrate.deployments.environment import load_environment

        descriptor_path = Path(
            os.environ["OKG_ENVIRONMENT_DESCRIPTOR"]
        ).expanduser().resolve()
        environment = load_environment(descriptor_path, environ=os.environ)
        manifest = resolve_deployment(args.deployment)
        resolved_dsn = require_postgres_dsn(
            manifest,
            deployment=args.deployment,
        )
        local_env_path = environment.runtime.local_env_file
        return {
            "ok": True,
            "deployment": args.deployment,
            "manifest_path": str(environment.definition_path),
            "deployment_dir": str(manifest.deployment_dir),
            "local_env_path": str(local_env_path),
            "local_env_exists": local_env_path.is_file(),
            "local_env_loaded": [],
            "local_env_skipped": {},
            "postgres": {
                "declared": True,
                "raw_dsn": "<sealed-environment>",
                "required_env": [],
                "resolved": True,
                "resolved_dsn": _redact_env_value(
                    resolved_dsn,
                    show_values=show_values,
                ),
            },
            "environment": [],
            "authority": {
                "mode": "sealed_environment",
                "environment_id": environment.id,
                "descriptor_path": str(descriptor_path),
                "definition_digest": environment.definition_digest,
            },
            "unresolved": [],
            "recommended_commands": [
                f"{okg_prefix()} deployment show {shlex.quote(args.deployment)} --json"
            ],
        }, resolved_dsn

    manifest_path = _deployment_manifest_path_for_env(args.deployment)
    raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
    pg_block = raw_manifest.get("postgres") or {}
    raw_dsn = pg_block.get("dsn") if isinstance(pg_block, dict) else None
    if not isinstance(raw_dsn, str):
        raw_dsn = None
    required_env = _env_refs(raw_dsn)

    local_env_path = (
        Path(args.local_env).expanduser().resolve()
        if getattr(args, "local_env", None)
        else configured_local_env_path()
    )
    local_values = (
        parse_local_env(local_env_path) if local_env_path.is_file() else {}
    )
    local_profile_authoritative = _dogfood_local_profile_is_authoritative(args)

    env_rows: list[dict[str, object]] = []
    values_for_resolution: dict[str, str] = {}
    authority_conflicts: list[dict[str, object]] = []
    for name in required_env:
        process_value = os.environ.get(name)
        local_value = local_values.get(name)
        if (
            local_profile_authoritative
            and local_value
        ):
            source = "local_profile"
            value = local_value
            is_set = True
            if process_value and process_value != local_value:
                authority_conflicts.append({
                    "name": name,
                    "process_env_ignored": True,
                    "process_value": _redact_env_value(
                        process_value,
                        show_values=show_values,
                    ),
                    "local_profile_value": _redact_env_value(
                        local_value,
                        show_values=show_values,
                    ),
                    "reason": "dogfood_local_profile_authority",
                })
        elif process_value:
            source = "process_env"
            value = process_value
            is_set = True
        elif local_value:
            source = "local_profile"
            value = local_value
            is_set = True
        else:
            source = "missing"
            value = ""
            is_set = False
        if is_set:
            values_for_resolution[name] = value
        env_rows.append({
            "name": name,
            "required": True,
            "set": is_set,
            "source": source,
            "value": _redact_env_value(
                value if is_set else None,
                show_values=show_values,
            ),
        })

    unresolved = [
        row["name"] for row in env_rows
        if row["required"] and not row["set"]
    ]
    resolved_dsn: str | None
    if raw_dsn is None:
        resolved_dsn = None
    else:
        resolved = raw_dsn
        for name, value in values_for_resolution.items():
            resolved = resolved.replace(f"${{{name}}}", value)
        resolved_dsn = None if _env_refs(resolved) else resolved

    authority_resolution: dict[str, Any] | None = None
    if unresolved == ["OKG_DSN"]:
        from okg.substrate.deployment_authority import (
            resolve_deployment_dsn_authority,
        )

        authority_resolution = resolve_deployment_dsn_authority(
            args.deployment,
            local_profile_authoritative=local_profile_authoritative,
        )
        authority_dsn = authority_resolution.get("dsn")
        authority = authority_resolution.get("authority")
        authority_mode = (
            authority.get("mode")
            if isinstance(authority, dict)
            else "deployment_authority"
        )
        if isinstance(authority_dsn, str) and authority_dsn:
            resolved_dsn = authority_dsn
            values_for_resolution["OKG_DSN"] = authority_dsn
            unresolved = []
            for row in env_rows:
                if row["name"] == "OKG_DSN":
                    row.update({
                        "set": True,
                        "source": authority_mode,
                        "value": _redact_env_value(
                            authority_dsn,
                            show_values=show_values,
                        ),
                    })

    ok = raw_dsn is not None and not unresolved and resolved_dsn is not None
    suggested_dsn = suggested_worktree_dsn(
        deployment_name=str(raw_manifest.get("name") or args.deployment),
        start=manifest_path.parent,
    )
    recommended = _recommended_dsn_commands(
        deployment=str(args.deployment),
        unresolved=unresolved,
        resolved_dsn=resolved_dsn,
        suggested_dsn=suggested_dsn,
        show_values=show_values,
    )

    profile_load = load_local_env(
        path=local_env_path,
        override=local_profile_authoritative,
    )
    if authority_resolution and isinstance(
        authority_resolution.get("authority"), dict,
    ):
        authority_status = dict(authority_resolution["authority"])
        if authority_conflicts:
            authority_status["conflict_count"] = len(authority_conflicts)
            authority_status["conflicts"] = authority_conflicts
        if isinstance(authority_resolution.get("target"), dict):
            authority_status["target"] = authority_resolution["target"]
    else:
        authority_status = {
            "mode": (
                "dogfood_local_profile"
                if local_profile_authoritative else "process_env_precedence"
            ),
            "local_profile_authoritative": local_profile_authoritative,
            "conflict_count": len(authority_conflicts),
            "conflicts": authority_conflicts,
        }
    payload = {
        "ok": ok,
        "deployment": args.deployment,
        "manifest_path": str(manifest_path),
        "deployment_dir": str(manifest_path.parent),
        "local_env_path": str(local_env_path),
        "local_env_exists": local_env_path.is_file(),
        "local_env_loaded": sorted(profile_load.loaded),
        "local_env_skipped": profile_load.skipped,
        "postgres": {
            "declared": raw_dsn is not None,
            "raw_dsn": _redact_env_value(raw_dsn, show_values=show_values),
            "required_env": required_env,
            "resolved": resolved_dsn is not None,
            "resolved_dsn": _redact_env_value(
                resolved_dsn,
                show_values=show_values,
            ),
            **(
                {
                    "suggested_isolated_dsn": _redact_env_value(
                        suggested_dsn,
                        show_values=show_values,
                    ),
                }
                if unresolved
                else {}
            ),
        },
        "environment": env_rows,
        "authority": authority_status,
        "unresolved": unresolved,
        "recommended_commands": recommended,
    }
    return payload, resolved_dsn


def _ready_postgres_capacity_check_level(
    capacity: Mapping[str, Any],
    *,
    readiness_profile: str,
) -> str:
    """Sealed capacity is fail-closed in every readiness profile."""
    del readiness_profile
    if not bool(capacity.get("applicable", True)):
        return "skipped"
    return "pass" if bool(capacity.get("ok")) else "blocker"


def _deployment_ready_live_checks(
    *,
    deployment: str,
    source_namespace: str | None = None,
    deployment_dir: Path,
    dsn: str,
    max_active_queue_depth: int,
    max_source_age_seconds: int,
    readiness_profile: str = "general",
    max_producer_age_seconds: int = _READY_DOGFOOD_MAX_PRODUCER_AGE_SECONDS,
    skip_doctor: bool = False,
    deadline: "ReadinessDeadline | None" = None,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    # Issue #1290: the run is bounded, every database probe is bounded, and
    # a probe that runs out of time reads `unknown` rather than taking the
    # rest of the evidence with it.
    if deadline is None:
        deadline = ReadinessDeadline()
    payload: dict[str, Any] = {
        "checks": [],
        "blockers": [],
        "warnings": [],
        "unknowns": [],
        "probes": [],
        "deadline": deadline.to_dict(),
    }
    # Host-filesystem headroom sentinel (PACT bound-host-storage-pressure).
    # Deliberately FIRST — before the database connect — so an ENOSPC'd
    # host still reports WHY the database is down (the 2026-07-17 incident
    # shape: Postgres PANIC was the first symptom). A `blocker` verdict is
    # a readiness blocker (dogfood trust degrades; next_action names the
    # reclaim runbook); `warn` is a warning; an undeclared threshold pair
    # is a loud misconfiguration warning, never a silent default (P0).
    try:
        from okg.substrate.runtime.host_headroom import (
            check_host_disk_headroom,
        )

        headroom = check_host_disk_headroom(
            deployment=deployment, deployment_dir=deployment_dir,
        )
        payload["host_storage"] = headroom.to_dict()
        if headroom.status == "blocker":
            _ready_add_check(
                payload,
                "host.storage_headroom",
                "blocker",
                headroom.message,
                headroom.to_dict(),
            )
        elif headroom.status in {"warn", "misconfigured", "error"}:
            _ready_add_check(
                payload,
                "host.storage_headroom",
                "warning",
                headroom.message,
                headroom.to_dict(),
            )
        else:
            _ready_add_check(
                payload,
                "host.storage_headroom",
                "pass",
                headroom.message,
                headroom.to_dict(),
            )
    except Exception as exc:  # noqa: BLE001 - the sentinel must not take down readiness; its failure is itself a loud warning.
        payload["host_storage"] = {
            "ok": False,
            "code": "host_headroom_probe_failed",
            "message": str(exc),
        }
        _ready_add_check(
            payload,
            "host.storage_headroom",
            "warning",
            f"host headroom sentinel failed: {exc}",
            payload["host_storage"],
        )
    # Deployed-code drift probe. The 07-12 okg-workspace incident left the
    # sync root detached on unmerged hotfixes and the deployment ran 313
    # commits behind for six days -- a doctor check nobody types is no signal,
    # so readiness carries it. Dogfood treats any measured upstream movement
    # as stale context and an unmeasurable authority as unavailable. General
    # profiles retain the advisory warning because they do not declare the
    # exact-release dogfood contract.
    try:
        from okg.substrate.runtime.deployment_drift import (
            check_deployment_code_drift,
        )

        drift = check_deployment_code_drift(deployment=deployment)
        payload["code_drift"] = drift.to_dict()
        drift_unavailable = drift.status in {"error", "misconfigured"}
        drift_behind = bool(
            isinstance(drift.commits_behind, int)
            and drift.commits_behind > 0
        )
        dogfood_drift_blocker = (
            readiness_profile == "dogfood"
            and (drift_unavailable or drift_behind or not drift.ok)
        )
        _ready_add_check(
            payload,
            "deployment.code_drift",
            (
                "blocker"
                if dogfood_drift_blocker
                else "pass"
                if drift.ok and not drift_behind
                else "warning"
            ),
            "; ".join(drift.problems)
            or (
                f"deployed revision is {drift.commits_behind} commit(s) "
                f"behind {drift.base_ref}"
                if drift_behind
                else (
                    "sync_root is clean and at the installed okg build "
                    f"{drift.installed_revision}"
                )
                if drift.mode == "pinned"
                else "deployed revision is merged, tracked, and current"
            ),
            drift.to_dict(),
        )
    except Exception as exc:  # noqa: BLE001 - normalize probe failure into fail-closed readiness evidence.
        payload["code_drift"] = {
            "ok": False,
            "status": "error",
            "code": "code_drift_probe_failed",
            "message": str(exc),
        }
        _ready_add_check(
            payload,
            "deployment.code_drift",
            "blocker" if readiness_profile == "dogfood" else "warning",
            f"code-drift probe failed: {exc}",
            payload["code_drift"],
        )
    try:
        with db_connect(
            dsn, connect_timeout=5, autocommit=True,
        ) as conn, ready_statement_timeout(conn, deadline.per_probe_seconds):
            # A row-count limit does not bound execution time; only this
            # does. Readiness owns the connection, so a session-level
            # statement_timeout bounds every statement any probe issues,
            # including the ones inside helpers that never see this call.
            database = _ready_database_status(conn)
            payload["database"] = database
            if database["missing_relations"]:
                _ready_add_check(
                    payload,
                    "database.migrations",
                    "blocker",
                    "deployment database is missing required migrated relations",
                    database,
                )
                _ready_add_check(
                    payload,
                    "database.dependent_checks",
                    "skipped",
                    "generation, queue, source, doctor, and MCP checks require migrations",
                )
                return payload
            _ready_add_check(
                payload,
                "database.migrations",
                "pass",
                "required migrated relations are present",
                database,
            )
            try:
                postgres_capacity = _ready_postgres_capacity_status(
                    conn,
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                    connection_dsn=dsn,
                )
            except Exception as exc:  # noqa: BLE001 - readiness fails closed.
                postgres_capacity = {
                    "schema": "okg.postgres_capacity_readiness/v1",
                    "ok": False,
                    "status": "unavailable",
                    "problems": [
                        "PostgreSQL capacity authority could not be verified"
                    ],
                    "error": {
                        "type": type(exc).__name__,
                    },
                    # The profile this run judges by (#3958), not a fixed
                    # dogfood: the repair re-runs the same checks.
                    "repair_command": (
                        f"{okg_prefix()} deployment ready {deployment}"
                        f"{_ready_checks_flag(readiness_profile)} --json"
                    ),
                }
            database["postgres_capacity"] = postgres_capacity
            capacity_ok = bool(postgres_capacity.get("ok"))
            capacity_applicable = bool(
                postgres_capacity.get("applicable", True)
            )
            _ready_add_check(
                payload,
                "database.postgres_capacity",
                _ready_postgres_capacity_check_level(
                    postgres_capacity,
                    readiness_profile=readiness_profile,
                ),
                (
                    "deployment has no sealed Postgres capacity authority"
                    if not capacity_applicable
                    else "PostgreSQL worker capacity matches sealed authority"
                    if capacity_ok
                    else "PostgreSQL worker capacity is not authoritative"
                ),
                postgres_capacity,
            )
            # Issue #3143: on EVERY deployment, warn (never block) when the
            # worker pool is too small for TimescaleDB or job failures pile up.
            from okg.substrate.db import timescaledb_workers

            workers_level, workers_message, workers_details = (
                timescaledb_workers.timescaledb_worker_health(conn)
            )
            database["timescaledb_workers"] = workers_details
            _ready_add_check(
                payload,
                "database.timescaledb_workers",
                workers_level,
                workers_message,
                workers_details,
            )

            ok, generation = _ready_required_dependency(
                payload,
                check_id="generation.unavailable",
                dependency="_ready_generation_status",
                message="latest published generation check failed",
                call=lambda: _ready_generation_status(conn, deployment),
                deadline=deadline,
            )
            # Issue #1290: a probe that ran out of time was NOT measured.
            # `generation` is the one result other probes consume, so an
            # unknown here suppresses exactly its dependents and nothing
            # else — reporting "no latest published generation" off an
            # unmeasured probe is the fabricated-blocker shape of #1289.
            generation_unknown = False
            if not ok:
                if not _ready_probe_timed_out(generation):
                    return payload
                generation_unknown = True
                payload["generation"] = generation
            if not generation_unknown:
                payload["generation"] = generation
                if generation.get("status") != "published":
                    _ready_add_check(
                        payload,
                        "generation.latest_published",
                        "blocker",
                        "no latest published generation is available",
                        generation,
                    )
                else:
                    _ready_add_check(
                        payload,
                        "generation.latest_published",
                        "pass",
                        "latest published generation resolved",
                        generation,
                    )
                _ready_add_generation_count_integrity_check(payload, generation)

            if generation_unknown:
                # Running this with a null generation id is what turned one
                # unresolved identity into a cascade of policy blockers in
                # the 2026-08 recovery. Say "not measured" instead.
                _ready_skip_unknown_dependent(
                    payload,
                    check_id="nomos.source_lineage",
                    message=(
                        "Nomos source-lineage was not evaluated: it reads the "
                        "published generation, which was not measured"
                    ),
                    depends_on="_ready_generation_status",
                )
            else:
                ok, source_lineage = _ready_required_dependency(
                    payload,
                    check_id="nomos.source_lineage.unavailable",
                    dependency="_ready_nomos_source_lineage_status",
                    message="Nomos source-lineage readiness check failed",
                    call=lambda: _ready_nomos_source_lineage_status(
                        conn,
                        deployment_name=deployment,
                        readiness_profile=readiness_profile,
                        generation_id=generation.get("generation_id"),
                    ),
                    deadline=deadline,
                )
                if not ok and not _ready_probe_timed_out(source_lineage):
                    return payload
                if ok:
                    payload["nomos_source_lineage"] = source_lineage
                    _ready_add_check(
                        payload,
                        "nomos.source_lineage",
                        "pass" if source_lineage.get("ok") else "blocker",
                        (
                            "Nomos source-lineage enforcement is operational"
                            if source_lineage.get("ok")
                            else "Nomos source-lineage enforcement is not "
                            "operational"
                        ),
                        source_lineage,
                    )

            # A checkout other than this release driving the database is
            # invisible until something refuses. okg-workspace sat
            # unpublished for weeks behind a Nomos fingerprint mismatch
            # whose cause was simply that the sync-root checkout had
            # claimed ownership and activated a newer rollout baseline.
            # Report it as what it is, naming both writers.
            ok, writer_driver = _ready_required_dependency(
                payload,
                check_id="deployment.writer_driver.unavailable",
                dependency="_ready_writer_driver_status",
                message="writer-driver comparison failed",
                call=lambda: _ready_writer_driver_status(
                    conn,
                    deployment_name=deployment,
                    deployment_dir=deployment_dir,
                ),
                deadline=deadline,
            )
            payload["writer_driver"] = writer_driver
            if ok and writer_driver.get("status") not in (
                "no_recorded_writer",
                "unavailable",
            ):
                driver_ok = bool(writer_driver.get("ok"))
                _ready_add_check(
                    payload,
                    "deployment.writer_driver",
                    "pass" if driver_ok else "warning",
                    (
                        "the recorded writer matches the running release"
                        if driver_ok
                        else str(writer_driver.get("message") or "")
                    ),
                    writer_driver,
                )

            # fix-nomos-read-path-ux (req.readux.readiness-read-probe, #640):
            # prove the standard read subjects can actually read at the
            # active bundle, so "healthy but 100% deny" is a named blocker.
            ok, read_probe = _ready_required_dependency(
                payload,
                check_id="nomos.read_path.unavailable",
                dependency="_ready_nomos_read_probe_status",
                message="Nomos read-subject probe readiness check failed",
                call=lambda: _ready_nomos_read_probe_status(
                    conn,
                    deployment_name=deployment,
                    deployment_dir=deployment_dir,
                    readiness_profile=readiness_profile,
                ),
                deadline=deadline,
            )
            payload["nomos_read_path"] = read_probe
            if not ok:
                # Issue #1290: not measured, so it neither passes nor
                # blocks. No later probe reads this result — keep going.
                if not _ready_probe_timed_out(read_probe):
                    return payload
                read_probe = {}
            if not read_probe:
                pass
            elif str(read_probe.get("status") or "") == "skipped":
                _ready_add_check(
                    payload,
                    "nomos.read_path",
                    "skipped",
                    "Nomos read-subject probe not required for this manifest",
                    read_probe,
                )
            elif read_probe.get("ok"):
                _ready_add_check(
                    payload,
                    "nomos.read_path",
                    "pass",
                    "standard read subjects can read at the active Nomos bundle",
                    read_probe,
                )
            else:
                _ready_add_check(
                    payload,
                    "nomos.read_path",
                    "blocker",
                    "standard read subjects are denied at the active Nomos bundle",
                    read_probe,
                )

            ok, queues = _ready_required_dependency(
                payload,
                check_id="queues.unavailable",
                dependency="_ready_queue_status",
                message="queue readiness check failed",
                call=lambda: _ready_queue_status(
                    conn,
                    deployment_dir=deployment_dir,
                    max_active_queue_depth=max_active_queue_depth,
                ),
                deadline=deadline,
            )
            if not ok:
                if not _ready_probe_timed_out(queues):
                    return payload
                # Issue #1290: the queue probe was never measured, so it
                # neither passes nor blocks. Everything after it is
                # independent of it — keep going and report what CAN be
                # measured rather than suppressing it all.
                payload["queues"] = queues
                queues = {}
            else:
                payload["queues"] = queues
            if not queues:
                pass
            elif queues.get("dlq_status") == "unknown":
                # Queue health was measured; the dead-letter scan was not.
                # Report the measured half and name the unmeasured half.
                _ready_add_check(
                    payload,
                    "queues.dlq",
                    "unknown",
                    "dead-letter count was not measured within its deadline",
                    queues.get("dlq_unknown") or queues,
                )
                if queues.get("status") == "stale":
                    _ready_add_check(
                        payload,
                        "queues.stale",
                        "blocker",
                        "runtime queue has stale active work",
                        queues,
                    )
                elif (
                    int(queues.get("active_queue_depth") or 0)
                    > max_active_queue_depth
                ):
                    _ready_add_check(
                        payload,
                        "queues.active_depth",
                        "blocker",
                        "active queue depth exceeds readiness bound",
                        queues,
                    )
            elif queues.get("status") == "stale":
                _ready_add_check(
                    payload,
                    "queues.stale",
                    "blocker",
                    "runtime queue has stale active work",
                    queues,
                )
            elif queues["dlq_count"] > 0:
                # Check dead-letter before the generic ok==False branch: a
                # dead-letter queue now sets ok=False (single-point health
                # fix), so this must win to emit the DLQ-specific blocker
                # instead of a misleading "unavailable".
                _ready_add_check(
                    payload,
                    "queues.dlq",
                    "blocker",
                    "dead-letter queue is non-empty",
                    queues,
                )
            elif queues.get("ok") is False:
                _ready_add_check(
                    payload,
                    "queues.unavailable",
                    "blocker",
                    "runtime queue health is unavailable",
                    queues,
                )
            elif queues["active_queue_depth"] > max_active_queue_depth:
                _ready_add_check(
                    payload,
                    "queues.active_depth",
                    "blocker",
                    "active queue depth exceeds readiness bound",
                    queues,
                )
            else:
                _ready_add_check(
                    payload,
                    "queues.bounds",
                    "pass",
                    "queue depth and DLQ are within readiness bounds",
                    queues,
                )

            from okg.substrate.cli.deployment_readiness import (
                SourceRegistryUnloadable,
                _ready_undeclared_agent_context_sources,
            )

            registry_unloadable = False
            try:
                source_groups = _ready_source_groups_for_profile(
                    readiness_profile,
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                )
            except SourceRegistryUnloadable as exc:
                # Issue #3958 (standard): the registry is the only place the
                # baseline sources are declared, so a registry that does not
                # load is reported as itself -- red, by name -- and never as
                # the database failure the outer handler would make of it.
                registry_unloadable = True
                source_groups = {}
                _ready_add_check(
                    payload,
                    "sources.registry.unavailable",
                    "blocker",
                    "the source registry does not load, so the baseline "
                    "sources cannot be read",
                    {
                        "ok": False,
                        "status": "unavailable",
                        "code": "source_registry_unloadable",
                        "registry_path": str(exc.registry_path),
                        "error": type(exc.error).__name__,
                        "message": str(exc.error),
                    },
                )
            standard_runtime = (
                _ready_declared_runtime_status(deployment_dir)
                if readiness_profile == "standard"
                else None
            )

            undeclared = _ready_undeclared_agent_context_sources(
                readiness_profile,
                deployment=deployment,
                deployment_dir=deployment_dir,
                source_groups=source_groups,
            )
            if undeclared:
                # A registry mistake must not pass silently, and it must not
                # read as a failure either: warnings leave trust_level alone.
                _ready_add_check(
                    payload,
                    "sources.readiness_undeclared",
                    "warning",
                    "agent-context sources not declared in the source "
                    "registry were left out of readiness: "
                    + ", ".join(undeclared),
                    {"undeclared": undeclared},
                )
            if source_groups:
                source_aliases = _ready_source_aliases(deployment_dir)
                progress_freshness_sources = _ready_progress_freshness_sources(
                    deployment_dir,
                    readiness_profile,
                )
                ok, source_freshness = _ready_required_dependency(
                    payload,
                    check_id="sources.freshness.unavailable",
                    dependency="_ready_source_freshness_status",
                    message="source freshness readiness check failed",
                    call=lambda: _ready_source_freshness_status(
                        conn,
                        deployment=deployment,
                        source_namespace=source_namespace,
                        max_source_age_seconds=max_source_age_seconds,
                        source_groups=source_groups,
                        progress_freshness_sources=progress_freshness_sources,
                        source_aliases=source_aliases,
                    ),
                    deadline=deadline,
                )
                payload["source_freshness"] = source_freshness
                if undeclared and isinstance(source_freshness, dict):
                    source_freshness["undeclared"] = list(undeclared)
                if not ok:
                    # Issue #1290: not measured; nothing later reads it.
                    if not _ready_probe_timed_out(source_freshness):
                        return payload
                    source_freshness = {}
                if not source_freshness:
                    pass
                elif source_freshness["missing"] or source_freshness["stale"]:
                    _ready_add_check(
                        payload,
                        "sources.freshness",
                        "blocker",
                        "required source freshness is missing or stale",
                        source_freshness,
                    )
                else:
                    _ready_add_check(
                        payload,
                        "sources.freshness",
                        "pass",
                        "required source watermarks are fresh",
                        source_freshness,
                    )

            if (
                source_groups
                and standard_runtime is not None
                and standard_runtime in {"disabled", "not_declared"}
            ):
                # Issue #3958 (standard): sync liveness exists because a
                # worker should be syncing. A runtime that is off by
                # declaration, or undeclared (the runtime's own default is
                # off), has no producers to be live.
                reason = (
                    "runtime_disabled_by_config"
                    if standard_runtime == "disabled"
                    else "runtime_not_declared"
                )
                _ready_add_check(
                    payload,
                    "sources.sync_liveness",
                    "skipped",
                    "source-sync liveness is not applicable: the runtime is "
                    + (
                        "disabled by configuration"
                        if standard_runtime == "disabled"
                        else "not declared, which reads as disabled"
                    ),
                    {"status": "skipped", "reason": reason},
                )
            elif source_groups:
                ok, source_sync_liveness = _ready_required_dependency(
                    payload,
                    check_id="sources.sync_liveness.unavailable",
                    dependency="_ready_source_sync_liveness_status",
                    message="source-sync liveness readiness check failed",
                    call=lambda: _ready_source_sync_liveness_status(
                        dsn=dsn,
                        deployment=deployment,
                        deployment_dir=deployment_dir,
                        max_producer_age_seconds=max_producer_age_seconds,
                        readiness_profile=readiness_profile,
                        readiness_source_groups=source_groups,
                    ),
                    deadline=deadline,
                )
                if not ok:
                    # Issue #1290: not measured. Deliberately NOT stored --
                    # the source-sync status contract below is gated on this
                    # key, and composing it from unmeasured input is exactly
                    # the fabricated-blocker shape.
                    if not _ready_probe_timed_out(source_sync_liveness):
                        return payload
                else:
                    payload["source_sync_liveness"] = source_sync_liveness
            elif readiness_profile == "standard" and not registry_unloadable:
                # Issue #3958: a standard deployment that marks no source
                # `required_for_baseline` promises no freshness. Say so by
                # name rather than pass by omission, and run the advisory:
                # stale declared sources become a warning, which cannot
                # change trust_level. Chat and the console therefore show no
                # staleness for such a deployment (documented gap).
                not_checked = {
                    "status": "not_checked",
                    "reason": "no_baseline_sources_declared",
                }
                payload["source_freshness"] = dict(not_checked)
                _ready_add_check(
                    payload,
                    "sources.freshness",
                    "skipped",
                    "no source is marked required_for_baseline, so no source "
                    "is required to be fresh",
                    dict(not_checked),
                )
                _ready_add_check(
                    payload,
                    "sources.sync_liveness",
                    "skipped",
                    "source-sync liveness is checked for baseline sources, "
                    "and none is declared",
                    dict(not_checked),
                )
                from okg.substrate.cli.deployment_readiness import (
                    _ready_add_source_freshness_advisory,
                )

                _ready_add_source_freshness_advisory(
                    payload,
                    conn,
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                    max_source_age_seconds=max_source_age_seconds,
                    deadline=deadline,
                )

            ok, mcp = _ready_required_dependency(
                payload,
                check_id="mcp.graph_read.unavailable",
                dependency="_ready_mcp_status",
                message="MCP graph-read readiness check failed",
                call=lambda: _ready_mcp_status(conn, deployment),
                deadline=deadline,
            )
            payload["mcp"] = mcp
            from okg.substrate.cli.deployment_readiness import _ready_add_mcp_principal_check

            _ready_add_mcp_principal_check(payload, mcp)
            if not ok:
                # Issue #1290: not measured; nothing later reads it.
                if not _ready_probe_timed_out(mcp):
                    return payload
                mcp = {}
            if not mcp:
                pass
            elif not mcp.get("ok"):
                _ready_add_check(
                    payload,
                    "mcp.graph_read",
                    "blocker",
                    "MCP graph-read alignment failed",
                    mcp,
                )
            else:
                _ready_add_check(
                    payload,
                    "mcp.graph_read",
                    "pass",
                    "MCP graph reads resolve against a published generation",
                    mcp,
            )
            if isinstance(payload.get("source_sync_liveness"), dict):
                ok, source_sync_status = _ready_required_dependency(
                    payload,
                    check_id="sources.sync_status.unavailable",
                    dependency="_ready_build_source_sync_status_contract",
                    message="source-sync status contract composition failed",
                    call=lambda: _ready_build_source_sync_status_contract(
                        payload["source_sync_liveness"],
                        source_freshness=payload.get("source_freshness"),
                        generation=payload.get("generation"),
                        mcp=mcp,
                    ),
                    deadline=deadline,
                )
                if not ok:
                    # Issue #1290: not measured; nothing later reads it.
                    if not _ready_probe_timed_out(source_sync_status):
                        return payload
                else:
                    payload["source_sync_status"] = source_sync_status
                    _ready_add_source_sync_liveness_check(
                        payload,
                        payload["source_sync_status"],
                    )
            if (
                readiness_profile == "dogfood"
                and isinstance(payload.get("source_sync_liveness"), dict)
            ):
                ok, source_optimization = _ready_required_dependency(
                    payload,
                    check_id="sources.optimization_budget.unavailable",
                    dependency="_ready_source_optimization_status",
                    message="source optimization readiness check failed",
                    call=lambda: _ready_source_optimization_status(
                        deployment=deployment,
                        deployment_dir=deployment_dir,
                        dsn=dsn,
                        max_producer_age_seconds=max_producer_age_seconds,
                    ),
                    deadline=deadline,
                )
                payload["source_optimization"] = source_optimization
                if not ok:
                    # Issue #1290: not measured; nothing later reads it.
                    if not _ready_probe_timed_out(source_optimization):
                        return payload
                    source_optimization = {}
                if not source_optimization:
                    pass
                elif (
                    source_optimization.get("ok")
                    and source_optimization.get("status") == "warning"
                ):
                    _ready_add_check(
                        payload,
                        "sources.optimization_budget",
                        "warning",
                        (
                            "source optimization findings are advisory by "
                            "explicit deployment policy "
                            "(readiness.source_optimization.enforcement: warn)"
                            if source_optimization.get("enforcement") == "warn"
                            else "only latency-budget overruns are advisory by "
                            "explicit deployment policy (readiness."
                            "source_optimization.latency_budget_enforcement: "
                            "warn); every other finding still blocks"
                        ),
                        source_optimization,
                    )
                elif source_optimization.get("ok"):
                    _ready_add_check(
                        payload,
                        "sources.optimization_budget",
                        "pass",
                        "readiness-required source optimization evidence is within budget",
                        source_optimization,
                    )
                else:
                    _ready_add_check(
                        payload,
                        "sources.optimization_budget",
                        "blocker",
                        "readiness-required source optimization evidence violates budget or proof requirements",
                        source_optimization,
                    )
                # Issue #1291: publish capacity is its own release contract.
                # A near-timeout lock hold is a readiness warning (decision
                # 35a) instead of only a line in the worker log, and an
                # unmeasured hold reports unknown rather than passing by
                # omission.
                _ready_add_publisher_lock_headroom_check(
                    payload,
                    source_optimization or None,
                    (
                        generation.get("publisher_lock")
                        if isinstance(generation, Mapping) else None
                    ),
                )
            # Issue #2993: last, so an earlier probe's early return (already a
            # blocker) skips it and no earlier check's position moves.
            _ready_add_publish_block_check(payload, conn, deployment, deadline)
    except Exception as exc:  # noqa: BLE001 - readiness must report blockers.
        _ready_add_check(
            payload,
            "database.connectivity",
            "blocker",
            "could not connect to the deployment database",
            {"error": type(exc).__name__, "message": str(exc)},
        )
        return payload

    # #3958: standard runs the doctor too; `okg dogfood status` asks for it
    # only on request (`dogfood doctor`, `--details`), in every profile.
    if readiness_profile not in {"dogfood", "ci", "standard"}:
        doctor = {
            "ok": None,
            "status": "skipped",
            "reason": "doctor checks are not required for this readiness profile",
        }
        payload["doctor"] = doctor
        _ready_add_check(
            payload,
            "doctor.deployment",
            "skipped",
            "doctor check skipped for general readiness profile",
            doctor,
        )
        return payload

    ok, doctor = _ready_required_dependency(
        payload,
        check_id="doctor.okg_workspace.unavailable",
        dependency="_ready_doctor_status",
        message="deployment doctor readiness check failed",
        call=lambda: _ready_doctor_status(
            deployment,
            dsn=dsn,
            skip=skip_doctor,
        ),
        deadline=deadline,
    )
    payload["doctor"] = doctor
    if not ok:
        # Issue #1290: the doctor subprocess is the last probe; an unknown
        # here is recorded, not converted into a doctor failure.
        return payload
    if doctor.get("status") == "skipped":
        _ready_add_check(
            payload,
            "doctor.okg_workspace",
            "skipped",
            "doctor check skipped by operator flag",
        )
    elif doctor.get("ok"):
        _ready_add_check(
            payload,
            "doctor.okg_workspace",
            "pass",
            "deployment doctor passed",
            doctor,
        )
        _ready_add_paused_publisher_warnings(payload, doctor)
    else:
        _ready_add_check(
            payload,
            "doctor.okg_workspace",
            "blocker",
            "deployment doctor failed",
            doctor,
        )
    return payload


def _ready_add_paused_publisher_warnings(
    payload: dict[str, Any], doctor: Mapping[str, Any],
) -> None:
    """Carry a passing doctor's paused-publisher warning into readiness.

    A deliberately paused publisher leaves the deployment ready. Say so at
    the top level, where an operator reading readiness looks, rather than
    only inside the doctor's own report.
    """
    from okg.substrate.runtime.publisher_health import PAUSED_WARNING_CODES

    doctor_json = doctor.get("json")
    doctor_warnings = (
        doctor_json.get("warnings") if isinstance(doctor_json, Mapping) else None
    )
    for warning in doctor_warnings or []:
        if isinstance(warning, Mapping) and warning.get("code") in PAUSED_WARNING_CODES:
            _ready_add_check(
                payload,
                "publisher.schedule_paused",
                "warning",
                str(warning.get("message") or "publisher schedule is paused"),
                {"code": warning.get("code")},
            )


def _manifest_requires_nomos_runtime(manifest: Mapping[str, Any]) -> bool:
    # Single source of truth: honors the OKG_NOMOS_RUNTIME_ENFORCEMENT override
    # and the audit/enforce tri-state (audit also evaluates the boundary).
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )

    return manifest_nomos_enforcement_mode(manifest) != "off"


def _cmd_deployment_bug_report(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment bug-report` — preview/apply a GitHub issue."""
    _load_main_helpers()

    try:
        from okg.substrate.support.bug_report import (
            bug_report_preview_from_dict,
            build_bug_report_preview,
            create_github_issue,
        )

        if getattr(args, "preview_json_file", None):
            if not args.apply:
                raise SubstrateError(
                    "--preview-json-file requires --apply",
                    fix_hint=(
                        "review a preview JSON file, then pass "
                        "--preview-json-file with --apply"
                    ),
                )
            try:
                preview_data = json.loads(
                    Path(args.preview_json_file).read_text()
                )
            except (OSError, json.JSONDecodeError) as exc:
                raise SubstrateError(
                    f"could not read --preview-json-file "
                    f"{args.preview_json_file!r}: {exc}",
                    fix_hint="pass a readable preview JSON file",
                ) from exc
            preview = bug_report_preview_from_dict(preview_data)
        else:
            if not args.deployment:
                raise SubstrateError(
                    "--deployment is required unless --preview-json-file is used"
                )
            if not args.repo:
                raise SubstrateError(
                    "--repo/--issue-repo is required unless "
                    "--preview-json-file is used",
                    fix_hint="pass --repo owner/name",
                )

            dsn = _resolve_deployment_dsn(args)
            body_text = ""
            if getattr(args, "body_file", None):
                try:
                    body_text = Path(args.body_file).read_text()
                except OSError as exc:
                    raise SubstrateError(
                        f"could not read --body-file {args.body_file!r}: {exc}",
                        fix_hint="pass a readable UTF-8 text file",
                    ) from exc

            preview = build_bug_report_preview(
                deployment=args.deployment,
                repo=args.repo,
                deployment_repo=getattr(args, "deployment_repo", None),
                dsn=dsn,
                title=args.title,
                summary=args.summary,
                body_text=body_text,
                labels=tuple(args.label or ()),
            )
        if args.apply:
            payload = create_github_issue(
                preview,
                gh_binary=getattr(args, "gh_binary", "gh"),
            )
            _emit_ok(payload, as_json=as_json)
            return 0

        if as_json:
            _emit_ok(preview.to_dict(), as_json=True)
        else:
            print(f"repo: {preview.repo}")
            if preview.deployment_repo:
                print(f"deployment_repo: {preview.deployment_repo}")
            print(f"title: {preview.title}")
            if preview.labels:
                print(f"labels: {', '.join(preview.labels)}")
            print(f"body_sha256: {preview.body_sha256}")
            print("--- issue body ---")
            print(preview.body)
            print("--- end issue body ---")
        return 0
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)


def _cmd_deployment_up(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment up <name>` — idempotent current-state -> current-code.

    The post-merge runbook as one verb (standardize-deployment-authority
    req.deploy.one-verb): migrate -> catalog apply -> ingest+publish, with
    the resolved target echoed up front and per-step status. Profiles:
    --target dev resolves the worktree-isolated DSN; prod (default)
    resolves the declared deployment database. The runtime worker restart
    is reported as the explicit final step (it runs under the operator's
    supervision, not ours), never silently skipped.
    """
    _load_main_helpers()
    from okg.substrate.deployment_upgrade import (
        run_deployment_fleet_upgrade,
        run_deployment_upgrade,
    )

    try:
        if getattr(args, "all", False):
            payload = run_deployment_fleet_upgrade(
                _fleet_upgrade_options_from_args(args),
            )
        else:
            payload = run_deployment_upgrade(_upgrade_options_from_args(args))
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _cmd_deployment_prove_live(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    from okg.substrate.deployment_live_proof import (
        collect_live_deployment_proof,
        require_query_probe,
    )

    try:
        require_query_probe()
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    payload = collect_live_deployment_proof(
        deployment=str(args.deployment),
        profile=str(args.profile),
        operation_receipt=Path(args.operation_receipt),
        dsn=getattr(args, "dsn", None),
        max_receipt_age_seconds=float(args.max_receipt_age_seconds),
        max_observation_seconds=float(args.max_observation_seconds),
    )
    output = getattr(args, "output", None)
    if output:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, target)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") is True else 1


def _cmd_deployment_backup(args: argparse.Namespace, *, as_json: bool) -> int:
    if args.backup_subcmd == "restore":
        return _cmd_deployment_backup_restore(args, as_json=as_json)
    if args.backup_subcmd != "verify":
        raise SubstrateError(
            "unknown deployment backup command",
            data={"subcommand": args.backup_subcmd},
        )
    from okg.substrate.deployment_upgrade import (
        UpgradeOptions,
        run_deployment_backup_verify,
    )

    options = UpgradeOptions(
        deployment=args.deployment,
        profile=getattr(args, "profile", "prod"),
        dsn=getattr(args, "dsn", None),
        state_file=(
            Path(args.state_file).expanduser()
            if getattr(args, "state_file", None)
            else None
        ),
        backup_dir=(
            Path(args.backup_dir).expanduser()
            if getattr(args, "backup_dir", None)
            else None
        ),
        backup_keep=int(getattr(args, "backup_keep", 1)),
        resume=bool(getattr(args, "resume", True)),
        reset_state=bool(getattr(args, "reset_state", False)),
        readiness_profile="dogfood",
        independent_backup_verify=True,
    )
    payload = run_deployment_backup_verify(options)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _cmd_deployment_backup_restore(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """#3415: go back to the backup `okg up` took before an upgrade."""
    from okg.substrate.deployment_installed_restore import run_installed_restore

    try:
        payload = run_installed_restore(
            str(args.deployment),
            Path(args.archive).expanduser(),
            apply=bool(getattr(args, "apply", False)),
        )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _upgrade_options_from_args(args: argparse.Namespace):
    from okg.substrate.deployment_upgrade import UpgradeOptions

    if not getattr(args, "deployment", None):
        raise SubstrateError(
            "deployment is required unless --all is set",
            fix_hint="run `okg up <deployment>` or `okg up --all`",
        )
    # `--plan` is the opt-in plan-only preview: every mutate phase reports what
    # it WOULD do without applying it, and the run is reported as a plan (not a
    # partial). Otherwise the mutate phases default ON.
    plan_only = bool(getattr(args, "plan", False))
    apply_services = bool(getattr(args, "apply_services", True)) and not plan_only
    sync_code = bool(getattr(args, "sync_code", True)) and not plan_only
    sync_deps = bool(getattr(args, "sync_deps", True)) and not plan_only
    return UpgradeOptions(
        deployment=args.deployment,
        profile=getattr(args, "profile", "prod"),
        dsn=getattr(args, "dsn", None),
        include=tuple(getattr(args, "include", None) or ()),
        state_file=(
            Path(args.state_file).expanduser()
            if getattr(args, "state_file", None) else None
        ),
        backup_dir=(
            Path(args.backup_dir).expanduser()
            if getattr(args, "backup_dir", None) else None
        ),
        backup_keep=(
            1 if getattr(args, "backup_keep", None) is None
            else int(args.backup_keep)
        ),
        verified_backup_state=(
            Path(args.verified_backup_state).expanduser()
            if getattr(args, "verified_backup_state", None) else None
        ),
        verified_backup_source_revision=getattr(
            args, "verified_backup_source_revision", None
        ),
        precheckout_target_revision=getattr(args, "precheckout_target_revision", None),
        lifecycle_plan=(
            Path(args.lifecycle_plan).expanduser()
            if getattr(args, "lifecycle_plan", None) else None
        ),
        confirm_safety_plan=getattr(args, "confirm_safety_plan", None),
        readiness_profile=getattr(args, "readiness_profile", "general"),
        apply_services=apply_services,
        skip_backup=bool(getattr(args, "skip_backup", False)),
        skip_migrate=bool(getattr(args, "skip_migrate", False)),
        skip_destructive_migrations=bool(
            getattr(args, "skip_destructive_migrations", False),
        ),
        confirm_destructive_migrations=bool(
            getattr(args, "confirm_destructive_migrations", False),
        ),
        sync_code=sync_code,
        sync_deps=sync_deps,
        plan_only=plan_only,
        resume=bool(getattr(args, "resume", True)),
        reset_state=bool(getattr(args, "reset_state", False)),
        runtime_fence_owner=getattr(args, "runtime_fence_owner", None),
        stop_after=getattr(args, "stop_after", None),
        from_contract=(
            Path(args.from_contract).expanduser()
            if getattr(args, "from_contract", None) else None
        ),
        accept_unknown_compat=bool(getattr(args, "accept_unknown_compat", False)),
    )


def _fleet_upgrade_options_from_args(args: argparse.Namespace):
    from okg.substrate.deployment_default import FLEET_HINT, deployment_fleet
    from okg.substrate.deployment_upgrade import (
        FleetUpgradeOptions,
        load_fleet_dispositions,
    )

    if getattr(args, "deployment", None):
        raise SubstrateError(
            "deployment argument cannot be combined with --all",
            data={"deployment": args.deployment},
            fix_hint="run one of: `okg up <deployment>` or `okg up --all`",
        )
    if getattr(args, "dsn", None):
        raise SubstrateError(
            "--dsn cannot be used with okg up --all",
            fix_hint="run per-deployment upgrades when overriding a DSN",
        )
    if getattr(args, "state_file", None):
        raise SubstrateError(
            "--state-file cannot be used with okg up --all",
            fix_hint="use --state-dir for per-deployment fleet state files",
        )
    if getattr(args, "from_contract", None) or getattr(
        args, "accept_unknown_compat", False,
    ):
        raise SubstrateError(
            "--from-contract and --accept-unknown-compat cannot be used with "
            "okg up --all",
            fix_hint="upgrade each installed deployment with `okg up <deployment>`",
        )
    # The kept fleet is repository configuration, not a package constant: a
    # checkout that declares none refuses rather than sweep nothing or a
    # built-in list (#2823).
    fleet = deployment_fleet()
    if not fleet.kept:
        raise SubstrateError(
            "okg up --all has no kept fleet to upgrade",
            data={"config": fleet.source},
            fix_hint=(
                f"list the deployments under {FLEET_HINT} (key: kept), or "
                "upgrade one deployment with `okg up <deployment>`"
            ),
        )
    plan_only = bool(getattr(args, "plan", False))
    apply_services = bool(getattr(args, "apply_services", True)) and not plan_only
    sync_code = bool(getattr(args, "sync_code", True)) and not plan_only
    sync_deps = bool(getattr(args, "sync_deps", True)) and not plan_only
    return FleetUpgradeOptions(
        deployments=fleet.kept,
        profile=getattr(args, "profile", "prod"),
        include=tuple(getattr(args, "include", None) or ()),
        dispositions=(
            load_fleet_dispositions(getattr(args, "fleet_dispositions"))
            if getattr(args, "fleet_dispositions", None)
            else {}
        ),
        state_dir=(
            Path(args.state_dir).expanduser()
            if getattr(args, "state_dir", None) else None
        ),
        backup_dir=(
            Path(args.backup_dir).expanduser()
            if getattr(args, "backup_dir", None) else None
        ),
        backup_keep=(
            1 if getattr(args, "backup_keep", None) is None
            else int(args.backup_keep)
        ),
        readiness_profile=getattr(args, "readiness_profile", "general"),
        apply_services=apply_services,
        skip_backup=bool(getattr(args, "skip_backup", False)),
        skip_migrate=bool(getattr(args, "skip_migrate", False)),
        skip_destructive_migrations=bool(
            getattr(args, "skip_destructive_migrations", False),
        ),
        confirm_destructive_migrations=bool(
            getattr(args, "confirm_destructive_migrations", False),
        ),
        sync_code=sync_code,
        sync_deps=sync_deps,
        plan_only=plan_only,
        resume=bool(getattr(args, "resume", True)),
        reset_state=bool(getattr(args, "reset_state", False)),
        stop_after=getattr(args, "stop_after", None),
    )


def _cmd_deployment_release(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment release <name>` — inspect/enforce release skew."""
    from okg.substrate.deployment_release import build_release_report

    dsn = _resolve_deployment_dsn(args)
    report = build_release_report(
        deployment=args.deployment,
        dsn=dsn,
        process_role=getattr(args, "process_role", None) or "cli",
    )
    _emit_ok(report, as_json=as_json)
    return 0 if report.get("ok") else 1


def _cmd_deployment_isolated(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment isolated {list,gc}` — worktree-DB lifecycle."""
    from okg.substrate.deployment_isolation import gc_isolated, list_isolated
    from okg.substrate.pact.decisions import resolve_ledger_dsn

    # Same authority chain the ledger uses (incl. the worktree fallback
    # to the main checkout's local.env - finding #2's fix).
    resolution = resolve_ledger_dsn(explicit_dsn=getattr(args, "dsn", None))
    dsn = resolution.get("dsn")
    if not dsn:
        _emit_ok(
            {
                "ok": False,
                "reason": resolution.get("reason") or "DSN unresolved",
                "deployment": args.deployment,
            },
            as_json=as_json,
        )
        return 1
    if args.iso_action == "list":
        payload = list_isolated(args.deployment, dsn)
    else:
        payload = gc_isolated(args.deployment, dsn, yes=args.yes)
    payload["resolved_target"] = resolution.get("target")
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _cmd_deployment_nomos_rollout_audit(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    _load_main_helpers()
    try:
        payload = _nomos_rollout_inventory(
            repo_deployment_dir=Path(args.repo_deployment_dir),
            external_deployment_dir=_resolve_external_deployment_dir(
                args.deployment_dir,
            ),
            profile_dir=Path(args.profile_dir),
        )
        if getattr(args, "apply_baseline", None):
            payload["baseline_application"] = _apply_nomos_rollout_baseline(
                deployment=str(args.apply_baseline),
                dsn=getattr(args, "dsn", None),
                inventory=payload,
            )
            payload["ok"] = bool(
                payload.get("ok")
                and payload["baseline_application"].get("ok")
            )
        if getattr(args, "out", None):
            out = Path(args.out).expanduser()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(_json_safe(payload), indent=2, sort_keys=True),
                encoding="utf-8",
            )
            payload["out"] = str(out)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _cmd_deployment_nomos_source_lineage_rollout_audit(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    _load_main_helpers()
    try:
        payload = _nomos_source_lineage_rollout_inventory(
            repo_deployment_dir=Path(args.repo_deployment_dir),
            external_deployment_dir=_resolve_external_deployment_dir(
                args.deployment_dir,
            ),
            profile_dir=Path(args.profile_dir),
            runtime_report=bool(getattr(args, "runtime_report", False)),
            readiness_profile=getattr(args, "readiness_profile", None),
        )
        if getattr(args, "apply_baseline", None):
            payload["baseline_application"] = _apply_nomos_rollout_baseline(
                deployment=str(args.apply_baseline),
                dsn=getattr(args, "dsn", None),
                inventory=payload,
            )
            payload["ok"] = bool(
                payload.get("ok")
                and payload["baseline_application"].get("ok")
            )
        if getattr(args, "restamp_publisher_derived", None):
            payload["publisher_derived_restamp"] = (
                _restamp_nomos_source_lineage_publisher_derived(
                    deployment=str(args.restamp_publisher_derived),
                    dsn=getattr(args, "dsn", None),
                    inventory=payload,
                )
            )
            payload["ok"] = bool(
                payload.get("ok")
                and payload["publisher_derived_restamp"].get("ok")
            )
        if getattr(args, "repair_laundered_derived", None):
            payload["laundered_derived_repair"] = (
                _repair_nomos_laundered_derived_lineage(
                    deployment=str(args.repair_laundered_derived),
                    dsn=getattr(args, "dsn", None),
                    inventory=payload,
                    batch_size=int(
                        getattr(args, "restamp_batch_size", 5000) or 5000
                    ),
                    dry_run=bool(
                        getattr(args, "repair_laundered_derived_dry_run", False)
                    ),
                )
            )
            # `complete`, not `ok`: `ok` only says the repair ran. Exiting 0
            # while rows are still laundered would tell the operator the
            # recovery command succeeded while the doctor stays red (#1544).
            payload["ok"] = bool(
                payload.get("ok")
                and payload["laundered_derived_repair"].get("ok")
                and payload["laundered_derived_repair"].get("complete")
            )
        if getattr(args, "restamp_active_policy_fingerprints", None):
            payload["active_policy_fingerprint_restamp"] = (
                _restamp_nomos_source_lineage_active_policy_fingerprints(
                    deployment=str(args.restamp_active_policy_fingerprints),
                    dsn=getattr(args, "dsn", None),
                    inventory=payload,
                    batch_size=int(getattr(args, "restamp_batch_size", 5000) or 5000),
                )
            )
            payload["ok"] = bool(
                payload.get("ok")
                and payload["active_policy_fingerprint_restamp"].get("ok")
            )
        if getattr(args, "out", None):
            out = Path(args.out).expanduser()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(_json_safe(payload), indent=2, sort_keys=True),
                encoding="utf-8",
            )
            payload["out"] = str(out)
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


def _normalize_rollout_owner(owner: Any) -> dict[str, Any] | None:
    """Return a structured owner mapping or None when the owner is unusable.

    A structured owner must carry at minimum a non-empty ``name`` (or
    ``team``/``id``) and a ``contact`` so a deferred rollout is attributable.
    A bare string is NOT accepted as structured (it has no contact handle).
    """
    if not isinstance(owner, Mapping):
        return None
    name = ""
    for key in ("name", "team", "id"):
        value = owner.get(key)
        if isinstance(value, str) and value.strip():
            name = value.strip()
            break
    contact = owner.get("contact")
    contact = contact.strip() if isinstance(contact, str) and contact.strip() else ""
    if not name or not contact:
        return None
    return {"name": name, "contact": contact}


def _parse_rollout_expiry(value: Any) -> "datetime.date | None":
    """Parse an ISO ``YYYY-MM-DD`` (or full ISO datetime) expiry date."""
    import datetime

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            return datetime.date.fromisoformat(text[:10])
        except ValueError:
            return None
    return None


def _legacy_audit_deferred_rollout(
    rollout: Mapping[str, Any],
    *,
    deployment_name: str,
    deployment_dsn_key: str | None,
    enforce_dsn_keys: Mapping[str, str] | None = None,
    today: "datetime.date | None" = None,
) -> list[dict[str, Any]]:
    """Audit a ``nomos.rollout.status: deferred`` entry; return error findings.

    An empty list means the deferral is bounded and acceptable. Findings are
    returned (rather than raised) so the caller can re-flag the entry as failed.
    Bounds enforced:

    * a structured ``owner`` (name/team + contact) is required;
    * an ``expiry`` / ``review_by`` date is required and must not have passed
      (compared against ``today``, injected for testability);
    * ``status: deferred`` is forbidden when the deployment shares a
      database/DSN with an enforce-mode deployment.
    """
    import datetime

    if today is None:
        today = datetime.date.today()
    findings: list[dict[str, Any]] = []

    owner = _normalize_rollout_owner(rollout.get("owner"))
    if owner is None:
        findings.append({
            "code": "nomos.rollout.deferred_owner_missing",
            "reason_code": "nomos.rollout.deferred_owner_missing",
            "severity": "error",
            "message": (
                "deferred rollout must declare a structured owner "
                "(name/team and contact)"
            ),
            "path": "nomos.rollout.owner",
        })

    raw_expiry = (
        rollout.get("expiry")
        if rollout.get("expiry") is not None
        else rollout.get("review_by")
    )
    expiry = _parse_rollout_expiry(raw_expiry)
    if raw_expiry is None or expiry is None:
        findings.append({
            "code": "nomos.rollout.deferred_expiry_missing",
            "reason_code": "nomos.rollout.deferred_expiry_missing",
            "severity": "error",
            "message": (
                "deferred rollout must declare an expiry/review_by date "
                "(YYYY-MM-DD)"
            ),
            "path": "nomos.rollout.expiry",
        })
    elif expiry < today:
        findings.append({
            "code": "nomos.rollout.deferred_expired",
            "reason_code": "nomos.rollout.deferred_expired",
            "severity": "error",
            "message": (
                f"deferred rollout expired on {expiry.isoformat()} "
                f"(today {today.isoformat()})"
            ),
            "path": "nomos.rollout.expiry",
            "expiry": expiry.isoformat(),
            "today": today.isoformat(),
        })

    if deployment_dsn_key and enforce_dsn_keys:
        shared = sorted(
            other
            for other, key in enforce_dsn_keys.items()
            if key == deployment_dsn_key and other != deployment_name
        )
        if shared:
            findings.append({
                "code": "nomos.rollout.deferred_shared_enforce_dsn",
                "reason_code": "nomos.rollout.deferred_shared_enforce_dsn",
                "severity": "error",
                "message": (
                    "deferred rollout is forbidden while sharing a "
                    "database/DSN with an enforce-mode deployment"
                ),
                "path": "postgres.dsn",
                "shared_with": shared,
                "dsn_key": deployment_dsn_key,
            })
    return findings


def _legacy_rollout_deployment_dsn_key(
    manifest: Mapping[str, Any],
) -> str | None:
    """Return a normalized database/DSN identity for shared-DB comparison.

    Prefers the explicit ``postgres.database`` (after env interpolation of the
    DSN), falling back to the raw DSN string with env-var placeholders left as
    written so two manifests that share ``${OKG_DSN}`` + the same database
    compare equal without resolving secrets.
    """
    postgres = manifest.get("postgres")
    if not isinstance(postgres, Mapping):
        return None
    database = postgres.get("database")
    raw_dsn = postgres.get("dsn")
    parts: list[str] = []
    if isinstance(raw_dsn, str) and raw_dsn.strip():
        parts.append(raw_dsn.strip())
    if isinstance(database, str) and database.strip():
        parts.append(f"db={database.strip()}")
    if not parts:
        return None
    return "|".join(parts)


def _legacy_enforce_mode_dsn_keys(
    *deployment_roots: Path,
) -> dict[str, str]:
    """Map enforce-mode deployment name -> DSN key across the given roots."""
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )

    keys: dict[str, str] = {}
    for root in deployment_roots:
        if root is None or not root.exists():
            continue
        for manifest_path in sorted(root.glob("*/deployment.yaml")):
            try:
                manifest = _ready_load_yaml_mapping(manifest_path)
            except Exception:  # noqa: BLE001 - best-effort inventory.
                continue
            if manifest_nomos_enforcement_mode(manifest) != "enforce":
                continue
            dsn_key = _legacy_rollout_deployment_dsn_key(manifest)
            if dsn_key is None:
                continue
            name = str(manifest.get("name") or manifest_path.parent.name)
            keys[name] = dsn_key
    return keys


# The reusable policy authority lives below the CLI layer. Keep these historical
# CLI names as aliases so operator commands and imports exercise the exact same
# implementation as source admission.
from okg.substrate.nomos import rollout as _rollout_policy  # noqa: E402

def audit_deferred_rollout(
    rollout: Mapping[str, Any],
    *,
    deployment_name: str,
    deployment_dsn_key: str | None,
    enforce_dsn_keys: Mapping[str, str] | None = None,
    today: "datetime.date | None" = None,
) -> list[dict[str, Any]]:
    return _rollout_policy.audit_deferred_rollout(
        rollout,
        deployment_name=deployment_name,
        deployment_dsn_key=deployment_dsn_key,
        enforce_dsn_keys=enforce_dsn_keys,
        today=today,
    )


def _rollout_deployment_dsn_key(manifest: Mapping[str, Any]) -> str | None:
    return _rollout_policy.deployment_dsn_key(manifest)


def _enforce_mode_dsn_keys(*deployment_roots: Path) -> dict[str, str]:
    return _rollout_policy.enforce_mode_dsn_keys(*deployment_roots)


def _apply_nomos_rollout_baseline(
    *,
    deployment: str,
    dsn: str | None,
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    match = next(
        (
            entry for entry in inventory.get("entries", [])
            if entry.get("name") == deployment
            and entry.get("kind") == "deployment"
        ),
        None,
    )
    if match is None:
        raise SubstrateError(
            f"deployment {deployment!r} not found in Nomos rollout inventory",
            fix_hint="check --deployment-dir or the deployment name",
        )
    manifest_path = Path(str(match["path"]))
    if not dsn:
        try:
            dsn = _resolve_deployment_dsn_from_dir(
                argparse.Namespace(dsn=None),
                manifest_path.parent,
            )
        except SubstrateError:
            dsn = _resolve_deployment_dsn(argparse.Namespace(
                deployment=deployment,
                dsn=None,
            ))
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos import seed_deployment_baseline_nomos_state
    from okg.substrate.nomos.baseline import deployment_baseline_spec

    baseline_spec = deployment_baseline_spec(
        deployment=deployment,
        deployment_dir=manifest_path.parent,
    )
    if not baseline_spec.get("required"):
        raise SubstrateError(
            f"deployment {deployment!r} does not require a Nomos baseline"
        )
    with db_connect(dsn) as conn:
        payload = seed_deployment_baseline_nomos_state(
            conn,
            deployment_name=deployment,
            manifest=baseline_spec["manifest"],
            source_names=baseline_spec["source_names"],
            source_policies=baseline_spec["source_policies"],
            runtime_source_names=baseline_spec["runtime_source_names"],
            policy_graph=baseline_spec["policy_graph"],
        )
        conn.commit()
    return payload


def _restamp_nomos_source_lineage_publisher_derived(
    *,
    deployment: str,
    dsn: str | None,
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    match = next(
        (
            entry for entry in inventory.get("entries", [])
            if entry.get("name") == deployment
            and entry.get("kind") == "deployment"
        ),
        None,
    )
    if match is None:
        raise SubstrateError(
            f"deployment {deployment!r} not found in Nomos rollout inventory",
            fix_hint="check --deployment-dir or the deployment name",
        )
    manifest_path = Path(str(match["path"]))
    if not dsn:
        try:
            dsn = _resolve_deployment_dsn_from_dir(
                argparse.Namespace(dsn=None),
                manifest_path.parent,
            )
        except SubstrateError:
            dsn = _resolve_deployment_dsn(argparse.Namespace(
                deployment=deployment,
                dsn=None,
            ))
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos.source_lineage_repair import (
        restamp_publisher_derived_lineage,
    )

    with db_connect(dsn) as conn:
        payload = restamp_publisher_derived_lineage(conn)
        conn.commit()
    payload["deployment_name"] = deployment
    payload["publish_command"] = (
        f"{okg_prefix()} run --once --deployment {shlex.quote(deployment)} "
        "--inline --apply --json"
    )
    return payload


def _repair_nomos_laundered_derived_lineage(
    *,
    deployment: str,
    dsn: str | None,
    inventory: Mapping[str, Any],
    batch_size: int = 5000,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the #1544 laundered-derived-lineage repair on one deployment."""
    dsn = _nomos_rollout_entry_dsn(
        deployment=deployment,
        dsn=dsn,
        inventory=inventory,
    )
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos.source_lineage_repair import (
        repair_laundered_derived_lineage,
    )

    with db_connect(dsn) as conn:
        payload = repair_laundered_derived_lineage(
            conn,
            deployment_name=deployment,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    payload["publish_command"] = (
        f"{okg_prefix()} run --once --deployment {shlex.quote(deployment)} "
        "--inline --apply --json"
    )
    return payload


def _nomos_rollout_entry_dsn(
    *,
    deployment: str,
    dsn: str | None,
    inventory: Mapping[str, Any],
) -> str:
    """Resolve one rollout-inventory entry's DSN, or refuse by name."""
    match = next(
        (
            entry for entry in inventory.get("entries", [])
            if entry.get("name") == deployment
            and entry.get("kind") == "deployment"
        ),
        None,
    )
    if match is None:
        raise SubstrateError(
            f"deployment {deployment!r} not found in Nomos rollout inventory",
            fix_hint="check --deployment-dir or the deployment name",
        )
    if dsn:
        return dsn
    manifest_path = Path(str(match["path"]))
    try:
        return _resolve_deployment_dsn_from_dir(
            argparse.Namespace(dsn=None),
            manifest_path.parent,
        )
    except SubstrateError:
        return _resolve_deployment_dsn(argparse.Namespace(
            deployment=deployment,
            dsn=None,
        ))


def _restamp_nomos_source_lineage_active_policy_fingerprints(
    *,
    deployment: str,
    dsn: str | None,
    inventory: Mapping[str, Any],
    batch_size: int = 5000,
) -> dict[str, Any]:
    match = next(
        (
            entry for entry in inventory.get("entries", [])
            if entry.get("name") == deployment
            and entry.get("kind") == "deployment"
        ),
        None,
    )
    if match is None:
        raise SubstrateError(
            f"deployment {deployment!r} not found in Nomos rollout inventory",
            fix_hint="check --deployment-dir or the deployment name",
        )
    manifest_path = Path(str(match["path"]))
    if not dsn:
        try:
            dsn = _resolve_deployment_dsn_from_dir(
                argparse.Namespace(dsn=None),
                manifest_path.parent,
            )
        except SubstrateError:
            dsn = _resolve_deployment_dsn(argparse.Namespace(
                deployment=deployment,
                dsn=None,
            ))
    from okg.substrate.db.connection import connect as db_connect
    from okg.substrate.nomos.source_lineage_repair import (
        restamp_active_policy_lineage_fingerprints,
    )

    with db_connect(dsn) as conn:
        payload = restamp_active_policy_lineage_fingerprints(
            conn,
            deployment_name=deployment,
            batch_size=batch_size,
        )
        conn.commit()
    payload["publish_command"] = (
        f"{okg_prefix()} run --once --deployment {shlex.quote(deployment)} "
        "--inline --apply --json"
    )
    return payload


def _cmd_deployment_read_status(args: argparse.Namespace, *, as_json: bool) -> int:
    """Read the existing lifecycle journal through the shared typed projection."""

    from okg.substrate.cli.deployment_read_surfaces import (
        deployment_read_agent_payload,
        deployment_read_human_text,
    )

    state_file = Path(args.state_file) if args.state_file else (
        Path(".okg") / "up" / f"{args.deployment}-{args.profile}.json"
    )
    try:
        if as_json:
            _emit_ok(deployment_read_agent_payload(state_file), as_json=True)
        else:
            print(deployment_read_human_text(state_file))
    except (OSError, ValueError) as exc:
        return _emit_error(SubstrateError(str(exc)), as_json=as_json)
    return 0


def _cmd_deployment_conformance_schema(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Expose only the pure wire schema; lifecycle execution is not in scope."""

    from okg.substrate.contracts.deployment_product import conformance_schema

    _emit_ok(
        conformance_schema(
            args.kind, protocol_version=getattr(args, "protocol_version", "v1")
        ),
        as_json=as_json,
    )
    return 0


def _load_trusted_external_review(document: bytes) -> Any:
    """Load an out-of-band acknowledgement record as inert data.

    A reviewer publishes the acknowledgement somewhere the validator cannot
    reach, so the operator hands the file over instead.  It is read by the
    request and receipt rules and by nothing else: duplicate keys refused,
    unknown fields refused by the model, every failure a stable code.  No
    code runs, no database is opened and no network call is made.
    """

    from pydantic import ValidationError

    from okg.substrate.contracts.deployment_product import (
        ConformanceContractError,
        ConformanceExternalEvidenceOrigin,
    )

    kind = "trusted_external_review"

    def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        loaded: dict[str, Any] = {}
        for key, value in pairs:
            if key in loaded:
                raise ConformanceContractError(
                    f"conformance_{kind}_duplicate_key",
                    "$",
                    observed="duplicate-key",
                )
            loaded[key] = value
        return loaded

    try:
        raw = json.loads(document, object_pairs_hook=_object)
    except ConformanceContractError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise ConformanceContractError(
            f"conformance_{kind}_invalid_json",
            "$",
            observed=type(exc).__name__,
        ) from exc
    if not isinstance(raw, Mapping):
        raise ConformanceContractError(
            f"conformance_{kind}_invalid",
            "$",
            expected="object",
            observed=type(raw).__name__,
        )
    try:
        return ConformanceExternalEvidenceOrigin.model_validate(raw)
    except ValidationError as exc:
        first = exc.errors()[0]
        path = ".".join(str(item) for item in first.get("loc", ())) or "$"
        raise ConformanceContractError(
            f"conformance_{kind}_invalid",
            path,
            observed=first.get("type", "validation-error"),
        ) from exc


def _acknowledgement_bindings(origin: Any, review: Any) -> list[dict[str, Any]]:
    """Report the acknowledgement this exchange bound, and where it disagrees.

    Every value is copied from the receipt's own acknowledgement record; the
    disagreement list carries field names read off the model, never the
    reviewer's values.  A receipt naming no acknowledgement reports none.

    Only a ``current`` acknowledgement is weighed against the review, so only
    then can a field disagree.  Against a ``historical`` one the review is
    inert and the list stays empty: reporting differences the validator never
    consulted would read as a complaint about a run that passed.
    """

    if origin is None:
        return []
    differing: list[str] = []
    if origin.acknowledgement_status == "current":
        differing = [
            name
            for name in type(origin).model_fields
            if getattr(origin, name) != getattr(review, name)
        ]
    return [
        {
            "consumer_id": origin.consumer_id,
            "source_revision": origin.source_revision,
            "acknowledged_schema_digest": origin.acknowledged_schema_digest,
            "acknowledgement_status": origin.acknowledgement_status,
            "trusted_review_fields_differing": differing,
        }
    ]


def _cmd_deployment_conformance_validate(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Validate only inert wire documents; never execute consumer code."""

    from okg.substrate.contracts.deployment_product import (
        CONFORMANCE_PROTOCOL,
        CONFORMANCE_SCHEMA_CATALOG,
        CONFORMANCE_VALIDATION_RESULT_SCHEMA,
        CONFORMANCE_VALIDATION_RESULT_SCHEMA_V2,
        conformance_document_protocol_version,
        ConformanceContractError,
        conformance_receipt_digest,
        conformance_request_digest,
        conformance_schema_catalog,
        load_conformance_receipt,
        load_conformance_request,
        validate_conformance_exchange,
    )
    from okg.substrate.distributions.conformance_diagnostics import (
        ConformanceProfileError,
        check_conformance_diagnostic_profiles,
    )

    scope = "exchange" if args.receipt else "request"
    result: dict[str, Any] = {
        "schema_version": CONFORMANCE_VALIDATION_RESULT_SCHEMA,
        "protocol": CONFORMANCE_PROTOCOL,
        "scope": scope,
        "schema_catalog_version": CONFORMANCE_SCHEMA_CATALOG,
        "schema_catalog_digest": conformance_schema_catalog()["schema_digest"],
        "request_digest": None,
        "receipt_digest": None,
        "receipt_verdict": None,
        "valid": False,
        "diagnostic": None,
    }
    # The acknowledgement report is part of the trusted-review mode, so a run
    # without the flag keeps exactly the output contract it had before the
    # flag existed -- same keys, same bytes, same exit code.
    review_path = getattr(args, "trusted_external_review", None)
    if review_path is not None:
        result["acknowledgement_bindings"] = []

    def read_document(value: str, *, input_name: str) -> bytes:
        path = Path(value)
        descriptor: int | None = None
        try:
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_BINARY", 0)
                | getattr(os, "O_NONBLOCK", 0),
            )
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise OSError("conformance input is not a regular file")
            if metadata.st_size > _CONFORMANCE_MAX_DOCUMENT_BYTES:
                raise SubstrateError(
                    "external-distribution conformance input is too large",
                    data={
                        "exception_type": "InputTooLarge",
                        "input": input_name,
                        "max_bytes": _CONFORMANCE_MAX_DOCUMENT_BYTES,
                    },
                )
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                document = stream.read(_CONFORMANCE_MAX_DOCUMENT_BYTES + 1)
        except OSError as exc:
            raise SubstrateError(
                "could not read external-distribution conformance input",
                data={
                    "exception_type": type(exc).__name__,
                    "input": input_name,
                },
            ) from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(document) > _CONFORMANCE_MAX_DOCUMENT_BYTES:
            raise SubstrateError(
                "external-distribution conformance input is too large",
                data={
                    "exception_type": "InputTooLarge",
                    "input": input_name,
                    "max_bytes": _CONFORMANCE_MAX_DOCUMENT_BYTES,
                },
            )
        return document

    if review_path is not None and not args.receipt:
        _emit_error(
            SubstrateError(
                "a trusted external review applies only to a complete exchange",
                data={
                    "exception_type": "ReceiptRequired",
                    "input": "trusted_external_review",
                },
            ),
            as_json=as_json,
        )
        return 2

    try:
        request_bytes = read_document(args.request, input_name="request")
        receipt_bytes = (
            read_document(args.receipt, input_name="receipt")
            if args.receipt
            else None
        )
        review_bytes = (
            read_document(review_path, input_name="trusted_external_review")
            if review_path is not None
            else None
        )
    except SubstrateError as exc:
        _emit_error(exc, as_json=as_json)
        return 2

    try:
        protocol_version = conformance_document_protocol_version(request_bytes)
        catalog = conformance_schema_catalog(protocol_version=protocol_version)
        result.update(
            schema_version=(
                CONFORMANCE_VALIDATION_RESULT_SCHEMA
                if protocol_version == "v1"
                else CONFORMANCE_VALIDATION_RESULT_SCHEMA_V2
            ),
            protocol=catalog["protocol"],
            schema_catalog_version=catalog["schema_version"],
            schema_catalog_digest=catalog["schema_digest"],
        )
        review = (
            _load_trusted_external_review(review_bytes)
            if review_bytes is not None
            else None
        )
        request = load_conformance_request(request_bytes)
        result["request_digest"] = conformance_request_digest(request)
        if receipt_bytes is not None:
            receipt = load_conformance_receipt(receipt_bytes)
            result["receipt_digest"] = conformance_receipt_digest(receipt)
            result["receipt_verdict"] = receipt.verdict
            if review is not None:
                # Reported before the exchange rules run, so a refusal still
                # says which acknowledgement was weighed against the review.
                result["acknowledgement_bindings"] = _acknowledgement_bindings(
                    receipt.claim.external_evidence_origin, review
                )
            validate_conformance_exchange(
                request, receipt, trusted_external_review=review
            )
            # The settled credential and environment profiles (issue 1794)
            # sit above the wire contract; a receipt that breaks one is
            # reported as not valid with the same diagnostic shape.
            check_conformance_diagnostic_profiles(request, receipt)
    except (ConformanceContractError, ConformanceProfileError) as exc:
        result["diagnostic"] = exc.to_dict()
        _emit_ok(result, as_json=as_json)
        return 1

    result["valid"] = True
    _emit_ok(result, as_json=as_json)
    return 0


_INSTANCE_CONFIG_MAX_BYTES = 256 * 1024


def _cmd_deployment_install_plan(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Plan an install; write only plan.json and lock.json, run nothing.

    A thin wrapper: every refusal, digest and file write belongs to
    `okg.substrate.distributions.install_plan`, so the CLI and any
    programmatic caller refuse identically.
    """

    from okg.substrate.distributions.install_plan import (
        InstallPlanError,
        write_install_plan,
    )

    path = Path(args.instance)
    try:
        if not path.is_file():
            raise OSError("instance config is not a regular file")
        if path.stat().st_size > _INSTANCE_CONFIG_MAX_BYTES:
            raise OSError("instance config is too large")
        instance = fastyaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return _emit_error(
            SubstrateError(
                "could not read the instance config",
                data={"instance": str(path), "exception_type": type(exc).__name__},
                fix_hint="pass a readable JSON or YAML instance config",
            ),
            as_json=as_json,
        )
    if instance is None:
        instance = {}
    if not isinstance(instance, Mapping):
        return _emit_error(
            SubstrateError(
                "the instance config must be a mapping",
                data={"instance": str(path)},
                fix_hint="write the config as a mapping of name, modules and secrets",
            ),
            as_json=as_json,
        )

    try:
        result = write_install_plan(
            args.package,
            args.bundle,
            instance,
            args.output,
            replace_lock=bool(args.replace_lock),
        )
    except InstallPlanError as exc:
        return _emit_error(exc, as_json=as_json)
    _emit_ok(result, as_json=as_json)
    return 0


def _read_install_documents(directory: str) -> tuple[dict, dict]:
    """Read one install directory's `plan.json` and `lock.json`."""
    from okg.substrate.distributions.install_plan import LOCK_FILENAME, PLAN_FILENAME

    root = Path(directory)
    return (
        json.loads((root / PLAN_FILENAME).read_text(encoding="utf-8")),
        json.loads((root / LOCK_FILENAME).read_text(encoding="utf-8")),
    )


def _cmd_deployment_install_receipt(
    args: argparse.Namespace,
    *,
    as_json: bool,
) -> int:
    """Write one upgrade, rollback or drift receipt; change nothing else.

    A thin wrapper: every refusal, digest and file write belongs to
    `okg.substrate.distributions.lifecycle_receipts`, so the CLI and any
    programmatic caller refuse identically.
    """

    from okg.substrate.distributions.install_plan import InstallPlanError
    from okg.substrate.distributions.lifecycle_receipts import (
        LifecycleReceiptError,
        ReleaseSide,
        resolve_drift_receipt,
        resolve_rollback_receipt,
        resolve_upgrade_receipt,
        write_lifecycle_receipt,
    )
    from okg.substrate.catalog.module_provenance import UpgradeModuleProviderDropped

    # Drift compares two locks and needs no release ids; the other two
    # kinds record a revert between named releases, and a receipt naming
    # an empty release would prove nothing the #1185 evidence can bind.
    missing = [
        name
        for name, value in (
            ("--baseline-release", args.baseline_release),
            ("--candidate-release", args.candidate_release),
            ("--baseline-package", args.baseline_package),
        )
        if not value
        and (args.kind != "drift")
        and (name != "--baseline-package" or args.kind == "rollback")
    ]
    if missing:
        return _emit_error(
            SubstrateError(
                f"a {args.kind} receipt needs {', '.join(missing)}",
                data={"kind": args.kind, "missing": missing},
                fix_hint="pass the sealed-release ids the receipt must record",
            ),
            as_json=as_json,
        )
    try:
        baseline_plan, baseline_lock = _read_install_documents(args.baseline)
        candidate_plan, candidate_lock = _read_install_documents(args.candidate)
    except (OSError, ValueError) as exc:
        return _emit_error(
            SubstrateError(
                "could not read an install directory's plan and lock",
                data={"exception_type": type(exc).__name__},
                fix_hint="point --baseline and --candidate at install-plan outputs",
            ),
            as_json=as_json,
        )

    baseline = ReleaseSide(
        baseline_plan, baseline_lock, str(args.baseline_release or ""),
    )
    candidate = ReleaseSide(
        candidate_plan, candidate_lock, str(args.candidate_release or ""),
    )
    try:
        if args.kind == "upgrade":
            receipt = resolve_upgrade_receipt(
                baseline, candidate, okg_version=args.okg_version,
            )
        elif args.kind == "rollback":
            receipt = resolve_rollback_receipt(
                baseline, candidate, baseline_package_path=args.baseline_package,
            )
        else:
            receipt = resolve_drift_receipt(baseline_lock, candidate_lock)
        result = write_lifecycle_receipt(receipt, args.output, args.kind)
    except (
        LifecycleReceiptError,
        InstallPlanError,
        UpgradeModuleProviderDropped,
    ) as exc:
        return _emit_error(exc, as_json=as_json)
    _emit_ok(result, as_json=as_json)
    return 0


def _cmd_deployment(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment ...` — deployment authoring workflows."""
    _load_main_helpers()

    try:
        if args.subcmd == "new":
            return _cmd_deployment_new(args, as_json=as_json)
        if args.subcmd in ("add-source", "source-types"):
            from okg.substrate.cli import source_add as _source_add

            handler = (
                _source_add.cmd_add_source
                if args.subcmd == "add-source"
                else _source_add.cmd_source_types
            )
            return handler(args, as_json=as_json)
        if args.subcmd == "remove-source":
            from okg.substrate.cli import source_remove as _source_remove

            return _source_remove.cmd_remove_source(args, as_json=as_json)
        if args.subcmd == "show":
            return _cmd_deployment_show(args, as_json=as_json)
        if args.subcmd == "read-status":
            return _cmd_deployment_read_status(args, as_json=as_json)
        if args.subcmd == "install-plan":
            return _cmd_deployment_install_plan(args, as_json=as_json)
        if args.subcmd == "install-receipt":
            return _cmd_deployment_install_receipt(args, as_json=as_json)
        if args.subcmd == "conformance":
            if args.conformance_subcmd == "schema":
                return _cmd_deployment_conformance_schema(args, as_json=as_json)
            if args.conformance_subcmd == "validate":
                return _cmd_deployment_conformance_validate(args, as_json=as_json)
        if args.subcmd == "env":
            return _cmd_deployment_env(args, as_json=as_json)
        if args.subcmd == "ready":
            return _cmd_deployment_ready(args, as_json=as_json)
        if args.subcmd == "prove-live":
            return _cmd_deployment_prove_live(args, as_json=as_json)
        if args.subcmd == "plan":
            return _cmd_deployment_plan(args, as_json=as_json)
        if args.subcmd == "lint":
            return _cmd_deployment_lint(args, as_json=as_json)
        if args.subcmd == "compat":
            return _cmd_deployment_compat(args, as_json=as_json)
        if args.subcmd == "profile":
            return _cmd_deployment_profile(args, as_json=as_json)
        if args.subcmd == "list":
            return _cmd_deployment_list(args, as_json=as_json)
        if args.subcmd == "fork-worktree":
            return _cmd_deployment_fork_worktree(args, as_json=as_json)
        if args.subcmd == "gc-forks":
            return _cmd_deployment_gc_forks(args, as_json=as_json)
        if args.subcmd == "gc-scaffolds":
            return _cmd_deployment_gc_scaffolds(args, as_json=as_json)
        if args.subcmd == "isolated":
            return _cmd_deployment_isolated(args, as_json=as_json)
        if args.subcmd == "nomos-rollout-audit":
            return _cmd_deployment_nomos_rollout_audit(args, as_json=as_json)
        if args.subcmd == "nomos-source-lineage-rollout-audit":
            return _cmd_deployment_nomos_source_lineage_rollout_audit(
                args,
                as_json=as_json,
            )
        if args.subcmd == "up":
            return _cmd_deployment_up(args, as_json=as_json)
        if args.subcmd == "apply":
            return _cmd_deployment_apply(args, as_json=as_json)
        if args.subcmd == "backup":
            return _cmd_deployment_backup(args, as_json=as_json)
        if args.subcmd == "release":
            return _cmd_deployment_release(args, as_json=as_json)
        if args.subcmd == "bug-report":
            return _cmd_deployment_bug_report(args, as_json=as_json)
        if args.subcmd == "bootstrap":
            from okg.substrate.deployment_bootstrap import (
                apply_staged,
                create_plan,
                draft,
                inspect_source,
                status,
                verify,
            )

            if args.bootstrap_subcmd == "plan":
                payload = create_plan(
                    deployment=Path(args.deployment),
                    brief_file=Path(args.brief),
                    sources=list(args.source or []),
                    name=getattr(args, "name", None),
                    template_base=args.template_base,
                    dsn_env=args.dsn_env,
                    apply=args.apply,
                )
            elif args.bootstrap_subcmd == "inspect-source":
                payload = inspect_source(
                    deployment=Path(args.deployment),
                    source_id=args.source_id,
                    apply=args.apply,
                    max_records=args.max_records,
                )
            elif args.bootstrap_subcmd == "draft":
                payload = draft(
                    deployment=Path(args.deployment),
                    apply=args.apply,
                )
            elif args.bootstrap_subcmd == "verify":
                payload = verify(
                    deployment=Path(args.deployment),
                    dsn=args.dsn,
                    apply=args.apply,
                    structural_only=args.structural_only,
                )
            elif args.bootstrap_subcmd == "apply":
                payload = apply_staged(
                    deployment=Path(args.deployment),
                    apply=args.apply,
                    dsn=getattr(args, "dsn", None),
                )
            elif args.bootstrap_subcmd == "status":
                payload = status(deployment=Path(args.deployment))
            else:
                return _emit_error(
                    SubstrateError(
                        f"unknown subcommand: okg deployment bootstrap "
                        f"{args.bootstrap_subcmd!r}",
                    ),
                    as_json=as_json,
                )
        else:
            return _emit_error(
                SubstrateError(
                    f"unknown subcommand: okg deployment {args.subcmd!r}",
                ),
                as_json=as_json,
            )
    except SubstrateError as e:
        return _emit_error(e, as_json=as_json)

    from okg.substrate.cli._output import payload_verdict_exit

    payload, code = payload_verdict_exit(payload, source="deployment_bootstrap")
    _emit_ok(payload, as_json=as_json)
    return code


def _cmd_deployment_list(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment list` — host-wide OKG deployment inventory.

    Enumerates every deployment on the host (manifest-discovered or
    MCP-registered) with its DB/port/role, MCP registration state (scope +
    enabled), reachability, and live node_count. Disambiguates which MCP
    server points at which graph.
    """
    from okg.substrate.cli.deployment_inventory import collect_deployment_inventory

    payload = collect_deployment_inventory(
        probe=not getattr(args, "no_probe", False)
    )
    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_deployment_apply(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """Plan or explicitly execute digest-bound deployment reconciliation."""
    import psycopg

    _load_main_helpers()
    from okg.substrate.deployment_apply import (
        apply_deployment_plan,
        plan_deployment_apply,
    )
    from okg.substrate.deployments import resolve_deployment

    try:
        from okg.substrate.deployments.admission import inactive_plan, require_active_deployment

        archived = inactive_plan(str(args.deployment))
        if archived is not None:
            if bool(getattr(args, "apply", False)):
                require_active_deployment(str(args.deployment))
            _emit_ok(archived, as_json=as_json)
            return 1
        manifest = resolve_deployment(str(args.deployment))
        if bool(getattr(args, "apply", False)) and getattr(args, "dsn", None):
            raise SubstrateError(
                "deployment apply refuses --dsn during worker restart",
                data={"dsn_override": redact_dsn(str(args.dsn))},
                fix_hint=(
                    "bind the target DSN through the deployment manifest/local "
                    "environment so the supervisor and apply service share one target"
                ),
            )
        dsn = _resolve_deployment_dsn_from_dir(args, manifest.deployment_dir)
        if bool(getattr(args, "apply", False)):
            confirm = str(getattr(args, "confirm", "") or "")
            if not confirm:
                raise SubstrateError(
                    "deployment apply requires --confirm <plan-id> with --apply",
                    fix_hint=(
                        f"run `okg deployment apply {args.deployment} --json` "
                        "and confirm the returned plan id"
                    ),
                )
            payload = apply_deployment_plan(
                deployment=manifest.name,
                deployment_dir=manifest.deployment_dir,
                dsn=dsn,
                confirm=confirm,
                worker_ack_timeout_seconds=float(
                    getattr(args, "worker_ack_timeout_seconds", 30.0)
                ),
            )
        else:
            if getattr(args, "confirm", None):
                raise SubstrateError(
                    "--confirm is valid only with --apply",
                    fix_hint="omit --confirm for dry-run or add --apply",
                )
            payload = plan_deployment_apply(
                deployment=manifest.name,
                deployment_dir=manifest.deployment_dir,
                dsn=dsn,
            )
            if getattr(args, "dsn", None) and payload.get("next_command"):
                # `--apply` refuses --dsn and plans against the manifest's
                # database, so a pasted command would confirm against a plan
                # this dry run never read. Withhold it and say why.
                payload["next_command"] = None
                payload["summary"] = (
                    f"dry run against --dsn: nothing was applied; "
                    f"{len(payload['pending_stages'])} stage(s) pending "
                    f"({', '.join(payload['pending_stages'])}). `--apply` uses "
                    "the database the deployment manifest or local environment "
                    "names, not --dsn: bind it there, then dry-run again "
                    "without --dsn for the apply command"
                )
    except (SubstrateError, OSError, ValueError, psycopg.Error) as exc:
        error = exc if isinstance(exc, SubstrateError) else SubstrateError(str(exc))
        return _emit_error(error, as_json=as_json)
    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_deployment_fork_worktree(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment fork-worktree` — fork the OKG for a worktree + wire its
    branch-pinned MCP (idempotent). Closes the per-worktree approval gap."""
    import os as _os
    from pathlib import Path as _Path

    from okg.substrate.cli.deployment_worktree import fork_worktree
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    from okg.substrate.deployment_default import resolve_command_deployment

    try:
        deployment = resolve_command_deployment(
            getattr(args, "deployment", None),
            command="okg deployment fork-worktree",
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    worktree_path = _Path(getattr(args, "worktree_path", None) or _os.getcwd())
    res = resolve_deployment_dsn_authority(
        deployment,
        local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
    )
    dsn = res.get("dsn")
    if not dsn:
        return _emit_error(
            SubstrateError(
                f"could not resolve a write DSN for deployment {deployment!r}",
                fix_hint="okg deployment env doctor "
                f"{deployment} --json to inspect DSN authority",
            ),
            as_json=as_json,
        )
    payload = fork_worktree(
        worktree_path=worktree_path,
        dsn=dsn,
        deployment=deployment,
        base_branch=getattr(args, "base_branch", None) or "default",
    )
    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_deployment_gc_forks(args: argparse.Namespace, *, as_json: bool) -> int:
    """`okg deployment gc-forks` — reap experiment forks whose worktree is gone
    (dry-run unless --apply)."""
    import os as _os
    from pathlib import Path as _Path

    from okg.substrate.cli.deployment_worktree import gc_forks
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    from okg.substrate.deployment_default import resolve_command_deployment

    try:
        deployment = resolve_command_deployment(
            getattr(args, "deployment", None),
            command="okg deployment gc-forks",
        )
    except SubstrateError as exc:
        return _emit_error(exc, as_json=as_json)
    repo_path = _Path(getattr(args, "repo_path", None) or _os.getcwd())
    res = resolve_deployment_dsn_authority(
        deployment,
        local_profile_authoritative=deployment_self_hosted_authoritative(deployment),
    )
    dsn = res.get("dsn")
    if not dsn:
        return _emit_error(
            SubstrateError(
                f"could not resolve a write DSN for deployment {deployment!r}",
            ),
            as_json=as_json,
        )
    payload = gc_forks(
        dsn=dsn, deployment=deployment, repo_path=repo_path,
        apply=getattr(args, "apply", False),
    )
    _emit_ok(payload, as_json=as_json)
    return 0


def _cmd_deployment_gc_scaffolds(
    args: argparse.Namespace, *, as_json: bool,
) -> int:
    """`okg deployment gc-scaffolds` — list and (with --yes) remove stale
    operator-state deployment scaffolds under the user-state root."""
    from okg.substrate.deployment_scaffold_gc import gc_scaffolds

    root = getattr(args, "root", None)
    payload = gc_scaffolds(
        yes=bool(getattr(args, "yes", False)),
        root=Path(root) if root else None,
    )
    _emit_ok(payload, as_json=as_json)
    return 0 if payload.get("ok") else 1


# `_cmd_catalog_reconcile` moved to `cli/catalog.py` by `extract-cli-catalog`.

# `_cmd_catalog_load` moved to `cli/catalog.py` by `extract-cli-catalog`.


# ---------------------------------------------------------------------------
# Re-export shims: the `_ready_*` and `_nomos*` helpers were extracted into
# sibling modules (`deployment_readiness`, `nomos_rollout_audit`) by
# `decompose-deployment-cli-godmodules`. They are re-exported here (eagerly, via
# explicit `from ... import ... as ...`) so every existing
# `from okg.substrate.cli.deployment import _ready_x / _nomos_y` keeps resolving,
# the KEPT `_cmd_*` / `_deployment_*` handlers can call the moved helpers by
# bare name, and tests that `monkeypatch.setattr` these names on THIS module
# rebind the exact name the moved code reads.
#
# The moved code routes every shared reference through the `_dep` module object
# (i.e. THIS module), so a `monkeypatch.setattr(cli.deployment, "_ready_x", ...)`
# is observed by the moved code too. To keep that `_dep` import circular-safe
# regardless of import order, each sibling does its
# `import okg.substrate.cli.deployment as _dep` at its OWN BOTTOM (after all its
# defs). So whether `cli.deployment` or a sibling is imported first, by the time
# control reaches these from-imports the sibling's defs already exist.
from okg.substrate.cli.deployment_readiness import (  # noqa: E402
    ReadinessDeadline as ReadinessDeadline,
    ready_statement_timeout as ready_statement_timeout,
    _ready_probe_timed_out as _ready_probe_timed_out,
    _ready_skip_unknown_dependent as _ready_skip_unknown_dependent,
    _ready_record_probe as _ready_record_probe,
    _ready_add_check as _ready_add_check,
    _ready_check_is_command_failed as _ready_check_is_command_failed,
    _ready_check_is_unavailable as _ready_check_is_unavailable,
    _ready_finalize_payload_state as _ready_finalize_payload_state,
    _ready_mapping as _ready_mapping,
    _ready_payload_status as _ready_payload_status,
    _ready_add_source_sync_liveness_check as _ready_add_source_sync_liveness_check,
    _ready_as_float as _ready_as_float,
    _ready_build_source_sync_status_contract as _ready_build_source_sync_status_contract,
    _ready_database_status as _ready_database_status,
    _ready_postgres_capacity_status as _ready_postgres_capacity_status,
    _ready_declared_baseline_source_groups as _ready_declared_baseline_source_groups,
    _ready_declared_runtime_status as _ready_declared_runtime_status,
    _ready_manifest_and_origin as _ready_manifest_and_origin,
    _ready_resolved_readiness_profile as _ready_resolved_readiness_profile,
    _ready_source_freshness_advisory_status as _ready_source_freshness_advisory_status,
    _ready_add_source_freshness_advisory as _ready_add_source_freshness_advisory,
    SourceRegistryUnloadable as SourceRegistryUnloadable,
    _ready_doctor_status as _ready_doctor_status,
    _ready_embeddings_enabled as _ready_embeddings_enabled,
    _ready_add_generation_count_integrity_check as _ready_add_generation_count_integrity_check,
    _ready_add_publish_block_check as _ready_add_publish_block_check,
    _ready_generation_status as _ready_generation_status,
    _ready_writer_driver_status as _ready_writer_driver_status,
    _ready_is_workspace_graph_deployment as _ready_is_workspace_graph_deployment,
    _ready_load_yaml_mapping as _ready_load_yaml_mapping,
    _ready_mcp_status as _ready_mcp_status,
    _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE as _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE,
    _ready_nomos_active_state_message as _ready_nomos_active_state_message,
    _ready_nomos_active_state_status as _ready_nomos_active_state_status,
    _ready_nomos_audit_status as _ready_nomos_audit_status,
    _ready_nomos_blocking_required as _ready_nomos_blocking_required,
    _ready_nomos_read_probe_status as _ready_nomos_read_probe_status,
    _ready_nomos_source_lineage_status as _ready_nomos_source_lineage_status,
    _ready_parse_datetime as _ready_parse_datetime,
    _ready_parse_json as _ready_parse_json,
    _ready_progress_freshness_sources as _ready_progress_freshness_sources,
    _ready_no_change_expected_delta_is_all_zero as _ready_no_change_expected_delta_is_all_zero,
    _ready_queue_status as _ready_queue_status,
    _ready_required_dependency as _ready_required_dependency,
    _ready_rollout_project_command as _ready_rollout_project_command,
    _ready_rollout_repo_root as _ready_rollout_repo_root,
    _ready_rollout_source_registry as _ready_rollout_source_registry,
    _ready_rollout_source_sync_summary as _ready_rollout_source_sync_summary,
    _ready_rollout_status as _ready_rollout_status,
    _ready_runtime_config_warning as _ready_runtime_config_warning,
    _ready_runtime_source_sync as _ready_runtime_source_sync,
    _ready_slug_for_dsn as _ready_slug_for_dsn,
    _ready_source_freshness_status as _ready_source_freshness_status,
    _ready_source_aliases as _ready_source_aliases,
    _ready_source_groups_for_profile as _ready_source_groups_for_profile,
    _ready_source_optimization_full_pipeline_proof_failed as _ready_source_optimization_full_pipeline_proof_failed,
    _ready_source_optimization_full_pipeline_proof_passed as _ready_source_optimization_full_pipeline_proof_passed,
    _ready_source_optimization_has_full_pipeline_proof as _ready_source_optimization_has_full_pipeline_proof,
    _ready_source_optimization_latest_sample_over_budget as _ready_source_optimization_latest_sample_over_budget,
    _ready_source_optimization_no_change_dominant as _ready_source_optimization_no_change_dominant,
    _ready_source_optimization_observability_gap as _ready_source_optimization_observability_gap,
    _ready_source_optimization_observability_gaps as _ready_source_optimization_observability_gaps,
    _ready_source_optimization_proof_age_seconds as _ready_source_optimization_proof_age_seconds,
    _ready_add_publisher_lock_headroom_check as _ready_add_publisher_lock_headroom_check,
    _ready_publisher_lock_headroom_status as _ready_publisher_lock_headroom_status,
    _ready_source_optimization_publisher_lock_contention as _ready_source_optimization_publisher_lock_contention,
    _ready_source_optimization_publisher_path_blocker as _ready_source_optimization_publisher_path_blocker,
    _ready_source_optimization_status as _ready_source_optimization_status,
    _ready_source_optimization_status_from_ledger as _ready_source_optimization_status_from_ledger,
    _ready_source_optimization_violation_reason as _ready_source_optimization_violation_reason,
    _ready_source_progress_freshness_row as _ready_source_progress_freshness_row,
    _ready_source_sync_liveness_status as _ready_source_sync_liveness_status,
    _ready_source_sync_recovery_commands as _ready_source_sync_recovery_commands,
)
from okg.substrate.cli.nomos_rollout_audit import (  # noqa: E402
    _nomos_add_string_or_list as _nomos_add_string_or_list,
    _nomos_audited_source_policies as _nomos_audited_source_policies,
    _nomos_empty_source_lineage_policy_coverage as _nomos_empty_source_lineage_policy_coverage,
    _nomos_manifest_dsn_env_vars as _nomos_manifest_dsn_env_vars,
    _nomos_match_value_contains_wildcard as _nomos_match_value_contains_wildcard,
    _nomos_normalized_source_policies as _nomos_normalized_source_policies,
    _nomos_policy_fingerprint as _nomos_policy_fingerprint,
    _nomos_required_deployment_names as _nomos_required_deployment_names,
    _nomos_rollout_deployment_entries as _nomos_rollout_deployment_entries,
    _nomos_rollout_entry as _nomos_rollout_entry,
    _nomos_rollout_inventory as _nomos_rollout_inventory,
    _nomos_rollout_profile_entries as _nomos_rollout_profile_entries,
    _nomos_source_lineage_apply_baseline_command as _nomos_source_lineage_apply_baseline_command,
    _nomos_source_lineage_attach_runtime_rollout_state as _nomos_source_lineage_attach_runtime_rollout_state,
    _nomos_source_lineage_dsn_recovery_command as _nomos_source_lineage_dsn_recovery_command,
    _nomos_source_lineage_entry_deployment_dir as _nomos_source_lineage_entry_deployment_dir,
    _nomos_source_lineage_entry_readiness_profile as _nomos_source_lineage_entry_readiness_profile,
    _nomos_source_lineage_ids_for_source_entry as _nomos_source_lineage_ids_for_source_entry,
    _nomos_source_lineage_policy_coverage as _nomos_source_lineage_policy_coverage,
    _nomos_source_lineage_read_policy_source_ids as _nomos_source_lineage_read_policy_source_ids,
    _nomos_source_lineage_resolve_entry_dsn as _nomos_source_lineage_resolve_entry_dsn,
    _nomos_source_lineage_repair_laundered_derived_command as _nomos_source_lineage_repair_laundered_derived_command,
    _nomos_source_lineage_restamp_active_policy_command as _nomos_source_lineage_restamp_active_policy_command,
    _nomos_source_lineage_restamp_publisher_derived_command as _nomos_source_lineage_restamp_publisher_derived_command,
    _nomos_source_lineage_rollout_deployment_entries as _nomos_source_lineage_rollout_deployment_entries,
    _nomos_source_lineage_rollout_entry as _nomos_source_lineage_rollout_entry,
    _nomos_source_lineage_rollout_inventory as _nomos_source_lineage_rollout_inventory,
    _nomos_source_lineage_rollout_owner as _nomos_source_lineage_rollout_owner,
    _nomos_source_lineage_rollout_profile_entries as _nomos_source_lineage_rollout_profile_entries,
    _nomos_source_lineage_rollout_report as _nomos_source_lineage_rollout_report,
    _nomos_source_lineage_rollout_rollback as _nomos_source_lineage_rollout_rollback,
    _nomos_source_lineage_rollout_state_from_runtime_reports as _nomos_source_lineage_rollout_state_from_runtime_reports,
    _nomos_source_lineage_runtime_rollout_state as _nomos_source_lineage_runtime_rollout_state,
    _nomos_source_lineage_source_entries as _nomos_source_lineage_source_entries,
    _nomos_source_lineage_static_rollout_state as _nomos_source_lineage_static_rollout_state,
    _nomos_source_names as _nomos_source_names,
    _nomos_source_registry_entries as _nomos_source_registry_entries,
    _nomos_runtime_source_subject_ids as _nomos_runtime_source_subject_ids,
)


def _compat_reexport_names() -> tuple[Any, ...]:
    """Keep extracted helper re-exports visible to static analysis."""
    return (
        _ready_check_is_command_failed,
        _ready_check_is_unavailable,
        _ready_finalize_payload_state,
        _ready_mapping,
        _ready_payload_status,
        _ready_add_source_sync_liveness_check,
        _ready_as_float,
        _ready_build_source_sync_status_contract,
        _ready_database_status,
        _ready_postgres_capacity_status,
        _ready_declared_baseline_source_groups,
        _ready_declared_runtime_status,
        _ready_manifest_and_origin,
        _ready_resolved_readiness_profile,
        _ready_source_freshness_advisory_status,
        _ready_add_source_freshness_advisory,
        SourceRegistryUnloadable,
        _ready_doctor_status,
        _ready_embeddings_enabled,
        _ready_add_generation_count_integrity_check,
        _ready_generation_status,
        _ready_is_workspace_graph_deployment,
        _ready_mcp_status,
        _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE,
        _ready_nomos_active_state_message,
        _ready_nomos_active_state_status,
        _ready_nomos_audit_status,
        _ready_nomos_blocking_required,
        _ready_nomos_read_probe_status,
        _ready_nomos_source_lineage_status,
        _ready_parse_datetime,
        _ready_parse_json,
        _ready_progress_freshness_sources,
        _ready_no_change_expected_delta_is_all_zero,
        _ready_queue_status,
        _ready_rollout_project_command,
        _ready_rollout_repo_root,
        _ready_rollout_source_registry,
        _ready_rollout_source_sync_summary,
        _ready_rollout_status,
        _ready_runtime_config_warning,
        _ready_runtime_source_sync,
        _ready_slug_for_dsn,
        _ready_source_freshness_status,
        _ready_source_aliases,
        _ready_source_groups_for_profile,
        _ready_source_optimization_full_pipeline_proof_failed,
        _ready_source_optimization_full_pipeline_proof_passed,
        _ready_source_optimization_has_full_pipeline_proof,
        _ready_source_optimization_latest_sample_over_budget,
        _ready_source_optimization_no_change_dominant,
        _ready_source_optimization_observability_gap,
        _ready_source_optimization_observability_gaps,
        _ready_source_optimization_proof_age_seconds,
        _ready_add_publisher_lock_headroom_check,
        _ready_publisher_lock_headroom_status,
        _ready_source_optimization_publisher_lock_contention,
        _ready_source_optimization_publisher_path_blocker,
        _ready_source_optimization_status,
        _ready_source_optimization_status_from_ledger,
        _ready_source_optimization_violation_reason,
        _ready_source_progress_freshness_row,
        _ready_source_sync_liveness_status,
        _ready_source_sync_recovery_commands,
        _nomos_add_string_or_list,
        _nomos_audited_source_policies,
        _nomos_empty_source_lineage_policy_coverage,
        _nomos_manifest_dsn_env_vars,
        _nomos_match_value_contains_wildcard,
        _nomos_normalized_source_policies,
        _nomos_policy_fingerprint,
        _nomos_required_deployment_names,
        _nomos_rollout_deployment_entries,
        _nomos_rollout_entry,
        _nomos_rollout_inventory,
        _nomos_rollout_profile_entries,
        _nomos_source_lineage_apply_baseline_command,
        _nomos_source_lineage_attach_runtime_rollout_state,
        _nomos_source_lineage_dsn_recovery_command,
        _nomos_source_lineage_entry_deployment_dir,
        _nomos_source_lineage_entry_readiness_profile,
        _nomos_source_lineage_ids_for_source_entry,
        _nomos_source_lineage_policy_coverage,
        _nomos_source_lineage_read_policy_source_ids,
        _nomos_source_lineage_resolve_entry_dsn,
        _nomos_source_lineage_repair_laundered_derived_command,
        _nomos_source_lineage_restamp_active_policy_command,
        _nomos_source_lineage_restamp_publisher_derived_command,
        _nomos_source_lineage_rollout_deployment_entries,
        _nomos_source_lineage_rollout_entry,
        _nomos_source_lineage_rollout_inventory,
        _nomos_source_lineage_rollout_owner,
        _nomos_source_lineage_rollout_profile_entries,
        _nomos_source_lineage_rollout_report,
        _nomos_source_lineage_rollout_rollback,
        _nomos_source_lineage_rollout_state_from_runtime_reports,
        _nomos_source_lineage_runtime_rollout_state,
        _nomos_source_lineage_source_entries,
        _nomos_source_lineage_static_rollout_state,
        _nomos_source_names,
        _nomos_source_registry_entries,
    )
