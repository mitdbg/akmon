"""DB-free deployment lint diagnostics.

This module is intentionally CLI-agnostic.  It builds stable, JSON-ready
diagnostic envelopes that command code can render as text or attach under a
future ``deployment ready --explain`` key without changing the existing ready
payload shape.
"""
from __future__ import annotations

import ast
import datetime
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from okg.substrate import fastyaml
from okg.substrate.library.sources.change_probe import (
    BANNED_PROBE_KINDS,
    CHANGE_PROBE_KIND_ATTR,
    SOUND_PROBE_KINDS,
)
from okg.substrate.sources.registry import STRICT_ADMISSION_CONTRACT


LINT_SCHEMA = "okg.deployment_lint.v1"

# Source classes exempt from the change-probe contract — a CLOSED allowlist.
# These perform no scope work (live_overlay reads through at MCP time;
# registry_only is documentation-only), so there is nothing to make incremental.
_PROBE_EXEMPT_SOURCE_CLASSES = frozenset({"registry_only", "live_overlay"})
_SOURCE_ACTIVATION_PROBE_CODES = frozenset({
    "deployment.source_registry.module_missing",
    "deployment.source_registry.class_missing",
    "deployment.source_registry.module_not_found",
    "deployment.source_registry.module_source_unreadable",
    "deployment.source_registry.class_not_found",
    "deployment.source_registry.probe_missing",
    "deployment.source_registry.probe_runtime_missing",
    "deployment.source_registry.probe_unsound",
    "deployment.source_registry.poll_probe_unsound",
    "deployment.source_registry.class_profile_invalid",
    "deployment.source_registry.class_profile_mismatch",
    "deployment.source_registry.param_reserved",
    "deployment.source_registry.param_unknown",
    "deployment.source_registry.param_missing",
    "deployment.source_registry.param_key_invalid",
})

READY_EXPLAIN_SCHEMA = "okg.deployment_ready.explain_diagnostics.v1"

_SEVERITY_ORDER = {
    "blocker": 0,
    "warning": 1,
    "info": 2,
    "skipped": 3,
}
_ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ENV_AUTHORITY_SOURCES = frozenset({
    "operator_secret",
    "operator_env",
    "local_env",
    "process_env",
    "worktree_main_local_env",
    "ci_secret",
})
_ENV_AUTHORITY_PURPOSES = frozenset({"postgres.dsn"})


@dataclass(frozen=True)
class DiagnosticFinding:
    """One operator-facing deployment diagnostic."""

    code: str
    severity: str
    stage: str
    message: str
    why: str
    location: dict[str, Any] | None = None
    observed: Any = None
    expected: Any = None
    next_action: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "severity": self.severity,
            "stage": self.stage,
            "message": self.message,
            "why": self.why,
        }
        if self.location:
            payload["location"] = dict(self.location)
        if self.observed is not None:
            payload["observed"] = self.observed
        if self.expected is not None:
            payload["expected"] = self.expected
        if self.next_action:
            payload["next_action"] = self.next_action
        if self.details:
            payload["details"] = dict(self.details)
        return payload


def lint_deployment(
    deployment: str | Path,
    *,
    deployments_root: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    live: bool = False,
) -> dict[str, Any]:
    """Return a stable lint report for one deployment.

    Default lint is static: it does not connect to a database, import and
    instantiate deployment classes, or mutate local env.  Importability checks
    use module-spec lookup and source-file AST inspection.
    """

    findings: list[DiagnosticFinding] = []
    env = os.environ if env is None else env
    manifest_path = resolve_deployment_manifest_path(
        deployment,
        deployments_root=deployments_root,
    )
    deployment_label = str(deployment)
    raw_manifest: dict[str, Any] | None = None

    if manifest_path is None:
        findings.append(_finding(
            code="deployment.manifest.not_found",
            severity="blocker",
            stage="manifest",
            message=f"deployment {deployment_label!r} was not found",
            why="Lint cannot inspect a deployment without deployment.yaml.",
            next_action=(
                "check deployments/<name>/ or set OKG_DEPLOYMENTS_DIR"
            ),
        ))
        return _report(
            deployment=deployment_label,
            manifest_path=None,
            findings=findings,
            live=live,
        )

    deployment_dir = manifest_path.parent
    try:
        raw = fastyaml.safe_load(
            manifest_path.read_text(encoding="utf-8"),
        ) or {}
    except Exception as exc:  # noqa: BLE001 - normalized below.
        findings.append(_finding(
            code="deployment.manifest.yaml_parse",
            severity="blocker",
            stage="manifest",
            message="deployment.yaml could not be parsed",
            why="A malformed manifest prevents every downstream check.",
            location=_loc(manifest_path),
            observed=type(exc).__name__,
            expected="valid YAML mapping",
            next_action="fix deployment.yaml syntax",
            details={"error": str(exc)},
        ))
        return _report(
            deployment=deployment_label,
            manifest_path=manifest_path,
            findings=findings,
            live=live,
        )
    if not isinstance(raw, dict):
        findings.append(_finding(
            code="deployment.manifest.not_mapping",
            severity="blocker",
            stage="manifest",
            message="deployment.yaml must contain a mapping",
            why="The deployment loader reads named top-level settings.",
            location=_loc(manifest_path),
            observed=type(raw).__name__,
            expected="mapping",
            next_action="replace deployment.yaml with a key/value mapping",
        ))
        return _report(
            deployment=deployment_label,
            manifest_path=manifest_path,
            findings=findings,
            live=live,
        )
    raw_manifest = raw
    deployment_label = str(raw_manifest.get("name") or deployment_dir.name)

    _check_manifest_paths(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_release_block(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_manifest_model(
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_unread_keys(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_runtime_database(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_manifest_env(
        raw_manifest,
        manifest_path=manifest_path,
        env=env,
        findings=findings,
    )
    _check_source_registry(
        raw_manifest,
        deployment_dir=deployment_dir,
        findings=findings,
        enforce_probe=True,
    )
    _check_enrichers(
        raw_manifest,
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_checkpoint_attestors(
        raw_manifest,
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_edge_precedence(
        raw_manifest,
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_runtime_declarations(
        raw_manifest,
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
        live=live,
    )
    _check_deployment_posture(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_source_privacy_posture(
        raw_manifest,
        deployment_dir=deployment_dir,
        env=env,
        findings=findings,
    )
    _check_frozen_sql_checks(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    _check_dead_relation_surfaces(
        raw_manifest,
        manifest_path=manifest_path,
        findings=findings,
    )
    composed_catalog = _check_catalog(
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
    )
    # Reuse the catalog rather than composing a second time (uncached, ~100ms per
    # deployment). `None` here means composition FAILED, and it is passed through
    # as "not supplied" so the route check re-attempts and emits its own
    # unevaluated blocker -- it must not be read as an empty catalog, which would
    # report every field routable-by-vacuum.
    _check_record_reference_routes(
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
        findings=findings,
        composed=composed_catalog,
        # A non-list value is reported as `deployment.manifest.key_type` by
        # the model check; iterating it here would crash lint instead.
        record_prefixes=tuple(
            entry for entry in (
                raw_manifest.get("record_prefixes")
                if isinstance(raw_manifest.get("record_prefixes"), list) else ()
            )
            if isinstance(entry, Mapping)
        ),
    )

    if live:
        findings.append(_finding(
            code="deployment.live_checks.not_implemented",
            severity="skipped",
            stage="runtime",
            message="live database and runtime checks are not implemented here",
            why=(
                "The lint command remains read-only and DB-free; readiness "
                "owns live doctor and runtime checks."
            ),
            location=_loc(manifest_path),
            next_action=(
                "run deployment ready --explain for live readiness checks"
            ),
        ))
    else:
        findings.append(_finding(
            code="deployment.live_checks.skipped",
            severity="skipped",
            stage="runtime",
            message="live database and runtime checks were skipped",
            why=(
                "Static lint is DB-free by default; readiness owns live "
                "doctor and runtime checks."
            ),
            location=_loc(manifest_path),
            next_action=(
                "run deployment ready --explain when live checks are needed"
            ),
        ))

    findings.extend(_check_agent_surface_text(
        raw_manifest if raw_manifest is not None else {},
        deployment=deployment_label,
        deployment_dir=deployment_dir,
        manifest_path=manifest_path,
    ))

    return _report(
        deployment=deployment_label,
        manifest_path=manifest_path,
        findings=findings,
        live=live,
    )


def _check_agent_surface_text(
    raw_manifest: Mapping[str, Any],
    *,
    deployment: str,
    deployment_dir: Path,
    manifest_path: Path,
) -> list[DiagnosticFinding]:
    """The authored agent-surface text, scanned against what it describes.

    BOTH files: the MCP instructions and the chat system prompt. `build_server`
    scans only the instructions, because an edit to chat prose must not refuse
    an unrelated stdio launch — which left the prompt scanned by nothing at
    all until this check existed, and `req.esd.drift-refused` unenforced for
    the channel it was written for. Open WebUI never delivers the MCP
    server's initialize instructions to a model, so that prompt is the only
    instruction channel a chat assistant has.

    Static: files on disk against declared sets, no server and no database.
    """
    from okg.substrate.external_backed.live_relations import (
        LiveRelationInvalid,
    )
    from okg.substrate.mcp.effective_surface import (
        AgentSurfaceInvalid,
        SurfaceTextNamesUnservedOperator,
        assert_surface_files_agree,
        parse_agent_surface,
    )

    # The block's own refusals become `deployment.agent_surface.invalid`.
    # `_check_manifest_model` skips those two types, so the refusal is
    # reported once, under its own code, rather than as the model loader's
    # "unexpected error". Any other exception is a defect in the input or the
    # check, and becomes `deployment.agent_surface.check_error`: never an
    # empty list, which would read as a clean deployment, and never a crash,
    # which would lose every other finding -- and, in `okg deployments
    # audit`, every other deployment's.
    try:
        surface = parse_agent_surface(raw_manifest, manifest_path=manifest_path)
    except (AgentSurfaceInvalid, LiveRelationInvalid) as exc:
        return [_finding(
            code="deployment.agent_surface.invalid",
            severity="blocker",
            stage="manifest",
            message=str(exc).split(". ")[0],
            why=(
                "The agent_surface block is the one statement of what an "
                "agent may call; a block that cannot be parsed bounds "
                "nothing, and the server refuses to load it."
            ),
            location=_loc(manifest_path),
            observed=str(exc),
            next_action=(
                getattr(exc, "fix_hint", None)
                or "fix the agent_surface block per the reported error"
            ),
            details={"error_code": getattr(exc, "code", None)},
        )]
    except Exception as exc:  # noqa: BLE001 - reported as a blocker below.
        return [_agent_surface_check_error(
            exc, step="parsing the agent_surface block",
            manifest_path=manifest_path,
        )]
    if surface is None:
        return []

    # One scan per file, so every offending file is reported in one run:
    # `assert_surface_files_agree` raises on the first file it refuses. When
    # the instructions and the chat prompt are the SAME file, it is scanned
    # twice against two different sets (`served`, then the chat projection's
    # `tools`), so it can produce two findings with different tool lists.
    findings: list[DiagnosticFinding] = []
    for scan, step in (
        (("instructions",), "scanning the MCP instructions file"),
        (("chat_prompt",), "scanning the chat system prompt"),
    ):
        try:
            assert_surface_files_agree(
                surface,
                deployment=deployment,
                deployment_dir=deployment_dir,
                scan=scan,
            )
        except SurfaceTextNamesUnservedOperator as exc:
            data = exc.data or {}
            findings.append(_finding(
                code="deployment.agent_surface.text_names_unserved_operator",
                severity="blocker",
                stage="manifest",
                message=str(exc).split(". ")[0],
                why=(
                    "Authored surface text that names an operator the surface "
                    "withholds tells a model to make a call that will be "
                    "refused at the wire."
                ),
                next_action=(
                    exc.fix_hint
                    or "add the operator to agent_surface.served, or remove the name"
                ),
                details={
                    "source": data.get("source"),
                    "unserved": data.get("unserved"),
                    "served": data.get("served"),
                },
            ))
        except Exception as exc:  # noqa: BLE001 - reported as a blocker.
            findings.append(_agent_surface_check_error(
                exc, step=step, manifest_path=manifest_path,
            ))
    return findings


def _agent_surface_check_error(
    exc: Exception, *, step: str, manifest_path: Path,
) -> DiagnosticFinding:
    """One blocker for an exception the agent-surface check did not expect."""
    return _finding(
        code="deployment.agent_surface.check_error",
        severity="blocker",
        stage="manifest",
        message=f"the agent-surface check failed while {step}",
        why=(
            "An error that is not a declared refusal leaves the agent surface "
            "unchecked; reporting it keeps the lint from reading as clean and "
            "keeps every other finding."
        ),
        location=_loc(manifest_path),
        observed=f"{type(exc).__name__}: {exc}",
        expected="an agent surface the check can read",
        next_action=(
            f"fix the input or the check that raised {type(exc).__name__}"
        ),
        details={"step": step, "error_type": type(exc).__name__},
    )


def source_activation_probe_findings(
    name: str,
    body: Mapping[str, Any],
    *,
    deployment_dir: str | Path,
    registry_path: str | Path,
) -> list[dict[str, Any]]:
    """Return hard probe-contract findings for one prospective active source.

    Normal lint may defer missing-probe findings for an entry explicitly marked
    ``enabled_by_default: false``.  Activation and explicit ingest use this
    seam with enforcement restored, before that dormant candidate can run.
    Structural registry validation remains the normal deployment lint's job.
    """
    deployment_path = Path(deployment_dir)
    source_registry_path = Path(registry_path)
    findings: list[DiagnosticFinding] = []
    _check_source_entry(
        name,
        body,
        deployment_dir=deployment_path,
        registry_path=source_registry_path,
        pointer=f"/sources/{name}",
        findings=findings,
        enforce_probe=True,
        # Grade as normal lint does; an unreadable registry grades as strict.
        legacy_document=_registry_is_legacy(source_registry_path),
    )
    return _sort_finding_dicts([
        finding.as_dict()
        for finding in findings
        if finding.code in _SOURCE_ACTIVATION_PROBE_CODES
        and finding.severity == "blocker"
    ])



def _registry_is_legacy(registry_path: Path) -> bool:
    try:
        document = fastyaml.safe_load(
            registry_path.read_text(encoding="utf-8"),
        )
    except Exception:  # noqa: BLE001 - unreadable grades as strict
        return False
    return (
        isinstance(document, Mapping)
        and document.get("admission_contract") != STRICT_ADMISSION_CONTRACT
    )

def resolve_deployment_manifest_path(
    deployment: str | Path,
    *,
    deployments_root: str | Path | None = None,
) -> Path | None:
    """Resolve a deployment name, directory, or manifest path."""

    candidate = Path(deployment).expanduser()
    if candidate.is_file():
        return candidate.resolve()
    if candidate.is_dir():
        manifest = candidate / "deployment.yaml"
        return manifest.resolve() if manifest.is_file() else None

    if deployments_root is not None:
        manifest = Path(deployments_root).expanduser() / str(deployment)
        manifest = manifest / "deployment.yaml"
        return manifest.resolve() if manifest.is_file() else None

    try:
        from okg.substrate.deployments import find_deployment_dir

        deployment_dir = find_deployment_dir(str(deployment))
    except Exception:  # noqa: BLE001 - lint reports not-found, not tracebacks.
        deployment_dir = None
    if deployment_dir is None:
        return None
    manifest = deployment_dir / "deployment.yaml"
    return manifest.resolve() if manifest.is_file() else None


def diagnostics_from_ready_env_status(
    env_payload: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Adapt the existing ``_deployment_ready_env_status`` shape."""

    if not isinstance(env_payload, Mapping):
        return []
    findings: list[DiagnosticFinding] = []
    manifest_path = env_payload.get("manifest_path")
    location = _loc(Path(str(manifest_path))) if manifest_path else None

    for name in env_payload.get("unresolved") or []:
        findings.append(_finding(
            code="deployment.env.required_missing",
            severity="blocker",
            stage="environment",
            message=f"required environment variable {name} is not set",
            why="Live readiness cannot resolve the deployment DSN.",
            location=location,
            observed="missing",
            expected=str(name),
            next_action="run deployment env doctor for authority details",
            details={
                "recommended_commands": list(
                    env_payload.get("recommended_commands") or []
                ),
            },
        ))

    authority = env_payload.get("authority")
    if isinstance(authority, Mapping):
        conflicts = list(authority.get("conflicts") or [])
        for conflict in conflicts:
            name = (
                conflict.get("name")
                if isinstance(conflict, Mapping)
                else None
            )
            findings.append(_finding(
                code="deployment.env.authority_conflict",
                severity="warning",
                stage="environment",
                message=(
                    f"multiple sources set {name}"
                    if name else "multiple environment sources conflict"
                ),
                why="Operators need to know which source won.",
                location=location,
                next_action="review deployment env doctor authority order",
                details=dict(conflict) if isinstance(conflict, Mapping) else {},
            ))
    return _sorted_dicts(findings)


def build_ready_explain_diagnostics(
    ready_payload: Mapping[str, Any],
    *,
    lint_report: Mapping[str, Any] | None = None,
    env_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the additive diagnostics envelope for ready ``--explain``."""

    findings: list[dict[str, Any]] = []
    if isinstance(lint_report, Mapping):
        findings.extend(list(lint_report.get("findings") or []))
    if isinstance(env_report, Mapping):
        findings.extend(list(env_report.get("findings") or []))
    else:
        env_payload = ready_payload.get("environment")
        if isinstance(env_payload, Mapping):
            findings.extend(diagnostics_from_ready_env_status(env_payload))

    for item in ready_payload.get("blockers") or []:
        findings.append(_ready_check_finding(item, severity="blocker"))
    for item in ready_payload.get("warnings") or []:
        findings.append(_ready_check_finding(item, severity="warning"))
    for item in ready_payload.get("checks") or []:
        if isinstance(item, Mapping) and item.get("status") == "skipped":
            findings.append(_ready_check_finding(item, severity="skipped"))

    normalized = _sort_finding_dicts(findings)
    return {
        "schema": READY_EXPLAIN_SCHEMA,
        "deployment": ready_payload.get("deployment"),
        "ok": not any(f.get("severity") == "blocker" for f in normalized),
        "finding_count": len(normalized),
        "findings": normalized,
    }


def attach_ready_explain_diagnostics(
    ready_payload: Mapping[str, Any],
    *,
    lint_report: Mapping[str, Any] | None = None,
    env_report: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a copy of readiness JSON with an additive diagnostics key."""

    out = dict(ready_payload)
    out["diagnostics"] = build_ready_explain_diagnostics(
        ready_payload,
        lint_report=lint_report,
        env_report=env_report,
    )
    return out


def format_diagnostic_summary(report: Mapping[str, Any]) -> str:
    """Concise human-readable rendering for CLI callers."""

    findings = list(report.get("findings") or [])
    if not findings:
        return "deployment lint: no findings"
    lines = [
        (
            f"deployment lint: {len(findings)} finding(s), "
            f"ok={str(report.get('ok')).lower()}"
        )
    ]
    for finding in findings:
        location = finding.get("location") or {}
        suffix = f" ({location.get('path')})" if location.get("path") else ""
        lines.append(
            f"- {finding.get('severity')} {finding.get('code')}: "
            f"{finding.get('message')}{suffix}"
        )
        if finding.get("why"):
            lines.append(f"  why: {finding.get('why')}")
        if finding.get("next_action"):
            lines.append(f"  next: {finding.get('next_action')}")
    return "\n".join(lines)


def _check_manifest_paths(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    deployment_dir = manifest_path.parent
    for key in ("schema_dir", "source_registry"):
        value = raw.get(key)
        if not isinstance(value, str) or not value.strip():
            findings.append(_finding(
                code=f"deployment.manifest.{key}_missing",
                severity="blocker",
                stage="manifest",
                message=f"deployment.yaml is missing {key}",
                why="This path is required before substrate can load sources.",
                location=_loc(manifest_path, pointer=f"/{key}"),
                observed=value,
                expected="relative path",
                next_action=f"add {key}: <path> to deployment.yaml",
            ))
            continue
        path = deployment_dir / value
        if not path.exists():
            findings.append(_finding(
                code=f"deployment.manifest.{key}_not_found",
                severity="blocker",
                stage="manifest",
                message=f"{key} points at a missing path",
                why="The declaration cannot be used as written.",
                location=_loc(manifest_path, pointer=f"/{key}"),
                observed=str(path),
                expected="existing path",
                next_action=f"create {path} or update {key}",
            ))


def _check_release_block(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """A declared `release:` block must carry a channel.

    Issue #2127. `load_release_contract` refuses a manifest whose `release`
    is not a mapping with `channel` set to dev or prod, and every command
    that enforces the release contract goes through it -- `okg run`, the
    publish gate, `okg mcp-serve`. A block with `kind: standalone` and no
    channel therefore satisfies the publish path and is refused by all of
    them, which is exactly the shape an older fix hint told operators to
    hand-write. Nothing reported it until the operator ran the next command
    and read a refusal about a key they had never heard of.

    Absent is fine here and is NOT reported: a deployment with no `release`
    block at all skips the contract entirely
    (`enforce_release_contract_if_declared`). Only a block that exists and
    cannot load is a defect this check can name.
    """
    if "release" not in raw:
        return
    release = raw.get("release")
    if not isinstance(release, dict):
        findings.append(_finding(
            code="deployment.manifest.release_not_mapping",
            severity="blocker",
            stage="manifest",
            message="deployment.yaml release must be a mapping",
            why=(
                "Every command that loads the release contract reads named "
                "keys from this block and refuses anything else."
            ),
            location=_loc(manifest_path, pointer="/release"),
            observed=type(release).__name__,
            expected="mapping",
            next_action="replace release with a mapping, or remove it",
        ))
        return
    channel = release.get("channel")
    if isinstance(channel, str) and channel.strip() in {"dev", "prod"}:
        return
    findings.append(_finding(
        code="deployment.manifest.release_channel_missing",
        severity="blocker",
        stage="manifest",
        message="deployment.yaml declares release without a valid channel",
        why=(
            "A declared release block with no dev/prod channel is refused "
            "by okg run, the publish gate and okg mcp-serve. Removing the "
            "block is also valid; a half-written one never loads."
        ),
        location=_loc(manifest_path, pointer="/release/channel"),
        observed=channel,
        expected="dev or prod",
        next_action=(
            "add `channel: dev` under release, or delete the release block"
        ),
    ))


def _check_manifest_model(
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Surface the model-level contract rejections as blocker findings.

    ``DeploymentManifest.from_yaml`` is the canonical strict-resolve path:
    the ``deployment-manifest-model`` PACT made it RAISE
    ``DeploymentConfigError`` on an unknown top-level key and on a
    declared-but-missing optional path, instead of silently dropping the
    block. Lint must produce a structured finding for each rather than
    crash, so we call ``from_yaml`` here, catch that one exception type,
    and map the message to a specific finding code. Other failures
    (missing required keys, unresolvable ``${...}`` placeholders) are
    already covered by ``_check_manifest_paths`` / ``_check_manifest_env``;
    we leave them to those checks and do not emit a duplicate here.
    """
    try:
        from okg.substrate.deployments import (
            DeploymentConfigError,
            DeploymentKeyTypeError,
            DeploymentManifest,
        )
    except Exception:  # noqa: BLE001 - lint must not raise on import issues.
        return

    # Outside the import guard above on purpose: these name the refusals the
    # agent-surface check reports, and an import failure here must surface
    # rather than skip the model check in silence.
    from okg.substrate.external_backed.live_relations import (
        LiveRelationInvalid,
    )
    from okg.substrate.mcp.effective_surface import AgentSurfaceInvalid

    try:
        DeploymentManifest.from_yaml(manifest_path)
    except (AgentSurfaceInvalid, LiveRelationInvalid):
        # Reported once, under `deployment.agent_surface.invalid`, by
        # `_check_agent_surface_text`, which parses the block itself so the
        # report does not depend on the model loading this far.
        return
    except DeploymentKeyTypeError as exc:
        findings.append(_finding(
            code="deployment.manifest.key_type",
            severity="blocker",
            stage="manifest",
            message=f"deployment.yaml key {exc.key!r} has the wrong type",
            why=(
                "The key table in okg.substrate.deployments.keys says which "
                "JSON types each top-level value may take; a wrong type was "
                "previously dropped or coerced without an error."
            ),
            location=_loc(manifest_path),
            observed=exc.found,
            expected=" or ".join(exc.allowed),
            next_action=f"give {exc.key!r} a value of an allowed type",
        ))
    except DeploymentConfigError as exc:
        message = str(exc)
        if "unknown top-level key" in message:
            findings.append(_finding(
                code="deployment.manifest.unknown_key",
                severity="blocker",
                stage="manifest",
                message="deployment.yaml has an unknown top-level key",
                why=(
                    "An unknown top-level block is a typo or a key the model "
                    "does not know; it would be silently dropped, so the "
                    "manifest contract rejects it."
                ),
                location=_loc(manifest_path),
                observed=message,
                expected="only known top-level keys",
                next_action=(
                    "fix the key, or add it to the known-key census in "
                    "okg.substrate.deployments"
                ),
            ))
        elif "points at a missing path" in message:
            findings.append(_finding(
                code="deployment.manifest.declared_path_missing",
                severity="blocker",
                stage="manifest",
                message="a declared optional manifest path does not exist",
                why=(
                    "A declared optional path must exist; silently coercing a "
                    "typo to None would disable the component it configures."
                ),
                location=_loc(manifest_path),
                observed=message,
                expected="existing declared path",
                next_action="create the path or remove the key",
            ))
        else:
            # Any other model-level config rejection is still a blocker;
            # surface it generically rather than crash the lint run.
            findings.append(_finding(
                code="deployment.manifest.model_rejected",
                severity="blocker",
                stage="manifest",
                message="deployment manifest failed model validation",
                why="The strict manifest model rejected this deployment.yaml.",
                location=_loc(manifest_path),
                observed=message,
                expected="a model-valid manifest",
                next_action="fix the manifest per the reported error",
            ))
    except ValueError:
        # Missing required keys (ValueError) and unresolvable ${...}
        # placeholders are reported by _check_manifest_paths /
        # _check_manifest_env; do not double-report here.
        return
    except Exception as exc:  # noqa: BLE001 - surface the unexpected, fail loud.
        # A genuinely unexpected model exception (not the ValueError /
        # DeploymentConfigError cases the strict-resolve contract documents and
        # the other checks own) must FAIL the lint gate loudly rather than be
        # silently swallowed by a bare ``return``. Capture its type and message
        # so the operator sees what went wrong instead of a clean pass.
        findings.append(_finding(
            code="deployment.manifest.model_load_error",
            severity="blocker",
            stage="manifest",
            message="loading the deployment manifest raised an unexpected error",
            why=(
                "DeploymentManifest.from_yaml is the canonical strict-resolve "
                "path; an exception type the contract does not anticipate "
                "(not a missing-key ValueError or a DeploymentConfigError) "
                "would otherwise be swallowed, letting a broken manifest pass "
                "lint. Surfacing it keeps lint fail-loud."
            ),
            location=_loc(manifest_path),
            observed=f"{type(exc).__name__}: {exc}",
            expected="a manifest that loads without an unexpected error",
            next_action=(
                "fix the manifest or the model-load code path that raised "
                f"{type(exc).__name__}"
            ),
        ))


def _check_unread_keys(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Warn on a set key that nothing in okg acts on.

    The key table records who reads each key. A key read only by the
    deployment's own code gets no warning; a key nothing reads, and a nested
    key a block accepts but nothing reads, gets one. Lint still passes.
    """
    from okg.substrate.deployments.keys import (
        DEPLOYMENT_KEYS_BY_NAME,
        READ_BY_NOTHING,
    )

    unread: list[tuple[str, str]] = []
    for key, value in raw.items():
        row = DEPLOYMENT_KEYS_BY_NAME.get(key) if isinstance(key, str) else None
        if row is None:
            continue
        if row.read_by == READ_BY_NOTHING:
            unread.append((key, row.reason))
        if isinstance(value, Mapping):
            unread.extend(
                (f"{key}.{nested}", reason)
                for nested, reason in row.unread_keys.items()
                if nested in value
            )
    for key, reason in sorted(unread):
        findings.append(_finding(
            code="deployment.manifest.unread_key",
            severity="warning",
            stage="manifest",
            message=f"nothing in okg acts on the content of {key!r}",
            why=reason,
            location=_loc(manifest_path),
            observed=key,
            expected="only keys okg or the deployment's own code reads",
            next_action=f"remove {key!r} unless the deployment's own code reads it",
        ))


def _check_runtime_database(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Blocker when ``runtime.enabled: true`` declares no ``postgres.database``.

    A runtime-enabled deployment that never names its database is the
    synth-bench-class footgun: the worker comes up but has no declared,
    isolated database authority, so it silently shares whatever
    ``${OKG_DSN}`` happens to point at. The contract requires an enabled
    runtime to declare ``postgres.database``.
    """
    runtime = raw.get("runtime")
    if not isinstance(runtime, Mapping) or runtime.get("enabled") is not True:
        return
    pg_block = raw.get("postgres")
    database = pg_block.get("database") if isinstance(pg_block, Mapping) else None
    if isinstance(database, str) and database.strip():
        return
    findings.append(_finding(
        code="deployment.runtime.database_undeclared",
        severity="blocker",
        stage="runtime",
        message="runtime.enabled is true but postgres.database is not declared",
        why=(
            "A runtime-enabled deployment must name its database so its "
            "worker has an explicit, isolated database authority instead of "
            "silently sharing whatever ${OKG_DSN} resolves to."
        ),
        location=_loc(manifest_path, pointer="/postgres/database"),
        observed=database,
        expected="postgres.database: <name>",
        next_action="declare postgres.database in deployment.yaml",
    ))


def _check_manifest_env(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    env: Mapping[str, str],
    findings: list[DiagnosticFinding],
) -> None:
    pg_block = raw.get("postgres")
    raw_dsn = pg_block.get("dsn") if isinstance(pg_block, Mapping) else None
    dsn_pointer = "/postgres/dsn"
    if raw_dsn is None and "postgres_dsn" in raw:
        raw_dsn = raw.get("postgres_dsn")
        dsn_pointer = "/postgres_dsn"
    if raw_dsn is None:
        findings.append(_finding(
            code="deployment.env.dsn_not_declared",
            severity="warning",
            stage="environment",
            message="deployment has no declared Postgres DSN",
            why="Live commands need a DSN authority before they can run.",
            location=_loc(manifest_path, pointer=dsn_pointer),
            expected="postgres.dsn or postgres_dsn",
            next_action="declare postgres.dsn in deployment.yaml",
        ))
        return
    if not isinstance(raw_dsn, str):
        findings.append(_finding(
            code="deployment.env.dsn_not_string",
            severity="blocker",
            stage="environment",
            message="postgres.dsn must be a string",
            why="DSN interpolation only works on string values.",
            location=_loc(manifest_path, pointer="/postgres/dsn"),
            observed=type(raw_dsn).__name__,
            expected="string",
            next_action="replace postgres.dsn with a DSN string",
        ))
        return
    env_refs = _env_refs(raw_dsn)
    declared = _declared_env_authorities(
        raw,
        env_refs,
        manifest_path=manifest_path,
        findings=findings,
    )
    missing = [name for name in env_refs if not env.get(name)]
    undeclared_missing = [
        name for name in missing if name not in declared
    ]
    declared_missing = [
        name for name in missing if name in declared
    ]
    if undeclared_missing:
        findings.append(_finding(
            code="deployment.env.required_missing",
            severity="warning",
            stage="environment",
            message="deployment DSN references unset environment variables",
            why=(
                "Static lint stays DB-free, but readiness will need these "
                "values to resolve the DSN."
            ),
            location=_loc(manifest_path, pointer=dsn_pointer),
            observed=undeclared_missing,
            expected=(
                "set variables or declare contract.environment.required"
            ),
            next_action=(
                "add contract.environment.required metadata or run "
                "deployment env doctor for authority details"
            ),
        ))
    if declared_missing:
        findings.append(_finding(
            code="deployment.env.required_declared_unset",
            severity="info",
            stage="environment",
            message="deployment DSN env refs are declared but unset",
            why=(
                "Static lint can validate non-secret env authority metadata "
                "without requiring CI to know the secret value."
            ),
            location=_loc(manifest_path, pointer=dsn_pointer),
            observed=declared_missing,
            expected="operator supplies values before runtime commands",
            next_action="run deployment env doctor for authority details",
            details={
                "authorities": {
                    name: declared[name] for name in declared_missing
                },
            },
        ))


def _declared_env_authorities(
    raw: Mapping[str, Any],
    names: list[str],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> dict[str, dict[str, Any]]:
    if not names:
        return {}
    contract = raw.get("contract")
    environment = (
        contract.get("environment") if isinstance(contract, Mapping) else None
    )
    required = (
        environment.get("required")
        if isinstance(environment, Mapping) else None
    )
    if required is None:
        return {}
    declared: dict[str, dict[str, Any]] = {}
    for name in names:
        entry = _env_authority_entry(required, name)
        if entry is None:
            continue
        normalized, problems = _validate_env_authority_entry(entry)
        if problems:
            findings.append(_finding(
                code="deployment.env.authority_invalid",
                severity="warning",
                stage="environment",
                message=f"env authority declaration for {name} is invalid",
                why=(
                    "Unset secret-backed env refs stop being CI debt only "
                    "when their non-secret authority metadata is complete."
                ),
                location=_loc(
                    manifest_path,
                    pointer=f"/contract/environment/required/{name}",
                ),
                observed=problems,
                expected=(
                    "purpose: postgres.dsn, source, owner, review_by"
                ),
                next_action=(
                    f"fix contract.environment.required.{name}"
                ),
            ))
            continue
        declared[name] = normalized
    return declared


def _env_authority_entry(required: Any, name: str) -> Any:
    if isinstance(required, Mapping):
        return required.get(name)
    if isinstance(required, list):
        for item in required:
            if isinstance(item, Mapping) and item.get("name") == name:
                return item
    return None


def _validate_env_authority_entry(
    entry: Any,
) -> tuple[dict[str, Any], list[str]]:
    problems: list[str] = []
    normalized: dict[str, Any] = {}
    if not isinstance(entry, Mapping):
        return {}, ["authority entry must be a mapping"]

    purpose = entry.get("purpose")
    if isinstance(purpose, str) and purpose.strip():
        purpose = purpose.strip()
    else:
        purpose = ""
    if purpose not in _ENV_AUTHORITY_PURPOSES:
        problems.append("purpose must be postgres.dsn")
    else:
        normalized["purpose"] = purpose

    source = entry.get("source")
    if isinstance(source, str) and source.strip():
        source = source.strip()
    else:
        source = ""
    if source not in _ENV_AUTHORITY_SOURCES:
        problems.append(
            "source must be one of "
            + ", ".join(sorted(_ENV_AUTHORITY_SOURCES))
        )
    else:
        normalized["source"] = source

    owner = entry.get("owner")
    if isinstance(owner, str) and owner.strip():
        normalized["owner"] = owner.strip()
    else:
        problems.append("owner must be a non-empty string")

    review_by_raw = entry.get("review_by")
    review_by = _parse_review_date(review_by_raw)
    if review_by is None:
        problems.append("review_by must be an ISO YYYY-MM-DD date")
    elif review_by < datetime.date.today():
        problems.append(f"review_by is expired: {review_by.isoformat()}")
    else:
        normalized["review_by"] = review_by.isoformat()

    reason = entry.get("reason")
    if isinstance(reason, str) and reason.strip():
        normalized["reason"] = reason.strip()
    return normalized, problems


def _parse_review_date(value: Any) -> datetime.date | None:
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str) and value.strip():
        date_text = value.strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
            return None
        try:
            return datetime.date.fromisoformat(date_text)
        except ValueError:
            return None
    return None


def _check_source_registry(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    findings: list[DiagnosticFinding],
    enforce_probe: bool = True,
) -> None:
    registry_value = raw.get("source_registry")
    if not isinstance(registry_value, str) or not registry_value.strip():
        return
    registry_path = deployment_dir / registry_value
    if not registry_path.is_file():
        return
    try:
        doc = fastyaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        findings.append(_finding(
            code="deployment.source_registry.yaml_parse",
            severity="blocker",
            stage="sources",
            message="source_registry.yaml could not be parsed",
            why="Sources cannot be validated until the registry parses.",
            location=_loc(registry_path),
            observed=type(exc).__name__,
            expected="valid YAML mapping",
            next_action="fix source_registry.yaml syntax",
            details={"error": str(exc)},
        ))
        return
    if not isinstance(doc, Mapping):
        findings.append(_finding(
            code="deployment.source_registry.not_mapping",
            severity="blocker",
            stage="sources",
            message="source_registry.yaml must contain a mapping",
            why="The source registry reads a top-level sources mapping.",
            location=_loc(registry_path),
            observed=type(doc).__name__,
            expected="mapping",
            next_action="replace source_registry.yaml with a mapping",
        ))
        return
    raw_sources = doc.get("sources") or {}
    if not isinstance(raw_sources, Mapping):
        findings.append(_finding(
            code="deployment.source_registry.sources_not_mapping",
            severity="blocker",
            stage="sources",
            message="source_registry.yaml sources must be a mapping",
            why="Each source entry is keyed by source name.",
            location=_loc(registry_path, pointer="/sources"),
            observed=type(raw_sources).__name__,
            expected="mapping",
            next_action="make sources a mapping of source name to config",
        ))
        return

    for name in sorted(raw_sources):
        body = raw_sources[name]
        pointer = f"/sources/{name}"
        if not isinstance(body, Mapping):
            findings.append(_finding(
                code="deployment.source_registry.source_not_mapping",
                severity="blocker",
                stage="sources",
                message=f"source {name!r} must be a mapping",
                why="Source declarations use named keys such as module/class.",
                location=_loc(registry_path, pointer=pointer),
                observed=type(body).__name__,
                expected="mapping",
                next_action="replace the source body with a mapping",
            ))
            continue
        # Profile scaffolds carry setup candidates that cannot run until an
        # operator supplies configuration.  An explicit false is the only
        # dormant state: an omitted flag preserves the historical "runs by
        # default" behavior.  Keep inspecting dormant candidates (including
        # imports, profiles, and unsound declarations), but report a missing
        # runtime probe or poll selector as a warning until activation.  The
        # same entry becomes a blocker as soon as it is enabled.
        entry_enforces_probe = (
            enforce_probe
            and body.get("enabled_by_default") is not False
        )
        _check_source_entry(
            name,
            body,
            deployment_dir=deployment_dir,
            registry_path=registry_path,
            pointer=pointer,
            findings=findings,
            enforce_probe=entry_enforces_probe,
            # Wording, plus one grade: a params key a legacy reader's
            # **kwargs absorbs warns instead of blocking (issue #2150, see
            # `_check_source_strict_admission`).
            legacy_document=(
                doc.get("admission_contract") != STRICT_ADMISSION_CONTRACT
            ),
        )


def _check_source_entry_keys(
    name: str,
    body: Mapping[str, Any],
    *,
    registry_path: Path,
    pointer: str,
    findings: list[DiagnosticFinding],
    legacy_document: bool,
) -> None:
    """Name each entry key the registry loader refuses, as ingest would.

    A misspelled key such as ``publicaton_mode`` used to surface only as the
    profile check's ``publication_mode=None``, leaving the reader to guess
    the typo; ingest's loader names it. Same allowed set and suggestion as
    the loader (the strict schema's source properties are
    ``SOURCE_ENTRY_CONTROL_KEYS``). A legacy registry's loader accepts its
    retired keys (``schedule``) with a warning, so lint does too; a strict
    registry refuses them, and lint gives the loader's advice to delete.
    """
    from okg.substrate.sources.registry import (
        LEGACY_RETIRED_ENTRY_KEYS,
        SOURCE_ENTRY_CONTROL_KEYS,
        _unknown_key_error,
    )

    for key in body:
        if key in SOURCE_ENTRY_CONTROL_KEYS:
            continue
        if legacy_document and key in LEGACY_RETIRED_ENTRY_KEYS:
            continue
        error = _unknown_key_error(
            registry_path, f"sources.{name}", key, SOURCE_ENTRY_CONTROL_KEYS,
            source=name,
        )
        if key in LEGACY_RETIRED_ENTRY_KEYS:
            # The strict loader's own advice: nothing reads a
            # per-source `schedule`; moving it under params would let a
            # **kwargs reader absorb it silently.
            next_action = (
                f"delete the `{key}` block of source {name!r} (key "
                f"sources.{name}.{key}); nothing reads it, and a scheduled "
                "refresh is the source's `sync.poll` block"
            )
        elif error.suggestion:
            next_action = f"rename {str(key)!r} to {error.suggestion!r}"
        else:
            next_action = error.hint.lstrip("; ").rstrip(".")
        findings.append(_finding(
            code="deployment.source_registry.entry_key_unknown",
            severity="blocker",
            stage="sources",
            message=f"source {name!r} has an unknown key {str(key)!r}",
            why=(
                "The registry loader refuses an entry key it does not know, "
                "so ingest stops on this entry."
            ),
            location=_loc(registry_path, pointer=f"{pointer}/{key}"),
            observed=str(key),
            expected=(
                f"one of: {', '.join(sorted(SOURCE_ENTRY_CONTROL_KEYS))}"
            ),
            next_action=next_action,
            details={"key": str(key), "suggestion": error.suggestion},
        ))


def _check_source_entry(
    name: str,
    body: Mapping[str, Any],
    *,
    deployment_dir: Path,
    registry_path: Path,
    pointer: str,
    findings: list[DiagnosticFinding],
    enforce_probe: bool = True,
    legacy_document: bool = False,
) -> None:
    _check_source_entry_keys(
        name, body, registry_path=registry_path, pointer=pointer,
        findings=findings, legacy_document=legacy_document,
    )
    if not body.get("ownership_id"):
        findings.append(_finding(
            code="deployment.source_registry.ownership_missing",
            severity="blocker",
            stage="sources",
            message=f"source {name!r} is missing ownership_id",
            why="Producer authority and provenance require ownership_id.",
            location=_loc(registry_path, pointer=f"{pointer}/ownership_id"),
            expected="non-empty ownership_id",
            next_action="add ownership_id to the source entry",
        ))

    admission_policy = body.get("admission_policy")
    if isinstance(admission_policy, Mapping):
        from okg.substrate.producer_authority import (
            check_admission_policy_block_consistency,
        )

        for problem in check_admission_policy_block_consistency(
            admission_policy,
            source_name=name,
        ):
            findings.append(_finding(
                code="deployment.source_registry.admission_policy_block_drift",
                severity="blocker",
                stage="sources",
                message=(
                    f"source {name!r} admission_policy blocks have drifted"
                ),
                why=(
                    "output_signature (clause_ids), policy_clauses, and "
                    "output_scope_summary triplicate the subtype/triple set; "
                    "drift means admission and the human-readable summary "
                    "disagree about what the source may emit."
                ),
                location=_loc(
                    registry_path,
                    pointer=f"{pointer}/admission_policy",
                ),
                observed=problem,
                expected=(
                    "output_signature, policy_clauses, and "
                    "output_scope_summary declare the same set"
                ),
                next_action=(
                    "reconcile the admission_policy blocks so the signature, "
                    "clauses, and scope summary agree"
                ),
            ))

    source_class = body.get("source_class")
    registry_only = source_class == "registry_only"

    # Write-envelope declaration (spec §9.3, PACT consistency-output-envelopes).
    # A fact-producing source needs a write envelope so reuse analysis can ask
    # what it COULD write, not merely what it wrote last time. An envelope is
    # derivable when the source declares an output_signature (nodes/edges) or an
    # explicit write_envelope block; otherwise the producer is undeclared. This
    # is advisory (info) — the gap is made VISIBLE, never silently filled with
    # an invented policy. Registry-only entries emit no facts, so no envelope is
    # expected of them.
    if not registry_only:
        signature = (
            admission_policy.get("output_signature")
            if isinstance(admission_policy, Mapping) else None
        )
        signature = signature if isinstance(signature, Mapping) else {}
        has_signature = bool(signature.get("nodes") or signature.get("edges"))
        has_explicit_envelope = (
            isinstance(admission_policy, Mapping)
            and admission_policy.get("write_envelope") is not None
        )
        if not (has_signature or has_explicit_envelope):
            reason = (
                "no_admission_policy"
                if not isinstance(admission_policy, Mapping)
                else "admission_policy_without_output_signature_or_write_envelope"
            )
            findings.append(_finding(
                code="producer.envelope_missing",
                severity="info",
                stage="sources",
                message=(
                    f"source {name!r} declares no producer write envelope"
                ),
                why=(
                    "Reuse analysis needs the full write envelope (node "
                    "subtypes, edge triples, claim keys, value vocabularies, "
                    "or an explicit unbounded marker) to reason about what a "
                    "producer COULD write; an undeclared source can never be "
                    "certified skippable. Declared-derived only — observed "
                    "output never substitutes."
                ),
                location=_loc(
                    registry_path,
                    pointer=f"{pointer}/admission_policy",
                ),
                observed=reason,
                expected=(
                    "admission_policy.output_signature (envelope derives from "
                    "it) or an explicit admission_policy.write_envelope"
                ),
                next_action=(
                    "declare admission_policy.output_signature, or an explicit "
                    "admission_policy.write_envelope (use unbounded: [...] for "
                    "a genuinely free-form source)"
                ),
                details={"reason": reason},
            ))

    if source_class is None:
        findings.append(_finding(
            code="deployment.source_registry.legacy_contract",
            severity="warning",
            stage="sources",
            message=f"source {name!r} uses the legacy source contract",
            why="Legacy entries are accepted but not fully schedulable.",
            location=_loc(registry_path, pointer=f"{pointer}/source_class"),
            expected="source_class profile",
            next_action="declare a source_class profile when migrating",
        ))
    else:
        try:
            from okg.substrate.sources.profiles import (
                PROFILE_NAMES,
                validate_profile_tuple,
            )

            if source_class not in PROFILE_NAMES:
                raise ValueError(
                    f"unknown source_class {source_class!r}; "
                    f"valid profiles: {sorted(PROFILE_NAMES)}"
                )
            validate_profile_tuple(
                source_class=str(source_class),
                record_identity_kind=_as_str_or_none(
                    body.get("record_identity_kind"),
                ),
                source_revision_kind=_as_str_or_none(
                    body.get("source_revision_kind"),
                ),
                deletion_semantics=_as_str_or_none(
                    body.get("deletion_semantics"),
                ),
                publication_mode=_as_str_or_none(
                    body.get("publication_mode"),
                ),
            )
        except Exception as exc:  # noqa: BLE001 - normalized below.
            findings.append(_finding(
                code="deployment.source_registry.profile_invalid",
                severity="blocker",
                stage="sources",
                message=f"source {name!r} has an invalid source profile",
                why="Invalid source profile tuples are rejected at ingest.",
                location=_loc(registry_path, pointer=pointer),
                observed=str(exc),
                expected="one valid source profile tuple",
                next_action="fix the source_class/profile fields",
            ))

    if registry_only:
        return
    module_name = body.get("module")
    class_name = body.get("class")
    if not isinstance(module_name, str) or not module_name.strip():
        findings.append(_finding(
            code="deployment.source_registry.module_missing",
            severity="blocker",
            stage="sources",
            message=f"source {name!r} is missing module",
            why="Runnable source entries need an importable module.",
            location=_loc(registry_path, pointer=f"{pointer}/module"),
            expected="dotted Python module",
            next_action="add module: pkg.mod to the source entry",
        ))
    if not isinstance(class_name, str) or not class_name.strip():
        findings.append(_finding(
            code="deployment.source_registry.class_missing",
            severity="blocker",
            stage="sources",
            message=f"source {name!r} is missing class",
            why="Runnable source entries need an adapter class.",
            location=_loc(registry_path, pointer=f"{pointer}/class"),
            expected="class name",
            next_action="add class: AdapterClass to the source entry",
        ))
    if isinstance(module_name, str) and module_name.strip():
        findings.extend(_check_python_module_class(
            module_name.strip(),
            class_name.strip() if isinstance(class_name, str) else None,
            deployment_dir=deployment_dir,
            location=_loc(registry_path, pointer=pointer),
            stage="sources",
            code_prefix="deployment.source_registry",
            label=f"source {name!r}",
        ))
        _check_source_probe(
            name,
            source_class=_as_str_or_none(source_class),
            module_name=module_name.strip(),
            class_name=class_name.strip() if isinstance(class_name, str) else None,
            deployment_dir=deployment_dir,
            registry_path=registry_path,
            pointer=pointer,
            findings=findings,
            enforce_probe=enforce_probe,
        )
        _check_source_strict_admission(
            name,
            body,
            source_class=_as_str_or_none(source_class),
            module_name=module_name.strip(),
            class_name=class_name.strip() if isinstance(class_name, str) else None,
            deployment_dir=deployment_dir,
            registry_path=registry_path,
            pointer=pointer,
            findings=findings,
            legacy_document=legacy_document,
        )
    _check_poll_probe(
        name,
        body,
        registry_path=registry_path,
        pointer=pointer,
        findings=findings,
        enforce_probe=enforce_probe,
    )


def _check_source_strict_admission(
    name: str,
    body: Mapping[str, Any],
    *,
    source_class: str | None,
    module_name: str,
    class_name: str | None,
    deployment_dir: Path,
    registry_path: Path,
    pointer: str,
    findings: list[DiagnosticFinding],
    legacy_document: bool = False,
) -> None:
    """Report here what the strict adapter admission would refuse at ingest.

    The strict contract in `okg/substrate/ingest/adapter_factory.py` compares
    the registry's `source_class` against the adapter class's own `profile`
    and refuses `params` keys the substrate injects. Nothing static used to
    read the class side, so a registry whose `source_class` contradicts its
    own reader linted `ok: true` and was refused only once an operator ran
    `okg deployment add-source --apply` -- which promotes the whole registry
    to `strict_v1` and then plans every entry, so one bad shipped default
    blocked adding an unrelated source (issue #2126, findings F2/F13).

    Read statically, by AST, to keep lint's no-import contract: the two
    class-side facts this needs are class-level string literals.

    `params` go through `check_source_params`, the one comparison ingest and
    doctor also run (#2571 item 2): a reserved key, a key that is not a named
    keyword parameter, and a missing required key. The constructor is read
    from the class's source text (`_source_constructor_parameters`); a class
    whose constructor a static read cannot state exactly (an inherited
    `__init__`, a metaclass, a decorated `__init__`, ...) yields reserved-key
    findings only, and doctor, which imports, checks the rest.
    """
    from okg.substrate.ingest.adapter_factory import (
        RUNNABLE_SOURCE_PROFILE_NAMES,
    )
    from okg.substrate.ingest.source_params import (
        RESERVED_ADAPTER_PARAM_NAMES,
        SOURCE_PARAM_RESERVED,
        check_source_params,
    )

    # Blocker on a legacy document too (issue #2150). The legacy branch of
    # `source_adapter_init_params` would still run these entries, but the
    # first `okg deployment add-source` rewrites the whole document as
    # strict_v1 (`guided_add.promote_registry_to_strict`) and then validates
    # it with this same lint. Grading by the document's own
    # `admission_contract` made lint answer `ok: true` for a registry the
    # next add-source refused on sources the operator never touched. The
    # verdict therefore must not depend on the contract field: lint and the
    # add-source validate step see one answer. A legacy document only gets
    # the sentence explaining why a registry the runtime runs is refused.
    # One exception (#2571 item 2): a params key a legacy reader absorbs
    # through **kwargs warns. The legacy runtime passes it and the reader
    # uses it (okg-deployments intersystems-ops reads `seed` from
    # `**params`), so the key is not a defect; the warning names the
    # add-source refusal that promotion would bring.
    severity = "blocker"
    promotion = "" if not legacy_document else (
        " This registry is still on the legacy contract; the first "
        "`okg deployment add-source` rewrites it as "
        f"{STRICT_ADMISSION_CONTRACT}, after which this is refused."
    )

    # An absent or null `params` authors nothing, so required keys are still
    # missing (strict refuses null separately, as source_params_not_mapping).
    params = body.get("params")
    if params is None:
        params = {}
    if isinstance(params, Mapping):
        parameters = (
            _source_constructor_parameters(
                module_name, class_name, deployment_dir=deployment_dir,
            )
            if class_name is not None
            else None
        )
        problems = check_source_params(params, parameters)
        for problem in problems:
            if problem.code == SOURCE_PARAM_RESERVED:
                continue
            findings.append(_source_param_finding(
                name,
                problem,
                registry_path=registry_path,
                pointer=pointer,
                promotion=promotion,
                # A legacy reader whose **kwargs absorbs the key runs today
                # (intersystems-ops readers take `**params`), so lint warns
                # rather than blocking what the runtime accepts; strict
                # refuses it, and the promotion sentence says so.
                severity=(
                    "blocker"
                    if not legacy_document or problem.refused_by_legacy
                    else "warning"
                ),
            ))
        for key in (
            problem.key for problem in problems
            if problem.code == SOURCE_PARAM_RESERVED
        ):
            findings.append(_finding(
                code="deployment.source_registry.param_reserved",
                severity=severity,
                stage="sources",
                message=(
                    f"source {name!r} authors the substrate-owned param "
                    f"{key!r}"
                ),
                why=(
                    "The strict adapter contract injects this parameter from "
                    "deployment authority; a registry-authored value is "
                    "refused, so the source cannot be imported." + promotion
                ),
                location=_loc(
                    registry_path, pointer=f"{pointer}/params/{key}",
                ),
                observed=f"params.{key} declared in the registry",
                expected=(
                    f"no params.{key}; the substrate supplies it "
                    f"(reserved: {sorted(RESERVED_ADAPTER_PARAM_NAMES)})"
                ),
                next_action=(
                    f"delete params.{key} from source {name!r} in "
                    f"{registry_path.name}"
                ),
                details={"reserved_param": key, "source": name},
            ))

    if source_class is None or class_name is None:
        return
    if source_class not in RUNNABLE_SOURCE_PROFILE_NAMES:
        # An unknown or registry_only source_class is already reported by the
        # profile-tuple check; nothing here to add.
        return
    source_path = _module_source_path(module_name, deployment_dir)
    if source_path is None:
        return
    class_profile = _class_str_attr_from_ast(
        source_path,
        class_name,
        "profile",
        module_name=module_name,
        deployment_dir=deployment_dir,
    )
    if class_profile is None:
        return
    if class_profile not in RUNNABLE_SOURCE_PROFILE_NAMES:
        findings.append(_finding(
            code="deployment.source_registry.class_profile_invalid",
            severity=severity,
            stage="sources",
            message=(
                f"source {name!r} names a reader class whose declared "
                f"profile {class_profile!r} is not a source profile"
            ),
            why=(
                "The strict adapter contract reads `profile` off the class "
                "and refuses any value outside the named profile matrix, so "
                "this source can never be imported." + promotion
            ),
            location=_loc(registry_path, pointer=f"{pointer}/class"),
            observed=f"{class_name}.profile == {class_profile!r}",
            expected=(
                "a class `profile` in "
                f"{sorted(RUNNABLE_SOURCE_PROFILE_NAMES)}"
            ),
            next_action=(
                f"set `profile` on {class_name} in {module_name} to the "
                "profile the reader actually implements"
            ),
            details={
                "class_profile": class_profile,
                "module": module_name,
                "class": class_name,
                "source": name,
            },
        ))
        return
    if class_profile != source_class:
        findings.append(_finding(
            code="deployment.source_registry.class_profile_mismatch",
            severity=severity,
            stage="sources",
            message=(
                f"source {name!r} declares source_class {source_class!r} but "
                f"its reader class declares profile {class_profile!r}"
            ),
            why=(
                "The strict adapter contract refuses the mismatch at import, "
                "and the two profiles do not share a run mode, so the source "
                "could not run under the registry's value even if it loaded."
                + promotion
            ),
            location=_loc(registry_path, pointer=f"{pointer}/source_class"),
            observed=(
                f"{pointer}/source_class == {source_class!r}; "
                f"{class_name}.profile == {class_profile!r}"
            ),
            expected=f"source_class == {class_profile!r}",
            next_action=(
                f"set source_class to {class_profile!r} for source {name!r} "
                f"in {registry_path.name} (and the profile tuple that goes "
                f"with it), or change `profile` on {class_name} in "
                f"{module_name}"
            ),
            details={
                "registry_source_class": source_class,
                "class_profile": class_profile,
                "module": module_name,
                "class": class_name,
                "source": name,
            },
        ))



def _source_param_finding(
    name: str,
    problem: Any,
    *,
    registry_path: Path,
    pointer: str,
    promotion: str,
    severity: str,
) -> DiagnosticFinding:
    """One lint finding for an unknown or missing `params` key."""
    from okg.substrate.ingest.source_params import (
        SOURCE_PARAM_KEY_INVALID,
        SOURCE_PARAM_MISSING,
    )

    missing = problem.code == SOURCE_PARAM_MISSING
    return _finding(
        code={
            SOURCE_PARAM_MISSING: "deployment.source_registry.param_missing",
            SOURCE_PARAM_KEY_INVALID: (
                "deployment.source_registry.param_key_invalid"
            ),
        }.get(problem.code, "deployment.source_registry.param_unknown"),
        severity=severity,
        stage="sources",
        message=f"source {name!r}: {problem.message}",
        why=(
            "Ingest compares params with the reader's constructor and "
            "refuses this source, the same check doctor runs." + promotion
        ),
        location=_loc(registry_path, pointer=f"{pointer}/params/{problem.key}"),
        observed=(
            f"params.{problem.key} absent" if missing
            else f"params.{problem.key} declared in the registry"
        ),
        expected="params keys the reader's constructor names",
        next_action=(
            f"add params.{problem.key} to source {name!r} in "
            f"{registry_path.name}" if missing
            else f"rename or delete params.{problem.key} in source {name!r} "
            f"in {registry_path.name}"
        ),
        details={
            "source": name,
            "param": problem.key,
            "source_param_code": problem.code,
        },
    )


def _source_constructor_parameters(
    module_name: str,
    class_name: str,
    *,
    deployment_dir: Path,
) -> Any:
    """The reader constructor's parameters, read without importing it.

    Returns None when the class cannot be found or its constructor cannot be
    stated exactly from source text.
    """
    from okg.substrate.ingest.source_params import (
        static_constructor_parameters,
    )

    resolved = _static_class_module(
        module_name, class_name, deployment_dir=deployment_dir,
    )
    if resolved is None:
        return None
    tree, defining_module, defining_class, is_package = resolved
    return static_constructor_parameters(
        tree,
        defining_class,
        target=f"{defining_module}.{defining_class}",
        base_is_safe=lambda base: _static_base_is_safe(
            base,
            tree=tree,
            module_name=defining_module,
            is_package=is_package,
            deployment_dir=deployment_dir,
            seen=set(),
        ),
    )


# Bases whose class-creation behavior is known not to change a subclass's
# own `__init__` signature (no metaclass `__call__`, no `__signature__`, no
# `__init_subclass__` rewriting the class).
_SAFE_STATIC_BASES = frozenset({
    ("builtins", "object"),
    ("abc", "ABC"),
    ("typing", "Generic"),
})


def _static_base_is_safe(
    base: ast.expr,
    *,
    tree: ast.Module,
    module_name: str,
    is_package: bool,
    deployment_dir: Path,
    seen: set[tuple[str, str]],
) -> bool:
    """Whether a base class leaves a subclass's explicit `__init__` in charge.

    Resolved statically, like the reader: the base must be a known-safe
    standard base or a module-level class that declares no metaclass, binds no
    `__signature__`/`__init_subclass__`, carries no decorator other than a
    plain dataclass, and whose own bases are safe. Anything unresolved is
    unsafe, which makes the caller answer "unknown".
    """
    from okg.substrate.ingest.source_params import bound_names

    if isinstance(base, ast.Subscript):  # Generic[T]
        base = base.value
    if isinstance(base, ast.Name) and base.id == "object":
        return True
    reference: tuple[str, str] | None = None
    if isinstance(base, ast.Name):
        bindings = [n for n in tree.body if base.id in bound_names(n)]
        if len(bindings) != 1:
            return False
        binding = bindings[0]
        if isinstance(binding, ast.ClassDef):
            reference = (module_name, base.id)
        elif isinstance(binding, ast.ImportFrom):
            alias = next(
                (a for a in binding.names if (a.asname or a.name) == base.id),
                None,
            )
            target_module = _resolve_import_module(
                binding, module_name, is_package=is_package,
            )
            if alias is None or target_module is None:
                return False
            reference = (target_module, alias.name)
        else:
            return False
    elif isinstance(base, ast.Attribute) and isinstance(base.value, ast.Name):
        bindings = [n for n in tree.body if base.value.id in bound_names(n)]
        if len(bindings) != 1 or not isinstance(bindings[0], ast.Import):
            return False
        alias = next(
            (
                a for a in bindings[0].names
                if (a.asname or a.name.split(".")[0]) == base.value.id
            ),
            None,
        )
        if alias is None or (alias.asname is None and "." in alias.name):
            return False
        reference = (alias.name, base.attr)
    if reference is None:
        return False
    if reference in _SAFE_STATIC_BASES:
        return True
    if reference in seen or len(seen) > 16:
        return False
    seen.add(reference)
    resolved = _static_class_module(
        reference[0], reference[1], deployment_dir=deployment_dir,
    )
    if resolved is None:
        return False
    base_tree, base_module, base_class, base_is_package = resolved
    node = next(
        n for n in base_tree.body
        if isinstance(n, ast.ClassDef) and n.name == base_class
    )
    if node.keywords:
        return False
    from okg.substrate.ingest.source_params import (
        mutates_class_at_module_level,
    )

    if mutates_class_at_module_level(base_tree, base_class):
        return False
    if any(
        bound_names(item) & {"__signature__", "__init_subclass__"}
        for item in node.body
    ):
        return False
    if node.decorator_list:
        from okg.substrate.ingest.source_params import is_plain_dataclass

        if not is_plain_dataclass(node, base_tree):
            return False
    return all(
        _static_base_is_safe(
            parent,
            tree=base_tree,
            module_name=base_module,
            is_package=base_is_package,
            deployment_dir=deployment_dir,
            seen=seen,
        )
        for parent in node.bases
    )


def _static_class_module(
    module_name: str,
    class_name: str,
    *,
    deployment_dir: Path,
    _depth: int = 0,
) -> tuple[ast.Module, str, str, bool] | None:
    """The parsed module that defines `class_name`, following re-exports.

    Returns the tree, the defining module and class name, and whether that
    module is a package. A re-export counts only when it is the module's
    single binding of the name; a guarded or repeated binding may not be the
    runtime one.
    """
    from okg.substrate.ingest.source_params import bound_names

    if _depth > 4:
        return None
    source_path = _module_source_path(module_name, deployment_dir)
    if source_path is None:
        return None
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
    except (OSError, SyntaxError, ValueError):
        return None
    is_package = source_path.name == "__init__.py"
    bindings = [node for node in tree.body if class_name in bound_names(node)]
    if len(bindings) != 1:
        return None
    binding = bindings[0]
    if isinstance(binding, ast.ClassDef):
        return tree, module_name, class_name, is_package
    if not isinstance(binding, ast.ImportFrom):
        return None
    for alias in binding.names:
        if (alias.asname or alias.name) != class_name or alias.name == "*":
            continue
        target_module = _resolve_import_module(
            binding, module_name, is_package=is_package,
        )
        if target_module is None:
            return None
        return _static_class_module(
            target_module, alias.name,
            deployment_dir=deployment_dir, _depth=_depth + 1,
        )
    return None

def _check_source_probe(
    name: str,
    *,
    source_class: str | None,
    module_name: str,
    class_name: str | None,
    deployment_dir: Path,
    registry_path: Path,
    pointer: str,
    findings: list[DiagnosticFinding],
    enforce_probe: bool,
) -> None:
    """Enforce the incremental-source contract (#323) at lint time.

    A runnable source must resolve to a content-identity-SOUND change-probe:
    a class-level ``change_probe_kind`` literal that lint reads via AST (it does
    not import the adapter) and a runtime ``self.change_probe`` assignment that
    the SourceRunner can execute. Exempt classes are a closed allowlist
    (registry_only, live_overlay). A declared stat-digest probe is rejected
    outright. Normal lint now fails closed for missing declaration/runtime
    wiring; ``enforce_probe`` remains an internal seam for focused helper tests,
    not an operator-configurable migration switch.
    """
    if source_class in _PROBE_EXEMPT_SOURCE_CLASSES:
        return
    if class_name is None:
        return
    source_path = _module_source_path(module_name, deployment_dir)
    if source_path is None:
        return  # module_not_found already flagged by _check_python_module_class
    probe_kind = _source_probe_kind_from_ast(
        source_path, class_name,
        module_name=module_name, deployment_dir=deployment_dir,
    )

    if probe_kind is None:
        findings.append(_finding(
            code="deployment.source_registry.probe_missing",
            severity="blocker" if enforce_probe else "warning",
            stage="sources",
            message=(
                f"source {name!r} does not declare a change-probe "
                f"(class attribute {CHANGE_PROBE_KIND_ATTR})"
            ),
            why=(
                "Every runnable source must resolve to a content-identity-sound "
                "change-probe so a no-change sync short-circuits before reading "
                "its whole scope."
            ),
            location=_loc(registry_path, pointer=pointer),
            observed="no change_probe_kind",
            expected=(
                f"a class-level {CHANGE_PROBE_KIND_ATTR} in "
                f"{sorted(SOUND_PROBE_KINDS)} (use content_hash as the floor)"
            ),
            next_action=(
                f"declare {CHANGE_PROBE_KIND_ATTR} and set self.change_probe in "
                "the adapter, or disable the source"
            ),
            details={"module": module_name, "class": class_name},
        ))
        return

    if probe_kind in BANNED_PROBE_KINDS:
        findings.append(_finding(
            code="deployment.source_registry.probe_unsound",
            severity="blocker",
            stage="sources",
            message=(
                f"source {name!r} declares an unsound stat-digest probe "
                f"({CHANGE_PROBE_KIND_ATTR}={probe_kind!r})"
            ),
            why=(
                "mtime/size are not content identity: a mtime-preserving "
                "in-place edit is a silent false-unchanged."
            ),
            location=_loc(registry_path, pointer=pointer),
            observed=probe_kind,
            expected=f"a sound probe in {sorted(SOUND_PROBE_KINDS)}",
            next_action="use a content-identity probe or the content_hash floor",
            details={"module": module_name, "class": class_name},
        ))
        return

    if probe_kind not in SOUND_PROBE_KINDS:
        findings.append(_finding(
            code="deployment.source_registry.probe_unsound",
            severity="blocker",
            stage="sources",
            message=(
                f"source {name!r} declares an unrecognized change-probe "
                f"({CHANGE_PROBE_KIND_ATTR}={probe_kind!r})"
            ),
            why="An unrecognized probe kind cannot be proven content-sound.",
            location=_loc(registry_path, pointer=pointer),
            observed=probe_kind,
            expected=f"one of {sorted(SOUND_PROBE_KINDS)}",
            next_action=f"set {CHANGE_PROBE_KIND_ATTR} to a recognized sound kind",
            details={"module": module_name, "class": class_name},
        ))
        return

    if not _source_probe_runtime_from_ast(
        source_path,
        class_name,
        module_name=module_name,
        deployment_dir=deployment_dir,
    ):
        findings.append(_finding(
            code="deployment.source_registry.probe_runtime_missing",
            severity="blocker" if enforce_probe else "warning",
            stage="sources",
            message=(
                f"source {name!r} declares {CHANGE_PROBE_KIND_ATTR}="
                f"{probe_kind!r} but does not expose runtime self.change_probe"
            ),
            why=(
                "The static declaration proves the intended probe family, but "
                "the SourceRunner only short-circuits when the adapter instance "
                "sets self.change_probe. Declaration-only probes silently fall "
                "back to full reads and leave the runtime contract unsatisfied."
            ),
            location=_loc(registry_path, pointer=pointer),
            observed="no self.change_probe assignment found by AST",
            expected="adapter __init__/__post_init__ sets self.change_probe",
            next_action=(
                "instantiate a ContentHashProbe, MutableApiProbe, UnionProbe, "
                "or other sound probe and assign it to self.change_probe"
            ),
            details={"module": module_name, "class": class_name},
        ))


# Poll must reuse the adapter/runner change-probe contract or conservatively
# enqueue. Producer-owned selectors are a second, narrower identity boundary
# that can suppress the sound runner gate before it executes.
_ALLOWED_POLL_PROBES = frozenset({"always_changed", "change_probe"})


def _check_poll_probe(
    name: str,
    body: Mapping[str, Any],
    *,
    registry_path: Path,
    pointer: str,
    findings: list[DiagnosticFinding],
    enforce_probe: bool,
) -> None:
    """Require a runner-owned or conservative poll decision (issue #323)."""
    sync = body.get("sync")
    if not isinstance(sync, Mapping):
        return
    triggers = sync.get("triggers")
    if not (isinstance(triggers, (list, tuple)) and "poll" in triggers):
        return
    poll = sync.get("poll")
    poll_probe = poll.get("probe") if isinstance(poll, Mapping) else None
    if isinstance(poll_probe, str) and poll_probe in _ALLOWED_POLL_PROBES:
        return
    legacy_fallback = sync.get("cheap_probe")
    # Dormant catalog entries may defer an absent selector until activation,
    # but an explicitly authored unsupported selector is already a known
    # unsound decision and remains a blocker even while dormant.
    selector_is_explicitly_unsound = (
        poll_probe is not None or legacy_fallback is not None
    )
    findings.append(_finding(
        code="deployment.source_registry.poll_probe_unsound",
        severity=(
            "blocker"
            if enforce_probe or selector_is_explicitly_unsound
            else "warning"
        ),
        stage="sources",
        message=(
            f"source {name!r} does not declare a supported poll decision "
            f"(sync.poll.probe={poll_probe!r})"
        ),
        why=(
            "Producer-owned stat_tree, content_hash, ttl, and remote_etag "
            "selectors duplicate source identity and can return unchanged "
            "before the adapter's validated runner change probe executes."
        ),
        location=_loc(registry_path, pointer=f"{pointer}/sync"),
        observed={
            "poll_probe": poll_probe,
            "legacy_cheap_probe": legacy_fallback,
        },
        expected=sorted(_ALLOWED_POLL_PROBES),
        next_action=(
            "set sync.poll.probe to change_probe, or use always_changed when "
            "the adapter cannot make a sound short-circuit decision"
        ),
    ))


def _source_probe_kind_from_ast(
    source_path: Path,
    class_name: str,
    *,
    module_name: str,
    deployment_dir: Path,
    _depth: int = 0,
) -> str | None:
    """Statically read the adapter class's ``change_probe_kind`` literal."""
    return _class_str_attr_from_ast(
        source_path,
        class_name,
        CHANGE_PROBE_KIND_ATTR,
        module_name=module_name,
        deployment_dir=deployment_dir,
        _depth=_depth,
    )


def _class_str_attr_from_ast(
    source_path: Path,
    class_name: str,
    attr: str,
    *,
    module_name: str,
    deployment_dir: Path,
    _depth: int = 0,
) -> str | None:
    """Statically read one class-level string literal off an adapter class.

    Follows ``from ... import ClassName`` re-exports (e.g. a source registered
    via the ``code_graph`` aggregator module whose class is defined in
    ``code_graph_dataflow``) so an attribute declared on the real class is seen
    regardless of which module the registry names. Imports nothing (consistent
    with the static-lint contract). Returns the declared value, or None when
    the class / that attribute cannot be resolved.
    """
    if _depth > 4:
        return None
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
    except (OSError, SyntaxError):
        return None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return _class_level_str_attr(node, attr)
    # Not defined here — follow a re-export of this class to its source module.
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            if alias.name == "*":
                continue
            if (alias.asname or alias.name) != class_name:
                continue
            target_module = _resolve_import_module(
                node, module_name,
                is_package=source_path.name == "__init__.py",
            )
            if target_module is None:
                continue
            target_path = _module_source_path(target_module, deployment_dir)
            if target_path is None:
                continue
            value = _class_str_attr_from_ast(
                target_path, alias.name, attr,
                module_name=target_module, deployment_dir=deployment_dir,
                _depth=_depth + 1,
            )
            if value is not None:
                return value
    return None


def _source_probe_runtime_from_ast(
    source_path: Path,
    class_name: str,
    *,
    module_name: str,
    deployment_dir: Path,
    _depth: int = 0,
) -> bool:
    """Statically confirm the adapter exposes a runtime ``self.change_probe``.

    The lint still avoids importing deployment code. It accepts assignments in
    construction methods (``__init__`` / ``__post_init__``) on the concrete class
    or resolvable base classes. A concrete construction method that does not set
    ``self.change_probe`` blocks base-class inference; otherwise a subclass could
    override construction without calling ``super()`` and still pass lint.
    """
    if _depth > 4:
        return False
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
    except (OSError, SyntaxError):
        return False
    classes = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
    }
    imports = _class_import_map(
        tree, module_name=module_name, source_path=source_path,
    )
    cls = classes.get(class_name)
    if cls is not None:
        return _class_or_bases_set_change_probe(
            cls,
            classes,
            imports,
            module_name=module_name,
            deployment_dir=deployment_dir,
            seen=set(),
            depth=_depth,
        )

    # Not defined here — follow a re-export of this class to its source module.
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            if alias.name == "*":
                continue
            if (alias.asname or alias.name) != class_name:
                continue
            target_module = _resolve_import_module(
                node, module_name,
                is_package=source_path.name == "__init__.py",
            )
            if target_module is None:
                continue
            target_path = _module_source_path(target_module, deployment_dir)
            if target_path is None:
                continue
            if _source_probe_runtime_from_ast(
                target_path,
                alias.name,
                module_name=target_module,
                deployment_dir=deployment_dir,
                _depth=_depth + 1,
            ):
                return True
    return False


def _class_or_bases_set_change_probe(
    node: ast.ClassDef,
    classes: Mapping[str, ast.ClassDef],
    imports: Mapping[str, tuple[str, str]],
    *,
    module_name: str,
    deployment_dir: Path,
    seen: set[str],
    depth: int,
) -> bool:
    if node.name in seen:
        return False
    seen.add(node.name)
    if _class_constructor_sets_instance_change_probe(node):
        return True
    if _class_defines_constructor(node):
        return False
    for base in node.bases:
        base_name = _base_local_class_name(base)
        base_node = classes.get(base_name)
        if base_node is not None:
            if _class_or_bases_set_change_probe(
                base_node,
                classes,
                imports,
                module_name=module_name,
                deployment_dir=deployment_dir,
                seen=seen,
                depth=depth,
            ):
                return True
            continue
        imported = _imported_base_ref(base, imports)
        if imported is None:
            continue
        target_module, target_class = imported
        target_path = _module_source_path(target_module, deployment_dir)
        if target_path is None:
            continue
        if _source_probe_runtime_from_ast(
            target_path,
            target_class,
            module_name=target_module,
            deployment_dir=deployment_dir,
            _depth=depth + 1,
        ):
            return True
    return False


def _class_import_map(
    tree: ast.Module,
    *,
    module_name: str,
    source_path: Path,
) -> dict[str, tuple[str, str]]:
    imports: dict[str, tuple[str, str]] = {}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            target_module = _resolve_import_module(
                node, module_name, is_package=source_path.name == "__init__.py",
            )
            if target_module is None:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                imports[alias.asname or alias.name] = (target_module, alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name.split(".", 1)[0]
                imports[local] = (alias.name, "")
    return imports


def _class_constructor_sets_instance_change_probe(node: ast.ClassDef) -> bool:
    for stmt in node.body:
        if not (
            isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
            and stmt.name in {"__init__", "__post_init__"}
        ):
            continue
        if _method_sets_instance_change_probe(stmt):
            return True
    return False


def _class_defines_constructor(node: ast.ClassDef) -> bool:
    return any(
        isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef))
        and stmt.name in {"__init__", "__post_init__"}
        for stmt in node.body
    )


def _method_sets_instance_change_probe(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> bool:
    def _walk(statements: list[ast.stmt]) -> bool:
        for child in statements:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if _assignment_sets_instance_change_probe(child):
                return True
            for value in ast.iter_child_nodes(child):
                if isinstance(value, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
                    continue
                if isinstance(value, ast.stmt):
                    if _walk([value]):
                        return True
                elif _expr_sets_instance_change_probe(value):
                    return True
        return False

    return _walk(list(node.body))


def _assignment_sets_instance_change_probe(node: ast.AST) -> bool:
    if isinstance(node, ast.Assign):
        if any(_is_self_change_probe(target) for target in node.targets):
            return True
    elif isinstance(node, ast.AnnAssign):
        if _is_self_change_probe(node.target):
            return True
    elif isinstance(node, ast.AugAssign):
        if _is_self_change_probe(node.target):
            return True
    return False


def _expr_sets_instance_change_probe(node: ast.AST) -> bool:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        if _assignment_sets_instance_change_probe(child):
            return True
        if _expr_sets_instance_change_probe(child):
            return True
    return False


def _is_self_change_probe(node: ast.AST) -> bool:
    if isinstance(node, ast.Attribute):
        return (
            node.attr == "change_probe"
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        )
    if isinstance(node, (ast.Tuple, ast.List)):
        return any(_is_self_change_probe(elt) for elt in node.elts)
    if isinstance(node, ast.Starred):
        return _is_self_change_probe(node.value)
    return False


def _base_local_class_name(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Subscript):
        return _base_local_class_name(node.value)
    return None


def _imported_base_ref(
    node: ast.AST,
    imports: Mapping[str, tuple[str, str]],
) -> tuple[str, str] | None:
    if isinstance(node, ast.Subscript):
        return _imported_base_ref(node.value, imports)
    if isinstance(node, ast.Name):
        ref = imports.get(node.id)
        return ref if ref and ref[1] else None
    parts = _attribute_parts(node)
    if len(parts) < 2:
        return None
    class_name = parts[-1]
    for prefix_len in range(len(parts) - 1, 0, -1):
        prefix = ".".join(parts[:prefix_len])
        ref = imports.get(prefix)
        if ref is None or ref[1]:
            continue
        module_tail = parts[prefix_len:-1]
        module_name = ".".join((ref[0], *module_tail))
        return module_name, class_name
    return None


def _attribute_parts(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        return [*_attribute_parts(node.value), node.attr]
    if isinstance(node, ast.Subscript):
        return _attribute_parts(node.value)
    return []


def _resolve_import_module(
    node: ast.ImportFrom,
    current_module: str,
    *,
    is_package: bool = False,
) -> str | None:
    """Resolve an ImportFrom's target module, handling relative imports.

    When ``current_module`` is a PACKAGE (its source is the ``__init__.py``), a
    level-1 relative import (``from .sub import X``) refers to a submodule of THIS
    package, not the parent — so a package strips one fewer level than a regular
    module. This is what lets a deployment register a source by its package
    (``sources.findings``) whose ``__init__`` re-exports the class from a
    submodule (``from .findings import FindingsSource``).
    """
    if not node.level:
        return node.module
    parts = current_module.split(".")
    strip = node.level - 1 if is_package else node.level
    if strip > len(parts):
        return None
    base = parts[: len(parts) - strip] if strip else list(parts)
    if node.module:
        base = base + node.module.split(".")
    return ".".join(base) if base else None


def _class_level_str_attr(class_node: ast.ClassDef, attr: str) -> str | None:
    for stmt in class_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == attr:
                    value = stmt.value
                    if isinstance(value, ast.Constant) and isinstance(
                        value.value, str,
                    ):
                        return value.value
        elif (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == attr
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        ):
            return stmt.value.value
    return None


_LLM_EXTRACTOR_MODULE = "okg.substrate.library.enrichers.extraction.base"
_EXTRACTOR_ANCESTRY_MAX_DEPTH = 32


def _extractor_ancestry_from_ast(
    module_name: str, class_name: str, *, deployment_dir: Path,
) -> bool | None:
    """Prove extractor ancestry without executing deployment Python.

    None means unknown, not non-extractor. Bindings must be unique and
    unconditional: executing Python to disambiguate them would violate lint's
    no-import contract. Constructors do not alter declared ancestry.
    """
    active: set[tuple[str, str]] = set()
    proven: dict[tuple[str, str], bool] = {}
    parsed: dict[Path, ast.Module] = {}
    remaining = 512

    def resolve(module: str, symbol: str, depth: int) -> bool | None:
        nonlocal remaining
        key = (module, symbol)
        if key in proven:
            return proven[key]
        if remaining <= 0:
            return None
        remaining -= 1
        result = inspect_symbol(module, symbol, depth)
        if result is not None:
            proven[key] = result
        return result

    def inspect_symbol(module: str, symbol: str, depth: int) -> bool | None:
        key = (module, symbol)
        if depth > _EXTRACTOR_ANCESTRY_MAX_DEPTH or key in active:
            return None
        source = _module_source_path(module, deployment_dir)
        if source is None:
            return None
        if key == (_LLM_EXTRACTOR_MODULE, "LLMExtractor"):
            canonical = Path(__file__).parent / "library/enrichers/extraction/base.py"
            return True if source == canonical.resolve() else None
        try:
            if source not in parsed:
                parsed[source] = ast.parse(source.read_text(encoding="utf-8"))
            tree = parsed[source]
        except (OSError, SyntaxError, UnicodeError):
            return None
        namespace_ambiguous = False

        class NamespaceMutation(ast.NodeVisitor):
            # Scan function bodies too: a module-level helper call can mutate
            # exports, and resolving arbitrary call graphs would require
            # execution. Ordinary bodies without namespace operations do not
            # taint resolution; a dormant mutator is conservatively unknown.
            def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
                nonlocal namespace_ambiguous
                namespace_ambiguous = True

            def visit_Global(self, node: ast.Global) -> None:
                nonlocal namespace_ambiguous
                namespace_ambiguous = True

            def visit_Name(self, node: ast.Name) -> None:
                nonlocal namespace_ambiguous
                if node.id in {"exec", "eval", "globals", "locals", "vars", "__builtins__"}:
                    namespace_ambiguous = True

            def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
                nonlocal namespace_ambiguous
                if any(alias.name in {"exec", "eval", "globals", "locals", "vars", "modules"}
                       for alias in node.names):
                    namespace_ambiguous = True

            def visit_Attribute(self, node: ast.Attribute) -> None:
                nonlocal namespace_ambiguous
                if node.attr in {"exec", "eval", "globals", "locals", "vars",
                                 "__dict__", "__bases__", "modules"}:
                    namespace_ambiguous = True
                self.generic_visit(node)

        NamespaceMutation().visit(tree)
        if namespace_ambiguous:
            return None
        bindings: dict[str, list[ast.AST | None]] = {}
        star_import = False

        def bind(name: str, node: ast.AST | None) -> None:
            bindings.setdefault(name, []).append(node)

        class UncertainBindings(ast.NodeVisitor):
            def visit_Name(self, node: ast.Name) -> None:
                if isinstance(node.ctx, (ast.Store, ast.Del)):
                    bind(node.id, None)

            def visit_Attribute(self, node: ast.Attribute) -> None:
                if isinstance(node.ctx, (ast.Store, ast.Del)):
                    parts = _attribute_parts(node)
                    if parts:
                        bind(parts[0], None)
                self.generic_visit(node)

            def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
                if node.name:
                    bind(node.name, None)
                self.generic_visit(node)

            def visit_MatchAs(self, node: ast.MatchAs) -> None:
                if node.name:
                    bind(node.name, None)
                self.generic_visit(node)

            def visit_MatchStar(self, node: ast.MatchStar) -> None:
                if node.name:
                    bind(node.name, None)

            def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
                if node.rest:
                    bind(node.rest, None)
                self.generic_visit(node)

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                bind(node.name, None)

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                bind(node.name, None)

            visit_AsyncFunctionDef = visit_FunctionDef

            def visit_Import(self, node: ast.Import) -> None:
                for alias in node.names:
                    bind(alias.asname or alias.name.split(".")[0], None)

            def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
                nonlocal star_import
                for alias in node.names:
                    if alias.name == "*":
                        star_import = True
                    else:
                        bind(alias.asname or alias.name, None)

        for stmt in tree.body:
            if isinstance(stmt, ast.ClassDef):
                bind(stmt.name, stmt)
            elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
                for alias in stmt.names:
                    if alias.name == "*":
                        star_import = True
                    else:
                        name = alias.asname or (
                            alias.name.split(".")[0]
                            if isinstance(stmt, ast.Import) else alias.name
                        )
                        bind(name, stmt)
            else:
                UncertainBindings().visit(stmt)
        if star_import:
            return None

        def reference(expr: ast.AST) -> bool | None:
            if isinstance(expr, ast.Name):
                if expr.id == "object" and expr.id not in bindings:
                    return False
                return resolve(module, expr.id, depth + 1)
            parts = _attribute_parts(expr) if isinstance(expr, ast.Attribute) else []
            if len(parts) < 2:
                return None
            candidates = bindings.get(parts[0], [])
            if len(candidates) != 1:
                return None
            stmt = candidates[0]
            if not isinstance(stmt, (ast.Import, ast.ImportFrom)):
                return None
            target = import_reference(stmt, parts[0])
            if target is None:
                return None
            target_module, target_symbol = target
            path = [target_module, *([target_symbol] if target_symbol else []), *parts[1:]]
            return resolve(".".join(path[:-1]), path[-1], depth + 1)

        def import_reference(stmt: ast.AST, name: str) -> tuple[str, str] | None:
            if isinstance(stmt, ast.ImportFrom):
                target = _resolve_import_module(
                    stmt, module, is_package=source.name == "__init__.py",
                )
                for alias in stmt.names:
                    if (alias.asname or alias.name) == name and target:
                        return target, alias.name
            elif isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if (alias.asname or alias.name.split(".")[0]) == name:
                        return (alias.name if alias.asname else name), ""
            return None

        candidates = bindings.get(symbol, [])
        if len(candidates) != 1 or candidates[0] is None:
            return None
        node = candidates[0]
        active.add(key)
        try:
            if isinstance(node, ast.ClassDef):
                if node.decorator_list or node.keywords:
                    return None
                results = [reference(base) for base in node.bases]
                if any(result is True for result in results):
                    return True
                return None if any(result is None for result in results) else False
            target = import_reference(node, symbol)
            if target is not None and target[1]:
                return resolve(*target, depth + 1)
            return None
        finally:
            active.remove(key)

    return resolve(module_name, class_name, 0)


def _check_enrichers(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    specs = raw.get("enrichers") or []
    if not specs:
        return
    if not isinstance(specs, list):
        findings.append(_finding(
            code="deployment.enrichers.not_list",
            severity="blocker",
            stage="enrichers",
            message="enrichers must be a list",
            why="Publisher startup reads enrichers in declaration order.",
            location=_loc(manifest_path, pointer="/enrichers"),
            observed=type(specs).__name__,
            expected="list",
            next_action="make enrichers a list of class declarations",
        ))
        return
    for idx, spec in enumerate(specs):
        pointer = f"/enrichers/{idx}"
        if not isinstance(spec, Mapping):
            findings.append(_finding(
                code="deployment.enrichers.entry_not_mapping",
                severity="blocker",
                stage="enrichers",
                message="enricher entry must be a mapping",
                why="Each enricher entry declares at least class: ...",
                location=_loc(manifest_path, pointer=pointer),
                observed=type(spec).__name__,
                expected="mapping",
                next_action="replace the entry with a mapping",
            ))
            continue
        class_path = spec.get("class")
        if not isinstance(class_path, str) or not class_path.strip():
            findings.append(_finding(
                code="deployment.enrichers.class_missing",
                severity="blocker",
                stage="enrichers",
                message="enricher entry is missing class",
                why="The publisher cannot load an unnamed enricher.",
                location=_loc(manifest_path, pointer=f"{pointer}/class"),
                expected="pkg.mod.Class",
                next_action="add class: pkg.mod.Class",
            ))
            continue
        module_name, _, class_name = class_path.strip().rpartition(".")
        if not module_name or not class_name:
            findings.append(_finding(
                code="deployment.enrichers.class_path_invalid",
                severity="blocker",
                stage="enrichers",
                message="enricher class path is not pkg.mod.Class",
                why="The publisher splits class paths on the final dot.",
                location=_loc(manifest_path, pointer=f"{pointer}/class"),
                observed=class_path,
                expected="pkg.mod.Class",
                next_action="use a fully-qualified class path",
            ))
            continue
        class_findings = _check_python_module_class(
            module_name,
            class_name,
            deployment_dir=deployment_dir,
            location=_loc(manifest_path, pointer=pointer),
            stage="enrichers",
            code_prefix="deployment.enrichers",
            label=f"enricher {class_path!r}",
        )
        findings.extend(class_findings)
        config = spec.get("config") or {}
        model_backed = isinstance(config, Mapping) and bool(
            config.get("llm_model") or config.get("llm_canary")
        )
        if not model_backed or class_findings:
            continue
        ancestry = _extractor_ancestry_from_ast(
            module_name, class_name, deployment_dir=deployment_dir,
        )
        if ancestry is None:
            findings.append(_finding(
                code="deployment.enrichers.extractor_ancestry_unknown",
                severity="blocker",
                stage="enrichers",
                message=f"model-backed enricher {class_name} ancestry cannot be verified",
                why="Static lint cannot prove the extractor contract without executing ambiguous Python.",
                location=_loc(manifest_path, pointer=f"{pointer}/class"),
                observed=class_path,
                expected="statically resolvable class ancestry",
                next_action="use explicit class bases and unambiguous imports so lint can verify the quarantine contract",
            ))
        elif ancestry:
            retry_limit = (
                config.get("input_quarantine_max_attempts")
                if isinstance(config, Mapping)
                else None
            )
            if model_backed and (
                not isinstance(retry_limit, int)
                or isinstance(retry_limit, bool)
                or retry_limit <= 0
            ):
                findings.append(_finding(
                    code="deployment.enrichers.input_quarantine_limit_missing",
                    severity="blocker",
                    stage="enrichers",
                    message=(
                        f"model-backed enricher {class_name} requires an "
                        "explicit positive input_quarantine_max_attempts"
                    ),
                    why=(
                        "A silent retry-limit default changes which input the "
                        "publisher permanently gives up on. The operator must "
                        "author this data-loss boundary explicitly."
                    ),
                    location=_loc(
                        manifest_path,
                        pointer=(
                            f"{pointer}/config/"
                            "input_quarantine_max_attempts"
                        ),
                    ),
                    observed=retry_limit,
                    expected="positive integer",
                    next_action=(
                        "set config.input_quarantine_max_attempts to an "
                        "operator-approved positive integer"
                    ),
                ))



def _edge_triples_by_producer(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
) -> tuple[dict[str, set[tuple[str, str, str]]], set[str], dict[str, str]]:
    """Statically declared edge triples per producer, and every producer name.

    Sources declare triples in ``admission_policy`` (``output_signature.edges``
    and edge ``policy_clauses``); enrichers in their class's
    ``requires_narrowings``, read from source without importing it. A
    producer that declares nothing statically contributes no triple.
    """
    triples: dict[str, set[tuple[str, str, str]]] = {}
    producers: set[str] = set()
    identities: dict[str, str] = {}

    def policy_triples(policy: Any) -> set[tuple[str, str, str]]:
        found: set[tuple[str, str, str]] = set()
        if not isinstance(policy, Mapping):
            return found
        signature = policy.get("output_signature")
        if isinstance(signature, Mapping):
            for edge in signature.get("edges") or ():
                if isinstance(edge, Mapping) and all(
                    isinstance(edge.get(key), str)
                    for key in ("src_subtype", "edge_type", "dst_subtype")
                ):
                    found.add((
                        str(edge["src_subtype"]),
                        str(edge["edge_type"]),
                        str(edge["dst_subtype"]),
                    ))
        for clause in policy.get("policy_clauses") or ():
            triple = clause.get("triple") if isinstance(clause, Mapping) else None
            if (
                isinstance(clause, Mapping)
                and clause.get("record_kind") == "edge"
                and isinstance(triple, list)
                and len(triple) == 3
                and all(isinstance(part, str) for part in triple)
            ):
                found.add((triple[0], triple[1], triple[2]))
        return found

    registry_value = raw.get("source_registry")
    doc: Any = {}
    if isinstance(registry_value, str) and registry_value.strip():
        registry_path = deployment_dir / registry_value
        if registry_path.is_file():
            try:
                doc = fastyaml.safe_load(
                    registry_path.read_text(encoding="utf-8"),
                ) or {}
            except Exception:  # noqa: BLE001 - reported by _check_source_registry
                doc = {}
    sources = doc.get("sources") if isinstance(doc, Mapping) else None
    for name, body in (sources or {}).items() if isinstance(sources, Mapping) else ():
        producers.add(str(name))
        if isinstance(body, Mapping) and body.get("ownership_id"):
            # Ingest writes a source's rows under its ownership id.
            identities[str(name)] = str(body["ownership_id"])
            producers.add(str(body["ownership_id"]))
        policy = body.get("admission_policy") if isinstance(body, Mapping) else None
        declared = policy_triples(policy)
        if declared:
            triples.setdefault(str(name), set()).update(declared)
    specs = raw.get("enrichers")
    for spec in specs if isinstance(specs, list) else ():
        if not isinstance(spec, Mapping) or not isinstance(spec.get("class"), str):
            continue
        module_name, _, class_name = str(spec["class"]).strip().rpartition(".")
        class_node = _enricher_class_node(
            module_name, class_name, deployment_dir=deployment_dir,
        )
        config = spec.get("config")
        name = (
            config.get("name")
            if isinstance(config, Mapping) and isinstance(config.get("name"), str)
            else None
        ) or (
            _class_level_str_attr(class_node, "name")
            if class_node is not None else None
        )
        if not name:
            continue
        producer = f"_enricher:{name}"
        producers.add(producer)
        found = triples.setdefault(producer, set())
        found.update(policy_triples(spec.get("authority")))
        if class_node is None:
            continue
        declared = _class_level_literal(class_node, "requires_narrowings")
        if not isinstance(declared, (list, tuple)):
            continue
        emits = _class_level_literal(class_node, "emits_edge_types")
        emitted = (
            {str(edge_type) for edge_type in emits}
            if isinstance(emits, (list, tuple)) else None
        )
        for triple in declared:
            if (
                isinstance(triple, (list, tuple))
                and len(triple) == 3
                and all(isinstance(part, str) for part in triple)
                and (emitted is None or triple[1] in emitted)
            ):
                found.add((triple[0], triple[1], triple[2]))
    return triples, producers, identities


def _enricher_class_node(
    module_name: str,
    class_name: str,
    *,
    deployment_dir: Path,
) -> ast.ClassDef | None:
    if not module_name or not class_name:
        return None
    path = _module_source_path(module_name, deployment_dir)
    if path is None:
        return None
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError):
        return None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    return None


def _class_level_literal(class_node: ast.ClassDef, attr: str) -> Any:
    for stmt in class_node.body:
        value: ast.expr | None = None
        if isinstance(stmt, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == attr
            for target in stmt.targets
        ):
            value = stmt.value
        elif (
            isinstance(stmt, ast.AnnAssign)
            and isinstance(stmt.target, ast.Name)
            and stmt.target.id == attr
        ):
            value = stmt.value
        if value is not None:
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return None
    return None


def _check_edge_precedence(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Refuse an unranked collision of edge producers (item 92a).

    A producer a type's ranking names and another producer that declare the
    same (src subtype, edge type, dst subtype) can write the same edge; the
    graph decides between them by rank only when the ranking names both.
    Also refuses a malformed ``edge_precedence`` block and a ranked producer
    the deployment does not configure (a typo would leave the real producer
    unranked). Producers that share a triple of an unranked type keep the
    last-writer-wins path and are not reported: statically sharing a triple
    does not show they share edge identities, and ranking every such type is
    out of this change's scope.
    """
    from okg.substrate.graph_store_internal.edge_declarers import (
        parse_edge_precedence,
    )

    try:
        ranking = parse_edge_precedence(raw.get("edge_precedence"))
    except ValueError as exc:
        findings.append(_finding(
            code="deployment.edge_precedence.invalid",
            severity="blocker",
            stage="manifest",
            message=f"edge_precedence is invalid: {exc}",
            why=(
                "The graph writer stores the highest-ranked producer's "
                "version of a shared edge; a malformed ranking cannot order them."
            ),
            location=_loc(manifest_path, pointer="/edge_precedence"),
            expected="{edge_type: [producer, ...]} with each producer once",
            next_action="fix edge_precedence in deployment.yaml",
        ))
        return
    triples, producers, identities = _edge_triples_by_producer(
        raw, deployment_dir=deployment_dir,
    )
    for edge_type, ranked in sorted(ranking.items()):
        unknown = sorted(set(ranked) - producers)
        if unknown:
            findings.append(_finding(
                code="deployment.edge_precedence.unknown_producer",
                severity="blocker",
                stage="manifest",
                message=(
                    f"edge_precedence.{edge_type} names producers this "
                    f"deployment does not configure: {', '.join(unknown)}"
                ),
                why=(
                    "A ranked name that matches no source or enricher leaves "
                    "the real producer unranked, and a shared edge it writes "
                    "is then refused at write time."
                ),
                location=_loc(manifest_path, pointer=f"/edge_precedence/{edge_type}"),
                observed=unknown,
                expected=(
                    "a source_registry name or _enricher:<enricher name>"
                ),
                next_action="fix the producer names in edge_precedence",
                details={"edge_type": edge_type, "configured": sorted(producers)},
            ))
    by_triple: dict[tuple[str, str, str], set[str]] = {}
    for producer, declared in triples.items():
        for triple in declared:
            by_triple.setdefault(triple, set()).add(producer)
    for triple, sharing in sorted(by_triple.items()):
        if len(sharing) < 2:
            continue
        edge_type = triple[1]
        ranked = set(ranking.get(edge_type, ()))
        # A source ranked by its ownership id is ranked under its name too.
        ranked |= {
            name for name, ownership in identities.items() if ownership in ranked
        }
        unranked = sorted(sharing - ranked)
        # Scope (Jason, 2026-10-01): only ranked edge types are decided by
        # rank. A triple only unranked producers share keeps today's path;
        # a ranked producer sharing a triple with an unranked one is a
        # collision the ranking cannot order.
        if not unranked or not (sharing & ranked):
            continue
        findings.append(_finding(
            code="deployment.edge_precedence.unranked_collision",
            severity="blocker",
            stage="manifest",
            message=(
                f"{', '.join(sorted(sharing))} all write {triple[0]} "
                f"-{edge_type}-> {triple[2]} edges, and edge_precedence."
                f"{edge_type} does not rank {', '.join(unranked)}"
            ),
            why=(
                "The graph keeps one row per edge. Without a ranking that "
                "names every producer of the edge, the stored version depends "
                "on which producer wrote last, so an incremental publish and a "
                "rebuild disagree."
            ),
            location=_loc(manifest_path, pointer="/edge_precedence"),
            observed={
                "edge_type": edge_type,
                "triple": list(triple),
                "producers": sorted(sharing),
                "unranked": unranked,
            },
            expected=f"edge_precedence.{edge_type} lists every producer above",
            next_action=(
                f"add edge_precedence.{edge_type}: [highest-ranked, ..., "
                "lowest] naming each producer"
            ),
        ))


def _check_checkpoint_attestors(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    from okg.substrate.publisher.checkpoint_attestations import (
        CheckpointAttestationConfigurationError,
        inspect_checkpoint_attestor_declarations,
    )

    deployment_name = str(raw.get("name") or deployment_dir.name)
    try:
        declarations = inspect_checkpoint_attestor_declarations(
            deployment_dir,
            deployment_name=deployment_name,
        )
    except CheckpointAttestationConfigurationError as exc:
        findings.append(_finding(
            code="deployment.checkpoint_attestors.invalid",
            severity="blocker",
            stage="checkpoint_attestors",
            message="checkpoint attestor declarations are invalid",
            why=(
                "The publisher must reject malformed, substituted, or "
                "authority-overriding attestor configuration before publication."
            ),
            location=_loc(manifest_path, pointer="/checkpoint_attestors"),
            observed=str(exc),
            expected="strict id/class/config declarations",
            next_action="fix deployment.yaml checkpoint_attestors",
        ))
        return

    for index, declaration in enumerate(declarations):
        module_name, separator, class_name = declaration.class_ref.partition(":")
        if not separator:
            module_name, _separator, class_name = declaration.class_ref.rpartition(".")
        findings.extend(_check_python_module_class(
            module_name,
            class_name,
            deployment_dir=deployment_dir,
            location=_loc(
                manifest_path,
                pointer=f"/checkpoint_attestors/{index}/class",
            ),
            stage="checkpoint_attestors",
            code_prefix="deployment.checkpoint_attestors",
            label=f"checkpoint attestor {declaration.class_ref!r}",
        ))


def _check_runtime_declarations(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
    live: bool,
) -> None:
    runtime = raw.get("runtime") or {}
    if not isinstance(runtime, Mapping):
        findings.append(_finding(
            code="deployment.runtime.not_mapping",
            severity="blocker",
            stage="runtime",
            message="runtime must be a mapping",
            why="Runtime options and workflows are named settings.",
            location=_loc(manifest_path, pointer="/runtime"),
            observed=type(runtime).__name__,
            expected="mapping",
            next_action="make runtime a mapping",
        ))
        return
    workflows = runtime.get("workflows") or []
    if workflows and not isinstance(workflows, list):
        findings.append(_finding(
            code="deployment.runtime.workflows_not_list",
            severity="blocker",
            stage="runtime",
            message="runtime.workflows must be a list",
            why="The runtime imports workflow modules from a list.",
            location=_loc(manifest_path, pointer="/runtime/workflows"),
            observed=type(workflows).__name__,
            expected="list of dotted modules",
            next_action="make runtime.workflows a list",
        ))
        return
    for idx, workflow in enumerate(workflows):
        pointer = f"/runtime/workflows/{idx}"
        if not isinstance(workflow, str) or not workflow.strip():
            findings.append(_finding(
                code="deployment.runtime.workflow_invalid",
                severity="blocker",
                stage="runtime",
                message="runtime workflow must be a dotted module string",
                why="The runtime imports each workflow declaration.",
                location=_loc(manifest_path, pointer=pointer),
                observed=workflow,
                expected="dotted Python module",
                next_action="replace the workflow entry with a module path",
            ))
            continue
        findings.extend(_check_python_module_class(
            workflow.strip(),
            None,
            deployment_dir=deployment_dir,
            location=_loc(manifest_path, pointer=pointer),
            stage="runtime",
            code_prefix="deployment.runtime",
            label=f"workflow {workflow!r}",
        ))
    if runtime.get("enabled") is True and not live:
        findings.append(_finding(
            code="deployment.runtime.dbos_schema_skipped",
            severity="skipped",
            stage="runtime",
            message="DBOS/runtime schema readiness was not checked",
            why="Static lint does not connect to the deployment database.",
            location=_loc(manifest_path, pointer="/runtime"),
            next_action="run readiness explain for live runtime checks",
        ))


def _registry_sources(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
) -> tuple[Path, Mapping[str, Any]] | None:
    """Return the registry path and its ``sources`` mapping, or None.

    Parse failures are reported by ``_check_source_registry``; this helper
    only yields a registry that check accepted.
    """
    registry_value = raw.get("source_registry")
    if not isinstance(registry_value, str) or not registry_value.strip():
        return None
    registry_path = deployment_dir / registry_value
    if not registry_path.is_file():
        return None
    try:
        doc = fastyaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}
    except Exception:  # noqa: BLE001 - reported by _check_source_registry.
        return None
    sources = doc.get("sources") if isinstance(doc, Mapping) else None
    if not isinstance(sources, Mapping):
        return None
    return registry_path, sources


def _check_source_privacy_posture(
    raw: Mapping[str, Any],
    *,
    deployment_dir: Path,
    env: Mapping[str, str],
    findings: list[DiagnosticFinding],
) -> None:
    """Warn about what ``store_raw: false`` needs and does (issue #2504).

    Each source's policy is resolved through the same Nomos audit ingest uses
    (inline ``source_policy``, then ``nomos.source_policy_defaults``). A
    source whose audit fails is skipped: ingest then either stores it raw
    under a deferred rollout or refuses it, and neither path needs the key.
    """
    registry = _registry_sources(raw, deployment_dir=deployment_dir)
    if registry is None:
        return
    registry_path, sources = registry
    try:
        from okg.substrate.nomos.deployment_audit import (
            audit_source_privacy_policy,
        )
    except Exception:  # noqa: BLE001 - lint must not raise on import issues.
        return
    not_raw: dict[str, Mapping[str, Any]] = {}
    for name in sorted(sources):
        entry = sources[name]
        if not isinstance(entry, Mapping):
            continue
        try:
            report = audit_source_privacy_policy(str(name), entry, manifest=raw)
        except Exception:  # noqa: BLE001 - lint must not raise on audit bugs.
            continue
        policy = report.get("source_policy")
        if report.get("ok") and isinstance(policy, Mapping):
            if policy.get("store_raw") is False:
                not_raw[str(name)] = policy
    if not not_raw:
        return

    from okg.substrate.alias.pseudonymize import (
        PSEUDONYM_KEY_ENV,
        PseudonymKeyUnavailable,
        load_pseudonym_service,
    )

    try:
        load_pseudonym_service(env=env)
    except PseudonymKeyUnavailable as exc:
        findings.append(_finding(
            code="deployment.nomos.pseudonym_key_missing",
            severity="warning",
            stage="nomos",
            message=(
                f"sources with store_raw: false need {PSEUDONYM_KEY_ENV}, "
                "which is unset or too short"
            ),
            why=(
                "A store_raw: false source pseudonymizes identifiers with "
                "this key; without it every such source fails at ingest "
                "with PseudonymKeyUnavailable."
            ),
            location=_loc(registry_path, pointer="/sources"),
            observed=sorted(not_raw),
            expected=f"{PSEUDONYM_KEY_ENV} set from the deployment secret store",
            next_action=(
                f"set {PSEUDONYM_KEY_ENV} (at least 16 bytes) before ingest, "
                "or set store_raw: true for these sources"
            ),
            details={"error": str(exc)},
        ))

    for name, policy in not_raw.items():
        pii_classes = policy.get("pii_classes")
        declared = (
            set(pii_classes.values()) if isinstance(pii_classes, Mapping) else set()
        )
        if declared & {"non_pii", "free_text"}:
            continue
        findings.append(_finding(
            code="deployment.nomos.store_raw_masks_all",
            severity="warning",
            stage="nomos",
            message=(
                f"source {name!r} has store_raw: false and declares no "
                "non_pii or free_text attribute, so lexical search over it "
                "will match nothing"
            ),
            why=(
                "An attribute pii_classes does not declare is treated as "
                "sensitive and stored as [nomos:masked]; search text is "
                "built from those masked values."
            ),
            location=_loc(registry_path, pointer=f"/sources/{name}"),
            observed=sorted(str(value) for value in declared),
            expected="pii_classes marking the searchable attributes",
            next_action=(
                "declare source_policy.pii_classes: mark `text` and the "
                "subtypes' searchable fields non_pii, or free_text (needs "
                "the privacy extra's detector, else ingest holds the facts)"
            ),
        ))


def _check_deployment_posture(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Run the static portion of the Nomos posture audit inside lint.

    Surfaces a finding when ``nomos.deployment_posture`` is missing or
    incomplete (or fails its class requirements), reusing
    ``audit_deployment_policy``'s static manifest checks. Findings are
    warnings so a posture gap is reported but does not hard-block an otherwise
    valid deployment; a deployment that defers its rollout
    (``nomos.rollout.status: deferred``) is exempt from the missing-posture
    finding because its bounded-rollout governance is checked separately.
    """
    nomos = raw.get("nomos")
    nomos_map = nomos if isinstance(nomos, Mapping) else {}
    rollout = nomos_map.get("rollout") if isinstance(nomos_map, Mapping) else None
    deferred = (
        isinstance(rollout, Mapping)
        and str(rollout.get("status") or "") == "deferred"
    )
    has_posture = (
        isinstance(nomos_map, Mapping)
        and isinstance(nomos_map.get("deployment_posture"), Mapping)
    )

    if not has_posture:
        if deferred:
            return
        findings.append(_finding(
            code="deployment.nomos.deployment_posture_missing",
            severity="warning",
            stage="nomos",
            message="deployment has no nomos.deployment_posture",
            why=(
                "A declared posture must either be enforced or surfaced as a "
                "gap; an absent posture leaves the deployment ungoverned."
            ),
            location=_loc(manifest_path, pointer="/nomos/deployment_posture"),
            expected="nomos.deployment_posture mapping",
            next_action=(
                "add nomos.deployment_posture, or declare nomos.rollout "
                "(status: deferred) with an owner and review_by date"
            ),
        ))
        return

    try:
        from okg.substrate.nomos.deployment_audit import audit_deployment_policy
    except Exception:  # noqa: BLE001 - lint must not raise on import issues.
        return
    audit = audit_deployment_policy(raw, source_registry=None)
    for raw_finding in audit.get("findings") or []:
        if not isinstance(raw_finding, Mapping):
            continue
        code = str(raw_finding.get("code") or "")
        # Only posture-shaped findings are static manifest checks; source
        # findings need the registry which lint audits separately.
        if not code.startswith("nomos.audit.deployment_posture") and code not in {
            "nomos.audit.unknown_deployment_class",
        }:
            continue
        findings.append(_finding(
            code=f"deployment.{code}",
            severity="warning",
            stage="nomos",
            message=str(raw_finding.get("message") or "posture audit finding"),
            why=(
                "The static deployment-posture audit reports an incomplete or "
                "non-conforming posture."
            ),
            location=_loc(
                manifest_path,
                pointer="/" + str(raw_finding.get("path") or "nomos").replace(
                    ".", "/",
                ),
            ),
            details={
                "audit_code": code,
                **(
                    dict(raw_finding.get("details") or {})
                    if isinstance(raw_finding.get("details"), Mapping)
                    else {}
                ),
            },
        ))


_SQL_CHECK_BLOCKS = ("consistency_gates", "post_publish_consistency_checks")


def _check_frozen_sql_checks(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Enforce the SQL-check freeze (PACT consistency-checks-as-ir).

    The deterministic_sql (candidate_sql) authoring path is frozen: the
    landed fleet's check_ids are grandfathered with a warning until their
    deployments migrate; any NEW SQL check is a BLOCKER pointing at the IR
    authoring surface and the equivalence harness.
    """
    from okg.substrate.consistency.sql_gate import (
        FROZEN_SQL_CHECK_ALLOWLIST,
        SQL_CHECK_FREEZE_POINTER,
    )

    deployment_name = str(raw.get("name") or "")
    for block in _SQL_CHECK_BLOCKS:
        entries = raw.get(block)
        if not isinstance(entries, list):
            continue
        for idx, entry in enumerate(entries):
            if not isinstance(entry, Mapping):
                continue
            check_id = str(entry.get("check_id") or f"{block}[{idx}]")
            pointer = f"/{block}/{idx}"
            if (deployment_name, check_id) in FROZEN_SQL_CHECK_ALLOWLIST:
                findings.append(_finding(
                    code="deployment.consistency.sql_check_frozen",
                    severity="warning",
                    stage="consistency",
                    message=(
                        f"{check_id} uses the frozen deterministic_sql path"
                    ),
                    why=SQL_CHECK_FREEZE_POINTER,
                    location=_loc(manifest_path, pointer=pointer),
                    next_action=(
                        "re-author as ir_consistency_checks and prove parity "
                        "with okg.substrate.consistency.equivalence"
                    ),
                ))
            else:
                findings.append(_finding(
                    code="deployment.consistency.new_sql_check",
                    severity="blocker",
                    stage="consistency",
                    message=(
                        f"{check_id} is a NEW deterministic_sql check; the "
                        "SQL path is frozen"
                    ),
                    why=SQL_CHECK_FREEZE_POINTER,
                    location=_loc(manifest_path, pointer=pointer),
                    expected=(
                        "an ir_consistency_checks entry (transform-IR program)"
                    ),
                    next_action=(
                        "author the check as ir_consistency_checks; see PACT "
                        "consistency-checks-as-ir and prove parity with "
                        "okg.substrate.consistency.equivalence"
                    ),
                ))


# Retired live-graph relations → the maintained chronos-native read surface.
# nodes_live/edges_live still exist as tables but the chronos-native publisher
# no longer writes them (publisher.py: "Chronos-native publishing dropped
# okg.nodes_live/okg.edges_live"), so a check reading them scans an EMPTY table
# and silently passes. v_nodes/v_edges are NOT the fix — they select FROM the
# dead tables and inherit the deadness. Live data lives in the Chronos base
# tables okg.graph_nodes/okg.graph_edges; but a raw SQL gate cannot apply the
# generation pin (Chronos applies it via the checkout session), so the real fix
# is to author the check as an ir_consistency_checks program, which reads the
# pinned graph surfaces correctly.
_DEAD_GRAPH_RELATIONS = {
    "okg.nodes_live": "okg.graph_nodes",
    "okg.edges_live": "okg.graph_edges",
}
_DEAD_RELATION_RE = re.compile(
    r"\bokg\.(nodes_live|edges_live)\b", re.IGNORECASE
)
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_SQL_TEXT_KEYS = ("candidate_sql", "coverage_sql")


def _strip_sql_line_comments(sql: str) -> str:
    """Drop ``-- …`` line comments before dead-relation scanning so a merely
    commented-out read is not a false positive. The stripped text feeds
    read-detection only (never execution), so corrupting a ``--`` that sits
    inside a string literal cannot mis-detect a real FROM clause (which never
    lives inside a literal). Block comments / string literals are left as a
    known, low-risk edge."""
    return _SQL_LINE_COMMENT_RE.sub("", sql)


def _check_dead_relation_surfaces(
    raw: Mapping[str, Any],
    *,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
) -> None:
    """Flag consistency-gate SQL bound to a retired, unmaintained relation.

    A gate reading okg.nodes_live/okg.edges_live on a chronos-native deployment
    scans an empty table and silently passes (a false-negative publish gate).
    A NEW check gets a BLOCKER; the landed fleet's grandfathered checks (also
    migrating off the frozen SQL path) get a WARNING so the fleet's lint is not
    double-blocked. The diagnostic names the check, the dead relation, and the
    maintained surface + IR-migration fix.

    The grandfather tier is FROZEN_SQL_CHECK_ALLOWLIST (shared with the SQL
    freeze). consistency-deployment-currency (task.dep.prototype-disposition)
    retired the prototype gates of two private deployments (one deleted
    since) and dropped their allowlist pairs, so a dead-relation read by
    either is now a BLOCKER
    (no longer grandfathered). cern-twiki is the only remaining grandfathered
    pair, pending its cross-repo IR re-authoring (task.dep.twiki-ir-reauthor);
    delete this grandfather tier once that lands and no manifest reads the dead
    relations.
    """
    from okg.substrate.consistency.sql_gate import FROZEN_SQL_CHECK_ALLOWLIST

    deployment_name = str(raw.get("name") or "")
    for block in _SQL_CHECK_BLOCKS:
        entries = raw.get(block)
        if not isinstance(entries, list):
            continue
        for idx, entry in enumerate(entries):
            if not isinstance(entry, Mapping):
                continue
            sql_text = _strip_sql_line_comments("\n".join(
                str(entry.get(key) or "") for key in _SQL_TEXT_KEYS
            ))
            dead = sorted({
                f"okg.{m.group(1).lower()}"
                for m in _DEAD_RELATION_RE.finditer(sql_text)
            })
            if not dead:
                continue
            check_id = str(entry.get("check_id") or f"{block}[{idx}]")
            replacements = ", ".join(
                f"{rel} -> {_DEAD_GRAPH_RELATIONS[rel]}" for rel in dead
            )
            grandfathered = (
                (deployment_name, check_id) in FROZEN_SQL_CHECK_ALLOWLIST
            )
            findings.append(_finding(
                code="deployment.consistency.dead_relation_read",
                severity="warning" if grandfathered else "blocker",
                stage="consistency",
                message=(
                    f"{check_id} reads retired relation(s) "
                    f"{', '.join(dead)}"
                ),
                why=(
                    "The chronos-native publisher no longer maintains "
                    f"{', '.join(dead)} (the tables exist but stay empty), so "
                    "the check scans an empty relation and silently passes."
                ),
                location=_loc(manifest_path, pointer=f"/{block}/{idx}"),
                observed=", ".join(dead),
                expected=(
                    "the maintained chronos-native surface "
                    f"({replacements})"
                ),
                next_action=(
                    "re-author as ir_consistency_checks (it reads the pinned "
                    "graph surfaces correctly); a raw SQL read of "
                    "okg.graph_nodes/okg.graph_edges is NOT generation-pinned"
                ),
                details={"dead_relations": dead, "grandfathered": grandfathered},
            ))


def _check_catalog(
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
):
    """Verify the catalog composes; RETURN it so later checks need not recompose.

    `compose_catalog` is not cached and costs ~100ms per deployment, which is
    material in the lint suites that lint every in-repo deployment (measured on
    CI: composing a second time added ~8s to
    test_lint_gate_is_green_over_all_in_repo_deployments alone). Returns None
    when composition failed — the caller must treat that as "unavailable", never
    as an empty catalog.
    """
    try:
        from okg.substrate.catalog.modules_compose import compose_catalog

        return compose_catalog(deployment_dir)
    except Exception as exc:  # noqa: BLE001 - operator-facing lint.
        code = getattr(exc, "code", None) or "deployment.catalog.compose_error"
        data = getattr(exc, "data", None)
        location_path = manifest_path
        narrowing_location_keys = {
            "narrowing_schema_misplaced": "schema_path",
            "narrowing_bridge_directory_symlink": "bridges_path",
        }
        if code in narrowing_location_keys and isinstance(data, Mapping):
            narrowing_path = data.get(narrowing_location_keys[code])
            if isinstance(narrowing_path, str):
                location_path = Path(narrowing_path)
        findings.append(_finding(
            code=f"deployment.catalog.{code}",
            severity="blocker",
            stage="catalog",
            message="deployment catalog could not be composed",
            why=(
                "Catalog composition validates modules, deployment schemas, "
                "bridges, and narrowing subtype references."
            ),
            location=_loc(location_path),
            observed=str(exc),
            expected="composable catalog",
            next_action=(
                getattr(exc, "fix_hint", None)
                or "fix module/schema/bridge declarations"
            ),
            details=dict(data) if isinstance(data, Mapping) else {},
        ))
        # Explicit: None means "could not compose", NOT "empty catalog". A caller
        # reusing this must re-derive or report unevaluated — never read the
        # absence as a clean catalog.
        return None


#: Module that declares each reference field's target subtypes, for the fields
#: whose whole target set is owned by one module. Used only to turn a finding
#: into actionable advice ("add `bookkeeping`"); a field absent here still gets
#: reported, just without a module hint. Verified against
#: `ComposedCatalog.subtype_origin` by
#: test_deployment_lint_record_routes.py::test_field_owner_hints_match_catalog_origin,
#: so this table cannot drift from the ontology without failing a test.
_REFERENCE_FIELD_OWNER_HINT: dict[str, str] = {
    "project_ref": "bookkeeping",
    "study_ref": "agent_memory",
    "domain_refs": "agent_memory",
    "session_ref": "agent_sessions",
}


def _check_record_reference_routes(
    *,
    deployment_dir: Path,
    manifest_path: Path,
    findings: list[DiagnosticFinding],
    record_prefixes: tuple[Mapping[str, Any], ...],
    composed: Any = None,
) -> None:
    """Flag record template fields this deployment's catalog cannot route.

    A record template that advertises a reference field with no declared route
    creates the primary node and DROPS the link, returning only a warning — the
    #947 defect. The build-time guard for that class composes every module, so it
    cannot see a deployment that activates a subset: `study references project`
    exists only with `bookkeeping` active, so a deployment omitting it still
    advertises `project_ref` on `study` with nothing to route to.

    Fail-closed: a catalog that cannot be composed yields an `unevaluated`
    finding, never silence. `_check_catalog` already blocks on a compose failure,
    but a caller reading only this check's output must not be able to mistake
    "could not evaluate" for "nothing wrong".

    `composed` accepts the catalog `_check_catalog` already built. Passing it
    avoids a second `compose_catalog` (uncached, ~100ms per deployment — measured
    on CI as ~8s on the lint-every-deployment test alone). Absence means
    "not supplied", so this still composes for itself; it must NEVER be read as
    an empty catalog, which is why the parameter defaults to None rather than to
    a blank catalog object. `record_prefixes` is required so lint evaluates the
    same deployment extensions as a real record write; omitting it would silently
    fall back to builtins and could make the two surfaces disagree.
    """
    from okg.substrate.mcp.record import evaluate_record_reference_routes

    # EVERY step that can fail belongs inside this try, not just the compose.
    # `narrowing_triples()` / `.subtypes` / `.modules` are catalog operations and
    # the evaluator itself can raise; leaving them outside meant an exception
    # there propagated out of lint_deployment and took down every OTHER check,
    # producing no finding at all — the opposite of fail-closed.
    try:
        if composed is None:
            from okg.substrate.catalog.modules_compose import compose_catalog

            composed = compose_catalog(deployment_dir)
        active_subtypes = composed.subtypes
        triples = composed.narrowing_triples()
        active_modules = set(composed.modules)
        unroutable, evaluated = evaluate_record_reference_routes(
            prefixes=record_prefixes,
            active_subtypes=active_subtypes,
            narrowing_triples=triples,
        )
    except Exception as exc:  # noqa: BLE001 - operator-facing lint.
        findings.append(_finding(
            code="deployment.record.reference_routes_unevaluated",
            severity="blocker",
            stage="catalog",
            message=(
                "record reference routes could not be evaluated"
            ),
            why=(
                "Without a composed catalog and a completed evaluation this "
                "check cannot tell whether a record template advertises a link "
                "the deployment cannot create. Reporting nothing would read as "
                "'no problems'."
            ),
            location=_loc(manifest_path),
            observed=f"{type(exc).__name__}: {exc}",
            expected="composable catalog and a completed route evaluation",
            next_action=(
                getattr(exc, "fix_hint", None)
                or "fix the catalog composition error reported above, then "
                   "re-run lint"
            ),
        ))
        return

    # A deployment that activates NO record template checked zero fields. That is
    # not the same as having no problems, and it must not read as clean: if a
    # deployment later dropped `agent_memory`, its existing findings would drop
    # to zero and look FIXED. Distinct `skipped` state, never silence.
    if not evaluated["available_templates"]:
        findings.append(_finding(
            code="deployment.record.reference_routes_not_applicable",
            severity="skipped",
            stage="catalog",
            message=(
                "record reference routes were not evaluated: no record template "
                "is available in this catalog"
            ),
            why=(
                "The record templates need their target subtypes (agent_memory "
                "and friends) in the active catalog. With none available this "
                "check examined zero fields, so a clean result here says "
                "nothing about the deployment."
            ),
            location=_loc(manifest_path),
            observed=(
                f"available_templates=0 of {evaluated['total_templates']}, "
                "checked_fields=0"
            ),
            expected=(
                "either a catalog that activates the record templates, or an "
                "explicit understanding that record() is unavailable here"
            ),
            next_action=(
                "no action if this deployment intentionally does not use "
                "record(); otherwise activate the modules that own the record "
                "subtypes"
            ),
            details=dict(evaluated),
        ))
        return
    for (kind, ref_field), info in sorted(unroutable.items()):
        owner = _REFERENCE_FIELD_OWNER_HINT.get(ref_field)
        missing_module = owner if owner and owner not in active_modules else None
        if missing_module:
            expected = (
                f"a declared route for {ref_field}; its target subtypes are "
                f"owned by the '{missing_module}' module, which this deployment "
                "does not activate"
            )
            next_action = (
                f"add '{missing_module}' to the deployment's modules and "
                "re-apply the catalog (okg catalog load --deployment "
                "<name> --apply)"
            )
        else:
            expected = f"a declared route for {ref_field}"
            next_action = (
                "declare the missing narrowing in the owning module, or stop "
                f"advertising {ref_field} on the {kind} record template"
            )
        findings.append(_finding(
            code="deployment.record.unroutable_reference_field",
            severity="warning",
            stage="catalog",
            message=(
                f"record(kind=\"{kind}\") advertises {ref_field} but this "
                "catalog declares no route for it"
            ),
            why=(
                "record() will create the primary node and DROP the link, "
                "returning a warning rather than an error, so a caller that "
                "does not read reference_diagnostics sees a silent data loss."
            ),
            location=_loc(manifest_path),
            observed=(
                f"{ref_field} -> {info.get('edge_type')} -> (no allowed target "
                f"subtype); reasons: {', '.join(info.get('reasons') or []) or 'none reported'}"
            ),
            expected=expected,
            next_action=next_action,
            details={
                "record_kind": kind,
                "field": ref_field,
                "edge_type": info.get("edge_type"),
                "role": info.get("role"),
                "reasons": list(info.get("reasons") or []),
                "missing_module": missing_module,
                # Carried on every finding so a consumer can always tell how
                # much surface was examined to produce it.
                **evaluated,
            },
        ))


def _check_python_module_class(
    module_name: str,
    class_name: str | None,
    *,
    deployment_dir: Path,
    location: dict[str, Any],
    stage: str,
    code_prefix: str,
    label: str,
) -> list[DiagnosticFinding]:
    findings: list[DiagnosticFinding] = []
    source_path = _module_source_path(module_name, deployment_dir)
    if source_path is None:
        findings.append(_finding(
            code=f"{code_prefix}.module_not_found",
            severity="blocker",
            stage=stage,
            message=f"{label} module is not importable",
            why=(
                "The declaration points at a module the runtime cannot "
                "resolve."
            ),
            location=location,
            observed=module_name,
            expected="importable Python module",
            next_action="fix the module path or Python package layout",
        ))
        return findings
    if class_name is None:
        return findings
    try:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"),
            filename=str(source_path),
        )
    except (OSError, SyntaxError) as exc:
        findings.append(_finding(
            code=f"{code_prefix}.module_source_unreadable",
            severity="blocker",
            stage=stage,
            message=f"{label} module source could not be inspected",
            why="Static lint needs source text to avoid importing code.",
            location=location,
            observed=str(exc),
            expected="readable Python source",
            next_action="fix module source syntax or path",
        ))
        return findings
    declared = {
        node.name for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    declared.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(
            node.target, ast.Name,
        ):
            declared.add(node.target.id)
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                declared.add(alias.asname or alias.name)
    if class_name not in declared:
        findings.append(_finding(
            code=f"{code_prefix}.class_not_found",
            severity="blocker",
            stage=stage,
            message=f"{label} class is not declared in the module source",
            why="The runtime would fail when looking up the declared class.",
            location=location,
            observed=class_name,
            expected=f"class or exported symbol in {module_name}",
            next_action="fix the class name or module path",
            details={"module_source": str(source_path)},
        ))
    return findings


def _module_source_path(module_name: str, deployment_dir: Path) -> Path | None:
    rel = Path(*module_name.split("."))
    local_candidates = [
        deployment_dir / rel.with_suffix(".py"),
        deployment_dir / rel / "__init__.py",
        deployment_dir.parent / rel.with_suffix(".py"),
        deployment_dir.parent / rel / "__init__.py",
    ]
    for candidate in local_candidates:
        if candidate.is_file():
            return candidate.resolve()
    for entry in sys.path:
        if not entry:
            continue
        root = Path(entry)
        for candidate in (
            root / rel.with_suffix(".py"),
            root / rel / "__init__.py",
        ):
            if candidate.is_file():
                return candidate.resolve()
    return None


def _report(
    *,
    deployment: str,
    manifest_path: Path | None,
    findings: list[DiagnosticFinding],
    live: bool,
) -> dict[str, Any]:
    normalized = _sorted_dicts(findings)
    return {
        "schema": LINT_SCHEMA,
        "deployment": deployment,
        "manifest_path": str(manifest_path) if manifest_path else None,
        "deployment_dir": (
            str(manifest_path.parent) if manifest_path is not None else None
        ),
        "mode": "live" if live else "static",
        "db_required": bool(live),
        "ok": not any(f["severity"] == "blocker" for f in normalized),
        "finding_count": len(normalized),
        "findings": normalized,
    }


def _finding(
    *,
    code: str,
    severity: str,
    stage: str,
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
        stage=stage,
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


def _env_refs(value: str | None) -> list[str]:
    if not isinstance(value, str):
        return []
    return sorted(set(_ENV_REF_RE.findall(value)))


def _as_str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _sorted_dicts(findings: list[DiagnosticFinding]) -> list[dict[str, Any]]:
    return _sort_finding_dicts([finding.as_dict() for finding in findings])


def _sort_finding_dicts(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        findings,
        key=lambda f: (
            _SEVERITY_ORDER.get(str(f.get("severity")), 99),
            str(f.get("stage") or ""),
            str(f.get("code") or ""),
            str((f.get("location") or {}).get("path") or ""),
            str((f.get("location") or {}).get("pointer") or ""),
            str(f.get("message") or ""),
        ),
    )


def _ready_check_finding(
    item: Any,
    *,
    severity: str,
) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        item = {"message": str(item)}
    check_id = str(item.get("id") or item.get("check") or "ready.check")
    return {
        "code": f"deployment.ready.{check_id}",
        "severity": severity,
        "stage": "ready",
        "message": str(item.get("message") or check_id),
        "why": "Readiness reported this condition.",
        "details": dict(item),
    }


__all__ = [
    "DiagnosticFinding",
    "LINT_SCHEMA",
    "READY_EXPLAIN_SCHEMA",
    "attach_ready_explain_diagnostics",
    "build_ready_explain_diagnostics",
    "diagnostics_from_ready_env_status",
    "format_diagnostic_summary",
    "lint_deployment",
    "resolve_deployment_manifest_path",
    "source_activation_probe_findings",
]
