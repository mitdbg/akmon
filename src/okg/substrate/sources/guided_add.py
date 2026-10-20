"""Guided add-a-source flow behind `okg deployment add-source` (issue #1802).

The flow has two halves. `plan_add_source` reads everything and writes
nothing: it resolves the deployment, checks the source type is a supported
reader, validates the user's settings against the reader's declared schema,
checks access, works out which ontology modules and privacy-posture
decisions the source needs, runs a bounded preview read, and renders the
update / removal behaviour in plain language. `apply_add_source` is the one
confirmation: it saves the definition, validates the deployment, applies the
catalog when modules changed, brings the access policy up to date with the
new source, imports and publishes through the existing `okg ingest` policy,
and finishes with one searchable sample and its source.

Every apply step is recorded in `<deployment>/.okg/source-add/<name>.json`.
A failed step names its cause, leaves the saved definition disabled, and
re-running the same command resumes from validation: same registry key, same
record identities, so a retry duplicates nothing and tears nothing down.

Secrets never enter this module's output: access is checked by environment
variable NAME (presence only) or by `gh auth status`; settings that look like
secrets are refused before anything is saved.
"""
from __future__ import annotations

import copy
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePath
from typing import Any, Callable, Mapping, Optional, Sequence

import yaml
from jsonschema import Draft202012Validator

from okg.deployment import ConnectorRegistration, registration_registry_entry
from okg.substrate import fastyaml
from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.errors import AddSourceError, SubstrateError
from okg.substrate.publish_outcome import (
    BLOCKED,
    NO_CHANGE,
    NO_SOURCE_WORK,
    PUBLISHED,
    SKIPPED,
    classify_publish,
    describe_generation_command,
)
from okg.substrate.nomos.core import looks_secret_key
from okg.substrate.source_run_modes import (
    WHOLE_SCOPE_REQUEST,
    resolve_whole_scope_request,
)
from okg.substrate.sources.guided_catalog import (
    SUPPORTED_SOURCE_TYPES,
    SupportedSourceType,
    packaged_type,
    removal_behaviour_text,
    supported_type,
    update_behaviour_text,
)
from okg.substrate.sources.guided_defaults import EXTRACTORS_YAML
from okg.substrate.sources.redaction import redact_dsn, redact_mapping, redact_text


STRICT_ADMISSION_CONTRACT = "strict_v1"
LEDGER_DIRNAME = ".okg/source-add"
PREVIEW_RECORD_LIMIT = 5
PREVIEW_SCAN_LIMIT = 2000
SUBPROCESS_TIMEOUT_SECONDS = 1800

Runner = Callable[..., "subprocess.CompletedProcess[str]"]


@dataclass
class Step:
    name: str
    status: str  # ok | failed | pending | skipped
    detail: dict[str, Any] = field(default_factory=dict)
    cause: Optional[str] = None
    fix_hint: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"step": self.name, "status": self.status}
        if self.detail:
            out["detail"] = self.detail
        if self.cause:
            out["cause"] = self.cause
        if self.fix_hint:
            out["fix_hint"] = self.fix_hint
        return out


@dataclass
class AddSourcePlan:
    deployment_name: str
    deployment_dir: Path
    registry_path: Path
    name: str
    source_type: Optional[SupportedSourceType]
    settings: dict[str, Any]
    params: dict[str, Any]
    entry: dict[str, Any]
    steps: list[Step] = field(default_factory=list)
    decisions: list[dict[str, Any]] = field(default_factory=list)
    preview: dict[str, Any] = field(default_factory=dict)
    existing_entry: Optional[dict[str, Any]] = None
    # Single words from the previewed records' body text (never their title,
    # name or path); the sample step searches them one at a time.
    body_queries: list[str] = field(default_factory=list)
    # The formats the previewed folder holds and the planned change to
    # extractors.yaml (local-folder only; see `_plan_file_readers`).
    reader_formats: list[str] = field(default_factory=list)
    file_readers: Optional[dict[str, Any]] = None
    # Schema files shipped next to a packaged connector's `connector.json`
    # and the deployment paths the save step copies them to (#2478).
    connector_schemas: list[dict[str, str]] = field(default_factory=list)
    # Files a source manifest generates (`--manifest`): the saved manifest
    # copy and the catalog types, each with the text the save step writes
    # and whether it is new (`write`), replaces an earlier generated copy
    # (`replace`) or is already there (`present`).
    generated_files: list[dict[str, str]] = field(default_factory=list)
    # Plain-language behaviour that replaces the registration's own text
    # (a manifest's `deletion: never` never removes a record).
    behaviour_overrides: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return all(step.status != "failed" for step in self.steps)

    @property
    def failed_step(self) -> Optional[Step]:
        for step in self.steps:
            if step.status == "failed":
                return step
        return None

    def as_dict(self) -> dict[str, Any]:
        registration = (
            self.source_type.registration if self.source_type else None
        )
        return {
            "schema": "okg.deployment.add_source.plan/v1",
            "deployment": self.deployment_name,
            "deployment_dir": str(self.deployment_dir),
            "name": self.name,
            "type": self.source_type.type_id if self.source_type else None,
            "ok": self.ok,
            "settings": redact_mapping(dict(self.settings)),
            "retry_of_saved_definition": self.existing_entry is not None,
            "steps": [step.as_dict() for step in self.steps],
            "decisions": list(self.decisions),
            "preview": self.preview,
            "behaviour": {
                "update": (
                    update_behaviour_text(registration) if registration else None
                ),
                "removal": self.behaviour_overrides.get("removal") or (
                    removal_behaviour_text(registration) if registration else None
                ),
                "refresh": (
                    self.source_type.refresh.replace(
                        "<name>", self.deployment_name,
                    )
                    if self.source_type else None
                ),
            },
        }


@dataclass
class AddSourceOutcome:
    plan: AddSourcePlan
    status: str  # published | no_change | failed
    steps: list[Step]
    generation_id: Optional[str] = None
    counts: dict[str, Any] = field(default_factory=dict)
    sample: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        failed = next((s for s in self.steps if s.status == "failed"), None)
        return {
            "schema": "okg.deployment.add_source.outcome/v1",
            "deployment": self.plan.deployment_name,
            "name": self.plan.name,
            "type": (
                self.plan.source_type.type_id if self.plan.source_type else None
            ),
            "status": self.status,
            "generation_id": self.generation_id,
            "counts": self.counts,
            "sample": self.sample,
            "steps": [step.as_dict() for step in self.steps],
            "failed_step": failed.as_dict() if failed else None,
            "retry": (
                None if failed is None else
                "Correct the cause and re-run the same command; the saved "
                "definition is kept disabled until the import publishes."
            ),
            "behaviour": self.plan.as_dict()["behaviour"],
        }


# --------------------------------------------------------------------------
# Planning (read-only)
# --------------------------------------------------------------------------


def plan_add_source(
    *,
    deployment: str,
    name: str,
    type_id: Optional[str],
    settings: Mapping[str, Any],
    credential_refs: Sequence[str] = (),
    registration_path: Optional[str | Path] = None,
    preview_limit: int = PREVIEW_RECORD_LIMIT,
    environ: Optional[Mapping[str, str]] = None,
    runner: Optional[Runner] = None,
    deployments_root: Optional[str | Path] = None,
    manifest_path: Optional[str | Path] = None,
) -> AddSourcePlan:
    """Read-only plan: validate, decide, preview. Never writes.

    With `manifest_path` the source is described by a source manifest
    (`okg.source/v1`) instead of a supported type or a packaged connector.
    """
    env = dict(os.environ if environ is None else environ)
    run = runner or _default_runner
    steps: list[Step] = []

    deployment_dir, deployment_name, registry_path = _resolve_deployment(
        deployment, deployments_root=deployments_root,
    )
    plan = AddSourcePlan(
        deployment_name=deployment_name,
        deployment_dir=deployment_dir,
        registry_path=registry_path,
        name=name,
        source_type=None,
        settings=dict(settings),
        params={},
        entry={},
        steps=steps,
    )
    if not _valid_source_name(name):
        steps.append(Step(
            "name", "failed",
            cause=f"{name!r} is not a valid source name",
            fix_hint="use letters, digits, '-' or '_' (start with a letter)",
        ))
        return plan

    if manifest_path is not None:
        _plan_manifest(
            plan, Path(manifest_path), env=env, run=run, preview_limit=preview_limit,
        )
        return plan

    source_type = _resolve_type(type_id, registration_path, steps)
    if source_type is None:
        return plan
    plan.source_type = source_type

    raw_registry = _load_registry(registry_path)
    existing = (raw_registry.get("sources") or {}).get(name)
    if isinstance(existing, Mapping):
        same_reader = (
            existing.get("module") == source_type.registration.module
            and existing.get("class") == source_type.registration.class_name
        )
        if not same_reader:
            steps.append(Step(
                "name", "failed",
                cause=(
                    f"source {name!r} already exists in this deployment with "
                    "a different reader"
                ),
                fix_hint="choose another name, or remove the old source first",
            ))
            return plan
        plan.existing_entry = dict(existing)

    if not _validate_settings(source_type, plan.settings, steps):
        return plan
    plan.params = _params_for(source_type, plan.name, plan.settings, deployment_dir)
    plan.entry = build_registry_entry(
        source_type,
        deployment_name=deployment_name,
        name=name,
        params=plan.params,
        credential_refs=tuple(credential_refs),
    )
    steps.append(Step("settings", "ok", detail={
        "settings": redact_mapping(dict(plan.settings)),
    }))

    if not _check_reader(plan, steps):
        return plan
    if not _check_access(
        source_type, plan.entry, env, run, steps,
        credential_refs=tuple(credential_refs),
    ):
        return plan
    if not _decide_modules(
        source_type, deployment_dir, plan, registration_path=registration_path,
    ):
        return plan
    _decide_posture(source_type, deployment_dir, plan)
    _preview(source_type, plan, preview_limit, steps)
    return plan


def build_registry_entry(
    source_type: SupportedSourceType,
    *,
    deployment_name: str,
    name: str,
    params: Mapping[str, Any],
    credential_refs: Sequence[str] = (),
    enabled: bool = True,
) -> dict[str, Any]:
    """The strict source-registry body for one instance of a supported type."""
    entry = registration_registry_entry(source_type.registration)
    ownership_id = f"{deployment_name}.{name}"
    entry["ownership_id"] = ownership_id
    entry["admission_policy"]["producer_id"] = ownership_id
    entry["enabled_by_default"] = bool(enabled)
    signature = entry["admission_policy"].get("output_signature") or {}
    if signature.get("nodes") or signature.get("edges"):
        # The human-readable scope summary must name the same set as the
        # declared signature (deployment lint checks the two agree).
        entry["admission_policy"]["output_scope_summary"] = {
            "nodes": [str(n["subtype"]) for n in signature.get("nodes") or ()],
            "edges": [
                f"{e['src_subtype']} {e['edge_type']} {e['dst_subtype']}"
                for e in signature.get("edges") or ()
            ],
            "scope": source_type.summary,
        }
    entry["params"] = json.loads(json.dumps(dict(params)))
    entry["source_policy"] = json.loads(json.dumps(dict(source_type.source_policy)))
    refs = list(dict.fromkeys(
        [*source_type.registration.credential_refs, *credential_refs]
    ))
    if refs:
        entry["credential_refs"] = refs
    elif "credential_refs" in entry:
        del entry["credential_refs"]
    return entry


def _resolve_type(
    type_id: Optional[str],
    registration_path: Optional[str | Path],
    steps: list[Step],
) -> Optional[SupportedSourceType]:
    if registration_path is not None:
        path = Path(registration_path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            registration = ConnectorRegistration.from_asset_payload(payload)
        except (OSError, ValueError, SubstrateError) as exc:
            steps.append(Step(
                "type", "failed",
                cause=(
                    f"connector registration {path} is not usable: "
                    f"{redact_text(str(exc))}"
                ),
                fix_hint=(
                    "pass the connector asset body the package declares "
                    "(`ConnectorRegistration.as_asset_payload()`)"
                ),
            ))
            return None
        source_type = packaged_type(registration)
        steps.append(Step("type", "ok", detail={
            "type": source_type.type_id, "origin": "packaged_connector",
        }))
        return source_type
    source_type = supported_type(type_id or "")
    if source_type is None:
        steps.append(Step(
            "type", "failed",
            detail={"supported_types": sorted(SUPPORTED_SOURCE_TYPES)},
            cause=(
                f"no installed reader supports source type {type_id!r}; a new "
                "reader is needed before this kind of source can be added"
            ),
            fix_hint=(
                "run `okg deployment source-types` for the supported types, or "
                "see the developer guide's 'Add a source adapter' section; "
                "arbitrary files or services are not translated automatically"
            ),
        ))
        return None
    steps.append(Step("type", "ok", detail={
        "type": source_type.type_id, "origin": "built_in_reader",
    }))
    return source_type


def _validate_settings(
    source_type: SupportedSourceType,
    settings: dict[str, Any],
    steps: list[Step],
) -> bool:
    secret_like = sorted(key for key in settings if looks_secret_key(str(key)))
    if secret_like:
        steps.append(Step(
            "settings", "failed",
            detail={"secret_like_settings": secret_like},
            cause=(
                "settings must not carry secrets; "
                f"{', '.join(secret_like)} look like credentials"
            ),
            fix_hint=(
                "store the value in an environment variable and pass its NAME "
                "with --credential NAME; only the name is saved"
            ),
        ))
        return False
    schema = dict(source_type.settings_schema)
    for key, spec in (schema.get("properties") or {}).items():
        if key not in settings and "default" in spec:
            settings[key] = copy.deepcopy(spec["default"])
    errors = sorted(
        Draft202012Validator(schema).iter_errors(settings),
        key=lambda err: list(err.absolute_path),
    )
    if errors:
        problems = [
            f"{'/'.join(str(p) for p in err.absolute_path) or '<settings>'}: "
            f"{err.message}"
            for err in errors
        ]
        steps.append(Step(
            "settings", "failed",
            detail={"problems": problems},
            cause="settings do not match what this source type needs",
            fix_hint=(
                "run `okg deployment source-types` to see the settings this "
                "type accepts, then pass --setting name=value"
            ),
        ))
        return False
    return True


def _params_for(
    source_type: SupportedSourceType,
    name: str,
    settings: Mapping[str, Any],
    deployment_dir: Path,
) -> dict[str, Any]:
    """Translate user settings into the reader's constructor parameters."""
    params: dict[str, Any] = dict(source_type.fixed_params)
    if source_type.type_id == "local-folder":
        params.update({
            "repo": name,
            "repo_root": str(Path(str(settings["root"])).expanduser()),
            "extractors_dir": str(deployment_dir),
            "include_globs": list(settings.get("include") or ()),
            "exclude_globs": list(settings.get("exclude") or ()),
            # A local folder is often not a git checkout. Where git does
            # see it, doc-corpus still reads only files git tracks.
            "non_git_walk": True,
        })
        return params
    if source_type.type_id == "github":
        params.update({
            "repo": str(settings["repository"]),
            "include_discussion": bool(settings.get("include_discussion", False)),
        })
        return params
    params.update(dict(settings))
    return params


def _check_reader(plan: AddSourcePlan, steps: list[Step]) -> bool:
    from okg.substrate.deployment_lint import source_activation_probe_findings

    findings = source_activation_probe_findings(
        plan.name,
        plan.entry,
        deployment_dir=plan.deployment_dir,
        registry_path=plan.registry_path,
    )
    if findings:
        steps.append(Step(
            "reader", "failed",
            detail={"findings": findings},
            cause="the installed reader for this type is not runnable here",
            fix_hint=str(findings[0].get("next_action") or "reinstall okg"),
        ))
        return False
    assert plan.source_type is not None
    if plan.source_type.type_id == "local-folder":
        root = Path(plan.params["repo_root"])
        if not root.is_dir():
            steps.append(Step(
                "reader", "failed",
                cause=f"folder {root} does not exist or is not a directory",
                fix_hint="pass --setting root=<absolute folder path>",
            ))
            return False
    steps.append(Step("reader", "ok", detail={
        "module": plan.entry["module"], "class": plan.entry["class"],
    }))
    return True


def _check_access(
    source_type: SupportedSourceType,
    entry: Mapping[str, Any],
    env: Mapping[str, str],
    run: Runner,
    steps: list[Step],
    *,
    credential_refs: Sequence[str] = (),
) -> bool:
    mode = source_type.access_mode
    if credential_refs and mode in ("none", "gh_cli"):
        # #2480: the reader never reads a credential variable in these
        # modes, so a --credential NAME would be saved and then ignored.
        how = (
            "reads without a credential" if mode == "none" else
            "reads through your existing `gh auth login` session"
        )
        steps.append(Step(
            "access", "failed",
            detail={
                "code": "credential_not_used",
                "mode": mode,
                "credential_refs": list(credential_refs),
            },
            cause=(
                f"this source type {how}, so --credential "
                f"{', '.join(credential_refs)} would be ignored"
            ),
            fix_hint=(
                "drop --credential and re-run"
                if mode == "none" else
                "drop --credential; run `gh auth login` if `gh auth status` "
                "fails, then re-run"
            ),
        ))
        return False
    if mode == "none":
        steps.append(Step("access", "ok", detail={"mode": "none"}))
        return True
    if mode == "env":
        refs = [str(ref) for ref in (entry.get("credential_refs") or ())]
        missing = [ref for ref in refs if not env.get(ref)]
        if not refs or missing:
            steps.append(Step(
                "access", "failed",
                detail={"credential_refs": refs, "missing": missing or refs},
                cause=(
                    "a credential is required and is not available: "
                    + ", ".join(missing or ["no credential reference given"])
                ),
                fix_hint=(
                    "export the variable in your shell and pass its NAME with "
                    "--credential NAME; the value is never saved"
                ),
            ))
            return False
        steps.append(Step("access", "ok", detail={
            "mode": "env", "credential_refs": refs,
        }))
        return True
    if mode == "gh_cli":
        search_path = env.get("PATH") or ""
        gh = shutil.which("gh", path=search_path) if search_path else None
        if gh is None:
            steps.append(Step(
                "access", "failed",
                cause="the GitHub CLI (`gh`) is not installed",
                fix_hint="install GitHub CLI, run `gh auth login`, then retry",
            ))
            return False
        try:
            proc = run(
                [gh, "auth", "status"], capture_output=True, text=True,
                timeout=60, env=dict(env), check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            steps.append(Step(
                "access", "failed",
                cause=f"`gh auth status` could not run: {exc}",
                fix_hint="run `gh auth login`, then retry",
            ))
            return False
        if proc.returncode != 0:
            steps.append(Step(
                "access", "failed",
                cause="no authorized GitHub CLI session",
                fix_hint="run `gh auth login`, then retry",
                detail={"stderr": redact_text((proc.stderr or "")[-300:])},
            ))
            return False
        steps.append(Step("access", "ok", detail={
            "mode": "gh_cli", "reused": "existing `gh auth login` session",
        }))
        return True
    steps.append(Step(
        "access", "failed", cause=f"unknown access mode {mode!r}",
    ))
    return False


def _decide_modules(
    source_type: SupportedSourceType,
    deployment_dir: Path,
    plan: AddSourcePlan,
    *,
    registration_path: Optional[str | Path] = None,
) -> bool:
    manifest = _load_yaml(deployment_dir / "deployment.yaml")
    present = [str(m) for m in (manifest.get("modules") or ())]
    missing = [m for m in source_type.ontology_modules if m not in present]
    if missing:
        plan.decisions.append({
            "kind": "ontology_modules",
            "add": missing,
            "why": (
                "the reader's records need these graph modules; OKG adds them "
                "to deployment.yaml and applies the catalog"
            ),
        })
    detail: dict[str, Any] = {
        "modules_present": present, "modules_to_add": missing,
    }
    if registration_path is not None:
        planned = _plan_connector_schemas(
            Path(registration_path), deployment_dir, manifest,
        )
        if planned.get("cause"):
            plan.steps.append(Step(
                "schema", "failed",
                detail={**detail, **planned.get("detail", {})},
                cause=str(planned["cause"]),
                fix_hint=str(planned["fix_hint"]),
            ))
            return False
        files = list(planned["files"])
        plan.connector_schemas = files
        to_copy = [f for f in files if f["action"] == "copy"]
        if to_copy:
            plan.decisions.append({
                "kind": "connector_schemas",
                "install": to_copy,
                "why": (
                    "the connector ships the graph schemas its records use; "
                    "OKG copies them into the deployment's schemas and "
                    "applies the catalog"
                ),
            })
        detail["connector_schemas"] = files
    plan.steps.append(Step("schema", "ok", detail=detail))
    return True


def _connector_schema_dir(
    deployment_dir: Path, manifest: Mapping[str, Any],
) -> Optional[Path]:
    """The deployment's `schema_dir`, resolved as the manifest loader does."""
    raw = manifest.get("schema_dir")
    if not isinstance(raw, str) or not raw.strip():
        return None
    value = raw.strip()
    for token in ("${deployment_dir}", "${manifest_dir}"):
        value = value.replace(token, str(deployment_dir))
    return deployment_dir / value


def _plan_connector_schemas(
    registration_path: Path,
    deployment_dir: Path,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Plan copying a connector's `schemas/` next to its registration (#2478).

    A packaged connector's setup writes `connector.json` beside a `schemas/`
    directory: node and edge types in `schemas/*.yaml`, narrowings in
    `schemas/bridges/*.yaml`. The catalog composes from the deployment's own
    `schema_dir`, so without these files every record the connector emits is
    a subtype violation and the publish is blocked. Each file is planned as
    `copy` (absent from the deployment) or `present` (same bytes already
    there). A deployment file with the same name and DIFFERENT content is
    refused, never overwritten: it is someone's schema.
    """
    source_root = registration_path.resolve().parent / "schemas"
    sources: list[tuple[Path, PurePath]] = []
    if source_root.is_dir():
        sources += [
            (path, PurePath(path.name))
            for path in sorted(source_root.glob("*.yaml")) if path.is_file()
        ]
        bridges = source_root / "bridges"
        if bridges.is_dir():
            sources += [
                (path, PurePath("bridges", path.name))
                for path in sorted(bridges.glob("*.yaml")) if path.is_file()
            ]
    if not sources:
        return {"files": []}
    schema_dir = _connector_schema_dir(deployment_dir, manifest)
    if schema_dir is None:
        return {
            "cause": (
                "the connector ships schemas but deployment.yaml names no "
                "`schema_dir` to install them into"
            ),
            "fix_hint": "add `schema_dir: schemas` to deployment.yaml and re-run",
            "detail": {"connector_schemas": [str(p) for p, _ in sources]},
        }
    links = [str(path) for path, _ in sources if path.is_symlink()]
    if links:
        return {
            "cause": (
                "the connector's schema files must be regular files, not "
                f"symbolic links: {', '.join(links)}"
            ),
            "fix_hint": "replace each link with the file it points to, then re-run",
            "detail": {"symlinked_schema_files": links},
        }
    files: list[dict[str, str]] = []
    conflicts: list[str] = []
    for source, rel in sources:
        target = schema_dir / rel
        if target.exists():
            if target.is_file() and target.read_bytes() == source.read_bytes():
                action = "present"
            else:
                conflicts.append(str(target))
                continue
        else:
            action = "copy"
        files.append({"from": str(source), "to": str(target), "action": action})
    if conflicts:
        return {
            "cause": (
                "the deployment already has a different schema file under "
                f"the same name: {', '.join(conflicts)}"
            ),
            "fix_hint": (
                "compare each file with the connector's copy; rename or "
                "remove the deployment's file if the connector's should "
                "replace it, then re-run"
            ),
            "detail": {"conflicting_schema_files": conflicts},
        }
    return {"files": files}


def _decide_posture(
    source_type: SupportedSourceType,
    deployment_dir: Path,
    plan: AddSourcePlan,
) -> None:
    manifest = _load_yaml(deployment_dir / "deployment.yaml")
    posture = ((manifest.get("nomos") or {}).get("deployment_posture") or {})
    if not isinstance(posture, Mapping):
        posture = {}
    policy = source_type.source_policy
    changes: list[dict[str, Any]] = []
    for key, value in (
        ("allowed_source_sensitivities", policy["sensitivity"]),
        ("allowed_data_classifications", policy["data_classification"]),
    ):
        allowed = posture.get(key)
        if isinstance(allowed, list) and value not in allowed:
            changes.append({"path": f"nomos.deployment_posture.{key}",
                            "add": value})
    if policy.get("live_call_allowed") and posture.get("live_call_default") == "deny":
        changes.append({
            "path": "nomos.deployment_posture.live_call_default",
            "from": "deny", "to": "review",
        })
    if changes:
        plan.decisions.append({
            "kind": "privacy_posture",
            "changes": changes,
            "why": (
                "this source reads data of a kind the deployment's privacy "
                "posture does not yet allow; confirming records that decision"
            ),
        })
    plan.steps.append(Step("policy", "ok", detail={
        "source_policy": dict(policy), "posture_changes": changes,
    }))


def _preview(
    source_type: SupportedSourceType,
    plan: AddSourcePlan,
    limit: int,
    steps: list[Step],
) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    started = time.monotonic()
    scope: dict[str, Any] = {}
    params = dict(plan.params)
    preview_dir: Optional[Path] = None
    if source_type.type_id == "local-folder":
        matched, truncated = _scan_folder(
            Path(params["repo_root"]),
            include=params.get("include_globs") or (),
            exclude=params.get("exclude_globs") or (),
            limit=PREVIEW_SCAN_LIMIT,
        )
        scope = {
            "root": params["repo_root"],
            "files_matched": len(matched),
            "files_matched_is_lower_bound": truncated,
            "files_previewed": min(len(matched), limit),
        }
        if not matched:
            steps.append(Step(
                "preview", "failed",
                detail={"scope": scope},
                cause="no files under the folder match the include patterns",
                fix_hint="check the folder path or pass --setting include=...",
            ))
            return
        params["include_globs"] = list(matched[:limit])
        # Plan the file readers this folder needs (#2974) and preview with
        # the extractors.yaml the save step will leave in place.
        # A scan that stopped at PREVIEW_SCAN_LIMIT has not seen every file,
        # so plan every packaged reader rather than guess from the part seen.
        plan.reader_formats = (
            list(_packaged_readers()["parser"]) if truncated
            else _folder_formats(matched)
        )
        readers = _plan_file_readers(
            Path(params["extractors_dir"]) / "extractors.yaml", plan.reader_formats,
        )
        plan.file_readers = readers
        if readers is not None:
            plan.decisions.append(_file_readers_decision(readers))
            if readers.get("text") is not None:
                preview_dir = Path(tempfile.mkdtemp(prefix="okg-add-source-"))
                (preview_dir / "extractors.yaml").write_text(
                    readers["text"], encoding="utf-8",
                )
                params["extractors_dir"] = str(preview_dir)
    elif source_type.type_id == "github":
        scope = {
            "repository": params["repo"],
            "items": "all issues and pull requests",
            "preview_read": (
                "the whole repository is read once for this preview and "
                "again by the import"
            ),
        }
    try:
        adapter = _construct_adapter(plan.entry, params)
        run = adapter.run("preview", mode="scope_complete")
        records: list[dict[str, Any]] = []
        relationships: list[dict[str, Any]] = []
        total_nodes = 0
        total_edges = 0
        body_queries: list[str] = []
        for fact in run.facts:
            if isinstance(fact, NodeFact):
                total_nodes += 1
                if len(records) < limit:
                    records.append(_describe_node(fact))
                for word in _body_words(fact):
                    if (
                        word not in body_queries
                        and len(body_queries) < _BODY_WORD_LIMIT
                    ):
                        body_queries.append(word)
            elif isinstance(fact, EdgeFact):
                total_edges += 1
                if len(relationships) < limit:
                    relationships.append({
                        "type": fact.edge_type, "from": fact.src, "to": fact.dst,
                    })
            if (
                source_type.type_id == "local-folder"
                and len(records) >= limit
                and len(relationships) >= limit
            ):
                break
    except Exception as exc:  # noqa: BLE001 - the cause is the product here
        steps.append(Step(
            "preview", "failed",
            detail={"scope": scope},
            cause=f"the reader could not read this source: {redact_text(str(exc))}",
            fix_hint=(
                "correct the setting or access problem named above and re-run"
            ),
        ))
        return
    finally:
        if preview_dir is not None:
            shutil.rmtree(preview_dir, ignore_errors=True)
    plan.body_queries = body_queries
    scope["records_read_in_preview"] = total_nodes
    scope["relationships_read_in_preview"] = total_edges
    plan.preview = {
        "scope": scope,
        "records": records,
        "relationships": relationships,
        "bounded": source_type.type_id == "local-folder",
        "elapsed_s": round(time.monotonic() - started, 3),
    }
    steps.append(Step("preview", "ok", detail={
        "records_shown": len(records), "relationships_shown": len(relationships),
    }))


def _construct_adapter(entry: Mapping[str, Any], params: Mapping[str, Any]) -> Any:
    import importlib
    import inspect

    module = importlib.import_module(str(entry["module"]))
    cls = getattr(module, str(entry["class"]))
    kwargs = dict(params)
    try:
        named = set(inspect.signature(cls).parameters)
    except (TypeError, ValueError):
        named = set(kwargs)
    kwargs = {key: value for key, value in kwargs.items() if key in named}
    if "source_name" in named:
        kwargs["source_name"] = entry["ownership_id"]
    return cls(**kwargs)


#: Record attributes shown as-is (after key-based secret redaction). Their
#: values are labels, paths and links; the value-shape redaction in
#: `redact_text` would mangle any 24+ character file name into `<redacted>`.
_DISPLAY_KEYS = frozenset({
    "title", "name", "path", "url", "html_url", "label", "document_id",
    "heading", "repo", "doc_id", "number",
})


def _display_attrs(attrs: Mapping[str, Any]) -> dict[str, Any]:
    """Redact by key name; keep plain display fields readable."""
    out: dict[str, Any] = {}
    for key, value in attrs.items():
        name = str(key)
        if looks_secret_key(name) or re.search(r"(?i)(pass|authorization)", name):
            out[name] = "<redacted>"
        elif name in _DISPLAY_KEYS and isinstance(value, (str, int)):
            out[name] = redact_dsn(str(value)) if isinstance(value, str) else value
        elif isinstance(value, Mapping):
            out[name] = redact_mapping(dict(value))
        else:
            out[name] = redact_text(value) if isinstance(value, str) else value
    return out


def _describe_node(fact: Any) -> dict[str, Any]:
    attrs = _display_attrs(dict(getattr(fact, "attrs", {}) or {}))
    label = None
    for key in ("title", "name", "path", "url", "label", "document_id"):
        value = attrs.get(key)
        if isinstance(value, str) and value:
            label = value
            break
    origin = {}
    for key in ("path", "url", "html_url", "repo"):
        if key in attrs:
            origin[key] = attrs[key]
    text = ""
    for key in ("title", "heading", "name", "body", "text", "excerpt"):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip():
            text = value.strip()
            break
    return {
        "subtype": fact.subtype,
        "node_id": fact.node_id,
        "label": label or fact.node_id,
        "origin": origin,
        "source_record_id": getattr(fact, "source_record_id", None),
        "search_text": " ".join(text.split()[:6]),
    }


# The sample step searches single body words, one search each, at most this
# many in total, and at most `_BODY_WORDS_PER_RECORD` from one record.
_BODY_WORD_LIMIT = 5
_BODY_WORDS_PER_RECORD = 3
_BODY_KEYS = ("body", "text")
_LABEL_KEYS = ("title", "heading", "heading_path", "name", "path", "label", "url")
_WORD = re.compile(r"[^\W\d_]{4,}")
# Text search keeps a URL, an e-mail address, a path or a host name as one
# term, so a word taken out of one is not searchable on its own. Plain
# substrings plus a dotted-name pattern, not one regex: this is a search
# tokenizer, not DSN handling (tests/substrate/test_no_private_dsn_handling.py).
_COMPOUND_MARKS = ("://", "@", "/", "\\")
_DOTTED_NAME = re.compile(r"\w\.\w")


def _is_compound_token(token: str) -> bool:
    return any(mark in token for mark in _COMPOUND_MARKS) or bool(
        _DOTTED_NAME.search(token)
    )


def _body_words(fact: Any) -> list[str]:
    """Up to three words from a record's body text that are not in its label.

    Longest words first (rare words, never short stop words), each a plain
    word outside any URL, e-mail address or path, lower-cased with `lower()`
    (`casefold()` would turn German ß into ss, which the index does not
    hold). A record whose only text is its file name or title yields none,
    so a source that imports names without contents has no body words.
    """
    attrs = dict(getattr(fact, "attrs", {}) or {})
    label_words: set[str] = set()
    for key in _LABEL_KEYS:
        value = attrs.get(key)
        if isinstance(value, str):
            label_words.update(w.lower() for w in _WORD.findall(value))
    for key in _BODY_KEYS:
        value = attrs.get(key)
        if not isinstance(value, str) or looks_secret_key(key):
            continue
        words: list[str] = []
        for token in value.split():
            token = token.strip("\"'()[]{}<>.,;:!?*`#")
            if _is_compound_token(token):
                continue
            for word in _WORD.findall(token):
                lowered = word.lower()
                if lowered not in label_words and lowered not in words:
                    words.append(lowered)
        if words:
            ranked = sorted(words, key=lambda w: (-len(w), words.index(w)))
            return ranked[:_BODY_WORDS_PER_RECORD]
    return []


def _scan_folder(
    root: Path,
    *,
    include: Sequence[str],
    exclude: Sequence[str],
    limit: int = PREVIEW_SCAN_LIMIT,
) -> tuple[list[str], bool]:
    """Bounded walk returning repo-relative paths matching the globs."""
    matched: list[str] = []
    seen = 0
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = PurePath(os.path.relpath(dirpath, root))
        # Do not descend into folders the exclude patterns cover (`.git`,
        # `node_modules`): a file inside them could never match.
        dirnames[:] = sorted(
            d for d in dirnames
            if not any(
                _glob_match(str(_rel_join(rel_dir, d, "x")), g) for g in exclude
            )
        )
        for filename in sorted(filenames):
            seen += 1
            if seen > limit:
                return matched, True
            rel = str(_rel_join(rel_dir, filename))
            if include and not any(_glob_match(rel, g) for g in include):
                continue
            if any(_glob_match(rel, g) for g in exclude):
                continue
            matched.append(rel)
    return matched, False


def _rel_join(rel_dir: PurePath, *parts: str) -> PurePath:
    return PurePath(*parts) if str(rel_dir) == "." else rel_dir.joinpath(*parts)


def _glob_match(rel: str, pattern: str) -> bool:
    """The reader's own matcher, so the preview scope equals the import."""
    from okg.substrate.library.sources.doc_corpus import _match_any

    return _match_any(PurePath(rel), [pattern])


# --------------------------------------------------------------------------
# Apply (the one confirmation)
# --------------------------------------------------------------------------


def apply_add_source(
    plan: AddSourcePlan,
    *,
    runner: Optional[Runner] = None,
    environ: Optional[Mapping[str, str]] = None,
    okg_argv_prefix: Optional[Sequence[str]] = None,
    dsn: Optional[str] = None,
    reconcile_policy: Optional[Callable[..., dict[str, Any]]] = None,
    admission_lookup: Optional[Callable[..., dict[str, Any]]] = None,
) -> AddSourceOutcome:
    """Save, validate, import, publish, and show one searchable sample.

    `admission_lookup(dsn=, deployment=, admission_ids=)` reports whether the
    admissions a failed attempt left staged have been published, and by
    which version (#2769); the default reads the deployment database.
    """
    if not plan.ok or plan.source_type is None:
        failed = plan.failed_step
        raise AddSourceError(
            "the plan has an unresolved problem; nothing was saved",
            data={"step": failed.name if failed else None,
                  "cause": failed.cause if failed else None},
            fix_hint=failed.fix_hint if failed else None,
        )
    run = runner or _default_runner
    env = dict(os.environ if environ is None else environ)
    if dsn:
        env["OKG_DSN"] = dsn
    prefix = list(okg_argv_prefix or running_okg_argv())
    ledger = _Ledger(plan)
    steps: list[Step] = []

    def fail(step: Step) -> AddSourceOutcome:
        steps.append(step)
        disabled = _set_enabled(plan, enabled=False)
        step.detail = {**step.detail, "definition_disabled": disabled}
        # Disabling rewrites `source_registry`, which is part of the
        # deployment manifest identity a publish is bound to. Leaving the
        # database claimed on the PRE-disable content is what made the retry
        # loop closed (#2213): the operator re-claimed the disabled registry,
        # the next attempt re-enabled it, and the publish drifted again.
        if disabled and step.name != "claim":
            reclaimed = _claim_identity(run, prefix, env, str(plan.deployment_dir))
            step.detail = {
                **step.detail, "identity_reclaimed": reclaimed.returncode == 0,
            }
            if reclaimed.returncode != 0:
                # Silence here leaves the operator in the exact #2213 stuck
                # state with nothing naming it: the database stays claimed on
                # the pre-disable registry, so the next publish drifts again.
                step.cause = (
                    f"{step.cause}; the deployment identity could NOT be "
                    "re-claimed after the definition was disabled, so a "
                    "publish will refuse on manifest drift until it is: "
                    + redact_text(
                        _error_message(_json_payload(reclaimed))
                        or _last_line(reclaimed.stderr)
                        or "the ownership claim was refused"
                    )
                )
        if not disabled:
            step.cause = (
                f"{step.cause}; the saved definition could NOT be disabled, "
                "check the registry file"
            )
        if schemas_copied:
            # The catalog may already have been applied with them, so they
            # stay; a re-run finds the same bytes and copies nothing.
            step.detail = {**step.detail, "schema_files_left_in_place": schemas_copied}
            step.cause = (
                f"{step.cause}; schema files left in place: "
                + ", ".join(schemas_copied)
            )
        if attempt_admission:
            # Any failure after the import: this attempt's admission is
            # either staged (the publish failed) or already published (a
            # later step failed). Either way a re-run binds to exactly these
            # admissions and recovers once the database says they are
            # published; nothing else counts.
            ledger.unpublished = list(dict.fromkeys(
                [*ledger.unpublished, attempt_admission],
            ))
        # Keep the version a publish of this attempt produced, so the failed
        # ledger still says where the records went.
        ledger.write(steps, status="failed", generation_id=attempt_generation)
        return AddSourceOutcome(plan=plan, status="failed", steps=steps)

    # Read the previous attempt BEFORE this attempt's first ledger write.
    previous = read_ledger(plan.deployment_dir, plan.name) or {}
    if previous.get("status") != "published":
        ledger.unpublished = [
            str(item) for item in previous.get("unpublished_admission_ids") or ()
        ]
    schemas_copied: list[str] = []
    attempt_admission: Optional[str] = None
    attempt_generation: Optional[str] = None
    try:
        changed = _save_definition(plan)
    except (OSError, ValueError, SubstrateError, subprocess.SubprocessError) as exc:
        return fail(Step(
            "save", "failed",
            cause=f"could not save the source definition: {redact_text(str(exc))}",
            fix_hint=(
                "check that the deployment directory is writable and, when it "
                "is a git checkout, that `git commit` works there"
            ),
        ))
    schemas_copied = list(changed.get("schemas_installed") or ())
    warning = changed.get("extractors_yaml_warning")
    steps.append(Step(
        "save", "ok", detail=changed,
        # Not a failure: the source is added, but some of its files will be
        # read by name only until the named entries are added by hand.
        fix_hint=(
            f"{warning['path']} has no reader for some of this folder's files "
            f"({warning['reason']}); add these entries to it, commit, and run "
            f"`okg ingest`: {json.dumps(warning['entries_to_add'])}"
        ) if warning else None,
    ))
    ledger.write(steps, status="running")

    dep = str(plan.deployment_dir)
    proc = _okg(run, prefix, ["deployment", "lint", dep, "--json"], env)
    if proc.returncode != 0:
        blocker = _lint_blocker(proc)
        return fail(Step(
            "validate", "failed",
            detail={**_json_tail(proc), "blocker": blocker},
            cause=_lint_cause(blocker),
            fix_hint=_lint_hint(proc),
        ))
    steps.append(Step("validate", "ok"))
    ledger.write(steps, status="running")

    # Apply the catalog when modules were added now, and on every attempt
    # until one run of this source has published: a retry after a failed
    # attempt must not skip the step just because deployment.yaml already
    # lists the module the failed attempt added.
    catalog_needed = (
        bool(changed.get("modules_added"))
        or bool(changed.get("schemas_installed"))
        or previous.get("status") != "published"
    )
    if catalog_needed:
        proc = _okg(run, prefix, [
            "catalog", "load", "--deployment", dep, "--apply", "--json",
        ], env)
        if proc.returncode != 0:
            # #2480: name the child's own refusal. A fixed "check OKG_DSN"
            # hint sent operators after the database connection when the
            # real cause was, e.g., a catalog-ownership refusal.
            payload = _json_payload(proc)
            error = payload.get("error")
            own_hint = (
                str(error.get("fix_hint") or "")
                if isinstance(error, Mapping) else ""
            )
            reason = redact_text(
                (str(error.get("message") or "") if isinstance(error, Mapping)
                 else str(error or ""))
                or _last_line(proc.stderr)
                or f"`okg catalog load` exited {proc.returncode} with no message"
            )
            return fail(Step(
                "catalog", "failed",
                detail=_json_tail(proc),
                cause=f"the graph catalog could not be applied: {reason}",
                fix_hint=(
                    f"{redact_text(own_hint)}; then re-run the same command"
                    if own_hint else
                    "fix the cause named above, then re-run the same command"
                ),
            ))
        steps.append(Step("catalog", "ok", detail={
            "modules_added": changed.get("modules_added") or [],
            "schemas_installed": changed.get("schemas_installed") or [],
        }))
    else:
        steps.append(Step("catalog", "skipped", detail={
            "reason": "no new modules and this source has already published",
        }))
    ledger.write(steps, status="running")

    # Saving the definition CHANGED what this deployment publishes: the
    # `source_registry` block is part of the manifest identity that the
    # publish path compares against the recorded catalog-ownership claim,
    # and a new ontology module changes the catalog the claim is bound to.
    # This flow owns both edits, so once the catalog is applied it re-claims
    # the identity it wrote. Without this the publish below always refused
    # with `drifted_blocks=['source_registry']`, and no hand claim could win
    # it back because the next attempt rewrote the registry again (#2213).
    # The claim runs AFTER the catalog step on purpose: claiming first would
    # record the pre-module catalog and then differ from what publishes.
    proc = _claim_identity(run, prefix, env, dep)
    if proc.returncode != 0:
        return fail(Step(
            "claim", "failed",
            detail=_json_tail(proc),
            cause=(
                "the saved definition changed what this deployment "
                "publishes and this checkout could not re-claim that "
                "identity: " + redact_text(
                    _error_message(_json_payload(proc))
                    or _last_line(proc.stderr)
                    or "the ownership claim was refused"
                )
            ),
            fix_hint=(
                "another writer owns this deployment database; run `okg "
                f"catalog ownership diagnose --deployment {dep} --json`, "
                "and when this checkout should own it re-run the claim with "
                "--force"
            ),
        ))
    steps.append(Step("claim", "ok", detail={"deployment": dep}))
    ledger.write(steps, status="running")

    # The access policy (Nomos) lists, by name, every source it lets import.
    # Install and provision write that list from the registry; saving a new
    # source changed the registry, so bring the policy up to date the same
    # way before importing, or the import is denied with
    # `nomos.default_deny` on every attempt (#2475).
    policy_step = _update_access_policy(plan, env, reconcile_policy)
    if policy_step.status == "failed":
        return fail(policy_step)
    steps.append(policy_step)
    ledger.write(steps, status="running")

    proc = _okg(run, prefix, [
        "ingest", "--deployment", dep, "--include", plan.name,
        "--no-predecessors", "--inline",
        "--mode", complete_scan_mode(plan.source_type.registration.source_class),
        "--json",
    ], env)
    report = _json_payload(proc)
    result = _source_result(report, plan.name)
    publish = _last_publish(report)
    if result is None or result.get("status") != "OK":
        cause = (
            str((result or {}).get("error") or "")
            or _error_message(report)
            or _last_line(proc.stderr)
            or "import did not complete"
        )
        return fail(Step(
            "import", "failed",
            detail={
                "source_result": _compact(result),
                "publish": _compact(publish),
                "registry": str(plan.registry_path),
            },
            cause=_admission_cause(cause, plan.registry_path),
            fix_hint=_import_fix_hint(cause, plan.registry_path),
        ))
    counts = {
        "records_added": int(result.get("nodes_appended") or 0),
        "relationships_added": int(result.get("edges_appended") or 0),
        "records_removed": int(result.get("retract_n") or 0),
        "relationships_removed": int(result.get("retract_e") or 0),
    }
    import_detail: dict[str, Any] = dict(counts)
    reason = str((result.get("source_health") or {}).get("reason") or "")
    if (
        plan.source_type is not None and plan.source_type.type_id == "manifest"
        and reason and not reason.startswith("files.matched=")
    ):
        # The manifest reader withheld a deletion (bad rows, a failed read,
        # or the mass-deletion guard): say why, with the way to allow it.
        import_detail["note"] = reason
    steps.append(Step("import", "ok", detail=import_detail))
    if any(counts.values()) and result.get("admission_id"):
        attempt_admission = str(result["admission_id"])
    outcome = classify_publish(publish)
    staged = list(ledger.unpublished)
    redriven: Optional[list[str]] = None
    binding_detail: Optional[dict[str, Any]] = None
    if outcome.kind == SKIPPED and outcome.reason == NO_SOURCE_WORK and staged:
        # #2769: a failed attempt's import is still staged. `okg ingest`
        # re-drives staged imports with a publish of its own BEFORE this
        # run's sources, then has no new work and reports a `no_source_work`
        # skip LAST. Accept an earlier publish of this run only when its
        # admission feedback names every admission the failed attempts of
        # THIS source left staged; another source's re-drive is no proof.
        earlier = _earlier_published(report, staged)
        if earlier is not None:
            publish = earlier
            outcome = classify_publish(publish)
            redriven = staged
    unchanged_generation: Optional[str] = None
    unchanged_reason = ""
    if outcome.kind == NO_CHANGE and outcome.generation_id:
        # The publisher kept the current version: the import matched it.
        unchanged_generation = outcome.generation_id
        unchanged_reason = "the import matched the current graph version"
    elif (
        outcome.kind == SKIPPED
        and outcome.reason == NO_SOURCE_WORK
        and previous.get("status") == "published"
        and previous.get("generation_id")
    ):
        # A re-run of a source that already published, with nothing new to
        # import (#2491): its last published version still holds its records.
        unchanged_generation = str(previous["generation_id"])
        unchanged_reason = "nothing new to import since this source last published"
    elif outcome.kind == SKIPPED and outcome.reason == NO_SOURCE_WORK and staged:
        # #2769: another run (a scheduled or hand-run `okg ingest`) may have
        # published the staged import already. Accept it only when the
        # database says those exact admissions were published, on the
        # default branch; records an older attempt published are no proof.
        binding_detail = _staged_admissions_published(
            plan, env, staged, admission_lookup,
        )
        if binding_detail.get("published"):
            unchanged_generation = str(binding_detail["generation_id"])
            unchanged_reason = (
                "an earlier run published the import a failed attempt of this "
                "source left staged"
            )
    if unchanged_generation is not None:
        generation_id = unchanged_generation
        attempt_generation = generation_id
        skip_detail: dict[str, Any] = {
            "no_change": True,
            "reason": unchanged_reason,
            "generation_id": generation_id,
        }
        if binding_detail is not None:
            skip_detail["published_admission_ids"] = staged
        steps.append(Step("publish", "skipped", detail=skip_detail))
        ledger.write(steps, status="running", generation_id=generation_id)
        sample, sample_step = _sample(run, prefix, env, plan, generation_id)
        steps.append(sample_step)
        if sample_step.status == "failed":
            return fail(sample_step)
        ledger.unpublished = []
        ledger.write(steps, status="published", generation_id=generation_id)
        return AddSourceOutcome(
            plan=plan, status="no_change", steps=steps,
            generation_id=generation_id, counts=counts, sample=sample,
        )
    if outcome.kind != PUBLISHED:
        reason = _publish_reason(publish) or _error_message(report) or (
            "no publish reported" if proc.returncode == 0 else _last_line(proc.stderr)
        )
        reason = redact_text(reason)
        if outcome.kind == BLOCKED and outcome.failed_checks:
            # Gate names are the publisher's own identifiers, never secrets;
            # the redactor would mask one that looks like `*_endpoint_*`.
            reason = f"blocked by {', '.join(outcome.failed_checks)}"
        if outcome.kind == BLOCKED and outcome.generation_id:
            # `okg generation list` shows only published versions; describe
            # shows a blocked one and its failed checks (#2494).
            fix_hint = outcome.fix_hint or (
                f"run `{describe_generation_command(outcome.generation_id, dep)}` "
                "to see the failed checks, fix them, then re-run the same command"
            )
            cause_version = f" (graph version {outcome.generation_id})"
        else:
            fix_hint = outcome.fix_hint or (
                "run `okg generation list --deployment <name> --json` to see "
                "the current graph version, fix the cause, then re-run the "
                "same command"
            )
            cause_version = ""
        publish_detail: dict[str, Any] = {"publish": _compact(publish)}
        if binding_detail is not None:
            # The staged admissions were looked up and are not (all)
            # published; say which, beside the publish failure.
            publish_detail["staged_admissions"] = binding_detail
        elif (
            outcome.kind == SKIPPED and outcome.reason == NO_SOURCE_WORK
            and not staged and previous.get("status") in ("failed", "running")
        ):
            # A failed or interrupted (`running`) attempt with no recorded
            # admission (a ledger written before #2769, a crash, or a failure
            # before the import): nothing binds
            # what is published to this source, so name the way out.
            fix_hint = (
                "this command cannot tell whether an earlier run published "
                "the import a failed attempt left staged; check with `okg "
                f"search --deployment {plan.deployment_name} --query <text> "
                "--json`, or start the source over as described under "
                "'Remove a source' in docs/use/sources/index.md and add it "
                "again"
            )
        return fail(Step(
            "publish", "failed",
            detail=publish_detail,
            cause=(
                "the import completed but the new graph version was not "
                f"published{cause_version}: {reason}"
            ),
            fix_hint=fix_hint,
        ))
    generation_id = str(publish.get("generation_id"))
    attempt_generation = generation_id
    publish_detail = {"generation_id": generation_id}
    if redriven is not None:
        publish_detail["redrive"] = {
            "label": publish.get("label"),
            "published_admission_ids": redriven,
            "reason": (
                "this run published the staged import of this source's "
                "failed attempt (these admissions); it had nothing new to "
                "import itself"
            ),
        }
    steps.append(Step("publish", "ok", detail=publish_detail))
    ledger.write(steps, status="running", generation_id=generation_id)

    sample, sample_step = _sample(run, prefix, env, plan, generation_id)
    steps.append(sample_step)
    if sample_step.status == "failed":
        return fail(sample_step)
    ledger.unpublished = []
    ledger.write(steps, status="published", generation_id=generation_id)
    return AddSourceOutcome(
        plan=plan, status="published", steps=steps,
        generation_id=generation_id, counts=counts, sample=sample,
    )


def _claim_identity(
    run: Runner,
    prefix: Sequence[str],
    env: Mapping[str, str],
    deployment_dir: str,
) -> "subprocess.CompletedProcess[str]":
    """Re-claim the manifest identity this flow just wrote.

    Not forced: a plain claim re-stamps the manifest fingerprint for the
    writer that already owns the database, and refuses when a DIFFERENT
    writer holds it -- which is a real conflict the operator must see, not
    something a guided flow may overwrite.
    """
    return _okg(run, prefix, [
        "catalog", "ownership", "claim", "--deployment", deployment_dir,
        "--json",
    ], env)


def _update_access_policy(
    plan: AddSourcePlan,
    env: Mapping[str, str],
    reconcile_policy: Optional[Callable[..., dict[str, Any]]],
) -> Step:
    """Bring the deployment's access policy up to date with the saved registry.

    Uses the same idempotent reconcile `okg deployment apply` runs for its
    `nomos_baseline` stage: it rebuilds the policy from `deployment.yaml` and
    the registry, activates it only when it differs from the active one, and
    reports whether the two now match. A deployment whose manifest turns the
    access policy off has nothing to update.

    The database is the one the `okg` subprocesses of this flow write to:
    the manifest's `postgres.dsn`, interpolated against the same environment
    they receive (where `--dsn` arrives as `OKG_DSN`). Resolving `--dsn`
    first here would let the policy land in a different database from the
    import whenever the manifest names a literal DSN.
    """
    from okg.substrate.nomos.runtime_enforcement import (
        manifest_nomos_enforcement_mode,
    )

    hint = (
        "fix the named cause, then re-run the same command; `okg provision "
        f"--deployment {plan.deployment_name} --include <an existing source> "
        "--no-predecessors` rebuilds the same policy"
    )
    try:
        manifest = _load_yaml(plan.deployment_dir / "deployment.yaml")
        mode = manifest_nomos_enforcement_mode(manifest)
    except (OSError, ValueError, yaml.YAMLError, SubstrateError) as exc:
        return Step(
            "policy", "failed",
            cause=(
                "could not read the access-policy setting in deployment.yaml: "
                + redact_text(str(exc))
            ),
            fix_hint=hint,
        )
    if mode == "off":
        return Step("policy", "skipped", detail={
            "reason": "this deployment does not enforce an access policy",
        })
    dsn = _subprocess_dsn(manifest, env)
    if dsn is None:
        return Step(
            "policy", "failed",
            cause=(
                "deployment.yaml names no database this flow can resolve "
                "(`postgres.dsn` is missing or uses an unset variable)"
            ),
            fix_hint=(
                "set `postgres.dsn` in deployment.yaml, or pass --dsn when "
                "it reads ${OKG_DSN}, then re-run the same command"
            ),
        )
    if reconcile_policy is None:
        from okg.substrate.nomos.baseline import reconcile_deployment_baseline

        reconcile_policy = reconcile_deployment_baseline
    try:
        result = reconcile_policy(
            deployment=plan.deployment_name,
            deployment_dir=plan.deployment_dir,
            dsn=dsn,
            apply=True,
        )
    except Exception as exc:  # noqa: BLE001 - every cause is reported, none hidden
        data = getattr(exc, "data", None)
        own_hint = getattr(exc, "fix_hint", None)
        return Step(
            "policy", "failed",
            detail=(
                {"error": redact_mapping(dict(data))}
                if isinstance(data, Mapping) and data else {}
            ),
            cause=(
                "the access policy could not be updated for the new source: "
                + redact_text(f"{type(exc).__name__}: {exc}")
            ),
            fix_hint=(
                f"{redact_text(own_hint)}; then re-run the same command"
                if own_hint else hint
            ),
        )
    if not result.get("converged"):
        return Step(
            "policy", "failed",
            detail={
                "desired": result.get("desired"),
                "active_after": result.get("active_after"),
            },
            cause=(
                "the access policy was rebuilt but the active policy still "
                "does not match it, so the import would be denied"
            ),
            fix_hint=(
                "if this deployment runs a policy activated by hand, add the "
                "new source to that policy and re-run the same command; "
                "otherwise inspect `okg doctor --deployment "
                f"{plan.deployment_name} --json` (the nomos_baseline stage)"
            ),
        )
    return Step("policy", "ok", detail={
        "mode": mode,
        "changed": bool(result.get("changed")),
        "policy_fingerprint": (result.get("active_after") or {}).get(
            "policy_fingerprint",
        ),
    })


def _subprocess_dsn(
    manifest: Mapping[str, Any], env: Mapping[str, str],
) -> Optional[str]:
    """The DSN an `okg ... --deployment <dir>` child of this flow resolves.

    Same rule as `cli.main._resolve_deployment_dsn_from_dir` with no --dsn:
    the manifest's `postgres.dsn`, `${VAR}` expanded from the child's
    environment by `dsn.expand`. None when it is missing, a variable is
    unset or empty, or the placeholder syntax is unsupported.
    """
    raw = (manifest.get("postgres") or {}).get("dsn")
    if not isinstance(raw, str) or not raw.strip():
        return None
    from okg.substrate.db import dsn as dsn_module

    return dsn_module.expand(raw.strip(), env).value


def complete_scan_mode(source_class: str) -> str:
    """The `okg ingest --mode` value that reads this profile's whole scope."""
    return resolve_whole_scope_request(source_class, WHOLE_SCOPE_REQUEST)


def promote_registry_to_strict(
    raw: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Return the registry document as the save step writes it.

    Adding a source promotes the WHOLE document to `strict_v1`, so every
    existing entry meets the strict admission rules at the validate step.
    `okg deployment lint` grades those rules as blockers whatever the
    document's own contract says, so its verdict on a legacy registry is the
    verdict the validate step reaches after this promotion (issue #2150).
    Returns the document and whether the contract line was added.

    Promotion also removes each entry's legacy per-source `schedule` block
    (issue #2952). The legacy schema accepted it, nothing in the runtime ever
    read it -- a scheduled refresh is the entry's `sync.poll` block -- and
    `strict_v1` refuses it, so keeping it would fail every add-source on a
    registry `okg deployment bootstrap` drafted before the fix. The removal
    is reported by `_save_definition` (`schedule_removed`), not silent. A
    document that is already `strict_v1` is returned unchanged: there a
    `schedule` key is the author's error and the strict schema names it.
    """
    if raw.get("admission_contract") == STRICT_ADMISSION_CONTRACT:
        return dict(raw), False
    promoted = {"admission_contract": STRICT_ADMISSION_CONTRACT, **raw}
    sources = promoted.get("sources")
    if isinstance(sources, Mapping) and legacy_schedule_sources(promoted):
        promoted["sources"] = {
            name: (
                {key: value for key, value in body.items() if key != "schedule"}
                if isinstance(body, Mapping) else body
            )
            for name, body in sources.items()
        }
    return promoted, True


def legacy_schedule_sources(raw: Mapping[str, Any]) -> list[str]:
    """Names of the sources whose entry carries a per-source `schedule`."""
    sources = raw.get("sources")
    if not isinstance(sources, Mapping):
        return []
    return sorted(
        str(name) for name, body in sources.items()
        if isinstance(body, Mapping) and "schedule" in body
    )


def _save_definition(plan: AddSourcePlan) -> dict[str, Any]:
    from okg.substrate.mcp.source_lifecycle import (
        _atomic_write_registry,
        _registry_mutation_lock,
    )

    assert plan.source_type is not None
    changed: dict[str, Any] = {"registry": str(plan.registry_path)}
    # Schemas go first: a conflict found now refuses before the registry
    # or deployment.yaml is touched.
    schemas_installed = _install_connector_schemas(plan)
    schemas_installed += _install_generated_files(plan)
    if schemas_installed:
        changed["schemas_installed"] = schemas_installed
    with _registry_mutation_lock(plan.registry_path):
        current = _load_registry(plan.registry_path)
        raw, promoted = promote_registry_to_strict(current)
        if promoted:
            changed["admission_contract_added"] = True
            schedule_removed = legacy_schedule_sources(current)
            if schedule_removed:
                changed["schedule_removed"] = schedule_removed
        sources = raw.setdefault("sources", {})
        entry = copy.deepcopy(plan.entry)
        entry["enabled_by_default"] = True
        sources[plan.name] = entry
        _atomic_write_registry(plan.registry_path, raw)

    manifest_path = plan.deployment_dir / "deployment.yaml"
    manifest = _load_yaml(manifest_path)
    manifest_changed = False
    modules_added: list[str] = []
    posture_changes: list[dict[str, Any]] = []
    for decision in plan.decisions:
        if decision["kind"] == "ontology_modules":
            modules = list(manifest.get("modules") or [])
            for module in decision["add"]:
                if module not in modules:
                    modules.append(module)
                    modules_added.append(module)
            manifest["modules"] = modules
            manifest_changed = True
        elif decision["kind"] == "privacy_posture":
            posture = manifest.setdefault("nomos", {}).setdefault(
                "deployment_posture", {},
            )
            for change in decision["changes"]:
                key = change["path"].rsplit(".", 1)[1]
                if "add" in change:
                    values = list(posture.get(key) or [])
                    if change["add"] not in values:
                        values.append(change["add"])
                    posture[key] = values
                else:
                    posture[key] = change["to"]
                posture_changes.append(change)
            manifest_changed = True
    if manifest_changed:
        manifest_path.write_text(
            yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8",
        )
    changed["modules_added"] = modules_added
    changed["posture_changes"] = posture_changes

    if plan.source_type.type_id == "local-folder":
        target = plan.deployment_dir / "extractors.yaml"
        # Re-plan against the file as it is now, for the formats the preview
        # found (the file may have changed since the dry run).
        readers = _plan_file_readers(target, plan.reader_formats)
        if readers is None:
            pass
        elif readers["action"] == "write":
            shutil.copyfile(EXTRACTORS_YAML, target)
            changed["extractors_yaml_written"] = str(target)
        elif readers["action"] == "add":
            # A deployment drafted by `okg deployment bootstrap` before
            # #2974 has an extractors.yaml with no parsers; without them
            # every file imports as a document holding only its name, and no
            # passage of its text is searchable. The file is rewritten, so
            # name any comment lines the rewrite drops.
            original = target.read_text(encoding="utf-8")
            target.write_text(readers["text"], encoding="utf-8")
            changed["extractors_yaml_updated"] = {
                "path": str(target),
                "added": readers["missing"],
                "rewritten": True,
                "comment_lines_dropped": _yaml_comment_line_count(original),
            }
        else:
            changed["extractors_yaml_warning"] = {
                "path": str(target),
                "missing": readers["missing"],
                "reason": readers["reason"],
                "entries_to_add": readers["entries"],
            }
    commit = _commit_configuration(
        plan, f"okg deployment add-source: save {plan.name}",
        extra_paths=[
            *(item["to"] for item in plan.connector_schemas),
            *(item["to"] for item in plan.generated_files),
        ],
    )
    if commit:
        changed["commit"] = commit
    return changed


def _install_connector_schemas(plan: AddSourcePlan) -> list[str]:
    """Copy the planned connector schema files; return the paths written.

    The preview's plan is re-checked against the files as they are now: a
    deployment file that appeared since with different content refuses
    (ValueError, reported by the save step), and one that appeared with the
    same bytes is left alone.
    """
    pending: list[tuple[Path, Path]] = []
    for item in plan.connector_schemas:
        source = Path(item["from"])
        target = Path(item["to"])
        if target.exists():
            if not (target.is_file() and target.read_bytes() == source.read_bytes()):
                raise ValueError(
                    f"{target} now differs from the connector's {source}; "
                    "nothing was overwritten"
                )
            continue
        pending.append((source, target))
    # Every file is checked before the first copy, so a refusal leaves the
    # deployment's schemas as they were.
    for source, target in pending:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
    return [str(target) for _, target in pending]


def _yaml_comment_line_count(text: str) -> int:
    """Lines of `text` holding a YAML comment, which a re-dump does not keep."""
    return sum(
        1 for line in text.splitlines()
        if line.lstrip().startswith("#") or re.search(r"\s#", line)
    )


def _packaged_readers() -> dict[str, dict[str, dict[str, Any]]]:
    """The packaged parser and chunker entries, keyed by format."""
    defaults = yaml.safe_load(EXTRACTORS_YAML.read_text(encoding="utf-8"))
    return {
        axis: {str(e["format"]): e for e in defaults.get(axis) or ()}
        for axis in ("parser", "chunker")
    }


def _folder_formats(paths: Sequence[str]) -> list[str]:
    """The packaged parser formats the folder's files map to, by extension."""
    from okg.substrate.library.sources.doc_corpus import DEFAULT_EXTENSION_TO_FORMAT

    found = {
        DEFAULT_EXTENSION_TO_FORMAT.get(PurePath(path).suffix.lower()) for path in paths
    }
    return [fmt for fmt in _packaged_readers()["parser"] if fmt in found]


def _plan_file_readers(
    path: Path, formats: Sequence[str],
) -> Optional[dict[str, Any]]:
    """The change extractors.yaml needs so this folder's `formats` are read.

    Returns None when nothing is missing, or when the file is not a mapping
    at all (the reader refuses such a file with its own error, as before).
    Otherwise a dict with `action`:

    - `write`: there is no file; the packaged defaults are written.
    - `add`: the file has the standard shape (singular `parser` and
      `chunker` lists whose entries all name a `format`); the missing
      entries are appended and the file is rewritten as `text`.
    - `warn`: the file has another shape the reader also accepts (plural
      keys, bare chunker lists, ...); it is left untouched and `entries`
      says what to add by hand.

    Only formats the folder holds are considered, so a format an operator
    removed on purpose comes back only when a folder with such files is
    added. A chunker is added only when no existing chunker handles the
    format: a `default` chunker, or a bare chunker list, handles all.
    """
    packaged = _packaged_readers()
    if not path.is_file():
        return {
            "action": "write", "path": str(path),
            "missing": {axis: list(packaged[axis]) for axis in packaged},
            "text": EXTRACTORS_YAML.read_text(encoding="utf-8"),
        }
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if document is None:
        document = {}
    if not isinstance(document, Mapping):
        return None

    def entries(axis: str) -> list[Any]:
        for key in (axis, f"{axis}s"):
            value = document.get(key)
            if isinstance(value, list):
                return value
        return []

    def fmt(entry: Any) -> Optional[str]:
        value = entry.get("format") if isinstance(entry, Mapping) else None
        return value.strip() if isinstance(value, str) and value.strip() else None

    parsers = {fmt(e) for e in entries("parser")} - {None}
    chunker_entries = entries("chunker")
    chunkers = {fmt(e) for e in chunker_entries} - {None}
    bare_chunker = any(
        isinstance(e, Mapping) and "format" not in e for e in chunker_entries
    )
    missing_parsers = [f for f in formats if f not in parsers]
    missing_chunkers: list[str] = []
    if not (bare_chunker or "default" in chunkers):
        for f in formats:
            if f in chunkers:
                continue
            chosen = f if f in packaged["chunker"] else "default"
            if chosen not in missing_chunkers:
                missing_chunkers.append(chosen)
    if not missing_parsers and not missing_chunkers:
        return None
    missing = {"parser": missing_parsers, "chunker": missing_chunkers}
    added = {
        axis: [copy.deepcopy(packaged[axis][f]) for f in missing[axis]]
        for axis in ("parser", "chunker")
    }
    # Only the section being extended must have the standard shape.
    reasons: list[str] = []
    for axis in ("parser", "chunker"):
        if not added[axis]:
            continue
        value = document.get(axis)
        if f"{axis}s" in document:
            reasons.append(f"uses `{axis}s:`")
        elif value is not None and not isinstance(value, list):
            reasons.append(f"`{axis}:` is not a list")
        elif any(fmt(e) is None for e in value or ()):
            reasons.append(f"a `{axis}:` entry names no format")
    if reasons:
        return {
            "action": "warn", "path": str(path), "missing": missing,
            "entries": added, "reason": "; ".join(reasons),
        }
    merged = dict(document)
    for axis in ("parser", "chunker"):
        if added[axis]:
            merged[axis] = list(document.get(axis) or []) + added[axis]
    return {
        "action": "add", "path": str(path), "missing": missing,
        "text": yaml.safe_dump(merged, sort_keys=False),
    }


def _file_readers_decision(readers: Mapping[str, Any]) -> dict[str, Any]:
    """The dry-run line for the planned extractors.yaml change."""
    missing = readers["missing"]
    changes = [
        f"{axis} {fmt}" for axis in ("parser", "chunker") for fmt in missing[axis]
    ]
    if readers["action"] == "write":
        why = f"writes the packaged file readers to {readers['path']}"
    elif readers["action"] == "add":
        why = (
            f"adds the file readers this folder needs to {readers['path']} "
            "(the file is rewritten; its comments are not kept)"
        )
    else:
        why = (
            f"WARNING: {readers['path']} has no reader for some of this "
            f"folder's files and is left unchanged ({readers['reason']}); those "
            "files import with their name only and no searchable text until "
            "you add these entries to it by hand"
        )
        changes = [
            f"{axis}: {json.dumps(e)}"
            for axis in ("parser", "chunker") for e in readers["entries"][axis]
        ]
    return {"kind": "file_readers", "why": why, "changes": changes}


def _commit_configuration(
    plan: AddSourcePlan,
    message: str,
    *,
    extra_paths: Sequence[str | Path] = (),
) -> Optional[str]:
    """Commit the configuration the flow wrote when the deployment is tracked.

    Publication reads the committed deployment tree and refuses uncommitted
    inputs, so saving a definition in a git-tracked deployment means
    committing it. Only the files this flow writes are added; the user's
    other changes are left alone (and reported by the publish step if they
    block it). The step ledger under `.okg/source-add/` is git-ignored.
    """
    dep = plan.deployment_dir
    try:
        top = subprocess.run(
            ["git", "-C", str(dep), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, check=False, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if top.returncode != 0:
        return None
    ledger_dir = dep / LEDGER_DIRNAME
    ledger_dir.mkdir(parents=True, exist_ok=True)
    ignore = ledger_dir / ".gitignore"
    if not ignore.is_file():
        ignore.write_text("*\n", encoding="utf-8")
    paths = [
        str(path.relative_to(dep))
        for path in (
            plan.registry_path,
            dep / "deployment.yaml",
            dep / "extractors.yaml",
        )
        if path.is_file()
    ]
    # Installed connector schemas are publish inputs like the files above
    # (#2478); one outside the deployment directory is not this repo's.
    # Compared resolved, so a `..` or a symlinked parent cannot pass.
    dep_resolved = dep.resolve()
    for extra in extra_paths:
        target = Path(extra).resolve()
        if target.is_file() and target.is_relative_to(dep_resolved):
            paths.append(str(target.relative_to(dep_resolved)))
    identity: list[str] = []
    for key, fallback in (
        ("user.name", "okg deployment add-source"),
        ("user.email", "okg-add-source@localhost"),
    ):
        probe = subprocess.run(
            ["git", "-C", str(dep), "config", "--get", key],
            capture_output=True, text=True, check=False, timeout=10,
        )
        if probe.returncode != 0 or not probe.stdout.strip():
            identity += ["-c", f"{key}={fallback}"]
    subprocess.run(
        ["git", "-C", str(dep), "add", "--", *paths],
        capture_output=True, text=True, check=True, timeout=30,
    )
    staged = subprocess.run(
        ["git", "-C", str(dep), "diff", "--cached", "--quiet", "--", *paths],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if staged.returncode == 0:
        return None  # nothing new to record
    subprocess.run(
        ["git", "-C", str(dep), *identity, "commit", "-q", "-m", message,
         "--only", "--", *paths],
        capture_output=True, text=True, check=True, timeout=30,
    )
    head = subprocess.run(
        ["git", "-C", str(dep), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True, timeout=10,
    )
    return head.stdout.strip()


def _set_enabled(plan: AddSourcePlan, *, enabled: bool) -> bool:
    """Flip the saved entry's `enabled_by_default`; False when that failed."""
    from okg.substrate.mcp.source_lifecycle import (
        _atomic_write_registry,
        _registry_mutation_lock,
    )

    try:
        with _registry_mutation_lock(plan.registry_path):
            raw = _load_registry(plan.registry_path)
            entry = (raw.get("sources") or {}).get(plan.name)
            if isinstance(entry, dict):
                entry["enabled_by_default"] = bool(enabled)
                _atomic_write_registry(plan.registry_path, raw)
        _commit_configuration(
            plan,
            f"okg deployment add-source: {'enable' if enabled else 'disable'} "
            f"{plan.name} after a failed step",
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return False
    return True


def _sample(
    run: Runner,
    prefix: Sequence[str],
    env: Mapping[str, str],
    plan: AddSourcePlan,
    generation_id: str,
) -> tuple[Optional[dict[str, Any]], Step]:
    # Search single words from the records' body text first, one search
    # each: a source that imported file names but not file contents shows up
    # here, at add time (#2974). When every body word misses (or there is no
    # body text), the titles and names are searched before the step fails,
    # and the result names the basis that passed and the words that missed.
    label_queries: list[str] = []
    for record in (plan.preview or {}).get("records") or ():
        for text in (record.get("search_text"), record.get("label")):
            words = [
                w.strip("#*`:,.()[]") for w in str(text or "").replace("/", " ").split()
            ]
            words = [w for w in words if len(w) > 2]
            if words:
                query = " ".join(words[:4])
                if query not in label_queries:
                    label_queries.append(query)
                break
    label_queries = label_queries[:3]
    attempts = [(word, "body") for word in plan.body_queries] + [
        (query, "label") for query in label_queries
    ]
    queries = [query for query, _ in attempts]
    missed: list[str] = []
    search_env = _env_with_deployment_root(env, plan)
    last: Optional["subprocess.CompletedProcess[str]"] = None
    for query, basis in attempts:
        proc = _okg(run, prefix, [
            "search", "--deployment", plan.deployment_name, "--query", query,
            "--method", "lexical", "--limit", "10",
            "--generation-id", generation_id, "--json",
        ], search_env)
        last = proc
        payload = _json_payload(proc)
        # A hit from another source does not show that this one is searchable.
        hits = [
            h for h in (payload or {}).get("results") or ()
            if isinstance(h, Mapping)
            and _hit_source(h) == plan.entry["ownership_id"]
        ]
        if proc.returncode != 0 or not hits:
            if basis == "body":
                missed.append(query)
            continue
        hit = hits[0]
        attrs = _display_attrs(dict(hit.get("attrs") or {}))
        origin = {
            key: attrs[key]
            for key in ("path", "url", "html_url", "repo", "doc_id")
            if key in attrs
        }
        origin.update(_traced_origin(
            run, prefix, search_env, plan, str(hit.get("node_id")), generation_id,
        ))
        sample = {
            "query": query,
            "node_id": hit.get("node_id"),
            "subtype": hit.get("subtype"),
            "label": attrs.get("title") or attrs.get("name") or hit.get("node_id"),
            "source": _hit_source(hit),
            "origin": origin,
            "generation_id": generation_id,
        }
        detail: dict[str, Any] = {"query": query, "basis": basis}
        if missed:
            detail["body_words_missed"] = missed
        warning: Optional[str] = None
        scope = (plan.preview or {}).get("scope") or {}
        if basis == "label" and scope.get("files_matched_is_lower_bound"):
            # Only a title matched, and the preview did not see every file:
            # some files may have been imported with their names only.
            warning = (
                "only a title was found, not text from a file, and the preview "
                f"stopped after {PREVIEW_SCAN_LIMIT} files; check that a word "
                "from a file's text is found with `okg search --deployment "
                f"{plan.deployment_name} --query <word> --json`"
            )
            detail["warning"] = warning
        return sample, Step("sample", "ok", detail=detail, fix_hint=warning)
    return None, Step(
        "sample", "failed",
        detail={
            "queries": queries, "body_words_missed": missed,
            **(_json_tail(last) if last else {}),
        },
        cause=(
            "the published version does not return this source's records "
            f"for {', '.join(repr(q) for q in queries)}"
            + (
                " (neither words from the records' text nor their titles "
                "are searchable)" if missed else ""
            )
        ),
        fix_hint=(
            "run `okg search --deployment <name> --query <text> --json`; "
            "if nothing is found, re-run the same command"
        ),
    )


def _hit_source(hit: Mapping[str, Any]) -> Optional[str]:
    """The source a search result came from, as `okg search --json` names it."""
    provenance = hit.get("provenance")
    if isinstance(provenance, Mapping) and provenance.get("source"):
        return str(provenance["source"])
    source = hit.get("source")
    return str(source) if source else None


def _traced_origin(
    run: Runner,
    prefix: Sequence[str],
    env: Mapping[str, str],
    plan: AddSourcePlan,
    node_id: str,
    generation_id: str,
) -> dict[str, Any]:
    """The record's original source, read from the published version itself.

    Chronos-native deployments keep no separate fact log, so the published
    generation's own node row (source, source record id, raw attributes) is
    the authority for "where did this come from".
    """
    del run, prefix  # the origin is read in-process, not through a subprocess
    dsn = env.get("OKG_DSN")
    if not dsn:
        return {}
    from okg.substrate.db import connection as db_connection
    from okg.substrate.graph_store import ChronosGraphStore

    try:
        with db_connection.connect(
            dsn, autocommit=True, application_name="okg-add-source",
            deployment=plan.deployment_name,
        ) as conn:
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            graph = store.checkout_generation(generation_id)
            rows = graph.query(
                "SELECT source, source_record_id, attrs FROM okg.graph_nodes "
                "WHERE node_id = :node_id",
                {"node_id": node_id},
            )
    except Exception as exc:  # noqa: BLE001 - origin is informative; the publish stands
        return {"trace_error": redact_text(str(exc))}
    if not rows:
        return {}
    row = rows[0]
    origin: dict[str, Any] = {"source": row.get("source")}
    record = row.get("source_record_id")
    if isinstance(record, str):
        try:
            record = json.loads(record)
        except ValueError:
            # Not a JSON document: the raw identifier is shown as redacted
            # text below instead of being parsed.
            pass
    if record:
        origin["record"] = (
            _display_attrs(dict(record)) if isinstance(record, Mapping)
            else redact_text(str(record))
        )
    attrs = row.get("attrs") or {}
    if isinstance(attrs, str):
        try:
            attrs = json.loads(attrs)
        except ValueError:
            attrs = {}
    shown = _display_attrs(dict(attrs)) if isinstance(attrs, Mapping) else {}
    for key in ("path", "url", "html_url", "repo", "doc_id"):
        value = shown.get(key)
        if isinstance(value, str) and value:
            origin.setdefault(key, value)
    return origin


def _env_with_deployment_root(
    env: Mapping[str, str], plan: AddSourcePlan,
) -> dict[str, str]:
    """Make the deployment resolvable BY NAME for read-side commands."""
    from okg.substrate.deployments import find_deployment_dir

    out = dict(env)
    try:
        resolved = find_deployment_dir(plan.deployment_name)
    except Exception:  # noqa: BLE001 - resolution failure means "not by name"
        resolved = None
    if resolved is None or Path(resolved).resolve() != plan.deployment_dir.resolve():
        out["OKG_DEPLOYMENTS_DIR"] = str(plan.deployment_dir.parent)
    return out


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


class _Ledger:
    def __init__(self, plan: AddSourcePlan) -> None:
        self.path = plan.deployment_dir / LEDGER_DIRNAME / f"{plan.name}.json"
        self.plan = plan
        # Admissions a failed publish of this source left staged (#2769).
        self.unpublished: list[str] = []

    def write(
        self,
        steps: Sequence[Step],
        *,
        status: str,
        generation_id: Optional[str] = None,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        body = {
            "schema": "okg.deployment.add_source.ledger/v1",
            "name": self.plan.name,
            "type": self.plan.source_type.type_id if self.plan.source_type else None,
            "settings": redact_mapping(dict(self.plan.settings)),
            "status": status,
            "generation_id": generation_id,
            "unpublished_admission_ids": list(self.unpublished),
            "steps": [step.as_dict() for step in steps],
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.path.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")


def read_ledger(deployment_dir: Path, name: str) -> Optional[dict[str, Any]]:
    path = Path(deployment_dir) / LEDGER_DIRNAME / f"{name}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _default_runner(argv: Sequence[str], **kwargs: Any) -> "subprocess.CompletedProcess[str]":
    kwargs.setdefault("capture_output", True)
    kwargs.setdefault("text", True)
    kwargs.setdefault("check", False)
    return subprocess.run(list(argv), **kwargs)


def _okg(
    run: Runner,
    prefix: Sequence[str],
    argv: Sequence[str],
    env: Mapping[str, str],
) -> "subprocess.CompletedProcess[str]":
    try:
        return run(
            [*prefix, *argv], capture_output=True, text=True, check=False,
            env=dict(env), timeout=SUBPROCESS_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        partial = exc.stdout or ""
        if isinstance(partial, bytes):
            partial = partial.decode("utf-8", "replace")
        return subprocess.CompletedProcess(
            list(exc.cmd), 124, stdout=str(partial),
            stderr=f"timed out after {SUBPROCESS_TIMEOUT_SECONDS}s",
        )


def _json_payload(proc: "subprocess.CompletedProcess[str]") -> dict[str, Any]:
    for stream in (proc.stdout, proc.stderr):
        text = (stream or "").strip()
        if not text:
            continue
        for candidate in (text, text.splitlines()[-1]):
            try:
                payload = json.loads(candidate)
            except ValueError:
                continue
            if isinstance(payload, dict):
                return payload
    return {}


def _json_tail(proc: "subprocess.CompletedProcess[str]") -> dict[str, Any]:
    payload = _json_payload(proc)
    if payload:
        return {"output": redact_mapping(payload)}
    return {"stderr": redact_text((proc.stderr or "")[-600:])}


def _publish_reason(publish: Optional[Mapping[str, Any]]) -> str:
    if not publish:
        return ""
    for key in ("error", "reason", "blocked_by", "status"):
        value = publish.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, Mapping) and value.get("message"):
            return str(value["message"])
        if isinstance(value, list) and value:
            return ", ".join(str(v) for v in value)
    return ""


def _last_line(text: Optional[str]) -> str:
    lines = [line for line in (text or "").splitlines() if line.strip()]
    return lines[-1][-400:] if lines else ""


def _compact(value: Optional[Mapping[str, Any]]) -> Optional[dict[str, Any]]:
    """Keep the operator-relevant keys of a large report row."""
    if not value:
        return None
    keep = (
        "name", "status", "stage", "error", "code", "fix_hint", "reason",
        "generation_id", "nodes_appended", "edges_appended", "retract_n",
        "retract_e", "admission_status", "admission_reject_reason",
        "nodes_added", "edges_added", "blocked_by", "blocked_reason",
        "error_data",
    )
    return redact_mapping({key: value[key] for key in keep if key in value})


def _error_message(payload: Mapping[str, Any]) -> str:
    error = payload.get("error")
    if isinstance(error, Mapping):
        message = str(error.get("message") or "")
        hint = error.get("fix_hint")
        return f"{message} ({hint})" if hint else message
    return str(error or "")


#: A substrate diagnostic code that prefixes a refusal, e.g.
#: `source_class_profile_mismatch: source 'x' at /sources/x/source_class: ...`.
#: Matched only at the start of a cause and only as lower-case snake_case with
#: no digits, a shape no credential takes -- `redact_text` otherwise replaces
#: any 24-plus-character token with `<redacted>`, which used to hide the one
#: word naming what failed (issue #2126, finding F14).
_DIAGNOSTIC_CODE_RE = re.compile(r"^([a-z][a-z_]{2,63}):\s")

#: What `okg ingest` writes when admission refuses a source; the rest of the
#: line is the reject reason. A reason that is exactly one diagnostic code
#: (e.g. `producer_edge_triple_not_allowed`) is shown as-is (#3006); anything
#: else after the prefix is still redacted.
_ADMISSION_REJECTED_PREFIX = "source admission rejected: "
_BARE_CODE_RE = re.compile(r"[a-z][a-z_]{2,63}")


def _admission_cause(cause: str, registry_path: Path) -> str:
    """Redact the cause without eating its leading diagnostic code."""
    if cause.startswith(_ADMISSION_REJECTED_PREFIX):
        reason = cause[len(_ADMISSION_REJECTED_PREFIX):]
        if _BARE_CODE_RE.fullmatch(reason):
            return cause
        return _ADMISSION_REJECTED_PREFIX + redact_text(reason)
    match = _DIAGNOSTIC_CODE_RE.match(cause)
    if match is None:
        return redact_text(cause)
    code = match.group(1)
    rest = redact_text(cause[match.end():])
    # `_adapter_admission_error` writes `source 'x' at /sources/x/<field>`;
    # prefix the registry file so the operator knows which file to edit.
    rest = rest.replace(" at /sources/", f" at {registry_path}#/sources/", 1)
    return f"{code}: {rest}"


def _import_fix_hint(cause: str, registry_path: Path) -> str:
    """Point at the registry only when the cause actually names a field.

    Most import failures have nothing to do with the registry (a lock
    timeout, an unreachable database), and telling the operator to edit a
    field the message never named sends them to the wrong file.
    """
    if _DIAGNOSTIC_CODE_RE.match(cause) and " at /sources/" in cause:
        return (
            f"fix the field the cause names in {registry_path}, then "
            "re-run the same command"
        )
    return "fix the named cause, then re-run the same command"


def _lint_blocker(
    proc: "subprocess.CompletedProcess[str]",
) -> dict[str, Any]:
    """The first blocking lint finding, as code/pointer/observed/next_action."""
    payload = _json_payload(proc)
    findings = payload.get("findings") if isinstance(payload, Mapping) else None
    if not isinstance(findings, list):
        return {}
    for finding in findings:
        if not isinstance(finding, Mapping):
            continue
        if finding.get("severity") != "blocker":
            continue
        location = finding.get("location")
        location = location if isinstance(location, Mapping) else {}
        return {
            "code": str(finding.get("code") or ""),
            "path": str(location.get("path") or ""),
            "pointer": str(location.get("pointer") or ""),
            "observed": str(finding.get("observed") or ""),
            "next_action": str(finding.get("next_action") or ""),
        }
    return {}


def _lint_cause(blocker: Mapping[str, Any]) -> str:
    """Name the file and field the blocker located, not just that it failed."""
    code = str(blocker.get("code") or "")
    if not code:
        return "the deployment does not validate with this source"
    where = "".join(
        part for part in (
            str(blocker.get("path") or ""),
            f"#{blocker['pointer']}" if blocker.get("pointer") else "",
        ) if part
    )
    observed = str(blocker.get("observed") or "")
    parts = [code]
    if where:
        parts.append(f"at {where}")
    if observed:
        parts.append(f"-- {redact_text(observed)}")
    return " ".join(parts)


def _lint_hint(proc: "subprocess.CompletedProcess[str]") -> str:
    payload = _json_payload(proc)
    findings = payload.get("findings") if isinstance(payload, Mapping) else None
    if isinstance(findings, list):
        for finding in findings:
            if isinstance(finding, Mapping) and finding.get("severity") == "blocker":
                return str(finding.get("next_action") or finding.get("message"))
    return "run `okg deployment lint <deployment> --json` and fix the first blocker"


def _source_result(report: Mapping[str, Any], name: str) -> Optional[dict[str, Any]]:
    body = report.get("report") if isinstance(report.get("report"), Mapping) else report
    for row in body.get("results") or ():
        if isinstance(row, Mapping) and row.get("name") == name:
            return dict(row)
    return None


#: The branch `okg ingest` and add-source publish to.
PUBLISH_BRANCH = "default"


def _staged_admissions_published(
    plan: AddSourcePlan,
    env: Mapping[str, str],
    admission_ids: Sequence[str],
    lookup: Optional[Callable[..., dict[str, Any]]],
) -> dict[str, Any]:
    """Whether every staged admission was published on the publish branch."""
    try:
        manifest = _load_yaml(plan.deployment_dir / "deployment.yaml")
        dsn = _subprocess_dsn(manifest, env)
        if dsn is None:
            return {"published": False, "reason": "no resolvable database"}
        found = (lookup or admission_publication)(
            dsn=dsn, deployment=plan.deployment_name, admission_ids=admission_ids,
        )
    except Exception as exc:  # noqa: BLE001 - the lookup failing refuses, visibly
        return {
            "published": False,
            "reason": f"admission lookup failed: {redact_text(str(exc))}",
        }
    return found


def admission_publication(
    *, dsn: str, deployment: str, admission_ids: Sequence[str],
) -> dict[str, Any]:
    """Look up whether these admissions were published, and by which version.

    `published` is True only when every id is found for this deployment with
    status `published` and a `published` event on the default branch; the
    version is the one named by the newest such event.
    """
    from okg.substrate.db.connection import connect as db_connect

    ids = [str(item) for item in admission_ids]
    if not ids:
        return {"published": False, "reason": "no admissions to look up"}
    with db_connect(dsn, connect_timeout=15) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT e.admission_id::text, e.status, ev.event_id,
                   ev.payload->>'generation_id', ev.payload->>'branch_name'
              FROM okg.admission_envelopes e
              LEFT JOIN LATERAL (
                SELECT event_id, payload
                  FROM okg.admission_events
                 WHERE admission_id = e.admission_id
                   AND event_type = 'published'
                 ORDER BY event_id DESC
                 LIMIT 1
              ) ev ON TRUE
             WHERE e.deployment_name = %s
               AND e.admission_id = ANY(%s::uuid[])
            """,
            (deployment, ids),
        )
        rows = cur.fetchall()
    return publication_verdict(ids, rows)


def publication_verdict(
    admission_ids: Sequence[str], rows: Sequence[Sequence[Any]],
) -> dict[str, Any]:
    """Decide from `(admission_id, status, event_id, generation_id, branch)`.

    Every id must be `published` with a published event on the default
    branch; a version published on another branch binds nothing.
    """
    by_id = {str(row[0]): row for row in rows}
    ids = [str(item) for item in admission_ids]
    statuses = {
        item: {
            "status": by_id[item][1] if item in by_id else "not_found",
            "generation_id": by_id[item][3] if item in by_id else None,
            "branch": by_id[item][4] if item in by_id else None,
        }
        for item in ids
    }
    bound = [
        by_id[item] for item in ids
        if item in by_id and by_id[item][1] == "published"
        and by_id[item][3] and by_id[item][4] == PUBLISH_BRANCH
    ]
    if not ids or len(bound) != len(ids):
        return {"published": False, "admissions": statuses}
    newest = max(bound, key=lambda row: int(row[2]))
    return {
        "published": True,
        "generation_id": str(newest[3]),
        "admissions": statuses,
    }


def _earlier_published(
    report: Mapping[str, Any], admission_ids: Sequence[str],
) -> Optional[dict[str, Any]]:
    """The newest earlier publish of this run that published these admissions.

    Only a re-drive or recovery promotion `okg ingest` runs ahead of the
    sources lands here; the last entry is this run's own commit publish. Its
    `admission_feedback` names the admissions that publish marked published,
    and every id in `admission_ids` must be among them, on the default branch.
    """
    wanted = {str(item) for item in admission_ids}
    if not wanted:
        return None
    body = report.get("report") if isinstance(report.get("report"), Mapping) else report
    rows = [row for row in (body.get("publishes") or ()) if isinstance(row, Mapping)]
    for row in reversed(rows[:-1]):
        if classify_publish(row).kind != PUBLISHED or not row.get("generation_id"):
            continue
        feedback = row.get("admission_feedback")
        if not isinstance(feedback, Mapping):
            continue
        # A row that does not name its branch binds nothing.
        if feedback.get("branch_name") != PUBLISH_BRANCH:
            continue
        marked = {str(item) for item in feedback.get("admission_ids") or ()}
        if wanted <= marked:
            return dict(row)
    return None


def _last_publish(report: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    body = report.get("report") if isinstance(report.get("report"), Mapping) else report
    publishes = [p for p in (body.get("publishes") or ()) if isinstance(p, Mapping)]
    return dict(publishes[-1]) if publishes else None


# --------------------------------------------------------------------------
# Source manifests (`--manifest`, PACT no-code-source-manifest)
# --------------------------------------------------------------------------

MANIFEST_SAMPLES_PER_TYPE = 3
_MANIFEST_FILES_SHOWN = 20


def _manual_path(plan: AddSourcePlan) -> str:
    return (
        "to make this change, remove the source with `okg deployment "
        f"remove-source {plan.deployment_name} {plan.name} --apply`, then add "
        "it again with the new manifest; that re-imports every record"
    )


def _manifest_failed(
    steps: list[Step], step: str, exc: Any, *, extra: Optional[Mapping[str, Any]] = None,
) -> None:
    detail = exc.as_dict() if hasattr(exc, "as_dict") else {}
    if extra:
        detail = {**detail, **extra}
    steps.append(Step(
        step, "failed", detail=detail, cause=str(exc),
        fix_hint=getattr(exc, "fix", None),
    ))


def _manifest_source_type(compiled: Any) -> SupportedSourceType:
    from okg.substrate.sources.guided_catalog import manifest_type

    return manifest_type(
        compiled.registration,
        summary=(
            compiled.manifest.description
            or f"Records the source manifest for {compiled.source} describes."
        ),
        reads_http=compiled.manifest.has_http,
    )


def _taken_type_names(
    plan: AddSourcePlan, registry: Mapping[str, Any], schema_dir: Optional[Path],
) -> dict[str, str]:
    """Type names other sources or schema files in the deployment declare."""
    from okg.substrate.sources.manifest import read_generated_header, schema_file_subtypes

    taken: dict[str, str] = {}
    for other, body in (registry.get("sources") or {}).items():
        if other == plan.name or not isinstance(body, Mapping):
            continue
        signature = (body.get("admission_policy") or {}).get("output_signature") or {}
        for node in signature.get("nodes") or ():
            if isinstance(node, Mapping) and node.get("subtype"):
                taken.setdefault(str(node["subtype"]), f"source {other!r}")
    if schema_dir is not None and schema_dir.is_dir():
        for path in sorted(schema_dir.rglob("*.yaml")):
            try:
                header = read_generated_header(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError):
                header = None
            if header is not None and header.source == plan.name:
                continue
            for subtype in schema_file_subtypes(path):
                taken.setdefault(
                    subtype,
                    f"the deployment schema {path.relative_to(plan.deployment_dir)}"
                    if path.is_relative_to(plan.deployment_dir) else f"schema {path}",
                )
    return taken


def _plan_generated_files(
    plan: AddSourcePlan, compiled: Any, schema_dir: Path,
) -> tuple[list[dict[str, str]], list[str]]:
    """(planned files, refusals). A file is replaced only when AKMON wrote it."""
    from okg.substrate.sources.manifest import read_generated_header

    wanted: list[tuple[Path, str]] = [
        (plan.deployment_dir / compiled.manifest_relpath, compiled.manifest_copy_text),
        (schema_dir / compiled.schema_relpath, compiled.schema_text),
    ]
    if compiled.bridges_text is not None:
        wanted.append((schema_dir / compiled.bridges_relpath, compiled.bridges_text))
    files: list[dict[str, str]] = []
    refusals: list[str] = []
    for target, text in wanted:
        if target.is_symlink() or (target.exists() and not target.is_file()):
            refusals.append(f"{target} is not a regular file")
            continue
        if not target.exists():
            files.append({"to": str(target), "action": "write", "text": text})
            continue
        current = target.read_text(encoding="utf-8")
        if current == text:
            files.append({"to": str(target), "action": "present", "text": text})
            continue
        header = read_generated_header(current)
        if header is None or header.source != plan.name:
            refusals.append(f"{target} exists and was not written by okg for source {plan.name!r}")
        elif not header.body_intact:
            refusals.append(f"{target} was edited by hand after okg wrote it")
        else:
            files.append({"to": str(target), "action": "replace", "text": text})
    return files, refusals


def _plan_manifest(
    plan: AddSourcePlan,
    manifest_path: Path,
    *,
    env: Mapping[str, str],
    run: Runner,
    preview_limit: int,
) -> None:
    from okg.substrate.sources.manifest import (
        ManifestError,
        additive_change_problems,
        compile_manifest,
        load_manifest,
        parse_manifest,
        read_generated_header,
    )

    steps = plan.steps
    try:
        manifest = load_manifest(manifest_path)
    except ManifestError as exc:
        _manifest_failed(steps, "manifest", exc)
        return
    raw_registry = _load_registry(plan.registry_path)
    existing = (raw_registry.get("sources") or {}).get(plan.name)
    if isinstance(existing, Mapping):
        if (existing.get("module"), existing.get("class")) != (
            "okg.substrate.library.sources.manifest_records", "ManifestRecordSource",
        ):
            steps.append(Step(
                "name", "failed",
                cause=(
                    f"source {plan.name!r} already exists in this deployment "
                    "with a different reader"
                ),
                fix_hint="choose another name, or remove the old source first",
            ))
            return
        plan.existing_entry = dict(existing)
    deployment_manifest = _load_yaml(plan.deployment_dir / "deployment.yaml")
    schema_dir = _connector_schema_dir(plan.deployment_dir, deployment_manifest)
    if schema_dir is None:
        steps.append(Step(
            "schema", "failed",
            cause=(
                "a source manifest generates graph types, and deployment.yaml "
                "names no `schema_dir` to install them into"
            ),
            fix_hint="add `schema_dir: schemas` to deployment.yaml and re-run",
        ))
        return
    try:
        compiled = compile_manifest(
            manifest, source=plan.name,
            taken=_taken_type_names(plan, raw_registry, schema_dir),
        )
    except ManifestError as exc:
        _manifest_failed(steps, "manifest", exc)
        return

    saved = plan.deployment_dir / compiled.manifest_relpath
    if plan.existing_entry is not None and not saved.is_file():
        steps.append(Step(
            "manifest", "failed",
            detail={"saved_manifest": str(saved)},
            cause=(
                f"source {plan.name!r} is already saved, but its manifest copy "
                f"{saved} is missing, so this change cannot be checked against it"
            ),
            fix_hint=(
                f"restore {saved} from the deployment's git history, or "
                + _manual_path(plan)
            ),
        ))
        return
    # Only a saved source is held to additive changes. After the source is
    # removed, its generated files remain and are replaced like any file
    # okg wrote, so "remove it and add it again" works for any change.
    if plan.existing_entry is not None and saved.is_file():
        text = saved.read_text(encoding="utf-8")
        header = read_generated_header(text)
        if header is None or header.source != plan.name or not header.body_intact:
            steps.append(Step(
                "manifest", "failed",
                detail={"saved_manifest": str(saved)},
                cause=(
                    f"the saved manifest copy {saved} was not written by okg "
                    "for this source, or was edited by hand"
                ),
                fix_hint=(
                    "edit your own manifest file and re-run with it; restore "
                    f"or remove {saved} first"
                ),
            ))
            return
        try:
            old = parse_manifest(text, base_dir=saved.parent, origin=str(saved))
        except ManifestError as exc:
            _manifest_failed(steps, "manifest", exc, extra={"saved_manifest": str(saved)})
            return
        refused = additive_change_problems(old, manifest, manual_path=_manual_path(plan))
        if refused:
            steps.append(Step(
                "manifest", "failed",
                detail={"problems": [p.as_dict() for p in refused]},
                cause=(
                    "a re-apply may only add types, fields and links: "
                    + "; ".join(p.message for p in refused)
                ),
                fix_hint=_manual_path(plan),
            ))
            return

    files, refusals = _plan_generated_files(plan, compiled, schema_dir)
    if refusals:
        steps.append(Step(
            "schema", "failed",
            detail={"refused_files": refusals},
            cause="; ".join(refusals),
            fix_hint=(
                "only files okg generated are replaced; move or rename the "
                "named file, then re-run"
            ),
        ))
        return
    plan.generated_files = files
    if manifest.deletion != "never" and manifest.max_deleted_fraction is not None:
        plan.behaviour_overrides["removal"] = (
            "A record missing from a complete read is removed, but a read that "
            f"would remove more than {manifest.max_deleted_fraction:g} of the "
            "earlier records applies its records and removes nothing "
            "(max_deleted_fraction). A read that fails, or that rejects a "
            "record, removes nothing."
        )
    if manifest.deletion == "never":
        plan.behaviour_overrides["removal"] = (
            "Records are never removed (`deletion: never`): a record missing "
            "from the files stays in the graph. A record that is read again "
            "loses links and passages its new content no longer has."
        )
    plan.source_type = _manifest_source_type(compiled)
    plan.params = dict(compiled.params)
    plan.entry = build_registry_entry(
        plan.source_type, deployment_name=plan.deployment_name, name=plan.name,
        params=plan.params,
    )
    steps.append(Step("manifest", "ok", detail={
        "manifest": str(manifest_path),
        "manifest_sha256": manifest.digest,
        "reads": [read.describe() for _type, read in manifest.all_reads()],
        "deletion": manifest.deletion,
        "max_deleted_fraction": manifest.max_deleted_fraction,
        "types": compiled.node_types,
    }))
    if not _check_reader(plan, steps):
        return
    if not _check_access(plan.source_type, plan.entry, env, run, steps):
        return
    to_write = [
        {"to": f["to"], "action": f["action"]} for f in files if f["action"] != "present"
    ]
    if to_write:
        plan.decisions.append({
            "kind": "generated_schemas",
            "install": to_write,
            "why": (
                "the manifest's record types become graph types; OKG writes "
                "the manifest copy and the generated schemas into the "
                "deployment and applies the catalog"
            ),
        })
    steps.append(Step("schema", "ok", detail={
        "generated_files": [{"to": f["to"], "action": f["action"]} for f in files],
    }))
    _decide_posture(plan.source_type, plan.deployment_dir, plan)
    _preview_manifest(plan, compiled, schema_dir, preview_limit, env=env)


def _compose_with_generated(
    deployment_dir: Path, schema_dir: Path, compiled: Any,
) -> Any:
    """Compose the deployment's catalog with the generated files, in a copy."""
    from okg.substrate.catalog.module_providers import builtin_module_providers
    from okg.substrate.catalog.modules_compose import (
        compose_catalog,
        merge_declared_providers,
    )
    from okg.substrate.deployments import resolve_declared_module_providers

    manifest_file = deployment_dir / "deployment.yaml"
    document = _load_yaml(manifest_file)
    declared = resolve_declared_module_providers(
        document, deployment_dir, manifest_path=manifest_file,
    )
    providers = (
        merge_declared_providers(builtin_module_providers(), declared)
        if declared else None
    )
    with tempfile.TemporaryDirectory(prefix="okg-manifest-preview-") as tmp:
        root = Path(tmp)
        target = root / "schemas"
        if schema_dir.is_dir():
            shutil.copytree(schema_dir, target, symlinks=True)
        target.mkdir(parents=True, exist_ok=True)
        (target / compiled.schema_relpath).write_text(compiled.schema_text, encoding="utf-8")
        if compiled.bridges_text is not None:
            (target / "bridges").mkdir(exist_ok=True)
            (target / compiled.bridges_relpath).write_text(
                compiled.bridges_text, encoding="utf-8",
            )
        aliases = deployment_dir / "aliases.json"
        if aliases.is_file():
            shutil.copyfile(aliases, root / "aliases.json")
        copy = {k: v for k, v in document.items() if k != "module_providers"}
        copy["schema_dir"] = "schemas"
        (root / "deployment.yaml").write_text(
            yaml.safe_dump(copy, sort_keys=False), encoding="utf-8",
        )
        return compose_catalog(root, providers=providers)


def _preview_manifest(
    plan: AddSourcePlan, compiled: Any, schema_dir: Path, limit: int,
    env: Optional[Mapping[str, str]] = None,
) -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.sources.manifest import (
        ManifestError,
        ManifestProblem,
        admission_problems,
        keyless_over_share,
        keyless_share_message,
        map_records,
        record_facts,
    )
    from okg.substrate.sources.manifest_http import read_inputs

    started = time.monotonic()
    manifest = compiled.manifest
    try:
        inputs = read_inputs(manifest, env=env)
    except ManifestError as exc:
        _manifest_failed(plan.steps, "preview", exc)
        return
    mapped = map_records(
        manifest, source=plan.name, inputs=inputs.records, input_types=inputs.record_types,
    )
    if any(p.code in _REJECTION_CODES for p in mapped.problems) or inputs.failed:
        # The reader applies the valid records and, under
        # missing_from_full_read, withholds ALL deletion for such a read.
        withheld = (
            "nothing is ever removed (deletion: never)"
            if manifest.deletion == "never" else
            "the valid records are imported, and no record is removed by any "
            "read until this is fixed"
        )
        mapped.problems = [
            replace(problem, message=f"{problem.message}; {withheld}")
            if problem.code in _REJECTION_CODES else problem
            for problem in mapped.problems
        ]
    over = keyless_over_share(mapped) if manifest.deletion != "never" else {}
    if over:
        mapped.problems.append(ManifestProblem(
            "keyless_share_exceeded",
            f"{keyless_share_message(over)}; every read would be refused",
            "check the type's key path against the data; records with no value at it are skipped",
        ))
    facts: list[Any] = []
    for record in mapped.records.values():
        facts.extend(record_facts(record, source=plan.name))
    composed = None
    unreadable = [
        replace(
            problem, blocking=False,
            message=(
                f"{problem.message}; the other "
                f"{'reads' if problem.code == 'http_read_failed' else 'files'} are imported "
                "and no record is removed until it reads"
            ),
        )
        for problem in inputs.failed
    ]
    mapped.problems = unreadable + list(mapped.problems)
    blocking = list(mapped.duplicate_keys_in_file) + [p for p in mapped.problems if p.blocking]
    if inputs.reads and not any(r["complete"] for r in inputs.reads):
        blocking.append(ManifestProblem(
            "no_read_completed",
            "no read completed, so applying would import nothing",
            "fix the causes named above and preview again",
        ))
    try:
        composed = _compose_with_generated(plan.deployment_dir, schema_dir, compiled)
    except Exception as exc:  # noqa: BLE001 - the catalog's own refusal is the message
        from okg.substrate.sources.manifest import ManifestProblem

        blocking.append(ManifestProblem(
            "catalog_refused",
            "the deployment's catalog does not compose with the generated "
            f"types: {redact_text(str(exc))}",
            "fix the cause named; a containment cycle needs one link renamed "
            "to a reference name such as related_to",
            path="types",
        ))
    admission = admission_problems(compiled, facts, composed)
    blocking += admission
    samples: dict[str, list[dict[str, Any]]] = {}
    for record in mapped.records.values():
        bucket = samples.setdefault(record.type, [])
        if len(bucket) < MANIFEST_SAMPLES_PER_TYPE:
            bucket.append({
                "key": record.key,
                "node_id": f"{plan.name}:{record.type}:{record.key}",
                "attrs": _display_attrs(record.attrs),
                "links": [
                    f"{link.edge_type} -> {link.dst_type}:{link.dst_key}"
                    for link in record.links
                ],
                "passages": len(record.passages),
            })
    records: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []
    body_queries: list[str] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            if len(records) < limit:
                records.append(_describe_node(fact))
            for word in _body_words(fact):
                if word not in body_queries and len(body_queries) < _BODY_WORD_LIMIT:
                    body_queries.append(word)
        elif isinstance(fact, EdgeFact) and len(relationships) < limit:
            relationships.append({"type": fact.edge_type, "from": fact.src, "to": fact.dst})
    plan.body_queries = body_queries
    problems = [p.as_dict() for p in (*blocking, *(p for p in mapped.problems if not p.blocking))]
    plan.preview = {
        "scope": {
            "files_matched": len(inputs.files),
            "files_unreadable": len(inputs.failed),
            "records_read_in_preview": len(mapped.records),
            "relationships_read_in_preview": sum(
                1 for f in facts if isinstance(f, EdgeFact)
            ),
        },
        "records": records,
        "relationships": relationships,
        "bounded": False,
        "elapsed_s": round(time.monotonic() - started, 3),
        "manifest": {
            "reads": [
                {k: v for k, v in r.items() if k != "elapsed_s"} for r in inputs.reads
            ],
            "max_deleted_fraction": manifest.max_deleted_fraction,
            "files": inputs.files[:_MANIFEST_FILES_SHOWN],
            "files_matched": len(inputs.files),
            "records_per_type": mapped.counts_by_type(),
            "samples": samples,
            "types": compiled.node_types,
            "links": [" ".join(t) for t in compiled.edge_triples],
            "deletion": manifest.deletion,
            "rejected_records": len(mapped.rejected),
            "dropped_links": mapped.dropped_links,
            "nested_conflicts": mapped.conflicts,
            "problems": problems,
            "admission": {
                "accepted": not admission and composed is not None,
                "problems": [p.as_dict() for p in admission],
            },
        },
    }
    if blocking:
        first = blocking[0]
        where = f" ({first.file})" if first.file else ""
        plan.steps.append(Step(
            "preview", "failed",
            detail={"problems": [p.as_dict() for p in blocking]},
            cause=f"{first.message}{where}",
            fix_hint=first.fix,
        ))
        return
    plan.steps.append(Step("preview", "ok", detail={
        "records_shown": len(records), "relationships_shown": len(relationships),
        "problems": len(problems),
    }))


# A record with no key value (key_missing) is skipped and does not withhold
# deletion; too many of them refuse the read (keyless_share_exceeded).
_REJECTION_CODES = frozenset({"invalid_value", "key_invalid", "record_not_object"})


def _install_generated_files(plan: AddSourcePlan) -> list[str]:
    """Write the manifest's generated files; return the paths written.

    Re-checked against the files as they are now: a file that changed since
    the preview and does not carry okg's intact header for this source is
    refused (ValueError, reported by the save step) before anything is
    written.
    """
    from okg.substrate.sources.manifest import read_generated_header

    pending: list[tuple[Path, str]] = []
    for item in plan.generated_files:
        target = Path(item["to"])
        text = item["text"]
        if target.is_file():
            current = target.read_text(encoding="utf-8")
            if current == text:
                continue
            header = read_generated_header(current)
            if header is None or header.source != plan.name or not header.body_intact:
                raise ValueError(
                    f"{target} changed since the preview and was not written by "
                    "okg for this source; nothing was overwritten"
                )
        elif target.exists() or target.is_symlink():
            raise ValueError(f"{target} is not a regular file; nothing was overwritten")
        pending.append((target, text))
    for target, text in pending:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return [str(target) for target, _ in pending]


def _resolve_deployment(
    deployment: str,
    *,
    deployments_root: Optional[str | Path],
) -> tuple[Path, str, Path]:
    from okg.substrate.deployment_lint import resolve_deployment_manifest_path

    manifest_path = resolve_deployment_manifest_path(
        deployment, deployments_root=deployments_root,
    )
    if manifest_path is None:
        raise AddSourceError(
            f"deployment {deployment!r} not found",
            data={"deployment": deployment},
            fix_hint="pass the deployment name, directory, or deployment.yaml path",
        )
    manifest = _load_yaml(manifest_path)
    name = str(manifest.get("name") or manifest_path.parent.name)
    registry_rel = str(manifest.get("source_registry") or "source_registry.yaml")
    registry_path = (manifest_path.parent / registry_rel).resolve()
    return manifest_path.parent, name, registry_path


def _load_registry(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"sources": {}}
    raw = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: source registry root must be a mapping")
    return raw


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    raw = fastyaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return raw if isinstance(raw, dict) else {}


def _valid_source_name(name: str) -> bool:
    return bool(name) and name[0].isalpha() and all(
        ch.isalnum() or ch in "-_" for ch in name
    )


__all__ = [
    "AddSourceError",
    "AddSourceOutcome",
    "AddSourcePlan",
    "Step",
    "apply_add_source",
    "build_registry_entry",
    "complete_scan_mode",
    "plan_add_source",
    "read_ledger",
]
