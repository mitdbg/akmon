"""Public deployment-scoped Nomos baseline reconciliation.

This module is the supported service boundary shared by deployment apply,
deployment upgrade, and provisioning.  Callers can inspect the exact desired
and active fingerprints without writes, then explicitly apply the same spec.
"""
from __future__ import annotations

import stat
from pathlib import Path
from typing import Any, Mapping

from okg.substrate import fastyaml
from okg.substrate.errors import SubstrateError
from okg.substrate.nomos.core import (
    CompiledPolicyBundle,
    NomosPolicyError,
    compile_config_graph,
    compile_policy_graph,
    load_policy_graph,
)
from okg.substrate.nomos.store import (
    deployment_baseline_config_graph,
    deployment_baseline_policy_graph,
    deployment_derived_source_families,
    load_active_config_set,
    load_active_policy_bundle,
    seed_deployment_baseline_nomos_state,
)
from okg.substrate.db.connection import connect as db_connect


_ROLLOUT_SOURCE_RUN_RULE_ID = "rollout_allow_registered_source_runs"


def _deployment_source_binding_policy(
    *,
    manifest: Mapping[str, Any],
    deployment: str,
    deployment_dir: Path,
    scope: str,
    expected_source_names: list[str],
) -> CompiledPolicyBundle | None:
    nomos = manifest.get("nomos") or {}
    if not isinstance(nomos, Mapping):
        raise SubstrateError("deployment nomos configuration must be a mapping")
    declared = nomos.get("source_binding_policy")
    if declared is None:
        return None
    if not isinstance(declared, str) or not declared.strip():
        raise SubstrateError(
            "nomos.source_binding_policy must be a non-empty path string"
        )
    declared_path = Path(declared).expanduser()
    if declared_path.is_absolute() or ".." in declared_path.parts:
        raise SubstrateError(
            "nomos.source_binding_policy must be a deployment-relative path "
            "without parent traversal"
        )
    artifact_path = deployment_dir / declared_path
    current = deployment_dir
    for part in declared_path.parts:
        current = current / part
        if current.is_symlink():
            raise SubstrateError(
                "nomos.source_binding_policy must not traverse symlinks",
                data={"path": str(current)},
            )
    try:
        artifact_stat = artifact_path.lstat()
    except FileNotFoundError:
        raise SubstrateError(
            f"declared Nomos source-binding policy does not exist: {artifact_path}",
            fix_hint="fix `nomos.source_binding_policy` in deployment.yaml",
        )
    if not stat.S_ISREG(artifact_stat.st_mode):
        raise SubstrateError(
            "nomos.source_binding_policy must name a regular file",
            data={"path": str(artifact_path)},
        )
    resolved_artifact_path = artifact_path.resolve(strict=True)
    try:
        resolved_artifact_path.relative_to(deployment_dir)
    except ValueError as exc:
        raise SubstrateError(
            "nomos.source_binding_policy must remain inside the deployment root",
            data={"path": str(resolved_artifact_path)},
        ) from exc
    try:
        bundle = compile_policy_graph(load_policy_graph(resolved_artifact_path))
    except NomosPolicyError as exc:
        raise SubstrateError(
            f"invalid Nomos source-binding policy: {resolved_artifact_path}: {exc}",
            fix_hint="fix the checked-in source-binding policy artifact",
        ) from exc

    expected_scope = {"deployment": deployment, "scope": scope}
    if bundle.default_effect != "deny" or dict(bundle.scope) != expected_scope:
        raise SubstrateError(
            "Nomos source-binding policy must default deny and use the exact "
            "deployment scope",
            data={
                "path": str(resolved_artifact_path),
                "expected_scope": expected_scope,
                "observed_scope": dict(bundle.scope),
                "default_effect": bundle.default_effect,
            },
        )

    expected_sources = set(expected_source_names)
    observed: dict[str, set[str]] = {}
    for rule in bundle.rules:
        if (
            rule.effect != "allow"
            or len(rule.subjects) != 1
            or len(rule.actions) != 1
            or rule.actions[0] not in {"source.ingest", "source.emit"}
        ):
            raise SubstrateError(
                "Nomos source-binding rules must be single-subject, "
                "single-action allow rules",
                data={
                    "path": str(resolved_artifact_path),
                    "rule_id": rule.rule_id,
                },
            )
        action = rule.actions[0]
        source = str(rule.match.get("source") or "")
        expected_match = {
            "deployment": deployment,
            "scope": scope,
            "source": source,
            "graph_branch": "default",
            "resource_kind": (
                "source" if action == "source.ingest" else "node"
            ),
            "classification": "internal",
            "principal_kind": "source_adapter",
        }
        if (
            not source
            or rule.subjects != (f"source:{source}",)
            or dict(rule.match) != expected_match
        ):
            raise SubstrateError(
                "Nomos source-binding rule must bind one exact source subject "
                "to the same source resource on the default branch",
                data={
                    "path": str(resolved_artifact_path),
                    "rule_id": rule.rule_id,
                },
            )
        actions = observed.setdefault(source, set())
        if action in actions:
            raise SubstrateError(
                "Nomos source-binding policy contains a duplicate source action",
                data={
                    "path": str(resolved_artifact_path),
                    "source": source,
                    "action": action,
                },
            )
        actions.add(action)
    expected_actions = {"source.ingest", "source.emit"}
    if set(observed) != expected_sources or any(
        actions != expected_actions for actions in observed.values()
    ):
        raise SubstrateError(
            "Nomos source-binding policy must cover every runtime source exactly",
            data={
                "path": str(resolved_artifact_path),
                "expected_sources": sorted(expected_sources),
                "observed_sources": sorted(observed),
            },
        )
    return bundle


def _compose_source_binding_policy(
    baseline: Mapping[str, Any],
    bindings: CompiledPolicyBundle | None,
) -> dict[str, Any]:
    if bindings is None:
        return dict(baseline)
    baseline_rules = list(baseline.get("rules") or [])
    retained = [
        rule for rule in baseline_rules
        if rule.get("rule_id") != _ROLLOUT_SOURCE_RUN_RULE_ID
    ]
    if len(retained) != len(baseline_rules) - 1:
        raise SubstrateError(
            "deployment source-binding policy could not replace the generic "
            "rollout source-run rule"
        )
    return {
        **dict(baseline),
        "rules": [*retained, *(rule.to_dict() for rule in bindings.rules)],
    }


def _assert_apply_digest_current(
    *,
    deployment: str,
    deployment_dir: Path | str,
    dsn: str,
    expected: str,
) -> None:
    # Import lazily to keep the public Nomos boundary usable independently of
    # deployment apply. When apply supplies a digest, both the pre-write and
    # pre-commit checks run inside the Nomos transaction: a concurrent file
    # edit therefore rolls back the stale activation instead of merely being
    # discovered after it became active.
    from okg.substrate.deployment_apply import desired_deployment_state

    observed = desired_deployment_state(
        deployment=deployment,
        deployment_dir=deployment_dir,
        dsn=dsn,
    )["desired_state_digest"]
    if observed != expected:
        raise SubstrateError(
            "Nomos baseline refused a stale deployment-apply digest",
            data={"expected": expected, "observed": observed},
            fix_hint="run deployment apply dry-run again and confirm the new plan",
        )


def deployment_baseline_spec(
    *,
    deployment: str,
    deployment_dir: Path | str,
) -> dict[str, Any]:
    """Compile and audit the desired baseline without touching Postgres."""
    from okg.substrate.deployment_upgrade import (
        _manifest_requires_nomos_runtime,
        _nomos_policy_source_ids,
        _nomos_source_policies,
    )
    from okg.substrate.deployments import resolve_source_registry_path
    from okg.substrate.nomos import audit_deployment_policy
    from okg.substrate.sources.registry import (
        expand_code_repos_sources,
        runtime_source_subject_ids,
    )

    root = Path(deployment_dir).expanduser().resolve()
    manifest_path = root / "deployment.yaml"
    if not manifest_path.is_file():
        raise SubstrateError(
            f"deployment manifest is missing: {manifest_path}",
            fix_hint="pass a deployment directory containing deployment.yaml",
        )
    raw_manifest = fastyaml.safe_load(manifest_path.read_text()) or {}
    if not isinstance(raw_manifest, Mapping):
        raise SubstrateError("deployment manifest must be a mapping")
    if not _manifest_requires_nomos_runtime(dict(raw_manifest)):
        return {
            "ok": True,
            "required": False,
            "deployment": deployment,
            "status": "skipped",
        }

    # Resolve via the manifest, not the raw YAML: placeholders such as
    # ${manifest_dir} are expanded by manifest loading only.  Resolving from
    # raw YAML yields a literal path that never exists, and the old
    # `else {}` then reseeded a baseline with no code-source allow rule and a
    # fingerprint the readiness phase rejects.
    from okg.substrate.deployments import DeploymentManifest

    declared = raw_manifest.get("source_registry")
    registry_path = Path(
        getattr(
            DeploymentManifest.from_yaml_lenient(manifest_path),
            "source_registry",
            None,
        )
        or resolve_source_registry_path(root, dict(raw_manifest))
    )
    if registry_path.is_file():
        raw_registry = fastyaml.safe_load(registry_path.read_text()) or {}
    elif declared:
        raise SubstrateError(
            f"declared source registry does not exist: {registry_path}",
            fix_hint="fix `source_registry` in deployment.yaml",
        )
    else:
        raw_registry = {}
    if not isinstance(raw_registry, dict):
        raise SubstrateError("source registry must be a mapping")
    from okg.substrate.sources.registry import (
        SourceRegistryUnknownKeyError,
        check_legacy_registry_keys,
    )

    try:
        check_legacy_registry_keys(raw_registry, registry_path)
    except SourceRegistryUnknownKeyError as exc:
        err = SubstrateError(
            f"source_registry.yaml is invalid: {exc}",
            data={"detail_code": exc.code},
            fix_hint="fix the key the message names in source_registry.yaml",
        )
        err.code = "source_registry_invalid"
        raise err from exc
    if raw_registry.get("code_repos"):
        raw_registry = {
            **raw_registry,
            "sources": expand_code_repos_sources(
                raw_registry,
                registry_path,
                allow_unresolved_base=True,
            ),
        }

    audit = audit_deployment_policy(
        dict(raw_manifest),
        source_registry=raw_registry,
        deployment=deployment,
    )
    if not audit.get("ok"):
        raise SubstrateError(
            "Nomos deployment policy audit failed",
            data={"findings": audit.get("findings")},
            fix_hint="fix the deployment Nomos posture and source policies",
        )

    scope = f"deployment:{deployment}"
    source_names = sorted({
        *_nomos_policy_source_ids(
            raw_registry,
            deployment=deployment,
            deployment_dir=root,
        ),
        *deployment_derived_source_families(raw_manifest),
    })
    source_policies = _nomos_source_policies(
        raw_registry,
        audit.get("source_policies") or {},
    )
    runtime_names = sorted(
        runtime_source_subject_ids(raw_registry.get("sources") or {})
    )
    raw_sources = raw_registry.get("sources") or {}
    if isinstance(raw_sources, Mapping):
        binding_source_names = sorted({
            str(raw.get("ownership_id") or name).strip()
            for name, raw in raw_sources.items()
            if isinstance(raw, Mapping)
            and str(raw.get("ownership_id") or name).strip()
        })
    else:
        binding_source_names = sorted({
            str(raw.get("ownership_id") or raw.get("name") or "").strip()
            for raw in raw_sources
            if isinstance(raw, Mapping)
            and str(
                raw.get("ownership_id") or raw.get("name") or ""
            ).strip()
        })
    baseline_policy_graph = deployment_baseline_policy_graph(
        dict(raw_manifest),
        deployment_name=deployment,
        scope=scope,
        activated_by="subject:nomos-rollout",
        source_names=source_names,
        source_policies=source_policies,
        runtime_source_names=runtime_names,
    )
    source_binding_policy = _deployment_source_binding_policy(
        manifest=raw_manifest,
        deployment=deployment,
        deployment_dir=root,
        scope=scope,
        expected_source_names=binding_source_names,
    )
    policy_graph = _compose_source_binding_policy(
        baseline_policy_graph,
        source_binding_policy,
    )
    policy = compile_policy_graph(policy_graph)
    config = compile_config_graph(deployment_baseline_config_graph(
        dict(raw_manifest),
        deployment_name=deployment,
        scope=scope,
    ))
    return {
        "ok": True,
        "required": True,
        "deployment": deployment,
        "scope": scope,
        "manifest": dict(raw_manifest),
        "source_names": source_names,
        "source_policies": source_policies,
        "runtime_source_names": runtime_names,
        "policy_graph": policy_graph,
        "policy_fingerprint": policy.policy_fingerprint,
        "config_fingerprint": config.config_fingerprint,
        "audit": audit,
    }


def reconcile_deployment_baseline(
    *,
    deployment: str,
    deployment_dir: Path | str,
    dsn: str,
    apply: bool = False,
    desired_state_digest: str | None = None,
) -> dict[str, Any]:
    """Inspect or explicitly converge one deployment's baseline."""
    spec = deployment_baseline_spec(
        deployment=deployment,
        deployment_dir=deployment_dir,
    )
    if not spec.get("required"):
        return spec
    scope = str(spec["scope"])
    with db_connect(dsn) as conn:
        active_policy = load_active_policy_bundle(
            conn, deployment_name=deployment, scope=scope,
        )
        active_config = load_active_config_set(
            conn, deployment_name=deployment, scope=scope,
        )
        before = {
            "policy_fingerprint": (
                active_policy.policy_fingerprint if active_policy else None
            ),
            "config_fingerprint": (
                active_config.config_fingerprint if active_config else None
            ),
        }
        converged_before = (
            before["policy_fingerprint"] == spec["policy_fingerprint"]
            and before["config_fingerprint"] == spec["config_fingerprint"]
        )
        result = None
        if apply and not converged_before:
            if desired_state_digest is not None:
                _assert_apply_digest_current(
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                    dsn=dsn,
                    expected=desired_state_digest,
                )
            result = seed_deployment_baseline_nomos_state(
                conn,
                deployment_name=deployment,
                manifest=spec["manifest"],
                source_names=spec["source_names"],
                source_policies=spec["source_policies"],
                runtime_source_names=spec["runtime_source_names"],
                policy_graph=spec["policy_graph"],
            )
            if desired_state_digest is not None:
                _assert_apply_digest_current(
                    deployment=deployment,
                    deployment_dir=deployment_dir,
                    dsn=dsn,
                    expected=desired_state_digest,
                )
            conn.commit()
        after_policy = load_active_policy_bundle(
            conn, deployment_name=deployment, scope=scope,
        )
        after_config = load_active_config_set(
            conn, deployment_name=deployment, scope=scope,
        )
    after = {
        "policy_fingerprint": (
            after_policy.policy_fingerprint if after_policy else None
        ),
        "config_fingerprint": (
            after_config.config_fingerprint if after_config else None
        ),
    }
    converged = (
        after["policy_fingerprint"] == spec["policy_fingerprint"]
        and after["config_fingerprint"] == spec["config_fingerprint"]
    )
    return {
        "ok": converged if apply else True,
        "required": True,
        "deployment": deployment,
        "scope": scope,
        "desired": {
            "policy_fingerprint": spec["policy_fingerprint"],
            "config_fingerprint": spec["config_fingerprint"],
            "desired_state_digest": desired_state_digest,
        },
        "active_before": before,
        "active_after": after,
        "converged": converged,
        "changed": bool(apply and not converged_before),
        "result": result,
    }


__all__ = ["deployment_baseline_spec", "reconcile_deployment_baseline"]
