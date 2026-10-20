"""Read-only lifecycle admission before active deployment effects.

Inspection deliberately continues to use the ordinary deployment resolver.
This module never resolves credentials or imports deployment code.
"""
import os
from pathlib import Path

from okg.substrate.errors import SubstrateError


class DeploymentArchived(SubstrateError):
    code = "deployment_archived"


def _sealed_manifest_path(deployment: str | Path) -> Path | None:
    """Read only the pinned definition; this never grants launch authority.

    Successful admission still reaches the unchanged full environment loader.
    No database/secret parsing is needed to refuse an archived definition.
    """
    descriptor = os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR")
    release_root = os.environ.get("OKG_RELEASE_ROOT")
    if not descriptor and not release_root:
        return None
    from okg.substrate.deployments import DeploymentResolutionError
    from okg.substrate.deployments.environment import (
        _definition_path, _load_yaml_mapping, find_environment_path,
    )

    if not descriptor or not release_root:
        raise DeploymentResolutionError("sealed deployment resolution environment is incomplete")
    descriptor_path = find_environment_path(descriptor)
    raw = _load_yaml_mapping(descriptor_path, label="environment descriptor")
    definition = raw.get("definition")
    if not isinstance(definition, dict) or not isinstance(definition.get("path"), str):
        raise DeploymentResolutionError("sealed environment definition.path is missing or invalid")
    path = _definition_path(definition["path"], descriptor_path=descriptor_path)
    requested = str(deployment)
    if isinstance(deployment, Path):
        matches = deployment.resolve() in {path, path.parent}
    else:
        matches = requested in {raw.get("id"), raw.get("graph_deployment_name")}
    if not matches:
        raise DeploymentResolutionError("sealed process cannot resolve a deployment outside its active environment")
    return path


def archived_deployment(deployment: str | Path) -> DeploymentArchived | None:
    from okg.substrate import fastyaml
    from okg.substrate.deployments import DeploymentConfigError, find_deployment_dir

    from okg.substrate.deployments import _looks_like_manifest_path, _resolve_manifest_path

    # Path callers already carry the exact selected profile/service authority.
    sealed_path = None if isinstance(deployment, Path) else _sealed_manifest_path(deployment)
    if sealed_path is not None:
        manifest_path = sealed_path
    elif isinstance(deployment, Path) or _looks_like_manifest_path(str(deployment)):
        manifest_path = _resolve_manifest_path(str(deployment))
        if manifest_path is None:
            return None
    else:
        directory = find_deployment_dir(str(deployment))
        if directory is None:
            return None  # Existing resolution owns missing-deployment errors.
        manifest_path = directory / "deployment.yaml"
    if not manifest_path.is_file():
        return None  # Standalone profiles need not have a deployment manifest.
    raw = fastyaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise DeploymentConfigError(f"deployment manifest must be a mapping: {manifest_path}")
    contract = raw.get("contract") or {}
    if not isinstance(contract, dict):
        raise DeploymentConfigError(f"deployment contract must be a mapping: {manifest_path}")
    if contract.get("lifecycle") != "archived":
        return None
    return DeploymentArchived(
        "archived deployments cannot perform active operations",
        data={
            "deployment": raw.get("name") or manifest_path.parent.name,
            "manifest_path": str(manifest_path.resolve()),
            "lifecycle": "archived",
            "archive_reason": contract.get("archive_reason"),
        },
        fix_hint="inspect the preserved deployment or stop its services; obtain an authorized operator lifecycle decision before activation",
    )


def require_active_deployment(deployment: str | Path) -> None:
    refusal = archived_deployment(deployment)
    if refusal is not None:
        raise refusal


def inactive_plan(deployment: str | Path) -> dict | None:
    refusal = archived_deployment(deployment)
    if refusal is None:
        return None
    return {"ok": False, "active": False, "executable": False, **refusal.to_json()}
