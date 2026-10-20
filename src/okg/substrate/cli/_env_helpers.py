"""Manifest-env resolution helpers shared by ``okg deployment env`` and
catalog-ownership flows.

Lifted out of dev's `cli/main.py` additions during the
``merge-dev-into-cleanup-wave`` PACT (2026-05-28). Leading underscore in the
filename signals "internal — `cli/deployment.py` + `cli/main.py` import from
here; YAML manifests don't reach in".

The four helpers wrap the workflow:

  1. ``_env_refs(value)`` — extract ``${VAR}`` placeholders from a string
     (used to discover which env vars a deployment.yaml's ``postgres.dsn``
     depends on).
  2. ``_deployment_manifest_path_for_env(deployment)`` — resolve a deployment
     name/path to its ``deployment.yaml`` without parsing env placeholders
     (needed precisely *because* we want to read raw ``${VAR}`` references
     before substitution).
  3. ``_redact_env_value(value, show_values)`` — redact secrets / DSNs
     unless the operator explicitly opts into showing values.
  4. ``_write_local_env_value(path, key, value)`` — idempotently upsert
     ``KEY=value`` into the local env profile (creates the file if needed,
     replaces an existing matching line in-place otherwise).
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.errors import SubstrateError


__all__ = [
    "_env_refs",
    "_deployment_manifest_path_for_env",
    "_redact_env_value",
    "_write_local_env_value",
]


_ENV_REF_RE = None


def _env_refs(value: str | None) -> list[str]:
    if not isinstance(value, str):
        return []
    import re
    global _ENV_REF_RE
    if _ENV_REF_RE is None:
        _ENV_REF_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
    return sorted(set(_ENV_REF_RE.findall(value)))


def _deployment_manifest_path_for_env(deployment: str) -> Path:
    """Resolve a deployment name/path without parsing env placeholders."""
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        find_deployment_dir,
    )

    candidate = Path(deployment).expanduser()
    if candidate.is_file():
        return candidate.resolve()
    if candidate.is_dir() and (candidate / "deployment.yaml").is_file():
        return (candidate / "deployment.yaml").resolve()

    try:
        deployment_dir = find_deployment_dir(deployment)
    except DeploymentResolutionError as exc:
        raise SubstrateError(str(exc)) from exc
    if deployment_dir is None:
        raise SubstrateError(
            f"deployment {deployment!r} not found",
            data={"deployment": deployment},
            fix_hint=(
                "check deployments/<name>/ or set OKG_DEPLOYMENTS_DIR"
            ),
        )
    manifest_path = deployment_dir / "deployment.yaml"
    if not manifest_path.is_file():
        raise SubstrateError(
            f"deployment {deployment!r} has no deployment.yaml",
            data={"deployment": deployment, "manifest_path": str(manifest_path)},
        )
    return manifest_path.resolve()


def _redact_env_value(value: str | None, *, show_values: bool) -> str | None:
    if value is None:
        return None
    if show_values:
        return value
    if value.startswith("${") and value.endswith("}"):
        return value
    if "://" not in value:
        return "<set>"
    return "<redacted-dsn>"


def _write_local_env_value(path: Path, key: str, value: str) -> None:
    import os
    import re
    import shlex

    key_re = re.compile(rf"^\s*(?:export\s+)?{re.escape(key)}\s*=")
    quoted = shlex.quote(value)
    next_line = f"{key}={quoted}"
    lines: list[str] = []
    replaced = False
    if path.is_file():
        lines = path.read_text().splitlines()
        for idx, line in enumerate(lines):
            if key_re.match(line):
                lines[idx] = next_line
                replaced = True
                break
    if not replaced:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(next_line)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    try:
        os.chmod(path, 0o600)
    except OSError:
        # Best-effort hardening; platforms/filesystems may reject chmod.
        pass
