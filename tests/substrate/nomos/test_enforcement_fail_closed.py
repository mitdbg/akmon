"""Trust boundary #683: Nomos runtime-enforcement resolution fails closed.

``deployment_nomos_enforcement_mode`` used a single blanket
``except Exception: return "off"`` around dir resolution *and* manifest
read/parse. A present-but-unreadable or unparseable manifest on an
enforce deployment therefore silently resolved to ``off`` -- a fail-open on
the enforcement authority surface.

Per design.md Section 2(b), absence is established only by a *positive*
signal (dir not found, ``deployment.yaml`` genuinely missing, or a parsed
manifest with no ``nomos`` block). Every other failure after the dir is
resolved (permission, decode, YAML parse) must deny -- raise a typed error
rather than default to ``off``. Two adjacent fail-opens are in scope: an
unrecognized manifest enforcement value and an invalid
``OKG_NOMOS_RUNTIME_ENFORCEMENT`` token both become hard errors.

These tests exercise the resolution directly (pure filesystem, no DB) and
fail on the pre-fix code, which swallowed every case to ``off``.
"""

from __future__ import annotations

import pathlib
from pathlib import Path

import pytest


def _write_manifest(root: Path, name: str, body: str) -> Path:
    deployment = root / name
    deployment.mkdir(parents=True, exist_ok=True)
    manifest = deployment / "deployment.yaml"
    manifest.write_text(body, encoding="utf-8")
    return manifest


def _enforce_manifest(root: Path, name: str = "demo") -> Path:
    return _write_manifest(
        root,
        name,
        "name: demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "nomos:\n  runtime_enforcement: enforce\n",
    )


# --- genuine-absence cases still resolve off -------------------------------


def test_missing_deployment_dir_resolves_off(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root = tmp_path / "deployments"
    root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    # No deployment named "nope" anywhere: find_deployment_dir -> None -> off.
    assert deployment_nomos_enforcement_mode("nope") == "off"


def test_dir_without_manifest_resolves_off(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root = tmp_path / "deployments"
    (root / "docs-site").mkdir(parents=True)  # dir exists, no deployment.yaml
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    assert deployment_nomos_enforcement_mode("docs-site") == "off"


def test_manifest_without_nomos_block_resolves_off(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root = tmp_path / "deployments"
    _write_manifest(root, "demo", "name: demo\nschema_dir: schemas\n")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    assert deployment_nomos_enforcement_mode("demo") == "off"


def test_present_enforce_manifest_still_resolves_enforce(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root = tmp_path / "deployments"
    _enforce_manifest(root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    assert deployment_nomos_enforcement_mode("demo") == "enforce"


# --- present-but-unreadable manifest denies (never off) --------------------


def test_unparseable_manifest_denies(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import (
        NomosEnforcementResolutionError,
        deployment_nomos_enforcement_mode,
    )

    root = tmp_path / "deployments"
    # Present manifest, but its YAML is corrupt: we cannot even read whether
    # this deployment enforces. That is an unreadable *present* source -> deny.
    _write_manifest(root, "demo", "nomos: [unterminated\n  bad: : :\n")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    with pytest.raises(NomosEnforcementResolutionError):
        deployment_nomos_enforcement_mode("demo")


def test_unreadable_manifest_permission_denies(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import (
        NomosEnforcementResolutionError,
        deployment_nomos_enforcement_mode,
    )

    root = tmp_path / "deployments"
    _enforce_manifest(root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))

    original_read_text = pathlib.Path.read_text

    def deny_manifest_read(self, *args, **kwargs):
        if self.name == "deployment.yaml":
            raise PermissionError(13, "Permission denied")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(pathlib.Path, "read_text", deny_manifest_read)
    with pytest.raises(NomosEnforcementResolutionError):
        deployment_nomos_enforcement_mode("demo")


# --- misconfigured deployments root fails closed (never off) ---------------


def test_nonexistent_deployments_dir_does_not_resolve_off(tmp_path, monkeypatch):
    """A set-but-nonexistent OKG_DEPLOYMENTS_DIR is a misconfiguration, not a
    positive absence signal. Resolution must fail closed (raise) instead of
    silently disabling enforcement -- the pre-fix blanket ``except`` degraded
    this to ``off``. find_deployment_dir raises DeploymentResolutionError here,
    which every enforcement consumer treats as fail-closed (blocker/unavailable),
    never as ``ok``.
    """
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.deployments import DeploymentResolutionError
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    missing_root = tmp_path / "does" / "not" / "exist"
    assert not missing_root.exists()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(missing_root))
    with pytest.raises(DeploymentResolutionError):
        deployment_nomos_enforcement_mode("demo")


# --- adjacent fail-opens: invalid config values are hard errors ------------


def test_invalid_env_override_token_is_hard_error(tmp_path, monkeypatch):
    from okg.substrate.nomos import (
        NomosEnforcementConfigError,
        deployment_nomos_enforcement_mode,
    )

    root = tmp_path / "deployments"
    _enforce_manifest(root)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    # A garbage kill-switch token must not be silently ignored (pre-fix it
    # fell through to the manifest, effectively no-op'ing the override).
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "sometimes")
    with pytest.raises(NomosEnforcementConfigError):
        deployment_nomos_enforcement_mode("demo")


def test_unrecognized_manifest_value_is_hard_error(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import (
        NomosEnforcementConfigError,
        deployment_nomos_enforcement_mode,
    )

    root = tmp_path / "deployments"
    _write_manifest(
        root,
        "demo",
        "name: demo\nnomos:\n  runtime_enforcement: sorta\n",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    with pytest.raises(NomosEnforcementConfigError):
        deployment_nomos_enforcement_mode("demo")


def test_manifest_helper_rejects_unrecognized_value(monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import (
        NomosEnforcementConfigError,
        manifest_nomos_enforcement_mode,
    )

    # No nomos key -> genuine absence -> off.
    assert manifest_nomos_enforcement_mode({"name": "demo"}) == "off"
    assert manifest_nomos_enforcement_mode({"nomos": {}}) == "off"
    # Present-but-bad value -> hard error naming the value.
    with pytest.raises(NomosEnforcementConfigError):
        manifest_nomos_enforcement_mode({"nomos": {"runtime_enforcement": "sorta"}})
