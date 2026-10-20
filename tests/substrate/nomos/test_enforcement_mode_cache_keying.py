"""Regression guard: the enforcement-mode memo keys on the deployments root.

Before the fix, ``deployment_nomos_enforcement_mode`` cached the resolved
deployment dir and mode by deployment NAME only, ignoring ``OKG_DEPLOYMENTS_DIR``.
A second deployment of the same name under a different root then resolved the
first root's stale dir -- the cross-test ordering pollution that flaked
``test_deployment_nomos_enforcement_mode_is_tristate`` in batch runs, and a
latent production hazard when a process resolves the same name against different
roots. This exercises that path directly within a single test, so it fails on
the name-only key regardless of the autouse cache-clearing fixture.
"""

from __future__ import annotations

import os
from pathlib import Path


def _write_demo_manifest_at(root: Path, *, mode: str) -> None:
    deployment = root / "demo"
    deployment.mkdir(parents=True, exist_ok=True)
    (deployment / "deployment.yaml").write_text(
        "name: demo\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        f"nomos:\n  runtime_enforcement: {mode}\n",
        encoding="utf-8",
    )


def test_enforcement_mode_cache_keys_on_deployments_root(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root_a = tmp_path / "a" / "deployments"
    root_b = tmp_path / "b" / "deployments"
    _write_demo_manifest_at(root_a, mode="enforce")
    _write_demo_manifest_at(root_b, mode="off")

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root_a))
    assert deployment_nomos_enforcement_mode("demo") == "enforce"

    # Same name, different root: must NOT serve root_a's cached "enforce".
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root_b))
    assert deployment_nomos_enforcement_mode("demo") == "off"

    # And switching back still resolves root_a independently.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root_a))
    assert deployment_nomos_enforcement_mode("demo") == "enforce"


def test_enforcement_mode_cache_invalidates_when_mtime_is_reused(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    from okg.substrate.nomos import deployment_nomos_enforcement_mode

    root = tmp_path / "deployments"
    manifest = root / "demo" / "deployment.yaml"
    fixed_ns = 1_700_000_000_000_000_000

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    _write_demo_manifest_at(root, mode="off")
    os.utime(manifest, ns=(fixed_ns, fixed_ns))
    assert deployment_nomos_enforcement_mode("demo") == "off"

    _write_demo_manifest_at(root, mode="audit")
    os.utime(manifest, ns=(fixed_ns, fixed_ns))
    assert deployment_nomos_enforcement_mode("demo") == "audit"
