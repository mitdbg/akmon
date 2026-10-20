"""Determinism of the manifest fingerprint (#828).

The fingerprint must be a pure function of file bytes in EVERY process.
Proven divergence pre-fix: a YAML ``!!set`` value in a behavior-bearing
block rendered via ``str`` with hash-randomized iteration order, so the
same committed manifest fingerprinted differently per process (six runs,
six fingerprints). Mixed-type mapping keys crashed with a raw TypeError.

These tests are deliberately NOT postgres-marked: they must run in every
lane, and a skipped determinism proof is no proof.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from okg.substrate.catalog.manifest_identity import (
    ManifestFingerprintError,
    manifest_fingerprint_for,
)

# Fixture covering every rendering class `default=str` historically
# handled: bare YAML dates and timestamps, nested mappings, mixed scalar
# lists, a partial `runtime` block, and a sibling registry file hashed by
# content. Its fingerprint below was computed AGAINST PRE-#828 CODE, so a
# change to this literal means the fix re-fingerprinted the fleet and
# every armed deployment would refuse on upgrade. Do not update the
# literal to make the test pass.
_PINNED_MANIFEST = """\
name: pinfix
modules: []
sync_root: /tmp/elsewhere
enrichers:
  - class: okg.demo.EnricherA
    config:
      weight: 1
      since: 2026-07-01
      window:
        start: 2026-07-01 10:00:00
        nested:
          deep: [1, 2.5, true, null, "s"]
maintained_derivations:
  producers:
    - producer_one
runtime:
  enabled: true
  workflows:
    publish_interval_seconds: 30
  source_sync:
    poll:
      enabled: true
  pools:
    ignored_ops_tuning: 4
extractor:
  module: okg.demo.extractor
viz:
  theme: cool
"""

_PINNED_REGISTRY = """\
sources:
  demo:
    kind: git
    path: .
"""

_PINNED_FINGERPRINT = (
    "sha256:85cb727da9c6b911185edd235e10af7c8b4f6c7437866cef8bc2d5588b8a9575"
)

_SET_MANIFEST = """\
name: setfix
modules: []
enrichers:
  - class: okg.demo.EnricherA
    config:
      kinds: !!set
        alpha: null
        beta: null
        gamma: null
        delta: null
        epsilon: null
        zeta: null
"""

_MIXED_KEY_MANIFEST = """\
name: mixedfix
modules: []
extractor:
  routes:
    1: numeric
    named: string
"""


def _write_dep(tmp_path: Path, manifest: str, registry: str | None = None) -> Path:
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(manifest)
    if registry is not None:
        (dep / "source_registry.yaml").write_text(registry)
    return dep


def test_pinned_fingerprint_unchanged(tmp_path: Path) -> None:
    """Set-free manifests keep their exact pre-#828 fingerprint."""
    dep = _write_dep(tmp_path, _PINNED_MANIFEST, _PINNED_REGISTRY)
    fp, blocks = manifest_fingerprint_for(dep)
    assert fp == _PINNED_FINGERPRINT
    assert set(blocks) == {
        "enrichers", "extractor", "maintained_derivations", "runtime",
        "source_registry", "sync_root",
    }


def test_set_value_is_deterministic_across_processes(tmp_path: Path) -> None:
    """A ``!!set`` value fingerprints identically under different hash
    seeds (pre-fix: per-process random order via ``str(set)``)."""
    dep = _write_dep(tmp_path, _SET_MANIFEST)
    script = (
        "from okg.substrate.catalog.manifest_identity import "
        "manifest_fingerprint_for; "
        f"print(manifest_fingerprint_for({str(dep)!r})[0])"
    )
    outputs = []
    for seed in ("1", "2"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        proc = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        outputs.append(proc.stdout.strip())
    assert outputs[0] == outputs[1]
    assert outputs[0].startswith("sha256:")
    # And the in-process value agrees with both subprocesses.
    assert manifest_fingerprint_for(dep)[0] == outputs[0]


def test_set_of_mixed_scalars_does_not_crash(tmp_path: Path) -> None:
    """Set elements of mixed types order by canonical serialization, not
    native comparison (naive sorted({1, 'a'}) would raise TypeError)."""
    dep = _write_dep(
        tmp_path,
        """\
name: mixedset
modules: []
enrichers:
  - config:
      kinds: !!set
        1: null
        a: null
        2.5: null
""",
    )
    fp_a, _ = manifest_fingerprint_for(dep)
    fp_b, _ = manifest_fingerprint_for(dep)
    assert fp_a == fp_b
    assert fp_a.startswith("sha256:")


def test_mixed_type_mapping_keys_raise_typed_error(tmp_path: Path) -> None:
    """Unserializable blocks fail loudly with the module's typed error,
    never a raw TypeError escaping json.dumps."""
    dep = _write_dep(tmp_path, _MIXED_KEY_MANIFEST)
    with pytest.raises(ManifestFingerprintError):
        manifest_fingerprint_for(dep)


def test_publisher_capacity_settings_do_not_change_deployment_identity(
    tmp_path: Path,
) -> None:
    """Issue #1479 classified ``publisher`` OUT. Prove it, do not assert it.

    The census test only checks that the key is classified somewhere. This
    checks the ruling it encodes: publish capacity and warning policy bound
    how long a publish may take and when it warns, never which rows it
    publishes, so raising the statement timeout or the warning budget must
    leave the deployment fingerprint alone. Were it IN, an operator raising a
    timeout for one cold rebuild would change the deployment's identity and
    refuse the next branch flip.
    """
    base_dir = tmp_path / "a"
    tuned_dir = tmp_path / "b"
    base_dir.mkdir()
    tuned_dir.mkdir()
    baseline = _write_dep(base_dir, _PINNED_MANIFEST, _PINNED_REGISTRY)
    tuned = _write_dep(
        tuned_dir,
        _PINNED_MANIFEST
        + """
publisher:
  kickoff_interval: "90s"
  hard_statement_timeout_seconds: 7200
  lock_hold_headroom_factor: 4.0
  allow_raised_statement_timeout: true
  first_publish_warning_budget:
    node_count: 2330000
    edge_count: 3650000
""",
        _PINNED_REGISTRY,
    )
    baseline_fp, baseline_blocks = manifest_fingerprint_for(baseline)
    tuned_fp, tuned_blocks = manifest_fingerprint_for(tuned)
    assert tuned_fp == baseline_fp
    # The block must not even be hashed, so the equality above cannot be an
    # accident of two blocks hashing alike.
    assert "publisher" not in tuned_blocks
    assert set(tuned_blocks) == set(baseline_blocks)
