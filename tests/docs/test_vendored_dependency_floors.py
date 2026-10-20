"""No-npm dependency floor guard
(refresh-vulnerable-dependencies; adversarial-review remediation).

Pure-stdlib so it runs in the no-DB smoke/fast lanes without a Node toolchain.
Every active owning npm lockfile resolves the reviewed Dependabot packages out
of their patched-floor vulnerable ranges. Legacy frontend lockfiles and vendored
browser bundles were archived under archive/ui/legacy-viz-2026-06-17, which
PACT layout-history-openspec deleted from the tree; they are not active
runtime dependencies.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# lockfile (repo-relative) -> {package: [(vuln_lo_inclusive, fix_version_exclusive), ...]}
# No active npm lockfiles remain after archiving the legacy UI. If a new active
# frontend appears, add its lockfile here instead of reviving archived bundles.
VULN_RANGES = {}

# Documented suppressions keyed on (lockfile, package, range): ONLY the named
# vulnerable RANGE is skipped; every other range for the same package is still
# evaluated. A suppression is allowed only for an alert whose patch is genuinely
# unreachable AND whose exposure does not apply to how the package is used.
SUPPRESSIONS = {}


def _vtuple(version: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", version)[:3])


def _resolved_versions(lockfile: Path, pkg: str) -> list[str]:
    """All resolved versions of `pkg` in an npm lockfile (v1/v2/v3)."""
    data = json.loads(lockfile.read_text())
    versions: list[str] = []
    # lockfileVersion 2/3: `packages` keyed by node_modules paths.
    for key, meta in (data.get("packages") or {}).items():
        if (
            isinstance(meta, dict)
            and meta.get("version")
            and (key == f"node_modules/{pkg}" or key.endswith(f"/node_modules/{pkg}"))
        ):
            versions.append(str(meta["version"]))

    # lockfileVersion 1: nested `dependencies`.
    def _walk(deps: dict) -> None:
        for name, meta in (deps or {}).items():
            if not isinstance(meta, dict):
                continue
            if name == pkg and meta.get("version"):
                versions.append(str(meta["version"]))
            _walk(meta.get("dependencies") or {})

    _walk(data.get("dependencies") or {})
    return versions


_CASES = [
    (lockfile, pkg, lo, fix)
    for lockfile, pkgs in VULN_RANGES.items()
    for pkg, ranges in pkgs.items()
    for (lo, fix) in ranges
]


def _in_range(version: str, lo: str, fix: str) -> bool:
    v = _vtuple(version)
    return _vtuple(lo) <= v < _vtuple(fix)


def test_no_resolved_version_in_vulnerable_range() -> None:
    for lockfile, pkg, lo, fix in _CASES:
        if (lockfile, pkg, (lo, fix)) in SUPPRESSIONS:
            continue
        path = ROOT / lockfile
        assert path.is_file(), f"missing lockfile: {lockfile}"
        versions = _resolved_versions(path, pkg)
        assert versions, f"{pkg} not resolved in {lockfile}"
        bad = [v for v in versions if _in_range(v, lo, fix)]
        assert not bad, (
            f"{pkg} resolves a version in the vulnerable range [{lo}, {fix}) "
            f"in {lockfile}: {bad}"
        )


def test_every_suppression_has_a_written_rationale():
    # A suppression with an empty rationale is a silent waiver -> not allowed.
    for key, rationale in SUPPRESSIONS.items():
        assert rationale and rationale.strip(), f"suppression {key} lacks a rationale"


def test_legacy_vendor_build_tooling_is_not_active() -> None:
    assert not (ROOT / "tools" / "vendor-build").exists()
    assert not (ROOT / "archive").exists()
