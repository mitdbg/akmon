"""Does naive corroboration repair to the LIE when the lie holds the majority?

claim-corroboration-signal made repair fire in production and recorded an
unmitigated risk: corroboration is NAIVE, so copies count as independent
support. A value stated by two documents beats a correct original stated once,
whether or not those two documents are independent.

Until now that risk was unmeasurable. In every preset the benchmark shipped, the
CLEAN value holds the majority, so whatever corroboration settles on is correct
by construction and ``mis_repaired`` is pinned at zero. A metric that can only
read zero is not evidence of safety.

These two arms are the measurement. They share a world and a seed and differ in
exactly one field -- ``visibility_corrupted`` -- so the comparison isolates the
majority direction:

  majority_wrong   corrupted value in 2 of 3 renderings -> the lie has support 2
  majority_clean   corrupted value in 1 of 3 renderings -> the truth has support 2

Each arm runs against its own database. Running both in one would let the first
arm's claims merge into the second's ingest (same world, same document names)
and quietly contaminate the comparison.
"""

from __future__ import annotations

from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import load_preset  # noqa: E402
from okg.bench.synth.runner import run_synth_benchmark  # noqa: E402
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, compose_catalog(d))
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _repair(dsn: str, tmp_path: Path, preset: str) -> dict:
    _install_composed_catalog(dsn, tmp_path)
    out = run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run", preset=preset
    )
    return out["scorecard"]["repair"]


def test_the_two_arms_differ_only_in_visibility() -> None:
    """The comparison is only meaningful if nothing else moves. Pinned as a
    pure test so a future edit to one preset cannot silently make the arms
    incomparable while both still 'pass'."""
    wrong = load_preset("majority_wrong")
    clean = load_preset("majority_clean")

    assert wrong.seed == clean.seed
    assert wrong.world_path == clean.world_path
    assert wrong.scale == clean.scale
    assert wrong.redundancy == clean.redundancy
    assert set(wrong.corruption.classes) == set(clean.corruption.classes)

    for name, spec in wrong.corruption.classes.items():
        other = clean.corruption.classes[name]
        assert spec.count == other.count
        assert spec.magnitude == other.magnitude
        # the ONE difference, and it points the right way
        assert spec.visibility_corrupted > other.visibility_corrupted


def test_majority_wrong_arm_repairs_to_the_lie(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The risk, demonstrated rather than argued.

    A non-zero mis_repaired here is the whole point: it is what makes
    mis_repaired a measurement instead of a field that structurally reads 0.
    """
    repair = _repair(clean_runtime, tmp_path, "majority_wrong")
    assert repair["mis_repaired"] > 0, (
        "the wrong-majority arm produced no mis-repair; either no bucket "
        f"settled or the arm is not realizing majority-wrong sites: {repair}"
    )
    assert sum(
        repair[s] for s in
        ("repaired", "mis_repaired", "detected_only", "unadjudicated")
    ) == repair["total"]


def test_majority_clean_arm_repairs_to_the_truth(
    clean_runtime: str, tmp_path: Path
) -> None:
    """The control. Same world, same seed, majority flipped to the clean value:
    corroboration must now settle correctly. Without this arm a non-zero
    mis_repaired next door would not show the metric DISCRIMINATES -- it could
    just be a scorer that calls everything wrong."""
    repair = _repair(clean_runtime, tmp_path, "majority_clean")
    assert repair["mis_repaired"] == 0, (
        f"clean majority must not mis-repair: {repair}"
    )
    assert repair["repaired"] > 0, (
        "the clean-majority arm settled nothing, so it does not establish that "
        f"the scorer can read 'repaired' at all: {repair}"
    )
