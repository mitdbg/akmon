"""task.arms.emit (+ the published half of task.arms.ordering): a REAL publish
per arm, read from the published bucket_resolution rows.

The all-signals arm settles a bucket on each of the FIVE decisive bases — four
via ingest stamps (operator_precedent, explicit_supersession,
claim_local_effective_date, declared_source_authority) and the fifth
(dependency_adjusted_support) from a corroboration majority — and the basis
histogram over the published generation carries all five with non-zero count,
read from the rows and NEVER from the config that requested them. The paired
NEGATIVE arm (same world + seed, signals withheld) leaves the isolation buckets
CONTESTED, proving each basis DEPENDS on its signal rather than co-occurring with
it. The disagreement buckets additionally show the higher-priority signal winning
on the published rows (the published half of the ordering proof).
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth.config import ADMITTABLE_SIGNALS  # noqa: E402
from okg.bench.synth.resolution import OUTCOME_CANONICAL, OUTCOME_CONTESTED  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    detection_state,
    resolutions_at,
    run_synth_benchmark,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402
from tests.bench.synth._precedence_arm_support import (  # noqa: E402
    DEP_DIR,
    all_signals_config,
    build_arm_manifest,
    default_arm_config,
)

pytestmark = pytest.mark.postgres


def _install_catalog(dsn: str, tmp_path: Path) -> None:
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
    apply_explicit_test_catalog(dsn, DEP_DIR)


def _run(dsn: str, tmp_path: Path, config):
    _install_catalog(dsn, tmp_path)
    result = run_synth_benchmark(
        dsn=dsn, deployment_dir=DEP_DIR, out_dir=tmp_path / "run", config=config
    )
    assert result["corrupted"]["status"] == "published", "needs a REAL generation"
    gen = result["corrupted"]["generation_id"]
    state = detection_state(dsn, gen)
    resolutions = resolutions_at(dsn, gen, state.claims)
    return result, {r.bucket: r for r in resolutions}, resolutions


def test_arm_settles_all_five_decisive_bases(clean_runtime: str, tmp_path: Path) -> None:
    dsn = clean_runtime
    result, by_bucket, resolutions = _run(dsn, tmp_path, all_signals_config())
    plan = result["manifest"]["precedence_arm"]["plan"]

    assert resolutions, "the arm corpus must produce bucket_resolution rows"

    # every sealed target is settled on its expected basis, with the expected
    # winner value — read from the PUBLISHED row, checked against SEALED gold.
    for target in plan["targets"]:
        row = by_bucket.get(target["bucket"])
        assert row is not None, f"{target['role']}: no published row for its bucket"
        assert row.outcome == OUTCOME_CANONICAL, (
            f"{target['role']} ({target['bucket']}): expected canonical, got "
            f"{row.outcome}/{row.basis}"
        )
        assert row.basis == target["expected_basis"], (
            f"{target['role']} ({target['bucket']}): basis {row.basis} != sealed "
            f"{target['expected_basis']}"
        )
        assert row.winner_value == target["expected_winner_value"], (
            f"{target['role']} ({target['bucket']}): winner value "
            f"{row.winner_value!r} != sealed {target['expected_winner_value']!r}"
        )

    # THE BASIS HISTOGRAM over the published generation carries all five decisive
    # bases with non-zero count — read from the rows, not from config.
    histogram = Counter(
        r.basis for r in resolutions if r.outcome == OUTCOME_CANONICAL
    )
    for basis in ADMITTABLE_SIGNALS:
        assert histogram[basis] >= 1, (
            f"decisive basis {basis!r} never appeared in the published generation: "
            f"{dict(histogram)}"
        )


def test_negative_arm_leaves_isolation_buckets_contested(
    clean_runtime: str, tmp_path: Path
) -> None:
    """Same world + seed, all signals WITHHELD (default arm): each 1-vs-1
    isolation bucket the arm would have stamped stays CONTESTED, so the basis in
    the arm run is proven to depend on the stamp, not to co-occur with it."""
    dsn = clean_runtime
    # the buckets the all-signals arm targets for single-signal isolation
    arm_plan = build_arm_manifest(all_signals_config())["precedence_arm"]["plan"]
    isolation_buckets = {
        t["bucket"]: t["expected_basis"]
        for t in arm_plan["targets"]
        if t["role"].startswith("isolate:")
        and t["expected_basis"] != "dependency_adjusted_support"
    }
    assert len(isolation_buckets) == 4, "expected 4 stamp-isolated buckets"

    _result, by_bucket, resolutions = _run(dsn, tmp_path, default_arm_config())
    assert resolutions, "the negative corpus must still produce resolution rows"

    for bucket, basis in isolation_buckets.items():
        row = by_bucket.get(bucket)
        assert row is not None, f"{bucket}: no published row in the negative arm"
        assert row.outcome == OUTCOME_CONTESTED, (
            f"{bucket}: withholding {basis} must leave it CONTESTED, got "
            f"{row.outcome}/{row.basis} — the basis does NOT depend on the signal"
        )

    # non-vacuity of the negative run: it still settles the majority buckets on
    # corroboration (so this is a working publish, not a dead one).
    settled = [r for r in resolutions if r.outcome == OUTCOME_CANONICAL]
    assert any(r.basis == "dependency_adjusted_support" for r in settled), (
        "the negative arm settled nothing on corroboration — the run is not "
        "exercising the engine at all"
    )
