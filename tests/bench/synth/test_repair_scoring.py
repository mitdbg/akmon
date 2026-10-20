"""The scorecard reports whether a repair was RIGHT, not just whether the
conflict was noticed.

Detection and repair are independent questions and the benchmark only answered
the first. ``resolution.adjudicate()`` already answered the second correctly but
was called from a test rather than the scorer, so no run reported it — the
398 -> 378+20 figure behind claim-corroboration-signal had to be recomputed by
hand in a scratch script, which is not a durable measurement.

Two things are pinned here:

1. EXHAUSTIVENESS - the four states cover the corruption set exactly. A
   corruption whose bucket has no resolution row is ``unadjudicated``, counted
   separately so it is never silently folded into "not repaired".
2. DELEGATION - the scorer calls the same adjudicate() the witnesses call. A
   second implementation in the runner would agree today and drift later, and
   the drift would be invisible because both sides would be "passing".
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

import yaml  # noqa: E402

from okg.bench.synth import runner as synth_runner  # noqa: E402
from okg.bench.synth.runner import (  # noqa: E402
    run_synth_benchmark,
    synth_value_equivalent,
)
from okg.substrate.catalog.modules_apply import apply_composed_catalog  # noqa: E402
from okg.substrate.catalog.modules_compose import compose_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEP_DIR = _REPO_ROOT / "deployments" / "synthetic-corpus-smoke"
_STATES = ("repaired", "mis_repaired", "detected_only", "unadjudicated")

# The `clean_runtime` fixture resolves its DSN from OKG_TEST_DSN *or*
# OKG_TEST_DSN_TEMPLATE (tests/substrate/conftest.py:212-215). A skipif keyed to
# OKG_TEST_DSN alone therefore skipped these three tests even inside a fully
# provisioned Postgres lane, because CI sets only OKG_TEST_DSN_TEMPLATE
# (.github/workflows/test.yml env). Key the guard to the same pair the fixture
# reads, so "no DB configured" is the only thing that skips.
_NO_DB = not (
    os.environ.get("OKG_TEST_DSN") or os.environ.get("OKG_TEST_DSN_TEMPLATE")
)


def _install_composed_catalog(dsn: str, tmp_path: Path) -> None:
    """Without the composed ontology the publish blocks on node_subtype
    narrowing, so the run never reaches scoring."""
    d = tmp_path / "catalog-scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "scratch", "schema_dir": "schemas",
        "modules": ["extraction", "consistency"],
    }))
    (d / "schemas").mkdir()
    composed = compose_catalog(d)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    from tests.pytest.catalog_fixture_builders import apply_explicit_test_catalog
    apply_explicit_test_catalog(dsn, _DEP_DIR)


def _run(dsn: str, tmp_path: Path):
    _install_composed_catalog(dsn, tmp_path)
    return run_synth_benchmark(
        dsn=dsn, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run", preset="smoke"
    )


@pytest.mark.skipif(_NO_DB, reason="needs a DB")
def test_repair_tally_is_exhaustive_over_the_corruption_set(
    clean_runtime: str, tmp_path: Path
) -> None:
    repair = _run(clean_runtime, tmp_path)["scorecard"]["repair"]

    for state in _STATES:
        assert state in repair, f"scorecard is missing repair state {state!r}"
    assert sum(repair[s] for s in _STATES) == repair["total"], repair

    # Non-vacuity: the smoke corpus renders some facts once, and a corruption on
    # a singly-rendered fact conflicts with nothing, so the engine emits no
    # resolution row for it. If that bucket were folded into "not repaired" the
    # scorecard would understate repair against a denominator it never had.
    assert repair["unadjudicated"] > 0, (
        "expected singly-rendered corruptions to be unadjudicated; a zero here "
        "means they are being silently absorbed into another state"
    )
    # detected and repaired are INDEPENDENT: a contested bucket is detected and
    # not repaired, so detection must not be inferred from the repair states.
    assert repair["detected"] >= repair["repaired"] + repair["detected_only"]


@pytest.mark.skipif(_NO_DB, reason="needs a DB")
def test_scorecard_delegates_to_adjudicate_rather_than_restating_it(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the runner recomputed the tally instead of delegating, patching
    adjudicate() would not change the scorecard and this test would fail.

    That is the whole point: the scorer and the falsification witnesses must
    read the SAME state machine, or they can diverge while both look green.
    """
    sentinel = {
        "per_corruption": {},
        "tally": {"repaired": 41, "mis_repaired": 0,
                  "detected_only": 0, "unadjudicated": 0},
        "detected": 7,
        "total": 41,
    }
    monkeypatch.setattr(synth_runner, "adjudicate", lambda **_: sentinel)

    repair = _run(clean_runtime, tmp_path)["scorecard"]["repair"]
    assert repair["repaired"] == 41 and repair["total"] == 41, (
        "the scorecard did not reflect the patched adjudicate(); the runner is "
        "restating the repair state machine instead of delegating to it"
    )


@pytest.mark.skipif(_NO_DB, reason="needs a DB")
def test_non_exhaustive_tally_fails_loudly(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exhaustiveness check must have teeth. A tally that does not cover the
    corruption set means a corruption fell out of the state machine, and a
    scorecard that reports it anyway would understate every rate."""
    monkeypatch.setattr(
        synth_runner,
        "adjudicate",
        lambda **_: {
            "per_corruption": {},
            "tally": {"repaired": 1, "mis_repaired": 0,
                      "detected_only": 0, "unadjudicated": 0},
            "detected": 0,
            "total": 9,  # 1 != 9
        },
    )
    with pytest.raises(synth_runner.RunnerError, match="not exhaustive"):
        _run(clean_runtime, tmp_path)


def test_declared_equivalence_folds_case_and_space_only() -> None:
    """Repair correctness turns on this rule, so it is declared in one place
    rather than left implicit. Anything looser would score a wrong repair as
    right."""
    assert synth_value_equivalent("Active", " active ")
    assert synth_value_equivalent("72  kg", "72 kg")
    assert not synth_value_equivalent("72 kg", "72kg")
    assert not synth_value_equivalent("4.2", "4.20")
