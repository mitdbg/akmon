"""PACT ``concord-failure-taxonomy`` — req.tax.per-error-record.

The funnel computes one row per injected error and the scorecard used to keep
only the counts derived from them, so a STORED run could not be re-analysed.
These witnesses hold the rows to the standard that makes them trustworthy: they
must re-derive every count the scorecard publishes, and a run where they do not
must REFUSE rather than report two different numbers from one run.
"""
from __future__ import annotations

import copy
import json

from pathlib import Path

import pytest

from okg.bench.synth.runner import (
    FUNNEL_ROWS_FILENAME,
    RunnerError,
    assert_rows_rederive_counts,
)
from okg.bench.synth.corruption import PARTITION_CROSS_DOC
from okg.bench.synth.resolution import FUNNEL_STATES

_STATES = {s: 0 for s in FUNNEL_STATES}


def _states(**counts):
    return {**_STATES, **counts}


_ROWS = [
    {"state": "repaired", "partition": PARTITION_CROSS_DOC},
    {"state": "repaired", "partition": PARTITION_CROSS_DOC},
    {"state": "not_detected", "partition": PARTITION_CROSS_DOC},
]
_COUNTS = {
    "total": 3,
    "states": _states(repaired=2, not_detected=1),
    "partition_state": {
        PARTITION_CROSS_DOC: {"not_detected": 1, "repaired": 2}
    },
    "detection_recall": {
        "conditional": {"denominator": 3, "detected": 2},
        "end_to_end": {"denominator": 3, "detected": 2},
    },
    "repair_rate": {
        "conditional": {"denominator": 2, "repaired": 2},
        "end_to_end": {"denominator": 3, "repaired": 2},
    },
}


# --------------------------------------------------------------------------- #
# The consistency rule itself — pure, so the refusal is witnessable with no DB. #
# --------------------------------------------------------------------------- #
def test_consistent_rows_and_counts_are_accepted() -> None:
    assert_rows_rederive_counts(_ROWS, _COUNTS)


def test_a_dropped_row_refuses() -> None:
    with pytest.raises(RunnerError, match="do not re-derive"):
        assert_rows_rederive_counts(_ROWS[:-1], _COUNTS)


def test_an_extra_row_refuses() -> None:
    with pytest.raises(RunnerError, match="do not re-derive"):
        assert_rows_rederive_counts([*_ROWS, _ROWS[0]], _COUNTS)


def test_a_row_in_the_wrong_state_refuses_even_at_the_right_total() -> None:
    """The totals agree and the DISTRIBUTION does not — the case a bare count
    check would wave through."""
    with pytest.raises(RunnerError, match="do not re-derive"):
        assert_rows_rederive_counts(
            [
                _ROWS[0],
                {"state": "mis_repaired", "partition": PARTITION_CROSS_DOC},
                _ROWS[2],
            ],
            _COUNTS,
        )
    for block, field in (
        ("partition_state", PARTITION_CROSS_DOC),
        ("detection_recall", "conditional"),
        ("repair_rate", "end_to_end"),
    ):
        perturbed = copy.deepcopy(_COUNTS)
        cell = perturbed[block][field]
        if block == "partition_state":
            cell["repaired"] -= 1
        elif block == "detection_recall":
            cell["detected"] -= 1
        else:
            cell["repaired"] -= 1
        with pytest.raises(RunnerError, match="do not re-derive"):
            assert_rows_rederive_counts(_ROWS, perturbed)


def test_an_unknown_terminal_state_refuses_by_name() -> None:
    with pytest.raises(RunnerError, match="unknown terminal state"):
        assert_rows_rederive_counts(
            [{"state": "invented", "partition": PARTITION_CROSS_DOC}],
            _COUNTS,
        )


# --------------------------------------------------------------------------- #
# A REAL scored run carries the rows, enriched, and they re-derive its counts.  #
# --------------------------------------------------------------------------- #
@pytest.mark.postgres
def test_a_scored_run_persists_rows_that_rederive_its_own_counts(
    clean_runtime: str, tmp_path: Path, world
) -> None:
    from okg.bench.synth.config import load_preset
    from okg.bench.synth.runner import run_synth_benchmark
    from tests.bench.synth.test_parity_replay import (
        _DEP_DIR, _install_composed_catalog,
    )

    _install_composed_catalog(clean_runtime, tmp_path)
    result = run_synth_benchmark(
        dsn=clean_runtime, deployment_dir=_DEP_DIR, out_dir=tmp_path / "run",
        config=load_preset("easy"),
    )
    funnel = result["scorecard"]["funnel"]
    # BESIDE the scorecard, not inside it: the scorecard carries the published
    # numbers and is golden-pinned byte-for-byte, so the per-error record it is
    # derived from travels as its own artifact.
    rows = result["funnel_rows"]
    assert json.loads(
        (tmp_path / "run" / FUNNEL_ROWS_FILENAME).read_text()
    ) == rows, "the persisted rows differ from the returned rows"

    assert rows, "a scored run published no per-error rows"
    assert "rows" not in funnel, (
        "rows are back inside the scorecard; every golden would have to carry "
        "the per-error record and would be rewritten by any change to it"
    )
    assert len(rows) == funnel["total"]
    assert_rows_rederive_counts(rows, funnel)

    # the two gold-side fields the analysis needs and the funnel row lacks
    for row in rows:
        assert row["layer"] in {"render", "kg_fork"}, row
        assert row["constraint_kind"], row
        assert isinstance(row["mandatory"], bool), row
        assert set(row["realized_vote"]) == {"corrupted", "clean", "other"}, row

    # and the rows are the object the published aggregates are computed from
    from collections import Counter
    by_class = Counter(r["corruption_class"] for r in rows)
    survival = result["scorecard"]["per_class_survival"]["by_class"]
    for name, cell in survival.items():
        assert by_class[name] == cell["sites"], (name, by_class[name], cell)
