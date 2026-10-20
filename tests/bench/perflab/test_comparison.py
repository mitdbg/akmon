"""Unit tests for the comparison-report grouping/ordering helpers
(bench-okg-internals task.comparison-report). The marginal-cost attribution
itself is proven by a real >=2-config sweep; these lock the swept-axis
detection + baseline ordering."""
from __future__ import annotations

import okg.bench.orchestrator as orch


def _exp(cid, **cfg):
    return orch.Experiment(
        id=cid,
        repo=orch.Repo("tinydb", "https://example/tinydb.git"),
        source_set="files+code",
        replay=orch.Replay(strategy="first_n", n=5),
        config=orch.BenchConfig(**cfg),
    )


def test_swept_axes_detects_single_axis():
    cells = [_exp("a", enrichers="none"), _exp("b", enrichers="all")]
    assert orch._swept_axes(cells) == ["enrichers"]


def test_swept_axes_detects_chronos():
    cells = [_exp("a", chronos_continuation_pct=50),
             _exp("b", chronos_continuation_pct=95)]
    assert orch._swept_axes(cells) == ["chronos_continuation_pct"]


def test_swept_axes_empty_when_identical():
    cells = [_exp("a", enrichers="all"), _exp("b", enrichers="all")]
    assert orch._swept_axes(cells) == []


def test_enricher_baseline_orders_none_first():
    vals = ["all", "none", "declarative", "code"]
    ordered = sorted(vals, key=lambda v: orch._axis_sort_key("enrichers", v))
    assert ordered[0] == "none"
    assert ordered[-1] == "all"


def test_numeric_axis_orders_ascending():
    vals = [95, 50, 75]
    ordered = sorted(
        vals, key=lambda v: orch._axis_sort_key("chronos_continuation_pct", v))
    assert ordered == [50, 75, 95]
