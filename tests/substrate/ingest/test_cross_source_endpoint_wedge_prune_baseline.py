"""Synthetic prune-baseline for the cross-source endpoint wedge (task
prune-baseline, decision D1).

SYNTHETIC. This runs on a disposable fixture deployment with a prune dangling
policy and three fixture sources. It is NOT a measurement of okg-workspace or
any real deployment (board #197: no okg-workspace data leaves the laptop).

Source a owns widgets, b and d write ``note references widget`` onto them
(b: 20 + 20 edges, d: 5 + 5 edges, in two disjoint sets). Phase 1 retracts
the first set under the prune policy as it stands (deferral off on a prune
deployment): the gate drops the dangling edges for good. Phase 2 retracts the
second set with deferral switched on for the same deployment: the edges wait.
Each phase restores the widgets afterwards. The test records, per producer
and edge type, what prune dropped, the waiting-table size and the publish
time, and what survives the restore. When ``OKG_WEDGE_PRUNE_OUT`` names a
file the numbers are written there as JSON (pact evidence).
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.pytest import endpoint_wedge_fixture as fx
from tests.pytest.source_jobs_inline import use_inline_source_jobs
from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

pytestmark = pytest.mark.postgres

B_EDGES = 20
D_EDGES = 5


def _widgets(prefix: str, count: int) -> list[dict[str, Any]]:
    return [fx.node(f"widget:{prefix}{i}", "widget") for i in range(count)]


def _pruned_attribution(outcome) -> dict[str, Any]:
    out = {"count": 0, "by_source": {}, "by_classification": []}
    for publish in outcome.report["publishes"]:
        for gate in publish.get("consistency_gates", []):
            details = gate.get("details") or {}
            out["count"] += int(details.get("pruned_dangling_edge_count") or 0)
            attribution = details.get("pruned_dangling_edge_attribution") or {}
            for bucket in (attribution.get("by_source") or {}).get("buckets", []):
                out["by_source"][bucket["source"]] = (
                    out["by_source"].get(bucket["source"], 0)
                    + int(bucket["count"])
                )
            out["by_classification"].extend(
                (attribution.get("by_classification") or {}).get("buckets", []),
            )
    return out


def _waiting_by_producer(dsn: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in fx.waiting_rows(dsn):
        key = f"{row['source']} {row['edge_type']} -> {row['expected_subtype']}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _set_deferral(dsn: str, dep: Path, mode: str) -> None:
    manifest_path = dep / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text())
    manifest["consistency"] = {
        "dangling_edges": "prune",
        "defer_dangling_edges": {"true": True, "false": False}.get(mode, mode),
        "max_deferred_edge_fraction": 1.0,
    }
    manifest_path.write_text(yaml.safe_dump(manifest))
    # A manifest edit changes the claimed identity; claim it again.
    _claim_layered_fixture_catalog(dsn, dep)


def _phase(
    dsn: str, dep: Path, prefix: str, keep: list[dict[str, Any]],
    all_widgets: list[dict[str, Any]], other_set: list[dict[str, Any]],
) -> dict[str, Any]:
    started = time.monotonic()
    fx.write_fixture(dep, "a", [*keep, *other_set], [])
    try:
        retract = fx.ingest(dsn, dep, "a")
    except Exception as exc:  # noqa: BLE001 - name the guard's data
        raise AssertionError(
            f"{exc}: {getattr(exc, 'data', None)}"
        ) from exc
    publish_seconds = round(time.monotonic() - started, 3)
    dangling_set = {f"widget:{prefix}{i}" for i in range(B_EDGES + D_EDGES)}
    retracted = {
        "publish_seconds": publish_seconds,
        "publish_status": [p["status"] for p in retract.report["publishes"]],
        "pruned": _pruned_attribution(retract),
        "waiting_rows": len(fx.waiting_rows(dsn)),
        "waiting_by_producer": _waiting_by_producer(dsn),
        "edges_in_graph_for_set": len([
            t for t in fx.edge_triples(dsn) if t[2] in dangling_set
        ]),
    }
    fx.write_fixture(dep, "a", [*keep, *all_widgets, *other_set], [])
    fx.ingest(dsn, dep, "a")
    restored = {
        "waiting_rows": len(fx.waiting_rows(dsn)),
        "edges_in_graph_for_set": len([
            t for t in fx.edge_triples(dsn) if t[2] in dangling_set
        ]),
    }
    return {"after_retraction": retracted, "after_restore": restored}


def test_synthetic_prune_baseline(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    use_inline_source_jobs(monkeypatch)
    dep = fx.build_deployment(
        tmp_path, clean_runtime,
        consistency={"dangling_edges": "prune",
                     "max_deferred_edge_fraction": 1.0},
    )
    set1 = _widgets("s1-", B_EDGES + D_EDGES)
    set2 = _widgets("s2-", B_EDGES + D_EDGES)
    keep = [fx.node("widget:keep", "widget")]

    def notes_and_edges(prefix: str, tag: str):
        nodes = [fx.node(f"note:{tag}{i}", "note") for i in range(B_EDGES)]
        edges = [
            fx.edge(f"note:{tag}{i}", f"widget:{prefix}{i}")
            for i in range(B_EDGES)
        ]
        return nodes, edges

    b1_nodes, b1_edges = notes_and_edges("s1-", "b1-")
    b2_nodes, b2_edges = notes_and_edges("s2-", "b2-")
    fx.write_fixture(
        dep, "b", [*b1_nodes, *b2_nodes], [*b1_edges, *b2_edges],
    )
    d_nodes = [fx.node(f"note:d{i}", "note") for i in range(2 * D_EDGES)]
    d_edges = [
        fx.edge(f"note:d{i}", f"widget:s1-{B_EDGES + i}")
        for i in range(D_EDGES)
    ] + [
        fx.edge(f"note:d{D_EDGES + i}", f"widget:s2-{B_EDGES + i}")
        for i in range(D_EDGES)
    ]
    fx.write_fixture(dep, "d", d_nodes, d_edges)
    fx.write_fixture(dep, "a", [*keep, *set1, *set2], [])
    first = fx.ingest(clean_runtime, dep, "a")
    assert first.any_failed is False, first.report
    assert [p["status"] for p in first.report["publishes"]] == ["published"]
    for source in ("b", "d"):
        run = fx.ingest(clean_runtime, dep, source)
        assert run.any_failed is False, run.report
        assert [p["status"] for p in run.report["publishes"]] == [
            "published"
        ], (source, run.report["publishes"])
    assert len(fx.edge_triples(clean_runtime)) == 2 * (B_EDGES + D_EDGES)

    # Phase 1: prune policy as it stands (deferral off on a prune deployment).
    _set_deferral(clean_runtime, dep, "auto")
    prune_today = _phase(clean_runtime, dep, "s1-", keep, set1, set2)
    # Phase 2: the same deployment with deferral switched on.
    _set_deferral(clean_runtime, dep, "true")
    with_deferral = _phase(clean_runtime, dep, "s2-", keep, set2, set1)

    today = prune_today["after_retraction"]
    assert today["pruned"]["count"] == B_EDGES + D_EDGES
    assert today["pruned"]["by_source"] == {
        "svc.b": B_EDGES, "svc.d": D_EDGES,
    }
    assert today["waiting_rows"] == 0
    assert prune_today["after_restore"]["edges_in_graph_for_set"] == 0

    deferred = with_deferral["after_retraction"]
    assert deferred["pruned"]["count"] == 0
    assert deferred["waiting_rows"] == B_EDGES + D_EDGES
    assert deferred["publish_status"] == ["published"]
    assert with_deferral["after_restore"]["waiting_rows"] == 0
    assert (
        with_deferral["after_restore"]["edges_in_graph_for_set"]
        == B_EDGES + D_EDGES
    )

    out = os.environ.get("OKG_WEDGE_PRUNE_OUT")
    if out:
        Path(out).write_text(
            json.dumps({
                "schema": "okg.cross_source_endpoint_wedge.prune_baseline.v1",
                "synthetic": True,
                "provenance": (
                    "SYNTHETIC: a disposable fixture deployment on a lab host "
                    "(database okg_test_lanesm_k1_prune), dangling_edges: "
                    "prune. NOT okg-workspace and not a real deployment; "
                    "board #197 kept all okg-workspace data off the lab host."
                ),
                "what_it_measures": (
                    "Edges whose target another source retracts, in known "
                    "amounts: b 20 and d 5 per set. Prune as it stands drops "
                    "them; with deferral they wait and come back when the "
                    "target returns."
                ),
                "known_amounts": {"svc.b": B_EDGES, "svc.d": D_EDGES},
                "prune_today": prune_today,
                "with_deferral": with_deferral,
                "limits": (
                    "Counts and timings come from 25 edges per phase on a "
                    "small graph; the publish times show no scale effect and "
                    "say nothing about okg-workspace. It sizes the change "
                    "(dropped becomes held and recoverable), not the real "
                    "volume."
                ),
            }, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
