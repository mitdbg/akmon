"""Unit tests for SuccessConditionLinker._resolve_edges.

Uses a hand-rolled fake cursor that returns pre-canned rows for the
SQL queries the linker issues. This isolates the dispatch / parse /
predicate-evaluation logic from the DB without spinning up Postgres;
the DB-backed end-to-end is covered by the integration test in
Phase 8.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch


from okg.substrate.library.linkers.success_condition import (
    SuccessConditionLinker,
)


@dataclass
class _FakeCursor:
    """Minimal psycopg-cursor stand-in.

    For each `cur.execute(sql, params)` the cursor pops the next
    expected (matcher, rows) tuple from `_steps`. The matcher is a
    substring that must appear in the SQL text.
    """

    steps: list[tuple[str, list[tuple]]] = field(default_factory=list)
    _last_rows: list[tuple] = field(default_factory=list)

    def execute(self, sql: str, params: Any = None) -> None:
        del params
        if not self.steps:
            raise AssertionError(
                f"unexpected SQL after script exhausted: {sql!r}"
            )
        matcher, rows = self.steps.pop(0)
        if matcher not in sql:
            raise AssertionError(
                f"SQL mismatch: expected substring {matcher!r}, "
                f"got {sql!r}"
            )
        self._last_rows = list(rows)

    def fetchall(self) -> list[tuple]:
        return list(self._last_rows)


def _resolve(linker: SuccessConditionLinker, steps: list) -> list:
    cur = _FakeCursor(steps=list(steps))
    with patch(
        "okg.substrate.library.linkers.success_condition._chronos.query",
        side_effect=_fake_chronos_query,
    ):
        return linker._resolve_edges(cur, skip_existing=True)


def _fake_chronos_query(
    cur: _FakeCursor,
    sql: str,
    params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    rows = cur.fetchall()
    if "subtype = 'experiment'" in sql:
        return [
            {"node_id": node_id, "attrs": attrs}
            for node_id, attrs in rows
        ]
    if "edge_type = 'tests'" in sql:
        return [{"src": src, "dst": dst} for src, dst in rows]
    if "attrs->>'experiment_ref'" in sql:
        return [
            {"node_id": node_id, "attrs": attrs}
            for node_id, attrs in rows
        ]
    if "edge_type = 'contains'" in sql:
        return [
            {
                "experiment_id": experiment_id,
                "result_id": result_id,
                "result_attrs": result_attrs,
            }
            for experiment_id, result_id, result_attrs in rows
        ]
    if "edge_id = ANY(:edge_ids)" in sql:
        return [
            {"src": src, "edge_type": edge_type, "dst": dst}
            for src, edge_type, dst in rows
        ]
    raise AssertionError(f"unmapped fake Chronos query: {sql!r}")


# ---------- happy path ----------


def test_emits_supports_when_predicate_satisfied() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        # 1. fetch experiments
        ("subtype = 'experiment'", [
            ("exp:k", {
                "success_condition": sc,
                "experiment_id": "exp:k",
            }),
        ]),
        # 2. fetch tests edges
        ("edge_type = 'tests'", [
            ("exp:k", "hyp:k"),
        ]),
        # 3. fetch results by attribute
        ("attrs->>'experiment_ref'", [
            ("result:r1", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.42,
            }),
        ]),
        # 4. fetch results via trial chain
        ("edge_type = 'contains'", []),
        # 5. existing edges
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert len(edges) == 1
    src, edge_type, dst, attrs = edges[0]
    assert src == "result:r1"
    assert edge_type == "supports"
    assert dst == "hyp:k"
    assert attrs["metric"] == "speedup"
    assert attrs["predicate"] == ">= 1.0"
    assert attrs["metric_value"] == 1.42
    assert attrs["experiment_id"] == "exp:k"


def test_emits_contradicts_when_predicate_fails() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:r2", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 0.87,
            }),
        ]),
        ("edge_type = 'contains'", []),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert len(edges) == 1
    assert edges[0][1] == "contradicts"


def test_emits_via_trial_chain() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "accuracy", "predicate": ">= 0.9"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:t", {"success_condition": sc, "experiment_id": "exp:t"}),
        ]),
        ("edge_type = 'tests'", [("exp:t", "hyp:t")]),
        # No direct experiment_ref attr; results come via trial chain
        ("attrs->>'experiment_ref'", []),
        ("edge_type = 'contains'", [
            ("exp:t", "result:r3", {
                "metric_name": "accuracy",
                "metric_value": 0.95,
            }),
        ]),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert len(edges) == 1
    assert edges[0][0] == "result:r3"
    assert edges[0][1] == "supports"


def test_metric_name_must_match() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:other", {
                "experiment_ref": "exp:k",
                "metric_name": "latency_ms",  # not the one in success_condition
                "metric_value": 10.0,
            }),
        ]),
        ("edge_type = 'contains'", []),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert edges == []


def test_skip_existing_outcome_edges() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:r1", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.42,
            }),
        ]),
        ("edge_type = 'contains'", []),
        # Existing supports edge already present.
        ("edge_id = ANY(:edge_ids)", [
            ("result:r1", "supports", "hyp:k"),
        ]),
    ]
    edges = _resolve(linker, steps)
    assert edges == []


def test_between_predicate_supports_inclusive_bounds() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({
        "metric": "speedup",
        "predicate": "between 0.9 and 1.1",
    })
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:lo", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 0.9,
            }),
            ("result:mid", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.0,
            }),
            ("result:hi", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.1,
            }),
            ("result:over", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.5,
            }),
        ]),
        ("edge_type = 'contains'", []),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    supports = sorted(e[0] for e in edges if e[1] == "supports")
    contradicts = sorted(e[0] for e in edges if e[1] == "contradicts")
    assert supports == ["result:hi", "result:lo", "result:mid"]
    assert contradicts == ["result:over"]


def test_experiment_without_tests_edge_skipped() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", []),  # no tests edge
    ]
    edges = _resolve(linker, steps)
    assert edges == []


def test_stale_unparseable_predicate_skipped_silently() -> None:
    """If source-side validation failed to catch a bad predicate (data
    drift, or a hand-edited node), the linker skips that experiment
    rather than crashing the publish cycle."""
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": "GARBAGE !!"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:r1", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": 1.0,
            }),
        ]),
        ("edge_type = 'contains'", []),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert edges == []


def test_non_numeric_metric_value_skipped() -> None:
    linker = SuccessConditionLinker()
    sc = json.dumps({"metric": "speedup", "predicate": ">= 1.0"})
    steps = [
        ("subtype = 'experiment'", [
            ("exp:k", {"success_condition": sc, "experiment_id": "exp:k"}),
        ]),
        ("edge_type = 'tests'", [("exp:k", "hyp:k")]),
        ("attrs->>'experiment_ref'", [
            ("result:r1", {
                "experiment_ref": "exp:k",
                "metric_name": "speedup",
                "metric_value": "wat",
            }),
        ]),
        ("edge_type = 'contains'", []),
        ("edge_id = ANY(:edge_ids)", []),
    ]
    edges = _resolve(linker, steps)
    assert edges == []


def test_linker_class_contract() -> None:
    """Sanity: declared class attrs match the substrate enricher
    Protocol expectations."""
    linker = SuccessConditionLinker()
    assert linker.name == "success_condition"
    assert "supports" in linker.emits_edge_types
    assert "contradicts" in linker.emits_edge_types
    assert ("result", "supports", "hypothesis") in linker.requires_narrowings
    assert ("result", "contradicts", "hypothesis") in linker.requires_narrowings
