"""SuccessConditionLinker — emit Result→Hypothesis supports/contradicts.

For each `Experiment` carrying a ``success_condition`` attribute (a JSON
string with ``metric`` and ``predicate`` keys) AND a ``tests`` outgoing
edge to a Hypothesis, the linker:

1. Parses the predicate via the narrow DSL in
   :mod:`okg.substrate.library.sources.experimental.predicates`.
2. Finds every Result reachable from the Experiment, either via the
   intra-module Trial chain (`Experiment contains Trial produces Result`)
   OR directly via ``Result.attrs.experiment_ref`` (the
   `BenchOutputsSource` materialisation path).
3. Filters Results by matching ``metric_name`` against the predicate's
   ``metric``.
4. Evaluates the predicate against ``metric_value`` and emits either
   ``Result supports Hypothesis`` or ``Result contradicts Hypothesis``.

This is the "fast lane" for hypothesis evidence (per the umbrella's D2
contract). The slow lane (operator-curated ``Observation`` in the
reasoning module) covers complex predicates the DSL cannot express.

Per `extend-substrate-experimental-with-compute-job-and-sources` Phase 6
(D2 + R2).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.library.sources.experimental.predicates import (
    Predicate,
    PredicateParseError,
)
from okg.substrate.library.linkers import _chronos


__all__ = ["SuccessConditionLinker"]


class SuccessConditionLinker:
    """Emit Result→Hypothesis `supports` / `contradicts` edges from
    each Experiment's `success_condition` predicate."""

    name = "success_condition"
    publisher_skip_when_clean = True
    reads_subtypes = ("experiment", "trial", "result")
    reads_edge_types = ("tests", "contains", "produces")
    emits_edge_types = ("supports", "contradicts")
    requires_narrowings = (
        ("result", "supports", "hypothesis"),
        ("result", "contradicts", "hypothesis"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        del incremental
        with conn.cursor() as cur:
            edges = self._resolve_edges(cur, skip_existing=True)
            emitted, skipped = self._emit_edges(
                cur, edges=edges, generation_id=generation_id,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
        )

    # ---------- impl ----------

    def _resolve_edges(
        self, cur, *, skip_existing: bool,
    ) -> list[tuple[str, str, str, dict[str, Any]]]:
        experiments = _fetch_experiments(cur)
        if not experiments:
            return []
        hypothesis_targets = _fetch_tests_edges(
            cur, experiment_ids=tuple(e["node_id"] for e in experiments),
        )
        # Index experiment → tests-target so we can decide whether to
        # emit anything at all.
        eligible = [
            e for e in experiments
            if e["node_id"] in hypothesis_targets
        ]
        if not eligible:
            return []

        # Materialise Result → Experiment via Trial chain + direct attr.
        results_by_experiment = _fetch_results_by_experiment(
            cur, experiment_ids=tuple(e["node_id"] for e in eligible),
        )

        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        for exp in eligible:
            exp_id = exp["node_id"]
            sc_raw = exp["attrs"].get("success_condition")
            if not sc_raw:
                continue
            try:
                if isinstance(sc_raw, str):
                    sc = json.loads(sc_raw)
                else:
                    sc = dict(sc_raw)  # tolerate JSONB → dict round-trip
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
            metric = sc.get("metric")
            predicate_text = sc.get("predicate")
            if not isinstance(metric, str) or not isinstance(
                predicate_text, str
            ):
                continue
            try:
                predicate = Predicate.parse(predicate_text)
            except PredicateParseError:
                # Source-side validation should have caught this; if
                # somehow stale data hit the live table, skip silently
                # rather than crash the publish cycle.
                continue

            hyp_ids = hypothesis_targets[exp_id]
            for result in results_by_experiment.get(exp_id, ()):
                if result["attrs"].get("metric_name") != metric:
                    continue
                value_raw = result["attrs"].get("metric_value")
                try:
                    value = float(value_raw)
                except (TypeError, ValueError):
                    continue
                passes = predicate.evaluate(value)
                edge_type = "supports" if passes else "contradicts"
                payload = {
                    "metric": metric,
                    "predicate": predicate_text,
                    "metric_value": value,
                    "experiment_id": exp_id,
                    "linker": self.name,
                }
                for hyp_id in hyp_ids:
                    edge = (
                        result["node_id"], edge_type, hyp_id, payload,
                    )
                    edges.append(edge)
        if skip_existing:
            existing = _existing_outcome_edge_keys(cur, edges)
            edges = [
                edge for edge in edges
                if (edge[0], edge[1], edge[2]) not in existing
            ]
        return edges

    def _emit_edges(
        self,
        cur,
        *,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        generation_id: int,
    ) -> tuple[int, int]:
        if not edges:
            return 0, 0
        observed_at = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        rows: list[tuple[Any, ...]] = []
        seen: set[str] = set()
        for src, edge_type, dst, attrs in sorted(
            edges, key=lambda item: (item[0], item[1], item[2]),
        ):
            dedupe_key = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{src}|{edge_type}|{dst}|{generation_id}",
            ).hex
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append((
                _mint_edge_id(src, edge_type, dst),
                src,
                dst,
                edge_type,
                json.dumps(attrs),
                json.dumps({
                    "src_node_id": src,
                    "dst_node_id": dst,
                    "linker": self.name,
                }),
                json.dumps({"generation_id": generation_id}),
                observed_at,
                dedupe_key,
                run_id,
            ))
        cur.execute(
            "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
            "WHERE dedupe_key = ANY(%s)",
            ([row[8] for row in rows],),
        )
        already = {row[0] for row in cur.fetchall()}
        to_insert = [row for row in rows if row[8] not in already]
        for row in to_insert:
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s, %s, %s::jsonb,
                        'derived_deterministic', 1.0,
                        %s,
                        %s::jsonb, %s::jsonb,
                        %s, %s, %s, 'I')
                """,
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    f"_enricher:{self.name}",
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                ),
            )
        return len(to_insert), len(rows) - len(to_insert)


# ---------- helpers ----------


def _fetch_experiments(cur) -> list[dict[str, Any]]:
    rows = _chronos.query(
        cur,
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'experiment'
           AND attrs ? 'success_condition'
        """,
    )
    return [
        {"node_id": str(row["node_id"]), "attrs": dict(row["attrs"] or {})}
        for row in rows
    ]


def _fetch_tests_edges(
    cur, *, experiment_ids: tuple[str, ...],
) -> dict[str, list[str]]:
    if not experiment_ids:
        return {}
    rows = _chronos.query(
        cur,
        """
        SELECT src, dst
          FROM okg.graph_edges
         WHERE edge_type = 'tests'
           AND src = ANY(:experiment_ids)
        """,
        {"experiment_ids": list(experiment_ids)},
    )
    out: dict[str, list[str]] = {}
    for row in rows:
        out.setdefault(str(row["src"]), []).append(str(row["dst"]))
    return out


def _fetch_results_by_experiment(
    cur, *, experiment_ids: tuple[str, ...],
) -> dict[str, list[dict[str, Any]]]:
    """Index Results by parent Experiment via two paths:

    1. ``Result.attrs.experiment_ref`` (BenchOutputsSource path).
    2. ``Experiment contains Trial`` + ``Trial produces Result``
       (substrate-existing intra-module narrowings).
    """
    if not experiment_ids:
        return {}

    out: dict[str, list[dict[str, Any]]] = {}
    seen_result_ids: set[tuple[str, str]] = set()

    # Path 1 — direct attribute reference.
    rows = _chronos.query(
        cur,
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'result'
           AND attrs->>'experiment_ref' = ANY(:experiment_ids)
        """,
        {"experiment_ids": list(experiment_ids)},
    )
    for row in rows:
        attrs_d = dict(row["attrs"] or {})
        exp_id = str(attrs_d.get("experiment_ref"))
        key = (exp_id, str(row["node_id"]))
        if key in seen_result_ids:
            continue
        seen_result_ids.add(key)
        out.setdefault(exp_id, []).append(
            {"node_id": str(row["node_id"]), "attrs": attrs_d}
        )

    # Path 2 — via Trial chain.
    chain_rows = _chronos.query(
        cur,
        """
        SELECT exp.src AS experiment_id,
               r.node_id AS result_id,
               r.attrs AS result_attrs
          FROM okg.graph_edges AS exp
          JOIN okg.graph_edges AS tr
            ON tr.src = exp.dst
           AND tr.edge_type = 'produces'
          JOIN okg.graph_nodes AS r
            ON r.node_id = tr.dst
           AND r.subtype = 'result'
         WHERE exp.edge_type = 'contains'
           AND exp.src = ANY(:experiment_ids)
        """,
        {"experiment_ids": list(experiment_ids)},
    )
    for row in chain_rows:
        key = (str(row["experiment_id"]), str(row["result_id"]))
        if key in seen_result_ids:
            continue
        seen_result_ids.add(key)
        out.setdefault(str(row["experiment_id"]), []).append({
            "node_id": str(row["result_id"]),
            "attrs": dict(row["result_attrs"] or {}),
        })

    return out


def _existing_outcome_edge_keys(
    cur,
    edges: list[tuple[str, str, str, dict[str, Any]]],
) -> set[tuple[str, str, str]]:
    if not edges:
        return set()
    rows = _chronos.query(
        cur,
        """
        SELECT src, edge_type, dst
          FROM okg.graph_edges
         WHERE edge_id = ANY(:edge_ids)
        """,
        {
            "edge_ids": [
                _mint_edge_id(src, edge_type, dst)
                for src, edge_type, dst, _attrs in edges
            ]
        },
    )
    return {
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in rows
    }


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)
