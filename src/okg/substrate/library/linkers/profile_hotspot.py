"""ProfileHotspotLinker — deterministic profiling/benchmark evidence links."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg

from okg.substrate.enrichers.base import (
    EnrichResult,
    EnricherPrelockPlan,
    IncrementalContext,
)
from okg.substrate.library.linkers._prelock import PrelockPlannableLinker
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers.semantic_symbol import (
    SemanticSymbolIndex,
    resolution_method_attrs,
)


__all__ = ["ProfileHotspotDiagnostics", "ProfileHotspotLinker"]


@dataclass(frozen=True)
class _Node:
    node_id: str
    subtype: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class _CodeSymbol:
    node_id: str
    path: str
    start_line: int
    end_line: int
    kind: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class ProfileHotspotDiagnostics:
    eligible_nodes: int
    matched_nodes: int
    emitted_edges: int
    unmatched_node_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_nodes": self.eligible_nodes,
            "matched_nodes": self.matched_nodes,
            "emitted_edges": self.emitted_edges,
            "unmatched_node_count": self.unmatched_node_count,
        }


class ProfileHotspotLinker(PrelockPlannableLinker):
    """Link profile hotspots, benchmarks, and budgets to stable targets."""

    name = "profile_hotspot"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "profile_hotspot",
        "benchmark_measurement",
        "performance_budget",
        "profile_run",
        "code_symbol",
        "interface_endpoint",
        "data_asset",
        "test_case",
        "test_report",
        "acceptance_criterion",
    )
    emits_edge_types = ("affects", "references", "measured_by", "budgeted_by")
    requires_narrowings = (
        ("profile_hotspot", "affects", "code_symbol"),
        ("profile_hotspot", "references", "interface_endpoint"),
        ("profile_hotspot", "references", "data_asset"),
        ("benchmark_measurement", "references", "profile_run"),
        ("benchmark_measurement", "references", "performance_budget"),
        ("benchmark_measurement", "measured_by", "test_case"),
        ("benchmark_measurement", "measured_by", "test_report"),
        ("benchmark_measurement", "references", "acceptance_criterion"),
        ("performance_budget", "budgeted_by", "acceptance_criterion"),
        ("performance_budget", "references", "interface_endpoint"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
        prelock_plan: EnricherPrelockPlan | None = None,
    ) -> EnrichResult:
        with conn.cursor() as cur:
            if prelock_plan is not None:
                return self._enrich_from_plan(
                    cur,
                    prelock_plan,
                    generation_id=generation_id,
                    incremental=incremental,
                )
            edges = self._prelock_edges(cur, incremental, {})
            if edges is None:
                return EnrichResult(enricher_name=self.name, n_edges_emitted=0)
            emitted, skipped = self._emit_edges(
                cur,
                edges=edges,
                generation_id=generation_id,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
        )

    def _prelock_edges(
        self,
        cur,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> list[tuple[str, str, str, dict[str, Any]]] | None:
        del incremental, timings_ms
        edges, _unmatched = self._resolve_edges(cur, skip_existing=True)
        return edges

    def diagnostics(self, conn: psycopg.Connection) -> ProfileHotspotDiagnostics:
        with conn.cursor() as cur:
            nodes = _fetch_nodes(cur, (
                "profile_hotspot",
                "benchmark_measurement",
                "performance_budget",
            ))
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched = {src for src, _edge_type, _dst, _attrs in edges}
        return ProfileHotspotDiagnostics(
            eligible_nodes=len(nodes),
            matched_nodes=len(matched),
            emitted_edges=len(edges),
            unmatched_node_count=len(unmatched),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        nodes = _fetch_nodes(cur, (
            "profile_hotspot",
            "benchmark_measurement",
            "performance_budget",
        ))
        symbols = _SymbolIndex(_fetch_symbols(cur))
        targets = _TargetIndex(_fetch_nodes(cur, (
            "profile_run",
            "interface_endpoint",
            "data_asset",
            "performance_budget",
            "test_case",
            "test_report",
            "acceptance_criterion",
        )))
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for node in nodes:
            node_edges: list[tuple[str, str, str, dict[str, Any]]] = []
            if node.subtype == "profile_hotspot":
                path = _normalize_path(str(node.attrs.get("path") or ""))
                line = _int_or_none(node.attrs.get("line"))
                semantic = symbols.by_semantic_attrs(node.attrs)
                if semantic:
                    symbol, resolution_attrs = semantic
                    node_edges.append(_edge(
                        node,
                        "affects",
                        _Node(symbol.node_id, "code_symbol", {}),
                        "semantic_identity",
                        resolution_attrs,
                    ))
                elif path and line is not None:
                    symbol = symbols.by_path_line(path, line)
                    if symbol:
                        node_edges.append(_edge(
                            node,
                            "affects",
                            _Node(symbol.node_id, "code_symbol", {}),
                            "hotspot_span_symbol",
                        ))
                for target, resolution in targets.hotspot_targets(node):
                    node_edges.append(_edge(node, "references", target, resolution))
            elif node.subtype == "benchmark_measurement":
                for edge_type, target, resolution in targets.benchmark_targets(node):
                    node_edges.append(_edge(node, edge_type, target, resolution))
            elif node.subtype == "performance_budget":
                for edge_type, target, resolution in targets.budget_targets(node):
                    node_edges.append(_edge(node, edge_type, target, resolution))

            if not node_edges:
                unmatched.append({
                    "node_id": node.node_id,
                    "subtype": node.subtype,
                    "name": node.attrs.get("name") or node.attrs.get("scenario") or node.attrs.get("scope") or "",
                })
                continue
            edges.extend(_dedupe_edges(node_edges))
        if skip_existing:
            edges = _filter_new_edges(cur, edges)
        return edges, unmatched

    def _emit_edges(
        self,
        cur,
        *,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        generation_id: int,
    ) -> tuple[int, int]:
        if not edges:
            return 0, 0
        rows = self._edge_rows(edges, generation_id=generation_id)
        to_insert = self._rows_to_insert(cur, rows)
        self._insert_rows(cur, to_insert)
        return len(to_insert), len(rows) - len(to_insert)

    def _edge_rows(
        self,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        *,
        generation_id: int,
    ) -> list[tuple[Any, ...]]:
        observed_at = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        rows: list[tuple[Any, ...]] = []
        seen: set[str] = set()
        for src, edge_type, dst, attrs in sorted(
            edges,
            key=lambda item: (item[0], item[1], item[2]),
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
        return rows

    def _rows_to_insert(
        self, cur, rows: list[tuple[Any, ...]],
    ) -> list[tuple[Any, ...]]:
        cur.execute(
            "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
            "WHERE dedupe_key = ANY(%s)",
            ([row[8] for row in rows],),
        )
        already = {row[0] for row in cur.fetchall()}
        return [row for row in rows if row[8] not in already]

    def _insert_rows(self, cur, to_insert: list[tuple[Any, ...]]) -> None:
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


class _SymbolIndex:
    def __init__(self, symbols: list[_CodeSymbol]) -> None:
        self.by_node_id = {symbol.node_id: symbol for symbol in symbols}
        self.semantic_symbols = SemanticSymbolIndex(symbols)
        self.path_symbols: dict[str, list[_CodeSymbol]] = {}
        for symbol in symbols:
            self.path_symbols.setdefault(symbol.path, []).append(symbol)
        for items in self.path_symbols.values():
            items.sort(key=lambda s: (s.end_line - s.start_line, s.start_line, s.node_id))

    def by_path_line(self, path: str, line: int) -> _CodeSymbol | None:
        candidates = [
            s for s in self.path_symbols.get(path, [])
            if s.start_line <= line <= s.end_line
        ]
        non_module = [s for s in candidates if s.kind != "module"]
        return (non_module or candidates or [None])[0]

    def by_semantic_attrs(
        self,
        attrs: dict[str, Any],
    ) -> tuple[_CodeSymbol, dict[str, str]] | None:
        match = self.semantic_symbols.match_attrs(attrs)
        if not match:
            return None
        symbol = self.by_node_id.get(match.node_id)
        if not symbol:
            return None
        return symbol, match.attrs


class _TargetIndex:
    def __init__(self, targets: list[_Node]) -> None:
        self.by_id = {target.node_id: target for target in targets}
        self.by_subtype_text: dict[tuple[str, str], list[_Node]] = {}
        for target in targets:
            for key in _target_keys(target):
                self.by_subtype_text.setdefault((target.subtype, key), []).append(target)

    def hotspot_targets(self, node: _Node) -> list[tuple[_Node, str]]:
        out: list[tuple[_Node, str]] = []
        endpoint = self._unique("interface_endpoint", node.attrs.get("symbol_hint") or node.attrs.get("name"))
        if endpoint:
            out.append((endpoint, "hotspot_symbol_hint_interface"))
        asset = self._unique("data_asset", node.attrs.get("asset_hint"))
        if asset:
            out.append((asset, "hotspot_asset_hint"))
        return out

    def benchmark_targets(self, node: _Node) -> list[tuple[str, _Node, str]]:
        out: list[tuple[str, _Node, str]] = []
        profile_run_id = str(node.attrs.get("profile_run_id") or "")
        profile_run = self.by_id.get(profile_run_id)
        if profile_run and profile_run.subtype == "profile_run":
            out.append(("references", profile_run, "profile_run_id"))
        budget = self._budget_for_measurement(node)
        if budget:
            out.append(("references", budget, "scenario_metric_budget"))
        for subtype, edge_type, attr, resolution in (
            ("test_case", "measured_by", "test_case_id", "test_case_id"),
            ("test_report", "measured_by", "test_report_id", "test_report_id"),
            ("acceptance_criterion", "references", "criterion_id", "criterion_id"),
        ):
            target = self.by_id.get(str(node.attrs.get(attr) or ""))
            if target and target.subtype == subtype:
                out.append((edge_type, target, resolution))
        return out

    def budget_targets(self, node: _Node) -> list[tuple[str, _Node, str]]:
        out: list[tuple[str, _Node, str]] = []
        criterion = self.by_id.get(str(node.attrs.get("criterion_id") or ""))
        if criterion and criterion.subtype == "acceptance_criterion":
            out.append(("budgeted_by", criterion, "criterion_id"))
        endpoint = self._unique("interface_endpoint", node.attrs.get("scope"))
        if endpoint:
            out.append(("references", endpoint, "budget_scope_interface"))
        return out

    def _budget_for_measurement(self, node: _Node) -> _Node | None:
        scenario = _key(node.attrs.get("scenario"))
        metric = _key(node.attrs.get("metric"))
        matches = [
            target for target in self.by_id.values()
            if target.subtype == "performance_budget"
            and _key(target.attrs.get("scope")) == scenario
            and _key(target.attrs.get("metric")) == metric
        ]
        return matches[0] if len(matches) == 1 else None

    def _unique(self, subtype: str, value: Any) -> _Node | None:
        keys = _value_keys(value)
        matches: list[_Node] = []
        seen: set[str] = set()
        for key in keys:
            for target in self.by_subtype_text.get((subtype, key), []):
                if target.node_id not in seen:
                    seen.add(target.node_id)
                    matches.append(target)
        return matches[0] if len(matches) == 1 else None


def _edge(
    src: _Node,
    edge_type: str,
    dst: _Node,
    resolution: str,
    extra_attrs: dict[str, str] | None = None,
) -> tuple[str, str, str, dict[str, Any]]:
    return (
        src.node_id,
        edge_type,
        dst.node_id,
        {
            "resolution": resolution,
            "linker": ProfileHotspotLinker.name,
            **resolution_method_attrs(resolution),
            **(extra_attrs or {}),
        },
    )


def _fetch_nodes(cur, subtypes: tuple[str, ...]) -> list[_Node]:
    rows = _chronos.nodes(cur, subtypes)
    return [
        _Node(str(row["node_id"]), str(row["subtype"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _fetch_symbols(cur) -> list[_CodeSymbol]:
    rows = _chronos.nodes(cur, ("code_symbol",))
    source_rows = [(row["node_id"], row["attrs"]) for row in rows]
    out: list[_CodeSymbol] = []
    for node_id, attrs in source_rows:
        attrs = dict(attrs or {})
        path = _normalize_path(str(attrs.get("path") or ""))
        start = _int_or_none(attrs.get("start_line"))
        end = _int_or_none(attrs.get("end_line"))
        if not path or start is None or end is None:
            continue
        out.append(_CodeSymbol(
            node_id=str(node_id),
            path=path,
            start_line=min(start, end),
            end_line=max(start, end),
            kind=str(attrs.get("kind") or ""),
            attrs=attrs,
        ))
    return out


def _filter_new_edges(
    cur,
    edges: list[tuple[str, str, str, dict[str, Any]]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    if not edges:
        return []
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
    existing = {
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in rows
    }
    return [
        edge for edge in edges
        if (edge[0], edge[1], edge[2]) not in existing
    ]


def _dedupe_edges(
    edges: list[tuple[str, str, str, dict[str, Any]]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    out: list[tuple[str, str, str, dict[str, Any]]] = []
    seen: set[tuple[str, str, str]] = set()
    for edge in edges:
        key = (edge[0], edge[1], edge[2])
        if key in seen:
            continue
        seen.add(key)
        out.append(edge)
    return out


def _target_keys(node: _Node) -> set[str]:
    attrs = node.attrs
    values = [node.node_id]
    if node.subtype == "interface_endpoint":
        values.extend([attrs.get("name"), attrs.get("handler"), attrs.get("route"), attrs.get("endpoint_id")])
    elif node.subtype == "data_asset":
        values.extend([attrs.get("name"), attrs.get("asset_id")])
    elif node.subtype == "performance_budget":
        values.extend([attrs.get("budget_id"), attrs.get("scope")])
    elif node.subtype == "test_case":
        values.extend([attrs.get("name"), attrs.get("classname")])
    elif node.subtype == "test_report":
        values.extend([attrs.get("report_id"), attrs.get("path")])
    elif node.subtype == "acceptance_criterion":
        values.extend([attrs.get("criterion_id"), attrs.get("slug"), attrs.get("text")])
    return {key for value in values for key in _value_keys(value)}


def _value_keys(value: Any) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {_key(raw), _key(raw.rsplit("/", 1)[-1]), _key(raw.rsplit(".", 1)[-1])}


def _key(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)


def _normalize_path(path: str) -> str:
    path = path.replace("\\", "/").strip().strip("/")
    while path.startswith("./"):
        path = path[2:]
    return path


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
