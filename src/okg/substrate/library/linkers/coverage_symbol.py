"""CoverageSymbolLinker - deterministic coverage report to symbol links."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

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


__all__ = ["CoverageSymbolDiagnostics", "CoverageSymbolLinker"]


@dataclass(frozen=True)
class _Node:
    node_id: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class _CodeSymbol:
    node_id: str
    path: str
    start_line: int
    end_line: int
    kind: str
    attrs: dict[str, Any] | None = None


@dataclass(frozen=True)
class CoverageSymbolDiagnostics:
    coverage_reports: int
    symbols: int
    matched_symbols: int
    emitted_edges: int

    def as_dict(self) -> dict[str, int]:
        return {
            "coverage_reports": self.coverage_reports,
            "symbols": self.symbols,
            "matched_symbols": self.matched_symbols,
            "emitted_edges": self.emitted_edges,
        }


class CoverageSymbolLinker(PrelockPlannableLinker):
    """Link coverage reports to code symbols with executed lines."""

    name = "coverage_symbol"
    publisher_skip_when_clean = True
    reads_subtypes = ("coverage_report", "code_symbol")
    emits_edge_types = ("references",)
    requires_narrowings = (
        ("coverage_report", "references", "code_symbol"),
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
        reports = _fetch_nodes(cur, "coverage_report")
        symbols = _fetch_symbols(cur)
        edges = _resolve_coverage_symbol_edges(
            reports,
            symbols,
        )
        return _filter_new_edges(cur, edges)

    def diagnostics(self, conn: psycopg.Connection) -> CoverageSymbolDiagnostics:
        with conn.cursor() as cur:
            reports = _fetch_nodes(cur, "coverage_report")
            symbols = _fetch_symbols(cur)
            edges = _resolve_coverage_symbol_edges(reports, symbols)
        return CoverageSymbolDiagnostics(
            coverage_reports=len(reports),
            symbols=len(symbols),
            matched_symbols=len({dst for _src, _edge, dst, _attrs in edges}),
            emitted_edges=len(edges),
        )

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


def _resolve_coverage_symbol_edges(
    coverage_reports: list[_Node],
    symbols: list[_CodeSymbol],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    semantic_symbols = SemanticSymbolIndex(symbols)
    symbol_by_id = {symbol.node_id: symbol for symbol in symbols}
    by_path: dict[str, list[_CodeSymbol]] = {}
    for symbol in symbols:
        if not symbol.path:
            continue
        by_path.setdefault(symbol.path, []).append(symbol)

    edges: list[tuple[str, str, str, dict[str, Any]]] = []
    for report in coverage_reports:
        semantic_covered = _symbol_count_map(
            report.attrs.get("covered_symbols")
            or report.attrs.get("covered_symbol_semantic_keys")
        )
        semantic_missing = _symbol_count_map(
            report.attrs.get("missing_symbols")
            or report.attrs.get("missing_symbol_semantic_keys")
        )
        for semantic_key, covered_count in sorted(semantic_covered.items()):
            match = semantic_symbols.by_semantic_key(semantic_key)
            if not match:
                continue
            symbol = symbol_by_id.get(match.node_id)
            if not symbol:
                continue
            edge = (
                report.node_id,
                "references",
                symbol.node_id,
                {
                    "resolution": "covered_symbol_semantic_identity",
                    "covered_line_count": covered_count,
                    "missing_line_count": semantic_missing.get(semantic_key, 0),
                    "symbol_kind": symbol.kind,
                    "linker": CoverageSymbolLinker.name,
                    **match.attrs,
                },
            )
            edges.append(edge)
        covered_by_path = _line_map(report.attrs.get("covered_lines"))
        missing_by_path = _line_map(report.attrs.get("missing_lines"))
        for path, covered_lines in sorted(covered_by_path.items()):
            if not covered_lines:
                continue
            missing_lines = missing_by_path.get(path, set())
            for symbol in by_path.get(path, []):
                covered = [
                    line for line in covered_lines
                    if symbol.start_line <= line <= symbol.end_line
                ]
                if not covered:
                    continue
                missing = [
                    line for line in missing_lines
                    if symbol.start_line <= line <= symbol.end_line
                ]
                edge = (
                    report.node_id,
                    "references",
                    symbol.node_id,
                    {
                        "resolution": "covered_symbol_span",
                        "file_path": path,
                        "covered_line_count": len(covered),
                        "missing_line_count": len(missing),
                        "symbol_kind": symbol.kind,
                        "linker": CoverageSymbolLinker.name,
                        **resolution_method_attrs("covered_symbol_span"),
                    },
                )
                edges.append(edge)
    return _dedupe_edges(edges)


def _fetch_nodes(cur, subtype: str) -> list[_Node]:
    rows = _chronos.nodes(cur, (subtype,))
    return [_Node(str(row["node_id"]), dict(row["attrs"] or {})) for row in rows]


def _fetch_symbols(cur) -> list[_CodeSymbol]:
    nodes = _fetch_nodes(cur, "code_symbol")
    out: list[_CodeSymbol] = []
    for node in nodes:
        path = _normalize_path(str(node.attrs.get("path") or ""))
        start = _int_or_none(node.attrs.get("start_line"))
        end = _int_or_none(node.attrs.get("end_line"))
        if not path or start is None or end is None:
            continue
        out.append(_CodeSymbol(
            node_id=node.node_id,
            path=path,
            start_line=min(start, end),
            end_line=max(start, end),
            kind=str(node.attrs.get("kind") or ""),
            attrs=node.attrs,
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


def _line_map(value: Any) -> dict[str, set[int]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, set[int]] = {}
    for raw_path, raw_lines in value.items():
        path = _normalize_path(str(raw_path))
        if not path:
            continue
        lines: set[int] = set()
        if isinstance(raw_lines, (list, tuple, set)):
            for raw_line in raw_lines:
                line = _int_or_none(raw_line)
                if line is not None:
                    lines.add(line)
        out[path] = lines
    return out


def _symbol_count_map(value: Any) -> dict[str, int]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    out: dict[str, int] = {}
    if isinstance(value, Mapping):
        for raw_key, raw_count in value.items():
            key = str(raw_key or "").strip()
            if not key:
                continue
            if isinstance(raw_count, (list, tuple, set)):
                out[key] = len(raw_count)
            else:
                count = _int_or_none(raw_count)
                out[key] = count if count is not None else 1
    elif isinstance(value, (list, tuple, set)):
        for raw_key in value:
            key = str(raw_key or "").strip()
            if key:
                out[key] = 1
    return out


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
