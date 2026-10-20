"""DataAssetLinker — deterministic symbol links for data/config touchpoints."""
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


__all__ = ["DataAssetDiagnostics", "DataAssetLinker"]


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
class DataAssetDiagnostics:
    eligible_touchpoints: int
    matched_touchpoints: int
    emitted_edges: int
    unmatched_touchpoint_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_touchpoints": self.eligible_touchpoints,
            "matched_touchpoints": self.matched_touchpoints,
            "emitted_edges": self.emitted_edges,
            "unmatched_touchpoint_count": self.unmatched_touchpoint_count,
        }


class DataAssetLinker(PrelockPlannableLinker):
    """Link code symbols to data_asset/config_key nodes by span metadata."""

    name = "data_asset"
    publisher_skip_when_clean = True
    reads_subtypes = ("code_symbol", "data_asset", "config_key")
    emits_edge_types = ("reads", "writes", "references")
    requires_narrowings = (
        ("code_symbol", "reads", "data_asset"),
        ("code_symbol", "writes", "data_asset"),
        ("code_symbol", "references", "data_asset"),
        ("code_symbol", "references", "config_key"),
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

    def diagnostics(self, conn: psycopg.Connection) -> DataAssetDiagnostics:
        with conn.cursor() as cur:
            touchpoints = _fetch_nodes(cur, ("data_asset", "config_key"))
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched = {dst for _src, _edge_type, dst, _attrs in edges}
        return DataAssetDiagnostics(
            eligible_touchpoints=len(touchpoints),
            matched_touchpoints=len(matched),
            emitted_edges=len(edges),
            unmatched_touchpoint_count=len(unmatched),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        touchpoints = _fetch_nodes(cur, ("data_asset", "config_key"))
        symbols = _SymbolIndex(_fetch_symbols(cur))
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for touchpoint in touchpoints:
            match = self._match_touchpoint(touchpoint, symbols)
            if match is None:
                unmatched.append({
                    "node_id": touchpoint.node_id,
                    "subtype": touchpoint.subtype,
                    "path": touchpoint.attrs.get("path") or "",
                    "line": touchpoint.attrs.get("line") or "",
                })
                continue
            symbol, resolution, resolution_attrs = match
            edge_type = _edge_type_for_touchpoint(touchpoint)
            edge = (
                symbol.node_id,
                edge_type,
                touchpoint.node_id,
                {
                    "resolution": resolution,
                    "access_kind": str(touchpoint.attrs.get("access_kind") or "reference"),
                    "line": _int_or_none(touchpoint.attrs.get("line")) or 0,
                    "source": _normalize_path(str(touchpoint.attrs.get("path") or "")),
                    "linker": self.name,
                    **resolution_attrs,
                },
            )
            edges.append(edge)
        if skip_existing:
            edges = _filter_new_edges(cur, edges)
        return edges, unmatched

    def _match_touchpoint(
        self,
        touchpoint: _Node,
        symbols: "_SymbolIndex",
    ) -> tuple[_CodeSymbol, str, dict[str, str]] | None:
        semantic = symbols.by_semantic_attrs(touchpoint.attrs)
        if semantic:
            symbol, resolution_attrs = semantic
            return symbol, "semantic_identity", resolution_attrs

        symbol_id = str(touchpoint.attrs.get("source_symbol_id") or "")
        if symbol_id:
            symbol = symbols.by_id(symbol_id)
            if symbol:
                return symbol, "source_symbol_id", resolution_method_attrs("source_symbol_id")

        path = _normalize_path(str(touchpoint.attrs.get("path") or ""))
        line = _int_or_none(touchpoint.attrs.get("line"))
        if path and line is not None:
            symbol = symbols.by_path_line(path, line)
            if symbol:
                return symbol, "enclosing_touchpoint_symbol", resolution_method_attrs("enclosing_touchpoint_symbol")
        return None

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

    def by_id(self, node_id: str) -> _CodeSymbol | None:
        return self.by_node_id.get(node_id)

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

    def by_path_line(self, path: str, line: int) -> _CodeSymbol | None:
        candidates = [
            s for s in self.path_symbols.get(path, [])
            if s.start_line <= line <= s.end_line
        ]
        non_module = [s for s in candidates if s.kind != "module"]
        return (non_module or candidates or [None])[0]


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


def _edge_type_for_touchpoint(touchpoint: _Node) -> str:
    if touchpoint.subtype == "config_key":
        return "references"
    access_kind = str(touchpoint.attrs.get("access_kind") or "reference")
    if access_kind in {"read", "consume", "subscribe", "receive", "request"}:
        return "reads"
    if access_kind in {"write", "publish", "send", "schedule", "mutate"}:
        return "writes"
    return "references"


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
