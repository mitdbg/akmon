"""InterfaceHandlerLinker — deterministic entrypoint to handler links."""
from __future__ import annotations
from okg.substrate.library.sources._code_graph_common import _lexical_symbol_name

import json
import re
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


__all__ = ["InterfaceHandlerDiagnostics", "InterfaceHandlerLinker"]


@dataclass(frozen=True)
class _Node:
    node_id: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class _CodeSymbol:
    node_id: str
    path: str
    name: str
    qualified_name: str
    start_line: int
    end_line: int
    kind: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class InterfaceHandlerDiagnostics:
    eligible_endpoints: int
    matched_endpoints: int
    emitted_edges: int
    unmatched_endpoint_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_endpoints": self.eligible_endpoints,
            "matched_endpoints": self.matched_endpoints,
            "emitted_edges": self.emitted_edges,
            "unmatched_endpoint_count": self.unmatched_endpoint_count,
        }


class InterfaceHandlerLinker(PrelockPlannableLinker):
    """Link interface_endpoint nodes to concrete code_symbol handlers."""

    name = "interface_handler"
    publisher_skip_when_clean = True
    reads_subtypes = ("interface_endpoint", "code_symbol")
    emits_edge_types = ("routes_to",)
    requires_narrowings = (
        ("interface_endpoint", "routes_to", "code_symbol"),
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

    def diagnostics(self, conn: psycopg.Connection) -> InterfaceHandlerDiagnostics:
        with conn.cursor() as cur:
            endpoints = _fetch_nodes(cur, "interface_endpoint")
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched = {src for src, _edge_type, _dst, _attrs in edges}
        return InterfaceHandlerDiagnostics(
            eligible_endpoints=len(endpoints),
            matched_endpoints=len(matched),
            emitted_edges=len(edges),
            unmatched_endpoint_count=len(unmatched),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        endpoints = _fetch_nodes(cur, "interface_endpoint")
        symbols = _SymbolIndex(_fetch_symbols(cur))
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for endpoint in endpoints:
            match = self._match_endpoint(endpoint, symbols)
            if match is None:
                unmatched.append({
                    "node_id": endpoint.node_id,
                    "kind": endpoint.attrs.get("kind") or "",
                    "name": endpoint.attrs.get("name") or "",
                    "path": endpoint.attrs.get("path") or "",
                })
                continue
            symbol, resolution, resolution_attrs = match
            edge = (
                endpoint.node_id,
                "routes_to",
                symbol.node_id,
                {
                    "resolution": resolution,
                    "handler": symbol.name,
                    "handler_symbol_id": symbol.node_id,
                    "linker": self.name,
                    **resolution_attrs,
                },
            )
            edges.append(edge)
        if skip_existing:
            edges = _filter_new_edges(cur, edges)
        return edges, unmatched

    def _match_endpoint(
        self,
        endpoint: _Node,
        symbols: "_SymbolIndex",
    ) -> tuple[_CodeSymbol, str, dict[str, str]] | None:
        attrs = endpoint.attrs
        path = _normalize_path(str(attrs.get("path") or ""))

        semantic = symbols.by_semantic_attrs(attrs)
        if semantic:
            symbol, resolution_attrs = semantic
            return symbol, "semantic_identity", resolution_attrs

        handler_symbol_id = str(attrs.get("handler_symbol_id") or "")
        if handler_symbol_id:
            symbol = symbols.by_id(handler_symbol_id)
            if symbol:
                return symbol, "handler_symbol_id", resolution_method_attrs("handler_symbol_id")

        for handler_name in _handler_names(attrs):
            symbol = symbols.by_path_name(path, handler_name) if path else None
            if symbol:
                return symbol, "handler_name_same_file", resolution_method_attrs("handler_name_same_file")
            symbol = symbols.by_unique_name(handler_name)
            if symbol:
                return symbol, "handler_name_unique", resolution_method_attrs("handler_name_unique")

        for handler_name in _cli_handler_names(attrs):
            symbol = symbols.by_path_name(path, handler_name) if path else None
            if symbol:
                return symbol, "cli_command_handler_same_file", resolution_method_attrs("cli_command_handler_same_file")
            symbol = symbols.by_unique_name(handler_name)
            if symbol:
                return symbol, "cli_command_handler_unique", resolution_method_attrs("cli_command_handler_unique")

        line = _int_or_none(attrs.get("start_line"))
        if path and line is not None:
            symbol = symbols.by_path_line(path, line)
            if symbol:
                return symbol, "enclosing_endpoint_declaration_symbol", resolution_method_attrs("enclosing_endpoint_declaration_symbol")
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
        self.path_name: dict[tuple[str, str], list[_CodeSymbol]] = {}
        self.name_symbols: dict[str, list[_CodeSymbol]] = {}
        for symbol in symbols:
            self.path_symbols.setdefault(symbol.path, []).append(symbol)
            self._add_path_name(symbol.path, symbol.name, symbol)
            self._add_path_name(symbol.path, symbol.qualified_name, symbol)
            self._add_name(symbol.name, symbol)
            self._add_name(symbol.qualified_name, symbol)
            lexical_name = _lexical_symbol_name(
                str(symbol.attrs.get("repo") or ""), symbol.path, symbol.qualified_name,
            )
            self._add_path_name(symbol.path, lexical_name, symbol)
            self._add_name(lexical_name, symbol)
        for items in self.path_symbols.values():
            items.sort(key=lambda s: (s.end_line - s.start_line, s.start_line, s.node_id))

    def _add_path_name(self, path: str, name: str, symbol: _CodeSymbol) -> None:
        if not name:
            return
        items = self.path_name.setdefault((path, name), [])
        if all(existing.node_id != symbol.node_id for existing in items):
            items.append(symbol)

    def _add_name(self, name: str, symbol: _CodeSymbol) -> None:
        if not name:
            return
        items = self.name_symbols.setdefault(name, [])
        if all(existing.node_id != symbol.node_id for existing in items):
            items.append(symbol)

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

    def by_path_name(self, path: str, name: str) -> _CodeSymbol | None:
        matches = self.path_name.get((path, name), [])
        non_module = [s for s in matches if s.kind != "module"]
        matches = non_module or matches
        if len(matches) == 1:
            return matches[0]
        return None

    def by_unique_name(self, name: str) -> _CodeSymbol | None:
        matches = self.name_symbols.get(name, [])
        non_module = [s for s in matches if s.kind != "module"]
        matches = non_module or matches
        if len(matches) == 1:
            return matches[0]
        return None

    def by_path_line(self, path: str, line: int) -> _CodeSymbol | None:
        candidates = [
            s for s in self.path_symbols.get(path, [])
            if s.start_line <= line <= s.end_line
        ]
        non_module = [s for s in candidates if s.kind != "module"]
        return (non_module or candidates or [None])[0]


def _fetch_nodes(cur, subtype: str) -> list[_Node]:
    rows = _chronos.nodes(cur, (subtype,))
    return [
        _Node(str(row["node_id"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _fetch_symbols(cur) -> list[_CodeSymbol]:
    nodes = _fetch_nodes(cur, "code_symbol")
    out: list[_CodeSymbol] = []
    for node in nodes:
        path = _normalize_path(str(node.attrs.get("path") or ""))
        start = _int_or_none(node.attrs.get("start_line"))
        end = _int_or_none(node.attrs.get("end_line"))
        name = str(node.attrs.get("name") or "")
        qualified_name = str(node.attrs.get("qualified_name") or name)
        if not path or start is None or end is None or not name:
            continue
        out.append(_CodeSymbol(
            node_id=node.node_id,
            path=path,
            name=name,
            qualified_name=qualified_name,
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


def _handler_names(attrs: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in (
        "handler",
        "handler_name",
        "callback",
        "callback_name",
        "function",
        "function_name",
    ):
        value = str(attrs.get(key) or "").strip()
        if value:
            out.append(value.split(":")[-1].split(".")[-1])
            out.append(value)
    return _dedupe_names(out)


def _cli_handler_names(attrs: dict[str, Any]) -> list[str]:
    if str(attrs.get("kind") or "") != "cli_command":
        return []
    name = str(attrs.get("name") or "").strip()
    if not name:
        return []
    slug = re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_")
    if not slug:
        return []
    return _dedupe_names([
        f"_cmd_{slug}",
        f"cmd_{slug}",
        f"handle_{slug}",
        f"{slug}_command",
    ])


def _dedupe_names(names: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
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
