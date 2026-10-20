"""DataFlowEvidenceLinker — deterministic links for Semgrep/CodeQL evidence."""
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


__all__ = ["DataFlowEvidenceDiagnostics", "DataFlowEvidenceLinker"]


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
class DataFlowEvidenceDiagnostics:
    eligible_evidence_nodes: int
    matched_evidence_nodes: int
    emitted_edges: int
    unmatched_evidence_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_evidence_nodes": self.eligible_evidence_nodes,
            "matched_evidence_nodes": self.matched_evidence_nodes,
            "emitted_edges": self.emitted_edges,
            "unmatched_evidence_count": self.unmatched_evidence_count,
        }


class DataFlowEvidenceLinker(PrelockPlannableLinker):
    """Link data-flow observations/steps to deterministic graph targets."""

    name = "data_flow_evidence"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "data_flow_observation",
        "data_flow_step",
        "code_symbol",
        "data_asset",
        "config_key",
    )
    emits_edge_types = ("references",)
    requires_narrowings = (
        ("data_flow_observation", "references", "code_symbol"),
        ("data_flow_observation", "references", "data_asset"),
        ("data_flow_observation", "references", "config_key"),
        ("data_flow_step", "references", "code_symbol"),
        ("data_flow_step", "references", "data_asset"),
        ("data_flow_step", "references", "config_key"),
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

    def diagnostics(self, conn: psycopg.Connection) -> DataFlowEvidenceDiagnostics:
        with conn.cursor() as cur:
            evidence = _fetch_nodes(cur, ("data_flow_observation", "data_flow_step"))
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched = {src for src, _edge_type, _dst, _attrs in edges}
        return DataFlowEvidenceDiagnostics(
            eligible_evidence_nodes=len(evidence),
            matched_evidence_nodes=len(matched),
            emitted_edges=len(edges),
            unmatched_evidence_count=len(unmatched),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        evidence = _fetch_nodes(cur, ("data_flow_observation", "data_flow_step"))
        symbols = _SymbolIndex(_fetch_symbols(cur))
        assets = _TargetIndex(_fetch_nodes(cur, ("data_asset", "config_key")))
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for node in evidence:
            node_edges: list[tuple[str, str, str, dict[str, Any]]] = []
            symbol_match = _symbol_for_evidence(node, symbols)
            if symbol_match:
                symbol, resolution, resolution_attrs = symbol_match
                node_edges.append((
                    node.node_id,
                    "references",
                    symbol.node_id,
                    {
                        "resolution": resolution,
                        "path": _normalize_path(str(node.attrs.get("path") or "")),
                        "start_line": _int_or_none(node.attrs.get("start_line")) or 0,
                        "linker": self.name,
                        **resolution_attrs,
                    },
                ))
            for target, resolution in assets.targets_for_evidence(node):
                node_edges.append((
                    node.node_id,
                    "references",
                    target.node_id,
                    {
                        "resolution": resolution,
                        "hint": _target_hint(node.attrs),
                        "linker": self.name,
                        **resolution_method_attrs(resolution),
                    },
                ))
            if not node_edges:
                unmatched.append({
                    "node_id": node.node_id,
                    "subtype": node.subtype,
                    "path": node.attrs.get("path") or "",
                    "start_line": node.attrs.get("start_line") or "",
                })
                continue
            edges.extend(node_edges)
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
        self.asset_by_name: dict[str, list[_Node]] = {}
        self.config_by_key: dict[str, list[_Node]] = {}
        for target in targets:
            if target.subtype == "data_asset":
                for key in _target_name_keys(target.attrs.get("name")):
                    self.asset_by_name.setdefault(key, []).append(target)
            elif target.subtype == "config_key":
                for key in _target_name_keys(target.attrs.get("key")):
                    self.config_by_key.setdefault(key, []).append(target)

    def targets_for_evidence(self, node: _Node) -> list[tuple[_Node, str]]:
        out: list[tuple[_Node, str]] = []
        for key in ("asset_id", "data_asset_id"):
            value = str(node.attrs.get(key) or "")
            if value and value in self.by_id and self.by_id[value].subtype == "data_asset":
                out.append((self.by_id[value], key))
        for key in ("config_id", "config_key_id"):
            value = str(node.attrs.get(key) or "")
            if value and value in self.by_id and self.by_id[value].subtype == "config_key":
                out.append((self.by_id[value], key))

        asset_hint = str(node.attrs.get("asset_hint") or "").strip()
        out.extend((target, "asset_hint") for target in self._unique_assets(asset_hint))
        config_hint = str(
            node.attrs.get("config_key_hint")
            or node.attrs.get("config_hint")
            or "",
        ).strip()
        out.extend((target, "config_key_hint") for target in self._unique_configs(config_hint))

        deduped: list[tuple[_Node, str]] = []
        seen: set[str] = set()
        for target, resolution in out:
            if target.node_id in seen:
                continue
            seen.add(target.node_id)
            deduped.append((target, resolution))
        return deduped

    def _unique_assets(self, hint: str) -> list[_Node]:
        if not hint:
            return []
        matches = self.asset_by_name.get(_target_key(hint), [])
        return matches if len(matches) == 1 else []

    def _unique_configs(self, hint: str) -> list[_Node]:
        if not hint:
            return []
        matches = self.config_by_key.get(_target_key(hint), [])
        return matches if len(matches) == 1 else []


def _symbol_for_evidence(
    node: _Node,
    symbols: _SymbolIndex,
) -> tuple[_CodeSymbol, str, dict[str, str]] | None:
    semantic = symbols.by_semantic_attrs(node.attrs)
    if semantic:
        symbol, resolution_attrs = semantic
        return symbol, "semantic_identity", resolution_attrs

    path = _normalize_path(str(node.attrs.get("path") or ""))
    line = _int_or_none(node.attrs.get("start_line"))
    if not path or line is None:
        span = node.attrs.get("source_span")
        if isinstance(span, dict):
            path = _normalize_path(str(span.get("path") or path))
            line = _int_or_none(span.get("start_line") or span.get("line"))
    if path and line is not None:
        symbol = symbols.by_path_line(path, line)
        if symbol:
            return (
                symbol,
                "evidence_span_symbol",
                resolution_method_attrs("evidence_span_symbol"),
            )
    return None


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


def _target_hint(attrs: dict[str, Any]) -> str:
    for key in (
        "asset_id",
        "data_asset_id",
        "asset_hint",
        "config_id",
        "config_key_id",
        "config_key_hint",
        "config_hint",
    ):
        value = str(attrs.get(key) or "")
        if value:
            return value
    return ""


def _target_name_keys(value: Any) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {_target_key(raw), _target_key(raw.rsplit(".", 1)[-1])}


def _target_key(value: str) -> str:
    return value.strip().lower()


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
