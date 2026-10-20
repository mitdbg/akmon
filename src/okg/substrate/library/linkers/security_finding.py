"""SecurityFindingLinker — deterministic links for scanner evidence."""
from __future__ import annotations

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


__all__ = ["SecurityFindingDiagnostics", "SecurityFindingLinker"]


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
class SecurityFindingDiagnostics:
    eligible_findings: int
    matched_findings: int
    emitted_edges: int
    unmatched_finding_count: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_findings": self.eligible_findings,
            "matched_findings": self.matched_findings,
            "emitted_edges": self.emitted_edges,
            "unmatched_finding_count": self.unmatched_finding_count,
        }


class SecurityFindingLinker(PrelockPlannableLinker):
    """Link quality/security/secret/vulnerability nodes to deterministic targets."""

    name = "security_finding"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "code_quality_finding",
        "security_finding",
        "secret_finding",
        "dependency_vulnerability",
        "source_file",
        "code_symbol",
        "dependency_package",
        "data_asset",
        "config_key",
    )
    emits_edge_types = ("affects", "references")
    requires_narrowings = (
        ("code_quality_finding", "affects", "source_file"),
        ("code_quality_finding", "affects", "code_symbol"),
        ("security_finding", "affects", "source_file"),
        ("security_finding", "affects", "code_symbol"),
        ("security_finding", "affects", "dependency_package"),
        ("security_finding", "references", "data_asset"),
        ("secret_finding", "affects", "source_file"),
        ("secret_finding", "references", "config_key"),
        ("secret_finding", "references", "data_asset"),
        ("dependency_vulnerability", "affects", "dependency_package"),
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
        del timings_ms
        if _can_use_incremental(incremental):
            edges, _unmatched = self._resolve_edges_incremental(
                cur,
                incremental=incremental,
                skip_existing=True,
            )
        else:
            edges, _unmatched = self._resolve_edges(
                cur,
                skip_existing=True,
            )
        return edges

    def diagnostics(self, conn: psycopg.Connection) -> SecurityFindingDiagnostics:
        with conn.cursor() as cur:
            findings = _fetch_nodes(cur, (
                "code_quality_finding",
                "security_finding",
                "secret_finding",
                "dependency_vulnerability",
            ))
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched = {src for src, _edge_type, _dst, _attrs in edges}
        return SecurityFindingDiagnostics(
            eligible_findings=len(findings),
            matched_findings=len(matched),
            emitted_edges=len(edges),
            unmatched_finding_count=len(unmatched),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        findings = _fetch_nodes(cur, (
            "code_quality_finding",
            "security_finding",
            "secret_finding",
            "dependency_vulnerability",
        ))
        files = _FileIndex(_fetch_nodes(cur, ("source_file",)))
        symbols = _SymbolIndex(_fetch_symbols(cur))
        packages = _PackageIndex(_fetch_nodes(cur, ("dependency_package",)))
        targets = _DataTargetIndex(_fetch_nodes(cur, ("data_asset", "config_key")))
        return self._resolve_edges_for(
            cur,
            findings=findings,
            files=files,
            symbols=symbols,
            packages=packages,
            targets=targets,
            skip_existing=skip_existing,
        )

    def _resolve_edges_incremental(
        self,
        cur,
        *,
        incremental: IncrementalContext,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        dirty = _fetch_nodes_by_ids(
            cur,
            node_ids=incremental.dirty_node_ids,
            subtypes=self.reads_subtypes,
        )
        if not dirty:
            return [], []

        finding_subtypes = (
            "code_quality_finding",
            "dependency_vulnerability",
            "secret_finding",
            "security_finding",
        )
        dirty_findings = [
            node for node in dirty
            if node.subtype in finding_subtypes
        ]
        dirty_targets = [
            node for node in dirty
            if node.subtype not in finding_subtypes
        ]
        if dirty_findings:
            findings = dirty_findings
            files = _FileIndex(_fetch_nodes(cur, ("source_file",)))
            symbols = _SymbolIndex(_fetch_symbols(cur))
            packages = _PackageIndex(_fetch_nodes(cur, ("dependency_package",)))
            targets = _DataTargetIndex(
                _fetch_nodes(cur, ("data_asset", "config_key"))
            )
        else:
            findings = _fetch_nodes(cur, finding_subtypes)
            files = _FileIndex([
                node for node in dirty_targets
                if node.subtype == "source_file"
            ])
            symbols = _SymbolIndex(_nodes_to_symbols([
                node for node in dirty_targets
                if node.subtype == "code_symbol"
            ]))
            packages = _PackageIndex([
                node for node in dirty_targets
                if node.subtype == "dependency_package"
            ])
            targets = _DataTargetIndex([
                node for node in dirty_targets
                if node.subtype in {"data_asset", "config_key"}
            ])
        return self._resolve_edges_for(
            cur,
            findings=findings,
            files=files,
            symbols=symbols,
            packages=packages,
            targets=targets,
            skip_existing=skip_existing,
        )

    def _resolve_edges_for(
        self,
        cur,
        *,
        findings: list[_Node],
        files: _FileIndex,
        symbols: _SymbolIndex,
        packages: _PackageIndex,
        targets: _DataTargetIndex,
        skip_existing: bool,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for finding in findings:
            node_edges: list[tuple[str, str, str, dict[str, Any]]] = []
            if finding.subtype in {
                "code_quality_finding",
                "security_finding",
                "secret_finding",
            }:
                path = _normalize_path(str(finding.attrs.get("path") or ""))
                line = _int_or_none(finding.attrs.get("start_line"))
                source_file = files.by_path(path)
                if source_file:
                    node_edges.append(_edge(
                        finding,
                        "affects",
                        source_file,
                        "path_source_file",
                    ))
                if finding.subtype == "security_finding":
                    semantic = symbols.by_semantic_attrs(finding.attrs)
                    if semantic:
                        symbol, resolution_attrs = semantic
                        node_edges.append(_edge(
                            finding,
                            "affects",
                            _Node(symbol.node_id, "code_symbol", {}),
                            "semantic_identity",
                            resolution_attrs,
                        ))
                    elif path and line is not None:
                        symbol = symbols.by_path_line(path, line)
                        if symbol:
                            node_edges.append(_edge(
                                finding,
                                "affects",
                                _Node(symbol.node_id, "code_symbol", {}),
                                "file_span_code_symbol",
                            ))
                elif (
                    finding.subtype == "code_quality_finding"
                    and path
                    and line is not None
                ):
                    symbol = symbols.by_path_line(path, line)
                    if symbol:
                        node_edges.append(_edge(
                            finding,
                            "affects",
                            _Node(symbol.node_id, "code_symbol", {}),
                            "file_span_code_symbol",
                        ))
                if finding.subtype == "security_finding":
                    package = packages.for_finding(finding)
                    if package:
                        node_edges.append(_edge(
                            finding,
                            "affects",
                            package,
                            "package_hint",
                        ))
                if finding.subtype == "secret_finding":
                    for target, resolution in targets.for_secret(finding):
                        node_edges.append(_edge(finding, "references", target, resolution))
                if finding.subtype == "security_finding":
                    for target, resolution in targets.for_security(finding):
                        node_edges.append(_edge(finding, "references", target, resolution))

            if finding.subtype == "dependency_vulnerability":
                package = packages.for_finding(finding)
                if package:
                    node_edges.append(_edge(
                        finding,
                        "affects",
                        package,
                        "package_id_or_name",
                    ))

            if not node_edges:
                unmatched.append({
                    "node_id": finding.node_id,
                    "subtype": finding.subtype,
                    "path": finding.attrs.get("path") or "",
                    "package": finding.attrs.get("package_name") or "",
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


class _FileIndex:
    def __init__(self, files: list[_Node]) -> None:
        self.by_normalized_path = {
            _normalize_path(str(file.attrs.get("path") or "")): file
            for file in files
        }

    def by_path(self, path: str) -> _Node | None:
        return self.by_normalized_path.get(path)


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


class _PackageIndex:
    def __init__(self, packages: list[_Node]) -> None:
        self.by_id = {package.node_id: package for package in packages}
        self.by_name: dict[tuple[str, str], list[_Node]] = {}
        for package in packages:
            name = _package_key(package.attrs.get("name"))
            ecosystem = _ecosystem_key(package.attrs.get("ecosystem"))
            self.by_name.setdefault((ecosystem, name), []).append(package)
            self.by_name.setdefault(("", name), []).append(package)

    def for_finding(self, finding: _Node) -> _Node | None:
        package_id = str(finding.attrs.get("package_id") or "")
        if package_id and package_id in self.by_id:
            return self.by_id[package_id]
        name = _package_key(finding.attrs.get("package_name") or finding.attrs.get("package"))
        if not name:
            return None
        ecosystem = _ecosystem_key(finding.attrs.get("ecosystem"))
        for key in ((ecosystem, name), ("", name)):
            matches = self.by_name.get(key, [])
            if len(matches) == 1:
                return matches[0]
        return None


class _DataTargetIndex:
    def __init__(self, targets: list[_Node]) -> None:
        self.by_id = {target.node_id: target for target in targets}
        self.config_by_key: dict[str, list[_Node]] = {}
        self.asset_by_name: dict[str, list[_Node]] = {}
        for target in targets:
            if target.subtype == "config_key":
                key = _target_key(target.attrs.get("key"))
                self.config_by_key.setdefault(key, []).append(target)
            elif target.subtype == "data_asset":
                for key in _target_name_keys(target.attrs.get("name")):
                    self.asset_by_name.setdefault(key, []).append(target)

    def for_secret(self, finding: _Node) -> list[tuple[_Node, str]]:
        out: list[tuple[_Node, str]] = []
        for attr, subtype in (("config_id", "config_key"), ("asset_id", "data_asset")):
            target_id = str(finding.attrs.get(attr) or "")
            target = self.by_id.get(target_id)
            if target and target.subtype == subtype:
                out.append((target, attr))
        secret_key = _secret_key_hint(finding)
        config = self._unique_config(secret_key)
        if config:
            out.append((config, "redacted_context_key"))
        asset = self._unique_asset(_normalize_path(str(finding.attrs.get("path") or "")))
        if asset:
            out.append((asset, "path_data_asset"))
        return _dedupe_targets(out)

    def for_security(self, finding: _Node) -> list[tuple[_Node, str]]:
        out: list[tuple[_Node, str]] = []
        target_id = str(finding.attrs.get("asset_id") or finding.attrs.get("data_asset_id") or "")
        target = self.by_id.get(target_id)
        if target and target.subtype == "data_asset":
            out.append((target, "asset_id"))
        asset = self._unique_asset(str(finding.attrs.get("asset_hint") or ""))
        if asset:
            out.append((asset, "asset_hint"))
        return _dedupe_targets(out)

    def _unique_config(self, key: str) -> _Node | None:
        if not key:
            return None
        matches = self.config_by_key.get(_target_key(key), [])
        return matches[0] if len(matches) == 1 else None

    def _unique_asset(self, name: str) -> _Node | None:
        if not name:
            return None
        matches = self.asset_by_name.get(_target_key(name), [])
        return matches[0] if len(matches) == 1 else None


def _edge(
    finding: _Node,
    edge_type: str,
    target: _Node,
    resolution: str,
    extra_attrs: dict[str, str] | None = None,
) -> tuple[str, str, str, dict[str, Any]]:
    return (
        finding.node_id,
        edge_type,
        target.node_id,
        {
            "resolution": resolution,
            "linker": SecurityFindingLinker.name,
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


def _fetch_nodes_by_ids(
    cur,
    *,
    node_ids: frozenset[str],
    subtypes: tuple[str, ...],
) -> list[_Node]:
    if not node_ids:
        return []
    rows = _chronos.query(
        cur,
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
           AND subtype = ANY(:subtypes)
        """,
        {
            "node_ids": list(node_ids),
            "subtypes": list(subtypes),
        },
    )
    return [
        _Node(str(row["node_id"]), str(row["subtype"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _fetch_symbols(cur) -> list[_CodeSymbol]:
    rows = _chronos.nodes(cur, ("code_symbol",))
    return _nodes_to_symbols([
        _Node(str(row["node_id"]), "code_symbol", dict(row["attrs"] or {}))
        for row in rows
    ])


def _nodes_to_symbols(nodes: list[_Node]) -> list[_CodeSymbol]:
    out: list[_CodeSymbol] = []
    for node in nodes:
        attrs = dict(node.attrs or {})
        path = _normalize_path(str(attrs.get("path") or ""))
        start = _int_or_none(attrs.get("start_line"))
        end = _int_or_none(attrs.get("end_line"))
        if not path or start is None or end is None:
            continue
        out.append(_CodeSymbol(
            node_id=node.node_id,
            path=path,
            start_line=min(start, end),
            end_line=max(start, end),
            kind=str(attrs.get("kind") or ""),
            attrs=attrs,
        ))
    return out


def _can_use_incremental(incremental: IncrementalContext | None) -> bool:
    return (
        incremental is not None
        and not incremental.is_first_run
        and len(incremental.dirty_node_ids) <= 10_000
    )


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


def _dedupe_targets(targets: list[tuple[_Node, str]]) -> list[tuple[_Node, str]]:
    out: list[tuple[_Node, str]] = []
    seen: set[str] = set()
    for target, resolution in targets:
        if target.node_id in seen:
            continue
        seen.add(target.node_id)
        out.append((target, resolution))
    return out


def _secret_key_hint(finding: _Node) -> str:
    for key in ("key", "config_key", "config_key_hint"):
        value = str(finding.attrs.get(key) or "").strip()
        if value:
            return value
    context = str(finding.attrs.get("redacted_context") or "")
    match = re.match(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=", context)
    return match.group(1) if match else ""


def _target_name_keys(value: Any) -> set[str]:
    raw = str(value or "").strip()
    if not raw:
        return set()
    return {_target_key(raw), _target_key(raw.rsplit("/", 1)[-1])}


def _target_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _package_key(value: Any) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _ecosystem_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return {
        "pypi": "python",
        "python": "python",
        "npm": "npm",
        "node": "npm",
        "javascript": "npm",
        "golang": "go",
    }.get(raw, raw)


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
