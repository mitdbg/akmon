"""TestSymbolLinker — deterministic test evidence to symbol links."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from okg.substrate.enrichers.base import (
    EnrichResult,
    EnricherPrelockPlan,
    IncrementalContext,
)
from okg.substrate.library.linkers._prelock import PrelockPlannableLinker
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers._incremental import MAX_RELEVANCE_DIRTY_IDS
from okg.substrate.library.linkers.semantic_symbol import (
    SemanticSymbolIndex,
    resolution_method_attrs,
)


__all__ = ["TestSymbolDiagnostics", "TestSymbolLinker"]


@dataclass(frozen=True)
class _Node:
    node_id: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class _CodeSymbol:
    node_id: str
    path: str
    name: str
    start_line: int
    end_line: int
    kind: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class TestSymbolDiagnostics:
    eligible_test_cases: int
    matched_test_cases: int
    eligible_stack_frames: int
    matched_stack_frames: int
    emitted_edges: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_test_cases": self.eligible_test_cases,
            "matched_test_cases": self.matched_test_cases,
            "eligible_stack_frames": self.eligible_stack_frames,
            "matched_stack_frames": self.matched_stack_frames,
            "emitted_edges": self.emitted_edges,
        }


class TestSymbolLinker(PrelockPlannableLinker):
    """Link test_case/runtime_event/stack_frame nodes to code symbols."""

    name = "test_symbol"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "test_case",
        "runtime_event",
        "stack_frame",
        "code_symbol",
    )
    reads_edge_types = ("calls", "contains", "references")
    emits_edge_types = ("references", "points_to", "exercises")
    requires_narrowings = (
        ("test_case", "references", "code_symbol"),
        ("test_case", "exercises", "code_symbol"),
        ("runtime_event", "references", "code_symbol"),
        ("stack_frame", "points_to", "code_symbol"),
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
        if not _test_symbol_incremental_touches(cur, incremental):
            return None
        return self._resolve_edges(cur, skip_existing=True)

    def diagnostics(self, conn: psycopg.Connection) -> TestSymbolDiagnostics:
        with conn.cursor() as cur:
            test_cases = _fetch_nodes(cur, "test_case")
            stack_frames = _fetch_nodes(cur, "stack_frame")
            edges = self._resolve_edges(cur, skip_existing=False)
        matched_tests = {
            src for src, edge_type, _dst, _attrs in edges
            if edge_type == "references" and src.startswith("test-case:")
        }
        matched_frames = {
            src for src, edge_type, _dst, _attrs in edges
            if edge_type == "points_to" and src.startswith("stack-frame:")
        }
        return TestSymbolDiagnostics(
            eligible_test_cases=len(test_cases),
            matched_test_cases=len(matched_tests),
            eligible_stack_frames=len(stack_frames),
            matched_stack_frames=len(matched_frames),
            emitted_edges=len(edges),
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> list[tuple[str, str, str, dict[str, Any]]]:
        test_cases = _fetch_nodes(cur, "test_case")
        stack_frames = _fetch_nodes(cur, "stack_frame")
        symbols = _SymbolIndex(_fetch_symbols(cur))
        symbol_targets = _fetch_symbol_targets(cur)
        event_to_cases = _event_to_cases(cur)
        event_to_frames = _event_to_frames(cur)

        edges: list[tuple[str, str, str, dict[str, Any]]] = []

        frame_symbol: dict[str, tuple[str, dict[str, str]]] = {}
        for frame in stack_frames:
            path = _normalize_path(str(frame.attrs.get("path") or ""))
            line = _int_or_none(frame.attrs.get("line"))
            semantic = symbols.by_semantic_attrs(frame.attrs)
            if semantic:
                symbol, resolution_attrs = semantic
                resolution = "semantic_identity"
            elif path and line is not None:
                symbol = symbols.by_path_line(path, line)
                resolution = "enclosing_stack_frame_symbol"
                resolution_attrs = resolution_method_attrs(resolution)
            else:
                continue
            if not symbol:
                continue
            frame_symbol[frame.node_id] = (symbol.node_id, resolution_attrs)
            edge = (
                frame.node_id,
                "points_to",
                symbol.node_id,
                {
                    "line": line,
                    "file_path": path,
                    "resolution": resolution,
                    "linker": self.name,
                    **resolution_attrs,
                },
            )
            edges.append(edge)

        for event_id, frame_ids in event_to_frames.items():
            for frame_id in frame_ids:
                frame_match = frame_symbol.get(frame_id)
                if not frame_match:
                    continue
                symbol_id, frame_resolution_attrs = frame_match
                edge = (
                    event_id,
                    "references",
                    symbol_id,
                    {
                        "resolution": "stack_frame_symbol",
                        "stack_frame_id": frame_id,
                        "linker": self.name,
                        **frame_resolution_attrs,
                    },
                )
                edges.append(edge)

        case_symbol_attrs: dict[str, dict[str, dict[str, str]]] = {}
        case_exercised_symbol_attrs: dict[str, dict[str, dict[str, str]]] = {}
        for case in test_cases:
            path = _test_case_path(case.attrs)
            name = str(case.attrs.get("name") or "")
            semantic = symbols.by_semantic_attrs(case.attrs)
            if semantic:
                symbol, resolution_attrs = semantic
            else:
                symbol = symbols.by_path_name(path, name) if path and name else None
                resolution_attrs = resolution_method_attrs("test_case_path_name")
            if symbol:
                case_symbol_attrs.setdefault(case.node_id, {})[symbol.node_id] = (
                    resolution_attrs
                )
                for target_id in symbol_targets.get(symbol.node_id, set()):
                    target = symbols.by_id.get(target_id)
                    if target is None:
                        continue
                    if _is_test_path(target.path):
                        continue
                    case_exercised_symbol_attrs.setdefault(case.node_id, {})[
                        target_id
                    ] = resolution_attrs
            for event_id, case_ids in event_to_cases.items():
                if case.node_id not in case_ids:
                    continue
                for frame_id in event_to_frames.get(event_id, set()):
                    frame_match = frame_symbol.get(frame_id)
                    if frame_match:
                        symbol_id, frame_resolution_attrs = frame_match
                        case_symbol_attrs.setdefault(case.node_id, {})[
                            symbol_id
                        ] = frame_resolution_attrs

        for case_id, symbol_ids in sorted(case_symbol_attrs.items()):
            for symbol_id, resolution_attrs in sorted(symbol_ids.items()):
                edge = (
                    case_id,
                    "references",
                    symbol_id,
                    {
                        "resolution": "test_case_file_or_stack_frame",
                        "linker": self.name,
                        **resolution_attrs,
                    },
                )
                edges.append(edge)
        for case_id, symbol_ids in sorted(case_exercised_symbol_attrs.items()):
            for symbol_id, resolution_attrs in sorted(symbol_ids.items()):
                edge = (
                    case_id,
                    "exercises",
                    symbol_id,
                    {
                        "resolution": "test_code_symbol_reference",
                        "linker": self.name,
                        **resolution_attrs,
                    },
                )
                edges.append(edge)
        if skip_existing:
            edges = _filter_new_edges(cur, edges)
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


_DIRECT_TEST_SYMBOL_INPUT_SUBTYPES = frozenset({
    "test_case",
    "runtime_event",
    "stack_frame",
})


def _test_symbol_incremental_touches(
    cur,
    incremental: IncrementalContext | None,
) -> bool:
    """Return whether a dirty set can affect test-symbol links.

    Ordinary code-structure publishes touch many structural ``contains`` /
    ``references`` edges. Those should not force a global scan of every test
    case, stack frame, and symbol. Direct test evidence and any ``code_symbol``
    node change can affect this linker: stack frames resolve through
    path/line/semantic symbol indexes, so even non-test symbol edits fail open.
    Unknown and deleted inputs also fail open.
    """

    if incremental is None or incremental.is_first_run:
        return True
    if incremental.last_status != "success":
        return True
    if incremental.current_catalog_version_id is not None:
        if incremental.prev_catalog_version_id is None:
            return True
        if (
            incremental.prev_catalog_version_id
            != incremental.current_catalog_version_id
        ):
            return True
    if (
        len(incremental.dirty_node_ids) > MAX_RELEVANCE_DIRTY_IDS
        or len(incremental.dirty_edge_ids) > MAX_RELEVANCE_DIRTY_IDS
    ):
        return True

    if incremental.dirty_node_ids:
        rows = _dirty_test_symbol_nodes(cur, incremental.dirty_node_ids)
        if len(rows) != len(incremental.dirty_node_ids):
            return True
        for row in rows:
            subtype = str(row.get("subtype") or "")
            if subtype in _DIRECT_TEST_SYMBOL_INPUT_SUBTYPES:
                return True
            if subtype == "code_symbol":
                return True

    if incremental.dirty_edge_ids:
        rows = _dirty_test_symbol_edges(cur, incremental.dirty_edge_ids)
        if len(rows) != len(incremental.dirty_edge_ids):
            return True
        for row in rows:
            if _test_symbol_edge_is_relevant(row):
                return True

    return False


def _dirty_test_symbol_nodes(
    cur,
    dirty_node_ids: frozenset[str],
) -> list[dict[str, Any]]:
    return _chronos.query(
        cur,
        """
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE node_id = ANY(:node_ids)
        """,
        {"node_ids": sorted(dirty_node_ids)},
    )


def _dirty_test_symbol_edges(
    cur,
    dirty_edge_ids: frozenset[int],
) -> list[dict[str, Any]]:
    return _chronos.query(
        cur,
        """
        SELECT e.edge_id,
               e.edge_type,
               e.src,
               e.dst,
               src.subtype AS src_subtype,
               src.attrs AS src_attrs,
               dst.subtype AS dst_subtype,
               dst.attrs AS dst_attrs
          FROM okg.graph_edges e
          LEFT JOIN okg.graph_nodes src ON src.node_id = e.src
          LEFT JOIN okg.graph_nodes dst ON dst.node_id = e.dst
         WHERE e.edge_id = ANY(:edge_ids)
        """,
        {"edge_ids": sorted(dirty_edge_ids)},
    )


def _test_symbol_edge_is_relevant(row: dict[str, Any]) -> bool:
    edge_type = str(row.get("edge_type") or "")
    src_subtype = str(row.get("src_subtype") or "")
    dst_subtype = str(row.get("dst_subtype") or "")
    if not src_subtype or not dst_subtype:
        return True
    if (
        edge_type == "references"
        and src_subtype == "runtime_event"
        and dst_subtype == "test_case"
    ):
        return True
    if (
        edge_type == "contains"
        and src_subtype == "runtime_event"
        and dst_subtype == "stack_frame"
    ):
        return True
    if (
        edge_type in {"references", "calls"}
        and src_subtype == "code_symbol"
        and dst_subtype == "code_symbol"
    ):
        src_attrs = row.get("src_attrs") or {}
        return _is_test_path(_normalize_path(str(src_attrs.get("path") or "")))
    return bool(
        edge_type in {"references", "contains", "calls"}
        and (
            src_subtype in _DIRECT_TEST_SYMBOL_INPUT_SUBTYPES
            or dst_subtype in _DIRECT_TEST_SYMBOL_INPUT_SUBTYPES
        )
    )


class _SymbolIndex:
    def __init__(self, symbols: list[_CodeSymbol]) -> None:
        self.by_id: dict[str, _CodeSymbol] = {}
        self.semantic_symbols = SemanticSymbolIndex(symbols)
        self.path_symbols: dict[str, list[_CodeSymbol]] = {}
        self.path_name: dict[tuple[str, str], list[_CodeSymbol]] = {}
        for symbol in symbols:
            self.by_id[symbol.node_id] = symbol
            self.path_symbols.setdefault(symbol.path, []).append(symbol)
            self.path_name.setdefault((symbol.path, symbol.name), []).append(symbol)
        for items in self.path_symbols.values():
            items.sort(key=lambda s: (s.end_line - s.start_line, s.start_line, s.node_id))

    def by_path_line(self, path: str, line: int) -> _CodeSymbol | None:
        candidates = [
            s for s in self.path_symbols.get(path, [])
            if s.start_line <= line <= s.end_line
        ]
        non_module = [s for s in candidates if s.kind != "module"]
        return (non_module or candidates or [None])[0]

    def by_path_name(self, path: str, name: str) -> _CodeSymbol | None:
        matches = self.path_name.get((path, name), [])
        if len(matches) == 1:
            return matches[0]
        return None

    def by_semantic_attrs(
        self,
        attrs: dict[str, Any],
    ) -> tuple[_CodeSymbol, dict[str, str]] | None:
        match = self.semantic_symbols.match_attrs(attrs)
        if not match:
            return None
        symbol = self.by_id.get(match.node_id)
        if not symbol:
            return None
        return symbol, match.attrs


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
        if not path or start is None or end is None or not name:
            continue
        out.append(_CodeSymbol(
            node_id=node.node_id,
            path=path,
            name=name,
            start_line=min(start, end),
            end_line=max(start, end),
            kind=str(node.attrs.get("kind") or ""),
            attrs=node.attrs,
        ))
    return out


def _event_to_cases(cur) -> dict[str, set[str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT src, dst
          FROM okg.graph_edges
         WHERE edge_type = 'references'
           AND src LIKE 'runtime-event:%'
           AND dst LIKE 'test-case:%'
        """,
    )
    out: dict[str, set[str]] = {}
    for row in rows:
        out.setdefault(str(row["src"]), set()).add(str(row["dst"]))
    return out


def _event_to_frames(cur) -> dict[str, set[str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT src, dst
          FROM okg.graph_edges
         WHERE edge_type = 'contains'
           AND src LIKE 'runtime-event:%'
           AND dst LIKE 'stack-frame:%'
        """,
    )
    out: dict[str, set[str]] = {}
    for row in rows:
        out.setdefault(str(row["src"]), set()).add(str(row["dst"]))
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


def _fetch_symbol_targets(cur) -> dict[str, set[str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT src, dst
          FROM okg.graph_edges
         WHERE edge_type = ANY(:edge_types)
           AND src LIKE 'symbol:%'
           AND dst LIKE 'symbol:%'
        """,
        {"edge_types": ["references", "calls"]},
    )
    out: dict[str, set[str]] = {}
    for row in rows:
        out.setdefault(str(row["src"]), set()).add(str(row["dst"]))
    return out


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)


def _normalize_path(path: str) -> str:
    path = path.replace("\\", "/").strip().strip("/")
    while path.startswith("./"):
        path = path[2:]
    return path


def _test_case_path(attrs: dict[str, Any]) -> str:
    explicit = _normalize_path(str(attrs.get("file") or ""))
    if explicit:
        return explicit
    classname = str(attrs.get("classname") or "")
    if not classname:
        return ""
    return _normalize_path(classname.replace(".", "/") + ".py")


def _is_test_path(path: str) -> bool:
    return (
        path.startswith("tests/")
        or "/tests/" in path
        or Path(path).name.startswith("test_")
    )


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None
