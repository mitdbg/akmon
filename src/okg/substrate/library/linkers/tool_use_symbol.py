"""ToolUseSymbolLinker — deterministic tool event to symbol links."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers._incremental import (
    MAX_RELEVANCE_DIRTY_IDS,
    incremental_touches,
)
from okg.substrate.library.linkers.semantic_symbol import (
    CANONICAL_NAME_HINT_KEYS,
    SEMANTIC_KEY_HINT_KEYS,
    SemanticSymbolIndex,
    has_semantic_symbol_hint,
    resolution_method_attrs,
)


__all__ = ["ToolUseSymbolDiagnostics", "ToolUseSymbolLinker"]


_SED_RANGE_RE = re.compile(
    r"\bsed\s+-n\s+['\"]?(?P<start>\d+)(?:,(?P<end>\d+))?p['\"]?"
)
_FILE_LINE_RE = re.compile(
    r"(?P<path>[A-Za-z0-9_./-]+\.[A-Za-z0-9_+-]+):(?P<line>\d+)"
)


@dataclass(frozen=True)
class _LineSpan:
    start: int
    end: int
    method: str


@dataclass(frozen=True)
class _ToolEvent:
    node_id: str
    attrs: dict[str, Any]
    path: str
    span: _LineSpan | None


@dataclass(frozen=True)
class _CodeSymbol:
    node_id: str
    attrs: dict[str, Any]
    path: str
    start_line: int
    end_line: int
    kind: str


@dataclass(frozen=True)
class _MatchScope:
    event_ids: frozenset[str] | None = None
    paths: frozenset[str] | None = None
    semantic_keys: frozenset[str] | None = None
    canonical_names: frozenset[str] | None = None

    @property
    def empty(self) -> bool:
        return (
            self.event_ids == frozenset()
            and not self.paths
            and not self.semantic_keys
            and not self.canonical_names
        )


@dataclass(frozen=True)
class ToolUseSymbolDiagnostics:
    eligible_events: int
    matched_events: int
    emitted_edges: int
    unmatched_events: list[dict[str, Any]]

    @property
    def unmatched_event_count(self) -> int:
        return len(self.unmatched_events)

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible_events": self.eligible_events,
            "matched_events": self.matched_events,
            "emitted_edges": self.emitted_edges,
            "unmatched_event_count": self.unmatched_event_count,
            "unmatched_events": list(self.unmatched_events),
        }


class ToolUseSymbolLinker:
    """Link tool_use_event nodes to enclosing code_symbol nodes.

    The linker is conservative: line-based events must have a repo-relative
    ``file_path`` and a line hint. Events carrying semantic symbol identity
    can link without stale file/line hints because the key is already a
    source-backed symbol identity.
    """

    name = "tool_use_symbol"
    publisher_skip_when_clean = True
    reads_subtypes = ("tool_use_event", "code_symbol")
    emits_edge_types = ("affects",)
    requires_narrowings = (
        ("tool_use_event", "affects", "code_symbol"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        with conn.cursor() as cur:
            if not incremental_touches(
                cur,
                incremental,
                node_subtypes=self.reads_subtypes,
            ):
                return EnrichResult(enricher_name=self.name, n_edges_emitted=0)
            match_scope = _dirty_match_scope(cur, incremental)
            pairs, _unmatched = self._match_pairs(
                cur,
                scope=match_scope,
            )
            emitted, skipped = self._emit_edges(
                cur,
                pairs=pairs,
                generation_id=generation_id,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
        )

    def diagnostics(self, conn: psycopg.Connection) -> ToolUseSymbolDiagnostics:
        with conn.cursor() as cur:
            events = _fetch_eligible_events(cur)
            pairs, unmatched = self._match_pairs(cur)
        matched_events = {src for src, _dst, _attrs in pairs}
        return ToolUseSymbolDiagnostics(
            eligible_events=len(events),
            matched_events=len(matched_events),
            emitted_edges=len(pairs),
            unmatched_events=unmatched,
        )

    def _match_pairs(
        self,
        cur,
        *,
        scope: _MatchScope | None = None,
        only_event_ids: frozenset[str] | None = None,
    ) -> tuple[list[tuple[str, str, dict[str, Any]]], list[dict[str, Any]]]:
        if scope is not None and only_event_ids is not None:
            raise ValueError("pass either scope or only_event_ids, not both")
        if scope is None and only_event_ids is not None:
            scope = _MatchScope(event_ids=only_event_ids)
        if scope is not None and scope.empty:
            return [], []
        events = _fetch_eligible_events(
            cur,
            only_node_ids=scope.event_ids if scope else None,
            only_paths=scope.paths if scope else None,
            only_semantic_keys=scope.semantic_keys if scope else None,
            only_canonical_names=scope.canonical_names if scope else None,
        )
        event_paths = frozenset(event.path for event in events if event.path)
        event_semantic_keys = _semantic_hint_values(events, semantic=True)
        event_canonical_names = _semantic_hint_values(events, semantic=False)
        symbols_by_path = _fetch_symbols_by_path(
            cur,
            only_paths=event_paths if scope is not None else None,
            only_semantic_keys=(
                event_semantic_keys if scope is not None else None
            ),
            only_canonical_names=(
                event_canonical_names if scope is not None else None
            ),
        )
        all_symbols = [
            symbol
            for symbols in symbols_by_path.values()
            for symbol in symbols
        ]
        semantic_symbols = SemanticSymbolIndex(all_symbols)
        symbol_by_id = {symbol.node_id: symbol for symbol in all_symbols}
        pairs: list[tuple[str, str, dict[str, Any]]] = []
        unmatched: list[dict[str, Any]] = []

        for event in events:
            semantic = semantic_symbols.match_attrs(event.attrs)
            if semantic:
                symbol = symbol_by_id.get(semantic.node_id)
                if symbol:
                    pairs.append((
                        event.node_id,
                        symbol.node_id,
                        {
                            "file_path": event.path,
                            "line_start": event.span.start if event.span else 0,
                            "line_end": event.span.end if event.span else 0,
                            "line_match_method": (
                                event.span.method
                                if event.span else "semantic_identity"
                            ),
                            "symbol_start_line": symbol.start_line,
                            "symbol_end_line": symbol.end_line,
                            "symbol_kind": symbol.kind,
                            "resolution": "semantic_identity",
                            "linker": self.name,
                            **semantic.attrs,
                        },
                    ))
                    continue
            if event.span is None:
                unmatched.append({
                    "node_id": event.node_id,
                    "file_path": event.path,
                    "reason": "no_line_span_or_semantic_match",
                })
                continue
            candidates = _matching_symbols(
                event.span,
                symbols_by_path.get(event.path, []),
            )
            if not candidates:
                unmatched.append({
                    "node_id": event.node_id,
                    "file_path": event.path,
                    "line_start": event.span.start,
                    "line_end": event.span.end,
                    "reason": "no_symbol_span_match",
                })
                continue
            for symbol in candidates:
                pairs.append((
                    event.node_id,
                    symbol.node_id,
                    {
                        "file_path": event.path,
                        "line_start": event.span.start,
                        "line_end": event.span.end,
                        "line_match_method": event.span.method,
                        "symbol_start_line": symbol.start_line,
                        "symbol_end_line": symbol.end_line,
                        "symbol_kind": symbol.kind,
                        "resolution": "tool_span_symbol",
                        **resolution_method_attrs("tool_span_symbol"),
                        "linker": self.name,
                    },
                ))
        return pairs, unmatched

    def _emit_edges(
        self,
        cur,
        *,
        pairs: list[tuple[str, str, dict[str, Any]]],
        generation_id: int,
    ) -> tuple[int, int]:
        if not pairs:
            return 0, 0
        result = insert_deterministic_edges(
            cur,
            source=f"_enricher:{self.name}",
            candidates=[
                DerivedEdgeCandidate(
                    src=src,
                    edge_type="affects",
                    dst=dst,
                    attrs=attrs,
                    source_record_id={
                        "src_node_id": src,
                        "dst_node_id": dst,
                        "linker": self.name,
                    },
                    source_revision={"generation_id": generation_id},
                )
                for src, dst, attrs in sorted(
                    pairs,
                    key=lambda p: (p[0], p[1]),
                )
            ],
        )
        return result.inserted, result.skipped


def _fetch_eligible_events(
    cur,
    *,
    only_node_ids: frozenset[str] | None = None,
    only_paths: frozenset[str] | None = None,
    only_semantic_keys: frozenset[str] | None = None,
    only_canonical_names: frozenset[str] | None = None,
) -> list[_ToolEvent]:
    if only_node_ids is not None and not only_node_ids:
        return []
    if (
        only_paths is not None
        and not only_paths
        and only_semantic_keys is not None
        and not only_semantic_keys
        and only_canonical_names is not None
        and not only_canonical_names
    ):
        return []
    node_filter = ""
    params: dict[str, Any] = {}
    if only_node_ids is not None:
        node_filter = "AND node_id = ANY(:node_ids)"
        params["node_ids"] = sorted(only_node_ids)
    scope_filter = _event_scope_filter(
        params,
        only_paths=only_paths,
        only_semantic_keys=only_semantic_keys,
        only_canonical_names=only_canonical_names,
    )
    rows = _chronos.query(
        cur,
        f"""
        SELECT node_id, subtype, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'tool_use_event'
               {node_filter}
               {scope_filter}
        """,
        params,
    )
    if only_node_ids is not None and not rows:
        rows = _chronos.query(
            cur,
            """
            SELECT node_id, subtype, attrs
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": sorted(only_node_ids)},
        )
        rows = [
            row for row in rows
            if str(row.get("subtype") or "") == "tool_use_event"
        ]
    source_rows = [(row["node_id"], row["attrs"]) for row in rows]
    events: list[_ToolEvent] = []
    for node_id, attrs in source_rows:
        event_attrs = dict(attrs or {})
        path = _normalize_rel_path(str(event_attrs.get("file_path") or ""))
        has_semantic_hint = has_semantic_symbol_hint(event_attrs)
        if not path and not has_semantic_hint:
            continue
        span = _line_span_from_attrs(event_attrs, path=path)
        if span is None and not has_semantic_hint:
            continue
        events.append(_ToolEvent(
            node_id=str(node_id),
            attrs=event_attrs,
            path=path,
            span=span,
        ))
    return events


def _fetch_symbols_by_path(
    cur,
    *,
    only_paths: frozenset[str] | None = None,
    only_semantic_keys: frozenset[str] | None = None,
    only_canonical_names: frozenset[str] | None = None,
) -> dict[str, list[_CodeSymbol]]:
    if (
        only_paths is not None
        and not only_paths
        and only_semantic_keys is not None
        and not only_semantic_keys
        and only_canonical_names is not None
        and not only_canonical_names
    ):
        return {}
    params: dict[str, Any] = {}
    scope_filter = _symbol_scope_filter(
        params,
        only_paths=only_paths,
        only_semantic_keys=only_semantic_keys,
        only_canonical_names=only_canonical_names,
    )
    rows = _chronos.query(
        cur,
        f"""
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'code_symbol'
           AND attrs ? 'path'
           AND attrs ? 'start_line'
           AND attrs ? 'end_line'
               {scope_filter}
        """,
        params,
    )
    if only_paths is not None and not rows:
        rows = _chronos.query(
            cur,
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = 'code_symbol'
               AND attrs ? 'path'
               AND attrs ? 'start_line'
               AND attrs ? 'end_line'
            """,
        )
    source_rows = [(row["node_id"], row["attrs"]) for row in rows]
    out: dict[str, list[_CodeSymbol]] = {}
    for node_id, attrs in source_rows:
        symbol_attrs = dict(attrs or {})
        path = _normalize_rel_path(str(symbol_attrs.get("path") or ""))
        if not _symbol_in_scope(
            symbol_attrs,
            path=path,
            only_paths=only_paths,
            only_semantic_keys=only_semantic_keys,
            only_canonical_names=only_canonical_names,
        ):
            continue
        start = _int_or_none(symbol_attrs.get("start_line"))
        end = _int_or_none(symbol_attrs.get("end_line"))
        if not path or start is None or end is None:
            continue
        if end < start:
            start, end = end, start
        symbol = _CodeSymbol(
            node_id=str(node_id),
            attrs=symbol_attrs,
            path=path,
            start_line=start,
            end_line=end,
            kind=str(symbol_attrs.get("kind") or ""),
        )
        out.setdefault(path, []).append(symbol)
    for symbols in out.values():
        symbols.sort(
            key=lambda item: (
                item.end_line - item.start_line,
                item.start_line,
                item.node_id,
            )
        )
    return out


def _dirty_match_scope(
    cur,
    incremental: IncrementalContext | None,
) -> _MatchScope | None:
    if incremental is None or incremental.is_first_run:
        return None
    if incremental.last_status != "success":
        return None
    if incremental.current_catalog_version_id is not None:
        if incremental.prev_catalog_version_id is None:
            return None
        if (
            incremental.prev_catalog_version_id
            != incremental.current_catalog_version_id
        ):
            return None
    if not incremental.dirty_node_ids:
        return _MatchScope(event_ids=frozenset())
    rows = _fetch_dirty_nodes(cur, incremental.dirty_node_ids)
    if len(rows) != len(incremental.dirty_node_ids):
        return None
    dirty_by_subtype: dict[str, set[str]] = {}
    dirty_paths: set[str] = set()
    dirty_semantic_keys: set[str] = set()
    dirty_canonical_names: set[str] = set()
    for row in rows:
        subtype = str(row["subtype"])
        node_id = str(row["node_id"])
        dirty_by_subtype.setdefault(subtype, set()).add(node_id)
        if subtype == "code_symbol":
            attrs = dict(row.get("attrs") or {})
            path = _normalize_rel_path(str(attrs.get("path") or ""))
            if path:
                dirty_paths.add(path)
            semantic_key = _clean_attr(attrs.get("semantic_key"))
            if semantic_key:
                dirty_semantic_keys.add(semantic_key)
            canonical_name = _clean_attr(attrs.get("canonical_name"))
            if canonical_name:
                dirty_canonical_names.add(canonical_name)
    if dirty_by_subtype.get("code_symbol"):
        return _MatchScope(
            event_ids=frozenset(dirty_by_subtype.get("tool_use_event", set()))
            or None,
            paths=frozenset(dirty_paths),
            semantic_keys=frozenset(dirty_semantic_keys),
            canonical_names=frozenset(dirty_canonical_names),
        )
    return _MatchScope(
        event_ids=frozenset(dirty_by_subtype.get("tool_use_event", set())),
    )


def _fetch_dirty_nodes(
    cur,
    dirty_node_ids: frozenset[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    dirty_ids = sorted(dirty_node_ids)
    for index in range(0, len(dirty_ids), MAX_RELEVANCE_DIRTY_IDS):
        chunk = dirty_ids[index:index + MAX_RELEVANCE_DIRTY_IDS]
        out.extend(_chronos.query(
            cur,
            """
            SELECT node_id, subtype, attrs
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": chunk},
        ))
    return out


def _event_scope_filter(
    params: dict[str, Any],
    *,
    only_paths: frozenset[str] | None,
    only_semantic_keys: frozenset[str] | None,
    only_canonical_names: frozenset[str] | None,
) -> str:
    clauses: list[str] = []
    if only_paths:
        clauses.append("attrs->>'file_path' = ANY(:paths)")
        params["paths"] = sorted(only_paths)
    if only_semantic_keys:
        clauses.append(_json_any_clause(
            SEMANTIC_KEY_HINT_KEYS,
            param_name="semantic_keys",
        ))
        params["semantic_keys"] = sorted(only_semantic_keys)
    if only_canonical_names:
        clauses.append(_json_any_clause(
            CANONICAL_NAME_HINT_KEYS,
            param_name="canonical_names",
        ))
        params["canonical_names"] = sorted(only_canonical_names)
    if not clauses:
        return ""
    return "AND (" + " OR ".join(clauses) + ")"


def _symbol_scope_filter(
    params: dict[str, Any],
    *,
    only_paths: frozenset[str] | None,
    only_semantic_keys: frozenset[str] | None,
    only_canonical_names: frozenset[str] | None,
) -> str:
    clauses: list[str] = []
    if only_paths:
        clauses.append("attrs->>'path' = ANY(:paths)")
        params["paths"] = sorted(only_paths)
    if only_semantic_keys:
        clauses.append("attrs->>'semantic_key' = ANY(:semantic_keys)")
        params["semantic_keys"] = sorted(only_semantic_keys)
    if only_canonical_names:
        clauses.append("attrs->>'canonical_name' = ANY(:canonical_names)")
        params["canonical_names"] = sorted(only_canonical_names)
    if not clauses:
        return ""
    return "AND (" + " OR ".join(clauses) + ")"


def _json_any_clause(keys: tuple[str, ...], *, param_name: str) -> str:
    return "(" + " OR ".join(
        f"attrs->>'{key}' = ANY(:{param_name})"
        for key in keys
    ) + ")"


def _symbol_in_scope(
    attrs: dict[str, Any],
    *,
    path: str,
    only_paths: frozenset[str] | None,
    only_semantic_keys: frozenset[str] | None,
    only_canonical_names: frozenset[str] | None,
) -> bool:
    if (
        only_paths is None
        and only_semantic_keys is None
        and only_canonical_names is None
    ):
        return True
    if only_paths is not None and path in only_paths:
        return True
    semantic_key = _clean_attr(attrs.get("semantic_key"))
    if (
        only_semantic_keys is not None
        and semantic_key in only_semantic_keys
    ):
        return True
    canonical_name = _clean_attr(attrs.get("canonical_name"))
    return (
        only_canonical_names is not None
        and canonical_name in only_canonical_names
    )


def _semantic_hint_values(
    events: list[_ToolEvent],
    *,
    semantic: bool,
) -> frozenset[str]:
    keys = SEMANTIC_KEY_HINT_KEYS if semantic else CANONICAL_NAME_HINT_KEYS
    values: set[str] = set()
    for event in events:
        for key in keys:
            value = _clean_attr(event.attrs.get(key))
            if value:
                values.add(value)
    return frozenset(values)


def _clean_attr(value: Any) -> str:
    return str(value or "").strip()


def _line_span_from_attrs(
    attrs: dict[str, Any],
    *,
    path: str,
) -> _LineSpan | None:
    explicit = _explicit_line_span(attrs)
    if explicit is not None:
        return explicit
    text = "\n".join(
        str(attrs.get(key) or "")
        for key in ("command", "input_excerpt")
        if attrs.get(key)
    )
    if not text:
        return None

    for match in _FILE_LINE_RE.finditer(text):
        matched_path = _normalize_rel_path(match.group("path"))
        if matched_path == path or matched_path.endswith(f"/{path}"):
            line = int(match.group("line"))
            return _LineSpan(line, line, "file_line_text")

    match = _SED_RANGE_RE.search(text)
    if match and path in text:
        start = int(match.group("start"))
        end = int(match.group("end") or start)
        if end < start:
            start, end = end, start
        return _LineSpan(start, end, "sed_range")
    return None


def _explicit_line_span(attrs: dict[str, Any]) -> _LineSpan | None:
    for key in ("line", "line_number", "lineno"):
        line = _int_or_none(attrs.get(key))
        if line is not None:
            return _LineSpan(line, line, key)

    for start_key, end_key in (
        ("start_line", "end_line"),
        ("line_start", "line_end"),
        ("span_start", "span_end"),
        ("start", "end"),
    ):
        start = _int_or_none(attrs.get(start_key))
        end = _int_or_none(attrs.get(end_key))
        if start is not None:
            if end is None:
                end = start
            if end < start:
                start, end = end, start
            return _LineSpan(start, end, f"{start_key}:{end_key}")

    for key in ("location", "span", "range"):
        value = attrs.get(key)
        if isinstance(value, dict):
            start = _nested_line(value.get("start"))
            end = _nested_line(value.get("end"))
            if start is not None:
                if end is None:
                    end = start
                if end < start:
                    start, end = end, start
                return _LineSpan(start, end, key)
    return None


def _matching_symbols(
    span: _LineSpan,
    symbols: list[_CodeSymbol],
) -> list[_CodeSymbol]:
    non_module = [s for s in symbols if s.kind != "module"]
    candidates = [
        s for s in non_module
        if s.start_line <= span.end and s.end_line >= span.start
    ]
    if candidates:
        if span.start == span.end:
            containing = [
                s for s in candidates
                if s.start_line <= span.start <= s.end_line
            ]
            if containing:
                containing.sort(
                    key=lambda s: (
                        s.end_line - s.start_line,
                        s.start_line,
                        s.node_id,
                    )
                )
                return containing[:1]
        candidates.sort(
            key=lambda s: (
                s.start_line > span.end or s.end_line < span.start,
                s.end_line - s.start_line,
                s.start_line,
                s.node_id,
            )
        )
        return candidates[:25]

    module_candidates = [
        s for s in symbols
        if s.kind == "module"
        and s.start_line <= span.end
        and s.end_line >= span.start
    ]
    return module_candidates[:1]


def _nested_line(value: Any) -> int | None:
    if isinstance(value, dict):
        return _int_or_none(value.get("line") or value.get("row"))
    return _int_or_none(value)


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _normalize_rel_path(path: str) -> str:
    path = path.strip()
    if not path:
        return ""
    path = path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path.strip("/")
