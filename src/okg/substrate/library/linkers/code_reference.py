"""CodeReferenceResolver — deterministic code import/reference links."""
from __future__ import annotations
from okg.substrate.library.sources._code_graph_common import _lexical_symbol_name

import json
import re
import time
import uuid
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from typing import Any

import psycopg

from okg.substrate.enrichers.base import (
    EnricherPrelockPlan,
    EnrichResult,
    IncrementalContext,
)
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers._prelock import PrelockPlannableLinker
from okg.substrate.library.linkers._incremental import MAX_RELEVANCE_DIRTY_IDS
from okg.substrate.library.linkers.semantic_symbol import (
    CANONICAL_NAME_HINT_KEYS,
    SEMANTIC_KEY_HINT_KEYS,
    SemanticSymbolIndex,
    resolution_method_attrs,
)
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from okg.substrate.transform.programs import enrichment_program


__all__ = ["CodeReferenceDiagnostics", "CodeReferenceResolver"]


#: Rows per INSERT when the resolver writes its edge facts (issue #2941). The
#: publisher's connection is in autocommit mode, so one INSERT per edge was one
#: transaction per edge: the okg repository's 331,558 edges cost 331,558
#: commits and about 380 s of the publish lock. A batch keeps each statement
#: small next to the publish statement timeout.
_EMIT_BATCH_ROWS = 5000

#: Commit witness stamped on every edge fact this linker writes (issue #2941).
#: The column default reads the publisher's okg.source_program_checksum
#: setting, which has not always survived to this write: a transaction-local
#: setting on an autocommit connection ends with its own statement, and a
#: Chronos checkout read clears it. An empty witness is refused by
#: okg.enforce_commit_witness under the runtime role. Writing the checksum
#: explicitly keeps the rows witnessed however the caller set it up.
_ENRICHMENT_WITNESS = enrichment_program().checksum()


_BUILTIN_PYTHON_NAMES = frozenset({
    "bool",
    "bytes",
    "dict",
    "enumerate",
    "float",
    "int",
    "len",
    "list",
    "max",
    "min",
    "object",
    "open",
    "print",
    "range",
    "set",
    "str",
    "sum",
    "super",
    "tuple",
    "type",
})


def _record_timing(timings_ms: dict[str, float] | None, name: str, started: float) -> None:
    if timings_ms is None:
        return
    elapsed = max(0.0, (time.perf_counter() - started) * 1000.0)
    timings_ms[name] = round(timings_ms.get(name, 0.0) + elapsed, 3)


@dataclass(frozen=True)
class _Node:
    node_id: str
    attrs: dict[str, Any]


@dataclass
class _Resolution:
    """What one resolution read and derived, for the retraction step.

    ``complete`` stays false unless ``_resolve_edges`` ran to the end, so a
    run that resolved nothing retracts nothing.
    """

    complete: bool = False
    full: bool = False
    derived: set[tuple[str, str, str]] = dataclass_field(default_factory=set)
    reference_ids: set[str] = dataclass_field(default_factory=set)
    import_ids: set[str] = dataclass_field(default_factory=set)
    callers: set[str] = dataclass_field(default_factory=set)
    live_ids: set[str] = dataclass_field(default_factory=set)


@dataclass(frozen=True)
class _ResolutionScope:
    import_ids: frozenset[str] = frozenset()
    reference_ids: frozenset[str] = frozenset()
    paths: frozenset[str] = frozenset()
    semantic_keys: frozenset[str] = frozenset()
    canonical_names: frozenset[str] = frozenset()
    names: frozenset[str] = frozenset()

    @property
    def empty(self) -> bool:
        return (
            not self.import_ids
            and not self.reference_ids
            and not self.paths
            and not self.semantic_keys
            and not self.canonical_names
            and not self.names
        )


@dataclass(frozen=True)
class CodeReferenceDiagnostics:
    eligible_imports: int
    matched_imports: int
    eligible_references: int
    matched_references: int
    emitted_edges: int
    unmatched_imports: list[dict[str, Any]]
    unmatched_references: list[dict[str, Any]]
    unmatched_callers: list[dict[str, Any]] = dataclass_field(
        default_factory=list,
    )

    def as_dict(self) -> dict[str, Any]:
        return {
            "eligible_imports": self.eligible_imports,
            "matched_imports": self.matched_imports,
            "eligible_references": self.eligible_references,
            "matched_references": self.matched_references,
            "emitted_edges": self.emitted_edges,
            "unmatched_imports": list(self.unmatched_imports),
            "unmatched_references": list(self.unmatched_references),
            "unmatched_callers": list(self.unmatched_callers),
        }


class CodeReferenceResolver(PrelockPlannableLinker):
    """Resolve code imports/references to canonical graph nodes.

    The resolver intentionally uses only deterministic local evidence:
    package manifests for dependency imports, and unique code-symbol
    matches by file/path/name for symbol references. Ambiguous names stay
    unresolved instead of linking to a likely but unproven target.
    """

    name = "code_reference"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "code_import",
        "code_reference",
        "code_symbol",
        "dependency_package",
    )
    emits_edge_types = ("references", "calls")
    requires_narrowings = (
        ("code_import", "references", "dependency_package"),
        ("code_reference", "references", "code_symbol"),
        ("code_symbol", "calls", "code_symbol"),
    )
    # Issue 2941 fix 1: reads only the branch graph and its own prior
    # edge facts, and is deterministic, so the publisher may compute its
    # output before the publish lock (see linkers/_prelock.py).
    _prelock_insert_in_transaction = True

    def _resolve_for_enrich(
        self,
        cur,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], _Resolution] | None:
        """The edges ``enrich`` emits and what the resolution read (for the
        stale-declaration step, item 92a), or None when its scope is empty."""
        started = time.perf_counter()
        scope = _dirty_resolution_scope(cur, incremental)
        _record_timing(timings_ms, "resolve_edges.scope", started)
        if scope is not None and scope.empty:
            return None
        started = time.perf_counter()
        resolution = _Resolution()
        edges, _ = self._resolve_edges(
            cur,
            skip_existing=True,
            scope=scope,
            timings_ms=timings_ms,
            resolution=resolution,
        )
        _record_timing(timings_ms, "resolve_edges", started)
        return edges, resolution

    def _prelock_resolve(
        self,
        cur,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> tuple[list[tuple[Any, ...]] | None, Any]:
        resolved = self._resolve_for_enrich(cur, incremental, timings_ms)
        if resolved is None:
            return None, None
        return resolved

    def _prelock_edges(
        self,
        cur,
        incremental: IncrementalContext | None,
        timings_ms: dict[str, float],
    ) -> list[tuple[Any, ...]] | None:
        return self._prelock_resolve(cur, incremental, timings_ms)[0]

    def _finish_from_plan(
        self,
        cur,
        plan: EnricherPrelockPlan,
        *,
        generation_id: int,
        timings_ms: dict[str, float],
    ) -> int:
        """Item 92a under the lock: compute the stale declarations from the
        pre-lock resolution (its inputs are unchanged, or the plan would not
        be offered) against the declarations as they are now, and retract
        them."""
        if plan.linker_state is None:
            return 0
        started = time.perf_counter()
        stale = _stale_declarations(cur, plan.linker_state)
        _record_timing(timings_ms, "resolve_edges.stale_declarations", started)
        retracted = 0
        if stale:
            started = time.perf_counter()
            retracted = self._emit_retractions(
                cur, edges=stale, generation_id=generation_id,
            )
            _record_timing(timings_ms, "emit_retractions", started)
        timings_ms["emit_retractions.count"] = float(retracted)
        return retracted

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
        prelock_plan: EnricherPrelockPlan | None = None,
    ) -> EnrichResult:
        timings_ms: dict[str, float] = {}
        with conn.cursor() as cur:
            if prelock_plan is not None:
                return self._enrich_from_plan(
                    cur,
                    prelock_plan,
                    generation_id=generation_id,
                    incremental=incremental,
                )
            resolved = self._resolve_for_enrich(cur, incremental, timings_ms)
            if resolved is None:
                return EnrichResult(
                    enricher_name=self.name,
                    n_edges_emitted=0,
                    timings_ms=timings_ms,
                )
            edges, resolution = resolved
            # Item 92a: withdraw this linker's own declarations it no longer
            # derives, within what this run read, so a ranked edge passes to
            # the next-ranked producer instead of keeping a stale winner.
            started = time.perf_counter()
            stale = _stale_declarations(cur, resolution)
            _record_timing(timings_ms, "resolve_edges.stale_declarations", started)
            started = time.perf_counter()
            emitted, skipped = self._emit_edges(
                cur,
                edges=edges,
                generation_id=generation_id,
            )
            _record_timing(timings_ms, "emit_edges", started)
            retracted = 0
            if stale:
                started = time.perf_counter()
                retracted = self._emit_retractions(
                    cur,
                    edges=stale,
                    generation_id=generation_id,
                )
                _record_timing(timings_ms, "emit_retractions", started)
            timings_ms["emit_retractions.count"] = float(retracted)
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted + retracted,
            n_edges_skipped=skipped,
            timings_ms=timings_ms,
        )

    def diagnostics(self, conn: psycopg.Connection) -> CodeReferenceDiagnostics:
        with conn.cursor() as cur:
            imports = _fetch_nodes(cur, "code_import")
            references = _fetch_nodes(cur, "code_reference")
            edges, unmatched = self._resolve_edges(cur, skip_existing=False)
        matched_imports = {
            src for src, edge_type, dst, _attrs in edges
            if edge_type == "references" and src.startswith("import:")
        }
        matched_references = {
            src for src, _edge_type, _dst, _attrs in edges
            if src.startswith("reference:")
        }
        return CodeReferenceDiagnostics(
            eligible_imports=len(imports),
            matched_imports=len(matched_imports),
            eligible_references=len(references),
            matched_references=len(matched_references),
            emitted_edges=len(edges),
            unmatched_imports=unmatched["imports"],
            unmatched_references=unmatched["references"],
            unmatched_callers=unmatched["callers"],
        )

    def _resolve_edges(
        self,
        cur,
        *,
        skip_existing: bool,
        scope: _ResolutionScope | None = None,
        timings_ms: dict[str, float] | None = None,
        resolution: _Resolution | None = None,
    ) -> tuple[list[tuple[str, str, str, dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
        if scope is None:
            started = time.perf_counter()
            imports = _fetch_nodes(cur, "code_import")
            _record_timing(timings_ms, "resolve_edges.fetch.code_import", started)
            started = time.perf_counter()
            references = _fetch_nodes(cur, "code_reference")
            _record_timing(
                timings_ms,
                "resolve_edges.fetch.code_reference",
                started,
            )
        else:
            started = time.perf_counter()
            references = _fetch_references(cur, scope)
            references = _with_sibling_call_references(cur, references)
            _record_timing(
                timings_ms,
                "resolve_edges.fetch.scoped_code_reference",
                started,
            )
            started = time.perf_counter()
            imports = _fetch_imports(
                cur,
                scope,
                extra_paths=_paths_from_nodes(references),
            )
            _record_timing(
                timings_ms,
                "resolve_edges.fetch.scoped_code_import",
                started,
            )
            if not imports and not references:
                return [], {"imports": [], "references": [], "callers": []}
        started = time.perf_counter()
        symbols = _fetch_nodes(cur, "code_symbol")
        _record_timing(timings_ms, "resolve_edges.fetch.code_symbol", started)
        started = time.perf_counter()
        packages = _fetch_nodes(cur, "dependency_package")
        _record_timing(
            timings_ms,
            "resolve_edges.fetch.dependency_package",
            started,
        )

        started = time.perf_counter()
        symbol_index = _SymbolIndex(symbols)
        import_symbol_index = _ImportSymbolIndex(imports, symbol_index)
        package_index = _PackageIndex(packages)
        live_symbol_ids = {node.node_id for node in symbols}
        _record_timing(timings_ms, "resolve_edges.build_indexes", started)
        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        unmatched: dict[str, list[dict[str, Any]]] = {
            "imports": [],
            "references": [],
            "callers": [],
        }

        started = time.perf_counter()
        for node in imports:
            package_id = package_index.resolve_import(node.attrs)
            if package_id:
                edge = (
                    node.node_id,
                    "references",
                    package_id,
                    {
                        "resolution": "manifest_dependency",
                        "linker": self.name,
                    },
                )
                edges.append(edge)
            else:
                unmatched["imports"].append({
                    "node_id": node.node_id,
                    "module": node.attrs.get("module"),
                    "name": node.attrs.get("name"),
                    "reason": "no_dependency_package_match",
                })
        _record_timing(timings_ms, "resolve_edges.match.imports", started)

        started = time.perf_counter()
        # Issue #2836: one calls edge per (caller, callee), however many call
        # sites back it. Its line/reference_id come from the first call site
        # (lowest line, then reference id), so a rebuild and an incremental
        # run agree no matter the order the references were read in.
        call_edges: dict[
            tuple[str, str],
            tuple[tuple[Any, ...], tuple[str, str, str, dict[str, Any]]],
        ] = {}
        for node in references:
            symbol_id, resolution_attrs = symbol_index.resolve_reference(
                node.attrs,
                import_symbol_index=import_symbol_index,
            )
            if symbol_id:
                edge = (
                    node.node_id,
                    "references",
                    symbol_id,
                    {
                        "linker": self.name,
                        **resolution_attrs,
                    },
                )
                edges.append(edge)
                caller = str(node.attrs.get("enclosing_symbol_id") or "")
                if caller and caller != symbol_id:
                    # enclosing_symbol_id is copied from the reference
                    # node's attrs and bakes a line number into the id,
                    # so it goes stale on any edit until the owning
                    # file re-syncs. A calls edge from a symbol nobody
                    # emits fails producer-signature admission and
                    # zeroes the whole enricher run
                    # (harden-producer-emission-coherence
                    # req.coherence.linker-caller-validated) — validate
                    # against the same live symbol set used for dst
                    # resolution, and report misses.
                    if caller not in live_symbol_ids:
                        unmatched["callers"].append({
                            "node_id": node.node_id,
                            "caller": caller,
                            "callee": symbol_id,
                            "reason": "enclosing_symbol_id_not_in_live_symbols",
                        })
                    else:
                        call_edge = (
                            caller,
                            "calls",
                            symbol_id,
                            {
                                **resolution_attrs,
                                "reference_id": node.node_id,
                                "line": node.attrs.get("start_line"),
                                "linker": self.name,
                            },
                        )
                        rank = _call_site_rank(node)
                        prior = call_edges.get((caller, symbol_id))
                        if prior is None or rank < prior[0]:
                            call_edges[(caller, symbol_id)] = (rank, call_edge)
            else:
                unmatched["references"].append({
                    "node_id": node.node_id,
                    "path": node.attrs.get("path"),
                    "name": node.attrs.get("name"),
                    "qualified_name": node.attrs.get("qualified_name"),
                    "reason": "no_unique_code_symbol_match",
                })
        edges.extend(edge for _rank, edge in call_edges.values())
        _record_timing(timings_ms, "resolve_edges.match.references", started)
        if resolution is not None:
            resolution.full = scope is None
            resolution.derived = {
                (src, edge_type, dst) for src, edge_type, dst, _attrs in edges
            }
            resolution.reference_ids = {node.node_id for node in references}
            resolution.import_ids = {node.node_id for node in imports}
            resolution.callers = {
                caller
                for caller in (
                    str(node.attrs.get("enclosing_symbol_id") or "")
                    for node in references
                )
                if caller
            }
            resolution.live_ids = (
                live_symbol_ids
                | {node.node_id for node in packages}
                | resolution.reference_ids
                | resolution.import_ids
            )
            resolution.complete = True
        if skip_existing:
            started = time.perf_counter()
            edges = _filter_unchanged_edges(cur, edges)
            _record_timing(
                timings_ms,
                "resolve_edges.filter_existing_edges",
                started,
            )
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
        # One transaction for the whole write (issue #2941): the batches
        # commit together or not at all, so a refused row leaves no partial
        # edge set behind for the next run's skip check to treat as written.
        # On a connection already inside a transaction this is a savepoint.
        with cur.connection.transaction():
            return self._emit_rows(cur, rows)

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
            identity = f"{src}|{edge_type}|{dst}"
            if identity in seen:
                continue
            seen.add(identity)
            # Issue #2836: the key names the edge's attributes as well as its
            # identity. The publisher passes generation_id=0 on every cycle,
            # so a key of identity and generation alone matched the edge's
            # first fact forever and refused every later change to it.
            dedupe_key = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{identity}|{generation_id}|{_comparable_attrs(attrs)}",
            ).hex
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

    def _emit_retractions(
        self,
        cur,
        *,
        edges: list[tuple[str, str, str]],
        generation_id: int,
    ) -> int:
        """Write a retraction fact for each of this linker's own declarations
        it no longer derives (item 92a). The publisher applies them under the
        linker's producer id, which withdraws its declaration; a ranked edge
        then passes to the next-ranked declarer."""
        if not edges:
            return 0
        observed_at = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        rows: list[tuple[Any, ...]] = []
        for src, edge_type, dst in sorted(set(edges)):
            identity = f"{src}|{edge_type}|{dst}"
            rows.append((
                _mint_edge_id(src, edge_type, dst),
                src,
                dst,
                edge_type,
                json.dumps({}),
                json.dumps({
                    "src_node_id": src,
                    "dst_node_id": dst,
                    "linker": self.name,
                }),
                json.dumps({"generation_id": generation_id}),
                observed_at,
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{identity}|{generation_id}|retract",
                ).hex,
                run_id,
            ))
        with cur.connection.transaction():
            emitted, _skipped = self._emit_rows(cur, rows, op="R")
        return emitted

    def _emit_rows(
        self,
        cur,
        rows: list[tuple[Any, ...]],
        *,
        op: str = "I",
    ) -> tuple[int, int]:
        to_insert = self._rows_to_insert(cur, rows)
        self._insert_rows(cur, to_insert, op=op)
        return len(to_insert), len(rows) - len(to_insert)

    def _rows_to_insert(
        self, cur, rows: list[tuple[Any, ...]],
    ) -> list[tuple[Any, ...]]:
        # Write a row unless this linker's newest fact for the edge already
        # says the same thing: repeating it would only pile up facts, while
        # an edge that changed back to an earlier state must be written again.
        cur.execute(
            """
            SELECT DISTINCT ON (ef.src, ef.edge_type, ef.dst) ef.dedupe_key
              FROM okg.edge_facts ef
              JOIN unnest(%s::text[], %s::text[], %s::text[])
                     AS want(src, edge_type, dst)
                ON ef.src = want.src
               AND ef.edge_type = want.edge_type
               AND ef.dst = want.dst
             WHERE ef.source = %s
             ORDER BY ef.src, ef.edge_type, ef.dst, ef.fact_id DESC
            """,
            (
                [row[1] for row in rows],
                [row[3] for row in rows],
                [row[2] for row in rows],
                f"_enricher:{self.name}",
            ),
        )
        already = {row[0] for row in cur.fetchall()}
        return [row for row in rows if row[8] not in already]

    def _insert_rows(
        self, cur, to_insert: list[tuple[Any, ...]], *, op: str = "I",
    ) -> None:
        # One INSERT per batch, not per edge (issue #2941). WITH ORDINALITY
        # and ORDER BY keep the fact ids in the sorted order the per-row
        # loop gave them.
        for start in range(0, len(to_insert), _EMIT_BATCH_ROWS):
            batch = to_insert[start:start + _EMIT_BATCH_ROWS]
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op,
                   source_program_checksum)
                SELECT r.edge_id, r.src, r.dst, r.edge_type, r.attrs::jsonb,
                       'derived_deterministic', 1.0,
                       %s,
                       r.source_record_id::jsonb, r.source_revision::jsonb,
                       r.observed_at, r.dedupe_key, r.run_id, %s, %s
                  FROM unnest(
                         %s::bigint[], %s::text[], %s::text[], %s::text[],
                         %s::text[], %s::text[], %s::text[],
                         %s::timestamptz[], %s::text[], %s::uuid[]
                       ) WITH ORDINALITY
                    AS r(edge_id, src, dst, edge_type, attrs,
                         source_record_id, source_revision,
                         observed_at, dedupe_key, run_id, ord)
                 ORDER BY r.ord
                """,
                (
                    f"_enricher:{self.name}",
                    op,
                    _ENRICHMENT_WITNESS,
                    [row[0] for row in batch],
                    [row[1] for row in batch],
                    [row[2] for row in batch],
                    [row[3] for row in batch],
                    [row[4] for row in batch],
                    [row[5] for row in batch],
                    [row[6] for row in batch],
                    [row[7] for row in batch],
                    [row[8] for row in batch],
                    [row[9] for row in batch],
                ),
            )


class _PackageIndex:
    def __init__(self, packages: list[_Node]) -> None:
        self.by_ecosystem_name: dict[tuple[str, str], str] = {}
        for pkg in packages:
            ecosystem = str(pkg.attrs.get("ecosystem") or "").lower()
            name = str(pkg.attrs.get("name") or "").lower()
            if not ecosystem or not name:
                continue
            self.by_ecosystem_name[(ecosystem, _normalize_package(name))] = pkg.node_id

    def resolve_import(self, attrs: dict[str, Any]) -> str:
        ecosystem = _ecosystem_for_language(str(attrs.get("language") or ""))
        module = str(attrs.get("module") or "")
        if not ecosystem or not module or _is_local_or_relative_module(module):
            return ""
        package_name = _package_name_for_import(module, ecosystem=ecosystem)
        return self.by_ecosystem_name.get((ecosystem, _normalize_package(package_name)), "")


class _ImportSymbolIndex:
    def __init__(
        self,
        imports: list[_Node],
        symbol_index: "_SymbolIndex",
    ) -> None:
        self.symbol_index = symbol_index
        self.by_scope_binding: dict[tuple[str, str, str], str] = {}
        self.by_scope_module_binding: dict[tuple[str, str, str], str] = {}

        for import_node in imports:
            attrs = import_node.attrs
            repo = str(attrs.get("repo") or "")
            path = _normalize_path(str(attrs.get("path") or ""))
            module = _normalize_module(str(attrs.get("module") or ""))
            kind = str(attrs.get("kind") or "")
            name = str(attrs.get("name") or "")
            alias = str(attrs.get("alias") or "")
            if not repo or not path or not module:
                continue

            if kind == "from_import" and name:
                target = symbol_index.resolve_import_symbol(attrs)
                binding = alias or name
                if target and binding:
                    self.by_scope_binding[(repo, path, binding)] = target
                continue

            if kind == "import":
                binding = alias or module.split(".", 1)[0]
                if binding:
                    self.by_scope_module_binding[(repo, path, binding)] = module

    def resolve_reference(self, attrs: dict[str, Any]) -> str:
        repo = str(attrs.get("repo") or "")
        path = _normalize_path(str(attrs.get("path") or ""))
        name = str(attrs.get("name") or "")
        qname = str(attrs.get("qualified_name") or "")
        if not repo or not path:
            return ""

        candidates = [qname, name]
        if "." in qname:
            candidates.append(qname.split(".", 1)[0])
        for binding in candidates:
            target = self.by_scope_binding.get((repo, path, binding))
            if target:
                return target

        if "." not in qname:
            return ""
        binding, member = qname.split(".", 1)
        if "." in member:
            member = member.rsplit(".", 1)[-1]
        module = self.by_scope_module_binding.get((repo, path, binding))
        if not module or not member:
            return ""
        return self.symbol_index.resolve_module_member(module, member)


class _SymbolIndex:
    def __init__(self, symbols: list[_Node]) -> None:
        self.by_path_name: dict[tuple[str, str], list[str]] = {}
        self.by_repo_name: dict[tuple[str, str], list[str]] = {}
        self.by_path_qname: dict[tuple[str, str], list[str]] = {}
        self.by_module_name: dict[tuple[str, str], list[str]] = {}
        self.semantic = SemanticSymbolIndex(symbols)

        for symbol in symbols:
            attrs = symbol.attrs
            repo = str(attrs.get("repo") or "")
            path = _normalize_path(str(attrs.get("path") or ""))
            name = str(attrs.get("name") or "")
            qname = str(attrs.get("qualified_name") or "")
            if path and name:
                self.by_path_name.setdefault((path, name), []).append(symbol.node_id)
            if repo and name:
                self.by_repo_name.setdefault((repo, name), []).append(symbol.node_id)
            if path and qname:
                for lookup_name in {qname, _lexical_symbol_name(repo, path, qname)}:
                    self.by_path_qname.setdefault((path, lookup_name), []).append(symbol.node_id)
            module = _module_from_path(path)
            if module and name:
                self.by_module_name.setdefault((module, name), []).append(symbol.node_id)

    def resolve_reference(
        self,
        attrs: dict[str, Any],
        *,
        import_symbol_index: _ImportSymbolIndex | None = None,
    ) -> tuple[str, dict[str, str]]:
        semantic = self.semantic.match_attrs(attrs)
        if semantic:
            return (
                semantic.node_id,
                {
                    "resolution": semantic.resolution,
                    **semantic.attrs,
                },
            )
        path = _normalize_path(str(attrs.get("path") or ""))
        repo = str(attrs.get("repo") or "")
        name = str(attrs.get("name") or "")
        qname = str(attrs.get("qualified_name") or "")
        if not name or name in _BUILTIN_PYTHON_NAMES:
            return "", {}

        for key in ((path, qname), (path, name)):
            found = _unique(self.by_path_qname.get(key) or self.by_path_name.get(key))
            if found:
                return found, {
                    "resolution": "unique_code_symbol",
                    **resolution_method_attrs("unique_code_symbol"),
                }

        if import_symbol_index is not None:
            found = import_symbol_index.resolve_reference(attrs)
            if found:
                return found, {
                    "resolution": "unique_code_symbol",
                    **resolution_method_attrs("unique_code_symbol"),
                }

        found = _unique(self.by_repo_name.get((repo, name)))
        if found:
            return found, {
                "resolution": "unique_code_symbol",
                **resolution_method_attrs("unique_code_symbol"),
            }
        return "", {}

    def resolve_import_symbol(self, attrs: dict[str, Any]) -> str:
        module = _normalize_module(str(attrs.get("module") or ""))
        name = str(attrs.get("name") or "")
        if not module or not name:
            return ""
        return _unique(self.by_module_name.get((module, name))) or ""

    def resolve_module_member(self, module: str, member: str) -> str:
        module = _normalize_module(module)
        if not module or not member:
            return ""
        return _unique(self.by_module_name.get((module, member))) or ""


def _fetch_nodes(cur, subtype: str) -> list[_Node]:
    rows = _chronos_query(
        cur,
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = :subtype
        """,
        {"subtype": subtype},
    )
    return [
        _Node(str(row["node_id"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _dirty_resolution_scope(
    cur,
    incremental: IncrementalContext | None,
) -> _ResolutionScope | None:
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
    if len(incremental.dirty_node_ids) > MAX_RELEVANCE_DIRTY_IDS:
        return None
    if not incremental.dirty_node_ids:
        return _ResolutionScope()

    rows = _fetch_dirty_nodes(cur, incremental.dirty_node_ids)
    if len(rows) != len(incremental.dirty_node_ids):
        return None

    import_ids: set[str] = set()
    reference_ids: set[str] = set()
    paths: set[str] = set()
    semantic_keys: set[str] = set()
    canonical_names: set[str] = set()
    names: set[str] = set()
    saw_relevant = False
    for row in rows:
        subtype = str(row.get("subtype") or "")
        if subtype not in CodeReferenceResolver.reads_subtypes:
            continue
        saw_relevant = True
        if subtype == "dependency_package":
            return None
        node_id = str(row.get("node_id") or "")
        attrs = dict(row.get("attrs") or {})
        path = _normalize_path(str(attrs.get("path") or ""))
        if path:
            paths.add(path)
        if subtype == "code_import":
            if node_id:
                import_ids.add(node_id)
            _collect_import_hints(attrs, semantic_keys=semantic_keys, names=names)
        elif subtype == "code_reference":
            if node_id:
                reference_ids.add(node_id)
            _collect_reference_hints(
                attrs,
                semantic_keys=semantic_keys,
                canonical_names=canonical_names,
                names=names,
            )
        elif subtype == "code_symbol":
            _collect_symbol_hints(
                attrs,
                semantic_keys=semantic_keys,
                canonical_names=canonical_names,
                names=names,
            )

    if not saw_relevant:
        return _ResolutionScope()
    return _ResolutionScope(
        import_ids=frozenset(import_ids),
        reference_ids=frozenset(reference_ids),
        paths=frozenset(paths),
        semantic_keys=frozenset(semantic_keys),
        canonical_names=frozenset(canonical_names),
        names=frozenset(names),
    )


def _fetch_dirty_nodes(
    cur,
    dirty_node_ids: frozenset[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    dirty_ids = sorted(dirty_node_ids)
    for start in range(0, len(dirty_ids), MAX_RELEVANCE_DIRTY_IDS):
        chunk = dirty_ids[start:start + MAX_RELEVANCE_DIRTY_IDS]
        # Lenient on purpose: a failed read returns fewer rows than dirty
        # ids, so _dirty_resolution_scope gives up the scoped run and the
        # full run reads the graph through the strict _chronos_query.
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


def _fetch_references(cur, scope: _ResolutionScope) -> list[_Node]:
    if scope.empty:
        return []
    params: dict[str, Any] = {}
    clauses: list[str] = []
    if scope.reference_ids:
        clauses.append("node_id = ANY(:reference_ids)")
        params["reference_ids"] = sorted(scope.reference_ids)
    if scope.paths:
        clauses.append("attrs->>'path' = ANY(:paths)")
        params["paths"] = sorted(scope.paths)
    if scope.semantic_keys:
        clauses.append(_json_any_clause(
            SEMANTIC_KEY_HINT_KEYS,
            param_name="semantic_keys",
        ))
        params["semantic_keys"] = sorted(scope.semantic_keys)
    if scope.canonical_names:
        clauses.append(_json_any_clause(
            CANONICAL_NAME_HINT_KEYS,
            param_name="canonical_names",
        ))
        params["canonical_names"] = sorted(scope.canonical_names)
    if scope.names:
        clauses.append(
            "(attrs->>'name' = ANY(:names) "
            "OR attrs->>'qualified_name' = ANY(:names) "
            "OR attrs->>'raw_qualified_name' = ANY(:names))"
        )
        params["names"] = sorted(scope.names)
    if not clauses:
        return []
    return _nodes_from_rows(_chronos.query_strict(
        cur,
        f"""
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'code_reference'
           AND ({" OR ".join(clauses)})
        """,
        params,
    ))


def _fetch_imports(
    cur,
    scope: _ResolutionScope,
    *,
    extra_paths: frozenset[str] = frozenset(),
) -> list[_Node]:
    paths = frozenset({*scope.paths, *extra_paths})
    if scope.empty and not paths:
        return []
    params: dict[str, Any] = {}
    clauses: list[str] = []
    if scope.import_ids:
        clauses.append("node_id = ANY(:import_ids)")
        params["import_ids"] = sorted(scope.import_ids)
    if paths:
        clauses.append("attrs->>'path' = ANY(:paths)")
        params["paths"] = sorted(paths)
    if scope.semantic_keys:
        clauses.append(_json_any_clause(
            SEMANTIC_KEY_HINT_KEYS,
            param_name="semantic_keys",
        ))
        params["semantic_keys"] = sorted(scope.semantic_keys)
    if not clauses:
        return []
    return _nodes_from_rows(_chronos.query_strict(
        cur,
        f"""
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'code_import'
           AND ({" OR ".join(clauses)})
        """,
        params,
    ))


def _nodes_from_rows(rows: list[dict[str, Any]]) -> list[_Node]:
    return [
        _Node(str(row["node_id"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _paths_from_nodes(nodes: list[_Node]) -> frozenset[str]:
    return frozenset(
        path
        for path in (
            _normalize_path(str(node.attrs.get("path") or ""))
            for node in nodes
        )
        if path
    )


def _collect_import_hints(
    attrs: dict[str, Any],
    *,
    semantic_keys: set[str],
    names: set[str],
) -> None:
    del names
    semantic_key = _clean_attr(attrs.get("semantic_target_key"))
    if semantic_key:
        semantic_keys.add(semantic_key)


def _collect_reference_hints(
    attrs: dict[str, Any],
    *,
    semantic_keys: set[str],
    canonical_names: set[str],
    names: set[str],
) -> None:
    for key in SEMANTIC_KEY_HINT_KEYS:
        value = _clean_attr(attrs.get(key))
        if value:
            semantic_keys.add(value)
    for key in CANONICAL_NAME_HINT_KEYS:
        value = _clean_attr(attrs.get(key))
        if value:
            canonical_names.add(value)
    del names


def _collect_symbol_hints(
    attrs: dict[str, Any],
    *,
    semantic_keys: set[str],
    canonical_names: set[str],
    names: set[str],
) -> None:
    semantic_key = _clean_attr(attrs.get("semantic_key"))
    if semantic_key:
        semantic_keys.add(semantic_key)
    canonical_name = _clean_attr(attrs.get("canonical_name"))
    if canonical_name:
        canonical_names.add(canonical_name)
    for key in ("name", "qualified_name"):
        value = _clean_attr(attrs.get(key))
        if value:
            names.add(value)
            if "." in value:
                names.add(value.rsplit(".", 1)[-1])


def _json_any_clause(keys: tuple[str, ...], *, param_name: str) -> str:
    return "(" + " OR ".join(
        f"attrs->>'{key}' = ANY(:{param_name})"
        for key in keys
    ) + ")"


def _clean_attr(value: Any) -> str:
    return str(value or "").strip()


def _call_site_rank(node: _Node) -> tuple[Any, ...]:
    line = node.attrs.get("start_line")
    if isinstance(line, bool) or not isinstance(line, int):
        return (1, 0, node.node_id)
    return (0, line, node.node_id)


def _with_sibling_call_references(cur, references: list[_Node]) -> list[_Node]:
    """Add every reference made from the callers of ``references``.

    Issue #2836: a calls edge takes its line from the caller's first call
    site to that callee. A scoped run that read only some of the caller's
    references could pick a later call site than a rebuild does, so the
    scope is closed over each caller before edges are derived.
    """
    callers = sorted({
        caller
        for caller in (
            str(node.attrs.get("enclosing_symbol_id") or "")
            for node in references
        )
        if caller
    })
    if not callers:
        return references
    seen = {node.node_id for node in references}
    # strict: an unreadable branch must not read as "the caller has no other
    # call sites", which would let a scoped run pick a later one silently.
    siblings = _nodes_from_rows(_chronos.query_strict(
        cur,
        """
        SELECT node_id, attrs
          FROM okg.graph_nodes
         WHERE subtype = 'code_reference'
           AND attrs->>'enclosing_symbol_id' = ANY(:callers)
        """,
        {"callers": callers},
    ))
    return references + [node for node in siblings if node.node_id not in seen]


def _filter_unchanged_edges(
    cur,
    edges: list[tuple[str, str, str, dict[str, Any]]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    """Drop only the edges already live with the same attributes.

    Issue #2836: an edge whose identity survives a code change (a call
    site that moved) must still be re-emitted when its attributes changed,
    or the graph keeps the old line and reference id. The publisher stamps
    source lineage onto derived edges, so that attribute is left out of the
    comparison.
    """
    if not edges:
        return []
    edge_ids = [
        _mint_edge_id(src, edge_type, dst)
        for src, edge_type, dst, _attrs in edges
    ]
    rows = _chronos_query(
        cur,
        """
        SELECT src, edge_type, dst, attrs
          FROM okg.graph_edges
         WHERE edge_id = ANY(:edge_ids)
        """,
        {"edge_ids": edge_ids},
    )
    live: dict[tuple[str, str, str], list[str]] = {}
    for row in rows:
        live.setdefault(
            (str(row["src"]), str(row["edge_type"]), str(row["dst"])),
            [],
        ).append(_comparable_attrs(dict(row["attrs"] or {})))
    # Item 92a: an edge of a ranked type has declarer rows, and the live row
    # may be another producer's version. Compare with this linker's own
    # declaration there, so it records one even when a lower-ranked producer
    # wrote the same attributes. An edge with no declarer rows keeps the
    # comparison with the live row.
    recorded = _recorded_declarations(cur, edge_ids)
    out: list[tuple[str, str, str, dict[str, Any]]] = []
    for edge, edge_key in zip(edges, edge_ids):
        declared = recorded.get(edge_key)
        if declared:
            own = declared.get(_PRODUCER_ID)
            if own is None or own != _comparable_attrs(edge[3]):
                out.append(edge)
            continue
        if _comparable_attrs(edge[3]) not in live.get(
            (edge[0], edge[1], edge[2]),
            [],
        ):
            out.append(edge)
    return out


#: The producer id the publisher applies this linker's facts under.
_PRODUCER_ID = "_enricher:code_reference"


def _recorded_declarations(
    cur,
    edge_ids: list[int],
) -> dict[int, dict[str, str]]:
    """Every recorded declaration of ``edge_ids``: ``{edge_id: {source:
    comparable attrs}}``. Only ranked edge types have any (item 92a)."""
    if not edge_ids:
        return {}
    out: dict[int, dict[str, str]] = {}
    for row in _chronos.query_strict(
        cur,
        """
        SELECT edge_id, source, attrs
          FROM okg.graph_edge_declarers
         WHERE edge_id = ANY(:edge_ids)
        """,
        {"edge_ids": sorted(set(edge_ids))},
    ):
        out.setdefault(int(row["edge_id"]), {})[str(row["source"])] = (
            _comparable_attrs(dict(row["attrs"] or {}))
        )
    return out


def _own_declarations(
    cur,
    *,
    srcs: list[str] | None,
) -> set[tuple[str, str, str]]:
    """This linker's current declarations of ``references``/``calls`` edges
    from ``srcs`` (every source node when ``None``).

    A ranked edge's declaration is its declarer row; an unranked edge's is
    the live row it owns. A scoped run reads the live rows from its source
    nodes (source-node index) and looks their declarations up by key; a full
    run reads only this linker's own live rows (source index) and its own
    declarer rows, never other producers' rows.
    """
    if srcs is not None and not srcs:
        return set()
    if srcs is None:
        own_rows = _chronos.query_strict(
            cur,
            """
            SELECT src, edge_type, dst
              FROM okg.graph_edges
             WHERE source = :source
               AND edge_type = ANY(:edge_types)
            """,
            {
                "source": _PRODUCER_ID,
                "edge_types": list(CodeReferenceResolver.emits_edge_types),
            },
        )
        declared_rows = _chronos.query_strict(
            cur,
            """
            SELECT src, edge_type, dst
              FROM okg.graph_edge_declarers
             WHERE source = :source
               AND edge_type = ANY(:edge_types)
            """,
            {
                "source": _PRODUCER_ID,
                "edge_types": list(CodeReferenceResolver.emits_edge_types),
            },
        )
        return {
            (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
            for row in [*own_rows, *declared_rows]
        }
    rows = _chronos.query_strict(
        cur,
        """
        SELECT edge_id, src, edge_type, dst, source
          FROM okg.graph_edges
         WHERE edge_type = ANY(:edge_types)
           AND src = ANY(:srcs)
        """,
        {
            "edge_types": list(CodeReferenceResolver.emits_edge_types),
            "srcs": sorted(set(srcs)),
        },
    )
    own: set[tuple[str, str, str]] = set()
    by_id: dict[int, tuple[str, str, str]] = {}
    for row in rows:
        identity = (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        by_id[int(row["edge_id"])] = identity
        if str(row.get("source") or "") == _PRODUCER_ID:
            own.add(identity)
    if by_id:
        for row in _chronos.query_strict(
            cur,
            """
            SELECT edge_id
              FROM okg.graph_edge_declarers
             WHERE edge_id = ANY(:edge_ids)
               AND source = :source
            """,
            {"edge_ids": sorted(by_id), "source": _PRODUCER_ID},
        ):
            identity = by_id.get(int(row["edge_id"]))
            if identity is not None:
                own.add(identity)
    return own


def _stale_declarations(
    cur,
    resolution: _Resolution,
) -> list[tuple[str, str, str]]:
    """Own declarations this run read the inputs of and no longer derives.

    A full run covers every declaration. A scoped run covers ``references``
    edges from the references and imports it read and ``calls`` edges from
    the callers of those references (whose every call site it read). Only
    edges whose endpoints are live are retracted: a dangling edge is removed
    with its node.
    """
    if not resolution.complete:
        return []
    srcs = (
        None if resolution.full
        else sorted(
            resolution.reference_ids | resolution.import_ids | resolution.callers
        )
    )
    own = _own_declarations(cur, srcs=srcs)
    stale: list[tuple[str, str, str]] = []
    for src, edge_type, dst in sorted(own - resolution.derived):
        if not resolution.full:
            if edge_type == "calls" and src not in resolution.callers:
                continue
            if edge_type == "references" and src not in (
                resolution.reference_ids | resolution.import_ids
            ):
                continue
        if src in resolution.live_ids and dst in resolution.live_ids:
            stale.append((src, edge_type, dst))
    return stale


def _comparable_attrs(attrs: dict[str, Any]) -> str:
    return json.dumps(
        {key: value for key, value in attrs.items() if key != LINEAGE_ATTR},
        sort_keys=True,
        default=str,
    )


def _chronos_query(cur, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """Read the caller's Chronos branch; a failed read raises (issue #2941).

    Every caller takes an empty answer as a fact about the graph: no code
    symbols (so the run succeeds with no edges) or no live edges (so every
    edge is written again). The publisher records the raised error as a
    failed run and keeps the enricher's watermark.
    """
    return _chronos.query_strict(cur, sql, params)


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)


def _unique(values: list[str] | None) -> str:
    if not values:
        return ""
    deduped = sorted(set(values))
    return deduped[0] if len(deduped) == 1 else ""


def _ecosystem_for_language(language: str) -> str:
    return {
        "python": "python",
        "javascript": "npm",
        "typescript": "npm",
        "go": "go",
        "rust": "rust",
    }.get(language.lower(), "")


def _package_name_for_import(module: str, *, ecosystem: str) -> str:
    module = module.strip().strip("\"'")
    if ecosystem == "npm" and module.startswith("@"):
        parts = module.split("/")
        return "/".join(parts[:2]) if len(parts) >= 2 else module
    if ecosystem == "go":
        return module
    if ecosystem == "rust":
        return module.split("::", 1)[0]
    return re.split(r"[.:/]", module, maxsplit=1)[0]


def _normalize_package(name: str) -> str:
    return name.lower().replace("_", "-")


def _is_local_or_relative_module(module: str) -> bool:
    module = module.strip()
    return (
        not module
        or module.startswith(".")
        or module.startswith("/")
        or module.startswith("okg")
        or module.startswith("src.okg")
    )


def _normalize_path(path: str) -> str:
    path = path.replace("\\", "/").strip().strip("/")
    while path.startswith("./"):
        path = path[2:]
    return path


def _module_from_path(path: str) -> str:
    path = _normalize_path(path)
    if not path:
        return ""
    if path.startswith("src/"):
        path = path[4:]
    path = re.sub(r"\.[^.]+$", "", path)
    if path.endswith("/__init__"):
        path = path[: -len("/__init__")]
    return _normalize_module(path.replace("/", "."))


def _normalize_module(module: str) -> str:
    module = module.strip().strip(".")
    if module.startswith("src."):
        module = module[4:]
    return module
