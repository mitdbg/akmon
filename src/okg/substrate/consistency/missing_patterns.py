"""Name the catalog patterns a blocked transform gate is missing (#3624).

The transform consistency gate blocks a publish on edges whose
``(src_subtype, edge_type, dst_subtype)`` is not a declared narrowing and on
nodes whose subtype the catalog does not declare. Its report used to list only
row ids, so an operator could not tell which pattern was missing or that the
fix was one line in ``deployment.yaml`` (issue #3624: 1,181
``document contains section`` edges, fixed by adding ``document_starter``).

This module turns the violating ids into named patterns with counts, and for
each pattern names the okg library ontology modules that declare it. A module
that declares the exact pattern cannot be in the composed catalog -- if it
were, the pattern would be allowed -- so every module named here is one the
deployment does not load. Library declarations are read with the composer's
own extraction helpers, so a class name maps to a subtype name
(``DocumentChunk`` to ``document_chunk``) exactly as composition maps it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

from okg.substrate import fastyaml

#: At most this many patterns of each kind ride on a gate result. A larger set
#: is never dropped silently: ``*_patterns_truncated`` says so.
PATTERN_LIMIT = 20

_DEPLOYMENT_MANIFEST = "deployment.yaml"
_MISSING_NODE = "(missing node)"


#: One way to get a pattern into the catalog: the module names that must all
#: be loaded. A module's own declaration needs one module; a cross-link rule in
#: an ``extends.<target>`` block needs the extending module and its target.
ModuleSet = tuple[str, ...]


@dataclass(frozen=True)
class LibraryDeclarations:
    """Which okg library ontology modules declare each narrowing and subtype."""

    narrowings: Mapping[tuple[str, str, str], tuple[ModuleSet, ...]]
    subtypes: Mapping[str, tuple[str, ...]]
    #: The library's single-parent subtype refinement (``is_a``).
    subtype_parent: Mapping[str, str] = field(default_factory=dict)

    def _lineage(self, subtype: str) -> list[str]:
        """``subtype`` then its library ancestors, nearest first."""
        chain = [subtype]
        while chain[-1] in self.subtype_parent:
            parent = self.subtype_parent[chain[-1]]
            if parent in chain:
                break
            chain.append(parent)
        return chain

    def modules_for_narrowing(
        self, src_subtype: str, edge_type: str, dst_subtype: str,
    ) -> tuple[ModuleSet, ...]:
        """Module sets declaring the triple, or a triple over its ancestors.

        The gate allows an edge through the catalog's subtype-ancestor
        closure, so a narrowing declared on a parent subtype admits it too.
        """
        found: list[ModuleSet] = []
        for src in self._lineage(src_subtype):
            for dst in self._lineage(dst_subtype):
                for module_set in self.narrowings.get((src, edge_type, dst), ()):
                    if module_set not in found:
                        found.append(module_set)
        return tuple(found)

    def modules_for_subtype(self, subtype: str) -> tuple[str, ...]:
        return self.subtypes.get(subtype, ())


def _module_schema_paths(modules_root: Path) -> list[tuple[str, Path]]:
    """``(module name, schema path)`` for every library module, by name.

    The schema file is the one ``module.yaml`` names under
    ``provides.subtypes_file``; a module directory without a manifest, or
    whose manifest names no subtypes file, provides no subtypes.
    """
    found: list[tuple[str, Path]] = []
    for module_dir in sorted(p for p in modules_root.iterdir() if p.is_dir()):
        manifest_path = module_dir / "module.yaml"
        if not manifest_path.is_file():
            continue
        with manifest_path.open() as handle:
            manifest = fastyaml.safe_load(handle) or {}
        provides = manifest.get("provides") if isinstance(manifest, dict) else None
        subtypes_file = (
            provides.get("subtypes_file") if isinstance(provides, dict) else None
        )
        if not subtypes_file:
            continue
        schema_path = module_dir / str(subtypes_file)
        if schema_path.is_file():
            found.append((module_dir.name, schema_path))
    return found


@lru_cache(maxsize=4)
def library_declarations(modules_root: Path | None = None) -> LibraryDeclarations:
    """Index the okg library: narrowing triple and subtype -> declaring modules.

    Cached per root: the library ships with the package and does not change
    while a process runs. Uses the composer's own steps over every library
    module at once, in the composer's order: subtypes, then each module's
    ``EdgeNarrowings``, then the cross-link rules the composition resolver
    fires (``provides.intra_cross_link_rules`` and
    ``extends.<target>.cross_link_rules``). Subtype names and narrowing
    expansion therefore match composition exactly.
    """
    from okg.substrate.catalog.composition import resolve
    from okg.substrate.catalog.module_providers import providers_from_modules_root
    from okg.substrate.catalog.modules_compose import (
        ComposedCatalog,
        _extract_narrowings,
        _extract_subtypes,
        _inject_cross_link_narrowings_from_resolver,
        load_modules_root,
    )

    root = modules_root if modules_root is not None else load_modules_root()
    modules = _module_schema_paths(root)
    names = [name for name, _ in modules]
    composed = ComposedCatalog(deployment_name="okg-library-index", modules=names)
    subtypes: dict[str, set[str]] = {}
    for name, schema_path in modules:
        for subtype in _extract_subtypes(schema_path, name, composed):
            subtypes.setdefault(subtype, set()).add(name)
    for name, schema_path in modules:
        _extract_narrowings(schema_path, name, composed)
    providers = providers_from_modules_root(root)
    _inject_cross_link_narrowings_from_resolver(composed, providers=providers)
    # The injector records only `cross_link:<rule id>`; the resolver says
    # which module owns each rule and, for an `extends` rule, its target.
    rule_owners: dict[str, ModuleSet] = {}
    for resolved_rule in resolve(names, providers=providers).cross_link_rules:
        owners = (resolved_rule.source_module,)
        if resolved_rule.target_module:
            owners = (resolved_rule.source_module, resolved_rule.target_module)
        rule_owners[resolved_rule.rule.id] = owners
    narrowings: dict[tuple[str, str, str], list[ModuleSet]] = {}
    for row in composed.narrowings:
        origin = str(row.origin)
        if origin.startswith("cross_link:"):
            rule_id = origin.removeprefix("cross_link:").removesuffix(":reverse")
            module_set = rule_owners[rule_id]
        else:
            module_set = (origin,)
        triple = (row.parent_subtype, row.edge_archetype, row.child_subtype)
        sets = narrowings.setdefault(triple, [])
        if module_set not in sets:
            sets.append(module_set)
    return LibraryDeclarations(
        narrowings={k: tuple(sorted(v)) for k, v in narrowings.items()},
        subtypes={k: tuple(sorted(v)) for k, v in subtypes.items()},
        subtype_parent=dict(composed.subtype_parent),
    )


@lru_cache(maxsize=1)
def _reapply_command() -> str:
    """The catalog re-apply command, checked against the live parser."""
    from okg.substrate.cli.command_reference import okg_command

    return str(okg_command("catalog load", deployment="DEPLOYMENT_DIR", apply=True))


def _module_set_text(module_set: ModuleSet) -> str:
    head = f"`{module_set[0]}`"
    if len(module_set) == 1:
        return head
    rest = ", ".join(f"`{m}`" for m in module_set[1:])
    return f"{head} (with {rest})"


def _module_fix(module_sets: Sequence[ModuleSet]) -> str:
    if len(module_sets) == 1:
        what = _module_set_text(module_sets[0])
    else:
        what = "one of " + " or ".join(_module_set_text(m) for m in module_sets)
    # Editing deployment.yaml alone changes nothing: the gate reads the
    # APPLIED catalog. A module already listed means that catalog is stale.
    return (
        f"add {what} under `modules:` in {_DEPLOYMENT_MANIFEST} (if already "
        f"listed, the applied catalog is stale), then re-apply the catalog: "
        f"{_reapply_command()}"
    )


def _flat_modules(module_sets: Sequence[ModuleSet]) -> list[str]:
    out: list[str] = []
    for module_set in module_sets:
        for module in module_set:
            if module not in out:
                out.append(module)
    return out


def narrowing_pattern_entry(
    src_subtype: str | None,
    edge_type: str,
    dst_subtype: str | None,
    edge_count: int,
    library: LibraryDeclarations,
) -> dict[str, Any]:
    """One named narrowing pattern with its count, declaring modules and fix.

    A missing endpoint node (``None`` subtype) is named as such; no library
    module can declare it, and the dangling-edge check reports that edge too.
    """
    src = src_subtype if src_subtype is not None else _MISSING_NODE
    dst = dst_subtype if dst_subtype is not None else _MISSING_NODE
    pattern = f"{src} {edge_type} {dst}"
    module_sets = (
        library.modules_for_narrowing(src_subtype, edge_type, dst_subtype)
        if src_subtype is not None and dst_subtype is not None
        else ()
    )
    if module_sets:
        fix = _module_fix(module_sets)
    else:
        fix = (
            f"no okg library module declares `{pattern}`; declare it in an "
            "EdgeNarrowings class in the deployment's own schemas/bridges/ "
            "file, or stop emitting the edge"
        )
    return {
        "src_subtype": src_subtype,
        "edge_type": edge_type,
        "dst_subtype": dst_subtype,
        "pattern": pattern,
        "edge_count": int(edge_count),
        "library_modules": _flat_modules(module_sets),
        "library_module_sets": [list(m) for m in module_sets],
        "fix": fix,
    }


def node_subtype_pattern_entry(
    subtype: str,
    node_count: int,
    library: LibraryDeclarations,
) -> dict[str, Any]:
    """One unknown node subtype with its count, declaring modules and fix."""
    modules = library.modules_for_subtype(subtype)
    if modules:
        fix = _module_fix([(m,) for m in modules])
    else:
        fix = (
            f"no okg library module declares subtype `{subtype}`; declare it "
            "in the deployment's own schemas/ directory, or stop emitting it"
        )
    return {
        "subtype": subtype,
        "node_count": int(node_count),
        "library_modules": list(modules),
        "fix": fix,
    }


_NARROWING_GROUPS_SQL = """
    SELECT ns.subtype AS src_subtype,
           e.edge_type AS edge_type,
           nd.subtype AS dst_subtype,
           count(*) AS n
      FROM okg.graph_edges e
      LEFT JOIN okg.graph_nodes ns ON ns.node_id = e.src
      LEFT JOIN okg.graph_nodes nd ON nd.node_id = e.dst
     WHERE e.edge_id = ANY(:pattern_edge_ids)
     GROUP BY ns.subtype, e.edge_type, nd.subtype
     ORDER BY n DESC, ns.subtype, e.edge_type, nd.subtype
"""

_NODE_SUBTYPE_GROUPS_SQL = """
    SELECT n.subtype AS subtype, count(*) AS n
      FROM okg.graph_nodes n
     WHERE n.node_id = ANY(:pattern_node_ids)
     GROUP BY n.subtype
     ORDER BY n DESC, n.subtype
"""


def empty_pattern_details(error: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The pattern fields with nothing named.

    ``error`` is set only when naming the patterns failed; the gate still
    blocks on its violation ids, and the error is reported, never hidden.
    """
    return {
        "narrowing_violation_patterns": [],
        "narrowing_violation_patterns_truncated": False,
        "node_subtype_violation_patterns": [],
        "node_subtype_violation_patterns_truncated": False,
        "violation_patterns_error": dict(error) if error is not None else None,
    }


def violation_pattern_details(
    session: Any,
    *,
    narrowing_edge_ids: Sequence[int],
    node_subtype_node_ids: Sequence[str],
    library: LibraryDeclarations | None = None,
    limit: int = PATTERN_LIMIT,
) -> dict[str, Any]:
    """The gate-detail fields that name each missing pattern.

    ``session`` is a Chronos session over the graph the gate read. One grouped
    read per violation kind runs, and none when that kind has no violations,
    so a clean gate run pays nothing. Returns the ``*_patterns`` /
    ``*_patterns_truncated`` fields and ``violation_patterns_error`` (None).
    """
    out = empty_pattern_details()
    if not narrowing_edge_ids and not node_subtype_node_ids:
        return out
    lib = library if library is not None else library_declarations()
    if narrowing_edge_ids:
        rows = list(session.query(
            _NARROWING_GROUPS_SQL,
            {"pattern_edge_ids": [int(i) for i in narrowing_edge_ids]},
        ))
        out["narrowing_violation_patterns_truncated"] = len(rows) > limit
        out["narrowing_violation_patterns"] = [
            narrowing_pattern_entry(
                r["src_subtype"], str(r["edge_type"]), r["dst_subtype"],
                int(r["n"]), lib,
            )
            for r in rows[:limit]
        ]
    if node_subtype_node_ids:
        rows = list(session.query(
            _NODE_SUBTYPE_GROUPS_SQL,
            {"pattern_node_ids": [str(i) for i in node_subtype_node_ids]},
        ))
        out["node_subtype_violation_patterns_truncated"] = len(rows) > limit
        out["node_subtype_violation_patterns"] = [
            node_subtype_pattern_entry(str(r["subtype"]), int(r["n"]), lib)
            for r in rows[:limit]
        ]
    return out


def _merge_details(all_details: Sequence[Any]) -> dict[str, Any]:
    """One details view over several gate results (pre- and post-view passes).

    A pattern reported twice is kept once, with the larger count; truncation
    flags and lookup errors from any pass carry over.
    """
    narrowing: dict[str, dict[str, Any]] = {}
    node: dict[str, dict[str, Any]] = {}
    merged: dict[str, Any] = {
        "narrowing_violation_patterns_truncated": False,
        "node_subtype_violation_patterns_truncated": False,
        "violation_patterns_errors": [],
    }
    for details in all_details:
        if not isinstance(details, Mapping):
            continue
        for key, bucket, count in (
            ("narrowing_violation_patterns", narrowing, "edge_count"),
            ("node_subtype_violation_patterns", node, "node_count"),
        ):
            for item in details.get(key) or []:
                if not isinstance(item, Mapping):
                    continue
                name = str(item.get("pattern") or item.get("subtype"))
                kept = bucket.get(name)
                if kept is None or int(item.get(count) or 0) > int(kept.get(count) or 0):
                    bucket[name] = dict(item)
        for flag in (
            "narrowing_violation_patterns_truncated",
            "node_subtype_violation_patterns_truncated",
        ):
            merged[flag] = merged[flag] or bool(details.get(flag))
        error = details.get("violation_patterns_error")
        if isinstance(error, Mapping) and dict(error) not in merged["violation_patterns_errors"]:
            merged["violation_patterns_errors"].append(dict(error))
    merged["narrowing_violation_patterns"] = list(narrowing.values())
    merged["node_subtype_violation_patterns"] = list(node.values())
    return merged


def pattern_fix_lines(details: Any) -> list[str]:
    """Plain-text lines naming each missing pattern, its count and its fix."""
    if not isinstance(details, Mapping):
        return []
    merged = _merge_details([details])
    lines: list[str] = []
    for item in merged["narrowing_violation_patterns"]:
        lines.append(
            f"{item.get('edge_count')} edge(s) `{item.get('pattern')}` "
            f"have no declared narrowing: {item.get('fix')}"
        )
    if merged["narrowing_violation_patterns_truncated"]:
        lines.append(
            "more narrowing patterns are missing; okg generation describe "
            "lists every edge id"
        )
    for item in merged["node_subtype_violation_patterns"]:
        lines.append(
            f"{item.get('node_count')} node(s) of subtype "
            f"`{item.get('subtype')}` are not in the catalog: {item.get('fix')}"
        )
    if merged["node_subtype_violation_patterns_truncated"]:
        lines.append(
            "more node subtypes are missing; okg generation describe lists "
            "every node id"
        )
    for error in merged["violation_patterns_errors"]:
        lines.append(
            "the gate could not name the missing patterns "
            f"({error.get('type')}: {error.get('message')}); okg generation "
            "describe lists the violating ids"
        )
    return lines


def blocked_reason_fix_lines(blocked_reason: Any) -> list[str]:
    """The pattern fix lines of every failed consistency gate in a block.

    Reads ``failed_consistency_gates`` of a publisher ``blocked_reason`` and of
    a nested ``phase_b`` / ``phase_b_blocked_reason``. A pattern the pre-view
    and post-view gate passes both report is named once, with the larger count.
    """
    all_details: list[Any] = []
    pending = [blocked_reason]
    while pending:
        value = pending.pop(0)
        if not isinstance(value, Mapping):
            continue
        for gate in value.get("failed_consistency_gates") or []:
            if isinstance(gate, Mapping):
                all_details.append(gate.get("details"))
        pending.extend(
            value.get(key) for key in ("phase_b", "phase_b_blocked_reason")
        )
    if not all_details:
        return []
    merged = _merge_details(all_details)
    # Re-render from the merged view; errors are a list there, so feed them
    # back one by one through the single-details renderer's error path.
    errors = merged.pop("violation_patterns_errors")
    lines = pattern_fix_lines(merged)
    for error in errors:
        lines.extend(pattern_fix_lines({"violation_patterns_error": error}))
    return lines


__all__ = [
    "PATTERN_LIMIT",
    "LibraryDeclarations",
    "blocked_reason_fix_lines",
    "empty_pattern_details",
    "library_declarations",
    "narrowing_pattern_entry",
    "node_subtype_pattern_entry",
    "pattern_fix_lines",
    "violation_pattern_details",
]
