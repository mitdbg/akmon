"""Interface (FastAPI/Flask/MCP/CLI) endpoint source.

Extracted from ``library/sources/code_graph.py`` by Wave D-29
(2026-05) per ``extract-code-graph-interface``. Provides
`InterfaceSource`, the substrate's source for endpoint
facts: FastAPI/Flask/MCP-tool decorators and argparse CLI
subcommands get projected as endpoint nodes + edges.

The 9 helpers in this module are 100% specific to that
projection — they have no callers outside InterfaceSource.

``library/sources/code_graph.py`` re-exports `InterfaceSource`
at module-import time so YAML manifests with `module:
okg.substrate.library.sources.code_graph;
class: InterfaceSource` keep resolving.
"""
from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)

from ._code_graph_common import (
    DEFAULT_EXCLUDES,
    _PythonAnalyzer,
    _analyze_python,
    _call_name,
    _carry_cursor,
    _literal_str,
    _sha,
    _walk_files,
    _git_snapshot_readable_keys,
    _scope_keys_outside_walk,
    _scope_record_keys_in_walk,
)


__all__ = ["InterfaceSource"]


PY_EXTS = {".py"}



@dataclass
class InterfaceSource:
    repo: str
    repo_root: Path | str
    include_exts: Sequence[str] = tuple(sorted(PY_EXTS))
    exclude_globs: Sequence[str] = DEFAULT_EXCLUDES
    max_files: int = 5000
    max_bytes_per_file: int = 1_000_000

    name: str = "interfaces"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): churn-immune git scope probe;
    # the runner skips this source's FileWalk + AST extraction on no-change.
    change_probe_kind = "git_scope"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe, self._snapshot_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Fail closed unless an exact repository snapshot can be captured."""
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        try:
            return in_proc_workdir(self.repo_root) == self.repo_root
        except Exception:  # noqa: BLE001 - unavailable Git authority fails closed
            return False

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources._code_graph_git_snapshot import (
            build_git_visible_code_snapshot_probe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

        include_exts = {str(ext).lower() for ext in self.include_exts}
        config = {
            "repo": self.repo,
            "include_exts": sorted(self.include_exts),
            "exclude_globs": sorted(self.exclude_globs),
            "include_untracked": True,
            "max_files": self.max_files,
            "max_bytes_per_file": self.max_bytes_per_file,
        }
        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
        )

        def _in_scope(rel_path: str) -> bool:
            return Path(rel_path).suffix.lower() in include_exts

        change_probe = GitScopeProbe(
            repo_root=self.repo_root,
            config=config,
            emit_targets=emit_targets,
            scope_filter=_in_scope,
        )
        snapshot_probe = build_git_visible_code_snapshot_probe(
            repo_root=self.repo_root,
            config=config,
            emit_targets=emit_targets,
            scope_filter=_in_scope,
            max_files=self.max_files,
            max_bytes_per_file=self.max_bytes_per_file,
            source_label="interface snapshot",
        )
        return change_probe, snapshot_probe

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        # Per `add-delta-proportional-source-runs`: delegate the
        # walk + read + hash + skip-on-match to the substrate's
        # shared FileWalk helper.
        # Per `shrink-source-walk-cost`: also honor the watcher's
        # `sync_scope.scope_keys` to narrow the walk to changed
        # files only.
        from okg.substrate.library.sources._file_walk import FileWalk
        from okg.substrate.library.sources.code_graph_structure import (
            _CapturedCodeWalk,
            _CodeStructureInputSnapshot,
        )

        scope_keys = tuple(
            str(k).replace("\\", "/")
            for k in (sync_scope or {}).get("scope_keys", ())
            if k
        )
        scoped = (
            mode in {"scope_incomplete", "scope_complete"}
            and bool(scope_keys)
        )
        # Scope keys outside this source's paths are dropped from the read
        # and the retraction set, and counted (issue #3259).
        scope_keys_outside_source_paths = (
            _scope_keys_outside_walk(
                Path(self.repo_root),
                scope_keys,
                include_exts=self.include_exts,
                exclude_globs=self.exclude_globs,
            )
            if scoped else 0
        )
        completed_scope = mode in {"reconcile", "scope_complete"}
        cursor = _carry_cursor(
            cursor,
            scoped=scoped,
            authoritative_replay=authoritative_replay,
        )
        can_carry_completed_scope = (
            completed_scope
            and not scoped
            and isinstance(cursor, dict)
            and cursor.get("record_set") is not None
        )
        input_snapshot: _CodeStructureInputSnapshot | None = None
        if not scoped and self.probe_short_circuit_safe:
            input_snapshot = self._snapshot_probe.capture_snapshot()

        facts: list[NodeFact | EdgeFact] = []
        emit_legacy_endpoint_retractions = (
            isinstance(cursor, dict)
            and not cursor.get("interface_endpoint_stable_ids_v6")
        )
        prior_walk_cursor = (
            cursor
            if (
                mode in {"cursor", "scope_incomplete"}
                or scoped
                or can_carry_completed_scope
            )
            else None
        )
        walk = (
            _CapturedCodeWalk(
                snapshot=input_snapshot,
                prior_cursor=prior_walk_cursor,
            )
            if input_snapshot is not None
            else FileWalk(
                prior_cursor=prior_walk_cursor,
                repo_root=Path(self.repo_root),
                read_as="text",
            )
        )
        walker_paths: Iterable[Path]
        if scoped:
            walker_paths = self._scoped_paths(scope_keys)
        elif input_snapshot is not None:
            walker_paths = tuple(item.path for item in input_snapshot.files)
        else:
            walker_paths = _walk_files(
                Path(self.repo_root),
                include_exts=self.include_exts,
                exclude_globs=self.exclude_globs,
                max_files=self.max_files,
                max_bytes_per_file=self.max_bytes_per_file,
            )
        self.last_run_stats = {
            "scope_status": "scoped" if scoped else "full",
            "scope_keys": len(scope_keys),
            "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
        }
        walked_scope_keys: set[str] = set()
        changed_scope_keys: set[str] = set()
        for event in walk.iter(walker_paths):
            if scoped or can_carry_completed_scope:
                walked_scope_keys.add(event.rel_path)
            if not event.is_changed:
                continue
            if scoped or can_carry_completed_scope:
                changed_scope_keys.add(event.rel_path)
            try:
                analyzer = _analyze_python(
                    self.repo, event.rel_path, event.content,
                )
            except SyntaxError:
                continue
            # `_analyze_python` already compiled this file; reuse its tree
            # rather than paying `ast.parse` twice per file (issue #1325
            # item 3). `analyzer.tree` is the AST it visited, so the facts
            # below are derived from exactly the same parse as the symbols.
            tree = analyzer.tree
            if tree is None:
                # Unreachable while `_analyze_python` sets it. Raise rather
                # than skip: dropping a file's facts here would shrink a
                # completed scope and read as a deletion.
                raise SubstrateError(
                    "InterfaceSource: analyzer published no syntax tree",
                    data={"repo": self.repo, "path": event.rel_path},
                )
            facts.extend(_interface_facts(
                self.repo,
                event.rel_path,
                tree,
                analyzer,
                event.content_hash,
                text=event.content,
                emit_legacy_mcp_tool_retractions=(
                    emit_legacy_endpoint_retractions
                ),
            ))

        record_set = (
            self._record_set_for_facts(facts)
            if completed_scope else None
        )
        if can_carry_completed_scope and record_set is not None:
            deleted_scope_keys = set(walk.prior_files).difference(
                walked_scope_keys,
            )
            record_set = self._record_set_with_prior(
                cursor,
                current=record_set,
                replacement_keys=(
                    changed_scope_keys | deleted_scope_keys
                ),
            )
            completed_scope = record_set is not None

        # Same merge-on-scoped-run logic as CodeStructureSource.
        if scoped:
            scope_record_keys = self._scope_record_keys(scope_keys)
            deleted_scope_keys = scope_record_keys.difference(
                walked_scope_keys,
            )
            merged: dict[str, Any] = {}
            for k, v in walk.prior_files.items():
                if k in deleted_scope_keys:
                    continue
                merged[k] = v
            for k, v in walk.next_files.items():
                merged[k] = v
            if record_set is not None:
                record_set = self._record_set_with_prior(
                    cursor,
                    current=record_set,
                    replacement_keys=(
                        changed_scope_keys | deleted_scope_keys
                    ),
                )
                completed_scope = record_set is not None
            return SourceRun(
                facts=iter(facts),
                completed_scope=completed_scope,
                next_cursor={
                    "files": merged,
                    "run_id": run_id,
                    "interface_endpoint_stable_ids_v6": True,
                },
                run_mode=mode,
                record_set=record_set,
            )

        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=walk.next_cursor(
                run_id=run_id,
                interface_endpoint_stable_ids_v6=True,
            ),
            applied_raw_token=(
                input_snapshot.token if input_snapshot is not None else None
            ),
            applied_token_scope=(
                "global" if input_snapshot is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if input_snapshot is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
        )

    def _record_set_with_prior(
        self,
        cursor: Optional[dict[str, Any]],
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> Optional[RecordSet]:
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        prior = RecordSet.from_cursor(prior_raw)
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    @staticmethod
    def _record_set_for_facts(
        facts: Sequence[NodeFact | EdgeFact],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for fact in facts:
            if getattr(fact, "op", "I") == "R":
                continue
            path = fact.source_record_id.get("path")
            if not path:
                continue
            emission = records.setdefault(str(path), RecordEmission())
            if isinstance(fact, NodeFact):
                emission.descendants.append(RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                ))
            else:
                emission.edges.append(EdgeKey(
                    src=fact.src,
                    dst=fact.dst,
                    edge_type=fact.edge_type,
                    attrs=dict(fact.attrs),
                    provenance=fact.provenance,
                    confidence=fact.confidence,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                ))
        return RecordSet(records=records)

    def _scope_record_keys(self, scope_keys: tuple[str, ...]) -> set[str]:
        """Repo-relative record keys governed by `scope_keys`.

        Only keys the full walk would select (issue #3259); deleted files
        are kept so a scoped complete run can retract records that vanished.
        """
        return _scope_record_keys_in_walk(
            Path(self.repo_root),
            scope_keys,
            include_exts=self.include_exts,
            exclude_globs=self.exclude_globs,
        )

    def _scoped_paths(self, scope_keys: tuple[str, ...]) -> list[Path]:
        """Map watcher-supplied repo-relative paths to Paths,
        applying this source's include_exts / exclude_globs.

        On a git repository the full run reads a git-visible snapshot, so
        only keys that snapshot would read are read here; the rest stay in
        the retraction set (`_scope_record_keys`), as the full run would
        retract them."""
        keys = self._scope_record_keys(scope_keys)
        if self.probe_short_circuit_safe:
            keys = _git_snapshot_readable_keys(Path(self.repo_root), keys)
        out: list[Path] = []
        for key in sorted(keys):
            candidate = Path(self.repo_root) / key
            if not candidate.is_file():
                continue
            out.append(candidate)
        return out


def _interface_facts(
    repo: str,
    rel_path: str,
    tree: ast.AST,
    analyzer: _PythonAnalyzer,
    text_hash: str,
    *,
    text: str | None = None,
    emit_legacy_mcp_tool_retractions: bool = False,
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    revision = {"text_hash": text_hash}
    parser_paths = _argparse_parser_paths(tree, text=text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            handler = analyzer.name_to_symbol.get(node.name)
            for dec in node.decorator_list:
                implicit_mcp_tool = _is_implicit_mcp_tool(dec)
                kind, name, route, method = _decorator_endpoint(
                    dec, fallback_name=node.name,
                )
                if not kind:
                    continue
                eid = _decorator_endpoint_id(
                    repo=repo,
                    rel_path=rel_path,
                    kind=kind,
                    name=name,
                    route=route,
                    method=method,
                    handler=node.name,
                )
                attrs = {
                    "endpoint_id": eid,
                    "repo": repo,
                    "kind": kind,
                    "name": name,
                    "path": rel_path,
                    "method": method,
                    "route": route,
                    "handler": node.name,
                    "handler_symbol_id": handler.symbol_id if handler else "",
                    "start_line": node.lineno,
                    "text": f"{kind} {name} {route} {node.name}".strip(),
                }
                out.append(NodeFact(eid, "interface_endpoint", attrs, {"repo": repo, "path": rel_path, "endpoint_id": eid}, revision))
                if handler:
                    out.append(EdgeFact(
                        src=eid,
                        dst=handler.symbol_id,
                        edge_type="routes_to",
                        attrs={"handler": node.name},
                        source_record_id={
                            "repo": repo,
                            "path": rel_path,
                            "endpoint_id": eid,
                        },
                        source_revision=revision,
                    ))
                if (
                    emit_legacy_mcp_tool_retractions
                    and kind in {"http_route", "mcp_tool"}
                ):
                    for legacy_eid in _legacy_decorator_endpoint_ids(
                        repo,
                        rel_path,
                        name=name,
                        kind=kind,
                        node=node,
                        decorator=dec,
                        implicit_mcp_tool=implicit_mcp_tool,
                    ):
                        if legacy_eid == eid:
                            continue
                        out.append(NodeFact(
                            legacy_eid,
                            "interface_endpoint",
                            {
                                "endpoint_id": legacy_eid,
                                "repo": repo,
                                "kind": kind,
                                "name": "tool" if implicit_mcp_tool else name,
                                "path": rel_path,
                                "start_line": node.lineno,
                                "legacy_retraction": True,
                            },
                            {
                                "repo": repo,
                                "path": rel_path,
                                "endpoint_id": legacy_eid,
                            },
                            revision,
                            op="R",
                        ))
        if isinstance(node, ast.Call):
            call_name = _call_name(node.func)
            if call_name.endswith("add_parser") and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                cmd = node.args[0].value
                receiver = call_name.rsplit(".", 1)[0] if "." in call_name else ""
                parent = parser_paths.get(receiver, "")
                qualified = f"{parent} {cmd}".strip()
                eid = _cli_endpoint_id(
                    repo=repo, rel_path=rel_path, qualified=qualified,
                )
                attrs = {
                    "endpoint_id": eid,
                    "repo": repo,
                    "kind": "cli_command",
                    "name": qualified,
                    "command": cmd,
                    "parent": parent,
                    "path": rel_path,
                    "start_line": node.lineno,
                    "text": f"cli {qualified}",
                }
                out.append(NodeFact(eid, "interface_endpoint", attrs, {"repo": repo, "path": rel_path, "endpoint_id": eid}, revision))
                if emit_legacy_mcp_tool_retractions:
                    for legacy_eid in _legacy_cli_endpoint_ids(
                        repo, rel_path, cmd=cmd, node=node,
                    ):
                        if legacy_eid == eid:
                            continue
                        out.append(NodeFact(
                            legacy_eid,
                            "interface_endpoint",
                            {
                                "endpoint_id": legacy_eid,
                                "repo": repo,
                                "kind": "cli_command",
                                "name": cmd,
                                "path": rel_path,
                                "start_line": node.lineno,
                                "legacy_retraction": True,
                            },
                            {
                                "repo": repo,
                                "path": rel_path,
                                "endpoint_id": legacy_eid,
                            },
                            revision,
                            op="R",
                        ))
    return out


def _decorator_endpoint(
    dec: ast.AST, *, fallback_name: str = "",
) -> tuple[str, str, str, str]:
    if isinstance(dec, ast.Call):
        called = _call_name(dec.func)
        first = _literal_str(dec.args[0]) if dec.args else ""
        if not first:
            first = next(
                (
                    _literal_str(kw.value)
                    for kw in dec.keywords
                    if kw.arg == "name"
                ),
                "",
            )
        if called.endswith(".tool") or called == "tool":
            name = str(first or fallback_name or "")
            return ("mcp_tool", name or "tool", "", "")
        for meth in ("get", "post", "put", "delete", "patch", "route"):
            if called.endswith(f".{meth}"):
                route = str(first or "")
                return ("http_route", f"{meth.upper()} {route}", route, meth.upper())
    return ("", "", "", "")


def _is_implicit_mcp_tool(dec: ast.AST) -> bool:
    if not isinstance(dec, ast.Call):
        return False
    called = _call_name(dec.func)
    if not (called.endswith(".tool") or called == "tool"):
        return False
    if dec.args and _literal_str(dec.args[0]):
        return False
    return not any(
        kw.arg == "name" and _literal_str(kw.value)
        for kw in dec.keywords
    )


def _decorator_endpoint_id(
    *,
    repo: str,
    rel_path: str,
    kind: str,
    name: str,
    route: str,
    method: str,
    handler: str,
) -> str:
    payload = json.dumps(
        [rel_path, kind, name, route, method, handler],
        sort_keys=True,
    )
    return f"endpoint:{repo}:{kind}:{_sha(payload)}"


def _cli_endpoint_id(
    *, repo: str, rel_path: str, qualified: str,
) -> str:
    payload = json.dumps([rel_path, "cli_command", qualified], sort_keys=True)
    return f"endpoint:{repo}:cli:{_sha(payload)}"


# The only two attribute calls `_argparse_parser_paths` can act on. A call
# reaches `_call_name` through an `ast.Attribute`, whose `attr` is copied
# verbatim from the module text, so a file whose text contains neither
# identifier cannot produce a parser path -- and the whole-module `ast.walk`
# below is pure cost on it (issue #1325 item 3: this walk ran on all 3,732
# Python files in the repo, of which a handful build subparsers).
_ARGPARSE_SUBPARSER_MARKERS = ("add_subparsers", "add_parser")


def _argparse_parser_paths(
    tree: ast.AST,
    *,
    text: str | None = None,
) -> dict[str, str]:
    if text is not None and not any(
        marker in text for marker in _ARGPARSE_SUBPARSER_MARKERS
    ):
        return {}
    assignments = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
    ]
    paths: dict[str, str] = {}
    for _ in range(8):
        changed = False
        for assign in assignments:
            if len(assign.targets) != 1:
                continue
            target = assign.targets[0]
            if not isinstance(target, ast.Name):
                continue
            if not isinstance(assign.value, ast.Call):
                continue
            call_name = _call_name(assign.value.func)
            receiver = (
                call_name.rsplit(".", 1)[0]
                if "." in call_name else ""
            )
            if call_name.endswith(".add_subparsers"):
                next_path = paths.get(receiver, "")
            elif (
                call_name.endswith(".add_parser")
                and assign.value.args
                and _literal_str(assign.value.args[0])
            ):
                parent = paths.get(receiver, "")
                cmd = _literal_str(assign.value.args[0])
                next_path = f"{parent} {cmd}".strip()
            else:
                continue
            if paths.get(target.id) != next_path:
                paths[target.id] = next_path
                changed = True
        if not changed:
            break
    return paths


def _legacy_decorator_endpoint_ids(
    repo: str,
    rel_path: str,
    *,
    name: str,
    kind: str,
    node: ast.AST,
    decorator: ast.AST,
    implicit_mcp_tool: bool,
) -> list[str]:
    lines = {getattr(node, "lineno", 0), getattr(decorator, "lineno", 0)}
    node_line = getattr(node, "lineno", 0)
    if node_line:
        lines.update(range(max(1, node_line - 1500), node_line + 1501))
    names = {name}
    if implicit_mcp_tool and name != "tool":
        names.add("tool")
    out = []
    for candidate_name in sorted(n for n in names if n):
        out.extend(
            f"endpoint:{repo}:{kind}:"
            f"{_sha(rel_path + ':' + candidate_name + ':' + str(line))}"
            for line in sorted(lines)
            if line
        )
    return out


def _legacy_cli_endpoint_ids(
    repo: str,
    rel_path: str,
    *,
    cmd: str,
    node: ast.AST,
) -> list[str]:
    node_line = getattr(node, "lineno", 0)
    lines = set()
    if node_line:
        lines.update(range(max(1, node_line - 1500), node_line + 1501))
    return [
        f"endpoint:{repo}:cli:"
        f"{_sha(rel_path + ':' + cmd + ':' + str(line))}"
        for line in sorted(lines)
        if line
    ]
