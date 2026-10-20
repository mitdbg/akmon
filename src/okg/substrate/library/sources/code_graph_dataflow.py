"""CodeDataFlowSource — Python + JS/TS data-flow extractor.

Extracted from ``library/sources/code_graph.py`` by Wave D-27
(2026-05) per ``extract-code-graph-dataflow``. Provides
`CodeDataFlowSource`, the substrate's source for data-flow
edges (env var reads, config key references, file I/O, HTTP
calls, queue/job invocations, SQL relations).

This is the largest cluster from the original code_graph.py
file (~620 LOC). It pulls in the Python AST analyzer from
`_code_graph_common` and ~30 helpers that classify call
sites by access kind (`_open_mode`, `_http_access_kind`,
`_queue_access_kind`, `_job_access_kind`, etc.) and
extract config-key / data-asset facts.

``library/sources/code_graph.py`` re-exports
`CodeDataFlowSource` at module-import time so YAML manifests
with `module: okg.substrate.library.sources.code_graph;
class: CodeDataFlowSource` keep resolving.
"""
from __future__ import annotations

import ast
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import urlparse

from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
)

from ._scope_root import require_listable_directory
from ._code_graph_common import (
    DEFAULT_CODE_EXTS,
    DEFAULT_EXCLUDES,
    _Symbol,
    _carry_cursor,
    _call_name,
    _language_for_path,
    _literal_str,
    _module_qname,
    _read_text,
    _rel,
    _sha,
    _symbol_id,
    _walk_files,
    _git_snapshot_readable_keys,
    _scope_keys_outside_walk,
    _scope_record_keys_in_walk,
)


__all__ = ["CodeDataFlowSource"]



@dataclass
class CodeDataFlowSource:
    repo: str
    repo_root: Path | str
    include_exts: Sequence[str] = DEFAULT_CODE_EXTS
    exclude_globs: Sequence[str] = DEFAULT_EXCLUDES
    max_files: int = 5000
    max_bytes_per_file: int = 1_000_000

    name: str = "code_data_flow"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): declares the change-probe family
    # so deployment lint statically confirms this source is incremental. The
    # runner reads the INSTANCE attribute `self.change_probe` set below.
    change_probe_kind = "git_scope"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe, self._snapshot_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Fail closed when the reader is used outside a Git work-tree.

        Unscoped reads use the same tracked/non-ignored-untracked inventory as
        ``change_probe``. Ignored files and clean tracked symlinks are outside
        that reader scope, so this check does not need to walk the work-tree.
        """
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        try:
            root = Path(self.repo_root).resolve()
            return in_proc_workdir(root) == root
        except Exception:  # noqa: BLE001 - unavailable Git authority fails closed
            return False

    def _build_change_probe(self):
        """A churn-immune exact snapshot over this source's scope.

        The runner checks it before invoking run(): on a no-change cycle the
        whole file walk + AST/extraction is skipped. Changed unscoped runs parse
        the exact bytes captured by this probe and return that snapshot's token,
        so the runner can durably acknowledge it without a post-read/ABA gap.
        """
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
            source_label="code_data_flow snapshot",
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
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        has_inline_record_set = (
            isinstance(cursor, dict) and cursor.get("record_set") is not None
        )
        input_snapshot: _CodeStructureInputSnapshot | None = None
        if completed_scope and not scoped:
            # Issue #2892 review. A missing or unreadable repository walks as
            # an empty one, and an empty complete scope retracts everything
            # this source published. Refuse instead.
            require_listable_directory(
                Path(self.repo_root),
                source=f"code_data_flow ({self.repo})",
                role="repo_root",
                reason_code="code_data_flow_repo_root_unreadable",
            )
        if not scoped:
            from okg.substrate.library.sources._git_helpers import in_proc_workdir

            repo_root = Path(self.repo_root).resolve()
            if in_proc_workdir(repo_root) == repo_root:
                input_snapshot = self._snapshot_probe.capture_snapshot()
        can_carry_completed_scope = (
            completed_scope
            and not scoped
            and has_record_set_authority
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
        facts: list[NodeFact | EdgeFact] = []
        files_seen = 0
        files_scanned = 0
        walked_scope_keys: set[str] = set()
        changed_scope_keys: set[str] = set()
        for event in walk.iter(walker_paths):
            files_seen += 1
            if scoped or can_carry_completed_scope:
                walked_scope_keys.add(event.rel_path)
            if not event.is_changed:
                continue
            files_scanned += 1
            if scoped or can_carry_completed_scope:
                changed_scope_keys.add(event.rel_path)
            if event.content is None:
                continue
            facts.extend(
                self._facts_for_file(
                    event.rel_path,
                    event.content,
                    event.content_hash,
                )
            )

        deleted_scope_keys_for_carry = (
            set(walk.prior_files).difference(walked_scope_keys)
            if can_carry_completed_scope else set()
        )
        # Issue #2892. When the walk read every file it saw, the facts are the
        # whole scope, even when there are none. That is the authoritative
        # replay the runner asks for after the pipeline identity changes (for
        # example when `add-source` updates the access policy), so it returns
        # the complete record set. Only a walk that skipped unchanged files may
        # carry the prior record set forward.
        full_enumeration = (
            completed_scope and not scoped and files_scanned == files_seen
        )
        record_set = (
            self._record_set_for_facts(facts)
            if (
                completed_scope
                and (
                    scoped
                    or not has_record_set_authority
                    or full_enumeration
                    or bool(facts)
                    # A changed file was re-read in full, so what it yields
                    # now (possibly nothing) is exact for its key; the
                    # replacement keys below keep every other key's prior.
                    or bool(changed_scope_keys)
                    or bool(deleted_scope_keys_for_carry)
                )
            )
            else None
        )
        record_set_replacement_keys: set[str] | None = None
        if (
            can_carry_completed_scope
            and record_set is not None
            and not full_enumeration
        ):
            record_set_replacement_keys = (
                changed_scope_keys | deleted_scope_keys_for_carry
            )
            if has_inline_record_set:
                record_set = self._record_set_with_prior(
                    cursor,
                    current=record_set,
                    replacement_keys=record_set_replacement_keys,
                )
                record_set_replacement_keys = None
                completed_scope = record_set is not None

        emitted_nodes = sum(
            1 for fact in facts if isinstance(fact, NodeFact)
        )
        emitted_edges = sum(
            1 for fact in facts if isinstance(fact, EdgeFact)
        )
        scope_status = "scoped" if scoped else "full"
        if files_seen > 0 and files_scanned == 0:
            scope_status = (
                "scoped_unchanged_noop"
                if scoped else "full_unchanged_noop"
            )
        self.last_run_stats = {
            "scope_status": scope_status,
            "scope_keys": len(scope_keys),
            "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
            "files_seen": files_seen,
            "files_scanned": files_scanned,
            "files_skipped_unchanged": max(0, files_seen - files_scanned),
            "changed_scope_keys": len(changed_scope_keys),
            "emitted_nodes": emitted_nodes,
            "emitted_edges": emitted_edges,
        }

        if scoped:
            scope_record_keys = self._scope_record_keys(scope_keys)
            deleted_scope_keys = scope_record_keys.difference(
                walked_scope_keys,
            )
            merged: dict[str, Any] = {}
            for key, value in walk.prior_files.items():
                if key in deleted_scope_keys:
                    continue
                merged[key] = value
            for key, value in walk.next_files.items():
                merged[key] = value
            if record_set is not None:
                replacement_keys = changed_scope_keys | deleted_scope_keys
                if has_inline_record_set:
                    record_set = self._record_set_with_prior(
                        cursor,
                        current=record_set,
                        replacement_keys=replacement_keys,
                    )
                    completed_scope = record_set is not None
                else:
                    record_set_replacement_keys = replacement_keys
            return SourceRun(
                facts=iter(facts),
                completed_scope=completed_scope,
                next_cursor={"files": merged, "run_id": run_id},
                run_mode=mode,
                record_set=record_set,
                record_set_replacement_keys=record_set_replacement_keys,
            )

        next_cursor = walk.next_cursor(run_id=run_id)
        if (
            completed_scope
            and has_record_set_authority
            and record_set is None
        ):
            next_cursor = preserve_cursor_record_set_authority(
                next_cursor,
                cursor,
            )
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
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
            record_set_replacement_keys=record_set_replacement_keys,
        )

    def _iter_facts(self) -> Iterable[NodeFact | EdgeFact]:
        for path in _walk_files(Path(self.repo_root), include_exts=self.include_exts, exclude_globs=self.exclude_globs, max_files=self.max_files, max_bytes_per_file=self.max_bytes_per_file):
            rel_path = _rel(Path(self.repo_root), path)
            text = _read_text(path)
            text_hash = hashlib.sha256(text.encode()).hexdigest()
            language = _language_for_path(path)
            if language == "python":
                try:
                    tree = ast.parse(text)
                except SyntaxError:
                    continue
                yield from _data_flow_facts(
                    self.repo,
                    rel_path,
                    tree,
                    _module_symbol(self.repo, rel_path, text),
                    text_hash,
                )
                continue
            yield from _data_flow_facts_from_text(
                self.repo, rel_path, text, text_hash,
            )

    def _facts_for_file(
        self,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        language = _language_for_path(Path(rel_path))
        if language == "python":
            try:
                tree = ast.parse(text)
            except SyntaxError:
                return []
            return _data_flow_facts(
                self.repo,
                rel_path,
                tree,
                _module_symbol(self.repo, rel_path, text),
                text_hash,
            )
        return _data_flow_facts_from_text(
            self.repo,
            rel_path,
            text,
            text_hash,
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
        # On a git repository the full run reads a git-visible snapshot:
        # read only keys it would read; the rest stay in the retraction set.
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


def _data_flow_facts(
    repo: str,
    rel_path: str,
    tree: ast.AST,
    module_symbol: _Symbol,
    text_hash: str,
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    revision = {"text_hash": text_hash}
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str, str]] = set()

    def enclosing(line: int) -> Optional[_Symbol]:
        del line
        return module_symbol

    def emit(facts: list[NodeFact | EdgeFact]) -> None:
        for fact in facts:
            if isinstance(fact, NodeFact):
                if fact.node_id in seen_nodes:
                    continue
                seen_nodes.add(fact.node_id)
                out.append(fact)
                continue
            key = (
                fact.src,
                fact.edge_type,
                fact.dst,
                json.dumps(fact.attrs, sort_keys=True),
            )
            if key in seen_edges:
                continue
            seen_edges.add(key)
            out.append(fact)

    text_constants: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            text_constants.append((getattr(node, "lineno", 1), node.value))
        if isinstance(node, ast.Subscript):
            line = getattr(node, "lineno", 1)
            key = _env_key_from_subscript(node)
            if key:
                emit(_config_key_fact(repo, rel_path, key, "env", True, line, enclosing(line), revision))
            key = _config_key_from_subscript(node)
            if key:
                emit(_config_key_fact(repo, rel_path, key, "settings", False, line, enclosing(line), revision))
        if isinstance(node, ast.Call):
            call_name = _call_name(node.func)
            line = getattr(node, "lineno", 1)
            for key, source_kind, required in _config_keys_from_call(node, call_name):
                emit(_config_key_fact(repo, rel_path, key, source_kind, required, line, enclosing(line), revision))
            for kind, name, access_kind in _data_assets_from_call(node, call_name):
                emit(_data_asset_fact(repo, rel_path, kind, name, access_kind, line, enclosing(line), revision))
    for line, value in text_constants:
        for relation, access_kind in _sql_relations(value):
            emit(_data_asset_fact(repo, rel_path, "sql_relation", relation, access_kind, line, enclosing(line), revision))
        for url_host in _url_hosts(value):
            emit(_data_asset_fact(repo, rel_path, "external_api_host", url_host, "reference", line, enclosing(line), revision))
        if _looks_like_path(value):
            emit(_data_asset_fact(repo, rel_path, "file_path", value, "reference", line, enclosing(line), revision))
    return out


def _data_flow_facts_from_text(
    repo: str,
    rel_path: str,
    text: str,
    text_hash: str,
) -> list[NodeFact | EdgeFact]:
    out: list[NodeFact | EdgeFact] = []
    revision = {"text_hash": text_hash}
    module_symbol = _module_symbol(repo, rel_path, text)
    seen_nodes: set[str] = set()
    seen_edges: set[tuple[str, str, str, str]] = set()

    def enclosing(line: int) -> Optional[_Symbol]:
        del line
        return module_symbol

    def emit(facts: list[NodeFact | EdgeFact]) -> None:
        for fact in facts:
            if isinstance(fact, NodeFact):
                if fact.node_id in seen_nodes:
                    continue
                seen_nodes.add(fact.node_id)
                out.append(fact)
                continue
            key = (
                fact.src,
                fact.edge_type,
                fact.dst,
                json.dumps(fact.attrs, sort_keys=True),
            )
            if key in seen_edges:
                continue
            seen_edges.add(key)
            out.append(fact)

    for line, key in _js_env_keys(text):
        emit(_config_key_fact(
            repo, rel_path, key, "env", True, line,
            enclosing(line), revision,
        ))

    for line, key in _generic_config_keys(text):
        emit(_config_key_fact(
            repo, rel_path, key, "settings", False, line,
            enclosing(line), revision,
        ))

    for line, value in _string_literals_with_lines(text):
        for relation, access_kind in _sql_relations(value):
            emit(_data_asset_fact(
                repo, rel_path, "sql_relation", relation, access_kind,
                line, enclosing(line), revision,
            ))
        for url_host in _url_hosts(value):
            emit(_data_asset_fact(
                repo, rel_path, "external_api_host", url_host,
                "reference", line, enclosing(line), revision,
            ))
        if _looks_like_path(value):
            emit(_data_asset_fact(
                repo, rel_path, "file_path", value, "reference",
                line, enclosing(line), revision,
            ))

    for line, queue_name in _js_queue_kinds(text):
        emit(_data_asset_fact(
            repo, rel_path, "queue_kind", queue_name, "publish",
            line, enclosing(line), revision,
        ))

    for line, job_name in _js_job_kinds(text):
        emit(_data_asset_fact(
            repo, rel_path, "job_kind", job_name, "schedule",
            line, enclosing(line), revision,
        ))

    return out


def _module_symbol(
    repo: str,
    rel_path: str,
    text: str,
) -> _Symbol:
    module_qname = _module_qname(rel_path)
    return _Symbol(
        symbol_id=_symbol_id(repo, rel_path, module_qname, 1),
        name=Path(rel_path).stem,
        qname=module_qname,
        kind="module",
        start_line=1,
        end_line=max(1, len(text.splitlines())),
    )


def _line_for_offset(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, offset)) + 1


def _string_literals_with_lines(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    patterns = (
        r"'([^'\\]*(?:\\.[^'\\]*)*)'",
        r'"([^"\\]*(?:\\.[^"\\]*)*)"',
        r"`([^`\\]*(?:\\.[^`\\]*)*)`",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, re.DOTALL):
            value = match.group(1)
            value = (
                value.replace("\\'", "'")
                .replace('\\"', '"')
                .replace("\\`", "`")
            )
            out.append((_line_for_offset(text, match.start()), value))
    return out


def _js_env_keys(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    pattern = re.compile(
        r"process\.env(?:\.([A-Za-z_][A-Za-z0-9_]*)|"
        r"\[\s*['\"]([^'\"]+)['\"]\s*\])"
    )
    for match in pattern.finditer(text):
        key = match.group(1) or match.group(2)
        if key:
            out.append((_line_for_offset(text, match.start()), key))
    return out


def _generic_config_keys(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    pattern = re.compile(
        r"\b(?:config|cfg|settings|conf|features|flags)"
        r"(?:\.get)?\(\s*['\"]([^'\"]+)['\"]"
    )
    for match in pattern.finditer(text):
        out.append((_line_for_offset(text, match.start()), match.group(1)))
    return out


def _js_queue_kinds(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    pattern = re.compile(
        r"\b(?:queue|bus|emitter)\.(?:publish|send|emit)"
        r"\(\s*['\"]([^'\"]+)['\"]"
    )
    for match in pattern.finditer(text):
        out.append((_line_for_offset(text, match.start()), match.group(1)))
    return out


def _js_job_kinds(text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    patterns = (
        re.compile(r"\bjobKind\s*:\s*['\"]([^'\"]+)['\"]"),
        re.compile(r"\bjob_kind\s*:\s*['\"]([^'\"]+)['\"]"),
        re.compile(
            r"\b(?:scheduler|cron|jobs)\.(?:schedule|enqueue)"
            r"\(\s*['\"]([^'\"]+)['\"]"
        ),
    )
    for pattern in patterns:
        for match in pattern.finditer(text):
            out.append((_line_for_offset(text, match.start()), match.group(1)))
    return out


def _env_key_from_subscript(node: ast.Subscript) -> str:
    target = _call_name(node.value)
    if target not in {"os.environ", "environ"}:
        return ""
    sl = node.slice
    if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
        return sl.value
    return ""


def _config_key_from_subscript(node: ast.Subscript) -> str:
    target = _call_name(node.value)
    if not _is_config_object(target):
        return ""
    sl = node.slice
    if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
        return sl.value
    return ""


def _config_keys_from_call(node: ast.Call, call_name: str) -> list[tuple[str, str, bool]]:
    if not node.args:
        return []
    first = _literal_str(node.args[0])
    if not first:
        return []
    if call_name in {"os.getenv", "getenv", "os.environ.get", "environ.get"}:
        return [(first, "env", False)]
    if call_name.endswith(".get"):
        owner = call_name.rsplit(".", 1)[0]
        if _is_config_object(owner):
            return [(first, "settings", False)]
    if call_name.endswith(".getenv"):
        return [(first, "env", False)]
    return []


def _is_config_object(name: str) -> bool:
    tail = name.rsplit(".", 1)[-1].lower()
    return tail in {"config", "cfg", "settings", "conf", "features", "flags"}


def _config_key_fact(repo: str, rel_path: str, key: str, source_kind: str, required: bool, line: int, sym: Optional[_Symbol], revision: dict[str, Any]) -> list[NodeFact | EdgeFact]:
    cid = f"config:{repo}:{source_kind}:{key}"
    attrs = {
        "config_id": cid,
        "repo": repo,
        "key": key,
        "source_kind": source_kind,
        "required": required,
        "path": rel_path,
        "line": line,
        "source_symbol_id": sym.symbol_id if sym else "",
        "text": f"{source_kind} {key}",
    }
    out: list[NodeFact | EdgeFact] = [NodeFact(
        cid,
        "config_key",
        attrs,
        {
            "repo": repo,
            "path": rel_path,
            "key": key,
            "source_kind": source_kind,
        },
        revision,
    )]
    if sym:
        out.append(EdgeFact(
            src=sym.symbol_id,
            dst=cid,
            edge_type="references",
            attrs={"line": line, "source": rel_path, "access_kind": "read"},
            source_record_id={
                "repo": repo,
                "path": rel_path,
                "key": key,
                "source_kind": source_kind,
                "symbol_id": sym.symbol_id,
            },
            source_revision=revision,
        ))
    return out


def _data_asset_fact(repo: str, rel_path: str, kind: str, name: str, access_kind: str, line: int, sym: Optional[_Symbol], revision: dict[str, Any]) -> list[NodeFact | EdgeFact]:
    aid = f"asset:{repo}:{kind}:{_sha(name)}"
    attrs = {
        "asset_id": aid,
        "repo": repo,
        "kind": kind,
        "name": name,
        "access_kind": access_kind,
        "path": rel_path,
        "line": line,
        "source_symbol_id": sym.symbol_id if sym else "",
        "text": f"{kind} {name}",
    }
    out: list[NodeFact | EdgeFact] = [NodeFact(
        aid,
        "data_asset",
        attrs,
        {
            "repo": repo,
            "path": rel_path,
            "kind": kind,
            "name": name,
        },
        revision,
    )]
    if sym:
        edge_type = _edge_type_for_access(access_kind)
        out.append(EdgeFact(
            src=sym.symbol_id,
            dst=aid,
            edge_type=edge_type,
            attrs={
                "line": line,
                "access_kind": access_kind,
                "source": rel_path,
            },
            source_record_id={
                "repo": repo,
                "path": rel_path,
                "kind": kind,
                "name": name,
                "symbol_id": sym.symbol_id,
            },
            source_revision=revision,
        ))
    return out


def _edge_type_for_access(access_kind: str) -> str:
    if access_kind in {"read", "consume", "subscribe", "receive", "request"}:
        return "reads"
    if access_kind in {"write", "publish", "send", "schedule", "mutate"}:
        return "writes"
    return "references"


def _data_assets_from_call(node: ast.Call, call_name: str) -> list[tuple[str, str, str]]:
    out: list[tuple[str, str, str]] = []
    call_tail = call_name.rsplit(".", 1)[-1].lower()
    first = _literal_str(node.args[0]) if node.args else ""
    if first:
        if call_tail in {"open", "read_text", "read_bytes"}:
            out.append(("file_path", first, "read"))
        elif call_tail in {"write_text", "write_bytes"}:
            out.append(("file_path", first, "write"))
        elif call_tail in {"path", "filepath"} or call_name in {"Path", "pathlib.Path"}:
            if _looks_like_path(first):
                out.append(("file_path", first, "reference"))
        if call_tail == "open":
            mode = _open_mode(node)
            out[-1] = ("file_path", first, "write" if _open_mode_writes(mode) else "read")
        for host in _url_hosts(first):
            out.append(("external_api_host", host, _http_access_kind(call_tail)))
    for keyword in node.keywords:
        value = _literal_str(keyword.value)
        if not value:
            continue
        key = (keyword.arg or "").lower()
        if key in {"queue", "queue_name", "topic", "stream", "channel"}:
            out.append(("queue_kind", value, _queue_access_kind(call_tail)))
        elif key in {"job", "job_name", "job_kind", "task", "task_name"}:
            out.append(("job_kind", value, _job_access_kind(call_tail)))
        elif key in {"path", "filename", "file", "filepath"} and _looks_like_path(value):
            out.append(("file_path", value, "reference"))
        elif key in {"url", "endpoint"}:
            for host in _url_hosts(value):
                out.append(("external_api_host", host, _http_access_kind(call_tail)))
    if first and _is_queue_call(call_name):
        out.append(("queue_kind", first, _queue_access_kind(call_tail)))
    if first and _is_job_call(call_name):
        out.append(("job_kind", first, _job_access_kind(call_tail)))
    return out


def _open_mode(node: ast.Call) -> str:
    if len(node.args) > 1:
        mode = _literal_str(node.args[1])
        if mode:
            return mode
    for keyword in node.keywords:
        if keyword.arg == "mode":
            return _literal_str(keyword.value)
    return "r"


def _open_mode_writes(mode: str) -> bool:
    return any(ch in mode for ch in ("w", "a", "x", "+"))


def _http_access_kind(call_tail: str) -> str:
    if call_tail in {"post", "put", "patch", "delete"}:
        return "mutate"
    if call_tail in {"get", "head", "options"}:
        return "read"
    return "reference"


def _is_queue_call(call_name: str) -> bool:
    call_name_lower = call_name.lower()
    call_tail = call_name_lower.rsplit(".", 1)[-1]
    return call_tail in {
        "enqueue",
        "publish",
        "subscribe",
        "consume",
        "send",
        "receive",
        "put",
        "get",
        "listen",
    } and (
        call_tail in {"enqueue", "publish", "subscribe", "consume"}
        or any(marker in call_name_lower for marker in ("queue", "topic", "stream", "channel", "broker"))
    )


def _is_job_call(call_name: str) -> bool:
    call_name_lower = call_name.lower()
    call_tail = call_name_lower.rsplit(".", 1)[-1]
    if "job" in call_name_lower or "task" in call_name_lower or "scheduler" in call_name_lower:
        return call_tail in {"schedule", "delay", "enqueue", "run", "perform_async", "enqueue_job", "run_job"}
    return call_tail in {"schedule", "delay", "enqueue_job", "run_job", "perform_async"}


def _queue_access_kind(call_tail: str) -> str:
    if call_tail in {"consume", "subscribe", "receive", "get", "listen"}:
        return "consume"
    if call_tail in {"enqueue", "publish", "send", "put"}:
        return "publish"
    return "reference"


def _job_access_kind(call_tail: str) -> str:
    if call_tail in {"schedule", "delay", "enqueue_job", "perform_async"}:
        return "schedule"
    return "reference"


def _sql_relations(text: str) -> list[tuple[str, str]]:
    # Source scanning sees arbitrary docstrings, test fixtures, and UI strings
    # that merely contain SQL-ish keywords. Full SQL parser inference can turn
    # those literals into multi-GB parse workloads, so keep this extractor
    # bounded and deterministic.
    return sorted(set(_regex_sql_relations(text)))


def _regex_sql_relations(text: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for m in re.finditer(r"\b(FROM|JOIN|UPDATE|INTO)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)", text, flags=re.IGNORECASE):
        verb = m.group(1).upper()
        access_kind = "write" if verb in {"UPDATE", "INTO"} else "read"
        out.append((m.group(2), access_kind))
    return out


def _url_hosts(text: str) -> list[str]:
    hosts: set[str] = set()
    for url in re.findall(r"https?://[^\s\"']+", text):
        try:
            host = urlparse(url).netloc
        except ValueError:
            # Python 3.12+ urlsplit validates bracketed hosts and raises
            # ValueError on a malformed one — e.g. a JS template-literal
            # URL like `https://[${l.value}]/...`, whose `${l.value}`
            # isn't a real IPv6 literal. Such a string isn't a usable
            # host, so skip it (treat as a plain literal) rather than let
            # it crash the whole code-data-flow source run.
            continue
        if host:
            hosts.add(host)
    return sorted(hosts)


def _looks_like_path(text: str) -> bool:
    if len(text) > 200 or "\n" in text:
        return False
    # Possessive quantifiers (`++`) keep this linear: the `[./~]?`/`[\w.-]+`
    # overlap under a greedy `(?:…)+` is catastrophic (a 113-char `../` string
    # backtracks for ~2min), and the 200-char guard above is not small enough
    # to bound it. Possessive matching is non-backtracking and, because `/` is
    # an unambiguous delimiter here, accepts exactly the same strings.
    return bool(re.match(r"^(?:[./~]?[\w.-]++/)++[\w.-]++$", text))
