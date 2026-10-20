"""Explicit source for LSP-backed code resolution edges."""
from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from okg.substrate.library.extraction.code._protocols import Symbol
from okg.substrate.library.extraction.code.dispatch import code_extractor_for
from okg.substrate.library.extraction.code.lsp import (
    LspSemanticResolver,
    LspServerConfig,
    lsp_definition_candidate_count,
    lsp_definition_filter_fingerprint,
)
from okg.substrate.library.extraction.code.lsp_protocol import (
    LSP_SPEC_VERSION,
    semantic_key_for_symbol,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    RecordEmission,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
)

from ._code_graph_common import (
    DEFAULT_EXCLUDES,
    _dedupe_facts,
    _module_qname,
    _sha,
    _symbol_id,
    _walk_files,
    _scope_keys_outside_walk,
    _scope_record_keys_in_walk,
)


__all__ = ["CodeLspResolutionSource"]


_LSP_RESOLUTION_SCHEMA = "okg.code_lsp_resolution.v2"
_LSP_CACHE_SCHEMA = "okg.code_lsp_resolution.cache.v1"


@dataclass(frozen=True)
class _CachedLspFacts:
    facts: tuple[EdgeFact, ...]
    remapped: bool = False


@dataclass
class CodeLspResolutionSource:
    repo: str
    repo_root: Path | str
    server_command: Sequence[str]
    server_name: str
    language_id: str = "python"
    server_version: str = ""
    include_exts: Sequence[str] = (".py",)
    exclude_globs: Sequence[str] = DEFAULT_EXCLUDES
    max_files: int = 5000
    max_bytes_per_file: int = 1_000_000
    dependency_fanout_limit: int = 50
    reverse_dependency_fanout_limit: int = 50
    timeout_s: float = 5.0
    max_lsp_requests: int = 2000
    definition_request_concurrency: int = 16
    hot_path_max_lsp_requests: int = 0
    initialization_options: dict[str, Any] = field(default_factory=dict)

    name: str = "code_lsp_resolution"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): the runner evaluates this
    # content-identity probe before starting the file walk or language server.
    change_probe_kind = "git_scope"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self):
        """Cover every input that can change emitted LSP-resolution facts."""
        import sys

        from okg.substrate.library.extraction.code import (
            _protocols as code_protocols,
        )
        from okg.substrate.library.extraction.code import dispatch as code_dispatch
        from okg.substrate.library.extraction.code import lsp as lsp_extraction
        from okg.substrate.library.extraction.code import (
            lsp_protocol as lsp_protocol_module,
        )
        from okg.substrate.library.sources import (
            _code_graph_common as code_graph_common,
        )
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

        include_exts = {str(ext).lower() for ext in self.include_exts}

        def _in_read_scope(rel_path: str) -> bool:
            return Path(rel_path).suffix.lower() in include_exts

        # The dispatch registry is the authority for the effective language
        # extractor. Fingerprint its registered factory/class as well as the
        # dispatch and LSP modules so extraction changes invalidate the token.
        extractor_factory = getattr(code_dispatch, "_REGISTRY", {}).get(
            self.language_id
        )
        emit_targets = [
            sys.modules[__name__],
            code_graph_common,
            code_protocols,
            code_dispatch,
            lsp_extraction,
            lsp_protocol_module,
        ]
        if extractor_factory is not None:
            emit_targets.append(extractor_factory)

        return GitScopeProbe(
            repo_root=self.repo_root,
            config={
                "repo": self.repo,
                "repo_root": str(self.repo_root),
                "server_command": [str(part) for part in self.server_command],
                "server_name": self.server_name,
                "server_version": self.server_version,
                "language_id": self.language_id,
                "include_exts": sorted(str(ext) for ext in self.include_exts),
                "exclude_globs": sorted(
                    str(pattern) for pattern in self.exclude_globs
                ),
                "max_files": self.max_files,
                "max_bytes_per_file": self.max_bytes_per_file,
                "dependency_fanout_limit": self.dependency_fanout_limit,
                "reverse_dependency_fanout_limit": (
                    self.reverse_dependency_fanout_limit
                ),
                "timeout_s": self.timeout_s,
                "max_lsp_requests": self.max_lsp_requests,
                "definition_request_concurrency": (
                    self.definition_request_concurrency
                ),
                "hot_path_max_lsp_requests": self.hot_path_max_lsp_requests,
                "initialization_options": self.initialization_options,
                # Explicit for auditability: the source reads matching
                # untracked files, so the probe must cover them too.
                "include_untracked": True,
            },
            emit_targets=emit_targets,
            scope_filter=_in_read_scope,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        from okg.substrate.library.sources._file_walk import FileWalk

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
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        prior_cache_files = self._prior_lsp_cache_files(cursor)
        if scoped:
            walker_paths = self._scoped_paths(scope_keys)
        else:
            walker_paths = _walk_files(
                Path(self.repo_root),
                include_exts=self.include_exts,
                exclude_globs=self.exclude_globs,
                max_files=self.max_files,
                max_bytes_per_file=self.max_bytes_per_file,
            )

        walk = FileWalk(
            prior_cursor=cursor,
            repo_root=Path(self.repo_root),
            read_as="text",
        )

        files_seen = 0
        changed_paths: list[str] = []
        changed_content: dict[str, str] = {}
        walked_scope_keys: set[str] = set()
        for event in walk.iter(walker_paths):
            files_seen += 1
            if scoped:
                walked_scope_keys.add(event.rel_path)
            if not event.is_changed:
                continue
            changed_paths.append(event.rel_path)
            if isinstance(event.content, str):
                changed_content[event.rel_path] = event.content

        deleted_scope_keys = (
            self._scope_record_keys(scope_keys).difference(walked_scope_keys)
            if scoped else set()
        )
        pending_deferred_paths = (
            self._pending_deferred_paths(cursor)
            if mode == "reconcile"
            else set()
        )
        next_files = (
            _merged_scoped_files(
                walk.prior_files,
                walk.next_files,
                deleted_scope_keys,
            )
            if scoped else walk.next_files
        )

        if not changed_paths and not deleted_scope_keys and not pending_deferred_paths:
            next_cursor = {
                **walk.next_cursor(
                    lsp_resolution_schema=_LSP_RESOLUTION_SCHEMA,
                    lsp_resolution_cache=self._lsp_cache_cursor(
                        prior_cache_files,
                        next_files=next_files,
                        deleted_paths=deleted_scope_keys,
                    ),
                ),
                "files": next_files,
            }
            if completed_scope and has_record_set_authority:
                next_cursor = preserve_cursor_record_set_authority(
                    next_cursor,
                    cursor,
                )
            elif scoped:
                completed_scope = False
            self.last_run_stats = {
                "scope_status": "scoped" if scoped else "full",
                "scope_keys": len(scope_keys),
                "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
                "files_seen": files_seen,
                "changed_file_count": 0,
                "deleted_file_count": 0,
                "files_opened": 0,
                "files_skipped_unchanged": files_seen,
                "dependency_fanout": 0,
                "reverse_dependency_fanout": 0,
                "lsp_requests": 0,
                "references_reused": 0,
                "cache_hit_files": 0,
                "cache_miss_files": 0,
                "deferred_files": 0,
                "deferred_lsp_requests": 0,
                "definition_request_concurrency": self.definition_request_concurrency,
                "references_inspected": 0,
                "references_resolved": 0,
                "record_set_authority": (
                    "preserved"
                    if completed_scope and has_record_set_authority
                    else "not_available"
                ),
                "server_reused": False,
                "status": "no_change",
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=completed_scope,
                next_cursor=next_cursor,
                run_mode=mode,
                record_set=None,
                # Exactly the condition under which the cursor above kept the
                # prior authority. Without it the runner infers a footprint
                # from the empty stream and retracts every resolved reference
                # (issue #2122).
                preserves_prior_record_set=(
                    completed_scope and has_record_set_authority
                ),
            )

        if deleted_scope_keys and not changed_paths:
            record_set = RecordSet() if has_record_set_authority else None
            record_set_replacement_keys = (
                set(deleted_scope_keys) if record_set is not None else None
            )
            if scoped and not has_record_set_authority:
                completed_scope = False
            next_cursor = {
                **walk.next_cursor(
                    lsp_resolution_schema=_LSP_RESOLUTION_SCHEMA,
                    lsp_resolution_cache=self._lsp_cache_cursor(
                        prior_cache_files,
                        next_files=next_files,
                        deleted_paths=deleted_scope_keys,
                    ),
                ),
                "files": next_files,
            }
            self.last_run_stats = {
                "scope_status": "scoped" if scoped else "full",
                "scope_keys": len(scope_keys),
                "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
                "files_seen": files_seen,
                "changed_file_count": 0,
                "deleted_file_count": len(deleted_scope_keys),
                "files_opened": 0,
                "files_skipped_unchanged": files_seen,
                "dependency_fanout": 0,
                "reverse_dependency_fanout": 0,
                "lsp_requests": 0,
                "references_reused": 0,
                "cache_hit_files": 0,
                "cache_miss_files": 0,
                "deferred_files": 0,
                "deferred_lsp_requests": 0,
                "definition_request_concurrency": self.definition_request_concurrency,
                "references_inspected": 0,
                "references_resolved": 0,
                "record_set_authority": (
                    "replacement_slice"
                    if record_set_replacement_keys is not None
                    else "not_available"
                ),
                "server_reused": False,
                "status": "deleted_scope",
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=completed_scope,
                next_cursor=next_cursor,
                run_mode=mode,
                record_set=record_set,
                record_set_replacement_keys=record_set_replacement_keys,
            )

        fanout_content = dict(changed_content)
        for rel_path in pending_deferred_paths:
            if rel_path in fanout_content:
                continue
            try:
                fanout_content[rel_path] = (
                    Path(self.repo_root) / rel_path
                ).read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
        fanout_paths = self._dependency_fanout(fanout_content)
        reverse_fanout_paths = self._reverse_dependency_fanout(changed_paths)
        resolve_path_set = {
            *changed_paths,
            *reverse_fanout_paths,
            *pending_deferred_paths,
        }
        if completed_scope and not scoped:
            resolve_path_set.update(prior_cache_files)
        paths_to_resolve = sorted(resolve_path_set)
        paths_to_open = sorted({*paths_to_resolve, *fanout_paths})
        workspace_fingerprint = _sha(
            json.dumps(next_files, sort_keys=True, default=str),
            n=24,
        )
        definition_filter_fingerprint = lsp_definition_filter_fingerprint(
            language_id=self.language_id,
            workspace_paths=tuple(next_files),
        )
        source_frontier = _sha(
            json.dumps({
                "mode": mode,
                "scope_keys": scope_keys,
                "changed_paths": changed_paths,
                "fanout_paths": fanout_paths,
                "reverse_fanout_paths": reverse_fanout_paths,
                "deleted_scope_keys": sorted(deleted_scope_keys),
                "pending_deferred_paths": sorted(pending_deferred_paths),
            }, sort_keys=True, default=str),
            n=24,
        )
        semantic_symbol_ids = self._semantic_symbol_ids(paths_to_open)
        cached_facts_by_path: dict[str, _CachedLspFacts] = {}
        cache_hit_paths: set[str] = set()
        cache_miss_paths: set[str] = set()
        changed_path_set = set(changed_paths)
        for rel_path in paths_to_resolve:
            cached_facts = self._cached_facts_for_path(
                rel_path,
                prior_cache_files.get(rel_path),
                next_files=next_files,
                semantic_symbol_ids=semantic_symbol_ids,
                changed_paths=changed_path_set,
                workspace_fingerprint=workspace_fingerprint,
                source_frontier=source_frontier,
                definition_filter_fingerprint=definition_filter_fingerprint,
            )
            if cached_facts is None:
                cache_miss_paths.add(rel_path)
                continue
            cached_facts_by_path[rel_path] = cached_facts
            cache_hit_paths.add(rel_path)

        reference_counts = self._reference_counts_by_path(
            sorted(cache_miss_paths),
            changed_content=changed_content,
            workspace_paths=tuple(next_files),
        )
        all_lsp_reference_paths = [
            path for path, count in reference_counts.items() if count > 0
        ]
        (
            paths_with_lsp_references,
            deferred_paths,
            deferred_lsp_requests,
        ) = self._apply_lsp_hot_path_budget(
            all_lsp_reference_paths,
            reference_counts=reference_counts,
            mode=mode,
        )
        no_reference_miss_paths = cache_miss_paths.difference(
            all_lsp_reference_paths,
        )
        paths_to_open_for_lsp = sorted({
            *paths_with_lsp_references,
            *fanout_paths,
        })
        report = None
        if paths_with_lsp_references:
            resolver = LspSemanticResolver(
                repo=self.repo,
                repo_root=Path(self.repo_root),
                config=LspServerConfig(
                    command=list(self.server_command),
                    language_id=self.language_id,
                    server_name=self.server_name,
                    server_version=self.server_version,
                    initialization_options=self.initialization_options,
                    timeout_s=self.timeout_s,
                    max_definition_requests=self.max_lsp_requests,
                    definition_request_concurrency=(
                        self.definition_request_concurrency
                    ),
                ),
            )
            report = resolver.resolve_workspace(
                paths_to_open_for_lsp,
                resolve_paths=paths_with_lsp_references,
                workspace_fingerprint=workspace_fingerprint,
                source_frontier=source_frontier,
            )
        fresh_resolved = list(report.resolved) if report is not None else []
        cache_remapped_paths = {
            rel_path
            for rel_path, cached in cached_facts_by_path.items()
            if cached.remapped
        }
        all_cached_facts = [
            fact
            for rel_path in sorted(cached_facts_by_path)
            for fact in cached_facts_by_path[rel_path].facts
        ]
        cached_facts = [
            fact
            for rel_path in sorted(cached_facts_by_path)
            if completed_scope and (
                not scoped or rel_path in cache_remapped_paths
            )
            for fact in cached_facts_by_path[rel_path].facts
        ]
        fresh_facts = [
            fact
            for resolved in fresh_resolved
            for fact in resolved.edge_facts()
        ]
        resolve_set = set(paths_to_resolve)
        facts = _dedupe_facts([
            fact
            for fact in [*cached_facts, *fresh_facts]
            if str(fact.source_record_id.get("path") or "") in resolve_set
        ])
        references_reused = len({
            fact.src
            for fact in all_cached_facts
            if fact.edge_type == "references"
        })
        fresh_facts_by_path: dict[str, list[EdgeFact]] = {}
        for fact in fresh_facts:
            rel_path = str(fact.source_record_id.get("path") or "")
            if rel_path:
                fresh_facts_by_path.setdefault(rel_path, []).append(fact)
        next_cache_files = self._next_lsp_cache_files(
            prior_cache_files,
            next_files=next_files,
            deleted_paths=deleted_scope_keys,
        )
        for rel_path in deferred_paths:
            next_cache_files.pop(rel_path, None)
        for rel_path in sorted(cache_hit_paths):
            cached = cached_facts_by_path.get(rel_path)
            if cached is None:
                continue
            if not cached.remapped and rel_path in prior_cache_files:
                next_cache_files[rel_path] = prior_cache_files[rel_path]
                continue
            next_cache_files[rel_path] = self._cache_entry(
                rel_path,
                next_files=next_files,
                facts=cached.facts,
                references_inspected=self._cached_references_inspected(
                    prior_cache_files.get(rel_path),
                ),
                references_resolved=self._cached_references_resolved(
                    prior_cache_files.get(rel_path),
                ),
                unresolved_by_reason={},
                definition_filter_fingerprint=definition_filter_fingerprint,
            )
        for rel_path in sorted(no_reference_miss_paths):
            next_cache_files[rel_path] = self._cache_entry(
                rel_path,
                next_files=next_files,
                facts=[],
                references_inspected=0,
                references_resolved=0,
                unresolved_by_reason={},
                definition_filter_fingerprint=definition_filter_fingerprint,
            )
        if report is not None:
            for rel_path in paths_with_lsp_references:
                next_cache_files[rel_path] = self._cache_entry(
                    rel_path,
                    next_files=next_files,
                    facts=fresh_facts_by_path.get(rel_path, []),
                    references_inspected=(
                        report.references_inspected_by_path.get(rel_path, 0)
                    ),
                    references_resolved=(
                        report.references_resolved_by_path.get(rel_path, 0)
                    ),
                    unresolved_by_reason=dict(
                        report.unresolved_by_path.get(rel_path, {}),
                    ),
                    definition_filter_fingerprint=definition_filter_fingerprint,
                )
        if deferred_paths:
            completed_scope = False
        record_set = None
        record_set_replacement_keys: set[str] | None = None
        if completed_scope and not scoped:
            record_set = self._record_set_for_facts(facts)
        elif scoped and has_record_set_authority:
            record_set = self._record_set_for_facts(facts)
            record_set_replacement_keys = (
                cache_miss_paths.difference(deferred_paths)
                | cache_remapped_paths
                | deleted_scope_keys
            )
        elif scoped:
            completed_scope = False
        self.last_run_stats = {
            "scope_status": "scoped" if scoped else "full",
            "scope_keys": len(scope_keys),
            "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
            "files_seen": files_seen,
            "changed_file_count": len(changed_paths),
            "deleted_file_count": len(deleted_scope_keys),
            "files_opened": len(paths_to_open_for_lsp),
            "files_skipped_unchanged": max(0, files_seen - len(changed_paths)),
            "dependency_fanout": len(fanout_paths),
            "reverse_dependency_fanout": len(reverse_fanout_paths),
            "lsp_requests": (
                report.references_inspected if report is not None else 0
            ),
            "references_reused": references_reused,
            "cache_hit_files": len(cache_hit_paths),
            "cache_miss_files": len(cache_miss_paths),
            "cache_remapped_files": len(cache_remapped_paths),
            "lsp_miss_files": len(paths_with_lsp_references),
            "no_reference_miss_files": len(no_reference_miss_paths),
            "deferred_files": len(deferred_paths),
            "deferred_lsp_requests": deferred_lsp_requests,
            "hot_path_max_lsp_requests": self.hot_path_max_lsp_requests,
            "definition_request_concurrency": (
                report.definition_request_concurrency
                if report is not None
                else self.definition_request_concurrency
            ),
            "references_inspected": (
                report.references_inspected if report is not None else 0
            ),
            "references_resolved": len(fresh_resolved),
            "references_candidate_skipped": (
                report.references_candidate_skipped if report is not None else 0
            ),
            "references_skipped_by_reason": (
                dict(report.references_skipped_by_reason)
                if report is not None
                else {}
            ),
            "unresolved_by_reason": (
                dict(report.unresolved_by_reason)
                if report is not None
                else {}
            ),
            "server_reused": (
                bool(report.server_reused) if report is not None else False
            ),
            "enabled_methods": (
                list(report.enabled_methods) if report is not None else []
            ),
            "disabled_methods": (
                list(report.disabled_methods) if report is not None else []
            ),
            "server_capabilities": (
                dict(report.server_capabilities) if report is not None else {}
            ),
            "workspace_fingerprint": workspace_fingerprint,
            "source_frontier": source_frontier,
            "record_set_authority": (
                "replacement_slice"
                if record_set_replacement_keys is not None
                else "full"
                if record_set is not None
                else "not_available"
            ),
            "status": "deferred" if deferred_paths else "resolved",
        }
        next_cursor = {
            **walk.next_cursor(
                lsp_resolution_schema=_LSP_RESOLUTION_SCHEMA,
                workspace_fingerprint=workspace_fingerprint,
                lsp_resolution_cache=self._lsp_cache_cursor(
                    next_cache_files,
                    next_files=next_files,
                    deleted_paths=(),
                ),
            ),
            "files": next_files,
        }
        if deferred_paths:
            next_cursor["lsp_deferred"] = {
                "schema": "okg.code_lsp_resolution.deferred.v1",
                "reason": "hot_path_lsp_request_budget_exceeded",
                "paths": sorted(deferred_paths),
                "estimated_lsp_requests": deferred_lsp_requests,
                "hot_path_max_lsp_requests": self.hot_path_max_lsp_requests,
                "reconcile_mode": "reconcile",
            }
        if record_set is not None and record_set_replacement_keys is None:
            next_cursor["record_set"] = record_set.to_cursor()
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
        )

    def _dependency_fanout(self, changed_content: dict[str, str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for changed_path, text in changed_content.items():
            for rel_path in _python_import_fanout(
                text,
                rel_path=changed_path,
                repo_root=self.repo_root,
            ):
                if rel_path in seen:
                    continue
                seen.add(rel_path)
                out.append(rel_path)
                if len(out) >= self.dependency_fanout_limit:
                    return sorted(out)
        return sorted(out)

    def _reverse_dependency_fanout(self, changed_paths: Sequence[str]) -> list[str]:
        changed_modules = {
            _module_qname(path)
            for path in changed_paths
            if path.endswith(".py")
        }
        if not changed_modules:
            return []
        out: list[str] = []
        seen: set[str] = set(changed_paths)
        for path in _walk_files(
            Path(self.repo_root),
            include_exts=self.include_exts,
            exclude_globs=self.exclude_globs,
            max_files=self.max_files,
            max_bytes_per_file=self.max_bytes_per_file,
        ):
            rel_path = path.relative_to(self.repo_root).as_posix()
            if rel_path in seen:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            imports = _python_import_modules(text, rel_path=rel_path)
            if not changed_modules.intersection(imports):
                continue
            seen.add(rel_path)
            out.append(rel_path)
            if len(out) >= self.reverse_dependency_fanout_limit:
                return sorted(out)
        return sorted(out)

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
        out: list[Path] = []
        for key in sorted(self._scope_record_keys(scope_keys)):
            candidate = Path(self.repo_root) / key
            if candidate.is_file():
                out.append(candidate)
        return out

    @staticmethod
    def _record_set_for_facts(facts: Sequence[EdgeFact]) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for fact in facts:
            path = str(fact.source_record_id.get("path") or "")
            if not path:
                continue
            emission = records.setdefault(path, RecordEmission())
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

    def _prior_lsp_cache_files(self, cursor: Any) -> dict[str, Any]:
        if not isinstance(cursor, dict):
            return {}
        raw = cursor.get("lsp_resolution_cache")
        if not isinstance(raw, dict):
            return {}
        if raw.get("schema") != _LSP_CACHE_SCHEMA:
            return {}
        if raw.get("resolver") != self.server_name:
            return {}
        if raw.get("resolver_version") != self.server_version:
            return {}
        if raw.get("language_id") != self.language_id:
            return {}
        files = raw.get("files")
        return dict(files) if isinstance(files, dict) else {}

    def _lsp_cache_cursor(
        self,
        cache_files: dict[str, Any],
        *,
        next_files: dict[str, Any],
        deleted_paths: Iterable[str],
    ) -> dict[str, Any]:
        return {
            "schema": _LSP_CACHE_SCHEMA,
            "resolver": self.server_name,
            "resolver_version": self.server_version,
            "language_id": self.language_id,
            "lsp_spec_version": LSP_SPEC_VERSION,
            "files": self._next_lsp_cache_files(
                cache_files,
                next_files=next_files,
                deleted_paths=set(deleted_paths),
            ),
        }

    @staticmethod
    def _next_lsp_cache_files(
        cache_files: dict[str, Any],
        *,
        next_files: dict[str, Any],
        deleted_paths: Iterable[str],
    ) -> dict[str, Any]:
        deleted = set(deleted_paths)
        return {
            path: value
            for path, value in cache_files.items()
            if path in next_files and path not in deleted
        }

    def _cached_facts_for_path(
        self,
        rel_path: str,
        entry: Any,
        *,
        next_files: dict[str, Any],
        semantic_symbol_ids: dict[str, str],
        changed_paths: set[str],
        workspace_fingerprint: str,
        source_frontier: str,
        definition_filter_fingerprint: str,
    ) -> _CachedLspFacts | None:
        if not isinstance(entry, dict):
            return None
        if entry.get("sha256") != _file_sha(next_files.get(rel_path)):
            return None
        if (
            entry.get("definition_filter_fingerprint")
            != definition_filter_fingerprint
        ):
            return None
        raw_facts = entry.get("facts")
        if not isinstance(raw_facts, list):
            return None
        facts: list[EdgeFact] = []
        remapped_any = False
        for raw in raw_facts:
            fact = _edge_fact_from_cache(raw)
            if fact is None:
                return None
            remapped = self._remap_cached_fact(
                fact,
                semantic_symbol_ids=semantic_symbol_ids,
                changed_paths=changed_paths,
                workspace_fingerprint=workspace_fingerprint,
                source_frontier=source_frontier,
            )
            if remapped is None:
                return None
            remapped_fact, remapped_changed = remapped
            facts.append(remapped_fact)
            remapped_any = remapped_any or remapped_changed
        return _CachedLspFacts(facts=tuple(facts), remapped=remapped_any)

    def _remap_cached_fact(
        self,
        fact: EdgeFact,
        *,
        semantic_symbol_ids: dict[str, str],
        changed_paths: set[str],
        workspace_fingerprint: str,
        source_frontier: str,
    ) -> tuple[EdgeFact, bool] | None:
        attrs = dict(fact.attrs)
        semantic_target_key = str(attrs.get("semantic_target_key") or "")
        dst = fact.dst
        if semantic_target_key:
            remapped_dst = semantic_symbol_ids.get(semantic_target_key)
            if remapped_dst:
                dst = remapped_dst
            elif _symbol_path(dst, self.repo) in changed_paths:
                return None
        if dst == fact.dst:
            return fact, False
        attrs["workspace_fingerprint"] = workspace_fingerprint
        attrs["source_frontier"] = source_frontier
        return (
            EdgeFact(
                src=fact.src,
                dst=dst,
                edge_type=fact.edge_type,
                attrs=attrs,
                provenance=fact.provenance,
                confidence=fact.confidence,
                source_record_id=dict(fact.source_record_id),
                source_revision={
                    **dict(fact.source_revision),
                    "workspace_fingerprint": workspace_fingerprint,
                    "source_frontier": source_frontier,
                },
                op=fact.op,
            ),
            True,
        )

    def _cache_entry(
        self,
        rel_path: str,
        *,
        next_files: dict[str, Any],
        facts: Sequence[EdgeFact],
        references_inspected: int,
        references_resolved: int,
        unresolved_by_reason: dict[str, int],
        definition_filter_fingerprint: str,
    ) -> dict[str, Any]:
        return {
            "sha256": _file_sha(next_files.get(rel_path)),
            "facts": [_edge_fact_to_cache(fact) for fact in facts],
            "references_inspected": int(references_inspected),
            "references_resolved": int(references_resolved),
            "unresolved_by_reason": dict(unresolved_by_reason),
            "definition_filter_fingerprint": definition_filter_fingerprint,
        }

    @staticmethod
    def _cached_references_inspected(entry: Any) -> int:
        if not isinstance(entry, dict):
            return 0
        return int(entry.get("references_inspected") or 0)

    @staticmethod
    def _cached_references_resolved(entry: Any) -> int:
        if not isinstance(entry, dict):
            return 0
        return int(entry.get("references_resolved") or 0)

    @staticmethod
    def _pending_deferred_paths(cursor: Any) -> set[str]:
        if not isinstance(cursor, dict):
            return set()
        raw = cursor.get("lsp_deferred")
        if not isinstance(raw, dict):
            return set()
        if raw.get("schema") != "okg.code_lsp_resolution.deferred.v1":
            return set()
        return {
            str(path).replace("\\", "/").lstrip("/")
            for path in raw.get("paths") or []
            if path
        }

    def _reference_counts_by_path(
        self,
        paths: Sequence[str],
        *,
        changed_content: dict[str, str],
        workspace_paths: Sequence[str],
    ) -> dict[str, int]:
        extractor = code_extractor_for(self.language_id)
        out: dict[str, int] = {}
        for rel_path in paths:
            text = changed_content.get(rel_path)
            if text is None:
                try:
                    text = (Path(self.repo_root) / rel_path).read_text(
                        encoding="utf-8",
                        errors="replace",
                    )
                except OSError:
                    continue
            report = extractor.extract(text, path=rel_path)
            out[rel_path] = lsp_definition_candidate_count(
                report,
                rel_path=rel_path,
                workspace_paths=workspace_paths,
                language_id=self.language_id,
            )
        return out

    def _apply_lsp_hot_path_budget(
        self,
        paths: Sequence[str],
        *,
        reference_counts: dict[str, int],
        mode: str,
    ) -> tuple[list[str], set[str], int]:
        budget = int(self.hot_path_max_lsp_requests or 0)
        if budget <= 0 or mode == "reconcile":
            return list(paths), set(), 0
        selected: list[str] = []
        deferred: set[str] = set()
        used = 0
        deferred_requests = 0
        for rel_path in paths:
            count = int(reference_counts.get(rel_path) or 0)
            if count <= 0:
                selected.append(rel_path)
                continue
            if used + count <= budget:
                selected.append(rel_path)
                used += count
                continue
            deferred.add(rel_path)
            deferred_requests += count
        return selected, deferred, deferred_requests

    def _semantic_symbol_ids(self, paths: Sequence[str]) -> dict[str, str]:
        extractor = code_extractor_for(self.language_id)
        out: dict[str, str] = {}
        for rel_path in paths:
            path = Path(self.repo_root) / rel_path
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            report = extractor.extract(text, path=rel_path)
            module_name = _module_qname(rel_path)
            symbols = [
                Symbol(
                    kind="module",
                    name=Path(rel_path).stem,
                    qualified_name=module_name,
                    span_start=1,
                    span_end=max(1, len(text.splitlines())),
                ),
                *report.symbols,
            ]
            for sym in symbols:
                key = semantic_key_for_symbol(
                    repo=self.repo,
                    language=report.language,
                    module_name=getattr(sym, "module_name", "") or module_name,
                    qualified_name=sym.qualified_name,
                    kind=getattr(sym, "symbol_kind", "") or sym.kind,
                    package_name=getattr(sym, "package_name", "") or "",
                )
                out[key] = _symbol_id(
                    self.repo,
                    rel_path,
                    sym.qualified_name,
                    sym.span_start,
                )
        return out


def _python_import_fanout(
    text: str,
    *,
    rel_path: str,
    repo_root: Path,
) -> Iterable[str]:
    modules = _python_import_modules(text, rel_path=rel_path)
    out: list[str] = []
    for module in sorted(modules):
        rel = module.replace(".", "/")
        for candidate in (
            repo_root / f"{rel}.py",
            repo_root / rel / "__init__.py",
        ):
            if not candidate.is_file():
                continue
            out.append(candidate.relative_to(repo_root).as_posix())
            break
    return out


def _file_sha(entry: Any) -> str:
    if isinstance(entry, dict):
        return str(entry.get("sha256") or "")
    if isinstance(entry, str):
        return entry
    return ""


def _edge_fact_to_cache(fact: EdgeFact) -> dict[str, Any]:
    return {
        "src": fact.src,
        "dst": fact.dst,
        "edge_type": fact.edge_type,
        "attrs": dict(fact.attrs),
        "provenance": fact.provenance,
        "confidence": fact.confidence,
        "source_record_id": dict(fact.source_record_id),
        "source_revision": dict(fact.source_revision),
        "op": fact.op,
    }


def _edge_fact_from_cache(raw: Any) -> EdgeFact | None:
    if not isinstance(raw, dict):
        return None
    src = raw.get("src")
    dst = raw.get("dst")
    edge_type = raw.get("edge_type")
    if not isinstance(src, str) or not src:
        return None
    if not isinstance(dst, str) or not dst:
        return None
    if not isinstance(edge_type, str) or not edge_type:
        return None
    attrs = raw.get("attrs") or {}
    source_record_id = raw.get("source_record_id") or {}
    source_revision = raw.get("source_revision") or {}
    if not isinstance(attrs, dict):
        return None
    if not isinstance(source_record_id, dict):
        return None
    if not isinstance(source_revision, dict):
        return None
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        attrs=dict(attrs),
        provenance=str(raw.get("provenance") or "explicit"),
        confidence=float(raw.get("confidence", 1.0)),
        source_record_id=dict(source_record_id),
        source_revision=dict(source_revision),
        op=str(raw.get("op") or "I"),
    )


def _symbol_path(symbol_id: str, repo: str) -> str:
    prefix = f"symbol:{repo}:"
    if not symbol_id.startswith(prefix):
        return ""
    rest = symbol_id[len(prefix):]
    parts = rest.split(":")
    return parts[0] if parts else ""


def _python_import_modules(text: str, *, rel_path: str) -> set[str]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            module = _resolve_import_from_module(
                node.module or "",
                level=int(node.level or 0),
                rel_path=rel_path,
            )
            if module:
                modules.add(module)
    return modules


def _resolve_import_from_module(module: str, *, level: int, rel_path: str) -> str:
    if level <= 0:
        return module
    path = Path(rel_path)
    package_parts = list(path.with_suffix("").parts[:-1])
    if path.name == "__init__.py":
        package_parts = list(path.parent.parts)
    keep = max(0, len(package_parts) - level + 1)
    parts = package_parts[:keep]
    if module:
        parts.extend(part for part in module.split(".") if part)
    return ".".join(parts)


def _merged_scoped_files(
    prior_files: dict[str, Any],
    next_files: dict[str, Any],
    deleted_scope_keys: set[str],
) -> dict[str, Any]:
    merged = {
        key: value
        for key, value in prior_files.items()
        if key not in deleted_scope_keys
    }
    merged.update(next_files)
    return merged
