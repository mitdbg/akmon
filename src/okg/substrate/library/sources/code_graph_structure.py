"""Code-symbol extraction source.

Extracted from ``library/sources/code_graph.py`` by Wave D-30
(2026-05) per ``extract-code-graph-structure-and-manifest``.
Provides `CodeStructureSource`, the substrate's primary source
for code-symbol facts (module / class / function symbols,
imports, references). The 7 helpers in this module
(_parent_symbol_id, _import_attrs, _reference_attrs,
_resolve_reference_target, _normalized_reference_qname,
_reference_leaf_name, _reference_kind) are 100% specific to
that projection.

``library/sources/code_graph.py`` re-exports `CodeStructureSource`
at module-import time so YAML manifests with
`module: okg.substrate.library.sources.code_graph;
class: CodeStructureSource` keep resolving.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Sequence

from okg.substrate.errors import SourceRootMissing
from okg.substrate.library.extraction.code._protocols import (
    CodeSymbolReport,
    Import,
    Reference,
    Symbol,
)
from okg.substrate.library.extraction.code.lsp_protocol import (
    SEMANTIC_KEY_VERSION,
    canonical_symbol_name,
    semantic_key_for_symbol,
)
from okg.substrate.library.extraction.code.dispatch import code_extractor_for
from okg.substrate.library.extraction.code.python_ast import CodeParseFailed
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
    acquire_outcome_health,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
    source_state_without_runner_keys,
)

from ._code_graph_common import (
    DEFAULT_CODE_EXTS,
    DEFAULT_EXCLUDES,
    _archive_symbol_prefix,
    _carry_cursor,
    _dedupe_facts,
    _ensure_tree_sitter_registered,
    _file_id,
    _glob_match,
    _language_for_path,
    _module_qname,
    _read_text,
    _scoped_symbol_name,
    _sha,
    _symbol_id,
    _walk_files,
    _scope_keys_outside_walk,
    _scope_record_keys_in_walk,
)


__all__ = ["CodeStructureSource"]

_ARCHIVE_NAMES_VERSION = 1
_ARCHIVE_NAMES_CURSOR_KEY = "archive_qualified_names_version"
# Issue #1747: every code symbol now carries `scoped_qualified_name`, its
# alias key. A cursor written before that attribute existed must re-emit
# EVERY file once, or unchanged files would keep facts without the key. Only
# a full run may certify the version; a scoped run refreshes what it walks.
_SCOPED_NAMES_VERSION = 1
_SCOPED_NAMES_CURSOR_KEY = "scoped_qualified_names_version"


@dataclass(frozen=True)
class _CapturedCodeFile:
    path: Path
    rel_path: str
    text: str
    text_hash: str


@dataclass(frozen=True)
class _CodeStructureInputSnapshot:
    files: tuple[_CapturedCodeFile, ...]
    token: dict[str, Any]
    head_sha: str
    dirty_paths: frozenset[str]


class _CapturedCodeWalk:
    """FileWalk-compatible view over one immutable captured input."""

    def __init__(
        self,
        *,
        snapshot: _CodeStructureInputSnapshot,
        prior_cursor: Optional[dict[str, Any]],
    ) -> None:
        prior_files = (prior_cursor or {}).get("files")
        self._prior_files = (
            dict(prior_files) if isinstance(prior_files, dict) else {}
        )
        self._snapshot = snapshot
        self._next_files: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _prior_sha(entry: Any) -> Optional[str]:
        if isinstance(entry, dict):
            value = entry.get("sha256")
            return value if isinstance(value, str) else None
        return entry if isinstance(entry, str) else None

    def iter(self, _walker_paths: Iterable[Path]) -> Iterator[Any]:
        from okg.substrate.library.sources._file_walk import FileWalkEvent

        for item in self._snapshot.files:
            prior_sha = self._prior_sha(self._prior_files.get(item.rel_path))
            # mtime_ns=0 deliberately disables FileWalk's future mtime fast
            # path if this cursor is consumed by a scoped fallback run.
            self._next_files[item.rel_path] = {
                "sha256": item.text_hash,
                "mtime_ns": 0,
            }
            yield FileWalkEvent(
                rel_path=item.rel_path,
                content=item.text,
                content_hash=item.text_hash,
                is_changed=prior_sha != item.text_hash,
            )

    def next_cursor(self, **extra_fields: Any) -> dict[str, Any]:
        out: dict[str, Any] = {"files": dict(self._next_files)}
        out.update(extra_fields)
        return out

    @property
    def prior_files(self) -> dict[str, Any]:
        return dict(self._prior_files)

    @property
    def next_files(self) -> dict[str, dict[str, Any]]:
        return dict(self._next_files)


def _code_extractor_emit_targets() -> tuple[tuple[str, type[Any]], ...]:
    """Resolve every registered language to its concrete extractor class."""
    from okg.substrate.library.extraction.code.dispatch import available_languages

    targets: list[tuple[str, type[Any]]] = []
    for language in available_languages():
        extractor = code_extractor_for(language)
        targets.append((language, type(extractor)))
    return tuple(targets)


class _CodeStructureSnapshotProbe:
    """Factory namespace for the GitScopeProbe snapshot subclass."""

    @staticmethod
    def build(
        *,
        repo_root: Path,
        config: dict[str, Any],
        emit_targets: Sequence[Any],
        scope_filter: Any,
        include_exts: Sequence[str],
        exclude_globs: Sequence[str],
        max_files: int,
        max_bytes_per_file: int,
    ) -> Any:
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

        class SnapshotProbe(GitScopeProbe):
            def build_token(probe_self) -> dict[str, Any]:
                """Return the token from one validated immutable snapshot."""
                return dict(probe_self.capture_snapshot().token)

            def _source_paths(probe_self) -> tuple[Path, ...]:
                return tuple(_walk_files(
                    Path(probe_self.repo_root),
                    include_exts=include_exts,
                    exclude_globs=exclude_globs,
                    max_files=max_files,
                    max_bytes_per_file=max_bytes_per_file,
                ))

            def _dirty_manifest(probe_self) -> dict[str, dict[str, str]]:
                from okg.substrate.library.sources._git_helpers import (
                    in_proc_list_tracked,
                )

                manifest = super(SnapshotProbe, probe_self)._dirty_manifest()
                tracked = in_proc_list_tracked(Path(probe_self.repo_root))
                root = Path(probe_self.repo_root).resolve()
                for path in probe_self._source_paths():
                    rel_path = path.relative_to(root).as_posix()
                    # The general GitScopeProbe omits ignored files, while the
                    # code walker intentionally does not. Bind those inputs,
                    # plus followed symlink contents, into this source token.
                    if (
                        rel_path not in manifest
                        and (rel_path not in tracked or path.is_symlink())
                    ):
                        try:
                            raw = path.read_bytes()
                        except OSError:
                            content_sha = "absent"
                        else:
                            content_sha = hashlib.sha256(raw).hexdigest()
                        manifest[rel_path] = {
                            "kind": (
                                "tracked"
                                if rel_path in tracked
                                else "untracked"
                            ),
                            "sha256": content_sha,
                        }
                return manifest

            def _dirty_paths(probe_self) -> set[str]:
                return set(probe_self._dirty_manifest())

            def capture_snapshot(
                probe_self,
            ) -> _CodeStructureInputSnapshot:
                from okg.substrate.library.sources._git_helpers import (
                    in_proc_head_blob_bytes,
                    in_proc_head_snapshot,
                )
                from okg.substrate.library.sources.change_probe import (
                    ChangeProbeError,
                    tokens_equal,
                )

                root = Path(probe_self.repo_root).resolve()
                head = in_proc_head_snapshot(root)
                source_paths = probe_self._source_paths()
                dirty_manifest = probe_self._dirty_manifest()
                dirty_paths = frozenset(dirty_manifest)

                raw_by_path: dict[str, bytes] = {}
                captured: list[_CapturedCodeFile] = []
                for path in source_paths:
                    rel_path = path.relative_to(root).as_posix()
                    try:
                        raw = path.read_bytes()
                    except OSError as exc:
                        raise ChangeProbeError(
                            f"code_structure snapshot could not read {rel_path}"
                        ) from exc
                    raw_by_path[rel_path] = raw
                    text = raw.decode("utf-8", errors="replace")
                    captured.append(_CapturedCodeFile(
                        path=path,
                        rel_path=rel_path,
                        text=text,
                        text_hash=hashlib.sha256(
                            text.encode("utf-8"),
                        ).hexdigest(),
                    ))

                # A path Git reports as clean must equal the blob from the
                # captured tree. This closes the status/read race for clean
                # files without turning the poll probe into a full blob read.
                clean_regular = [
                    item.rel_path
                    for item in captured
                    if (
                        item.rel_path not in dirty_paths
                        and not item.path.is_symlink()
                    )
                ]
                head_blobs = in_proc_head_blob_bytes(
                    root,
                    clean_regular,
                    tree_sha=head.tree_sha,
                )
                for rel_path in clean_regular:
                    if head_blobs.get(rel_path) != raw_by_path[rel_path]:
                        raise ChangeProbeError(
                            "code_structure clean-file snapshot diverged "
                            f"from HEAD: {rel_path}"
                        )

                captured_manifest: dict[str, dict[str, str]] = {}
                for rel_path in sorted(dirty_paths):
                    raw = raw_by_path.get(rel_path)
                    if raw is None:
                        try:
                            raw = (root / rel_path).read_bytes()
                        except OSError:
                            content_sha = "absent"
                        else:
                            content_sha = hashlib.sha256(raw).hexdigest()
                    else:
                        content_sha = hashlib.sha256(raw).hexdigest()
                    captured_manifest[rel_path] = {
                        "kind": dirty_manifest[rel_path]["kind"],
                        "sha256": content_sha,
                    }

                token = probe_self.token_for_snapshot(
                    tree_sha=head.tree_sha,
                    head_sha=head.head_sha,
                    branch=head.branch,
                    dirty_manifest=captured_manifest,
                )

                # A durable snapshot needs stable membership and a token built
                # from exactly the bytes above. Concurrent mutation is retried
                # by the caller/runner as a changed input, never acknowledged.
                post_paths = tuple(
                    path.relative_to(root).as_posix()
                    for path in probe_self._source_paths()
                )
                captured_paths = tuple(item.rel_path for item in captured)
                if post_paths != captured_paths:
                    raise ChangeProbeError(
                        "code_structure input membership changed during snapshot"
                    )
                # Call the generic implementation explicitly: build_token()
                # dispatches back to this subclass's capture_snapshot(), which
                # would recurse. The base capture still invokes our overridden
                # dirty-manifest method, so ignored files and followed symlink
                # content remain part of the fresh final-state fence.
                generic_post_snapshot = GitScopeProbe.capture_snapshot(
                    probe_self
                )
                generic_post_token = probe_self.token_for_snapshot(
                    **generic_post_snapshot
                )
                if not tokens_equal(token, generic_post_token):
                    raise ChangeProbeError(
                        "code_structure input changed during snapshot"
                    )

                return _CodeStructureInputSnapshot(
                    files=tuple(captured),
                    token=token,
                    head_sha=head.head_sha,
                    dirty_paths=dirty_paths,
                )

        return SnapshotProbe(
            repo_root=repo_root,
            config=config,
            emit_targets=emit_targets,
            scope_filter=scope_filter,
        )




@dataclass
class CodeStructureSource:
    repo: str
    repo_root: Path | str
    include_exts: Sequence[str] = DEFAULT_CODE_EXTS
    exclude_globs: Sequence[str] = DEFAULT_EXCLUDES
    max_files: int = 5000
    max_bytes_per_file: int = 1_000_000
    git_frontier_skip: bool = False
    # Checkout acquisition (code-repos-auto-acquire): defaults off — in a
    # code_repos expansion the git-files source owns the checkout, so this
    # source inherits an already-materialised repo_root. Settable so this source
    # can own a checkout when configured standalone.
    url: Optional[str] = None
    tracked_branch: Optional[str] = None
    manage_checkout: bool = False

    name: str = "code_structure"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): churn-immune git scope probe at
    # the runner gate (supersedes the inline git_frontier_skip short-circuit).
    change_probe_kind = "git_scope"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.repo_root = Path(self.repo_root).expanduser().resolve()
        self._acquire_health: Optional[SourceHealth] = None
        self.change_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Fail closed when the reader is used outside a Git work-tree."""
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        try:
            return in_proc_workdir(self.repo_root) == self.repo_root
        except Exception:  # noqa: BLE001 - unavailable Git authority fails closed
            return False

    def ensure_checkout(self) -> Optional[SourceHealth]:
        """Materialise repo_root before the change-probe reads HEAD (owner-only).

        A no-op unless manage_checkout is set with a url. Never raises; maps a
        non-fast-forward to acquire_stale and a failed clone to acquire_failed.
        """
        if not self.manage_checkout or not self.url:
            return None
        from okg.substrate.library.sources._git_helpers import clone_or_pull

        outcome = clone_or_pull(
            Path(self.repo_root),
            self.url,
            tracked_branch=self.tracked_branch,
        )
        self._acquire_health = acquire_outcome_health(outcome)
        return self._acquire_health

    def _citation_fields(self, rel_path: str) -> dict[str, str]:
        """Source-supplied citation attrs for a repo-relative path.

        Always includes ``citation_label`` (``<repo>/<path>``);
        ``citation_url`` only when it truthfully links the ingested
        revision — configured remote on a supported host, known HEAD
        sha, file clean vs HEAD (source-supplied-search-citations).
        HEAD sha and dirty paths resolve from the current run snapshot when
        one exists, otherwise once for that run's live fallback.
        """
        from okg.substrate.library.sources._source_urls import (
            citation_label,
            dirty_paths,
            head_commit_sha,
            repo_file_url,
        )

        label = citation_label(self.repo, rel_path)
        if not label:
            return {}
        fields = {"citation_label": label}
        if not self.url:
            return fields
        if not hasattr(self, "_citation_head_sha"):
            self._citation_head_sha = head_commit_sha(self.repo_root)
            self._citation_dirty = dirty_paths(self.repo_root)
        if (
            self._citation_head_sha
            and self._citation_dirty is not None
            and rel_path not in self._citation_dirty
        ):
            url = repo_file_url(
                self.url, self._citation_head_sha, rel_path,
            )
            if url:
                fields["citation_url"] = url
        return fields

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources.change_probe import emit_version_for
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        include_exts = {str(ext).lower() for ext in self.include_exts}
        extractor_targets = _code_extractor_emit_targets()
        extractor_manifest = {
            language: emit_version_for(target)
            for language, target in extractor_targets
        }

        def _in_scope(rel_path: str) -> bool:
            return Path(rel_path).suffix.lower() in include_exts

        return _CodeStructureSnapshotProbe.build(
            repo_root=self.repo_root,
            config={
                "repo": self.repo,
                "include_exts": sorted(self.include_exts),
                "exclude_globs": sorted(self.exclude_globs),
                "max_files": self.max_files,
                "max_bytes_per_file": self.max_bytes_per_file,
                # Emission depends on the citation url config
                # (citation-source-urls) — a url change must re-emit.
                "url": self.url or "",
                # The wrapper dispatches into independently registered
                # extractor implementations.  Include the complete live
                # registry so an implementation or language binding change
                # invalidates the token without hand-maintained version bumps.
                "code_extractor_manifest": extractor_manifest,
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
                *(target for _, target in extractor_targets),
            ),
            scope_filter=_in_scope,
            include_exts=self.include_exts,
            exclude_globs=self.exclude_globs,
            max_files=self.max_files,
            max_bytes_per_file=self.max_bytes_per_file,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        # Degrade, never crash: a MANAGED checkout that failed to materialise has
        # nothing to index — emit zero facts with the acquisition health. Only
        # guards the managed path; an unmanaged code-structure source keeps
        # indexing a plain (possibly non-git) directory as before.
        if self.manage_checkout and not (Path(self.repo_root) / ".git").exists():
            return SourceRun(
                facts=iter(()),
                completed_scope=False,
                next_cursor=dict(cursor or {}),
                run_mode=mode,
                health=self._acquire_health
                or SourceHealth(
                    status="acquire_failed", reason="checkout unavailable"
                ),
            )
        # An unmanaged root that is missing would walk as an empty tree: a
        # complete-scope run would then retract every code fact and report
        # success. Refuse instead.
        if not Path(self.repo_root).is_dir():
            raise SourceRootMissing(
                f"code_structure source {self.name!r}: repo_root "
                f"{str(self.repo_root)!r} does not exist or is not a directory",
                data={
                    "source": self.name,
                    "setting": "repo_root",
                    "path": str(self.repo_root),
                },
                fix_hint=SourceRootMissing.FIX_HINT,
            )
        # Unscoped git runs capture immutable bytes and drive extraction from
        # that snapshot, so the applied token and emitted facts have one input
        # identity. Scoped event runs and non-git direct callers retain the
        # shared FileWalk delta path; they do not claim global token authority.
        #
        # Per `shrink-source-walk-cost`: when the watcher provides
        # `sync_scope.scope_keys` (fs-event-triggered runs), narrow
        # the walk to JUST those paths. The walker still applies
        # include_exts / exclude_globs, but the rglob is bounded
        # by the watcher's hint. For poll-triggered or full-
        # reconcile runs (scope_keys is empty), fall through to
        # the full repo walk.
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
        cursor = _carry_cursor(
            cursor,
            scoped=scoped,
            authoritative_replay=authoritative_replay,
        )
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        has_inline_record_set = (
            isinstance(cursor, dict) and cursor.get("record_set") is not None
        )
        archive_names_current = (
            isinstance(cursor, dict)
            and cursor.get(_ARCHIVE_NAMES_CURSOR_KEY) == _ARCHIVE_NAMES_VERSION
        )
        scoped_names_current = (
            isinstance(cursor, dict)
            and cursor.get(_SCOPED_NAMES_CURSOR_KEY) == _SCOPED_NAMES_VERSION
        )
        input_snapshot: _CodeStructureInputSnapshot | None = None
        for attr_name in ("_citation_head_sha", "_citation_dirty"):
            if hasattr(self, attr_name):
                delattr(self, attr_name)
        if not scoped:
            from okg.substrate.library.sources._git_helpers import in_proc_workdir

            repo_root = Path(self.repo_root).resolve()
            if in_proc_workdir(repo_root) == repo_root:
                input_snapshot = self.change_probe.capture_snapshot()
                # Citation fields are fact-determining input too. Bind them to
                # the same HEAD/dirty snapshot used by extraction.
                self._citation_head_sha = input_snapshot.head_sha
                self._citation_dirty = input_snapshot.dirty_paths

        current_git_frontier: dict[str, Any] | None = None
        if (
            self.git_frontier_skip
            and completed_scope
            and not scoped
        ):
            current_git_frontier = self._git_frontier()
            prior_git_frontier = (
                cursor.get("git_frontier")
                if isinstance(cursor, dict) else None
            )
            if (
                has_record_set_authority
                and archive_names_current
                and scoped_names_current
                and
                current_git_frontier is not None
                and current_git_frontier == prior_git_frontier
                and input_snapshot is not None
                and self._snapshot_matches_cursor(input_snapshot, cursor)
            ):
                next_cursor = source_state_without_runner_keys(cursor)
                next_cursor["git_frontier"] = current_git_frontier
                file_count = len(next_cursor.get("files") or {})
                self.last_run_stats = {
                    "scope_status": "git_frontier_no_change",
                    "scope_keys": 0,
                    "files_seen": 0,
                    "files_scanned": 0,
                    "files_skipped_unchanged": file_count,
                    "changed_scope_keys": 0,
                    "emitted_nodes": 0,
                    "emitted_edges": 0,
                    "git_frontier_dirty_paths": len(
                        current_git_frontier.get("dirty_paths") or (),
                    ),
                }
                return SourceRun(
                    facts=iter(()),
                    completed_scope=True,
                    next_cursor=preserve_cursor_record_set_authority(
                        next_cursor,
                        cursor,
                    ),
                    applied_raw_token=input_snapshot.token,
                    applied_token_scope="global",
                    applied_token_authority="exact",
                    run_mode=mode,
                    record_set=None,
                    # The cursor above carries the prior footprint forward
                    # (this branch requires has_record_set_authority), so the
                    # runner must not infer one from the empty stream and
                    # retract the whole index (issue #2122).
                    preserves_prior_record_set=True,
                )
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

        # Per `cleanup-personal-okg-data-quality` C1 item 1, complete
        # runs may only short-circuit unchanged files when the adapter
        # can still declare a full record_set footprint. Otherwise an
        # empty delta emission would look like "everything retracted".
        # We therefore honor the cursor for incremental modes, scoped
        # complete runs, and unscoped complete runs whose prior cursor
        # has the record_set needed to carry unchanged records forward.
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
            refresh_archive_name = (
                not archive_names_current
                and bool(_archive_symbol_prefix(self.repo, event.rel_path))
            )
            if (
                not event.is_changed
                and not refresh_archive_name
                and scoped_names_current
            ):
                continue
            files_scanned += 1
            if scoped or can_carry_completed_scope:
                changed_scope_keys.add(event.rel_path)
            full_path = Path(self.repo_root) / event.rel_path
            facts.extend(
                self._facts_for_file(
                    full_path,
                    event.rel_path,
                    event.content if event.content is not None else _read_text(full_path),
                    event.content_hash,
                )
            )
        self.last_run_stats = {
            "scope_status": "scoped" if scoped else "full",
            "scope_keys": len(scope_keys),
            "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
            "files_seen": files_seen,
            "files_scanned": files_scanned,
            "files_skipped_unchanged": max(0, files_seen - files_scanned),
            "changed_scope_keys": len(changed_scope_keys),
            "emitted_nodes": sum(
                1 for fact in facts if isinstance(fact, NodeFact)
            ),
            "emitted_edges": sum(
                1 for fact in facts if isinstance(fact, EdgeFact)
            ),
        }
        if (
            completed_scope
            and not scoped
            and has_record_set_authority
            and not can_carry_completed_scope
            and facts
        ):
            return self.run(run_id, mode=mode)

        deleted_scope_keys_for_carry = (
            set(walk.prior_files).difference(walked_scope_keys)
            if can_carry_completed_scope else set()
        )
        can_build_complete_record_set = (
            completed_scope
            and (
                scoped
                or
                not has_record_set_authority
                or bool(facts)
                or bool(changed_scope_keys)
                or bool(deleted_scope_keys_for_carry)
            )
        )
        record_set = (
            self._record_set_for_facts(facts)
            if can_build_complete_record_set else None
        )
        record_set_replacement_keys: set[str] | None = None
        if (
            can_carry_completed_scope
            and has_inline_record_set
            and record_set is None
        ):
            record_set = self._record_set_with_prior(
                cursor,
                current=RecordSet(),
                replacement_keys=set(),
            )
        if can_carry_completed_scope and record_set is not None:
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
        # For scoped (narrow) runs we MUST merge the prior cursor's
        # `files` map with the new entries — paths outside the
        # narrow set retain their prior sha+mtime so the next
        # full reconcile / poll has accurate baseline.
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
                record_set_replacement_keys = (
                    changed_scope_keys | deleted_scope_keys
                )
                record_set = self._record_set_with_prior(
                    cursor,
                    current=record_set,
                    replacement_keys=record_set_replacement_keys,
                ) if has_inline_record_set else record_set
                if has_inline_record_set:
                    record_set_replacement_keys = None
                    completed_scope = record_set is not None
                elif has_record_set_authority:
                    completed_scope = True
                else:
                    record_set = None
                    record_set_replacement_keys = None
                    completed_scope = False
            return SourceRun(
                facts=iter(facts),
                completed_scope=completed_scope,
                # A partial run cannot certify archives outside its scope.
                next_cursor={
                    "files": merged,
                    **({_ARCHIVE_NAMES_CURSOR_KEY: _ARCHIVE_NAMES_VERSION}
                       if archive_names_current else {}),
                    **({_SCOPED_NAMES_CURSOR_KEY: _SCOPED_NAMES_VERSION}
                       if scoped_names_current else {}),
                },
                applied_raw_token=None,
                applied_token_scope="scoped",
                applied_token_authority="unavailable",
                run_mode=mode,
                record_set=record_set,
                record_set_replacement_keys=record_set_replacement_keys,
            )

        next_cursor = (
            preserve_cursor_record_set_authority(walk.next_cursor(), cursor)
            if completed_scope and has_record_set_authority and record_set is None
            else walk.next_cursor()
        )
        next_cursor[_ARCHIVE_NAMES_CURSOR_KEY] = _ARCHIVE_NAMES_VERSION
        next_cursor[_SCOPED_NAMES_CURSOR_KEY] = _SCOPED_NAMES_VERSION
        if current_git_frontier is not None:
            next_cursor["git_frontier"] = current_git_frontier

        applied_raw_token = (
            input_snapshot.token if input_snapshot is not None else None
        )
        return SourceRun(
            facts=iter(facts),
            completed_scope=completed_scope,
            next_cursor=next_cursor,
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global" if applied_raw_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
        )

    @staticmethod
    def _snapshot_matches_cursor(
        snapshot: _CodeStructureInputSnapshot,
        cursor: Optional[dict[str, Any]],
    ) -> bool:
        prior_files = (cursor or {}).get("files")
        if not isinstance(prior_files, dict):
            return False
        if set(prior_files) != {item.rel_path for item in snapshot.files}:
            return False
        for item in snapshot.files:
            prior_entry = prior_files.get(item.rel_path)
            prior_sha = (
                prior_entry.get("sha256")
                if isinstance(prior_entry, dict)
                else prior_entry
            )
            if prior_sha != item.text_hash:
                return False
        return True

    def _git_frontier(self) -> dict[str, Any] | None:
        """Return a cheap git working-tree frontier for opt-in no-change proof."""
        try:
            from ._git_helpers import in_proc_head_sha, in_proc_status_porcelain

            repo_root = Path(self.repo_root)
            head_sha = in_proc_head_sha(repo_root)
            dirty_paths = sorted(
                path for path in in_proc_status_porcelain(repo_root)
                if self._frontier_path_included(path)
            )
        except Exception:
            return None

        dirty_meta: list[dict[str, Any]] = []
        for rel_path in dirty_paths:
            path = Path(self.repo_root) / rel_path
            try:
                stat_result = path.stat()
            except OSError:
                dirty_meta.append({
                    "path": rel_path,
                    "exists": False,
                    "mtime_ns": None,
                    "size": None,
                })
                continue
            dirty_meta.append({
                "path": rel_path,
                "exists": True,
                "mtime_ns": stat_result.st_mtime_ns,
                "size": stat_result.st_size,
            })
        return {
            "schema": "okg.code_structure.git_frontier.v1",
            "head": head_sha,
            "dirty_paths": dirty_meta,
        }

    def _frontier_path_included(self, rel_path: str) -> bool:
        rel = str(rel_path).replace("\\", "/").lstrip("/")
        if not rel:
            return False
        if Path(rel).suffix.lower() not in {e.lower() for e in self.include_exts}:
            return False
        return not _glob_match(rel, self.exclude_globs)

    def _record_set_with_prior(
        self,
        cursor: Optional[dict[str, Any]],
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> Optional[RecordSet]:
        """Build a complete record_set using a prior full footprint.

        Complete runs can skip unchanged files only if they still
        declare a complete source footprint. Carry the prior full-source
        footprint and replace only records that changed or disappeared
        in this run's governed slice.
        """
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
        """Map a watcher-supplied `scope_keys` list of repo-relative
        paths to Path objects under self.repo_root, applying the
        adapter's include_exts / exclude_globs filters."""
        out: list[Path] = []
        for key in sorted(self._scope_record_keys(scope_keys)):
            candidate = Path(self.repo_root) / key
            if not candidate.is_file():
                continue
            out.append(candidate)
        return out

    def _facts_for_file(
        self, path: Path, rel_path: str, text: str, text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        language = _language_for_path(path)
        _ensure_tree_sitter_registered(language)
        extractor = code_extractor_for(language)
        try:
            report = extractor.extract(text, path=rel_path)
        except CodeParseFailed as exc:
            # One unparseable file (e.g. Python-2-era syntax surviving in a
            # legacy repo) must not abort indexing of the entire codebase.
            # Skip it; the source_file node from git-files still exists.
            # Mirrors code_graph_interface.py's per-file `except SyntaxError`.
            import logging
            logging.getLogger(__name__).warning(
                "code-structure: skipping unparseable file %s: %s",
                rel_path, exc,
            )
            return []
        return self._facts_for_report(rel_path, text, text_hash, report)

    def _facts_for_report(
        self, rel_path: str, text: str, text_hash: str,
        report: CodeSymbolReport,
    ) -> list[NodeFact | EdgeFact]:
        revision = {"text_hash": text_hash}
        out: list[NodeFact | EdgeFact] = []
        file_id = _file_id(self.repo, rel_path)
        # Per dogfood-canary follow-up (2026-05-18): pass the
        # module-level docstring through from the report so
        # `@implements_concept: <slug>` lines at the top of a
        # file (not inside a class/function body) reach the
        # DeclarativeLinker. Falls back to "" for extractors
        # that haven't been taught to surface the field yet.
        module_symbol = Symbol(
            kind="module",
            name=Path(rel_path).stem,
            qualified_name=_module_qname(rel_path),
            span_start=1,
            span_end=max(1, len(text.splitlines())),
            signature="",
            docstring=getattr(report, "module_docstring", "") or "",
        )
        symbols = [module_symbol, *report.symbols]
        symbol_by_qname = {
            sym.qualified_name: sym
            for sym in symbols
        }
        symbol_by_name = {
            sym.name: sym
            for sym in symbols
        }
        symbol_id_by_qname = {
            sym.qualified_name: _symbol_id(
                self.repo, rel_path, sym.qualified_name, sym.span_start,
            )
            for sym in symbols
        }
        module_name_by_qname = {
            sym.qualified_name: (
                getattr(sym, "module_name", "") or _module_qname(rel_path)
            )
            for sym in symbols
        }
        semantic_key_by_qname = {
            sym.qualified_name: _symbol_semantic_key(
                self.repo,
                report.language,
                sym,
                module_name=module_name_by_qname[sym.qualified_name],
            )
            for sym in symbols
        }
        for sym in symbols:
            sid = symbol_id_by_qname[sym.qualified_name]
            parent_symbol_id = _parent_symbol_id(
                sym.qualified_name,
                symbol_id_by_qname,
            )
            module_name = module_name_by_qname[sym.qualified_name]
            symbol_kind = getattr(sym, "symbol_kind", "") or sym.kind
            canonical_name = (
                getattr(sym, "canonical_name", "")
                or canonical_symbol_name(
                    module_name=module_name,
                    qualified_name=sym.qualified_name,
                )
            )
            semantic_key = semantic_key_by_qname[sym.qualified_name]
            container_key = (
                getattr(sym, "container_key", "")
                or _parent_symbol_key(sym.qualified_name, semantic_key_by_qname)
            )
            attrs = {
                "symbol_id": sid,
                "repo": self.repo,
                "path": rel_path,
                "language": report.language,
                "kind": sym.kind,
                "symbol_kind": symbol_kind,
                "name": sym.name,
                "qualified_name": (
                    _archive_symbol_prefix(self.repo, rel_path) + sym.qualified_name
                ),
                # The alias key (issue #1747): unique per repository, file
                # and symbol, built from the RAW symbol name.
                "scoped_qualified_name": _scoped_symbol_name(
                    self.repo, rel_path, sym.qualified_name,
                ),
                "canonical_name": canonical_name,
                "semantic_key": semantic_key,
                "semantic_key_version": (
                    getattr(sym, "semantic_key_version", "")
                    or SEMANTIC_KEY_VERSION
                ),
                "module_name": module_name,
                "package_name": getattr(sym, "package_name", "") or "",
                "container_key": container_key,
                "start_line": sym.span_start,
                "end_line": sym.span_end,
                "start_col": getattr(sym, "start_col", 0) or 0,
                "end_col": getattr(sym, "end_col", 0) or 0,
                "signature": sym.signature,
                "docstring": sym.docstring,
                "parent_symbol_id": parent_symbol_id,
                "lsp_symbol_kind": getattr(sym, "lsp_symbol_kind", None),
                "lsp_symbol_kind_name": (
                    getattr(sym, "lsp_symbol_kind_name", "") or ""
                ),
                "visibility": getattr(sym, "visibility", "") or "",
                "is_exported": getattr(sym, "is_exported", None),
                "is_deprecated": getattr(sym, "is_deprecated", None),
                "definition_uri": (
                    getattr(sym, "definition_uri", "") or rel_path
                ),
                "definition_range": (
                    getattr(sym, "definition_range", "") or ""
                ),
                "selection_range": (
                    getattr(sym, "selection_range", "") or ""
                ),
                "identity_provenance": (
                    getattr(sym, "identity_provenance", "")
                    or "code_structure"
                ),
                "resolver": getattr(sym, "resolver", "") or "code_structure",
                "resolver_version": getattr(sym, "resolver_version", "") or "",
                "legacy_symbol_id": sid,
                "lsp_spec_version": getattr(sym, "lsp_spec_version", "") or "",
                "lsp_method": getattr(sym, "lsp_method", "") or "",
                "server_capability": (
                    getattr(sym, "server_capability", "") or ""
                ),
                "text": (
                    f"{sym.kind} {canonical_name} "
                    f"{sym.signature} {sym.docstring}"
                ).strip(),
            }
            # Source-supplied citation (source-supplied-search-citations):
            # a symbol cites its defining file's record.
            attrs.update(self._citation_fields(rel_path))
            out.append(NodeFact(
                node_id=sid,
                subtype="code_symbol",
                attrs=attrs,
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "symbol_id": sid,
                },
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=file_id,
                dst=sid,
                edge_type="contains",
                attrs={
                    "span_start": sym.span_start,
                    "span_end": sym.span_end,
                },
                source_record_id={
                    "repo": self.repo,
                    "path": rel_path,
                    "symbol_id": sid,
                },
                source_revision=revision,
            ))
            if parent_symbol_id:
                out.append(EdgeFact(
                    src=sid,
                    dst=parent_symbol_id,
                    edge_type="member_of",
                    attrs={},
                    source_record_id={
                        "repo": self.repo,
                        "path": rel_path,
                        "symbol_id": sid,
                    },
                    source_revision=revision,
                ))
        for imp in report.imports:
            imp_attrs = _import_attrs(self.repo, rel_path, report.language, imp)
            iid = f"import:{_sha(json.dumps([self.repo, rel_path, _import_identity_attrs(imp_attrs)], sort_keys=True))}"
            attrs = {
                "import_id": iid,
                **imp_attrs,
                "text": " ".join(str(v) for v in imp_attrs.values() if v),
            }
            out.append(NodeFact(
                node_id=iid,
                subtype="code_import",
                attrs=attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "import_id": iid},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=file_id,
                dst=iid,
                edge_type="contains",
                attrs={
                    "span_start": imp.span_start,
                    "span_end": imp.span_end,
                },
                source_record_id={"repo": self.repo, "path": rel_path, "import_id": iid},
                source_revision=revision,
            ))
        for ref in report.references:
            ref_attrs = _reference_attrs(
                self.repo,
                rel_path,
                report.language,
                ref,
                symbol_id_by_qname,
                semantic_key_by_qname,
            )
            rid = f"reference:{_sha(json.dumps([self.repo, rel_path, _reference_identity_attrs(ref_attrs)], sort_keys=True))}"
            attrs = {
                "reference_id": rid,
                **ref_attrs,
                "text": (
                    f"{ref_attrs.get('reference_kind')} "
                    f"{ref_attrs.get('qualified_name') or ref_attrs.get('name')}"
                ),
            }
            out.append(NodeFact(
                node_id=rid,
                subtype="code_reference",
                attrs=attrs,
                source_record_id={"repo": self.repo, "path": rel_path, "reference_id": rid},
                source_revision=revision,
            ))
            out.append(EdgeFact(
                src=file_id,
                dst=rid,
                edge_type="contains",
                attrs={
                    "span_start": ref.span_start,
                    "span_end": ref.span_end,
                },
                source_record_id={"repo": self.repo, "path": rel_path, "reference_id": rid},
                source_revision=revision,
            ))
            target = _resolve_reference_target(
                ref,
                symbol_by_qname,
                symbol_by_name,
                symbol_id_by_qname,
            )
            caller = symbol_id_by_qname.get(ref.caller_qname)
            reference_kind = str(ref_attrs.get("reference_kind") or "")
            if target:
                out.append(EdgeFact(
                    src=rid,
                    dst=target,
                    edge_type="references",
                    attrs=_reference_edge_attrs(ref_attrs, reference_kind),
                    source_record_id={"repo": self.repo, "path": rel_path, "reference_id": rid},
                    source_revision=revision,
                ))
                if caller and reference_kind == "call":
                    out.append(EdgeFact(
                        src=caller,
                        dst=target,
                        edge_type="calls",
                        attrs={
                            **_reference_edge_attrs(
                                ref_attrs,
                                reference_kind,
                            ),
                            "line": ref.span_start,
                        },
                        source_record_id={"repo": self.repo, "path": rel_path, "reference_id": rid},
                        source_revision=revision,
                    ))
        return _dedupe_facts(out)


def _parent_symbol_id(
    qname: str,
    symbol_id_by_qname: dict[str, str],
) -> str:
    if "." not in qname:
        return ""
    parent_qname = qname.rsplit(".", 1)[0]
    return symbol_id_by_qname.get(parent_qname, "")


def _import_attrs(
    repo: str,
    rel_path: str,
    language: str,
    imp: Import,
) -> dict[str, Any]:
    return {
        "repo": repo,
        "path": rel_path,
        "language": language,
        "module": imp.module,
        "name": imp.name,
        "alias": imp.alias,
        "kind": imp.kind,
        "start_line": imp.span_start,
        "end_line": imp.span_end,
        "start_col": getattr(imp, "start_col", 0) or 0,
        "end_col": getattr(imp, "end_col", 0) or 0,
        "semantic_target_key": getattr(imp, "semantic_target_key", "") or "",
        "resolution_kind": getattr(imp, "resolution_kind", "") or "",
        "resolution_method": getattr(imp, "resolution_method", "") or "",
        "resolver": getattr(imp, "resolver", "") or "code_structure",
        "resolver_version": getattr(imp, "resolver_version", "") or "",
        "confidence": getattr(imp, "confidence", "") or "",
        "lsp_spec_version": getattr(imp, "lsp_spec_version", "") or "",
        "lsp_method": getattr(imp, "lsp_method", "") or "",
        "server_capability": getattr(imp, "server_capability", "") or "",
    }


def _reference_attrs(
    repo: str,
    rel_path: str,
    language: str,
    ref: Reference,
    symbol_id_by_qname: dict[str, str],
    semantic_key_by_qname: dict[str, str],
) -> dict[str, Any]:
    qualified_name = (
        getattr(ref, "qualified_name", "")
        or _normalized_reference_qname(ref.callee_name)
    )
    name = _reference_leaf_name(qualified_name)
    reference_kind = getattr(ref, "reference_kind", "") or _reference_kind(ref)
    resolution_method = (
        getattr(ref, "resolution_method", "")
        or _resolution_method(ref.resolution_kind)
    )
    return {
        "repo": repo,
        "path": rel_path,
        "language": language,
        "name": name,
        "lexical_name": getattr(ref, "lexical_name", "") or ref.callee_name,
        "qualified_name": qualified_name,
        "raw_qualified_name": ref.callee_name,
        "caller_qname": ref.caller_qname,
        "reference_kind": reference_kind,
        "resolution_kind": ref.resolution_kind,
        "resolution_method": resolution_method,
        "resolver": getattr(ref, "resolver", "") or "code_structure",
        "resolver_version": getattr(ref, "resolver_version", "") or "",
        "confidence": (
            getattr(ref, "confidence", "")
            or _reference_confidence(ref.resolution_kind)
        ),
        "semantic_target_key": (
            getattr(ref, "semantic_target_key", "") or ""
        ),
        "semantic_enclosing_key": (
            getattr(ref, "semantic_enclosing_key", "")
            or semantic_key_by_qname.get(ref.caller_qname, "")
        ),
        "unresolved_reason": (
            getattr(ref, "unresolved_reason", "") or ""
        ),
        "candidate_count": getattr(ref, "candidate_count", None),
        "workspace_root": getattr(ref, "workspace_root", "") or "",
        "workspace_fingerprint": (
            getattr(ref, "workspace_fingerprint", "") or ""
        ),
        "source_frontier": getattr(ref, "source_frontier", "") or "",
        "lsp_spec_version": getattr(ref, "lsp_spec_version", "") or "",
        "lsp_method": getattr(ref, "lsp_method", "") or "",
        "server_capability": getattr(ref, "server_capability", "") or "",
        "syntax_optional_chain": "optional_chain" in ref.resolution_kind,
        "enclosing_symbol_id": symbol_id_by_qname.get(
            ref.caller_qname, "",
        ),
        "start_line": ref.span_start,
        "end_line": ref.span_end,
        "start_col": getattr(ref, "start_col", 0) or 0,
        "end_col": getattr(ref, "end_col", 0) or 0,
    }


def _symbol_semantic_key(
    repo: str,
    language: str,
    sym: Symbol,
    *,
    module_name: str,
) -> str:
    explicit = getattr(sym, "semantic_key", "") or ""
    if explicit:
        return explicit
    return semantic_key_for_symbol(
        repo=repo,
        language=language,
        module_name=module_name,
        qualified_name=sym.qualified_name,
        kind=getattr(sym, "symbol_kind", "") or sym.kind,
        package_name=getattr(sym, "package_name", "") or "",
    )


def _parent_symbol_key(
    qname: str,
    semantic_key_by_qname: dict[str, str],
) -> str:
    if "." not in qname:
        return ""
    parent_qname = qname.rsplit(".", 1)[0]
    return semantic_key_by_qname.get(parent_qname, "")


def _import_identity_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "repo",
        "path",
        "language",
        "module",
        "name",
        "alias",
        "kind",
        "start_line",
        "end_line",
        "start_col",
        "end_col",
    )
    return {key: attrs.get(key) for key in keys}


def _reference_identity_attrs(attrs: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "repo",
        "path",
        "language",
        "name",
        "qualified_name",
        "raw_qualified_name",
        "reference_kind",
        "resolution_kind",
        "syntax_optional_chain",
        "enclosing_symbol_id",
        "start_line",
        "end_line",
        "start_col",
        "end_col",
    )
    return {key: attrs.get(key) for key in keys}


def _reference_edge_attrs(
    ref_attrs: dict[str, Any],
    reference_kind: str,
) -> dict[str, Any]:
    out = {
        "resolution": ref_attrs.get("resolution_kind") or "",
        "resolution_kind": ref_attrs.get("resolution_kind") or "",
        "resolution_method": ref_attrs.get("resolution_method") or "",
        "reference_kind": reference_kind,
        "resolver": ref_attrs.get("resolver") or "",
        "resolver_version": ref_attrs.get("resolver_version") or "",
        "confidence": ref_attrs.get("confidence") or "",
    }
    for key in (
        "lsp_spec_version",
        "lsp_method",
        "server_capability",
        "semantic_target_key",
        "workspace_root",
        "workspace_fingerprint",
        "source_frontier",
    ):
        value = ref_attrs.get(key)
        if value:
            out[key] = value
    return out


def _resolution_method(resolution_kind: str) -> str:
    if resolution_kind == "cross_file_lsp":
        return "lsp"
    if resolution_kind in {
        "within_file",
        "within_file_optional_chain",
        "callback_argument",
        "returned_object",
        "public_export",
    }:
        return "syntax_within_file"
    if resolution_kind == "unresolved":
        return "unresolved"
    return "syntax"


def _reference_confidence(resolution_kind: str) -> str:
    if resolution_kind == "cross_file_lsp":
        return "high"
    if resolution_kind in {
        "within_file",
        "within_file_optional_chain",
        "callback_argument",
        "returned_object",
        "public_export",
    }:
        return "medium"
    return "low"


def _resolve_reference_target(
    ref: Reference,
    symbol_by_qname: dict[str, Symbol],
    symbol_by_name: dict[str, Symbol],
    symbol_id_by_qname: dict[str, str],
) -> str:
    qualified_name = _normalized_reference_qname(ref.callee_name)
    name = _reference_leaf_name(qualified_name)
    candidates = [
        qualified_name,
        name,
    ]
    for candidate in candidates:
        sym = symbol_by_qname.get(candidate) or symbol_by_name.get(candidate)
        if sym:
            return symbol_id_by_qname[sym.qualified_name]
    return ""


def _normalized_reference_qname(value: str) -> str:
    text = value.strip().replace("?.", ".").replace("?[", "[")
    text = re.sub(r"\.{2,}", ".", text)
    return text.rstrip(".?").strip()


def _reference_leaf_name(value: str) -> str:
    text = _normalized_reference_qname(value)
    if not text:
        return ""
    text = text.rsplit(".", 1)[-1]
    if "[" in text:
        text = text.split("[", 1)[0]
    return text.rstrip(".?")


def _reference_kind(ref: Reference) -> str:
    if ref.resolution_kind in {
        "callback_argument",
        "returned_object",
        "public_export",
    }:
        return ref.resolution_kind
    return "call"
