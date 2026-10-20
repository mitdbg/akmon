"""Exact Git-visible input snapshots for code-derived sources."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from okg.substrate.library.sources._git_helpers import (
    in_proc_head_blob_bytes,
    run_git,
)
from okg.substrate.library.sources.change_probe import (
    ChangeProbeError,
    tokens_equal,
)
from okg.substrate.library.sources.code_graph_structure import (
    _CapturedCodeFile,
    _CodeStructureInputSnapshot,
)
from okg.substrate.library.sources.git_scope_probe import GitScopeProbe


class _GitVisibleCodeSnapshotProbe(GitScopeProbe):
    """Capture the exact tracked/non-ignored bytes consumed by a code reader."""

    def __init__(
        self,
        *,
        repo_root: Path,
        config: Mapping[str, Any],
        emit_targets: Sequence[Any],
        scope_filter: Callable[[str], bool],
        max_files: int,
        max_bytes_per_file: int,
        source_label: str,
    ) -> None:
        super().__init__(
            repo_root=repo_root,
            config=config,
            emit_targets=emit_targets,
            scope_filter=scope_filter,
        )
        self._max_files = int(max_files)
        self._max_bytes_per_file = int(max_bytes_per_file)
        self._source_label = source_label

    @staticmethod
    def _head_symlink_paths(
        root: Path,
        tree_sha: str,
    ) -> set[str]:
        records = run_git(
            root,
            ["ls-tree", "-r", "-z", "--full-tree", tree_sha],
        )
        symlinks: set[str] = set()
        for record in records.split("\0"):
            if not record or "\t" not in record:
                continue
            header, rel_path = record.split("\t", 1)
            mode = header.split(" ", 1)[0]
            if mode == "120000":
                symlinks.add(rel_path)
        return symlinks

    def _source_paths(
        self,
        snapshot: Mapping[str, Any],
    ) -> tuple[Path, ...]:
        root = Path(self.repo_root).resolve()
        dirty = set(snapshot.get("dirty_manifest") or {})
        clean_symlinks = (
            self._head_symlink_paths(
                root,
                str(snapshot["tree_sha"]),
            )
            - dirty
        )
        paths: list[Path] = []
        for rel_path in sorted(self.file_paths_for_snapshot(snapshot)):
            if rel_path in clean_symlinks:
                continue
            path = root / rel_path
            try:
                if path.stat().st_size > self._max_bytes_per_file:
                    continue
            except (FileNotFoundError, NotADirectoryError):
                # Deletions remain represented by the dirty token and
                # record-set diff, but have no bytes to parse.
                continue
            except OSError as exc:
                # Issue #2892 review. Any other failure (an unreadable
                # directory) is not a deletion: skipping the file here would
                # let a complete run retract its facts.
                raise ChangeProbeError(
                    f"{self._source_label} could not stat {rel_path}: {exc}"
                ) from exc
            paths.append(path)
            if len(paths) > self._max_files:
                raise ValueError(
                    f"{self._source_label} exceeded "
                    f"max_files={self._max_files}; tighten exclude_globs "
                    "or raise the cap"
                )
        return tuple(paths)

    def build_token(self) -> dict[str, Any]:
        return dict(self.capture_snapshot().token)

    def capture_snapshot(self) -> _CodeStructureInputSnapshot:
        root = Path(self.repo_root).resolve()
        captured_git = GitScopeProbe.capture_snapshot(self)
        source_paths = self._source_paths(captured_git)
        dirty_manifest = dict(captured_git.get("dirty_manifest") or {})
        dirty_paths = frozenset(dirty_manifest)

        raw_by_path: dict[str, bytes] = {}
        captured: list[_CapturedCodeFile] = []
        for path in source_paths:
            rel_path = path.relative_to(root).as_posix()
            try:
                raw = path.read_bytes()
            except OSError as exc:
                raise ChangeProbeError(
                    f"{self._source_label} could not read {rel_path}"
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

        # Clean tracked bytes must equal the pinned tree. Dirty and untracked
        # bytes must equal the pre-read content manifest. These comparisons
        # make the applied token describe the bytes actually parsed rather
        # than a later re-read that could accept an A→B→A race.
        clean_paths = [
            item.rel_path
            for item in captured
            if item.rel_path not in dirty_paths
        ]
        head_blobs = in_proc_head_blob_bytes(
            root,
            clean_paths,
            tree_sha=str(captured_git["tree_sha"]),
        )
        for rel_path in clean_paths:
            if head_blobs.get(rel_path) != raw_by_path[rel_path]:
                raise ChangeProbeError(
                    f"{self._source_label} clean-file snapshot "
                    f"diverged from HEAD: {rel_path}"
                )
        for rel_path in dirty_paths.intersection(raw_by_path):
            observed_sha = hashlib.sha256(raw_by_path[rel_path]).hexdigest()
            if dirty_manifest[rel_path].get("sha256") != observed_sha:
                raise ChangeProbeError(
                    f"{self._source_label} dirty-file snapshot "
                    f"changed during capture: {rel_path}"
                )

        token = self.token_for_snapshot(**captured_git)
        post_git = GitScopeProbe.capture_snapshot(self)
        post_token = self.token_for_snapshot(**post_git)
        if not tokens_equal(token, post_token):
            raise ChangeProbeError(
                f"{self._source_label} input changed during snapshot"
            )
        post_paths = self._source_paths(post_git)
        if tuple(source_paths) != tuple(post_paths):
            raise ChangeProbeError(
                f"{self._source_label} input membership changed "
                "during snapshot"
            )
        return _CodeStructureInputSnapshot(
            files=tuple(captured),
            token=token,
            head_sha=str(captured_git["head_sha"]),
            dirty_paths=dirty_paths,
        )


def build_git_visible_code_snapshot_probe(
    *,
    repo_root: Path,
    config: Mapping[str, Any],
    emit_targets: Sequence[Any],
    scope_filter: Callable[[str], bool],
    max_files: int,
    max_bytes_per_file: int,
    source_label: str,
) -> GitScopeProbe:
    """Build the full-read counterpart to a cheap ``GitScopeProbe``."""
    return _GitVisibleCodeSnapshotProbe(
        repo_root=repo_root,
        config=config,
        emit_targets=emit_targets,
        scope_filter=scope_filter,
        max_files=max_files,
        max_bytes_per_file=max_bytes_per_file,
        source_label=source_label,
    )
