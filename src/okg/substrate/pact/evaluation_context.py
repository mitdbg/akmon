"""Immutable authorities for evaluating a captured PACT snapshot.

The normal CLI evaluates live process, filesystem, ledger, and Git state.
Sources that already captured those inputs must not reopen them while deriving
facts: doing so would mix snapshots and make an applied identity unsound.
``PactEvaluationContext`` is the optional seam between those two modes.
"""

from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from okg.substrate.pact.evidence_kinds import EvidenceKindRegistry
from okg.substrate.pact.fsm import DecisionEvent


@dataclass(frozen=True)
class CapturedFileLookup:
    """One authoritative file lookup.

    ``captured=False`` means no file authority was supplied and callers may
    retain their live default. ``captured=True, content=None`` means the
    captured authority says the path is missing; callers must not fall back.
    """

    captured: bool
    content: bytes | None = None

    @property
    def exists(self) -> bool:
        return self.captured and self.content is not None


def _normalize_key(value: str | Path) -> str:
    raw = str(value).replace("\\", "/")
    normalized = posixpath.normpath(raw)
    return normalized if normalized != "." else ""


@dataclass(frozen=True)
class PactEvaluationContext:
    """Captured read authorities used by PACT validation and review.

    Each authority is independently optional. Supplying one makes it strict:
    a missing key means captured-missing, never "try the live system".
    """

    env_truthiness: Mapping[str, bool] | None = None
    files: Mapping[str, bytes | None] | None = None
    file_mtimes: Mapping[str, float] | None = None
    evidence_registry: EvidenceKindRegistry | None = None

    ledger_authoritative: bool = False
    ledger_available: bool = True
    ledger_reason: str | None = None
    decision_events_by_change: Mapping[str, tuple[DecisionEvent, ...]] = field(
        default_factory=dict
    )
    ledger_intents: tuple[Mapping[str, Any], ...] = ()
    worktree_identity: Mapping[str, Any] = field(default_factory=dict)

    git_authoritative: bool = False
    git_head_sha: str | None = None
    git_base_ref: str | None = None
    git_base_branch: str = "dev"
    git_changed_paths: tuple[str, ...] | None = None
    git_available: bool = True
    affected_code_timestamps: Mapping[tuple[str, ...], Any] = field(
        default_factory=dict
    )
    redaction_home: str | None = None
    evaluation_time: float | None = None
    # Optional authority limited to PACT manifest lookups. Keep this appended
    # so existing positional construction of the public dataclass is stable.
    # Unlike ``files``, supplying this mapping must not make unrelated evidence,
    # test, registry, or configuration paths captured-missing.
    manifest_files: Mapping[str, bytes | None] | None = None
    # Archived PACTs retain formal checker output digests for audit, but the
    # output itself lives under worktree-local .scratch and is intentionally
    # absent from a clean checkout. Changed-archive CI may ignore only that
    # machine-local artifact digest while keeping model, config, checker, and
    # refinement bindings strict. Normal validation leaves this disabled.
    ignore_machine_local_formal_artifact_digest: bool = False

    def __post_init__(self) -> None:
        if type(self.ignore_machine_local_formal_artifact_digest) is not bool:
            raise TypeError(
                "ignore_machine_local_formal_artifact_digest must be a boolean"
            )
        env = (
            None
            if self.env_truthiness is None
            else MappingProxyType({
                str(key): bool(value)
                for key, value in self.env_truthiness.items()
            })
        )
        files = (
            None
            if self.files is None
            else MappingProxyType({
                _normalize_key(key): (
                    None if value is None else bytes(value)
                )
                for key, value in self.files.items()
            })
        )
        manifest_files = (
            None
            if self.manifest_files is None
            else MappingProxyType({
                _normalize_key(key): (
                    None if value is None else bytes(value)
                )
                for key, value in self.manifest_files.items()
            })
        )
        mtimes = (
            None
            if self.file_mtimes is None
            else MappingProxyType({
                _normalize_key(key): float(value)
                for key, value in self.file_mtimes.items()
            })
        )
        decisions = MappingProxyType({
            str(change_id): tuple(events)
            for change_id, events in self.decision_events_by_change.items()
        })
        intents = tuple(
            MappingProxyType(dict(intent))
            for intent in self.ledger_intents
        )
        worktree = MappingProxyType(dict(self.worktree_identity))
        timestamps = MappingProxyType({
            tuple(sorted(str(path) for path in paths)): timestamp
            for paths, timestamp in self.affected_code_timestamps.items()
        })
        object.__setattr__(self, "env_truthiness", env)
        object.__setattr__(self, "files", files)
        object.__setattr__(self, "manifest_files", manifest_files)
        object.__setattr__(self, "file_mtimes", mtimes)
        object.__setattr__(self, "decision_events_by_change", decisions)
        object.__setattr__(self, "ledger_intents", intents)
        object.__setattr__(self, "worktree_identity", worktree)
        object.__setattr__(self, "git_changed_paths", (
            None
            if self.git_changed_paths is None
            else tuple(sorted(str(path) for path in self.git_changed_paths))
        ))
        object.__setattr__(self, "affected_code_timestamps", timestamps)

    def env_is_truthy(self, name: str) -> bool | None:
        if self.env_truthiness is None:
            return None
        return bool(self.env_truthiness.get(name, False))

    @staticmethod
    def resolve_repo_path(repo_root: Path, ref: str | Path) -> Path:
        """Resolve lexically without consulting mutable symlink state."""
        root = Path(os.path.abspath(os.path.normpath(str(repo_root))))
        candidate = Path(ref)
        if candidate.is_absolute():
            return Path(os.path.abspath(os.path.normpath(str(candidate))))
        return Path(os.path.abspath(os.path.normpath(str(root / candidate))))

    def file_lookup(
        self,
        repo_root: Path,
        path: str | Path,
    ) -> CapturedFileLookup:
        if self.files is None:
            return CapturedFileLookup(captured=False)
        target = self.resolve_repo_path(repo_root, path)
        root = self.resolve_repo_path(repo_root, ".")
        keys = [_normalize_key(target)]
        try:
            keys.insert(0, _normalize_key(target.relative_to(root)))
        except ValueError:
            # Absolute paths outside the repository have no repo-relative key.
            pass
        for key in keys:
            if key in self.files:
                return CapturedFileLookup(
                    captured=True,
                    content=self.files[key],
                )
        return CapturedFileLookup(captured=True, content=None)

    def manifest_file_lookup(
        self,
        repo_root: Path,
        path: str | Path,
    ) -> CapturedFileLookup:
        """Resolve a manifest without broadening authority to other files.

        When no manifest-only authority was supplied, preserve the historical
        strict ``files`` behavior used by captured evaluation snapshots.
        """
        if self.manifest_files is None:
            return self.file_lookup(repo_root, path)
        target = self.resolve_repo_path(repo_root, path)
        root = self.resolve_repo_path(repo_root, ".")
        keys = [_normalize_key(target)]
        try:
            keys.insert(0, _normalize_key(target.relative_to(root)))
        except ValueError:
            # Absolute paths outside the repository have no repo-relative key.
            pass
        for key in keys:
            if key in self.manifest_files:
                return CapturedFileLookup(
                    captured=True,
                    content=self.manifest_files[key],
                )
        return CapturedFileLookup(captured=True, content=None)

    def captured_file_mtime(
        self,
        repo_root: Path,
        path: str | Path,
    ) -> float | None:
        if self.file_mtimes is None:
            return None
        target = self.resolve_repo_path(repo_root, path)
        root = self.resolve_repo_path(repo_root, ".")
        keys = [_normalize_key(target)]
        try:
            keys.insert(0, _normalize_key(target.relative_to(root)))
        except ValueError:
            pass
        for key in keys:
            if key in self.file_mtimes:
                return self.file_mtimes[key]
        return None

    def iter_repo_files(
        self,
        repo_root: Path,
        *,
        prefix: str = "",
        suffix: str = "",
    ) -> tuple[tuple[str, bytes, float | None], ...]:
        if self.files is None:
            return ()
        root = self.resolve_repo_path(repo_root, ".")
        normalized_prefix = _normalize_key(prefix)
        rows: list[tuple[str, bytes, float | None]] = []
        for key, content in self.files.items():
            if content is None:
                continue
            candidate = Path(key)
            if candidate.is_absolute():
                try:
                    relpath = candidate.relative_to(root).as_posix()
                except ValueError:
                    continue
            else:
                relpath = _normalize_key(candidate)
                if relpath == ".." or relpath.startswith("../"):
                    continue
            if normalized_prefix and not (
                relpath == normalized_prefix
                or relpath.startswith(normalized_prefix.rstrip("/") + "/")
            ):
                continue
            if suffix and not relpath.endswith(suffix):
                continue
            rows.append((
                relpath,
                content,
                self.captured_file_mtime(repo_root, relpath),
            ))
        rows.sort(key=lambda row: row[0])
        return tuple(rows)

    def affected_code_timestamp(
        self,
        paths: list[str],
    ) -> tuple[bool, Any]:
        if not self.git_authoritative:
            return False, None
        if self.git_head_sha is None:
            return True, None
        key = tuple(sorted(str(path) for path in paths))
        return True, self.affected_code_timestamps.get(key)


__all__ = ["CapturedFileLookup", "PactEvaluationContext"]
