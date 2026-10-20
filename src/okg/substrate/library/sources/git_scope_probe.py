"""GitScopeProbe — the reference ChangeProbe for fully-git-backed sources (#323).

The token is built from git's CONTENT identity, never from filesystem mtime:

  - ``content`` — HEAD ``^{tree}`` sha, plus a hash over the working-tree dirty
    set keyed by ``(path, content_sha)`` so an edit to an already-dirty file
    still busts the token (the path-set alone would not);
  - ``head`` — HEAD commit sha, DISTINCT from the tree sha, so a HEAD move to an
    identical tree (e.g. ``git reset --hard`` to an empty/merge commit) is
    detected;
  - ``config`` / ``emit_version`` — see ``change_probe``.

Because nothing in the token is derived from mtime, a worktree checkout that
resets mtimes without changing content leaves the token unchanged → the runner
short-circuits. That is the exact failure mode that defeated the per-file
``FileWalk`` mtime fast-path and regressed code-data-flow to ~106s.

The probe lets pygit2 / OS errors propagate; the runner boundary
(``change_probe.run_probe``) catches them and resolves to ``changed=True``
(fail toward a full read).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from okg.substrate.library.sources._git_helpers import (
    git_untracked_worktree_paths,
    in_proc_head_snapshot,
    in_proc_workdir,
    tracked_dirty_worktree_paths,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
    emit_version_for,
    tokens_equal,
)


# CURSOR_TOKEN_KEY is re-exported for callers that import it from here.
__all__ = ["GitScopeProbe", "CURSOR_TOKEN_KEY"]


class GitScopeProbe:
    """A churn-immune scope probe for a git-backed source.

    Parameters
    ----------
    repo_root:
        The git working-tree root.
    config:
        The adapter's effective configuration (include/exclude globs, scope
        roots, repo identity, extraction params, and any other fact-determining
        input). Hashed into the token's ``config`` component.
    emit_targets:
        The adapter's emission callables/modules. Hashed (via
        ``emit_version_for``) into a machine-derived ``emit_version`` so a change
        to emission logic busts the token with no manual bump.
    scope_filter:
        Optional ``(rel_path) -> bool`` to bound the dirty set to the source's
        own scope (e.g. only ``.py`` files). A wider dirty set only costs a
        spurious full read (the safe direction), never a missed change.
    """

    def __init__(
        self,
        *,
        repo_root: Any,
        config: Mapping[str, Any],
        emit_targets: Any,
        scope_filter: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self.repo_root = Path(repo_root)
        self._config_hash = _hash_config(config)
        self._emit_version = emit_version_for(*_as_tuple(emit_targets))
        self._scope_filter = scope_filter
        self._include_globs = _include_globs(config)
        self._exclude_globs = _exclude_globs(config)
        self._include_untracked = _include_untracked(config)
        self._subtree_prefix = str(config.get("_subtree_prefix") or "").strip("/")

    @classmethod
    def for_subtree(
        cls,
        subtree_root: Any,
        *,
        config: Mapping[str, Any],
        emit_targets: Any,
    ) -> "GitScopeProbe":
        """Build a probe for a source rooted at a SUBDIRECTORY of a git repo.

        The probe runs at the discovered git toplevel (so the dirty-set content
        reads resolve correctly), with a scope_filter that bounds the dirty set
        to the subtree (toplevel-relative). Raises ChangeProbeError if the path
        is not inside a git work-tree (the runner boundary resolves that to a
        full read). Use for openspec / pact whose roots are `<repo>/openspec`,
        `<repo>/pact`.
        """
        from okg.substrate.library.sources.change_probe import ChangeProbeError

        subtree = Path(subtree_root).resolve()
        toplevel = in_proc_workdir(subtree)
        if toplevel is None:
            raise ChangeProbeError(
                f"GitScopeProbe.for_subtree: {subtree} is not inside a git work-tree"
            )
        try:
            prefix = subtree.relative_to(toplevel).as_posix()
        except ValueError:
            prefix = ""
        if prefix in ("", "."):
            scope_filter = None  # subtree IS the toplevel
        else:
            pfx = prefix + "/"

            def scope_filter(rel_path: str) -> bool:
                return rel_path == prefix or rel_path.startswith(pfx)

        merged_config = dict(config)
        merged_config["_subtree_prefix"] = prefix
        return cls(
            repo_root=toplevel,
            config=merged_config,
            emit_targets=emit_targets,
            scope_filter=scope_filter,
        )

    @staticmethod
    def _content_component_from_snapshot(
        *,
        tree_sha: str,
        branch: str,
        dirty_manifest: Mapping[str, Mapping[str, str]],
    ) -> dict[str, Any]:
        digest = hashlib.sha256()
        digest.update(tree_sha.encode("utf-8"))
        digest.update(b"\x00branch\x00")
        digest.update(branch.encode("utf-8"))
        for path, identity in sorted(dirty_manifest.items()):
            digest.update(b"\x00")
            digest.update(path.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(str(identity["kind"]).encode("utf-8"))
            digest.update(b"\x00")
            digest.update(str(identity["sha256"]).encode("utf-8"))
        return {"kind": "git_tree", "value": digest.hexdigest()}

    def _dirty_manifest(self) -> dict[str, dict[str, str]]:
        tracked = self._tracked_dirty_paths()
        untracked = self._all_untracked_paths() if self._include_untracked else set()
        paths = tracked | untracked
        if self._scope_filter is not None:
            paths = {path for path in paths if self._scope_filter(path)}
        return {
            path: {
                "kind": (
                    "tracked+untracked"
                    if path in tracked and path in untracked
                    else "tracked" if path in tracked
                    else "untracked"
                ),
                "sha256": _working_tree_content_sha(self.repo_root, path),
            }
            for path in sorted(paths)
        }

    def _dirty_paths(self) -> set[str]:
        dirty = self._tracked_dirty_paths()
        if self._include_untracked:
            dirty.update(self._all_untracked_paths())
        return dirty

    def _tracked_dirty_paths(self) -> set[str]:
        return tracked_dirty_worktree_paths(
            self.repo_root,
            include_globs=self._include_globs or None,
            exclude_globs=self._exclude_globs,
            path_filter=self._scope_filter,
        )

    def _all_untracked_paths(self) -> set[str]:
        return git_untracked_worktree_paths(
            self.repo_root,
            include_globs=self._include_globs or None,
            exclude_globs=self._exclude_globs,
            path_filter=self._scope_filter,
            pathspecs=self._pathspecs(),
        )

    def _pathspecs(self) -> tuple[str, ...]:
        if not self._subtree_prefix:
            return ()
        return (self._subtree_prefix,)

    def build_token(self) -> dict[str, Any]:
        """Compose the current canonical token (validated by ``compose_token``)."""
        snapshot = self.capture_snapshot()
        return self.token_for_snapshot(**snapshot)

    def capture_snapshot(self) -> dict[str, Any]:
        """Capture the exact Git/worktree inputs used by this probe."""
        head = in_proc_head_snapshot(self.repo_root)
        return {
            "tree_sha": head.tree_sha,
            "head_sha": head.head_sha,
            "branch": head.branch,
            "dirty_manifest": self._dirty_manifest(),
        }

    def file_paths_for_snapshot(
        self,
        snapshot: Mapping[str, Any],
    ) -> set[str]:
        """Derive the source-visible file inventory from a pinned snapshot."""
        from okg.substrate.library.sources._git_helpers import (
            in_proc_tree_paths,
            path_included_by_globs,
        )

        paths = in_proc_tree_paths(
            self.repo_root,
            tree_sha=str(snapshot["tree_sha"]),
        )
        dirty_manifest = snapshot.get("dirty_manifest") or {}
        if self._include_untracked and isinstance(dirty_manifest, Mapping):
            paths.update(
                str(path)
                for path, identity in dirty_manifest.items()
                if isinstance(identity, Mapping)
                and "untracked" in str(identity.get("kind") or "")
                and identity.get("sha256") != "absent"
            )
        return {
            path
            for path in paths
            if path_included_by_globs(
                path,
                include_globs=self._include_globs or None,
                exclude_globs=self._exclude_globs,
            )
            and (
                self._scope_filter is None
                or self._scope_filter(path)
            )
        }

    def token_for_snapshot(
        self,
        *,
        tree_sha: str,
        head_sha: str,
        branch: str,
        dirty_manifest: Mapping[str, Mapping[str, str]],
    ) -> dict[str, Any]:
        """Compose a token from the exact Git/worktree capture emitted."""
        return compose_token(
            content=self._content_component_from_snapshot(
                tree_sha=tree_sha,
                branch=branch,
                dirty_manifest=dirty_manifest,
            ),
            head={"kind": "git_head", "value": head_sha},
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def probe(
        self,
        *,
        cursor: Optional[Mapping[str, Any]],
        sync_scope: Optional[Mapping[str, Any]],
    ) -> ProbeVerdict:
        prior = (
            cursor.get(CURSOR_TOKEN_KEY)
            if isinstance(cursor, Mapping)
            else None
        )
        token = self.build_token()
        if prior is None:
            return ProbeVerdict(changed=True, token=token, reason="no_prior_token")
        changed = not tokens_equal(token, prior)
        return ProbeVerdict(
            changed=changed,
            token=token,
            reason="git_scope_changed" if changed else "git_scope_unchanged",
        )


def _as_tuple(targets: Any) -> tuple[Any, ...]:
    if isinstance(targets, (list, tuple)):
        return tuple(targets)
    return (targets,)


def _hash_config(config: Mapping[str, Any]) -> str:
    payload = json.dumps(dict(config), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _exclude_globs(config: Mapping[str, Any]) -> tuple[str, ...]:
    raw = config.get("exclude_globs")
    if raw is None:
        raw = config.get("repo_file_exclude_globs")
    return tuple(str(item) for item in (raw or ()))


def _include_globs(config: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(str(item) for item in (config.get("include_globs") or ()))


def _include_untracked(config: Mapping[str, Any]) -> bool:
    if "include_untracked" in config:
        return bool(config["include_untracked"])
    if "repo_file_include_untracked" in config:
        return bool(config["repo_file_include_untracked"])
    return True


def _working_tree_content_sha(repo_root: Path, rel_path: str) -> str:
    path = repo_root / rel_path
    try:
        data = path.read_bytes()
    except OSError:
        # A path in the dirty set that we cannot read (deleted between status and
        # read). "absent" is a stable content identity for "gone".
        return "absent"
    return hashlib.sha256(data).hexdigest()
