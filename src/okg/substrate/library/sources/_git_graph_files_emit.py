"""GitFilesSource — emit ``SourceFile`` nodes for tracked files in a git repo.

Performs a full crawl using exactly three batched git invocations
(four when text/binary disambiguation is needed), and supports
incremental crawl via a ``state`` attribute the pipeline persists
between runs:

    {
        "tree_sha": "<HEAD tree SHA at last successful run>",
        "dirty_paths": ["path/a", "path/b"],  # dirty set at last run
    }

When ``state["tree_sha"]`` matches the current ``HEAD^{tree}`` AND
the dirty set is unchanged, the source emits zero nodes.

When ``state`` is ``None`` (or empty), the source performs a full
crawl. When ``state["tree_sha"]`` differs, the source uses
``git diff --name-status`` to determine the minimal re-emit set.
When only the dirty set differs, it re-emits each path in the
symmetric difference.

After each run, callers should read ``source.state`` to persist the
updated values.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec
from okg.substrate.library.sources._git_helpers import (
    DEFAULT_EXCLUDE_GLOBS,
    GitInvocationError,
    in_proc_diff_name_status,
    in_proc_head_snapshot,
    in_proc_index_paths,
    PathHistory,
    in_proc_path_history,
    in_proc_list_tracked,
    in_proc_ls_tree,
    in_proc_status_porcelain_paths,
    language_for_path,
    open_repository_at,
    path_included_by_globs,
    pruned_untracked_worktree_paths,
    scoped_untracked_worktree_paths,
    tracked_dirty_worktree_paths,
)

logger = logging.getLogger(__name__)


def normalize_scope_key(key: Any) -> str | None:
    """Repo-relative posix form of a scope key, or None when it names none.

    `./a`, `a//b` and `/a` name `a`, the form git reports, so a scoped run
    finds the file. A key that climbs out of the repository names no file of
    this source.
    """
    import posixpath

    if key is None:
        return None
    raw = str(key).replace("\\", "/").lstrip("/")
    if not raw:
        return None
    rel = posixpath.normpath(raw)
    if rel in ("", ".", "..") or rel.startswith("../"):
        return None
    return rel


@dataclass
class _FileMeta:
    path: str
    blob_sha: str
    size_bytes: int
    is_text: bool
    last_modified_commit_sha: str | None = None


def _common_git_dir(repo: Any) -> Path:
    """The directory that holds the object database, for linked worktrees too."""
    git_dir = Path(repo.path)
    commondir = git_dir / "commondir"
    if commondir.is_file():
        return (git_dir / commondir.read_text().strip()).resolve()
    return git_dir


_NULL_SHA_CHARS = frozenset("0")


def _shallow_or_reflog_records_commits(git_dir: Path, common_dir: Path) -> bool:
    """Report whether ``shallow`` or any reflog names a commit.

    A non-empty ``shallow`` file lists commits the repository holds. A reflog
    line is ``<old> <new> <ident>\\t<message>``; a ``<new>`` that is not all
    zeros records a ref moving to a commit. Both outlive a repository's
    objects, packs and refs, so either one means the repository had commits.
    A line that does not parse, or a file that cannot be read, also counts:
    any doubt means "not verified empty".
    """
    if (common_dir / "shallow").is_file():
        if (common_dir / "shallow").read_text(errors="replace").strip():
            return True
    for base in {git_dir.resolve(), common_dir.resolve()}:
        logs_dir = base / "logs"
        if not logs_dir.is_dir():
            continue
        for log_file in logs_dir.rglob("*"):
            if not log_file.is_file():
                continue
            for line in log_file.read_text(errors="replace").splitlines():
                if not line.strip():
                    continue
                fields = line.split(" ", 2)
                if len(fields) < 2 or set(fields[1]) - _NULL_SHA_CHARS:
                    return True
    return False


def head_is_verified_unborn(repo_root: Path) -> bool:
    """Report whether ``repo_root`` is a valid repository that never had commits.

    True only when every check below holds, read through a fresh libgit2
    handle:

    * the repository opens at ``repo_root`` itself, without searching parent
      directories (`open_repository_at`): its working tree (or, for a bare
      repository, its git directory) is ``repo_root``, so a broken ``.git``
      inside a parent repository is never answered for the parent;
    * HEAD is a symbolic ref to a branch that does not exist (unborn);
    * the repository holds no references at all;
    * no ``shallow`` file lists a commit, and no reflog under ``logs/``
      records a move to a commit -- both survive a loss of every object,
      pack and ref;
    * ``objects/pack`` holds no file -- no pack, index or interrupted
      ``tmp_pack_*`` from a fetch or clone that was killed;
    * ``objects/info`` holds no file -- no ``alternates`` (libgit2 skips a
      missing alternate store silently), no ``commit-graph`` or
      ``commit-graphs/`` left behind by a repository that had commits;
    * no object in the object database is a commit (read by header only,
      so a large staged file is not inflated).

    A repository that has commits but lost its refs (a deleted or
    ``.lock``-renamed branch ref, an interrupted fetch whose pack landed)
    fails the last checks, so it is never read as empty. Any error while
    checking answers False: only a repository that verifiably has nothing
    to read may report an empty result as complete.
    """
    try:
        import pygit2

        repo = open_repository_at(Path(repo_root))
        if not repo.head_is_unborn:
            return False
        if list(repo.references):
            return False
        common_dir = _common_git_dir(repo)
        if _shallow_or_reflog_records_commits(Path(repo.path), common_dir):
            return False
        objects_dir = common_dir / "objects"
        pack_dir = objects_dir / "pack"
        if pack_dir.is_dir() and any(pack_dir.iterdir()):
            return False
        info_dir = objects_dir / "info"
        if info_dir.is_dir() and any(info_dir.iterdir()):
            return False
        commit_type = pygit2.enums.ObjectType.COMMIT
        for oid in repo.odb:
            if repo.odb.read_header(oid)[0] == commit_type:
                return False
        return True
    except Exception:  # noqa: BLE001 - any doubt means "not verified empty"
        return False


def cursor_records_commits(cursor: Any) -> bool:
    """Report whether a prior git-files cursor saw a readable HEAD or tree.

    A repository that had commits cannot become verified empty: its rows
    were published from those commits, and reading it as empty would
    retract them. The operator removes such a repository from the config.
    """
    if not isinstance(cursor, Mapping):
        return False
    return bool(cursor.get("head_sha") or cursor.get("tree_sha"))


#: Cursor key listing the paths an incomplete unscoped run found gone but
#: could not retract (issue #3301). Only a complete run emits retractions,
#: yet the incomplete run still advances the cursor tree, so a later complete
#: run no longer sees the deletion in its tree diff. Every unscoped run
#: re-reads these paths: one present again is re-read, one still gone is
#: reported stale, and a complete run retracts it and clears the entry.
OWED_RETRACTION_PATHS_KEY = "owed_retraction_paths"

#: Cursor key and value naming the file attributes a cursor's files were
#: emitted with. Absent = last-modified and created_at from the newest commit
#: touching the path, merges included; 2 = authored commits, merges skipped,
#: plus `created_commit_sha` (issue #3468). An unscoped run from a cursor at
#: any other version re-emits every current file once; deletions still come
#: from the saved tree.
FILE_ATTRS_VERSION_KEY = "file_attrs_version"
FILE_ATTRS_VERSION = 2

#: Most owed paths a cursor keeps. An incomplete run whose list would pass
#: it keeps the prior cursor and reports itself partial instead of dropping
#: any: a file first read by an incomplete run is in no record set the
#: runner keeps, so only its owed entry can retract it.
OWED_RETRACTION_PATHS_LIMIT = 10_000

#: Health reason for an incomplete run held back by that limit.
OWED_RETRACTIONS_LIMIT_REASON = (
    "owed retractions exceed the limit of {limit} paths; run reconcile"
)


def owed_retraction_paths(state: Mapping[str, Any] | None) -> set[str]:
    """Paths a cursor still owes a retraction for."""
    if not isinstance(state, Mapping):
        return set()
    return {
        str(path)
        for path in (state.get(OWED_RETRACTION_PATHS_KEY) or ())
        if path
    }


def cursor_owes_retractions(state: Mapping[str, Any] | None) -> bool:
    """True while a cursor holds retractions no complete run has made."""
    return bool(owed_retraction_paths(state))


#: Health reason fragment for a repository whose HEAD became unreadable
#: after an earlier run read commits from it.
HAD_COMMITS_REASON = "had commits before, now has no readable HEAD"


@dataclass
class GitFilesSource:
    """Ingest source for working-tree files in a git repository.

    Parameters
    ----------
    repo_root : Path
        Path to the working tree root (contains ``.git/``).
    repo : str
        Canonical short name used in node IDs (``file:<repo>:<path>``).
    include_globs : list[str] | None
        If set, only paths matching one of these globs are emitted.
    include_untracked : bool
        If true, include untracked paths discovered by a pruned working-tree
        walk as source files. This is opt-in because historical deployments
        used this source as a tracked-file inventory, but coding agents need
        untracked worktree context.
    exclude_globs : tuple[str, ...]
        Paths matching any of these globs are skipped. Defaults to
        common binary/lockfile extensions.
    """

    repo_root: Path
    repo: str
    include_globs: list[str] | None = None
    include_untracked: bool = False
    exclude_globs: tuple[str, ...] = DEFAULT_EXCLUDE_GLOBS
    scoped_paths: tuple[str, ...] | None = None
    # Per-run state — pipeline injects the previous-run value via
    # set_run_state() and reads back via get_run_state() (or .state).
    state: dict[str, Any] = field(default_factory=dict)
    # Tracks deletions discovered during the run; published via
    # ``stale_node_ids`` on the IngestResult.
    _stale: list[str] = field(default_factory=list)
    _applied_snapshot: dict[str, Any] | None = None
    _head_snapshot: dict[str, str] | None = None
    # Set when HEAD could not be read. The domain still emits nothing, as
    # before; the substrate adapter reads this to report the run as partial
    # instead of passing an empty crawl off as a complete one.
    _head_error: str | None = None
    # Set when HEAD was unreadable because the repository verifiably never
    # had commits (`head_is_verified_unborn`).
    _head_unborn: bool = False
    _working_tree_identities: dict[str, tuple[str, int | None]] = field(
        default_factory=dict
    )
    # Cached results per run so nodes() and edges() share work.
    _emit_cache: list[NodeSpec] | None = None
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    ownership_id: str = "git_graph.files"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        if isinstance(self.repo_root, str):
            self.repo_root = Path(self.repo_root).expanduser()

    # --- Source protocol ----------------------------------------------------

    def nodes(self) -> Iterable[NodeSpec]:
        if self._emit_cache is None:
            self._compute_emit()
        yield from self._emit_cache or []

    def edges(self) -> Iterable[EdgeSpec]:
        # No intra-module edges in this source.
        return iter(())

    def aliases(self) -> dict[str, str]:
        """Bare relative paths resolve to canonical file IDs."""
        if self._emit_cache is None:
            self._compute_emit()
        return {n.attrs["path"]: n.node_id for n in (self._emit_cache or [])}

    # --- State hooks (pipeline integration) ---------------------------------

    def set_run_state(self, state: Any) -> None:
        """Receive the previous run's persisted state (a dict or None)."""
        self.state = dict(state) if state else {}

    def get_run_state(self) -> dict[str, Any]:
        """Return the post-run state for the pipeline to persist."""
        return dict(self.state)

    @property
    def stale_node_ids(self) -> list[str]:
        return list(self._stale)

    # --- Implementation ------------------------------------------------------

    def _compute_emit(self) -> None:
        """Perform incremental or full crawl and populate ``_emit_cache``."""
        prior_tree = self.state.get("tree_sha")
        prior_dirty = set(self.state.get("dirty_paths") or [])
        scoped_paths = self._scoped_paths()
        # Paths an earlier incomplete run found gone and could not retract
        # (issue #3301). An unscoped run re-reads each one whatever the tree
        # diff says.
        owed = (
            owed_retraction_paths(self.state) if scoped_paths is None else set()
        )
        scoped_path_set = set(scoped_paths or ())
        tracked_for_dirty: set[str] | None = None
        untracked_for_dirty: set[str] | None = None

        # Per `add-pygit2-in-process-git-helpers`: read HEAD tree
        # sha in-process via libgit2 instead of fork+exec'ing `git`.
        # Dirty path discovery is intentionally NOT repo-wide libgit2 status:
        # it enumerates untracked directories before exclude filtering, so a
        # repo-local cache such as .claude/worktrees can dominate source-sync.
        try:
            head_snapshot = in_proc_head_snapshot(self.repo_root)
            cur_tree = head_snapshot.tree_sha
            self._head_snapshot = {
                "head_sha": head_snapshot.head_sha,
                "tree_sha": head_snapshot.tree_sha,
                "branch": head_snapshot.branch,
            }
        except (GitInvocationError, Exception) as exc:
            if head_is_verified_unborn(self.repo_root):
                # A valid repository with no commits yet has no tree to
                # read: an empty result is the true answer, not a failure.
                # The adapter still refuses it when a prior cursor saw
                # commits (see `cursor_records_commits`).
                logger.info(
                    "git repository %s has no commits yet; emitting nothing",
                    self.repo_root,
                )
                self._head_unborn = True
                self._emit_cache = []
                return
            self._head_error = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "git rev-parse failed for %s; emitting nothing: %s",
                self.repo_root,
                self._head_error,
            )
            self._emit_cache = []
            return
        dirty_strategy = "full"
        scoped_dirty: set[str] | None = None
        tracked_dirty_for_manifest: set[str] = set()
        if scoped_paths is not None:
            dirty_strategy = "scoped"
            scoped_dirty = self._scoped_dirty_in_crawl(
                in_proc_status_porcelain_paths(
                    self.repo_root,
                    scoped_paths,
                )
            )
            cur_dirty = (prior_dirty - scoped_path_set) | scoped_dirty
        else:
            if self.include_untracked:
                tracked_for_dirty = self._all_tracked_paths()
                untracked_for_dirty = self._all_untracked_paths(tracked_for_dirty)
            else:
                untracked_for_dirty = set()
            tracked_dirty_for_manifest = self._tracked_dirty_paths()
            cur_dirty = tracked_dirty_for_manifest | untracked_for_dirty
        if scoped_paths is None:
            untracked_dirty = set(untracked_for_dirty or ())
            dirty_manifest: dict[str, dict[str, str]] = {}
            for path in sorted(cur_dirty):
                try:
                    data = (self.repo_root / path).read_bytes()
                except OSError:
                    content_sha = "absent"
                    content_size = None
                else:
                    content_sha = hashlib.sha256(data).hexdigest()
                    content_size = len(data)
                self._working_tree_identities[path] = (
                    content_sha,
                    content_size,
                )
                dirty_manifest[path] = {
                    "kind": (
                        "tracked+untracked"
                        if path in tracked_dirty_for_manifest
                        and path in untracked_dirty
                        else "tracked"
                        if path in tracked_dirty_for_manifest
                        else "untracked"
                    ),
                    "sha256": content_sha,
                }
            self._applied_snapshot = {
                "tree_sha": cur_tree,
                "head_sha": head_snapshot.head_sha,
                "branch": head_snapshot.branch,
                "dirty_manifest": dirty_manifest,
            }
        tree_changed = prior_tree != cur_tree
        dirty_changed = prior_dirty != cur_dirty
        reread_all = (
            scoped_paths is None
            and bool(prior_tree)
            and self.state.get(FILE_ATTRS_VERSION_KEY) != FILE_ATTRS_VERSION
        )
        # A scoped run reads only its paths, so it cannot bring the cursor to
        # the current file attributes; it carries the prior version on.
        attrs_version = (
            {FILE_ATTRS_VERSION_KEY: FILE_ATTRS_VERSION}
            if scoped_paths is None
            else {FILE_ATTRS_VERSION_KEY: self.state[FILE_ATTRS_VERSION_KEY]}
            if FILE_ATTRS_VERSION_KEY in self.state
            else {}
        )

        # Short-circuit: nothing changed
        if not tree_changed and not dirty_changed and not owed and not reread_all:
            self._emit_cache = []
            self.state = {
                "tree_sha": cur_tree,
                "dirty_paths": sorted(cur_dirty),
                **attrs_version,
            }
            self.last_run_stats = self._stats(
                scoped_paths=scoped_paths,
                dirty_strategy=dirty_strategy,
                dirty_paths_checked=(
                    len(scoped_path_set)
                    if scoped_paths is not None else len(cur_dirty)
                ),
                dirty_paths_current=(
                    len(scoped_dirty or set())
                    if scoped_paths is not None else len(cur_dirty)
                ),
                tree_changed=False,
                dirty_changed=False,
                files_considered=0,
                files_emitted=0,
                stale_files=0,
                scope_status=(
                    "scoped_unchanged_noop"
                    if scoped_paths is not None else "full_unchanged_noop"
                ),
            )
            return

        # Full crawl when no prior tree, otherwise incremental. Scoped runs are
        # driven by the watcher-provided path frontier: resolve only those paths
        # instead of enumerating the full git index and filtering afterward.
        if scoped_paths is not None:
            tracked = self._tracked_scoped_paths(scoped_paths)
            untracked = (
                self._untracked_scoped_paths(scoped_paths, tracked)
                if self.include_untracked
                else set()
            )
            paths_to_emit = set(scoped_paths)
            deleted = paths_to_emit - tracked - untracked
            self._stale.extend(f"file:{self.repo}:{p}" for p in deleted)
        else:
            tracked = (
                tracked_for_dirty
                if tracked_for_dirty is not None
                else self._all_tracked_paths()
            )
            untracked = (
                untracked_for_dirty
                if untracked_for_dirty is not None
                else self._all_untracked_paths(tracked)
                if self.include_untracked
                else set()
            )
            if not prior_tree:
                paths_to_emit = tracked | untracked
            else:
                paths_to_emit, deleted = self._incremental_paths(prior_tree, cur_tree)
                self._stale.extend(f"file:{self.repo}:{p}" for p in deleted)
                # Also re-emit paths whose dirty status changed
                symdiff = cur_dirty ^ prior_dirty
                paths_to_emit = paths_to_emit | symdiff
                if self.include_untracked:
                    removed_untracked = {
                        p for p in (prior_dirty - cur_dirty) if p not in tracked
                    }
                    self._stale.extend(
                        f"file:{self.repo}:{p}" for p in removed_untracked
                    )

        if (not prior_tree or reread_all) and scoped_paths is None:
            # A first crawl, or a cursor written with older file attributes:
            # every current file. The tree diff above still found deletions.
            paths_to_emit = tracked | untracked
        else:
            paths_to_emit = paths_to_emit | (cur_dirty & set(scoped_paths or ()))

        owed_present: set[str] = set()
        if owed:
            # An owed path present again is re-read; one still gone, or one
            # the include/exclude rules no longer select, is reported stale
            # again. Tracked existence is read from the current tree, not
            # from `tracked`, which comes from the cached repository handle's
            # index (issue #3291). Untracked existence uses this run's walk.
            in_tree = self._blob_paths_in_tree(owed, tree_sha=cur_tree)
            owed_present = {
                p
                for p in owed
                if (p in in_tree or p in untracked) and self._included(p)
            }
            paths_to_emit = paths_to_emit | owed_present
            known_stale = set(self._stale)
            self._stale.extend(
                f"file:{self.repo}:{p}"
                for p in sorted(owed - owed_present)
                if f"file:{self.repo}:{p}" not in known_stale
            )

        # Apply include/exclude filters
        paths_to_emit = {p for p in paths_to_emit if self._included(p)}

        # Resolve to current paths only. Cleaned-up dirty paths may no
        # longer be tracked; deleted paths already went to stale. An owed
        # path found in the current tree stays even when the cached index
        # does not list it yet.
        paths_to_emit &= tracked | untracked | owed_present

        files_considered = len(paths_to_emit)
        stale_count = len(self._stale)
        if not paths_to_emit:
            self._emit_cache = []
            self.state = {
                "tree_sha": cur_tree,
                "dirty_paths": sorted(cur_dirty),
                **attrs_version,
            }
            self.last_run_stats = self._stats(
                scoped_paths=scoped_paths,
                dirty_strategy=dirty_strategy,
                dirty_paths_checked=(
                    len(scoped_path_set)
                    if scoped_paths is not None else len(cur_dirty)
                ),
                dirty_paths_current=(
                    len(scoped_dirty or set())
                    if scoped_paths is not None else len(cur_dirty)
                ),
                tree_changed=tree_changed,
                dirty_changed=dirty_changed,
                files_considered=files_considered,
                files_emitted=0,
                stale_files=stale_count,
                scope_status=(
                    "scoped_no_emit"
                    if scoped_paths is not None else "full_no_emit"
                ),
            )
            return

        # Batched metadata for the union of paths we need
        ls_tree = self._batch_ls_tree(paths_to_emit, tree_sha=cur_tree)
        if self.include_untracked:
            ls_tree.update(self._working_tree_meta(paths_to_emit - set(ls_tree)))
        last_mod = self._batch_last_modified(
            paths_to_emit,
            head_sha=head_snapshot.head_sha,
        )
        text_class = self._batch_text_classification(paths_to_emit, ls_tree)

        emit: list[NodeSpec] = []
        for path in sorted(paths_to_emit):
            meta = ls_tree.get(path)
            if meta is None:
                continue  # path not in HEAD tree (e.g., dirty-only edge case)
            attrs: dict[str, Any] = {
                "path": path,
                "repo": self.repo,
                "git_blob_sha": meta.blob_sha,
                "size_bytes": meta.size_bytes,
                "language": language_for_path(path),
                "is_text": text_class.get(path, True),
                "is_dirty": path in cur_dirty or path in untracked,
            }
            history = last_mod.get(path)
            if history is not None:
                # Authored commits, not the merges that brought them to the
                # mainline (issue #3468): see `PathHistory`.
                attrs["last_modified_commit_sha"] = history.last_modified_sha
                attrs["last_modified_at"] = history.last_modified_at
                if history.created_sha is not None:
                    attrs["created_commit_sha"] = history.created_sha
                    attrs["created_at"] = history.created_at
            emit.append(
                NodeSpec(
                    node_id=f"file:{self.repo}:{path}",
                    subtype="source_file",
                    label=path,
                    attrs=attrs,
                    source=self.ownership_id,
                )
            )

        self._emit_cache = emit
        self.state = {
            "tree_sha": cur_tree,
            "dirty_paths": sorted(cur_dirty),
            **attrs_version,
        }
        self.last_run_stats = self._stats(
            scoped_paths=scoped_paths,
            dirty_strategy=dirty_strategy,
            dirty_paths_checked=(
                len(scoped_path_set)
                if scoped_paths is not None else len(cur_dirty)
            ),
            dirty_paths_current=(
                len(scoped_dirty or set())
                if scoped_paths is not None else len(cur_dirty)
            ),
            tree_changed=tree_changed,
            dirty_changed=dirty_changed,
            files_considered=files_considered,
            files_emitted=len(emit),
            stale_files=stale_count,
            scope_status=(
                "scoped_emit"
                if scoped_paths is not None else "full_emit"
            ),
        )

    def _stats(
        self,
        *,
        scoped_paths: tuple[str, ...] | None,
        dirty_strategy: str,
        dirty_paths_checked: int,
        dirty_paths_current: int,
        tree_changed: bool,
        dirty_changed: bool,
        files_considered: int,
        files_emitted: int,
        stale_files: int,
        scope_status: str,
    ) -> dict[str, Any]:
        return {
            "scope_status": scope_status,
            "scope_keys": list(scoped_paths or ()),
            "dirty_strategy": dirty_strategy,
            "dirty_paths_checked": dirty_paths_checked,
            "dirty_paths_current": dirty_paths_current,
            "tree_changed": tree_changed,
            "dirty_changed": dirty_changed,
            "files_considered": files_considered,
            "files_emitted": files_emitted,
            "stale_files": stale_files,
        }

    # --- Filter helpers ------------------------------------------------------

    def _included(self, path: str) -> bool:
        return path_included_by_globs(
            path,
            include_globs=self.include_globs,
            exclude_globs=self.exclude_globs,
        )

    def _scoped_paths(self) -> tuple[str, ...] | None:
        if self.scoped_paths is None:
            return None
        paths = tuple(
            dict.fromkeys(
                key
                for key in (
                    normalize_scope_key(path) for path in self.scoped_paths
                )
                if key is not None
            )
        )
        return tuple(path for path in paths if self._included(path))

    # --- Batched git calls ---------------------------------------------------

    def _blob_paths_in_tree(
        self, paths: Iterable[str], *, tree_sha: str,
    ) -> set[str]:
        """The paths that name a file (a blob) in the tree ``tree_sha``.

        Looks each path up in the tree one at a time; it does not walk the
        tree. A path that now names a directory or a gitlink is not a file:
        it is left out, so an owed retraction for it stays gone and is
        retracted instead of reaching a size read that a tree cannot answer.
        """
        import pygit2

        repo = open_repository_at(self.repo_root)
        tree = repo[tree_sha].peel(pygit2.Tree)
        found: set[str] = set()
        for path in paths:
            try:
                entry = tree[path]
            except KeyError:
                continue
            if entry.type_str == "blob":
                found.add(path)
        return found

    def _all_tracked_paths(self) -> set[str]:
        # Per `add-pygit2-in-process-git-helpers`: read the git
        # index in-process instead of fork+exec'ing `git ls-files`.
        return {p for p in in_proc_list_tracked(self.repo_root) if self._included(p)}

    def _tracked_scoped_paths(self, paths: Iterable[str]) -> set[str]:
        return set(in_proc_ls_tree(self.repo_root, paths))

    def _all_untracked_paths(self, tracked: set[str]) -> set[str]:
        """Return untracked files without asking git to scan excluded trees."""
        return pruned_untracked_worktree_paths(
            self.repo_root,
            tracked_paths=tracked,
            include_globs=self.include_globs,
            exclude_globs=self.exclude_globs,
        )

    def _tracked_dirty_paths(self) -> set[str]:
        return tracked_dirty_worktree_paths(
            self.repo_root,
            include_globs=self.include_globs,
            exclude_globs=self.exclude_globs,
        )

    def _untracked_scoped_paths(
        self,
        paths: Iterable[str],
        tracked: set[str],
    ) -> set[str]:
        # Exactly what the full crawl's pruned walk would admit (#3259).
        return scoped_untracked_worktree_paths(
            self.repo_root,
            paths,
            tracked_paths=tracked,
            include_globs=self.include_globs,
            exclude_globs=self.exclude_globs,
        )

    def _scoped_dirty_in_crawl(self, dirty: set[str]) -> set[str]:
        """Keep the dirty paths a full crawl would also count as dirty.

        Path-scoped status reports untracked and ignored files too. The full
        crawl counts a path dirty only when it is tracked (index or HEAD) or
        is an untracked file its pruned walk admits under include_untracked.
        Anything else left in the scoped frontier would make the next full
        run see a dirty-set change -- and, under include_untracked, a stale
        id -- for a file it never emitted.
        """
        if not dirty:
            return set()
        tracked = in_proc_index_paths(self.repo_root, dirty) | set(
            in_proc_ls_tree(self.repo_root, dirty)
        )
        untracked = (
            self._untracked_scoped_paths(dirty - tracked, tracked)
            if self.include_untracked
            else set()
        )
        return {path for path in dirty if path in tracked or path in untracked}

    def _incremental_paths(
        self, prior_tree: str, cur_tree: str
    ) -> tuple[set[str], set[str]]:
        """Return (added_or_modified_paths, deleted_paths) since prior_tree.

        Per `add-pygit2-in-process-git-helpers`: tree-to-tree diff
        runs in-process via libgit2. Renames/copies are split into
        delete(old) + add(new) by the helper, matching the legacy
        status-byte parser's behavior.
        """
        try:
            return in_proc_diff_name_status(self.repo_root, prior_tree, cur_tree)
        except (KeyError, Exception) as e:
            # prior_tree may no longer be reachable (force-push,
            # gc); fall back to a full crawl.
            logger.warning(
                "Unable to diff against prior tree %s (%s); falling back to full crawl",
                prior_tree,
                e,
            )
            return self._all_tracked_paths(), set()

    def _batch_ls_tree(
        self,
        paths: Iterable[str],
        *,
        tree_sha: str | None = None,
    ) -> dict[str, _FileMeta]:
        """Return blob_sha + size per path, looked up from HEAD's tree.

        Per `add-pygit2-in-process-git-helpers`: in-process tree walk
        via libgit2; the legacy `git ls-tree -r -l HEAD` shelled out
        per call (~15 ms fork+exec). is_text classification stays
        extension-based here (matching legacy); refining via
        content-sniff is out of scope.
        """
        result: dict[str, _FileMeta] = {}
        ls = in_proc_ls_tree(self.repo_root, paths, tree_sha=tree_sha)
        for path, (blob_sha, size) in ls.items():
            result[path] = _FileMeta(
                path=path,
                blob_sha=blob_sha,
                size_bytes=size,
                # is_text seeded by extension; refined below if uncertain
                is_text=Path(path).suffix.lower()
                not in {
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".svg",
                    ".pdf",
                    ".bin",
                    ".ico",
                    ".woff",
                    ".woff2",
                    ".ttf",
                    ".otf",
                    ".zip",
                    ".tar",
                    ".gz",
                    ".so",
                    ".dylib",
                    ".dll",
                    ".wasm",
                },
            )
        return result

    def _working_tree_meta(self, paths: Iterable[str]) -> dict[str, _FileMeta]:
        result: dict[str, _FileMeta] = {}
        for path in paths:
            identity = self._working_tree_identities.get(path)
            if identity is None:
                abs_path = self.repo_root / path
                if not abs_path.is_file():
                    continue
                try:
                    data = abs_path.read_bytes()
                except OSError:
                    continue
                identity = (hashlib.sha256(data).hexdigest(), len(data))
            content_sha, size_bytes = identity
            if content_sha == "absent" or size_bytes is None:
                continue
            result[path] = _FileMeta(
                path=path,
                blob_sha=f"working-tree:{content_sha[:40]}",
                size_bytes=size_bytes,
                is_text=Path(path).suffix.lower()
                not in {
                    ".png",
                    ".jpg",
                    ".jpeg",
                    ".gif",
                    ".svg",
                    ".pdf",
                    ".bin",
                    ".ico",
                    ".woff",
                    ".woff2",
                    ".ttf",
                    ".otf",
                    ".zip",
                    ".tar",
                    ".gz",
                    ".so",
                    ".dylib",
                    ".dll",
                    ".wasm",
                },
            )
        return result

    def _batch_last_modified(
        self,
        paths: Iterable[str],
        *,
        head_sha: str | None = None,
    ) -> dict[str, PathHistory]:
        """For each path, its last-modified and created commit, skipping
        merges (`in_proc_path_history`): one in-process commit walk via
        libgit2 with the changed files streamed from one `git log`.
        """
        return in_proc_path_history(
            self.repo_root,
            paths,
            head_sha=head_sha,
        )

    def _batch_text_classification(
        self, paths: Iterable[str], ls_tree: dict[str, _FileMeta]
    ) -> dict[str, bool]:
        """Refine is_text for files whose extension is ambiguous.

        Files whose extension is in BINARY_EXTENSIONS have is_text=False
        seeded by `_batch_ls_tree`; everything else is True. We do not
        currently invoke a second git call for refinement — extension
        is sufficient for the common cases. (The spec allows an
        optional fourth call here; not implemented in this pass.)
        """
        wanted = set(paths)
        return {p: meta.is_text for p, meta in ls_tree.items() if p in wanted}
