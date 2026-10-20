"""GitFilesSource — substrate adapter for tracked-file nodes.

Wraps the domain `okg.modules.git_graph.files_source.GitFilesSource`
domain logic (the three-batched-git-invocations crawl + the
incremental tree_sha / dirty_paths state machine) in the
substrate `SourceAdapter` Protocol.

Translation:
  - domain `state: dict` ↔ substrate `next_cursor: dict`
  - domain `NodeSpec` → substrate `NodeFact`
  - domain `EdgeSpec` → substrate `EdgeFact`
  - domain `stale_node_ids` (deletions) → substrate retraction
    `NodeFact(op='R')` rows when `mode in {reconcile, scope_complete}`.

Profile: `discovery_crawl` (the existing substrate profile that
covers "walk a scope, diff by content hash, absence implies
retraction"). No new profile needed.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import (
    SOURCE_RUN_PARTIAL_STATUS,
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

_RECORD_SET_CHRONOS_PROOF_SCHEMA = "okg.source_runner.record_set_chronos_proof.v1"
# Cursor key: the branch the last complete run linked every file to.
_LINKS_BRANCH_KEY = "files_linked_branch"


@functools.lru_cache(maxsize=1)
def _owed_work_aware_probe_class() -> type:
    """GitScopeProbe that reports a change while retractions are owed.

    An incomplete run that found a file gone advances the cursor tree and the
    applied probe token, so the next complete run would read as unchanged and
    the runner could skip the very run that must retract the file (issue
    #3301). Built lazily to keep the substrate import light.
    """
    from okg.substrate.library.sources._git_graph_files_emit import (
        cursor_owes_retractions,
    )
    from okg.substrate.library.sources.change_probe import ProbeVerdict
    from okg.substrate.library.sources.git_scope_probe import GitScopeProbe

    class OwedWorkAwareGitScopeProbe(GitScopeProbe):
        def probe(self, *, cursor, sync_scope):  # noqa: ANN001, ANN201
            verdict = super().probe(cursor=cursor, sync_scope=sync_scope)
            if not verdict.changed and cursor_owes_retractions(cursor):
                return ProbeVerdict(
                    changed=True,
                    token=verdict.token,
                    reason="owed_retractions_pending",
                )
            return verdict

    return OwedWorkAwareGitScopeProbe


@dataclass
class GitFilesSource:
    """Substrate adapter for git-tracked source files.

    Constructor kwargs mirror the domain emitter exactly so
    `source_registry.yaml` configs port verbatim.
    """

    repo: str
    repo_root: Path | str
    include_globs: Optional[list[str]] = None
    include_untracked: bool = False
    exclude_globs: Optional[tuple[str, ...]] = None
    source_name: Optional[str] = None
    graph_branch: str = "default"
    # Checkout acquisition (code-repos-auto-acquire): when manage_checkout is set
    # with a url, this source OWNS materialising repo_root (clone-if-missing /
    # ff-pull) via ensure_checkout(), which the runner calls BEFORE the
    # change-probe reads HEAD. In a code_repos expansion only git-files carries
    # manage_checkout, so a repo's checkout has a single owner.
    url: Optional[str] = None
    tracked_branch: Optional[str] = None
    manage_checkout: bool = False

    name: str = "git_graph_files"
    profile: str = "discovery_crawl"
    # Incremental-source contract (issue #323): churn-immune git scope probe over
    # the whole working tree (tracked + dirty + untracked) — a sound superset of
    # this source's read scope.
    change_probe_kind = "git_scope"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.repo_root, str):
            self.repo_root = Path(self.repo_root).expanduser()
        self._acquire_health: Optional[SourceHealth] = None
        self.change_probe = self._build_change_probe()

    @property
    def probe_short_circuit_safe(self) -> bool:
        """Only a real repository can provide the exact Git-bound snapshot.

        Ad-hoc callers may use this source against an ordinary directory.  The
        reader still works there, but the GitScopeProbe cannot bind its output
        to a durable applied token, so those runs must always read.
        """
        from okg.substrate.library.sources._git_helpers import in_proc_workdir

        root = Path(self.repo_root).resolve()
        try:
            return in_proc_workdir(root) == root
        except Exception:  # noqa: BLE001 - unavailable Git authority fails closed
            return False

    def ensure_checkout(self) -> Optional[SourceHealth]:
        """Materialise repo_root before the change-probe reads HEAD.

        Owner-only: a no-op unless manage_checkout is set with a url. Never
        raises (clone_or_pull returns a structured outcome); a non-fast-forward
        maps to acquire_stale (the existing tree is still indexed) and a failed
        clone to acquire_failed (run() then emits zero facts instead of crashing
        the domain crawl on a missing repo).
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

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources import _git_graph_files_emit
        from okg.substrate.library.sources._git_helpers import (
            DEFAULT_EXCLUDE_GLOBS,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        effective_excludes = (
            tuple(self.exclude_globs)
            if self.exclude_globs is not None
            else DEFAULT_EXCLUDE_GLOBS
        )
        return _owed_work_aware_probe_class()(
            repo_root=Path(self.repo_root),
            config={
                "repo": self.repo,
                "include_globs": sorted(self.include_globs or []),
                "exclude_globs": sorted(effective_excludes),
                "include_untracked": self.include_untracked,
                # Emission depends on the citation url config
                # (citation-source-urls) — a url change must re-emit.
                "url": self.url or "",
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
                _git_graph_files_emit,
            ),
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        """Drive the domain crawl, translate to NodeFact/EdgeFact."""
        # Degrade, never crash: a MANAGED checkout that failed to materialise has
        # nothing to index. Emit zero facts with the acquisition health rather
        # than letting the domain crawl raise on a missing repo and cascade-fail
        # the whole ingest layer. Only guards the managed path — an unmanaged
        # source keeps its existing behaviour on a missing repo_root.
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
        # Lazy-imported to keep substrate import light when no
        # caller invokes the adapter.
        from okg.substrate.library.sources._git_graph_files_emit import (
            HAD_COMMITS_REASON,
            OWED_RETRACTION_PATHS_KEY,
            OWED_RETRACTION_PATHS_LIMIT,
            OWED_RETRACTIONS_LIMIT_REASON,
            GitFilesSource as DomainGitFilesSource,
            cursor_records_commits,
            normalize_scope_key,
            owed_retraction_paths,
        )
        from okg.substrate.library.sources._git_helpers import (
            DEFAULT_EXCLUDE_GLOBS,
            path_included_by_globs,
        )

        excludes = (
            tuple(self.exclude_globs)
            if self.exclude_globs is not None
            else DEFAULT_EXCLUDE_GLOBS
        )
        raw_scope_keys = tuple(
            dict.fromkeys(
                key for key in (sync_scope or {}).get("scope_keys", ()) if key
            )
        )
        normalized_scope_keys = tuple(
            normalize_scope_key(key) for key in raw_scope_keys
        )
        # `./a` and `a//b` name `a`; a key leaving the repository names none.
        scope_keys = tuple(
            dict.fromkeys(key for key in normalized_scope_keys if key is not None)
        )
        scope_keys_escaping_repo = sum(
            1 for key in normalized_scope_keys if key is None
        )
        # Scoped by the keys the caller sent, even when none survives
        # normalization: such a run reads nothing rather than everything.
        scoped = mode in {"scope_incomplete", "scope_complete"} and bool(
            raw_scope_keys,
        )
        # A scope key narrows the source's own paths; it never widens them
        # (issue #3259). The domain crawl intersects the scoped paths with the
        # configured include and exclude globs, so a watcher event for a file
        # outside those paths reads nothing, emits nothing and retracts
        # nothing. The count is reported, not raised: watchers send events for
        # every file in the checkout.
        scope_keys_outside_source_paths = (
            scope_keys_escaping_repo
            + sum(
                1
                for key in scope_keys
                if not path_included_by_globs(
                    key,
                    include_globs=self.include_globs,
                    exclude_globs=excludes,
                )
            )
            if scoped else 0
        )
        completed_scope = not scoped and mode in {"reconcile", "scope_complete"}
        scoped_path_complete = scoped and mode == "scope_complete"
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        has_inline_record_set = (
            isinstance(cursor, dict)
            and cursor.get("record_set") is not None
        )
        has_verified_external_record_set = (
            completed_scope
            and self._has_verified_external_record_set(cursor)
        )
        can_carry_completed_scope = (
            completed_scope
            and (
                has_inline_record_set
                or has_verified_external_record_set
            )
        )
        # Retractions earlier incomplete runs could not make (issue #3301).
        # Read from the cursor itself: a complete run that cannot carry the
        # prior record set hands the crawl no prior state.
        prior_owed = owed_retraction_paths(cursor)
        can_probe_completed_scope = completed_scope and can_carry_completed_scope
        # Honor the persisted cursor when the adapter can preserve the
        # SourceRun record-set contract. Complete runs may short-circuit
        # unchanged tree state only if they carry the prior full-source
        # footprint and can either merge an inline prior record set or preserve
        # verified external record-set authority. Otherwise a delta emission
        # would look like "everything else was retracted".
        state = (
            source_state_without_runner_keys(cursor)
            if cursor
            and isinstance(cursor, dict)
            and (
                mode in {"cursor", "scope_incomplete"}
                or can_probe_completed_scope
            )
            else {}
        )
        domain = DomainGitFilesSource(
            repo_root=Path(self.repo_root),
            repo=self.repo,
            include_globs=self.include_globs,
            include_untracked=self.include_untracked,
            exclude_globs=excludes,
            scoped_paths=scope_keys if scoped else None,
            state=state,
        )

        # Materialize the crawl. nodes() populates _emit_cache;
        # edges() reads from it. Drain in this order so domain
        # invariants hold.
        nodes = list(domain.nodes())
        edges = list(domain.edges())
        stale_ids = list(domain.stale_node_ids or ())
        # Every file's member_of link names the branch it was read on, but
        # the tree diff from the cursor sees only changed files. After a
        # switch to another branch an unchanged file would keep its link to
        # the old branch, and an unchanged tree would leave the old branch
        # record in place with no file linked to the new one (issue #3780).
        # A complete run therefore reads the whole tree again: the full
        # inventory below replaces the prior footprint, and the runner's
        # diff retracts the old branch record and its links. The cursor keeps
        # the branch the last complete run linked the files to, so a cursor or
        # scoped run in between cannot hide the switch from it.
        switched_branch = (
            domain._head_snapshot["branch"]
            if domain._head_snapshot is not None else None
        )
        links_branch = next(
            (
                value
                for value in (
                    cursor.get(_LINKS_BRANCH_KEY),
                    cursor.get("branch_name"),
                )
                if isinstance(value, str) and value
            ),
            None,
        ) if isinstance(cursor, dict) else None
        if (
            completed_scope
            and state.get("tree_sha")
            and links_branch is not None
            and switched_branch
            and str(switched_branch) != links_branch
        ):
            state = {}
            domain = DomainGitFilesSource(
                repo_root=Path(self.repo_root),
                repo=self.repo,
                include_globs=self.include_globs,
                include_untracked=self.include_untracked,
                exclude_globs=excludes,
                scoped_paths=None,
                state=state,
            )
            nodes = list(domain.nodes())
            edges = list(domain.edges())
            stale_ids = list(domain.stale_node_ids or ())
        if domain._head_unborn and cursor_records_commits(cursor):
            # A repository an earlier run read commits from cannot become
            # verified empty; reading it so would retract every row.
            domain._head_error = HAD_COMMITS_REASON
        if domain._head_error is not None:
            # HEAD could not be read, so the crawl saw nothing. An empty crawl
            # is not an empty tree: report the run as partial, emit no facts
            # and keep the prior cursor, so neither this source's own full run
            # nor a factory composing it reads the gap as every file deleted.
            return SourceRun(
                facts=iter(()),
                completed_scope=False,
                next_cursor=dict(cursor or {}),
                run_mode=mode,
                health=SourceHealth(
                    status=SOURCE_RUN_PARTIAL_STATUS,
                    reason=(
                        f"git HEAD read failed for {self.repo}: "
                        f"{domain._head_error}"
                    )[:500],
                ),
            )
        external_record_set_authority = (
            completed_scope
            and has_record_set_authority
            and not can_carry_completed_scope
        )
        dirty_frontier_repaired = False
        if (
            external_record_set_authority
            and self.include_untracked
            and not (nodes or edges or stale_ids)
        ):
            # A persisted tree_sha + dirty_paths cursor can be unchanged even
            # when the external record-set authority is missing one of those
            # dirty working-tree paths. Re-emit only the dirty frontier as a
            # replacement slice so downstream sources can trust source_file
            # ownership without forcing a full git inventory.
            repair_scope = tuple(
                sorted(
                    str(path).replace("\\", "/").lstrip("/")
                    for path in (domain.state.get("dirty_paths") or ())
                    if path
                )
            )
            if repair_scope:
                domain = DomainGitFilesSource(
                    repo_root=Path(self.repo_root),
                    repo=self.repo,
                    include_globs=self.include_globs,
                    include_untracked=self.include_untracked,
                    exclude_globs=excludes,
                    scoped_paths=repair_scope,
                    state={},
                )
                nodes = list(domain.nodes())
                edges = list(domain.edges())
                stale_ids = list(domain.stale_node_ids or ())
                dirty_frontier_repaired = True
        # A crawl that starts with no saved tree enumerates every file the
        # CURRENT include/exclude rules select. Its record set is therefore
        # the whole footprint, not a slice to merge into the prior one: a
        # merge would keep every prior record this crawl no longer emits --
        # a file a new exclude glob now matches (issue #2483) -- and the
        # runner's record-set diff would never retract it. An authoritative
        # replay, which a registry config change forces, arrives this way.
        full_inventory = (
            not scoped
            and not state.get("tree_sha")
            and not dirty_frontier_repaired
        )
        # Use the HEAD commit + branch captured by the domain before its
        # immutable tree walk. The
        # GitBranch node we emit below is what makes HEAD movement
        # a first-class observable to the publisher: when `git reset
        # --hard <merge-commit>` advances HEAD to a commit whose
        # tree is identical to the parent's, NO SourceFile node
        # facts change (tree_sha cursor stays put), so without this
        # emission the publisher would see no new work and skip
        # the cycle. With it, head_commit_sha is a fact attribute
        # that changes per HEAD advance, the publisher projects the
        # change, and the generation advances.
        applied_snapshot = domain._applied_snapshot
        head_snapshot = domain._head_snapshot
        head_sha = (
            str(head_snapshot["head_sha"])
            if head_snapshot is not None else ""
        )
        branch_name = (
            str(head_snapshot["branch"])
            if head_snapshot is not None else "HEAD"
        )

        revision = {
            "tree_sha": domain.state.get("tree_sha", ""),
            "head_sha": head_sha,
        }
        prior_head_sha = (
            cursor.get("head_sha")
            if isinstance(cursor, dict)
            and isinstance(cursor.get("head_sha"), str)
            else None
        )
        prior_branch_name = (
            cursor.get("branch_name")
            if isinstance(cursor, dict)
            and isinstance(cursor.get("branch_name"), str)
            else None
        )
        head_changed = bool(head_sha and head_sha != prior_head_sha)
        branch_changed = bool(
            branch_name and branch_name != prior_branch_name
        )
        branch_node_id = f"branch:{self.repo}:{branch_name}"
        emit_branch_anchor = bool(
            head_sha
            and (
                head_changed
                or branch_changed
                or nodes
                or stale_ids
                or completed_scope
                or scoped_path_complete
            )
        )
        has_git_tree = bool(revision["tree_sha"])
        # An unchanged run over a verified external record set builds a
        # branch-only record set too: the replacement slice below merges it
        # into the external prior, as for a changed run. Without one the
        # runner read the branch record alone as the whole footprint and
        # retracted every file (issue #3666).
        can_build_complete_record_set = (
            completed_scope
            and has_git_tree
            and (
                not has_record_set_authority
                or external_record_set_authority
                or has_inline_record_set
                or has_verified_external_record_set
                or bool(nodes or stale_ids)
            )
        )
        can_build_scoped_record_set = (
            scoped_path_complete
            and has_git_tree
            and has_record_set_authority
        )
        record_set = (
            self._record_set_from_nodes(
                nodes,
                revision=revision,
                branch_node_id=branch_node_id,
                branch_name=branch_name,
            )
            if can_build_complete_record_set or can_build_scoped_record_set
            else None
        )
        def _replacement_keys() -> set[str]:
            replacement_keys = {
                str(node.attrs.get("path", node.node_id))
                for node in nodes
            }
            replacement_keys.update(
                self._record_keys_from_stale_ids(stale_ids),
            )
            if branch_name:
                replacement_keys.add(f"__branch__:{branch_name}")
            return replacement_keys

        record_set_replacement_keys: set[str] | None = None
        if full_inventory:
            # The complete footprint as it stands; the runner diffs it
            # against the prior authority, inline or external.
            pass
        elif (
            completed_scope and has_inline_record_set and record_set is not None
        ):
            record_set = self._record_set_with_prior(
                cursor,
                current=record_set,
                replacement_keys=_replacement_keys(),
            )
            completed_scope = record_set is not None
        elif (
            completed_scope
            and has_verified_external_record_set
            and record_set is not None
        ):
            record_set_replacement_keys = _replacement_keys()
        if scoped_path_complete and record_set is not None:
            record_set_replacement_keys = {
                str(node.attrs.get("path", node.node_id))
                for node in nodes
            }
            record_set_replacement_keys.update(
                self._record_keys_from_stale_ids(stale_ids),
            )
            if branch_name:
                record_set_replacement_keys.add(f"__branch__:{branch_name}")

        # Owed retractions (issue #3301). A complete unscoped run settles
        # every one. A crawl from the cursor re-read each owed path, so it
        # either emitted the path again or reports it stale and retracts it
        # below. A full inventory did not read the cursor: it retracts each
        # owed path it did not emit explicitly, because a file first read by
        # an incomplete run is in no record set the runner keeps, so the
        # runner's diff cannot retract it. Any other run settles the owed
        # paths it emitted, and those it retracted through a scoped
        # record-set slice; an unscoped one also owes every path it found
        # gone, since it cannot retract them itself.
        stale_paths = self._record_keys_from_stale_ids(stale_ids)
        emitted_paths = {
            str(node.attrs.get("path", node.node_id)) for node in nodes
        }
        if completed_scope and not scoped:
            if full_inventory:
                known_stale = set(stale_ids)
                stale_ids.extend(
                    f"file:{self.repo}:{path}"
                    for path in sorted(prior_owed - emitted_paths)
                    if f"file:{self.repo}:{path}" not in known_stale
                )
            owed: set[str] = set()
        else:
            owed = prior_owed - emitted_paths
            if scoped_path_complete and record_set is not None:
                owed -= stale_paths
            if not scoped:
                owed |= stale_paths
        if len(owed) > OWED_RETRACTION_PATHS_LIMIT:
            # Never drop or truncate the list: a dropped path first read by
            # an incomplete run is in no record set the runner keeps, so it
            # would stay in the graph for good. Hold the prior cursor
            # instead: this run's deletions stay in the tree diff from it,
            # and the next complete run retracts them with the listed ones.
            self.last_run_stats = {
                **(getattr(domain, "last_run_stats", {}) or {}),
                "run_mode": mode,
                "completed_scope": False,
                "owed_retraction_paths": len(owed),
                "owed_retractions_held": True,
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=False,
                next_cursor=dict(cursor or {}),
                run_mode=mode,
                health=SourceHealth(
                    status=SOURCE_RUN_PARTIAL_STATUS,
                    reason=OWED_RETRACTIONS_LIMIT_REASON.format(
                        limit=OWED_RETRACTION_PATHS_LIMIT,
                    ),
                ),
            )

        branch_edge_count = sum(
            1 for node in nodes if getattr(node, "subtype", None) == "source_file"
        )
        self.last_run_stats = {
            **(getattr(domain, "last_run_stats", {}) or {}),
            "run_mode": mode,
            "scoped": scoped,
            "completed_scope": completed_scope,
            "scoped_path_complete": scoped_path_complete,
            "scope_keys_outside_source_paths": scope_keys_outside_source_paths,
            "branch_name": branch_name,
            "head_changed": head_changed,
            "branch_changed": branch_changed,
            "branch_anchor_emitted": emit_branch_anchor,
            "emitted_nodes": len(nodes) + (1 if emit_branch_anchor else 0),
            "emitted_edges": len(edges) + branch_edge_count,
            "owed_retraction_paths": len(owed),
        }
        # Start from a base without the owed key, so a settled list cannot
        # survive from a base built on the prior cursor and retract twice.
        base_state = {
            key: value
            for key, value in dict(domain.state).items()
            if key != OWED_RETRACTION_PATHS_KEY
        }
        next_state = {
            **base_state,
            **({OWED_RETRACTION_PATHS_KEY: sorted(owed)} if owed else {}),
            **({"head_sha": head_sha} if head_sha else {}),
            **({"branch_name": branch_name} if branch_name else {}),
        }
        # Only a complete run relinks every file (issue #3780).
        files_linked_to = (
            branch_name if completed_scope and branch_name else links_branch
        )
        if files_linked_to:
            next_state[_LINKS_BRANCH_KEY] = files_linked_to

        def _facts() -> Iterator[Any]:
            # Emit the GitBranch node first when HEAD changed or this
            # run emits file facts that need a branch anchor.
            if emit_branch_anchor:
                yield NodeFact(
                    node_id=branch_node_id,
                    subtype="git_branch",
                    attrs={
                        "label": branch_name,
                        "name": branch_name,
                        "repo": self.repo,
                        "head_commit_sha": head_sha,
                    },
                    source_record_id={
                        "repo": self.repo,
                        "branch": branch_name,
                    },
                    source_revision=revision,
                    op="I",
                )
            from okg.substrate.library.sources._source_urls import (
                citation_label,
                repo_file_url,
            )
            for n in nodes:
                attrs = {"label": n.label, **n.attrs}
                # Source-supplied citation (source-supplied-search-
                # citations): label always names repo/path; the url is
                # emitted only when it truthfully links the ingested
                # revision — configured remote on a supported host, a
                # known HEAD sha, and a file that is clean vs HEAD.
                if n.subtype == "source_file":
                    cite_label = citation_label(
                        self.repo, n.attrs.get("path"),
                    )
                    if cite_label:
                        attrs["citation_label"] = cite_label
                        if self.url and head_sha and not n.attrs.get(
                            "is_dirty",
                        ):
                            file_url = repo_file_url(
                                self.url, head_sha, n.attrs.get("path"),
                            )
                            if file_url:
                                attrs["citation_url"] = file_url
                yield NodeFact(
                    node_id=n.node_id,
                    subtype=n.subtype,
                    attrs=attrs,
                    source_record_id={
                        "repo": self.repo,
                        "path": n.attrs.get("path", n.node_id),
                    },
                    source_revision=revision,
                    op="I",
                )
                # Tie every source_file to the branch it lives on.
                # Without this edge, source_file nodes are
                # structurally floating — present in the graph
                # but unattached to either the branch or the
                # commit that introduced them. The natural
                # `source_file member_of git_branch` edge fills
                # the gap.
                if head_sha and n.subtype == "source_file":
                    yield EdgeFact(
                        src=n.node_id,
                        dst=branch_node_id,
                        edge_type="member_of",
                        attrs={},
                        provenance="explicit",
                        confidence=1.0,
                        source_record_id={
                            "repo": self.repo,
                            "branch": branch_name,
                        },
                        source_revision=revision,
                        op="I",
                    )
            for e in edges:
                yield EdgeFact(
                    src=e.src,
                    dst=e.dst,
                    edge_type=e.edge_type,
                    attrs=dict(e.attrs),
                    provenance=e.provenance,
                    confidence=(e.confidence if e.confidence is not None else 1.0),
                    source_record_id={"repo": self.repo},
                    source_revision=revision,
                    op="I",
                )
            # Broad completed-scope runs can still publish adapter-authored
            # stale IDs. Scoped path-complete runs declare a record-set
            # replacement slice instead; SourceRunner owns those retractions.
            if completed_scope:
                for stale_id in stale_ids:
                    yield NodeFact(
                        node_id=stale_id,
                        subtype="source_file",
                        attrs={},
                        source_record_id={
                            "repo": self.repo,
                            "node_id": stale_id,
                        },
                        source_revision=revision,
                        op="R",
                    )

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=(
                preserve_cursor_record_set_authority(next_state, cursor)
                if completed_scope and has_record_set_authority and record_set is None
                else next_state
            ),
            applied_raw_token=(
                self.change_probe.token_for_snapshot(**applied_snapshot)
                if not scoped and applied_snapshot is not None else None
            ),
            applied_token_scope=(
                "global"
                if not scoped and applied_snapshot is not None
                else "unavailable"
            ),
            applied_token_authority=(
                "exact"
                if not scoped and applied_snapshot is not None
                else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
        )

    def _has_verified_external_record_set(
        self,
        cursor: Optional[dict[str, Any]],
    ) -> bool:
        if not isinstance(cursor, dict):
            return False
        if cursor.get("record_set_ref") is None:
            return False
        summary = cursor.get("record_set_summary")
        if not isinstance(summary, dict):
            return False
        proof = cursor.get("record_set_chronos_proof")
        if not (
            isinstance(proof, dict)
            and proof.get("ok") is True
            and proof.get("schema") == _RECORD_SET_CHRONOS_PROOF_SCHEMA
        ):
            return False
        if not self.source_name:
            return False
        return (
            proof.get("source_name") == self.source_name
            and proof.get("graph_branch") == self.graph_branch
            and summary.get("source_name") == self.source_name
            and summary.get("graph_branch") == self.graph_branch
            and summary.get("chronos_graph_verified") is True
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

    def _record_keys_from_stale_ids(self, stale_ids: list[str]) -> set[str]:
        prefix = f"file:{self.repo}:"
        return {
            stale_id[len(prefix):]
            for stale_id in stale_ids
            if stale_id.startswith(prefix)
        }

    def _record_set_from_nodes(
        self,
        nodes: list[Any],
        *,
        revision: dict[str, Any],
        branch_node_id: str | None = None,
        branch_name: str | None = None,
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for node in nodes:
            path = node.attrs.get("path", node.node_id)
            edges: list[EdgeKey] = []
            if branch_node_id and branch_name and node.subtype == "source_file":
                edges.append(
                    EdgeKey(
                        src=node.node_id,
                        dst=branch_node_id,
                        edge_type="member_of",
                        source_record_id={
                            "repo": self.repo,
                            "branch": branch_name,
                        },
                        source_revision=revision,
                    )
                )
            records[str(path)] = RecordEmission(
                primary=RecordNode(
                    node_id=node.node_id,
                    subtype=node.subtype,
                    source_record_id={
                        "repo": self.repo,
                        "path": path,
                    },
                    source_revision=revision,
                ),
                edges=edges,
            )
        # Include the GitBranch anchor in the record set so its
        # presence is asserted on every completed-scope run; if the
        # branch were renamed/removed, the absence would correctly
        # trigger a retraction at the next reconcile.
        if branch_node_id and branch_name:
            records[f"__branch__:{branch_name}"] = RecordEmission(
                primary=RecordNode(
                    node_id=branch_node_id,
                    subtype="git_branch",
                    source_record_id={
                        "repo": self.repo,
                        "branch": branch_name,
                    },
                    source_revision=revision,
                )
            )
        return RecordSet(records=records)


__all__ = ["GitFilesSource"]
