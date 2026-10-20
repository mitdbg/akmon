"""AgentSessionsSource — substrate adapter for Claude Code session
transcripts.

Wraps the domain `okg.modules.agent_sessions.source.AgentSessionSource`
domain logic (walk `<claude_projects_root>/<cwd>/<*.jsonl>`, parse
session metadata + chapters + chips + skills + permission events +
file edits + ssh hosts + subagents) in the substrate `SourceAdapter`
Protocol.

State translation:
  - domain `state` is a dict keyed by transcript path carrying
    `session_id`, `transcript_bytes`, `last_ingested_at`, and
    append-frontier state — used to skip idle sessions whose size
    hasn't grown and to parse active sessions from a verified byte
    offset.
  - substrate cursor: round-trips the same dict verbatim via
    `next_cursor`.

Profile: `discovery_crawl`. Configured cwd scopes prefilter Claude's encoded
top-level project directories before traversal; transcript cwd is the final
authorization check. Unbounded completed runs use record-set reconciliation.
Bounded-since completed runs retain per-session owned-node identities and emit
explicit node retractions (whose graph projection removes incident edges).

Coverage notes (documented 2026-04-25 from
`debug-substrate-okg-self` Phase 10):

- **Permission events captured**: only `interrupted`,
  `denied`, `hook_blocked`, and `mode_changed`. The
  high-volume per-tool-call grants (`auto_allow`,
  `tool_use`, etc.) are NOT emitted as `permission_event`
  nodes — they're aggregated into the agent_session node's
  `auto_allow_count` / `tool_call_count` attrs instead.
  A session with thousands of auto-allows produces 0-2
  permission_event nodes by design.
- **Chapters require explicit markers**: the domain
  `_extract.py` chapter extractor matches specific
  in-transcript markers (chapter-open / chapter-close
  events). Sessions whose JSONLs don't carry these markers
  emit 0 `agent_chapter` nodes — this is by design, not a
  bug. As of 2026-04-25, real Claude Code session JSONLs
  do not emit chapter markers in normal use, so most
  deployments see `agent_chapter` registered but unused.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT,
    APPLIED_TOKEN_BINDING_UNVERIFIED,
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
    source_state_without_runner_keys,
)
from okg.substrate.library.sources._path_scope import (
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources._claude_project_scope import (
    is_claude_project_dir_candidate,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
)
from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec


_TRANSCRIPT_CONTENT_MANIFEST_KEY = "transcript_content_manifest"
_COST_TABLE_CONTENT_KEY = ".okg-internal/agent-sessions-cost-table"
_REPO_FILE_SCAN_SKIP_DIRS = frozenset({
    ".git",
    ".claude",
    ".hg",
    ".svn",
    "__pycache__",
    ".venv",
    "node_modules",
    ".pytest_cache",
})
@dataclass
class AgentSessionsSource:
    """Substrate adapter for agent session transcripts.

    Constructor kwargs mirror the domain emitter so deployment
    `source_registry.yaml` configs port verbatim.
    """

    repo: str
    claude_projects_root: Path | str
    cwd_filter: Optional[list[Path | str]] = None
    cwd_globs: Optional[list[str]] = None
    claude_worktree_roots: tuple[Path | str, ...] = ()
    cwd_to_repo: Optional[dict[str, str]] = None
    repo_root: Path | str | None = None
    repo_file_include_untracked: bool = True
    repo_file_exclude_globs: tuple[str, ...] = ()
    since: str = "30d"
    idle_threshold_hours: int = 24
    cost_estimation: bool = True
    cost_table_path: Optional[str] = None
    # Per `add-tool-use-event-capture` (2026-04-27): opt-in
    # allowlist of tool names that emit `tool_use_event` nodes.
    # Default None means zero events emitted (existing behavior).
    # Setting e.g. `['Edit', 'Write', 'Bash']` materializes one
    # node per matching tool_use block, gated by the allowlist.
    capture_tool_use_events: Optional[list[str]] = None
    emit_proposal_slug_refs: bool = True
    # `references` edges to `host:<name>` for each ssh target. Only a
    # deployment that loads a source declaring host nodes can admit them.
    emit_ssh_host_refs: bool = True

    name: str = "agent_sessions"
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True
    # Incremental-source contract (issue #323): HYBRID scope — repo files (git)
    # PLUS external JSONL transcripts (non-git). UNION of a git scope probe and a
    # complete streaming content probe over the transcripts, so a change in
    # EITHER half re-emits. The complete floor reads every transcript byte but
    # does not parse/extract on no-change; unlike the retired append-offset
    # probe it detects arbitrary same-size middle rewrites.
    change_probe_kind = "union"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.claude_projects_root, str):
            self.claude_projects_root = Path(self.claude_projects_root).expanduser()
        if self.repo_root is not None:
            self.repo_root = Path(self.repo_root).expanduser().resolve()
        if self.cwd_filter:
            self.cwd_filter = [
                Path(p).expanduser() if isinstance(p, str) else p
                for p in self.cwd_filter
            ]
        if self.cwd_globs:
            self.cwd_globs = [str(Path(p).expanduser()) for p in self.cwd_globs]
        self.claude_worktree_roots = tuple(
            Path(p).expanduser() if isinstance(p, str) else p
            for p in (self.claude_worktree_roots or ())
        )
        self.repo_file_exclude_globs = tuple(self.repo_file_exclude_globs or ())
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources import (
            _agent_sessions_cost,
            _agent_sessions_emit,
        )
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_scope_probe import GitScopeProbe
        from okg.substrate.library.sources.union_probe import UnionProbe

        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
            _agent_sessions_emit,
            _agent_sessions_cost,
        )
        transcript_root = Path(self.claude_projects_root)
        # Sound superset of the domain's _iter_session_files scope (all *.jsonl
        # under the projects root, incl. subagent transcripts). The content-based
        # cwd filter is applied downstream by the reader; folding the cwd config
        # into the token still busts it if that filter changes.
        def _transcript_items() -> list[tuple[str, Path]]:
            out: list[tuple[str, Path]] = []
            if transcript_root.is_dir():
                for cwd_dir in sorted(transcript_root.iterdir()):
                    if not cwd_dir.is_dir():
                        continue
                    if not is_claude_project_dir_candidate(
                        cwd_dir,
                        projects_root=transcript_root,
                        cwd_filter=self.cwd_filter or (),
                        cwd_globs=self.cwd_globs or (),
                        claude_worktree_roots=self.claude_worktree_roots,
                    ):
                        continue
                    for parent_path in sorted(cwd_dir.glob("*.jsonl")):
                        if parent_path.is_symlink() or not parent_path.is_file():
                            continue
                        session_dir = cwd_dir / parent_path.stem
                        nested_dir = session_dir / "subagents"
                        nested_paths = (
                            ()
                            if session_dir.is_symlink() or nested_dir.is_symlink()
                            else (
                            *nested_dir.glob("agent-*.jsonl"),
                            *nested_dir.glob("agent-*.meta.json"),
                            )
                        )
                        for transcript_path in (parent_path, *sorted(nested_paths)):
                            if (
                                transcript_path.is_symlink()
                                or not transcript_path.is_file()
                            ):
                                continue
                            out.append((
                                transcript_path.relative_to(
                                    transcript_root
                                ).as_posix(),
                                transcript_path,
                            ))
            # Pricing overrides are fact-determining input, not mere config.
            # Capture them in the same immutable content snapshot as the
            # transcripts so the emitted costs and acknowledged raw token are
            # derived from exactly the same bytes. Absence is membership too.
            if self.cost_table_path:
                cost_table = Path(self.cost_table_path)
                if cost_table.is_file():
                    out.append((_COST_TABLE_CONTENT_KEY, cost_table))
            return out

        transcript_probe = ContentHashProbe(
            content_items=_transcript_items,
            config={
                "repo": self.repo,
                "since": self.since,
                "idle_threshold_hours": self.idle_threshold_hours,
                "cwd_filter": [str(p) for p in (self.cwd_filter or [])],
                "cwd_globs": list(self.cwd_globs or []),
                "claude_worktree_roots": [
                    str(path) for path in self.claude_worktree_roots
                ],
                "cwd_to_repo": dict(self.cwd_to_repo or {}),
                "capture_tool_use_events": list(self.capture_tool_use_events or []),
                "emit_proposal_slug_refs": self.emit_proposal_slug_refs,
                "emit_ssh_host_refs": self.emit_ssh_host_refs,
                "cost_estimation": self.cost_estimation,
                "cost_table_path": self.cost_table_path,
            },
            emit_targets=emit_targets,
        )
        self._transcript_item_supplier = _transcript_items
        self._transcript_probe = transcript_probe
        if self.repo_root is None:
            self._repo_probe = None
            return transcript_probe
        repo_probe = GitScopeProbe(
            repo_root=Path(self.repo_root),
            config={
                "repo": self.repo,
                "repo_file_exclude_globs": sorted(self.repo_file_exclude_globs),
                "repo_file_include_untracked": self.repo_file_include_untracked,
            },
            emit_targets=emit_targets,
        )
        self._repo_probe = repo_probe
        return UnionProbe([repo_probe, transcript_probe])

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        change_probe_verdict: Optional[ProbeVerdict] = None,
        authoritative_replay: bool = False,
    ) -> SourceRun:
        from okg.substrate.library.sources._agent_sessions_emit import (
            AgentSessionSource as DomainAgentSessionSource,
        )

        scoped = path_scope_requested(sync_scope)
        content_snapshot = None
        repo_snapshot = None
        pinned_repo_paths: set[str] | None = None
        if not scoped:
            from okg.substrate.library.sources._content_snapshot import (
                CapturedContentSnapshot,
            )

            content_snapshot = CapturedContentSnapshot(
                self._transcript_item_supplier(),
                mirror_relative_paths=True,
            )
            if self._repo_probe is not None:
                try:
                    repo_snapshot = self._repo_probe.capture_snapshot()
                    pinned_repo_paths = (
                        self._repo_probe.file_paths_for_snapshot(
                            repo_snapshot
                        )
                    )
                except Exception:  # noqa: BLE001 - direct non-git callers
                    repo_snapshot = None
                    pinned_repo_paths = self._known_repo_file_paths()
        scoped_paths = self._scoped_session_paths(sync_scope)
        scoped_cursor_keys = self._scoped_session_cursor_keys(sync_scope)
        scoped_record_keys = self._scoped_session_record_keys(sync_scope)
        nested_parent_state_keys = self._scoped_nested_parent_state_keys(sync_scope)
        completed_scope = mode in {"reconcile", "scope_complete"}
        full_replay = authoritative_replay and completed_scope and not scoped
        can_declare_record_set = (
            self._record_set_authoritative() or full_replay
        )
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        prior_record_set = (
            self._record_set_from_cursor(cursor)
            if completed_scope and not scoped and can_declare_record_set else None
        )
        can_carry_completed_scope = prior_record_set is not None
        can_probe_completed_scope = (
            completed_scope
            and not scoped
            and can_declare_record_set
            and has_record_set_authority
        )
        carry_replacement_record_keys: set[str] = set()
        source_state = (
            source_state_without_runner_keys(cursor)
            if cursor and (
                mode in {"cursor", "scope_incomplete"}
                or scoped
                or can_probe_completed_scope
                or (completed_scope and not can_declare_record_set)
            )
            else {}
        )
        prior_manifest = _content_manifest_from_cursor(cursor)
        # Runner- and probe-owned metadata must never be presented to the
        # domain emitter as if it were a transcript cursor entry.
        source_state.pop(_TRANSCRIPT_CONTENT_MANIFEST_KEY, None)
        source_state.pop(CURSOR_TOKEN_KEY, None)
        source_state.pop("change_probe_skips", None)
        prior_source_state = dict(source_state)
        if (
            completed_scope
            and not scoped
            and source_state
            and (can_carry_completed_scope or not can_declare_record_set)
        ):
            (
                source_state,
                carry_replacement_record_keys,
            ) = self._completed_scope_carry_state(source_state)
        current_manifest = (
            self._transcript_probe.manifest_from_item_hashes(
                content_snapshot.item_hashes,
                item_sizes=content_snapshot.item_sizes,
            )
            if content_snapshot is not None
            else self._current_transcript_manifest(
                scoped=scoped,
                prior_manifest=prior_manifest,
                change_probe_verdict=change_probe_verdict,
            )
        )
        if not scoped:
            self._invalidate_content_changed_source_state(
                source_state,
                prior_manifest=prior_manifest,
                current_manifest=current_manifest,
                content_snapshot=content_snapshot,
            )
        invalidated_state_keys = set(nested_parent_state_keys)
        if scoped:
            # The runner intentionally bypasses the global probe for a partial
            # scope. A scoped request therefore carries no complete-content
            # proof for its selected transcript and must invalidate that
            # transcript's resume state in every mode.
            invalidated_state_keys.update(scoped_cursor_keys | scoped_record_keys)
        if scoped and invalidated_state_keys:
            # A child transcript or metadata change leaves the parent JSONL
            # size unchanged. Keep the parent known for bounded-since
            # eligibility, but invalidate its append state so the remapped
            # scoped run performs a complete parent + child rescan.
            self._invalidate_scoped_source_state(
                source_state,
                keys=invalidated_state_keys,
                scoped_paths=scoped_paths,
            )
        domain = DomainAgentSessionSource(
            claude_projects_root=(
                content_snapshot.root
                if content_snapshot is not None
                else Path(self.claude_projects_root)
            ),
            display_projects_root=(
                Path(self.claude_projects_root)
                if content_snapshot is not None else None
            ),
            cwd_filter=self.cwd_filter,
            cwd_globs=self.cwd_globs,
            claude_worktree_roots=self.claude_worktree_roots,
            cwd_to_repo=self.cwd_to_repo,
            since="all" if full_replay else self.since,
            idle_threshold_hours=self.idle_threshold_hours,
            cost_estimation=self.cost_estimation,
            cost_table_path=(
                str(
                    content_snapshot.by_key[
                        _COST_TABLE_CONTENT_KEY
                    ].snapshot_path
                )
                if content_snapshot is not None
                and _COST_TABLE_CONTENT_KEY in content_snapshot.by_key
                else str(
                    content_snapshot.root
                    / _COST_TABLE_CONTENT_KEY
                    / "missing"
                )
                if content_snapshot is not None and self.cost_table_path
                else self.cost_table_path
            ),
            capture_tool_use_events=self.capture_tool_use_events,
            emit_proposal_slug_refs=self.emit_proposal_slug_refs,
            emit_ssh_host_refs=self.emit_ssh_host_refs,
            state=source_state,
        )
        if scoped:
            scoped_iter = tuple(scoped_paths)

            def _iter_scoped_session_files() -> Iterator[Path]:
                yield from scoped_iter

            domain._iter_session_files = _iter_scoped_session_files  # type: ignore[method-assign]

        nodes = list(domain.nodes())
        raw_edges = list(domain.edges())
        has_file_edges = self._has_normalizable_file_edges(raw_edges)
        known_repo_paths = (
            pinned_repo_paths
            if has_file_edges else None
        )
        edges = (
            self._normalize_file_edges(
                raw_edges,
                known_repo_paths=known_repo_paths,
            )
            if has_file_edges else raw_edges
        )
        # The domain emitter names a placeholder source_file node after the
        # session's working folder (a Claude worktree name, for example).
        # Normalization rewrites or drops the edges to it, so the placeholder
        # would be written with no edge and no record (#2366). Canonical
        # file:<repo>:<path> nodes are re-derived from the normalized edges.
        nodes = self._drop_placeholder_source_file_nodes(nodes)
        nodes.extend(
            self._source_file_nodes_for_normalized_edges(
                edges,
                known_repo_paths=known_repo_paths,
            )
        )
        stale_ids = list(domain.stale_node_ids or ())
        domain_state = domain.get_run_state()
        applied_token = None
        if content_snapshot is not None:
            transcript_token = self._transcript_probe.token_from_item_hashes(
                content_snapshot.item_hashes
            )
            applied_token = (
                self.change_probe.token_from_subtokens([
                    self._repo_probe.token_for_snapshot(**repo_snapshot),
                    transcript_token,
                ])
                if self._repo_probe is not None
                and repo_snapshot is not None
                else transcript_token
                if self._repo_probe is None
                else None
            )
            content_snapshot.close()
        sealed_snapshot_token = (
            content_snapshot is not None and applied_token is not None
        )
        normalizable_file_edges = sum(
            1
            for edge in raw_edges
            if edge.edge_type == "affects" and edge.dst.startswith("file:")
        )
        self.last_run_stats = {
            **(getattr(domain, "last_run_stats", {}) or {}),
            "raw_edges": len(raw_edges),
            "normalized_edges": len(edges),
            "normalizable_file_edges": normalizable_file_edges,
            "repo_file_inventory_required": has_file_edges,
            "repo_file_inventory_available": known_repo_paths is not None,
        }
        if (
            completed_scope
            and not scoped
            and can_declare_record_set
            and has_record_set_authority
            and not can_carry_completed_scope
            and (nodes or raw_edges or stale_ids)
        ):
            return self.run(
                run_id,
                mode=mode,
                authoritative_replay=authoritative_replay,
            )

        del run_id
        # Run-scoped digest of the whole emission. It attributes RETRACTIONS
        # only — a retraction has no surviving content of its own to hash, and
        # its revision never reaches a stored row's identity because a delete
        # keeps the prior row's payload. Inserted node/edge facts get a
        # per-record content hash instead (`_node_revision`/`_edge_revision`):
        # giving them this digest rotated every record's revision, and with it
        # every record's Nomos lineage envelope, whenever any single session
        # changed (#1714).
        revision = {
            "emission_sha256": hashlib.sha256(
                json.dumps(
                    {
                        "nodes": [
                            {
                                "node_id": node.node_id,
                                "subtype": node.subtype,
                                "attrs": node.attrs,
                            }
                            for node in nodes
                        ],
                        "edges": [
                            {
                                "src": edge.src,
                                "dst": edge.dst,
                                "edge_type": edge.edge_type,
                                "attrs": edge.attrs,
                                "provenance": edge.provenance,
                                "confidence": edge.confidence,
                            }
                            for edge in edges
                        ],
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode("utf-8")
            ).hexdigest(),
        }
        facts = self._facts_from_emit(
            nodes=nodes,
            edges=edges,
            revision=revision,
            mode=mode,
            stale_ids=stale_ids,
        )
        self._attach_current_owned_edges(domain_state, facts=facts)
        if not completed_scope:
            self._merge_incomplete_owned_footprint(
                domain_state,
                prior=prior_source_state,
            )
        if completed_scope and not can_declare_record_set:
            bounded_retractions = self._bounded_owned_retractions(
                prior=prior_source_state,
                current=domain_state,
                revision=revision,
            )
            facts.extend(bounded_retractions)
            self.last_run_stats["bounded_owned_node_retractions"] = len(
                [fact for fact in bounded_retractions if isinstance(fact, NodeFact)]
            )
            self.last_run_stats["bounded_owned_edge_retractions"] = len(
                [fact for fact in bounded_retractions if isinstance(fact, EdgeFact)]
            )
        can_build_complete_record_set = (
            completed_scope
            and can_declare_record_set
            and (
                not has_record_set_authority
                or can_carry_completed_scope
                or bool(facts)
            )
        )
        record_set = (
            self._record_set_for_facts(facts)
            if can_build_complete_record_set else None
        )
        if record_set is not None:
            self._require_emitted_nodes_in_record_set(facts, record_set)
        if (
            completed_scope
            and can_carry_completed_scope
            and prior_record_set is not None
            and record_set is not None
        ):
            record_set = self._record_set_with_prior(
                prior_record_set,
                current=record_set,
                replacement_keys=(
                    carry_replacement_record_keys | set(record_set.records)
                ),
            )
        if completed_scope and scoped and can_declare_record_set:
            record_set = self._scoped_record_set(
                cursor,
                current=record_set or RecordSet(),
                replacement_keys=scoped_record_keys,
            )
            completed_scope = record_set is not None

        manifest_to_persist = (
            prior_manifest if scoped else current_manifest
        )
        if manifest_to_persist is not None:
            domain_state[_TRANSCRIPT_CONTENT_MANIFEST_KEY] = manifest_to_persist

        def _facts() -> Iterator[Any]:
            yield from facts

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=(
                preserve_cursor_record_set_authority(domain_state, cursor)
                if completed_scope
                and can_declare_record_set
                and has_record_set_authority
                and record_set is None
                else domain_state
            ),
            applied_raw_token=applied_token,
            applied_token_scope=(
                "global" if applied_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_token is not None else "unavailable"
            ),
            # The token hashes the transcript bytes copied into
            # CapturedContentSnapshot before any were parsed, plus the repo
            # snapshot pinned before path normalization, so it stays true for
            # the facts emitted even if a live transcript grows mid-run.
            applied_token_binding=(
                APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT
                if sealed_snapshot_token
                else APPLIED_TOKEN_BINDING_UNVERIFIED
            ),
            run_mode=mode,
            record_set=record_set,
        )

    def cursor_blob_keys_for_scope(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> set[str] | None:
        if not path_scope_requested(sync_scope):
            return None
        return {
            *self._scoped_session_cursor_keys(sync_scope),
            *self._scoped_nested_parent_state_keys(sync_scope),
        }

    def _facts_from_emit(
        self,
        *,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        revision: dict[str, Any],
        mode: str,
        stale_ids: list[str],
    ) -> list[NodeFact | EdgeFact]:
        facts: list[NodeFact | EdgeFact] = []
        for n in nodes:
            node_attrs = {"label": n.label, **n.attrs}
            facts.append(NodeFact(
                node_id=n.node_id,
                subtype=n.subtype,
                attrs=node_attrs,
                source_record_id={
                    "repo": self.repo,
                    "node_id": n.node_id,
                },
                source_revision=self._node_revision(n, attrs=node_attrs),
                op="I",
            ))
        for e in edges:
            confidence = e.confidence if e.confidence is not None else 1.0
            facts.append(EdgeFact(
                src=e.src,
                dst=e.dst,
                edge_type=e.edge_type,
                attrs=dict(e.attrs),
                provenance=e.provenance,
                confidence=confidence,
                source_record_id={
                    "repo": self.repo,
                    "src": e.src,
                    "dst": e.dst,
                    "edge_type": e.edge_type,
                },
                source_revision=self._edge_revision(e, confidence=confidence),
                op="I",
            ))
        if mode in {"reconcile", "scope_complete"}:
            for stale_id in stale_ids:
                facts.append(NodeFact(
                    node_id=stale_id,
                    subtype="agent_session",
                    attrs={},
                    source_record_id={
                        "repo": self.repo,
                        "node_id": stale_id,
                    },
                    source_revision=revision,
                    op="R",
                ))
        return facts

    @staticmethod
    def _content_revision(payload: dict[str, Any]) -> dict[str, Any]:
        """Return the content hash of ONE emitted record.

        The registry declares ``source_revision_kind: content_hash`` for this
        adapter, and ``source_revision`` reaches two identity comparisons: the
        stored ``source_revision`` column and the Nomos lineage envelope in
        ``attrs`` (``sources[].source_revisions``). Both are compared by
        ``ChronosGraphWriteMixin._node_upsert_semantic_noop`` /
        ``_edge_upsert_semantic_noop``, which decide whether an upsert closes
        the live Chronos interval and opens a new one.

        So the revision must be derived from THIS record's content and nothing
        else. A whole-emission digest satisfies the letter of "content hash"
        while rotating on every record whenever any one session changes, which
        rewrote the entire agent-sessions footprint on every run (#1714).
        """
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return {"content_sha256": hashlib.sha256(encoded).hexdigest()}

    def _node_revision(
        self,
        node: NodeSpec,
        *,
        attrs: dict[str, Any],
    ) -> dict[str, Any]:
        return self._content_revision({
            "kind": "node",
            "node_id": str(node.node_id),
            "subtype": str(node.subtype),
            "attrs": attrs,
        })

    def _edge_revision(
        self,
        edge: EdgeSpec,
        *,
        confidence: float,
    ) -> dict[str, Any]:
        return self._content_revision({
            "kind": "edge",
            "src": str(edge.src),
            "dst": str(edge.dst),
            "edge_type": str(edge.edge_type),
            "attrs": dict(edge.attrs),
            "provenance": edge.provenance,
            "confidence": confidence,
        })

    def _record_set_for_facts(
        self,
        facts: list[NodeFact | EdgeFact],
    ) -> RecordSet:
        nodes: dict[str, NodeFact] = {
            fact.node_id: fact
            for fact in facts
            if isinstance(fact, NodeFact) and fact.op != "R"
        }
        assignments: dict[str, set[str]] = {}
        for node_id, fact in nodes.items():
            record_key = self._record_key_for_node_fact(fact)
            if record_key is not None:
                assignments.setdefault(node_id, set()).add(record_key)

        changed = True
        while changed:
            changed = False
            for fact in facts:
                if not isinstance(fact, EdgeFact) or fact.op == "R":
                    continue
                owner_keys = self._record_keys_for_edge(fact, assignments)
                if not owner_keys:
                    continue
                for endpoint in (fact.src, fact.dst):
                    if endpoint not in nodes:
                        continue
                    before = len(assignments.get(endpoint, set()))
                    assignments.setdefault(endpoint, set()).update(owner_keys)
                    if len(assignments[endpoint]) != before:
                        changed = True

        records: dict[str, RecordEmission] = {}
        for node_id, fact in nodes.items():
            for record_key in sorted(assignments.get(node_id, ())):
                emission = records.setdefault(record_key, RecordEmission())
                node = RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                )
                if fact.subtype == "agent_session" and emission.primary is None:
                    records[record_key] = RecordEmission(
                        primary=node,
                        descendants=emission.descendants,
                        edges=emission.edges,
                    )
                else:
                    emission.descendants.append(node)

        for fact in facts:
            if not isinstance(fact, EdgeFact) or fact.op == "R":
                continue
            for record_key in sorted(self._record_keys_for_edge(fact, assignments)):
                records.setdefault(record_key, RecordEmission()).edges.append(
                    EdgeKey(
                        src=fact.src,
                        dst=fact.dst,
                        edge_type=fact.edge_type,
                        attrs=dict(fact.attrs),
                        provenance=fact.provenance,
                        confidence=fact.confidence,
                        source_record_id=dict(fact.source_record_id),
                        source_revision=dict(fact.source_revision),
                    )
                )
        return RecordSet(records=records)

    @staticmethod
    def _since_is_unbounded(since: str | None) -> bool:
        if since is None:
            return True
        return str(since).strip().lower() in {"", "all"}

    def _record_set_authoritative(self) -> bool:
        return self._since_is_unbounded(self.since)

    def _completed_scope_carry_state(
        self,
        source_state: dict[str, Any],
    ) -> tuple[dict[str, Any], set[str]]:
        current_paths = self._current_session_paths_by_cursor_key()
        kept_state: dict[str, Any] = {}
        replacement_record_keys: set[str] = set()

        for cursor_key, prior in source_state.items():
            record_key = self._record_key_for_cursor_state(cursor_key, prior)
            path = current_paths.get(cursor_key)
            if path is None:
                if record_key is not None:
                    replacement_record_keys.add(record_key)
                continue
            if self._same_session_fingerprint(path, prior):
                kept_state[cursor_key] = prior
                continue
            if record_key is not None:
                replacement_record_keys.add(record_key)
            kept_state[cursor_key] = self._invalidated_session_state(prior)

        return kept_state, replacement_record_keys

    def _current_transcript_manifest(
        self,
        *,
        scoped: bool,
        prior_manifest: dict[str, Any] | None,
        change_probe_verdict: ProbeVerdict | None,
    ) -> dict[str, Any] | None:
        if scoped:
            return prior_manifest
        if change_probe_verdict is not None:
            if not change_probe_verdict.token:
                # A composite sibling may have failed before this child ran.
                # Never reuse the child's cache from an earlier probe cycle.
                return None
            manifest = self._transcript_probe.last_item_manifest()
            if manifest is not None:
                return manifest
            # The runner evaluated the probe but no complete manifest survived:
            # a read error or malformed probe result. The caller invalidates
            # every resume cursor rather than silently trusting stale state.
            return None
        # Direct adapter invocations (mostly focused tests) do not pass through
        # SourceRunner. Evaluate the same complete probe once so they exercise
        # the production cursor rules instead of falling back to size.
        try:
            self._transcript_probe.build_token()
        except Exception:  # noqa: BLE001 — unknown identity invalidates all
            return None
        return self._transcript_probe.last_item_manifest()

    def _invalidate_content_changed_source_state(
        self,
        source_state: dict[str, Any],
        *,
        prior_manifest: dict[str, Any] | None,
        current_manifest: dict[str, Any] | None,
        content_snapshot: Any = None,
    ) -> None:
        """Invalidate resume cursors for transcripts whose content changed.

        When ``content_snapshot`` is given, the reader parses the snapshot
        copies, so the append check reads the copied bytes too. The live file
        may have moved (or moved back) since the copy was taken; resuming
        from the saved offset on the strength of the live bytes would skip a
        rewritten prefix that exists only in the copy.
        """
        invalidate_all = (
            prior_manifest is None
            or current_manifest is None
            or prior_manifest.get("config_sha256")
            != current_manifest.get("config_sha256")
            or prior_manifest.get("emit_version_sha256")
            != current_manifest.get("emit_version_sha256")
        )
        if invalidate_all:
            for key, prior in list(source_state.items()):
                if self._record_key_for_cursor_state(key, prior) is not None:
                    source_state[key] = self._invalidated_session_state(prior)
            return

        prior_items = prior_manifest["items"]
        current_items = current_manifest["items"]
        changed_item_keys = {
            key
            for key in set(prior_items) | set(current_items)
            if prior_items.get(key) != current_items.get(key)
        }
        if _COST_TABLE_CONTENT_KEY in changed_item_keys:
            # A pricing-table change can alter every emitted session/subagent
            # cost while transcript bytes remain identical. Discard every
            # append cursor so the reader recomputes the complete cost-bearing
            # fact stream from the captured table bytes.
            for key, prior in list(source_state.items()):
                if self._record_key_for_cursor_state(key, prior) is not None:
                    source_state[key] = self._invalidated_session_state(prior)
            return
        invalidated_parent_keys: set[str] = set()
        root = Path(self.claude_projects_root)
        for item_key in changed_item_keys:
            path = root / item_key
            is_nested = False
            if self._is_subagent_transcript_path(path):
                is_nested = True
                path = self._parent_transcript_for_subagent(path) or path
            elif path.name.endswith(".meta.json"):
                is_nested = True
                path = self._parent_transcript_for_meta(path) or path
            cursor_keys = {
                self._cursor_key_for_path(path),
                path.stem,
            }
            if content_snapshot is None:
                append_check_path: Path | None = path
            else:
                captured = content_snapshot.by_key.get(item_key)
                append_check_path = (
                    captured.snapshot_path if captured is not None else None
                )
            if (
                not is_nested
                and append_check_path is not None
                and self._content_change_is_verified_append(
                    append_check_path,
                    cursor_keys=cursor_keys,
                    source_state=source_state,
                    prior_item_hash=prior_items.get(item_key),
                )
            ):
                continue
            invalidated_parent_keys.update(cursor_keys)

        for key in invalidated_parent_keys:
            prior = source_state.get(key)
            if prior is None:
                continue
            source_state[key] = self._invalidated_session_state(prior)

    @staticmethod
    def _content_change_is_verified_append(
        path: Path,
        *,
        cursor_keys: set[str],
        source_state: dict[str, Any],
        prior_item_hash: Any,
    ) -> bool:
        if not isinstance(prior_item_hash, str) or len(prior_item_hash) != 64:
            return False
        prior = next(
            (
                source_state[key]
                for key in cursor_keys
                if isinstance(source_state.get(key), dict)
            ),
            None,
        )
        if not isinstance(prior, dict):
            return False
        try:
            prior_size = int(prior.get("transcript_bytes"))
            current_size = path.stat().st_size
        except (OSError, TypeError, ValueError):
            return False
        if prior_size <= 0 or current_size <= prior_size:
            return False
        from okg.substrate.library.sources.content_hash_probe import (
            file_prefix_sha256,
        )

        return file_prefix_sha256(path, prior_size) == prior_item_hash

    @staticmethod
    def _invalidated_session_state(prior: Any) -> dict[str, Any]:
        state = dict(prior) if isinstance(prior, dict) else {}
        state["transcript_bytes"] = -1
        state["last_ingested_at"] = ""
        state.pop("append_cursor", None)
        state.pop("subagent_fingerprint", None)
        return state

    def _invalidate_scoped_source_state(
        self,
        source_state: dict[str, Any],
        *,
        keys: set[str],
        scoped_paths: tuple[Path, ...],
    ) -> None:
        existing_path_keys: set[str] = set()
        for path in scoped_paths:
            if not path.is_file():
                continue
            existing_path_keys.add(self._cursor_key_for_path(path))
            existing_path_keys.add(path.stem)
        for key in keys:
            prior = source_state.get(key)
            if prior is None:
                continue
            if key in existing_path_keys:
                source_state[key] = self._invalidated_session_state(prior)
            else:
                source_state.pop(key, None)

    def _attach_current_owned_edges(
        self,
        current: dict[str, Any],
        *,
        facts: list[NodeFact | EdgeFact],
    ) -> None:
        records = self._record_set_for_facts(facts).records
        for cursor_key, state in list(current.items()):
            if not isinstance(state, dict):
                continue
            record_key = self._record_key_for_cursor_state(cursor_key, state)
            emission = records.get(record_key or "")
            if emission is None:
                continue
            updated = dict(state)
            updated["owned_edges"] = [
                {
                    "src": edge.src,
                    "dst": edge.dst,
                    "edge_type": edge.edge_type,
                    "attrs": dict(edge.attrs),
                    "provenance": edge.provenance,
                    "confidence": edge.confidence,
                }
                for _identity, edge in sorted(
                    emission.edges_by_identity().items()
                )
            ]
            current[cursor_key] = updated

    def _merge_incomplete_owned_footprint(
        self,
        current: dict[str, Any],
        *,
        prior: dict[str, Any],
    ) -> None:
        prior_nodes_by_record: dict[str, dict[str, str]] = {}
        prior_edges_by_record: dict[
            str,
            dict[tuple[str, str, str], dict[str, Any]],
        ] = {}
        prior_complete_by_record: dict[str, bool] = {}
        prior_pending_by_record: dict[str, bool] = {}
        for cursor_key, state in prior.items():
            record_key = self._record_key_for_cursor_state(cursor_key, state)
            if record_key is None:
                continue
            prior_nodes_by_record.setdefault(record_key, {}).update(
                self._owned_nodes_from_cursor_state(state)
            )
            prior_edges_by_record.setdefault(record_key, {}).update(
                self._owned_edges_from_cursor_state(state)
            )
            if isinstance(state, dict):
                prior_complete_by_record[record_key] = (
                    prior_complete_by_record.get(record_key, True)
                    and state.get("owned_footprint_complete") is True
                )
                prior_pending_by_record[record_key] = (
                    prior_pending_by_record.get(record_key, False)
                    or state.get("owned_footprint_requires_complete_refresh") is True
                )
        for cursor_key, state in list(current.items()):
            if not isinstance(state, dict):
                continue
            record_key = self._record_key_for_cursor_state(cursor_key, state)
            if record_key is None:
                continue
            prior_nodes = prior_nodes_by_record.get(record_key, {})
            current_nodes = self._owned_nodes_from_cursor_state(state)
            prior_edges = prior_edges_by_record.get(record_key, {})
            current_edges = self._owned_edges_from_cursor_state(state)
            current_complete = state.get("owned_footprint_complete") is True
            owned_nodes = dict(prior_nodes)
            owned_nodes.update(current_nodes)
            owned_edges = dict(prior_edges)
            owned_edges.update(current_edges)
            updated = dict(state)
            updated["owned_nodes"] = [
                {"node_id": node_id, "subtype": subtype}
                for node_id, subtype in sorted(owned_nodes.items())
            ]
            updated["owned_edges"] = [
                payload
                for _identity, payload in sorted(owned_edges.items())
            ]
            updated["owned_footprint_complete"] = (
                current_complete
                or prior_complete_by_record.get(record_key, False)
            )
            pending_refresh = (
                prior_pending_by_record.get(record_key, False)
                or (
                    current_complete
                    and (
                        bool(set(prior_nodes).difference(current_nodes))
                        or bool(set(prior_edges).difference(current_edges))
                    )
                )
            )
            if pending_refresh:
                updated["owned_footprint_requires_complete_refresh"] = True
            else:
                updated.pop("owned_footprint_requires_complete_refresh", None)
            current[cursor_key] = updated

    def _bounded_owned_retractions(
        self,
        *,
        prior: dict[str, Any],
        current: dict[str, Any],
        revision: dict[str, Any],
    ) -> list[NodeFact | EdgeFact]:
        prior_nodes = self._owned_nodes_from_state(prior)
        current_nodes = self._owned_nodes_from_state(current)
        prior_edges = self._owned_edges_from_state(prior)
        current_edges = self._owned_edges_from_state(current)
        edge_retractions = [
            EdgeFact(
                src=payload["src"],
                dst=payload["dst"],
                edge_type=payload["edge_type"],
                attrs=dict(payload.get("attrs") or {}),
                provenance=str(payload.get("provenance") or "explicit"),
                confidence=float(payload.get("confidence", 1.0)),
                source_record_id={
                    "repo": self.repo,
                    "src": payload["src"],
                    "dst": payload["dst"],
                    "edge_type": payload["edge_type"],
                },
                source_revision={
                    **revision,
                    "bounded_owned_edge_retraction": True,
                },
                op="R",
            )
            for identity, payload in sorted(prior_edges.items())
            if identity not in current_edges
        ]
        node_retractions = [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs={},
                source_record_id={"repo": self.repo, "node_id": node_id},
                source_revision={
                    **revision,
                    "bounded_owned_node_retraction": True,
                },
                op="R",
            )
            for node_id, subtype in sorted(prior_nodes.items())
            if node_id not in current_nodes
        ]
        return [*edge_retractions, *node_retractions]

    def _owned_nodes_from_state(
        self,
        state: dict[str, Any],
    ) -> dict[str, str]:
        out: dict[str, str] = {}
        for cursor_state in state.values():
            out.update(self._owned_nodes_from_cursor_state(cursor_state))
        return out

    def _owned_edges_from_state(
        self,
        state: dict[str, Any],
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        out: dict[tuple[str, str, str], dict[str, Any]] = {}
        for cursor_state in state.values():
            out.update(self._owned_edges_from_cursor_state(cursor_state))
        return out

    @staticmethod
    def _owned_nodes_from_cursor_state(state: Any) -> dict[str, str]:
        if not isinstance(state, dict):
            return {}
        out: dict[str, str] = {}
        for raw in state.get("owned_nodes") or ():
            if not isinstance(raw, dict):
                continue
            node_id = raw.get("node_id")
            subtype = raw.get("subtype")
            if isinstance(node_id, str) and node_id and isinstance(subtype, str):
                out[node_id] = subtype
        return out

    @staticmethod
    def _owned_edges_from_cursor_state(
        state: Any,
    ) -> dict[tuple[str, str, str], dict[str, Any]]:
        if not isinstance(state, dict):
            return {}
        out: dict[tuple[str, str, str], dict[str, Any]] = {}
        for raw in state.get("owned_edges") or ():
            if not isinstance(raw, dict):
                continue
            src = raw.get("src")
            dst = raw.get("dst")
            edge_type = raw.get("edge_type")
            if not all(
                isinstance(value, str) and value
                for value in (src, dst, edge_type)
            ):
                continue
            identity = (src, edge_type, dst)
            out[identity] = {
                "src": src,
                "dst": dst,
                "edge_type": edge_type,
                "attrs": dict(raw.get("attrs") or {}),
                "provenance": str(raw.get("provenance") or "explicit"),
                "confidence": float(raw.get("confidence", 1.0)),
            }
        return out

    def _current_session_paths_by_cursor_key(self) -> dict[str, Path]:
        out: dict[str, Path] = {}
        root = Path(self.claude_projects_root)
        if not root.is_dir():
            return out
        for cwd_dir in root.iterdir():
            if not cwd_dir.is_dir():
                continue
            if not is_claude_project_dir_candidate(
                cwd_dir,
                projects_root=root,
                cwd_filter=self.cwd_filter or (),
                cwd_globs=self.cwd_globs or (),
                claude_worktree_roots=self.claude_worktree_roots,
            ):
                continue
            for jsonl in cwd_dir.glob("*.jsonl"):
                if jsonl.is_symlink() or not jsonl.is_file():
                    continue
                out[self._cursor_key_for_path(jsonl)] = jsonl.resolve()
        return out

    def _same_session_fingerprint(self, path: Path, prior: Any) -> bool:
        from okg.substrate.library.sources._agent_sessions_emit import (
            subagent_tree_fingerprint,
        )

        if not isinstance(prior, dict):
            return False
        if prior.get("filter_signature") != self._filter_signature():
            return False
        if (
            prior.get("owned_footprint_complete") is not True
            or prior.get("owned_footprint_requires_complete_refresh") is True
        ):
            return False
        try:
            stat = path.stat()
            prior_size = int(prior.get("transcript_bytes"))
        except (OSError, TypeError, ValueError):
            return False
        if stat.st_size != prior_size:
            return False
        session_id = str(prior.get("session_id") or path.stem)
        if prior.get("subagent_fingerprint") != subagent_tree_fingerprint(
            path,
            session_id,
        ):
            return False
        append_cursor = prior.get("append_cursor")
        if not isinstance(append_cursor, dict):
            return False
        try:
            return int(append_cursor.get("mtime_ns")) == int(stat.st_mtime_ns)
        except (TypeError, ValueError):
            return False

    def _filter_signature(self) -> str:
        return json.dumps(
            {
                "cwd_filter": [str(path) for path in self.cwd_filter or []],
                "cwd_globs": list(self.cwd_globs or []),
                "claude_worktree_roots": [
                    str(path) for path in self.claude_worktree_roots
                ],
            },
            sort_keys=True,
        )

    @staticmethod
    def _record_key_for_cursor_state(
        cursor_key: str,
        prior: Any,
    ) -> str | None:
        if isinstance(prior, dict):
            session_id = prior.get("session_id")
            if isinstance(session_id, str) and session_id:
                return session_id
            append_cursor = prior.get("append_cursor")
            if isinstance(append_cursor, dict):
                session_id = append_cursor.get("session_id")
                if isinstance(session_id, str) and session_id:
                    return session_id
        return Path(cursor_key).stem or None

    @staticmethod
    def _record_set_from_cursor(
        cursor: Optional[dict[str, Any]],
    ) -> Optional[RecordSet]:
        if not isinstance(cursor, dict):
            return None
        prior_raw = cursor.get("record_set")
        if prior_raw is None:
            return None
        return RecordSet.from_cursor(prior_raw)

    @staticmethod
    def _record_set_with_prior(
        prior: RecordSet,
        *,
        current: RecordSet,
        replacement_keys: set[str],
    ) -> RecordSet:
        records = {
            key: value
            for key, value in prior.records.items()
            if key not in replacement_keys
        }
        records.update(current.records)
        return RecordSet(records=records)

    @staticmethod
    def _record_keys_for_edge(
        fact: EdgeFact,
        assignments: dict[str, set[str]],
    ) -> set[str]:
        if fact.edge_type == "assigned_to" and fact.dst in assignments:
            return set(assignments[fact.dst])
        if fact.src in assignments:
            return set(assignments[fact.src])
        if fact.dst in assignments:
            return set(assignments[fact.dst])
        return set()

    @staticmethod
    def _record_key_for_node_fact(fact: NodeFact) -> str | None:
        node_id = fact.node_id
        if fact.subtype == "agent_session" and node_id.startswith("session:"):
            return node_id.removeprefix("session:")
        for prefix in ("chapter:", "chip:", "pevent:", "tool_use:"):
            if node_id.startswith(prefix):
                rest = node_id.removeprefix(prefix)
                session_id = rest.split(":", 1)[0]
                return session_id or None
        return None

    @staticmethod
    def _scoped_record_set(
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

    def _normalize_file_edges(
        self,
        edges: list[EdgeSpec],
        *,
        known_repo_paths: set[str] | None,
    ) -> list[EdgeSpec]:
        out: list[EdgeSpec] = []
        for edge in edges:
            if edge.edge_type != "affects" or not edge.dst.startswith("file:"):
                out.append(edge)
                continue
            parts = edge.dst.split(":", 2)
            if len(parts) != 3 or not parts[2]:
                continue
            relpath = parts[2]
            if known_repo_paths is not None and relpath not in known_repo_paths:
                continue
            out.append(
                EdgeSpec(
                    src=edge.src,
                    dst=f"file:{self.repo}:{relpath}",
                    edge_type=edge.edge_type,
                    attrs=dict(edge.attrs),
                    provenance=edge.provenance,
                    confidence=edge.confidence,
                )
            )
        return out

    @staticmethod
    def _drop_placeholder_source_file_nodes(
        nodes: list[NodeSpec],
    ) -> list[NodeSpec]:
        return [
            node
            for node in nodes
            if not (
                node.subtype == "source_file"
                and node.attrs.get("source_provider") == "agent_sessions"
            )
        ]

    @staticmethod
    def _require_emitted_nodes_in_record_set(
        facts: list[NodeFact | EdgeFact],
        record_set: RecordSet,
    ) -> None:
        # A written node that no record owns can never be retracted by the
        # record-set diff and fails the runner's coverage check after the
        # write. Refuse before anything is written.
        covered: set[str] = set()
        for emission in record_set.records.values():
            covered.update(emission.nodes_by_id())
        outside = sorted({
            fact.node_id
            for fact in facts
            if isinstance(fact, NodeFact)
            and fact.op != "R"
            and fact.node_id not in covered
        })
        if outside:
            raise SubstrateError(
                "agent_sessions: emitted nodes outside record set",
                data={"count": len(outside), "sample": outside[:10]},
            )

    @staticmethod
    def _has_normalizable_file_edges(edges: list[EdgeSpec]) -> bool:
        return any(
            edge.edge_type == "affects" and edge.dst.startswith("file:")
            for edge in edges
        )

    def _source_file_nodes_for_normalized_edges(
        self,
        edges: list[EdgeSpec],
        *,
        known_repo_paths: set[str] | None,
    ) -> list[NodeSpec]:
        # When repo_root is configured, git-files is the authoritative source for
        # canonical file:<repo>:* nodes. Without that coverage signal, emit stubs
        # for normalized file-affect edges so the graph remains closed.
        if known_repo_paths is not None:
            return []
        out: list[NodeSpec] = []
        seen: set[str] = set()
        for edge in edges:
            if edge.edge_type != "affects" or not edge.dst.startswith("file:"):
                continue
            parts = edge.dst.split(":", 2)
            if len(parts) != 3 or not parts[2] or edge.dst in seen:
                continue
            seen.add(edge.dst)
            out.append(
                NodeSpec(
                    node_id=edge.dst,
                    subtype="source_file",
                    label=parts[2],
                    attrs={
                        "path": parts[2],
                        "repo": parts[1],
                        "source_provider": "agent_sessions",
                    },
                    source="agent_sessions",
                )
            )
        return out

    def _known_repo_file_paths(self) -> set[str] | None:
        if self.repo_root is None:
            return None
        root = Path(self.repo_root)
        if not root.is_dir():
            return set()
        git_paths = self._known_repo_file_paths_from_git(root)
        if git_paths is not None:
            if not self.repo_file_include_untracked:
                return git_paths
            return git_paths | self._known_repo_file_paths_from_walk(root)
        return self._known_repo_file_paths_from_walk(root)

    def _known_repo_file_paths_from_walk(self, root: Path) -> set[str]:
        out: set[str] = set()
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                name
                for name in dirnames
                if name not in _REPO_FILE_SCAN_SKIP_DIRS
                and not self._repo_file_excluded(
                    ((Path(dirpath) / name).relative_to(root)).as_posix(),
                )
            ]
            base = Path(dirpath)
            for filename in filenames:
                path = base / filename
                try:
                    rel = path.relative_to(root).as_posix()
                except ValueError:
                    continue
                if not self._repo_file_excluded(rel):
                    out.add(rel)
        return out

    def _known_repo_file_paths_from_git(self, root: Path) -> set[str] | None:
        from okg.substrate.library.sources._git_helpers import (
            GitInvocationError,
            run_git,
        )

        try:
            output = run_git(root, ["ls-files", "--cached"])
        except (FileNotFoundError, GitInvocationError, OSError):
            return None
        return {
            line
            for line in output.splitlines()
            if line and not self._repo_file_excluded(line)
        }

    def _repo_file_excluded(self, rel_path: str) -> bool:
        return any(
            fnmatch.fnmatch(rel_path, pattern)
            for pattern in self.repo_file_exclude_globs
        )

    def _scoped_session_paths(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> tuple[Path, ...]:
        root = Path(self.claude_projects_root)
        direct: list[Path] = []
        jsonl_paths = resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".jsonl"],
            existing_only=False,
        )
        for jsonl_path in jsonl_paths:
            if self._is_subagent_transcript_path(jsonl_path):
                parent = self._parent_transcript_for_subagent(jsonl_path)
                if parent is not None:
                    direct.append(parent)
                continue
            direct.append(jsonl_path)
        meta_paths = resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".json"],
            existing_only=False,
        )
        for meta_path in meta_paths:
            parent = self._parent_transcript_for_meta(meta_path)
            if parent is not None:
                direct.append(parent)

        seen: set[Path] = set()
        out: list[Path] = []
        for path in direct:
            resolved = path.resolve()
            if resolved in seen or not self._session_path_is_candidate(resolved):
                continue
            seen.add(resolved)
            out.append(resolved)
        return tuple(out)

    def _scoped_session_record_keys(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> set[str]:
        root = Path(self.claude_projects_root)
        keys: set[str] = set()
        for path in resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".jsonl"],
            existing_only=False,
        ):
            target = path
            if self._is_subagent_transcript_path(path):
                target = self._parent_transcript_for_subagent(path)
                if target is None:
                    continue
            if self._session_path_is_candidate(target):
                keys.add(target.stem)
        meta_paths = resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".json"],
            existing_only=False,
        )
        for meta_path in meta_paths:
            parent = self._parent_transcript_for_meta(meta_path)
            if parent is not None and self._session_path_is_candidate(parent):
                keys.add(parent.stem)
        return keys

    def _scoped_session_cursor_keys(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> set[str]:
        root = Path(self.claude_projects_root)
        keys: set[str] = set()
        for path in resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".jsonl"],
            existing_only=False,
        ):
            target = path
            if self._is_subagent_transcript_path(path):
                target = self._parent_transcript_for_subagent(path)
                if target is None:
                    continue
            if self._session_path_is_candidate(target):
                keys.add(self._cursor_key_for_path(target))
        meta_paths = resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".json"],
            existing_only=False,
        )
        for meta_path in meta_paths:
            parent = self._parent_transcript_for_meta(meta_path)
            if parent is not None and self._session_path_is_candidate(parent):
                keys.add(self._cursor_key_for_path(parent))
        return keys

    def _scoped_nested_parent_state_keys(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> set[str]:
        root = Path(self.claude_projects_root)
        parents: list[Path] = []
        for path in resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".jsonl"],
            existing_only=False,
        ):
            if self._is_subagent_transcript_path(path):
                parent = self._parent_transcript_for_subagent(path)
                if parent is not None:
                    parents.append(parent)
        for path in resolve_scoped_paths(
            sync_scope,
            roots=[root],
            suffixes=[".json"],
            existing_only=False,
        ):
            parent = self._parent_transcript_for_meta(path)
            if parent is not None:
                parents.append(parent)

        keys: set[str] = set()
        for parent in parents:
            if not self._session_path_is_candidate(parent):
                continue
            keys.add(self._cursor_key_for_path(parent))
            keys.add(parent.stem)
        return keys

    def _cursor_key_for_path(self, path: Path) -> str:
        root = Path(self.claude_projects_root)
        try:
            return path.resolve().relative_to(root.resolve()).as_posix()
        except (OSError, ValueError):
            return str(path.resolve())

    def _session_path_is_candidate(self, path: Path) -> bool:
        root = Path(self.claude_projects_root).resolve()
        try:
            relative = path.resolve().relative_to(root)
        except (OSError, ValueError):
            return False
        if len(relative.parts) < 2:
            return False
        return is_claude_project_dir_candidate(
            root / relative.parts[0],
            projects_root=root,
            cwd_filter=self.cwd_filter or (),
            cwd_globs=self.cwd_globs or (),
            claude_worktree_roots=self.claude_worktree_roots,
        )

    @staticmethod
    def _is_subagent_transcript_path(path: Path) -> bool:
        return path.suffix == ".jsonl" and path.parent.name == "subagents"

    @classmethod
    def _parent_transcript_for_subagent(cls, subagent_path: Path) -> Path | None:
        if not cls._is_subagent_transcript_path(subagent_path):
            return None
        session_dir = subagent_path.parent.parent
        parent = session_dir.parent / f"{session_dir.name}.jsonl"
        return parent.resolve() if parent.is_file() else None

    @staticmethod
    def _parent_transcript_for_meta(meta_path: Path) -> Path | None:
        if not meta_path.name.endswith(".meta.json"):
            return None
        if meta_path.parent.name != "subagents":
            return None
        session_dir = meta_path.parent.parent
        parent = session_dir.parent / f"{session_dir.name}.jsonl"
        return parent.resolve() if parent.is_file() else None


def _content_manifest_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> dict[str, Any] | None:
    if not isinstance(cursor, dict):
        return None
    raw = cursor.get(_TRANSCRIPT_CONTENT_MANIFEST_KEY)
    if not isinstance(raw, dict):
        return None
    if raw.get("schema") != "okg.source.content_hash.item_manifest.v1":
        return None
    config_hash = raw.get("config_sha256")
    emit_hash = raw.get("emit_version_sha256")
    items = raw.get("items")
    if not (
        isinstance(config_hash, str)
        and len(config_hash) == 64
        and isinstance(emit_hash, str)
        and len(emit_hash) == 64
        and isinstance(items, dict)
    ):
        return None
    normalized_items: dict[str, str] = {}
    for key, value in items.items():
        if not (
            isinstance(key, str)
            and key
            and isinstance(value, str)
            and len(value) == 64
        ):
            return None
        normalized_items[key] = value
    raw_sizes = raw.get("sizes")
    normalized_sizes: dict[str, int] = {}
    if raw_sizes is not None:
        if not isinstance(raw_sizes, dict):
            return None
        for key, value in raw_sizes.items():
            if (
                not isinstance(key, str)
                or key not in normalized_items
                or not isinstance(value, int)
                or value < 0
            ):
                return None
            normalized_sizes[key] = value
    return {
        "schema": "okg.source.content_hash.item_manifest.v1",
        "config_sha256": config_hash,
        "emit_version_sha256": emit_hash,
        "items": normalized_items,
        "sizes": normalized_sizes,
    }


__all__ = ["AgentSessionsSource"]
