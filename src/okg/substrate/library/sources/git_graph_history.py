"""GitHistorySource — substrate adapter for the commit DAG.

Wraps the domain
`okg.modules.git_graph.history_source.GitHistorySource` domain
logic (single streaming `git log` + `git for-each-ref`) in the
substrate `SourceAdapter` Protocol.

State translation:
  - domain `state['seen_commit_shas']`: unbounded list of SHAs
  - substrate cursor: capped at `seen_shas_cap` (default 10000)
    most recent. Documented behavior change vs domain. The
    `git rev-list <current> --not <previous>` computation
    handles the tail.

Profile: `discovery_crawl`.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

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
)


# Documented cap; configurable per-instance.
DEFAULT_SEEN_SHAS_CAP = 10000


@dataclass
class GitHistorySource:
    """Substrate adapter for git commit / branch / tag / author."""

    repo: str
    repo_root: Path | str
    include_remotes: bool = False
    history_since: Optional[str] = None
    branch_filter: Optional[list[str]] = None
    source_file_stub_globs: Optional[list[str]] = None
    source_file_exclude_globs: Optional[list[str]] = None
    detect_renames: bool = False
    # Opt-in commit diffs; see the domain `GitHistorySource` docstring. The
    # three optional ones default (None) to the domain's defaults.
    store_diffs: bool = False
    diff_max_file_bytes: Optional[int] = None
    diff_max_commit_bytes: Optional[int] = None
    diff_exclude_globs: Optional[list[str]] = None
    diffs_since: Optional[str] = None
    seen_shas_cap: int = DEFAULT_SEEN_SHAS_CAP

    name: str = "git_graph_history"
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True

    # Incremental-source contract (issue #323): a history walk's emission is a
    # function of the full set of ref TIPS (branches/tags/optionally remotes),
    # NOT the HEAD tree -- so a GitScopeProbe would be unsound (it would miss a
    # non-HEAD branch advancing). GitRefsProbe digests the ref tips
    # membership-sensitively, so a branch/tag advance, create, or delete busts
    # the token; walk-shaping config is folded into the config component.
    change_probe_kind = "git_refs"

    def __post_init__(self) -> None:
        if isinstance(self.repo_root, str):
            self.repo_root = Path(self.repo_root).expanduser()
        if self.diffs_since is not None:
            # YAML reads an unquoted date as a `date`; keep the string form.
            self.diffs_since = str(self.diffs_since)
        self.change_probe = self._build_change_probe()

    def _diff_params(self) -> dict[str, Any]:
        """The diff settings given, for the domain source and the probe.

        Empty when diffs are off, so a deployment that never turns them on
        keeps the same probe config (and token) as before diffs existed.
        """
        if not self.store_diffs:
            return {}
        params: dict[str, Any] = {"store_diffs": True}
        for name in (
            "diff_max_file_bytes",
            "diff_max_commit_bytes",
            "diff_exclude_globs",
            "diffs_since",
        ):
            value = getattr(self, name)
            if value is not None:
                params[name] = list(value) if isinstance(value, list) else value
        return params

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources import _git_graph_history_emit
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )
        from okg.substrate.library.sources.git_refs_probe import GitRefsProbe

        return GitRefsProbe(
            repo_root=Path(self.repo_root),
            config={
                "repo": self.repo,
                "include_remotes": self.include_remotes,
                "history_since": self.history_since,
                "branch_filter": (
                    list(self.branch_filter) if self.branch_filter else None
                ),
                "source_file_stub_globs": (
                    list(self.source_file_stub_globs)
                    if self.source_file_stub_globs else None
                ),
                "source_file_exclude_globs": (
                    list(self.source_file_exclude_globs)
                    if self.source_file_exclude_globs else None
                ),
                "detect_renames": self.detect_renames,
                **self._diff_params(),
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
                _git_graph_history_emit,
            ),
            include_remotes=self.include_remotes,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        from okg.substrate.library.sources._git_graph_history_emit import (
            GitHistorySource as DomainGitHistorySource,
        )

        # Honor cursor in incremental modes only. See parallel comment
        # in `git_graph_files.py`: cursor + scope_complete + the
        # substrate runner's record_set diff retracts everything the
        # cursor short-circuit didn't re-emit. Keep cursor out of
        # scope_complete/reconcile until the adapter can signal
        # "incremental record_set, do not diff" to the runner.
        state = (
            dict(cursor) if cursor and mode in {"cursor", "scope_incomplete"} else {}
        )
        domain = DomainGitHistorySource(
            repo_root=Path(self.repo_root),
            repo=self.repo,
            include_remotes=self.include_remotes,
            history_since=self.history_since,
            branch_filter=self.branch_filter,
            source_file_stub_globs=self.source_file_stub_globs,
            source_file_exclude_globs=self.source_file_exclude_globs,
            detect_renames=self.detect_renames,
            **self._diff_params(),
            state=state,
        )

        nodes = list(domain.nodes())
        edges = list(domain.edges())
        domain_state = domain.get_run_state()
        completed_scope = mode in {"reconcile", "scope_complete"}

        # Cap seen_commit_shas to the substrate's documented size.
        seen = domain_state.get("seen_commit_shas") or []
        if len(seen) > self.seen_shas_cap:
            # Keep the MOST RECENT N shas — domain stores
            # newest-last so trim from the front.
            seen = seen[-self.seen_shas_cap :]
            domain_state["seen_commit_shas"] = seen

        record_set = (
            self._record_set_from_emit(nodes, edges)
            if completed_scope else None
        )

        def _facts() -> Iterator[Any]:
            for n in nodes:
                attrs = {"label": n.label, **n.attrs}
                yield NodeFact(
                    node_id=n.node_id,
                    subtype=n.subtype,
                    attrs=attrs,
                    source_record_id={
                        "repo": self.repo,
                        "node_id": n.node_id,
                    },
                    source_revision=self._node_revision(n, attrs=attrs),
                    op="I",
                    ownership_mode=n.ownership_mode,
                )
            for e in edges:
                confidence = e.confidence if e.confidence is not None else 1.0
                yield EdgeFact(
                    src=e.src,
                    dst=e.dst,
                    edge_type=e.edge_type,
                    attrs=dict(e.attrs),
                    provenance=e.provenance,
                    confidence=confidence,
                    source_record_id={
                        "repo": self.repo,
                        "src": str(e.src),
                        "edge_type": str(e.edge_type),
                        "dst": str(e.dst),
                    },
                    source_revision=self._edge_revision(
                        e,
                        confidence=confidence,
                    ),
                    op="I",
                )

        snapshot_exact = bool(domain_state.get("snapshot_exact"))
        # Per-run stats, replaced every run. The runner adds
        # `input_advanced_during_run` when it acknowledges this run's sealed
        # snapshot although the live refs moved after it was taken.
        self.last_run_stats = {"snapshot_exact": snapshot_exact}

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=domain_state,
            applied_raw_token=(
                self.change_probe.token_for_ref_tips(
                    domain_state["raw_ref_bindings"]
                )
                if snapshot_exact
                else None
            ),
            # With snapshot_exact the emitter listed the ref tips once and
            # walked history only from those pinned tips; commits are
            # immutable, so the token over those tips stays true for the facts
            # emitted even if another checkout moves a ref mid-run.
            applied_token_binding=(
                APPLIED_TOKEN_BINDING_SEALED_SNAPSHOT
                if snapshot_exact
                else APPLIED_TOKEN_BINDING_UNVERIFIED
            ),
            applied_token_scope=(
                "global"
                if domain_state.get("snapshot_exact")
                else "unavailable"
            ),
            applied_token_authority=(
                "exact"
                if domain_state.get("snapshot_exact")
                else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
        )

    def _record_set_from_emit(
        self,
        nodes: list[Any],
        edges: list[Any],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}

        for node in nodes:
            node_id = str(node.node_id)
            records[node_id] = RecordEmission(
                primary=RecordNode(
                    node_id=node_id,
                    subtype=str(node.subtype),
                    source_record_id={
                        "repo": self.repo,
                        "node_id": node_id,
                    },
                    source_revision=self._node_revision(
                        node,
                        attrs={"label": node.label, **node.attrs},
                    ),
                    ownership_mode=node.ownership_mode,
                )
            )

        for edge in edges:
            record = records.setdefault(str(edge.src), RecordEmission())
            record.edges.append(
                EdgeKey(
                    src=str(edge.src),
                    dst=str(edge.dst),
                    edge_type=str(edge.edge_type),
                    attrs=dict(edge.attrs),
                    provenance=edge.provenance,
                    confidence=(
                        edge.confidence if edge.confidence is not None else 1.0
                    ),
                    source_record_id={
                        "repo": self.repo,
                        "src": str(edge.src),
                        "edge_type": str(edge.edge_type),
                        "dst": str(edge.dst),
                    },
                    source_revision=self._edge_revision(
                        edge,
                        confidence=(
                            edge.confidence
                            if edge.confidence is not None
                            else 1.0
                        ),
                    ),
                )
            )

        return RecordSet(records=records)

    @staticmethod
    def _content_revision(payload: dict[str, Any]) -> dict[str, Any]:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return {"content_sha256": hashlib.sha256(encoded).hexdigest()}

    def _node_revision(
        self,
        node: Any,
        *,
        attrs: dict[str, Any],
    ) -> dict[str, Any]:
        return self._content_revision(
            {
                "kind": "node",
                "node_id": str(node.node_id),
                "subtype": str(node.subtype),
                "attrs": attrs,
            }
        )

    def _edge_revision(
        self,
        edge: Any,
        *,
        confidence: float,
    ) -> dict[str, Any]:
        return self._content_revision(
            {
                "kind": "edge",
                "src": str(edge.src),
                "dst": str(edge.dst),
                "edge_type": str(edge.edge_type),
                "attrs": dict(edge.attrs),
                "provenance": edge.provenance,
                "confidence": confidence,
            }
        )


__all__ = ["DEFAULT_SEEN_SHAS_CAP", "GitHistorySource"]
