"""RemoteHostsFromSessions — substrate adapter for the cross-cutting
RemoteHost catalog mined from agent transcripts.

Wraps the domain `okg.modules.compute_env.remote_hosts_source.
RemoteHostsFromSessions` domain logic (scan
`<claude_projects_root>/<cwd>/<*.jsonl>` for `ssh user@host`
patterns in Bash tool calls; emit one RemoteHost node per
distinct hostname plus REFERENCES edges from each
`session:<sessionId>` that touched it).

State translation:
  - domain `state['per_session']`: per-jsonl size + last_scanned
    + cached observations, used only after the adapter's complete-content
    manifest proves the file unchanged.
  - substrate cursor: domain state plus the source-owned content manifest,
    round-tripped verbatim.

Profile: `reference_catalog`. Authoritative `release_new` replays read a
membership-complete immutable transcript snapshot and return the complete
source-owned graph footprint, so a host or session reference absent from that
snapshot can be retracted.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources._path_scope import (
    path_scope_requested,
    resolve_scoped_paths,
)
from okg.substrate.library.sources._claude_project_scope import (
    is_repo_local_claude_worktree_project_dir,
)
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.change_probe import ProbeVerdict


_TRANSCRIPT_CONTENT_MANIFEST_KEY = "transcript_content_manifest"
_CONTENT_MANIFEST_SCHEMA = "okg.source.content_hash.item_manifest.v1"


@dataclass
class RemoteHostsFromSessionsSource:
    """Substrate adapter for cross-session ssh-host catalog."""

    repo: str
    claude_projects_root: Path | str
    cwd_filter: Optional[list[Path | str]] = None
    idle_threshold_hours: int = 24

    name: str = "compute_env_remote_hosts"
    profile: str = "reference_catalog"
    probe_short_circuit_safe = True

    # External transcript writers do not enforce append-only mutation. The
    # sound floor is therefore a complete, bounded-memory content identity over
    # exactly the files the domain reader can enumerate.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        if isinstance(self.claude_projects_root, str):
            self.claude_projects_root = Path(self.claude_projects_root).expanduser()
        if self.cwd_filter:
            self.cwd_filter = [
                Path(p).expanduser() if isinstance(p, str) else p
                for p in self.cwd_filter
            ]
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources import _compute_env_remote_hosts_emit
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        probe = ContentHashProbe(
            content_items=self._transcript_items,
            config={
                "repo": self.repo,
                "claude_projects_root": str(self.claude_projects_root),
                "cwd_filter": (
                    sorted(str(p) for p in self.cwd_filter)
                    if self.cwd_filter else None
                ),
                "idle_threshold_hours": self.idle_threshold_hours,
            },
            emit_targets=emission_identity_targets(
                self.name,
                sys.modules[__name__],
                _compute_env_remote_hosts_emit,
            ),
        )
        self._transcript_probe = probe
        return probe

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
        from okg.substrate.library.sources._content_snapshot import (
            CapturedContentSnapshot,
        )
        from okg.substrate.library.sources._compute_env_remote_hosts_emit import (
            RemoteHostsFromSessions as DomainRemoteHostsFromSessions,
        )

        scoped = path_scope_requested(sync_scope)
        completed_scope = (
            not scoped
            and (
                authoritative_replay
                or mode in {"release_new", "scope_complete", "reconcile"}
            )
        )
        content_snapshot = CapturedContentSnapshot(
            (
                self._scoped_transcript_items(sync_scope)
                if scoped else self._transcript_items()
            ),
            mirror_relative_paths=True,
        )
        captured_token = self._transcript_probe.token_from_item_hashes(
            content_snapshot.item_hashes
        )
        source_state = dict(cursor) if cursor else {}
        prior_manifest = _content_manifest_from_cursor(cursor)
        source_state.pop(_TRANSCRIPT_CONTENT_MANIFEST_KEY, None)
        if scoped:
            # A path-scoped request intentionally bypasses the global probe. Its
            # selected transcript must not inherit a size-only cache entry.
            # Clearing the small per-session cache is conservative and keeps
            # partial-scope deletion/error handling sound.
            source_state["per_session"] = {}
            current_manifest = prior_manifest
        else:
            current_manifest = self._transcript_probe.manifest_from_item_hashes(
                content_snapshot.item_hashes,
                item_sizes=content_snapshot.item_sizes,
            )
            if self._global_reader_cache_is_unsafe(
                prior_manifest=prior_manifest,
                current_manifest=current_manifest,
                change_probe_verdict=change_probe_verdict,
            ) or completed_scope:
                # A completed replay must rebuild its complete RecordSet. The
                # probe gate normally skips an unchanged source before this
                # point, but forcing the reader off must remain correct too.
                source_state["per_session"] = {}
        try:
            domain = DomainRemoteHostsFromSessions(
                claude_projects_root=content_snapshot.root,
                cwd_filter=self.cwd_filter,
                idle_threshold_hours=self.idle_threshold_hours,
                state=source_state,
                session_files=[
                    item.snapshot_path for item in content_snapshot.items
                ],
            )

            nodes = list(domain.nodes())
            edges = list(domain.edges())
            domain_state = domain.get_run_state()
            applied_raw_token = None if scoped else captured_token
        finally:
            content_snapshot.close()
        if current_manifest is not None:
            domain_state[_TRANSCRIPT_CONTENT_MANIFEST_KEY] = current_manifest

        revision = {
            # Scoped runs deliberately withhold global acknowledgement, but
            # their facts still bind to the exact immutable bytes consumed.
            # Never use orchestration identity (run_id) as data revision.
            "content_sha256": captured_token["components"]["content"]["value"]
        }
        facts: list[NodeFact | EdgeFact] = []
        for n in nodes:
            facts.append(NodeFact(
                node_id=n.node_id,
                subtype=n.subtype,
                attrs={"label": n.label, **n.attrs},
                source_record_id={
                    "repo": self.repo,
                    "node_id": n.node_id,
                },
                source_revision=revision,
                op="I",
            ))
        for e in edges:
            facts.append(EdgeFact(
                src=e.src,
                dst=e.dst,
                edge_type=e.edge_type,
                attrs=dict(e.attrs),
                provenance=e.provenance,
                confidence=(e.confidence if e.confidence is not None else 1.0),
                source_record_id={
                    "repo": self.repo,
                    "src": e.src,
                    "dst": e.dst,
                    "edge_type": e.edge_type,
                },
                source_revision=revision,
                op="I",
            ))

        def _facts() -> Iterator[Any]:
            yield from facts

        return SourceRun(
            facts=_facts(),
            completed_scope=completed_scope,
            next_cursor=domain_state,
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global" if applied_raw_token is not None else "unavailable"
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=(
                self._record_set_for_facts(facts)
                if completed_scope else None
            ),
        )

    def _transcript_items(self) -> list[tuple[str, Path]]:
        # Mirror the domain's full-scope enumeration exactly: every *.jsonl
        # immediately under each eligible cwd directory. Membership is rebuilt
        # on every call so add/delete operations affect both probe and reader.
        root = Path(self.claude_projects_root)
        out: list[tuple[str, Path]] = []
        if not root.is_dir():
            return out
        for cwd_dir in sorted(root.iterdir()):
            if not cwd_dir.is_dir():
                continue
            if is_repo_local_claude_worktree_project_dir(cwd_dir):
                continue
            for jsonl in sorted(cwd_dir.glob("*.jsonl")):
                if jsonl.is_file():
                    out.append((
                        jsonl.relative_to(root).as_posix(),
                        jsonl,
                    ))
        return out

    @staticmethod
    def _record_set_for_facts(
        facts: list[NodeFact | EdgeFact],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        for fact in facts:
            if isinstance(fact, NodeFact):
                records[fact.node_id] = RecordEmission(
                    primary=RecordNode(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        source_record_id=dict(fact.source_record_id),
                        source_revision=dict(fact.source_revision),
                    )
                )
                continue
            record = records.setdefault(fact.dst, RecordEmission())
            record.edges.append(EdgeKey(
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

    @staticmethod
    def _global_reader_cache_is_unsafe(
        *,
        prior_manifest: Optional[dict[str, Any]],
        current_manifest: Optional[dict[str, Any]],
        change_probe_verdict: Optional[ProbeVerdict],
    ) -> bool:
        if prior_manifest is None or current_manifest != prior_manifest:
            return True
        if change_probe_verdict is not None:
            return change_probe_verdict.changed or not change_probe_verdict.token
        return False

    def _scoped_session_files(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> tuple[Path, ...]:
        return resolve_scoped_paths(
            sync_scope,
            roots=[Path(self.claude_projects_root)],
            suffixes=[".jsonl"],
        )

    def _scoped_transcript_items(
        self,
        sync_scope: Optional[dict[str, Any]],
    ) -> list[tuple[str, Path]]:
        root = Path(self.claude_projects_root).resolve()
        return [
            (path.resolve().relative_to(root).as_posix(), path)
            for path in self._scoped_session_files(sync_scope)
        ]


def _content_manifest_from_cursor(
    cursor: Optional[dict[str, Any]],
) -> Optional[dict[str, Any]]:
    if not isinstance(cursor, dict):
        return None
    manifest = cursor.get(_TRANSCRIPT_CONTENT_MANIFEST_KEY)
    if not isinstance(manifest, dict):
        return None
    if manifest.get("schema") != _CONTENT_MANIFEST_SCHEMA:
        return None
    config_hash = manifest.get("config_sha256")
    emit_hash = manifest.get("emit_version_sha256")
    items = manifest.get("items")
    sizes = manifest.get("sizes")
    if not (
        isinstance(config_hash, str)
        and len(config_hash) == 64
        and isinstance(emit_hash, str)
        and len(emit_hash) == 64
        and isinstance(items, dict)
        and isinstance(sizes, dict)
    ):
        return None
    normalized_items: dict[str, str] = {}
    normalized_sizes: dict[str, int] = {}
    for key, item_hash in items.items():
        item_size = sizes.get(key)
        if not (
            isinstance(key, str)
            and key
            and isinstance(item_hash, str)
            and len(item_hash) == 64
            and isinstance(item_size, int)
            and item_size >= 0
        ):
            return None
        normalized_items[key] = item_hash
        normalized_sizes[key] = item_size
    if set(sizes) != set(items):
        return None
    return {
        "schema": _CONTENT_MANIFEST_SCHEMA,
        "config_sha256": config_hash,
        "emit_version_sha256": emit_hash,
        "items": normalized_items,
        "sizes": normalized_sizes,
    }


__all__ = ["RemoteHostsFromSessionsSource"]
