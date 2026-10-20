"""OpenSpecSource — substrate adapter for OpenSpec proposal artifacts.

Wraps the domain `okg.modules.openspec.source.OpenSpecSource`
domain logic (walk `openspec/changes/{,archive/}` + `specs/`,
parse the four file kinds per change, emit change / task /
delta / criterion / scenario / decision / risk nodes plus
structural CONTAINS / TARGETS / SUPERSEDES edges) in the
substrate `SourceAdapter` Protocol.

Cross-module: emits `change REFERENCES file:<repo>:<path>` for
each `@/path` and fenced-code path mention found in proposal
text. Resolves to a real `SourceFile` node only when git_graph
is also loaded — this adapter doesn't care.

Where the tree lives: OpenSpec keeps it at `<repo>/openspec/`, and proposal
text was written against that layout, so by default mentions resolve against
the parent of `openspec_root`. A repository that moved the tree (okg keeps it
at `history/openspec/`) passes `repo_root`: mentions then resolve against that
directory, and a mention of the tree's own files (`openspec/...`) is relocated
to where the tree now sits, so the `file:` edge names the path `git_graph`
emits. Node ids and node `path` attributes stay relative to the tree's parent
either way (`openspec/changes/<slug>`), so moving the tree does not re-key a
single node.

State translation:
  - domain `state['file_hashes']: dict[change_id, dict[file_path, sha256]]`
  - substrate cursor: same dict, round-tripped verbatim. The
    domain emitter skips a change if its file hashes match the
    prior run, so the cursor materially gates re-emission.

Profile: `discovery_crawl` (walk a scope, diff by content hash,
absence implies retraction).
"""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterator, Optional

from okg.substrate.library.sources.base import (
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
from okg.substrate.library.sources.change_probe import compose_token
from okg.substrate.library.sources.content_hash_probe import ContentHashProbe
from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._scope_root import (
    prior_record_count,
    require_listable_directory,
    walk_files_or_raise,
)


logger = logging.getLogger(__name__)

#: The tree's own directory name as proposal text spells it: OpenSpec keeps
#: the tree at `<repo>/openspec/`, and every mention was written that way.
AS_WRITTEN_TREE = "openspec"


def relocate_tree_path(relpath: str, tree_prefix: str) -> str:
    """Map an as-written repo path onto the tree's current location.

    Only a path inside the tree itself (``openspec`` or ``openspec/...``)
    moves; every other path is returned unchanged. With the default layout
    ``tree_prefix`` is ``openspec`` and this is the identity.
    """
    if relpath == AS_WRITTEN_TREE:
        return tree_prefix
    if relpath.startswith(AS_WRITTEN_TREE + "/"):
        return tree_prefix + relpath[len(AS_WRITTEN_TREE):]
    return relpath


@dataclass
class _OpenSpecInputSnapshot:
    openspec_root: Path
    external_reference_manifest: dict[str, dict[str, str]]
    token: dict[str, Any]
    _temporary_directory: tempfile.TemporaryDirectory[str]
    #: The live tree did not exist when it was read. The snapshot is then an
    #: empty directory; the run decides whether that is a real empty scope.
    live_root_missing: bool = False

    def close(self) -> None:
        self._temporary_directory.cleanup()


class _OpenSpecSnapshotProbe(ContentHashProbe):
    """Capture the exact files later consumed by the OpenSpec reader."""

    def __init__(
        self,
        *,
        repo: str,
        openspec_root: Path,
        repo_root: Path,
        tree_prefix: str,
        include_archived: bool,
        materialize_referenced_capabilities: bool,
        filter_unresolved_crossrefs: bool,
        emit_targets: list[Any],
    ) -> None:
        self._snapshot_repo = repo
        self._snapshot_root = openspec_root
        self._live_repo_root = repo_root
        self._tree_prefix = tree_prefix
        self._snapshot_include_archived = include_archived
        self._snapshot_filter_crossrefs = filter_unresolved_crossrefs
        super().__init__(
            content_items=lambda: (),
            config={
                "repo": repo,
                "include_archived": include_archived,
                "materialize_referenced_capabilities": (
                    materialize_referenced_capabilities
                ),
                "filter_unresolved_crossrefs": filter_unresolved_crossrefs,
                # Where the tree sits in the repository decides which files
                # the mentions resolve to, so it is part of the token.
                "tree_prefix": tree_prefix,
            },
            emit_targets=emit_targets,
        )

    def capture_snapshot(self) -> _OpenSpecInputSnapshot:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="okg-openspec-snapshot-"
        )
        snapshot_repo_root = Path(temporary_directory.name) / "repo"
        snapshot_root = snapshot_repo_root / "openspec"
        try:
            content_by_relpath, live_root_missing = self._read_live_openspec()
            # Always materialize the snapshot tree, so the reader sees an
            # empty tree (and reports removed records) rather than a missing
            # temporary path.
            snapshot_root.mkdir(parents=True, exist_ok=True)
            for relpath, content in content_by_relpath.items():
                snapshot_path = snapshot_root / relpath
                snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                snapshot_path.write_bytes(content)

            external_manifest, external_items = self._external_reference_items(
                content_by_relpath,
                snapshot_repo_root=snapshot_repo_root,
            )
            token_items = [
                (f"openspec::{relpath}", content)
                for relpath, content in content_by_relpath.items()
            ]
            token_items.extend(external_items)

            token = self._token_for_items(token_items)
            return _OpenSpecInputSnapshot(
                openspec_root=snapshot_root,
                external_reference_manifest=external_manifest,
                token=token,
                _temporary_directory=temporary_directory,
                live_root_missing=live_root_missing,
            )
        except Exception:
            temporary_directory.cleanup()
            raise

    def _read_live_openspec_content(self) -> dict[str, bytes]:
        return self._read_live_openspec()[0]

    def _read_live_openspec(self) -> tuple[dict[str, bytes], bool]:
        """The live tree's files, and whether the tree was missing.

        Issue #2892 review. A missing tree reads as empty and is reported as
        missing; the run decides whether that is a legitimate empty scope. A
        tree that is there but cannot be read, or vanishes while it is
        walked, raises: `rglob` would skip it silently, and an empty complete
        scope retracts everything published before.
        """
        if not self._snapshot_root.exists():
            return {}, True
        require_listable_directory(
            self._snapshot_root,
            source=f"openspec ({self._snapshot_repo})",
            role="openspec_root",
            reason_code="openspec_root_unreadable",
        )
        live_files = walk_files_or_raise(self._snapshot_root)
        return {
            live_path.relative_to(self._snapshot_root).as_posix(): (
                live_path.read_bytes()
            )
            for live_path in live_files
        }, False

    def _external_reference_items(
        self,
        content_by_relpath: dict[str, bytes],
        *,
        snapshot_repo_root: Path | None,
    ) -> tuple[dict[str, dict[str, str]], list[tuple[str, bytes]]]:
        if not self._snapshot_filter_crossrefs:
            return {}, []

        reference_paths = _external_reference_paths_from_content(
            content_by_relpath,
            include_archived=self._snapshot_include_archived,
        )
        live_repo_root = self._live_repo_root.resolve()
        external_manifest: dict[str, dict[str, str]] = {}
        token_items: list[tuple[str, bytes]] = []
        for record_key, relpaths in sorted(reference_paths.items()):
            references: dict[str, str] = {}
            for relpath in sorted(relpaths):
                # `relpath` is as written (tree at `openspec/`); the live
                # file is wherever the tree sits now. The snapshot below keeps
                # the as-written layout, which is what the edge filter reads.
                live_relpath = relocate_tree_path(relpath, self._tree_prefix)
                candidate = (live_repo_root / live_relpath).resolve()
                try:
                    in_repo = candidate.is_relative_to(live_repo_root)
                except ValueError:
                    in_repo = False
                if in_repo and candidate.is_file():
                    content = candidate.read_bytes()
                    identity = "present:" + hashlib.sha256(content).hexdigest()
                    if snapshot_repo_root is not None:
                        snapshot_path = snapshot_repo_root / relpath
                        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
                        snapshot_path.write_bytes(content)
                    token_content = content
                else:
                    identity = "missing"
                    token_content = b"okg:external-reference:missing"
                references[relpath] = identity
                token_items.append(
                    (f"external::{record_key}::{relpath}", token_content)
                )
            if references:
                external_manifest[record_key] = references
        return external_manifest, token_items

    def _token_for_items(
        self,
        items: list[tuple[str, bytes]],
    ) -> dict[str, Any]:
        digest = hashlib.sha256()
        ordered = sorted(items)
        digest.update(f"n={len(ordered)}".encode("utf-8"))
        for key, content in ordered:
            digest.update(b"\x00")
            digest.update(key.encode("utf-8"))
            digest.update(b"\x00")
            digest.update(hashlib.sha256(content).hexdigest().encode("ascii"))
        return compose_token(
            content={"kind": "content_hash", "value": digest.hexdigest()},
            config=self._config_hash,
            emit_version=self._emit_version,
        )

    def build_token(self) -> dict[str, Any]:
        content_by_relpath = self._read_live_openspec_content()
        _, external_items = self._external_reference_items(
            content_by_relpath,
            snapshot_repo_root=None,
        )
        token_items = [
            (f"openspec::{relpath}", content)
            for relpath, content in content_by_relpath.items()
        ]
        token_items.extend(external_items)
        return self._token_for_items(token_items)


@dataclass
class OpenSpecSource:
    """Substrate adapter for OpenSpec proposals + canonical specs.

    Constructor kwargs mirror the domain emitter so deployment
    `source_registry.yaml` configs port verbatim.
    """

    repo: str
    openspec_root: Path | str
    include_archived: bool = True
    materialize_referenced_capabilities: bool = False
    filter_unresolved_crossrefs: bool = False
    #: The repository proposal mentions resolve against. Omitted, it is the
    #: parent of ``openspec_root`` (the OpenSpec layout). Set it when the tree
    #: is not at ``<repo>/openspec/``; ``openspec_root`` must then lie inside
    #: it, or construction refuses.
    repo_root: Path | str | None = None
    #: Issue #2892 review. When true, a missing ``openspec_root`` is an empty
    #: scope even after records were published, so a complete run retracts
    #: them. When false (the default), a missing tree is an empty scope only
    #: while nothing is published; otherwise the run refuses and retracts
    #: nothing.
    openspec_root_optional: bool = False

    name: str = "openspec"
    profile: str = "discovery_crawl"
    probe_short_circuit_safe = True
    # Complete captured-content identity lets run() parse the exact bytes named
    # by its applied token, including external reference existence/content.
    change_probe_kind = "content_hash"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.openspec_root, str):
            self.openspec_root = Path(self.openspec_root).expanduser()
        if isinstance(self.repo_root, str):
            self.repo_root = Path(self.repo_root).expanduser()
        self._tree_prefix = self._resolve_tree_prefix()
        self.change_probe = self._build_change_probe()

    def _live_repo_root(self) -> Path:
        if self.repo_root is None:
            return Path(self.openspec_root).parent
        return Path(self.repo_root)

    def _resolve_tree_prefix(self) -> str:
        """The tree's repo-relative location, e.g. ``history/openspec``.

        Without ``repo_root`` it is ``openspec`` by construction. With one,
        ``openspec_root`` must sit inside it: a tree outside the repository
        has no repo-relative location for a ``file:`` edge to name, and
        guessing one would emit edges to paths no source produces.
        """
        if self.repo_root is None:
            return AS_WRITTEN_TREE
        tree = Path(self.openspec_root).expanduser().resolve()
        root = Path(self.repo_root).expanduser().resolve()
        try:
            prefix = tree.relative_to(root).as_posix()
        except ValueError:
            raise ValueError(
                f"openspec source: openspec_root {str(tree)!r} is not inside "
                f"repo_root {str(root)!r}; set repo_root to the repository "
                "that contains the tree, or omit it when the tree is at "
                "<repo>/openspec/"
            ) from None
        if prefix in ("", "."):
            raise ValueError(
                f"openspec source: openspec_root {str(tree)!r} is repo_root "
                "itself; point openspec_root at the tree directory"
            )
        return prefix

    def _relocate_file_edges(self, edges: list[Any]) -> list[Any]:
        """Point ``file:`` edges into the tree at where the tree now sits.

        Runs after the resolvability filter, which reads the snapshot's
        as-written layout. A no-op for the default layout.
        """
        if self._tree_prefix == AS_WRITTEN_TREE:
            return edges
        file_prefix = f"file:{self.repo}:"
        out = []
        for edge in edges:
            dst = str(edge.dst)
            if dst.startswith(file_prefix):
                moved = relocate_tree_path(
                    dst.removeprefix(file_prefix), self._tree_prefix,
                )
                if file_prefix + moved != dst:
                    edge = replace(edge, dst=file_prefix + moved)
            out.append(edge)
        return out

    def _build_change_probe(self):
        import sys

        from okg.substrate.library.sources import _openspec_emit
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        root = Path(self.openspec_root)
        emit_targets = emission_identity_targets(
            self.name,
            sys.modules[__name__],
            _openspec_emit,
        )
        return _OpenSpecSnapshotProbe(
            repo=self.repo,
            openspec_root=root,
            repo_root=self._live_repo_root(),
            tree_prefix=self._tree_prefix,
            include_archived=self.include_archived,
            materialize_referenced_capabilities=(
                self.materialize_referenced_capabilities
            ),
            filter_unresolved_crossrefs=self.filter_unresolved_crossrefs,
            emit_targets=emit_targets,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
        source_has_published_facts: bool | None = None,
    ) -> SourceRun:
        from okg.substrate.library.sources._openspec_emit import (
            OpenSpecSource as DomainOpenSpecSource,
        )

        scope_keys = self._scope_keys(sync_scope)
        scope_records = self._scope_records_from_keys(scope_keys)
        if scope_keys and not scope_records:
            self.last_run_stats = {
                "scope_status": "ignored_scope_noop",
                "scope_keys": len(scope_keys),
                "recognized_records": 0,
                "nodes_emitted": 0,
                "edges_emitted": 0,
                "stale_ids": 0,
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=False,
                next_cursor=_cursor_without_inline_record_set(cursor),
                run_mode=mode,
                record_set=None,
            )

        completed_scope = mode in {"reconcile", "scope_complete"}
        has_record_set_authority = cursor_has_record_set_authority(cursor)
        can_probe_completed_scope = (
            completed_scope and not scope_records and has_record_set_authority
        )
        source_state = (
            source_state_without_runner_keys(cursor)
            if cursor and (
                mode == "cursor" or scope_records or can_probe_completed_scope
            )
            else {}
        )
        input_snapshot = self.change_probe.capture_snapshot()
        if completed_scope and input_snapshot.live_root_missing:
            # Decided on what the capture saw, not on an earlier check: a
            # tree that vanishes between the two is never read as empty.
            try:
                self._accept_missing_tree(
                    cursor,
                    source_has_published_facts=source_has_published_facts,
                )
            except BaseException:
                input_snapshot.close()
                raise
        external_reference_manifest = (
            input_snapshot.external_reference_manifest
        )
        if self.filter_unresolved_crossrefs:
            prior_external_manifest = _external_manifest_from_cursor(source_state)
            changed_external_records = {
                record_key
                for record_key in (
                    set(prior_external_manifest) | set(external_reference_manifest)
                )
                if prior_external_manifest.get(record_key)
                != external_reference_manifest.get(record_key)
            }
            if changed_external_records:
                # The domain emitter's inner cursor is keyed only by OpenSpec
                # file hashes.  Invalidate the affected records so an external
                # file transition re-emits and re-filters their REFERENCES
                # edges even when proposal.md itself is unchanged.
                file_hashes = dict(source_state.get("file_hashes") or {})
                for record_key in changed_external_records:
                    file_hashes.pop(record_key, None)
                source_state["file_hashes"] = file_hashes
        if scope_records and mode in {"reconcile", "scope_complete"}:
            file_hashes = dict(source_state.get("file_hashes") or {})
            for record_key in scope_records:
                file_hashes.pop(record_key, None)
            source_state["file_hashes"] = file_hashes
        domain = DomainOpenSpecSource(
            openspec_root=input_snapshot.openspec_root,
            repo=self.repo,
            include_archived=self.include_archived,
            state=source_state,
            scope_records=scope_records or None,
        )

        # The OpenSpec domain emitter emits the same node ids when a
        # slug exists in both `changes/<slug>/` and
        # `changes/archive/<date>-<slug>/` (re-opened changes, e.g.
        # `rearchitect-on-postgres`). Collapse those before streaming
        # to the runner. Runner dedupe is chunk-local; leaving 10k+
        # domain facts to stream can split a duplicate pair across
        # chunks and make repeated clean runs alternate active/archive
        # attrs.
        nodes = _dedupe_domain_nodes(list(domain.nodes()))
        edges = _dedupe_domain_edges(list(domain.edges()))
        if self.materialize_referenced_capabilities:
            nodes = self._with_referenced_capability_nodes(nodes, edges)
        if self.filter_unresolved_crossrefs:
            edges = self._filter_resolvable_edges(
                edges,
                nodes,
                openspec_root=input_snapshot.openspec_root,
            )
        node_to_record = {
            node.node_id: self._record_key_for_node(node) for node in nodes
        }

        stale_ids = list(domain.stale_node_ids or ())
        domain_state = domain.get_run_state()
        if scope_records:
            domain_state = self._merge_scoped_file_hashes(
                source_state,
                current=domain_state,
                replacement_keys=scope_records,
            )
        if self.filter_unresolved_crossrefs:
            domain_state["external_reference_manifest"] = external_reference_manifest

        revision = {
            "source_state_sha256": hashlib.sha256(
                json.dumps(
                    domain_state,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        }
        if scope_records:
            nodes = [
                node
                for node in nodes
                if node_to_record.get(node.node_id) in scope_records
            ]
            scoped_node_ids = {node.node_id for node in nodes}
            edges = [
                edge
                for edge in edges
                if edge.src in scoped_node_ids or edge.dst in scoped_node_ids
            ]
            if self.filter_unresolved_crossrefs:
                edges = self._filter_resolvable_edges(
                    edges,
                    nodes,
                    openspec_root=input_snapshot.openspec_root,
                )
            stale_ids = []
        # After every filter: the filter reads the snapshot's as-written
        # layout, and the record set below must see the edges as emitted.
        edges = self._relocate_file_edges(edges)
        # Issue #2892. With no prior file hashes the domain emitter compared
        # against nothing, so what it emitted is every record in scope, even
        # when that is none. That is the authoritative replay the runner asks
        # for after the pipeline identity changes (for example when
        # `add-source` updates the access policy). Only a run that skipped
        # records because their hashes matched may carry the prior record set
        # forward instead.
        full_enumeration = completed_scope and not scope_records and not (
            source_state.get("file_hashes")
        )
        record_set = None
        record_set_replacement_keys = None
        if completed_scope and (
            nodes
            or edges
            or stale_ids
            or not has_record_set_authority
            or full_enumeration
        ):
            record_set = self._record_set_from_emit(nodes, edges, revision=revision)
        if (
            completed_scope
            and not scope_records
            and has_record_set_authority
            and record_set is not None
            and not full_enumeration
        ):
            record_set_replacement_keys = self._changed_record_keys(
                source_state,
                current=domain_state,
                emitted_keys=set(record_set.records),
            )
        if completed_scope and scope_records:
            merged_record_set = self._scoped_record_set(
                cursor,
                current=record_set or RecordSet(),
                replacement_keys=scope_records,
            )
            if merged_record_set is not None:
                record_set = merged_record_set
            elif has_record_set_authority and record_set is not None:
                record_set_replacement_keys = set(scope_records)
            else:
                record_set = merged_record_set
                completed_scope = False

        self.last_run_stats = {
            "scope_status": "scoped" if scope_records else "full",
            "scope_keys": len(scope_keys),
            "recognized_records": len(scope_records),
            "nodes_emitted": len(nodes),
            "edges_emitted": len(edges),
            "stale_ids": len(stale_ids),
        }
        applied_raw_token = (
            input_snapshot.token
            if not scope_records
            else None
        )
        input_snapshot.close()

        def _facts() -> Iterator[Any]:
            for n in nodes:
                yield NodeFact(
                    node_id=n.node_id,
                    subtype=n.subtype,
                    attrs={"label": n.label, **n.attrs},
                    source_record_id={
                        "repo": self.repo,
                        "node_id": n.node_id,
                    },
                    source_revision=revision,
                    op="I",
                )
            for e in edges:
                edge_record_id = self._edge_source_record_id(
                    e,
                    node_to_record=node_to_record,
                )
                yield EdgeFact(
                    src=e.src,
                    dst=e.dst,
                    edge_type=e.edge_type,
                    attrs=dict(e.attrs),
                    provenance=e.provenance,
                    confidence=(e.confidence if e.confidence is not None else 1.0),
                    source_record_id=edge_record_id,
                    source_revision=revision,
                    op="I",
                )
            if mode in {"reconcile", "scope_complete"}:
                for stale_id in stale_ids:
                    # The domain emitter records change-level stale ids
                    # without preserving subtype; emit as the most
                    # specific kind we can infer from the prefix.
                    subtype = (
                        "spec_capability"
                        if stale_id.startswith("cap:")
                        else "open_spec_change"
                    )
                    yield NodeFact(
                        node_id=stale_id,
                        subtype=subtype,
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
                preserve_cursor_record_set_authority(domain_state, cursor)
                if completed_scope
                and has_record_set_authority
                and record_set is None
                else domain_state
            ),
            applied_raw_token=applied_raw_token,
            applied_token_scope=(
                "global"
                if applied_raw_token is not None
                else ("scoped" if scope_records else "unavailable")
            ),
            applied_token_authority=(
                "exact" if applied_raw_token is not None else "unavailable"
            ),
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=record_set_replacement_keys,
        )

    def _accept_missing_tree(
        self,
        cursor: Optional[dict[str, Any]],
        *,
        source_has_published_facts: bool | None,
    ) -> None:
        """Refuse a complete run over a missing tree unless it is truly empty.

        Issue #2892 review. A missing tree is an empty scope only when the
        repository around it can be read and either the registry marks the
        tree optional or the runner reports that this source has no facts in
        the fact log. The cursor is not evidence: a reset cursor, or a record
        set the runner stripped as unverified, would read as "nothing
        published" while live rows exist. A caller that does not say
        (``None``) is treated as having published.
        """
        root = Path(self.openspec_root)
        require_listable_directory(
            self._live_repo_root(),
            source=f"openspec ({self.repo})",
            role="repository root",
            reason_code="openspec_repo_root_unreadable",
        )
        if self.openspec_root_optional:
            published = prior_record_count(cursor)
            logger.warning(
                "openspec (%s): openspec_root %s is missing and "
                "openspec_root_optional is set; this complete run removes "
                "every openspec record this source published (%s in the "
                "prior record set)",
                self.repo,
                str(root),
                "an unknown number" if published is None else published,
            )
            return
        if source_has_published_facts is False:
            return
        raise SubstrateError(
            f"openspec ({self.repo}): openspec_root {str(root)!r} is missing "
            "but this source has published facts; refusing to retract them",
            data={
                "source": f"openspec ({self.repo})",
                "path": str(root),
                "source_has_published_facts": source_has_published_facts,
                "reason_code": "openspec_root_missing_with_published_records",
            },
            fix_hint=(
                "restore the openspec tree, correct openspec_root in "
                "source_registry.yaml, or set openspec_root_optional: true "
                "in the source's params to remove its records on purpose"
            ),
        )

    @staticmethod
    def _changed_record_keys(
        prior_state: dict[str, Any],
        *,
        current: dict[str, Any],
        emitted_keys: set[str] | None = None,
    ) -> set[str]:
        prior_hashes = dict(prior_state.get("file_hashes") or {})
        current_hashes = dict(current.get("file_hashes") or {})
        changed = {
            key
            for key, value in current_hashes.items()
            if prior_hashes.get(key) != value
        }
        removed = set(prior_hashes) - set(current_hashes)
        return set(emitted_keys or ()) | changed | removed

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

    def _with_referenced_capability_nodes(
        self,
        nodes: list[Any],
        edges: list[Any],
    ) -> list[Any]:
        from okg.substrate.library.sources.source_specs import NodeSpec

        existing = {node.node_id for node in nodes}
        out = list(nodes)
        for edge in edges:
            dst = str(edge.dst)
            if (
                edge.edge_type != "targets"
                or not dst.startswith("cap:")
                or dst in existing
            ):
                continue
            capability_name = dst.removeprefix("cap:")
            existing.add(dst)
            out.append(
                NodeSpec(
                    node_id=dst,
                    subtype="spec_capability",
                    label=capability_name,
                    attrs={
                        "capability_name": capability_name,
                        "purpose": "",
                        "requirement_count": 0,
                        "text": capability_name,
                        "source_subtype": "openspec_referenced_capability",
                    },
                    source="openspec",
                )
            )
        return out

    def _filter_resolvable_edges(
        self,
        edges: list[Any],
        nodes: list[Any],
        *,
        openspec_root: Path | None = None,
    ) -> list[Any]:
        node_ids = {node.node_id for node in nodes}
        return [
            edge
            for edge in edges
            if self._endpoint_resolvable(
                edge.src,
                node_ids,
                openspec_root=openspec_root,
            )
            and self._endpoint_resolvable(
                edge.dst,
                node_ids,
                openspec_root=openspec_root,
            )
        ]

    def _endpoint_resolvable(
        self,
        node_id: Any,
        node_ids: set[str],
        *,
        openspec_root: Path | None = None,
    ) -> bool:
        raw = str(node_id)
        if raw in node_ids:
            return True
        file_prefix = f"file:{self.repo}:"
        if raw.startswith(file_prefix):
            relpath = raw.removeprefix(file_prefix)
            repo_root = Path(openspec_root or self.openspec_root).parent.resolve()
            candidate = (repo_root / relpath).resolve()
            try:
                if not candidate.is_relative_to(repo_root):
                    return False
            except ValueError:
                return False
            return candidate.is_file()
        return False

    @staticmethod
    def _merge_scoped_file_hashes(
        prior_state: dict[str, Any],
        *,
        current: dict[str, Any],
        replacement_keys: set[str],
    ) -> dict[str, Any]:
        merged = _cursor_without_inline_record_set(prior_state)
        prior_hashes = dict(merged.get("file_hashes") or {})
        current_hashes = dict(current.get("file_hashes") or {})
        for key in replacement_keys:
            prior_hashes.pop(key, None)
        prior_hashes.update(current_hashes)
        merged["file_hashes"] = prior_hashes
        return merged

    @staticmethod
    def _scope_keys(sync_scope: Optional[dict[str, Any]]) -> list[str]:
        return [
            str(raw_key).replace("\\", "/").strip("/")
            for raw_key in (sync_scope or {}).get("scope_keys", ())
            if str(raw_key).strip()
        ]

    @staticmethod
    def _scope_records_from_keys(scope_keys: list[str]) -> set[str]:
        records: set[str] = set()
        for key in scope_keys:
            parts = key.split("/")
            if len(parts) >= 3 and parts[0] == "changes":
                if parts[1] == "archive" and len(parts) >= 4:
                    records.add(_archive_slug(parts[2]))
                else:
                    records.add(parts[1])
            elif len(parts) >= 2 and parts[0] == "specs":
                records.add(f"cap:{parts[1]}")
            elif len(parts) >= 3 and parts[0] == "hypotheses":
                if parts[1] == "archive" and len(parts) >= 4:
                    records.add(f"hyp:{_archive_slug(parts[2])}")
                else:
                    records.add(f"hyp:{parts[1]}")
            elif len(parts) >= 2 and parts[0] == "observations":
                if parts[1] == "archive" and len(parts) >= 3:
                    records.add(f"obs:{_archive_slug(Path(parts[2]).stem)}")
                else:
                    records.add(f"obs:{Path(parts[1]).stem}")
            elif len(parts) >= 2 and parts[0] == "insights":
                if parts[1] == "archive" and len(parts) >= 3:
                    records.add(f"insight:{_archive_slug(Path(parts[2]).stem)}")
                else:
                    records.add(f"insight:{Path(parts[1]).stem}")
        return records

    def _record_set_from_emit(
        self,
        nodes: list[Any],
        edges: list[Any],
        *,
        revision: dict[str, Any],
    ) -> RecordSet:
        records: dict[str, RecordEmission] = {}
        node_to_record: dict[str, str] = {}
        for node in nodes:
            record_key = self._record_key_for_node(node)
            if record_key is None:
                continue
            node_to_record[node.node_id] = record_key
            emission = records.setdefault(record_key, RecordEmission())
            record_node = RecordNode(
                node_id=node.node_id,
                subtype=node.subtype,
                source_record_id={
                    "repo": self.repo,
                    "node_id": node.node_id,
                },
                source_revision=revision,
            )
            if node.node_id.startswith(("change:", "cap:", "hyp:", "obs:", "insight:")):
                records[record_key] = RecordEmission(
                    primary=record_node,
                    descendants=emission.descendants,
                    edges=emission.edges,
                )
            else:
                emission.descendants.append(record_node)

        for edge in edges:
            record_key = node_to_record.get(edge.src) or node_to_record.get(edge.dst)
            if record_key is None:
                continue
            records.setdefault(record_key, RecordEmission()).edges.append(
                EdgeKey(
                    src=edge.src,
                    dst=edge.dst,
                    edge_type=edge.edge_type,
                    attrs=dict(edge.attrs),
                    provenance=edge.provenance,
                    confidence=(
                        edge.confidence if edge.confidence is not None else 1.0
                    ),
                    source_record_id=self._edge_source_record_id(
                        edge,
                        node_to_record=node_to_record,
                    ),
                    source_revision=revision,
                )
            )
        return RecordSet(records=records)

    def _edge_source_record_id(
        self,
        edge: Any,
        *,
        node_to_record: dict[str, str | None],
    ) -> dict[str, str]:
        record_node_id = (
            node_to_record.get(edge.src) or node_to_record.get(edge.dst) or edge.src
        )
        return {
            "repo": self.repo,
            "node_id": record_node_id,
            "src": edge.src,
            "dst": edge.dst,
            "edge_type": edge.edge_type,
        }

    @staticmethod
    def _record_key_for_node(node: Any) -> str | None:
        node_id = node.node_id
        attrs = getattr(node, "attrs", {}) or {}
        if node_id.startswith("change:"):
            return str(attrs.get("change_id") or node_id.removeprefix("change:"))
        if node_id.startswith("cap:"):
            return node_id
        if node_id.startswith(("hyp:", "obs:", "insight:")):
            return node_id
        change_id = attrs.get("change_id")
        if change_id:
            return str(change_id)
        capability_name = attrs.get("capability_name")
        if capability_name and (
            node_id.startswith("crit:") or node_id.startswith("scn:")
        ):
            parts = node_id.split(":")
            if len(parts) >= 3 and parts[1] == capability_name:
                return f"cap:{capability_name}"
        for prefix in (
            "task-group:",
            "task:",
            "verify:",
            "goal:",
            "nongoal:",
            "stakeholder:",
            "decision:",
            "alt:",
            "risk:",
            "delta:",
            "crit:",
            "scn:",
        ):
            if node_id.startswith(prefix):
                rest = node_id.removeprefix(prefix)
                slug = rest.split(":", 1)[0]
                return slug or None
        return None


def _dedupe_domain_nodes(nodes: list[Any]) -> list[Any]:
    by_id: dict[str, Any] = {}
    for node in nodes:
        by_id[node.node_id] = node
    return list(by_id.values())


def _dedupe_domain_edges(edges: list[Any]) -> list[Any]:
    by_key: dict[tuple[str, str, str], Any] = {}
    for edge in edges:
        by_key[(edge.src, edge.dst, edge.edge_type)] = edge
    return list(by_key.values())


def _external_reference_paths_from_content(
    content_by_relpath: dict[str, bytes],
    *,
    include_archived: bool,
) -> dict[str, set[str]]:
    """Resolve proposal references from the exact captured proposal bytes."""
    from okg.substrate.library.sources._openspec_emit import (
        OpenSpecSource as DomainOpenSpecSource,
    )
    from okg.substrate.library.sources._openspec_parse import (
        extract_path_mentions,
        parse_proposal_text,
    )

    active: dict[str, str] = {}
    archived: list[tuple[str, str]] = []
    for relpath in content_by_relpath:
        parts = Path(relpath).parts
        if (
            len(parts) == 3
            and parts[0] == "changes"
            and parts[2] == "proposal.md"
            and parts[1] != "archive"
        ):
            active[parts[1]] = relpath
        elif (
            len(parts) == 4
            and parts[:2] == ("changes", "archive")
            and parts[3] == "proposal.md"
        ):
            archived.append((_archive_slug(parts[2]), relpath))

    selected = dict(active)
    if include_archived:
        for slug, relpath in sorted(archived):
            if slug not in active:
                selected[slug] = relpath

    references: dict[str, set[str]] = {}
    for record_key, relpath in sorted(selected.items()):
        try:
            text = content_by_relpath[relpath].decode("utf-8")
        except UnicodeDecodeError:
            continue
        proposal = parse_proposal_text(
            text,
            fallback_title=Path(relpath).parent.name,
        )
        if proposal is None:
            continue
        full_text = "\n".join(
            [
                proposal.why_text,
                proposal.what_changes_text,
                proposal.impact_text,
            ]
        )
        proposal_dir_rel = Path("openspec") / Path(relpath).parent
        for mention in extract_path_mentions(full_text):
            normalized = DomainOpenSpecSource._normalize_path_mention(
                mention,
                proposal_dir_rel,
            )
            if normalized is not None:
                references.setdefault(record_key, set()).add(normalized)
    return references


def _cursor_without_inline_record_set(
    cursor: Optional[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(cursor, dict):
        return {}
    return {str(key): value for key, value in cursor.items() if key != "record_set"}


def _external_manifest_from_cursor(
    cursor: dict[str, Any],
) -> dict[str, dict[str, str]]:
    raw = cursor.get("external_reference_manifest")
    if not isinstance(raw, dict):
        return {}
    return {
        str(record_key): {
            str(path): str(identity)
            for path, identity in references.items()
        }
        for record_key, references in raw.items()
        if isinstance(references, dict)
    }


def _archive_slug(name: str) -> str:
    if (
        len(name) > 11
        and name[4:5] == "-"
        and name[7:8] == "-"
        and name[10:11] == "-"
    ):
        return name[11:]
    return name


__all__ = ["OpenSpecSource"]
