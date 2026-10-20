"""Shared fixture-reader used by library adapters.

All 9 library adapters support running from a local JSON
fixture. Fixture shape:

    {
      "nodes": [
        {"node_id": "...", "subtype": "...", "attrs": {...},
         "source_record_id": {...}, "source_revision": {...}},
        ...
      ],
      "edges": [
        {"src": "...", "dst": "...", "edge_type": "...",
         "attrs": {...},
         "source_record_id": {...}, "source_revision": {...}},
        ...
      ]
    }

Minimal and flat — the adapter's own normalization (e.g.
GitHub API → NodeFact shape) happens in its real `_fetch_live`
path. Fixtures skip normalization; they're already in fact shape.
"""
from __future__ import annotations

import hashlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
)


def _record_set_for(facts: list[NodeFact | EdgeFact]) -> RecordSet:
    """Declare the fixture's full logical-record footprint.

    A fixture file IS the complete scope by construction, so on a
    full-scope walk every emitted fact belongs to the declared record
    set: each node is its own record (keyed by node_id), each edge its
    own record (keyed by identity). A fact that disappears from the
    fixture therefore drops its record and the runner synthesizes the
    retraction; an unchanged fixture re-emits an identical record set,
    which is what lets a zero-appended-output re-sync classify as an
    authoritative replay instead of an unclassified rejection (#826).
    """
    records: dict[str, RecordEmission] = {}
    for fact in facts:
        # An explicit retraction fact declares the record GONE; the record
        # set is the current footprint, and the runner's coverage check
        # requires every declared record to be present after the run.
        if getattr(fact, "op", "I") == "R":
            continue
        if isinstance(fact, NodeFact):
            records[fact.node_id] = RecordEmission(
                primary=RecordNode(
                    node_id=fact.node_id,
                    subtype=fact.subtype,
                    source_record_id=dict(fact.source_record_id),
                    source_revision=dict(fact.source_revision),
                    ownership_mode=fact.ownership_mode,
                )
            )
        else:
            key = f"edge:{fact.src}|{fact.edge_type}|{fact.dst}"
            records[key] = RecordEmission(
                edges=[
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
                ]
            )
    return RecordSet(records=records)


@dataclass
class FixtureBackedAdapter:
    """Mixin-style base for adapters that read from a fixture.

    Subclasses set `name`, `profile`, and optionally override
    `_normalize_node` / `_normalize_edge` to transform fixture
    rows into NodeFact / EdgeFact. The default implementation
    assumes the fixture is already in fact shape.

    Incremental-source contract (issue #323): a fixture is local
    bytes on disk, so every fixture-backed adapter inherits the
    ContentHashProbe floor — read + hash the fixture file. An edit
    changes the sha; a delete (the file vanishes) drops the single
    membership entry; both bust the token. The probe is content-based
    so it is immune to mtime churn. A vanished file reads as ``absent``
    (a distinct identity), so a delete-then-recreate is still observed.
    `change_probe_kind` is declared here AND restated on each concrete
    registered subclass, because `deployment_lint` reads the attribute
    via AST on the *registered* class's own body (it does not walk
    bases). The runtime `self.change_probe` is inherited from here.
    """

    name: str
    profile: str
    fixture_path: Path | str

    #: Issue #323 probe family. Concrete subclasses MUST restate this as
    #: their own class-level literal so the static `deployment_lint` AST
    #: reader (which inspects only the registered class body) sees it.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            # Re-enumerate live state on each call so a deletion is
            # observed as a dropped (key, sha) entry. A missing fixture
            # yields the empty scope, whose digest differs from any
            # present-file digest -> the delete busts the token.
            fixture = Path(self.fixture_path)
            return [(str(fixture), fixture)] if fixture.is_file() else []

        # Emit-version covers BOTH the shared base module (where `run`
        # actually emits facts) and the concrete subclass module (in case
        # a subclass adds emission logic), so a change to either bumps the
        # token with no manual version field.
        emit_modules = [sys.modules[__name__]]
        own_module = sys.modules.get(type(self).__module__)
        if own_module is not None and own_module not in emit_modules:
            emit_modules.append(own_module)

        return ContentHashProbe(
            content_items=_items,
            config={
                "name": self.name,
                "profile": self.profile,
                "fixture_path": str(self.fixture_path),
            },
            emit_targets=emit_modules,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        sync_scope: dict[str, Any] | None = None,
    ) -> SourceRun:
        p = Path(self.fixture_path)
        try:
            raw_bytes = p.read_bytes()
        except FileNotFoundError as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path {p} not found",
                data={"adapter": self.name, "fixture_path": str(p)},
                fix_hint="pass a valid fixture_path or implement "
                         "production _fetch_live() for this adapter",
            ) from exc
        except OSError as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path could not be read",
                data={"adapter": self.name, "fixture_path": str(p)},
                fix_hint="make fixture_path readable or implement "
                         "production _fetch_live() for this adapter",
            ) from exc
        try:
            doc = json.loads(raw_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SubstrateError(
                f"source {self.name!r}: fixture_path is not valid JSON",
                data={"adapter": self.name, "fixture_path": str(p)},
                fix_hint="replace fixture_path with a valid JSON fixture",
            ) from exc

        scope_keys = {
            str(value)
            for value in (sync_scope or {}).get("scope_keys", ())
            if value
        }
        scoped = bool(scope_keys)
        applied_raw_token = None
        if not scoped:
            applied_raw_token = self.change_probe.token_from_item_hashes({
                str(p): hashlib.sha256(raw_bytes).hexdigest(),
            })

        def _node_in_scope(raw: dict[str, Any]) -> bool:
            if not scope_keys:
                return True
            record_id = raw.get("source_record_id") or {}
            candidates = {
                str(record_id.get("path") or ""),
                str(record_id.get("id") or ""),
                str(raw.get("node_id") or ""),
            }
            return bool(scope_keys & candidates)

        def _edge_in_scope(raw: dict[str, Any]) -> bool:
            if not scope_keys:
                return True
            record_id = raw.get("source_record_id") or {}
            candidates = {
                str(record_id.get("path") or ""),
                str(record_id.get("id") or ""),
                str(record_id.get("edge") or ""),
            }
            return bool(scope_keys & candidates)

        def _stream() -> Iterator[NodeFact | EdgeFact]:
            for n in doc.get("nodes", []):
                if not _node_in_scope(n):
                    continue
                yield NodeFact(
                    node_id=n["node_id"],
                    subtype=n["subtype"],
                    attrs=n.get("attrs", {}),
                    source_record_id=n.get("source_record_id",
                                           {"id": n["node_id"]}),
                    source_revision=n.get("source_revision", {"v": 1}),
                    op=n.get("op", "I"),
                    # A node several sources emit (a person, say) declares
                    # "substitute": one row, a claim per source, and coverage
                    # accepts it under any owner (pipeline.md, shared nodes).
                    ownership_mode=n.get("ownership_mode", "authoritative"),
                )
            for e in doc.get("edges", []):
                if not _edge_in_scope(e):
                    continue
                yield EdgeFact(
                    src=e["src"],
                    dst=e["dst"],
                    edge_type=e["edge_type"],
                    attrs=e.get("attrs", {}),
                    provenance=e.get("provenance", "explicit"),
                    confidence=float(e.get("confidence", 1.0)),
                    source_record_id=e.get("source_record_id",
                                           {"edge": f"{e['src']}|"
                                            f"{e['edge_type']}|"
                                            f"{e['dst']}"}),
                    source_revision=e.get("source_revision", {"v": 1}),
                    op=e.get("op", "I"),
                )

        # Modes that represent a full-scope walk. A fixture file
        # IS the full scope by construction, so the caller's
        # assertion of "scope walk" mode is honored. Full-scope walks
        # also declare their record set so an unchanged re-sync is an
        # authoritative replay, not an unclassifiable zero-output run
        # (#826); scoped/cursor runs keep the lazy stream and declare
        # no record-set authority.
        _full_walk_modes = {
            "reconcile", "scope_complete", "release_new",
        }
        if not scoped and mode in _full_walk_modes:
            facts = list(_stream())
            return SourceRun(
                facts=iter(facts),
                completed_scope=True,
                run_mode=mode,
                record_set=_record_set_for(facts),
                applied_raw_token=applied_raw_token,
                applied_token_scope="global",
                applied_token_authority="exact",
            )
        return SourceRun(
            facts=_stream(),
            completed_scope=False,
            run_mode=mode,
            applied_raw_token=applied_raw_token,
            applied_token_scope="unavailable" if scoped else "global",
            applied_token_authority="unavailable" if scoped else "exact",
        )

    def _fetch_live(
        self, run_id: str, *, mode: str,
    ) -> Iterable[NodeFact | EdgeFact]:
        """Override in real-network subclasses. Default raises.

        The stub lives in the individual adapter modules — each
        adapter documents its auth/pagination/rate-limit story
        there.
        """
        raise NotImplementedError(
            f"{self.name}: production HTTP client not implemented; "
            f"pass fixture_path= to run from a local snapshot"
        )
