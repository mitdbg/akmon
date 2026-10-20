"""OpenSearchCatalogSource -- model a search cluster, not its contents.

PACT `opensearch-reference-integration` (issue #2014). The OKG does not
store logs. It models the systems that hold them, so an agent can find the
right index and then ask that system directly.

One run emits:

  - one `search_cluster` node per cluster
  - one `search_index` node per in-scope index, plus a
    `search_cluster -[contains]-> search_index` edge
  - one `service` node per DECLARED service, plus a
    `service -[logs_to]-> search_index` edge per declared (service, index)
    pair

Services are declared, never discovered: OpenSearch has no idea which
process wrote to an index. The registry supplies them, and a declared
index that the cluster does not have is a hard error naming the index --
not a quietly dropped edge.

Identity is the index UUID, not its name. A rename keeps the UUID (one
attribute changes), a rollover mints a new one (a node is added, nothing
retracted), and a delete-then-recreate under the same name produces a
different one (the old node is retracted). Keying on the name gets all
three wrong.

Volatile metrics -- `doc_count`, `store_bytes`, cluster `health` -- are
recorded as attributes but kept OUT of the content hash. On a live cluster
they move constantly; hashing them would mint a new `source_revision` for
every node on every sync and churn the graph with no structural change.

Profile: `reference_catalog`. A cluster's index set is an authoritative,
slowly-changing catalog reconciled in full scope, so absence from a
completed scope implies retraction -- which is exactly why a run that
could not read the cluster raises instead of returning an empty scope.

Change probe: `content_hash` over the emitted payloads. It is worth being
plain about what this does and does not save. The cheapest thing this
source can ask the cluster IS the metadata read, so the probe costs the
same six requests the run does; what it saves is the downstream apply,
extraction and publish work when nothing moved. The probe digests the
FULL emitted payload -- volatile metrics included, unlike the revision
hash -- because a short circuit must prove identical emission, and a
probe that ignored the document counts would skip the run that was meant
to refresh them.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources._opensearch_client import (
    ClusterSnapshot,
    HttpMetadataProvider,
    MetadataProvider,
    RecordedMetadataProvider,
    content_revision,
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


class DeclaredServiceError(SubstrateError):
    """A declared service names an index the cluster does not have.

    Refused rather than skipped. A silently dropped `logs_to` edge would
    make the graph quietly wrong about who writes where, and the operator
    who mistyped an index name would never learn of it.
    """

    code = "opensearch_declared_index_unknown"


@dataclass(frozen=True)
class DeclaredService:
    """One service the operator says writes to some of these indices."""

    service_id: str
    name: str
    indices: tuple[str, ...]

    @classmethod
    def from_mapping(cls, raw: Any) -> "DeclaredService":
        if not isinstance(raw, dict):
            raise DeclaredServiceError(
                f"service entry must be a mapping, got {type(raw).__name__}",
                data={"entry": repr(raw)},
                fix_hint="declare each service as {service_id, name, indices}",
            )
        service_id = raw.get("service_id")
        if not service_id:
            raise DeclaredServiceError(
                f"service entry has no service_id: {raw!r}",
                data={"entry": dict(raw)},
                fix_hint="give every declared service a service_id",
            )
        indices = raw.get("indices") or ()
        if isinstance(indices, str):
            indices = (indices,)
        return cls(
            service_id=str(service_id),
            name=str(raw.get("name") or service_id),
            indices=tuple(str(i) for i in indices),
        )


@dataclass
class OpenSearchCatalogSource:
    """Adapter over any `MetadataProvider`.

    Exactly one of three ways to say where the metadata comes from:

      base_url     a live cluster, read over HTTP
      fixture_dir  recorded response bodies, replayed
      provider     an object, for a caller that already built one

    A deployment names `base_url` in its registry params; hosted CI names
    `fixture_dir`, which is the seam that lets these tests run the
    production parsing path with no container. Naming none, or more than
    one, is refused: a source that silently picked one would make its own
    data provenance a guess.

    `namespace` prefixes every node id, so two clusters in one deployment
    do not collide and this source's `service` nodes never collide with
    another source's.
    """

    namespace: str
    base_url: Optional[str] = None
    fixture_dir: Optional[Any] = None
    provider: Optional[MetadataProvider] = None
    services: Sequence[Any] = field(default_factory=tuple)
    index_exclude_globs: Sequence[str] = field(default_factory=tuple)
    timeout_seconds: float = 10.0
    name: str = "opensearch_catalog"
    profile: str = "reference_catalog"

    #: Incremental-source contract (issue #323). The content-hash floor
    #: over the payloads this source emits.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.provider = self._resolve_provider()
        self._services = tuple(
            DeclaredService.from_mapping(entry) if not isinstance(
                entry, DeclaredService
            ) else entry
            for entry in self.services
        )
        self.change_probe = self._build_change_probe()
        # The probe hashes exactly the bytes this source emits, volatile
        # attributes included, so an equal token does prove identical
        # emission.
        self.probe_short_circuit_safe = True

    def _resolve_provider(self) -> MetadataProvider:
        from pathlib import Path

        chosen = [
            name for name, value in (
                ("base_url", self.base_url),
                ("fixture_dir", self.fixture_dir),
                ("provider", self.provider),
            ) if value is not None
        ]
        if len(chosen) != 1:
            raise SubstrateError(
                f"source {self.name!r}: name exactly one of base_url, "
                f"fixture_dir, provider; got {chosen or 'none'}",
                data={"given": chosen},
                fix_hint=(
                    "set base_url for a live cluster, or fixture_dir to "
                    "replay recorded responses"
                ),
            )
        if self.provider is not None:
            return self.provider
        globs = tuple(str(g) for g in self.index_exclude_globs)
        if self.base_url is not None:
            return HttpMetadataProvider(
                base_url=str(self.base_url),
                timeout=self.timeout_seconds,
                excluded_globs=globs,
            )
        return RecordedMetadataProvider(
            root_dir=Path(str(self.fixture_dir)), excluded_globs=globs,
        )

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )
        from okg.substrate.library.sources.emission_identity import (
            emission_identity_targets,
        )

        def _items() -> list[tuple[str, bytes]]:
            """Re-read the cluster and render what a run would emit.

            Re-enumerated on every call, so a deleted index is observed as
            a dropped entry rather than as a stale digest.
            """
            return _probe_items(self._facts(self.provider.fetch()))

        return ContentHashProbe(
            content_items=_items,
            config={
                "namespace": self.namespace,
                "base_url": self.base_url,
                "index_exclude_globs": list(self.index_exclude_globs),
                "services": [
                    {
                        "service_id": s.service_id,
                        "name": s.name,
                        "indices": list(s.indices),
                    }
                    for s in self._services
                ],
            },
            emit_targets=emission_identity_targets(
                self.name, sys.modules[__name__],
            ),
        )

    # ---- identity -------------------------------------------------

    def _cluster_id(self, cluster_uuid: str) -> str:
        return f"{self.namespace}:search_cluster:{cluster_uuid}"

    def _index_id(self, index_uuid: str) -> str:
        return f"{self.namespace}:search_index:{index_uuid}"

    def _service_id(self, service_id: str) -> str:
        return f"{self.namespace}:service:{service_id}"

    # ---- fact stream ----------------------------------------------

    def _resolve_services(
        self, snapshot: ClusterSnapshot,
    ) -> dict[str, list[DeclaredService]]:
        """Map index UUID -> the services declared to write to it.

        Declared by NAME, because that is what an operator can write down;
        resolved to the UUID here so the edge carries the same identity
        the node does.
        """
        by_name = snapshot.by_name()
        out: dict[str, list[DeclaredService]] = {}
        for service in self._services:
            for index_name in service.indices:
                index = by_name.get(index_name)
                if index is None:
                    raise DeclaredServiceError(
                        f"service {service.service_id!r} declares index "
                        f"{index_name!r}, which this cluster does not have. "
                        f"Known indices: {sorted(by_name)}",
                        data={
                            "service_id": service.service_id,
                            "index": index_name,
                            "known": sorted(by_name),
                        },
                        fix_hint=(
                            "correct the index name in the source registry, "
                            "or create the index before ingesting"
                        ),
                    )
                out.setdefault(index.uuid, []).append(service)
        return out

    def _facts(self, snapshot: ClusterSnapshot) -> list[Any]:
        cluster = snapshot.cluster
        by_index = self._resolve_services(snapshot)
        ns = self.namespace

        cluster_node_id = self._cluster_id(cluster.uuid)
        cluster_rev = content_revision({
            **cluster.structural_payload(),
            # The declared scope rule is configuration, so it belongs in
            # the revision: narrowing or widening it changes what this
            # cluster node claims to model. The excluded COUNT does not --
            # a plugin minting a dated index every day would otherwise
            # churn the cluster's history.
            "excluded_globs": list(snapshot.excluded_globs),
        })
        facts: list[Any] = [
            NodeFact(
                node_id=cluster_node_id,
                subtype="search_cluster",
                attrs={
                    "search_cluster_id": cluster_node_id,
                    "name": cluster.name,
                    "label": cluster.name,
                    "cluster_uuid": cluster.uuid,
                    "version": cluster.version,
                    # volatile, recorded, not hashed
                    "health": cluster.health,
                    "node_count": cluster.node_count,
                    "index_count": len(snapshot.indices),
                    # What the cluster holds but the graph does not model.
                    # Surfaced rather than dropped in silence: a reader who
                    # counts three index nodes against a cluster of six
                    # should be able to see the rule that made the
                    # difference.
                    "excluded_index_count": len(snapshot.excluded),
                    "excluded_index_globs": list(snapshot.excluded_globs),
                    # Rows and fields the read declined to use. Volatile,
                    # so they do not mint a revision, but present: a
                    # dropped row nobody counts is a graph quietly
                    # disagreeing with its cluster.
                    "read_anomaly_count": snapshot.anomalies.total(),
                    "read_anomalies": snapshot.anomalies.as_dict(),
                    "namespace": ns,
                },
                source_record_id={
                    "namespace": ns, "cluster_uuid": cluster.uuid,
                },
                source_revision={"content_hash": cluster_rev},
            )
        ]

        for service in self._services:
            service_node_id = self._service_id(service.service_id)
            facts.append(
                NodeFact(
                    node_id=service_node_id,
                    subtype="service",
                    attrs={
                        "service_id": service_node_id,
                        "name": service.name,
                        "label": service.name,
                        "declared_service_id": service.service_id,
                        "namespace": ns,
                    },
                    source_record_id={
                        "namespace": ns, "service": service.service_id,
                    },
                    source_revision={
                        "content_hash": content_revision({
                            "service_id": service.service_id,
                            "name": service.name,
                            "indices": list(service.indices),
                        }),
                    },
                )
            )

        for index in snapshot.indices:
            index_node_id = self._index_id(index.uuid)
            index_rev = content_revision(index.structural_payload())
            record_id = {
                "namespace": ns,
                "cluster_uuid": cluster.uuid,
                "index_uuid": index.uuid,
            }
            writers = by_index.get(index.uuid, [])
            facts.append(
                NodeFact(
                    node_id=index_node_id,
                    subtype="search_index",
                    attrs={
                        "search_index_id": index_node_id,
                        "name": index.name,
                        "label": index.name,
                        "index_uuid": index.uuid,
                        "cluster": cluster.name,
                        "aliases": list(index.aliases),
                        "field_count": index.summary.total,
                        "field_summary": index.summary.as_list(),
                        "field_summary_truncated": index.summary.truncated,
                        "mapping_read": index.mapping_read,
                        "retention": index.retention,
                        "retention_read": index.retention_read,
                        # volatile, recorded, not hashed
                        "doc_count": index.doc_count,
                        "store_bytes": index.store_bytes,
                        # The cross-link rule's match field, and it holds
                        # NODE ids because that is what `service.service_id`
                        # holds on the other side of the match. It carried
                        # the bare declared id before, against a
                        # `declared_service_id` field the `service` module
                        # does not declare, so the rule could never fire.
                        "logged_by_refs": [
                            self._service_id(s.service_id) for s in writers
                        ],
                        # The human-readable form, kept separately rather
                        # than overloading the match field.
                        "logged_by": [s.service_id for s in writers],
                        "namespace": ns,
                    },
                    source_record_id=record_id,
                    source_revision={"content_hash": index_rev},
                )
            )
            facts.append(
                EdgeFact(
                    src=cluster_node_id,
                    dst=index_node_id,
                    edge_type="contains",
                    source_record_id=record_id,
                    source_revision={"content_hash": index_rev},
                )
            )
            for service in writers:
                facts.append(
                    EdgeFact(
                        src=self._service_id(service.service_id),
                        dst=index_node_id,
                        edge_type="logs_to",
                        attrs={"declared": True},
                        source_record_id={
                            **record_id, "service": service.service_id,
                        },
                        source_revision={"content_hash": index_rev},
                    )
                )
        return facts

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        sync_scope: Optional[dict] = None,
    ) -> SourceRun:
        """Read the cluster once and emit its whole footprint.

        A metadata read that fails raises out of here. It must never
        return `completed_scope=True` with nothing in it: under
        `missing_from_completed_scope` that would retract every node this
        source owns, so one network timeout would delete the cluster from
        the graph.
        """
        snapshot = self.provider.fetch()
        facts = self._facts(snapshot)
        # The exact identity of what this run consumed. The runner will not
        # let a source acknowledge a short circuit on a pre-read probe
        # token alone; only a token computed from the bytes actually
        # emitted proves that a later equal token means equal output.
        applied_raw_token = self.change_probe.token_from_item_hashes({
            key: hashlib.sha256(payload).hexdigest()
            for key, payload in _probe_items(facts)
        })
        return SourceRun(
            facts=iter(facts),
            completed_scope=True,
            run_mode=_policy_run_mode(mode),
            record_set=_record_set_for(facts),
            applied_raw_token=applied_raw_token,
            applied_token_scope="global",
            applied_token_authority="exact",
        )


#: The run modes a `reference_catalog` source is allowed to report.
#: `okg.substrate.ingest.runner` maps (profile, run_mode) onto a
#: retraction-safety policy and refuses a pair it does not know.
_CATALOG_RUN_MODES = frozenset({"release_new", "release_unchanged"})


def _policy_run_mode(mode: str) -> str:
    """Translate the caller's mode into one this profile has a policy for.

    The runner drives sources with generic mode names -- `cursor`,
    `scope_complete`, `reconcile` -- and an adapter that echoed them back
    produced `(reference_catalog, scope_complete)`, a pair with no
    retraction-safety policy, so the run failed at the point where
    retraction is decided. Echoing an argument you do not understand is
    how that happened.

    Every run of this source reads the whole catalog and declares its
    complete record set, so `release_new` is the honest answer: a new
    release of the catalog, against which absence means retraction. The
    `release_unchanged` case -- an upstream that has demonstrably not
    moved -- is handled before `run()` is reached, by the change probe.
    """
    return mode if mode in _CATALOG_RUN_MODES else "release_new"


def _probe_items(facts: Sequence[Any]) -> list[tuple[str, bytes]]:
    """Render the run's emitted payloads as (key, bytes) pairs.

    One renderer for both the change probe and the applied token, so the
    two can never disagree about what "this run emitted" means.
    """
    items: list[tuple[str, bytes]] = []
    for fact in facts:
        if isinstance(fact, NodeFact):
            key = f"node:{fact.node_id}"
        elif isinstance(fact, EdgeFact):
            key = f"edge:{fact.src}|{fact.edge_type}|{fact.dst}"
        else:
            continue
        items.append((
            key,
            json.dumps(
                fact.attrs, sort_keys=True, separators=(",", ":"),
            ).encode("utf-8"),
        ))
    return items


def _record_set_for(facts: Sequence[Any]) -> RecordSet:
    """Declare the run's full logical-record footprint.

    One read IS the complete scope, so every emitted fact belongs to the
    declared record set: each node is its own record keyed by node id,
    each edge its own record keyed by identity. A fact that disappears
    from the next read drops its record and the runner synthesizes the
    retraction.
    """
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
        elif isinstance(fact, EdgeFact):
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
