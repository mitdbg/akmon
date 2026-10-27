"""Substrate-side adapter contract for the views framework.

``ViewMaterializer`` is the deployment-author-facing registration
shape consumed by ``Publisher``. The publisher runs these adapters
through ``okg.substrate.publisher.native_view_stage``: the views
framework computes content, and the derived-artifact manifest is the
canonical publication record.

The legacy disk-manifest runtime has been retired. Native publication
never writes view output to deployment-local files.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:  # avoid circular import at runtime; types only
    from okg.substrate.contracts.deployment_product import ReadAssetResolution
    from okg.views.execution.llm import LLMClient
    from okg.views.planners.base import ViewDefinition
    from okg.views.types import Anchor


__all__ = [
    "AnchorResolver",
    "MaterializeOutcome",
    "READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE",
    "ResolvedReadAssetFactoryAuthority",
    "ViewDeltaScope",
    "ViewMaterializer",
]


# An AnchorResolver maps a fragment's source-anchor id to the substrate
# node id the anchor cites, or returns None when the anchor is aggregate
# / non-citing. The native V1 publisher stage projects those citations
# through the durable edge log.
AnchorResolver = Callable[[str], Optional[str]]


READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE = "__okg_read_asset_factory_authority__"


@dataclass(frozen=True, slots=True)
class ResolvedReadAssetFactoryAuthority:
    """Exact canonical resolution selected by an opt-in view factory.

    This marker carries the full typed resolution rather than a caller-authored
    id/digest/token triple. ``ViewMaterializer`` accepts only this exact marker
    type and derives all three execution fields from the selected asset.
    """

    resolution: "ReadAssetResolution"
    asset_id: str


@dataclass
class ViewDeltaScope:
    """Opt-in compute-incrementality contract handed to ``build_corpus``.

    When the publisher materializes a view whose definition is unchanged and a
    prior relation manifest exists (so the scheduler will take the scope-level
    ``refresh_relation_artifact`` carry-forward path), it hands the view the set
    of graph entities that changed this cycle, filtered to the subtypes/edge
    types the view declared it reads. A ``build_corpus`` that accepts a
    ``delta_scope`` keyword may scan ONLY those entities and stage ONLY the
    affected scopes' rows, instead of full-scanning the graph.

    The author MUST populate ``dirty_scope_keys`` with the COMPLETE set of scope
    keys whose rows could change — including scopes that became empty (e.g. a
    deleted file). The scheduler replaces exactly those scopes and carries every
    other scope forward from the prior generation, so an under-counted set
    serves stale rows (a P0). The substrate verifies byte-equality against a
    forced full rebuild; when in doubt, leave a scope IN.

    ``changed_nodes`` maps node subtype -> changed node ids; ``changed_edges``
    maps edge type -> changed edge ids (ints). ``changed_edge_endpoints`` maps
    edge type -> endpoint rows for changed edges when the source write-set can
    provide them, including deleted edges that are no longer visible at branch
    head. All are already filtered to the view's declared reads.
    """

    changed_nodes: dict[str, list[str]] = field(default_factory=dict)
    changed_edges: dict[str, list[int]] = field(default_factory=dict)
    changed_node_attr_fields: dict[
        str, dict[str, list[str]]
    ] = field(default_factory=dict)
    changed_edge_endpoints: dict[
        str, list[dict[str, str | int]]
    ] = field(default_factory=dict)
    # OUTPUT: build_corpus writes the complete dirty scope-key set here.
    dirty_scope_keys: set[str] = field(default_factory=set)
    # OUTPUT: build_corpus sets this True ONLY when it produced delta rows for a
    # COMPLETE dirty_scope_keys set (safe to scope-refresh). If it cannot
    # guarantee completeness — e.g. a deleted edge whose owning scope it cannot
    # resolve — it MUST leave this False and stage the full row set; the
    # substrate then falls back to the full diff. Worst case: no speedup, never a
    # stale carry-forward.
    scoped: bool = False
    # Optional output: build_corpus may record internal timings so benchmark
    # reports can isolate O(delta) extraction costs without changing output.
    timings_ms: dict[str, float] = field(default_factory=dict)
    # Optional output: scoped build_corpus implementations may hand relation
    # rows directly to sql_relation output slots, keyed by the template's
    # direct_rows_key. The publisher still validates scope/row identity/columns;
    # this only avoids a temp-table round trip when the rows were already built.
    relation_rows: dict[str, list[Mapping[str, Any]]] = field(default_factory=dict)
    # Optional output: a direct relation row producer may prove that a slot's
    # dirty rows changed, letting the scheduler skip the prior-row equality
    # probe and go straight to the dirty-scope refresh path. Absence preserves
    # the conservative compare-then-carry-forward behavior.
    relation_rows_known_changed: set[str] = field(default_factory=set)


@dataclass
class MaterializeOutcome:
    """Result of one publish-stage materializer invocation per view.

    Carries enough detail for the publisher to log + for tests
    to assert on. ``error`` is set iff the materialization
    failed; the publisher continues with subsequent views (a
    failure in one view does NOT abort the publish cycle —
    derived projections are best-effort, mirroring the existing
    enricher discipline).
    """

    view_id: str
    generation_id: int
    # Canonical authoring identity selected before the materializer was
    # imported. Runtime manifests/receipts point back to this asset but never
    # become authorable assets themselves.
    asset_id: Optional[str] = None
    asset_digest: Optional[str] = None
    fragments_total: int = 0
    fragments_reused: int = 0
    fragments_refreshed: int = 0
    fragments_added: int = 0
    fragments_dropped: int = 0
    llm_calls: int = 0
    manifest_path: Optional[Path] = None
    manifest_hash: Optional[str] = None
    freshness: str = "fresh"  # fresh | stale | degraded | failed
    status: str = "success"  # success | degraded | failed
    cause: Optional[str] = None
    omitted_work: list[str] = field(default_factory=list)
    scope: dict[str, Any] = field(default_factory=dict)
    retryable: Optional[bool] = None
    error: Optional[str] = None
    elapsed_ms: Optional[float] = None
    # Performance instrumentation (per materialization, per generation):
    corpus_build_ms: Optional[float] = None  # wall time inside build_corpus
    corpus_scan_scoped: bool = False  # True iff build_corpus ran O(delta)
    dirty_scope_count: Optional[int] = None  # # scopes rebuilt (scoped runs)
    relation_rows: Optional[int] = None  # rows the relation slot produced
    timings_ms: dict[str, float] = field(default_factory=dict)


@dataclass
class ViewMaterializer:
    """Deployment-author-facing registration for one materialized view.

    Deployment authors normally define these through
    ``views_registry.yaml``. ``okg catalog load`` projects registry
    entries as ``view_definition`` graph nodes, and publisher startup
    resolves those nodes into ``ViewMaterializer`` instances.

    Attributes
    ----------
    view_def_factory :
        Zero-arg callable returning a ``ViewDefinition``. Called
        once per materialization run so per-cycle parameter
        mutations don't leak between runs.
    build_corpus :
        ``(conn, *, generation_id) -> dict[str, Anchor]``. The
        deployment's corpus-build function (e.g.,
        ``deployments.af_pubs.views.supersedure_health.build_corpus``).
        Receives the publisher's open connection (so reads
        share the cycle's transactional view) and the integer
        generation pin.
    llm_client :
        Required if the planner emits any ``GeneratedSpec``;
        ``None`` is fine for quote-only views and template-only
        views (per Stage 1's ergonomic seam in
        ``add-incremental-views``).
    name :
        Human-readable label for log lines. Defaults to the
        view_def_id when the factory has been called once.
    """

    view_def_factory: Callable[[], "ViewDefinition"]
    build_corpus: Callable[..., "dict[str, Anchor]"]
    llm_client: Optional["LLMClient"] = None
    name: str = ""
    # Per `replace-portal-findings-with-views`: optional
    # callable that maps `anchor_id -> Optional[node_id]`.
    anchor_resolver: Optional[AnchorResolver] = None
    # The node subtypes and edge types this view's corpus reads. Mirrors the
    # enricher `reads_subtypes` / `reads_edge_types` declaration. The publisher
    # cancels a carry-forward when the generation's Chronos write-set touched any
    # declared type, so a view whose output depends on edges (or whose corpus is
    # still empty) rebuilds when its inputs appear — closing the node-only
    # lineage gap where an edge-derived view never re-materialized.
    reads_subtypes: tuple[str, ...] = ()
    reads_edge_types: tuple[str, ...] = ()
    # Inputs read from outside the graph (``publish_time``, ``source_runs``,
    # ``live:<relation>``). The publisher rebuilds a view that declares any,
    # on every published generation; see PACT view-output-carry-forward-3981.
    external_inputs: tuple[str, ...] = ()
    # Populated only after canonical typed-asset resolution. Native publication
    # refuses every materializer missing this exact digest-bound identity/token.
    asset_id: Optional[str] = None
    asset_digest: Optional[str] = None
    asset_execution_token: object | None = None

    def __post_init__(self) -> None:
        marker_missing = object()
        marker = getattr(
            self.view_def_factory,
            READ_ASSET_FACTORY_AUTHORITY_ATTRIBUTE,
            marker_missing,
        )
        if marker is marker_missing:
            return

        from okg.substrate.contracts.deployment_product import (
            ReadAssetResolution,
        )

        if type(marker) is not ResolvedReadAssetFactoryAuthority:
            raise ValueError(
                "native view factory authority must use the exact typed marker"
            )
        if type(marker.resolution) is not ReadAssetResolution:
            raise ValueError(
                "native view factory authority must carry a typed read-asset resolution"
            )

        selected_asset = next(
            (
                asset
                for asset in marker.resolution.assets
                if asset.qualified_id == marker.asset_id
            ),
            None,
        )
        if selected_asset is None:
            raise ValueError(
                "native view factory authority names an unselected read asset"
            )
        execution_token = marker.resolution.execution_token(marker.asset_id)
        derived = (
            marker.asset_id,
            selected_asset.digest,
            execution_token,
        )
        explicit = (
            self.asset_id,
            self.asset_digest,
            self.asset_execution_token,
        )
        explicit_presence = tuple(item is not None for item in explicit)
        if any(explicit_presence) and not all(explicit_presence):
            raise ValueError(
                "native view canonical authority fields must be all derived or all explicit"
            )
        if all(explicit_presence) and explicit != derived:
            raise ValueError(
                "native view explicit canonical authority does not match its factory resolution"
            )
        if not any(explicit_presence):
            self.asset_id, self.asset_digest, self.asset_execution_token = derived
