"""Views layer — fragment-level provenance over the substrate.

A view is a function from graph state to a rendered artifact
(structured table, audit report, or LLM-generated prose) with
declared dependencies, fragment-level provenance, and replayable
refresh. See ``openspec/changes/establish-views-contract`` for
the cross-cutting contract and ``openspec/changes/add-incremental-views``
for this layer's implementation slice.

This package is **library-only**. There is no top-level
``okg-view`` CLI verb (per the umbrella's Decision 2 — view
materialization runs as a stage of the publish cycle, owned by
``viz-materialized-view-nodes``). The public surface below is
the entire programmatic contract; everything else under
``okg.views.*`` is implementation detail.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------
from okg.views.types import (
    Anchor,
    Fragment,
    View,
    content_hash,
)

# ---------------------------------------------------------------------------
# Hermeticity wrapper
# ---------------------------------------------------------------------------
from okg.views.restricted_corpus import (
    HermeticityViolation,
    RestrictedCorpus,
)

# ---------------------------------------------------------------------------
# LLM dispatch (Protocol + two implementations + error class)
# ---------------------------------------------------------------------------
from okg.views.execution.errors import LLMCallFailed
from okg.views.execution.llm import (
    DeterministicStubLLM,
    LiteLLMClient,
    LLMClient,
)

# ---------------------------------------------------------------------------
# Algorithms — synthesize / refresh / assemble
# ---------------------------------------------------------------------------
from okg.views.assembly import assemble
from okg.views.refresh import (
    RefreshStats,
    fresh,
    is_stable,
    refresh,
)
from okg.views.synthesis import synthesize

# ---------------------------------------------------------------------------
# Markdown embed helpers
# ---------------------------------------------------------------------------
from okg.views.slot_embeds import (
    VIEW_SLOT_EMBED_MAX_LIMIT,
    ViewSlotEmbed,
    ViewSlotEmbedError,
    parse_view_slot_embed_directive,
    view_slot_embed_directive,
    view_slot_embed_from_payload,
)

# ---------------------------------------------------------------------------
# Persistence — atomic disk manifest writes
# ---------------------------------------------------------------------------
from okg.views.persistence import (
    load,
    save,
    view_from_dict,
    view_to_dict,
)

# ---------------------------------------------------------------------------
# Planner-side types
# ---------------------------------------------------------------------------
from okg.views.planners.base import (
    FragmentSpec,
    GeneratedSpec,
    PlanFn,
    QuoteSpec,
    ViewRelationColumn,
    ViewOutputTemplate,
    ViewDefinition,
    default_output_templates,
)

# ---------------------------------------------------------------------------
# Declarative view graphs
# ---------------------------------------------------------------------------
from okg.views.view_graphs import (
    FlatPageSpec,
    NeighborhoodPageSpec,
    VIEW_GRAPH_ARTIFACT_TYPE,
    VIEW_GRAPH_MEDIA_TYPE,
    ViewGraph,
    ViewGraphEdge,
    ViewGraphNode,
    ViewGraphSpec,
    build_collection_view_graph,
    load_view_graph_spec,
    view_graph_assembler,
    view_graph_payload_from_view,
    view_graph_planner,
    view_graph_to_corpus,
)

# ---------------------------------------------------------------------------
# Substrate adapter — read corpus at a generation pin
# ---------------------------------------------------------------------------
from okg.views.backbone import (
    AnchorBuilder,
    BackboneError,
    NoPublishedGenerationError,
    NodeRow,
    NodeSelector,
    by_type,
    by_type_and_attrs,
    clear_corpus_cache,
    corpus_from_okg,
    corpus_from_okg_cached,
    edges_as_anchors,
    edges_at_generation,
    render_markdown_table,
    resolve_published_generation,
    sql_view_anchor,
)


# Public surface — the *entire* programmatic contract for this
# package. Internal modules (and module-private helpers like
# ``_normalize``) are NOT re-exported. New names land here only
# when explicitly added; the snapshot test
# ``tests/views/test_public_surface.py`` fails if this list
# drifts from what the surface review approved.
__all__ = [
    # data types
    "Anchor",
    "Fragment",
    "View",
    "content_hash",
    # hermeticity
    "HermeticityViolation",
    "RestrictedCorpus",
    # LLM
    "DeterministicStubLLM",
    "LLMCallFailed",
    "LLMClient",
    "LiteLLMClient",
    # algorithms
    "RefreshStats",
    "assemble",
    "fresh",
    "is_stable",
    "refresh",
    "synthesize",
    # markdown embed helpers
    "VIEW_SLOT_EMBED_MAX_LIMIT",
    "ViewSlotEmbed",
    "ViewSlotEmbedError",
    "parse_view_slot_embed_directive",
    "view_slot_embed_directive",
    "view_slot_embed_from_payload",
    # persistence
    "load",
    "save",
    "view_from_dict",
    "view_to_dict",
    # planner types
    "FragmentSpec",
    "GeneratedSpec",
    "PlanFn",
    "QuoteSpec",
    "ViewRelationColumn",
    "ViewOutputTemplate",
    "ViewDefinition",
    "default_output_templates",
    # declarative view graphs
    "FlatPageSpec",
    "NeighborhoodPageSpec",
    "VIEW_GRAPH_ARTIFACT_TYPE",
    "VIEW_GRAPH_MEDIA_TYPE",
    "ViewGraph",
    "ViewGraphEdge",
    "ViewGraphNode",
    "ViewGraphSpec",
    "build_collection_view_graph",
    "load_view_graph_spec",
    "view_graph_assembler",
    "view_graph_payload_from_view",
    "view_graph_planner",
    "view_graph_to_corpus",
    # substrate adapter
    "AnchorBuilder",
    "BackboneError",
    "NoPublishedGenerationError",
    "NodeRow",
    "NodeSelector",
    "by_type",
    "by_type_and_attrs",
    "clear_corpus_cache",
    "corpus_from_okg",
    "corpus_from_okg_cached",
    "edges_as_anchors",
    "edges_at_generation",
    "render_markdown_table",
    "resolve_published_generation",
    "sql_view_anchor",
]
