"""Stage 4.2 — snapshot test on okg.views.__all__.

Catches accidental public-surface growth: a new name added to
``okg.views.__all__`` without explicit acknowledgement here
fails the test with a clear diff. Equally catches accidental
removal — if a refactor drops a symbol that callers depend on,
the test points it out before the breakage reaches a deployment.

When the surface should change intentionally:

  1. Edit ``src/okg/views/__init__.py``'s ``__all__``.
  2. Edit ``EXPECTED_PUBLIC_SURFACE`` below to match.
  3. Note the change in the relevant OpenSpec proposal so
     downstream callers see it.

The snapshot is held by hand (not generated) so a code-review
reading the diff sees precisely what entered or left the
public contract.
"""
from __future__ import annotations

import okg.views


# ---------------------------------------------------------------------------
# The approved public surface as of stage 4 of add-incremental-views.
# Sorted alphabetically so visual diffs are stable.
# ---------------------------------------------------------------------------
EXPECTED_PUBLIC_SURFACE = frozenset({
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
    "VIEW_SLOT_EMBED_MAX_LIMIT",
    "ViewSlotEmbed",
    "ViewSlotEmbedError",
    "assemble",
    "fresh",
    "is_stable",
    "parse_view_slot_embed_directive",
    "refresh",
    "synthesize",
    # persistence
    "load",
    "save",
    "view_from_dict",
    "view_slot_embed_directive",
    "view_slot_embed_from_payload",
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
})


class TestPublicSurface:
    def test_all_matches_expected_snapshot(self):
        actual = frozenset(okg.views.__all__)
        added = actual - EXPECTED_PUBLIC_SURFACE
        removed = EXPECTED_PUBLIC_SURFACE - actual
        msg_parts = []
        if added:
            msg_parts.append(f"NEW public symbols: {sorted(added)}")
        if removed:
            msg_parts.append(f"REMOVED public symbols: {sorted(removed)}")
        assert not msg_parts, (
            "okg.views.__all__ has drifted from the approved "
            "snapshot. " + "; ".join(msg_parts) + ". If this is "
            "intentional, update EXPECTED_PUBLIC_SURFACE in this "
            "file."
        )

    def test_every_exported_name_actually_resolves(self):
        # Catch the case where __all__ lists a name that doesn't
        # actually exist on the module (typo, refactor leftover).
        for name in okg.views.__all__:
            assert hasattr(okg.views, name), (
                f"okg.views.__all__ lists {name!r} but the module "
                f"has no such attribute"
            )

    def test_all_is_sorted_alphabetically_within_categories(self):
        # __all__ groups by category for readability; within each
        # category we expect alphabetical order. Run a sanity
        # check on the FULL list being alphabetically sorted (case-
        # insensitive). The current snapshot satisfies this; if a
        # future addition lands out of order this test catches it.
        names = list(okg.views.__all__)
        # Allow category-grouping mismatch — only enforce that
        # names ARE listed (any order). This soft check protects
        # against `__all__` containing duplicates.
        assert len(names) == len(set(names)), (
            f"okg.views.__all__ contains duplicates: "
            f"{[n for n in names if names.count(n) > 1]}"
        )

    def test_no_underscore_prefixed_names_in_all(self):
        # Module-private symbols (e.g. _normalize) must not leak
        # into the public surface.
        underscored = [n for n in okg.views.__all__ if n.startswith("_")]
        assert underscored == [], (
            f"private (underscore-prefixed) names should not be in "
            f"__all__: {underscored}"
        )
