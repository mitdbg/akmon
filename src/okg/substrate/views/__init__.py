"""Substrate-side adapters for the views framework.

The framework itself (anchors, fragments, refresh, etc.) lives
in ``okg.views``. This package holds the substrate-side glue:

* :class:`ViewMaterializer` — the deployment-author registration
  shape; passed to ``Publisher.__init__(materializers=[...])``.
* :class:`MaterializeOutcome` — return shape of each stage run.

The publish-cycle native stage lives at
``okg.substrate.publisher.native_view_stage``.

The prior ``Materializer`` Protocol (which wrote ``node_facts``
/ ``edge_facts`` directly per view) has been removed: the
umbrella's redesign in ``establish-views-contract`` Decision 2
puts the framework's ``okg.views.refresh.refresh()`` in charge
of that orchestration. Deployment authors register
:class:`ViewMaterializer` instances; the substrate-side stage
calls into the framework.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from okg.substrate.views.base import (
    MaterializeOutcome,
    ViewMaterializer,
)

if TYPE_CHECKING:
    from okg.substrate.views.registry import (
        ApplyViewsRegistryResult,
        ViewRegistryEntry,
        adapt_views_registry_entries,
        apply_views_registry,
        load_views_registry,
        materializers_from_catalog,
        materializers_from_registry,
        materializers_from_registry_entries,
        parse_views_registry,
    )

_REGISTRY_EXPORTS = {
    "ApplyViewsRegistryResult",
    "ViewRegistryEntry",
    "adapt_views_registry_entries",
    "apply_views_registry",
    "load_views_registry",
    "materializers_from_catalog",
    "materializers_from_registry",
    "materializers_from_registry_entries",
    "parse_views_registry",
}


def __getattr__(name: str) -> Any:
    if name in _REGISTRY_EXPORTS:
        from okg.substrate.views import registry

        return getattr(registry, name)
    raise AttributeError(name)

__all__ = [
    "ApplyViewsRegistryResult",
    "MaterializeOutcome",
    "ViewMaterializer",
    "ViewRegistryEntry",
    "adapt_views_registry_entries",
    "apply_views_registry",
    "load_views_registry",
    "materializers_from_catalog",
    "materializers_from_registry",
    "materializers_from_registry_entries",
    "parse_views_registry",
]
