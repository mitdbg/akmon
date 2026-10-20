"""Unit coverage for `LinkerRouter.route` and the alias-resolver seam.

Restores the routing semantics that `tests/substrate/test_extractor_routing.py`
covered before `retire-dead-projection-pool` deleted it. That deletion was
correct — the file drove the retired `Projector.project_batch` — but
`LinkerRouter` itself is LIVE: `publisher/extractor_stage.py:113` calls
`projector._router.route(...)` on every publish cycle. The routing rules below
had no coverage anywhere in `tests/` between that deletion and this file, which
`req.retire-dead-projection-pool.live-coverage-ported-first` forbids.

These are pure unit tests against the router's decision table — no database, no
publisher. The resolver is a seam, so a stub exercises the `{resolved}` paths
including the miss that must be reported as retryable.
"""
from __future__ import annotations

import pytest

from okg.extraction import ExtractedEntity
from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.projection.router import LinkerRouter


class _StubResolver:
    """Alias resolver seam: returns a canonical id, or None for a miss."""

    def __init__(self, table: dict[str, str] | None = None) -> None:
        self._table = table or {}
        self.calls: list[tuple[str, str]] = []

    def resolve(self, entity: ExtractedEntity) -> str | None:
        self.calls.append((entity.type, entity.value))
        return self._table.get(entity.value)


def _entity(type_: str = "ticket", value: str = "TICKET-42", confidence=0.9):
    return ExtractedEntity(
        type=type_, value=value, start=0, end=len(value), confidence=confidence
    )


def _route(**kw):
    base = dict(
        entity_type="ticket",
        src_subtype=None,
        edge_type="mentions",
        dst_template="ticket:{value}",
    )
    base.update(kw)
    return LinkerRoute(**base)


def test_unknown_entity_type_is_dropped() -> None:
    router = LinkerRouter([_route()])
    edge = router.route(
        _entity(type_="unknown_kind"),
        src_node_id="n:doc",
        src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is None


def test_simple_route_wins_and_substitutes_value() -> None:
    router = LinkerRouter([_route()])
    edge = router.route(
        _entity(),
        src_node_id="n:doc",
        src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is not None
    assert (edge.src, edge.edge_type, edge.dst) == (
        "n:doc", "mentions", "ticket:TICKET-42",
    )


def test_src_subtype_specific_route_applies_when_no_simple_route() -> None:
    router = LinkerRouter([
        _route(src_subtype="page", dst_template="page-ticket:{value}"),
        _route(src_subtype="_default", dst_template="any-ticket:{value}"),
    ])
    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is not None and edge.dst == "page-ticket:TICKET-42"


def test_default_route_is_the_fallback_for_an_unlisted_subtype() -> None:
    router = LinkerRouter([
        _route(src_subtype="page", dst_template="page-ticket:{value}"),
        _route(src_subtype="_default", dst_template="any-ticket:{value}"),
    ])
    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="attachment",
        resolver=_StubResolver(),
    )
    assert edge is not None and edge.dst == "any-ticket:TICKET-42"


def test_no_matching_subtype_and_no_default_is_dropped() -> None:
    router = LinkerRouter([_route(src_subtype="page")])
    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="attachment",
        resolver=_StubResolver(),
    )
    assert edge is None


def test_type_and_src_template_fields_substitute() -> None:
    router = LinkerRouter([_route(dst_template="{type}|{src}|{value}")])
    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is not None and edge.dst == "ticket|n:doc|TICKET-42"


def test_resolved_template_uses_the_resolver_result() -> None:
    resolver = _StubResolver({"TICKET-42": "n:canonical-ticket"})
    router = LinkerRouter([_route(dst_template="{resolved}")])
    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="page", resolver=resolver,
    )
    assert edge is not None and edge.dst == "n:canonical-ticket"
    assert resolver.calls == [("ticket", "TICKET-42")]


def test_resolver_miss_drops_the_edge_and_is_reported_as_retryable() -> None:
    """A `{resolved}` route whose alias has not landed yet is the ONE
    retryable drop: the caller records it so a later alias landing re-drives
    extraction. Losing this makes a mention silently unrecoverable."""
    misses: list[tuple[str, str]] = []
    router = LinkerRouter([_route(dst_template="{resolved}")])

    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(), resolver_misses=misses,
    )

    assert edge is None
    assert misses == [("ticket", "TICKET-42")]


def test_no_route_drop_is_not_recorded_as_a_retryable_miss() -> None:
    """Only resolver misses are retryable. A route-table change already
    rotates the extractor fingerprint, so no-route drops re-extract without a
    retry entry — recording them would accumulate permanent garbage."""
    misses: list[tuple[str, str]] = []
    router = LinkerRouter([_route()])

    router.route(
        _entity(type_="unknown_kind"), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(), resolver_misses=misses,
    )

    assert misses == []


def test_self_loop_is_dropped_and_not_recorded() -> None:
    misses: list[tuple[str, str]] = []
    router = LinkerRouter([_route(dst_template="{src}")])

    edge = router.route(
        _entity(), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(), resolver_misses=misses,
    )

    assert edge is None
    assert misses == []


def test_route_confidence_overrides_entity_confidence() -> None:
    router = LinkerRouter([_route(confidence=0.25)])
    edge = router.route(
        _entity(confidence=0.9), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is not None and edge.confidence == pytest.approx(0.25)


def test_entity_confidence_is_used_when_the_route_declares_none() -> None:
    router = LinkerRouter([_route()])
    edge = router.route(
        _entity(confidence=0.33), src_node_id="n:doc", src_subtype="page",
        resolver=_StubResolver(),
    )
    assert edge is not None and edge.confidence == pytest.approx(0.33)


def test_resolver_is_not_consulted_when_the_template_needs_no_resolution() -> None:
    """`{resolved}` is computed lazily. A value-only route must not pay for
    alias resolution — the extraction path runs this per entity per cycle."""
    resolver = _StubResolver()
    router = LinkerRouter([_route(dst_template="ticket:{value}")])

    router.route(
        _entity(), src_node_id="n:doc", src_subtype="page", resolver=resolver,
    )

    assert resolver.calls == []
