from __future__ import annotations

from okg.substrate.enrichers.base import IncrementalContext
from okg.substrate.library.linkers.declarative import DeclarativeLinker
from okg.substrate.library.linkers.rules import CrossLinkRule, RuleEndpoint
from okg.substrate.publisher.enricher_loader import derive_write_envelope


def _alias_rule() -> CrossLinkRule:
    return CrossLinkRule(
        id="entity_mention_resolves_via_alias_table",
        src=RuleEndpoint(subtypes=("entity_mention",), field="entity_value"),
        dst=RuleEndpoint(subtypes=(), via="alias_table"),
        matcher="alias_table",
        edge="mentions",
        bidirectional=True,
    )


def test_alias_table_clean_skip_requires_explicit_opt_in() -> None:
    ctx = IncrementalContext(
        prev_fact_watermark=1,
        prev_edge_watermark=1,
        dirty_node_ids=frozenset(),
        is_first_run=False,
        last_status="success",
    )

    conservative = DeclarativeLinker(rules=[_alias_rule()])
    opted_in = DeclarativeLinker(
        rules=[_alias_rule()],
        publisher_skip_alias_table_when_clean=True,
    )

    assert conservative.can_skip_clean(None, ctx) is False  # type: ignore[arg-type]
    assert opted_in.can_skip_clean(None, ctx) is True  # type: ignore[arg-type]


def test_alias_only_linker_declares_runtime_resolved_edge_envelope() -> None:
    linker = DeclarativeLinker(rules=[_alias_rule()])

    assert linker.requires_narrowings == ()
    envelope = derive_write_envelope(
        linker,
        producer_id="enricher:declarative_alias",
    )

    assert envelope.signature.get("edges", []) == []
    assert envelope.unbounded == ("edges",)
