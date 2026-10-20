"""okg#3272 (PACT lane-s-link-retry-ready): a link_retry miss is ready only
when the resolver resolves it, and a re-extracted node's stale rows are
acknowledged after a published checkpoint.

The fixture deployment of the #685 alias-recovery tests, with a resolver
that, like a routed or typed resolver, accepts only canonical ids of the
mentioned entity's type: an alias whose text matches but whose canonical is
another type exists in ``okg.aliases`` and still does not resolve.
"""
from __future__ import annotations

from typing import Any, Optional

import psycopg
import pytest

from okg.substrate.catalog.aliases import add_alias
from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.graph_store import ChronosGraphStore, NodeFact
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.projection.retry import LinkRetryKey, record_link_retry_misses
from okg.substrate.projection.router import PgAliasResolver
from okg.substrate.publisher import Publisher
from okg.substrate.types import PhaseBResult
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.test_publisher_chronos_alias_recovery import (  # noqa: E402
    _retry_rows,
    _seed_catalog_and_graph,
)

pytestmark = pytest.mark.postgres

KEY = LinkRetryKey("test", "default", "page:one", "concept", "dm")


class _TypedResolver:
    """Resolves only to a canonical id of the entity's own type."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self._inner = PgAliasResolver(conn)

    def resolve(self, entity: Any) -> Optional[str]:
        canonical = self._inner.resolve(entity)
        if canonical is None or not canonical.startswith(f"{entity.type}:"):
            return None
        return canonical


def _publisher(dsn: str) -> Publisher:
    return Publisher(
        dsn,
        "test",
        extractor=RegexEntityExtractor([("concept", r"\b(dm)\b")]),
        extractor_source_subtypes=["page"],
        routes=[LinkerRoute(
            entity_type="concept",
            src_subtype=None,
            edge_type="mentions",
            dst_template="{resolved}",
        )],
        resolver_factory=_TypedResolver,
        catalog_identity=bind_test_catalog_writer(dsn, "test"),
    )


def _blocked_gates(monkeypatch: pytest.MonkeyPatch) -> Any:
    import okg.substrate.publisher.publisher as publisher_module

    real = publisher_module.evaluate_gates
    monkeypatch.setattr(
        publisher_module,
        "evaluate_gates",
        lambda *args, **kwargs: PhaseBResult(
            source_local={},
            cross_source=[],
            excluded_sources={},
            blocked=True,
            blocked_reason={"reason": "forced_link_retry_test"},
            total_duration_ms=0,
            total_budget_exceeded=False,
        ),
    )
    return lambda: monkeypatch.setattr(publisher_module, "evaluate_gates", real)


# --------------------------------------------------------------------------- #
# The ready rule (task ready-means-resolves)                                  #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("alias_canonical", "aliased_before", "expected_ready"),
    [
        ("concept:diabetes", False, True),  # landed after the route read
        ("site:dm", True, False),           # the route saw it and still missed
        (None, False, False),               # no alias at all
    ],
)
def test_a_miss_is_ready_only_when_its_alias_landed_after_the_route(
    clean_runtime: str, alias_canonical: str | None, aliased_before: bool,
    expected_ready: bool,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        if alias_canonical is not None:
            add_alias(conn, alias="dm", canonical=alias_canonical, scope="concept")
        record_link_retry_misses(
            conn, [KEY], seen_by_route=lambda _v, _identity: aliased_before,
        )
        assert _retry_rows(conn) == [(*_key(KEY), expected_ready)]


def test_an_existing_ready_row_is_reset_when_the_route_saw_the_alias(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        add_alias(conn, alias="dm", canonical="site:dm", scope="concept")
        conn.execute(
            "INSERT INTO okg.link_retry (deployment_name, branch_name, node_id, "
            "entity_type, entity_value, ready_at) VALUES (%s, %s, %s, %s, %s, now())",
            _key(KEY),
        )
        record_link_retry_misses(conn, [KEY], seen_by_route=lambda _v, _identity: True)
        assert _retry_rows(conn) == [(*_key(KEY), False)]


def test_without_a_snapshot_the_value_only_rule_is_unchanged(clean_runtime: str) -> None:
    """Failure case for the old rule: alias text alone marks the row ready
    although the route already saw that alias and missed."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        add_alias(conn, alias="dm", canonical="site:dm", scope="concept")
        record_link_retry_misses(conn, [KEY])
        assert _retry_rows(conn) == [(*_key(KEY), True)]


def test_an_alias_landing_in_a_new_catalog_version_after_the_route_still_wakes(
    clean_runtime: str,
) -> None:
    """The review's race: the route read catalog version N (no alias); a
    catalog apply lands the alias at N+1 before the miss is recorded. The
    snapshot was taken at N, so the row is ready: no lost wakeup."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        # The route saw "dm" at version N; version N+1 lands it again (a new
        # row): it is not the row the route saw, so the miss wakes.
        add_alias(conn, alias="dm", canonical="site:dm", scope="concept")
        seen = {
            ("dm", (int(r[0]), str(r[1])))
            for r in conn.execute(
                "SELECT catalog_version_id, xmin::text FROM okg.aliases WHERE alias = 'dm'"
            ).fetchall()
        }
        conn.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) VALUES ('applied', now())"
        )
        add_alias(conn, alias="dm", canonical="concept:diabetes", scope="concept")
        record_link_retry_misses(
            conn, [KEY], seen_by_route=lambda v, identity: (v, identity) in seen,
        )
        assert _retry_rows(conn) == [(*_key(KEY), True)]


# --------------------------------------------------------------------------- #
# The production publisher (tasks reuse and stale)                             #
# --------------------------------------------------------------------------- #


def test_a_miss_the_resolver_cannot_resolve_no_longer_defeats_noop_reuse(
    clean_runtime: str,
) -> None:
    """An alias with the mention's text lands but resolves to another type.
    The waker marks the row ready once; the next publish re-extracts, still
    misses, and records it unready; a publish with no input change after
    that is reused. Before the fix it was invalidated by link_retry_ready on
    every publish."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
    publisher = _publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), False)]
        add_alias(conn, alias="dm", canonical="site:dm", scope="concept")
        assert _retry_rows(conn) == [(*_key(KEY), True)]

    woken = publisher.cycle()
    assert woken["status"] == "published"
    assert woken.get("extractor_edges_emitted", 0) == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), False)]

    quiet = publisher.cycle()
    assert quiet.get("reused_generation") is True, quiet
    assert (quiet.get("noop_reuse_invalidation") or {}).get("reason") != "link_retry_ready"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), False)]


def test_a_reextracted_node_loses_the_unready_rows_it_no_longer_observes(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact("page:two", "page", {"text": "dm again"}, {}, {})],
            source="alias-recovery-fixture",
        )
    publisher = _publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"
    other = LinkRetryKey("test", "default", "page:two", "concept", "dm")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), False), (*_key(other), False)]
        # page:one no longer mentions dm; page:two is untouched.
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact("page:one", "page", {"text": "patient recovered"}, {}, {"v": 2})],
            source="alias-recovery-fixture",
        )

    # The unready row of a mention the node no longer has goes when the node
    # is re-extracted, even in a cycle that does not publish (that cycle
    # stamps the extraction, so no later cycle would revisit the node).
    restore = _blocked_gates(monkeypatch)
    assert publisher.cycle()["status"] == "blocked"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(other), False)]
    restore()
    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(other), False)]


def test_a_stale_ready_row_waits_for_a_published_checkpoint(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A woken (ready) row the re-extraction no longer observes keeps
    today's receipt rule: deleted only after a published checkpoint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
    publisher = _publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("UPDATE okg.link_retry SET ready_at = now()")
        ChronosGraphStore.from_connection(conn, ensure_schema=False).apply_facts(
            [NodeFact("page:one", "page", {"text": "patient recovered"}, {}, {"v": 2})],
            source="alias-recovery-fixture",
        )
    restore = _blocked_gates(monkeypatch)
    assert publisher.cycle()["status"] == "blocked"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), True)]
    restore()
    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == []


def test_a_ready_row_of_a_node_that_cannot_be_extracted_is_acknowledged(
    clean_runtime: str,
) -> None:
    """A ready row whose node is gone is targeted, skipped (nothing to
    extract) and, before the fix, invalidated no-op reuse on every publish.
    Now it joins the receipt and goes with the published checkpoint."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
    publisher = _publisher(clean_runtime)
    assert publisher.cycle()["status"] == "published"
    gone = LinkRetryKey("test", "default", "page:gone", "concept", "dm")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.link_retry (deployment_name, branch_name, node_id, "
            "entity_type, entity_value, ready_at) VALUES (%s, %s, %s, %s, %s, now())",
            _key(gone),
        )
    assert publisher.cycle()["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert (*_key(gone), True) not in _retry_rows(conn)
    quiet = publisher.cycle()
    assert quiet.get("reused_generation") is True, quiet


def test_an_alias_removed_and_readded_during_the_route_still_wakes(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The review's lost-wakeup race through the real publisher: the alias
    the snapshot saw is removed before the route (so the route misses) and
    landed again after it. The row it lands is a new row, so the miss is
    ready. Failure case: a snapshot of alias text alone records it unready
    and loses the wakeup."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_and_graph(conn)
        add_alias(conn, alias="dm", canonical="concept:diabetes", scope="concept")
    publisher = _publisher(clean_runtime)
    real_route = publisher._extractor_stage.route
    calls: list[str] = []

    def route(ent: Any, **kw: Any) -> Any:
        with psycopg.connect(clean_runtime, autocommit=True) as other:
            other.execute("DELETE FROM okg.aliases WHERE alias = 'dm'")
        out = real_route(ent, **kw)
        calls.append(ent.value)
        with psycopg.connect(clean_runtime, autocommit=True) as other:
            add_alias(other, alias="dm", canonical="concept:diabetes", scope="concept")
        return out

    monkeypatch.setattr(publisher._extractor_stage, "route", route)
    assert publisher.cycle()["status"] == "published"
    assert calls == ["dm"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _retry_rows(conn) == [(*_key(KEY), True)]


def _key(key: LinkRetryKey) -> tuple[str, str, str, str, str]:
    return (key.deployment_name, key.branch_name, key.node_id, key.entity_type,
            key.entity_value)
