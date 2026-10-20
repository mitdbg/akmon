"""Live-path coverage for extractor edge emission.

Ported from the retired batch-projection tests (`test_extractor_routing.py`,
`projection/test_link_cache_alias_recovery.py`), which exercised routing, alias
resolution, resolver-miss recording and edge dedupe through
``Projector._extract_and_emit_edges``. That path has no callers — the live
implementation is ``Publisher._emit_chronos_extractor_edge_facts`` — so this
file re-asserts the same behaviors against the code that actually runs, per
`req.retire-dead-projection-pool.live-coverage-ported-first`.

The publisher is built against an unusable DSN and driven with doubles: the
emit path only needs a cursor, a chronos checkout that answers an edge_id
query, and the two link-retry helpers, all of which are substituted here.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.catalog.linker_config import LinkerRoute
from okg.substrate.projection import RegexEntityExtractor
from okg.substrate.publisher import Publisher, publisher as publisher_mod


def _extractor_config() -> dict[str, object]:
    return {
        "extractor": RegexEntityExtractor([("ticket", r"(TICKET-\d+)")]),
        "extractor_source_subtypes": ["page"],
        "routes": [
            LinkerRoute(
                entity_type="ticket",
                src_subtype=None,
                edge_type="mentions",
                dst_template="ticket:{value}",
            ),
        ],
    }


class _FakeCursor:
    """Answers the two dedupe reads and captures edge_facts inserts."""

    def __init__(self, present_dedupe_keys: set[str]) -> None:
        self._present = present_dedupe_keys
        self.inserts: list[tuple] = []
        self._rows: list[tuple] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def execute(self, sql: str, params: tuple | None = None) -> None:
        if "SELECT DISTINCT dedupe_key" in sql:
            keys = params[0] if params else []
            self._rows = [(k,) for k in keys if k in self._present]
        elif "INSERT INTO okg.edge_facts" in sql:
            self.inserts.append(params)
            self._rows = []
        elif "max(catalog_version_id)" in sql:
            # okg#3272: the route's catalog version, then its alias snapshot
            # (the second read finds no alias here).
            self._rows = [(1,)]
        else:
            self._rows = []

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class _FakeConn:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _FakeCheckout:
    """Stands in for the chronos checkout's already-live edge_id probe."""

    def __init__(self, live_edge_ids: set[int] | None = None) -> None:
        self._live = live_edge_ids or set()

    def query(self, sql: str, params: dict) -> list[dict]:
        ids = params.get("edge_ids", [])
        return [{"edge_id": i} for i in ids if int(i) in self._live]


@pytest.fixture
def emit(monkeypatch):
    """Drive `_emit_chronos_extractor_edge_facts` with doubles.

    Returns a callable taking the node rows plus optional dedupe state, and
    yielding the publisher's return value alongside the captured inserts and
    the link-retry keys the run recorded.
    """
    def _run(
        miss_nodes,
        *,
        present_dedupe_keys: set[str] | None = None,
        live_edge_ids: set[int] | None = None,
        resolver_misses_for=None,
        prior_ready=(),
    ):
        pub = Publisher("postgres://unused", "test", **_extractor_config())

        recorded: list = []
        monkeypatch.setattr(
            publisher_mod, "record_link_retry_misses",
            lambda conn, misses, **_kw: recorded.extend(misses),
        )
        monkeypatch.setattr(
            publisher_mod, "clear_unobserved_unready_link_retries",
            lambda conn, keys: 0,
        )
        monkeypatch.setattr(
            publisher_mod, "ready_link_retry_keys_for_nodes",
            lambda conn, **kw: list(prior_ready),
        )

        if resolver_misses_for is not None:
            real_route = pub._extractor_stage.route

            def _route(ent, *, src_node_id, src_subtype, resolver, resolver_misses):
                miss = resolver_misses_for(ent)
                if miss is not None:
                    resolver_misses.append(miss)
                    return None
                return real_route(
                    ent,
                    src_node_id=src_node_id,
                    src_subtype=src_subtype,
                    resolver=resolver,
                    resolver_misses=resolver_misses,
                )

            monkeypatch.setattr(pub._extractor_stage, "route", _route)

        monkeypatch.setattr(
            pub._extractor_stage, "resolver_factory", lambda conn: object(),
        )

        cur = _FakeCursor(present_dedupe_keys or set())
        conn = _FakeConn(cur)
        inserted, receipt = pub._emit_chronos_extractor_edge_facts(
            conn,
            checkout=_FakeCheckout(live_edge_ids),
            generation_id=7,
            miss_nodes=miss_nodes,
        )
        return SimpleNamespace(
            inserted=inserted,
            receipt=receipt,
            inserts=cur.inserts,
            retry_recorded=recorded,
        )

    return _run


def test_routes_an_entity_mention_to_a_derived_edge(emit) -> None:
    """Routing: a matched entity becomes one edge with the route's shape."""
    out = emit([("n:page1", "page", "see TICKET-42 for details")])

    assert out.inserted == 1
    assert len(out.inserts) == 1
    params = out.inserts[0]
    assert params[1] == "n:page1"
    assert params[2] == "ticket:TICKET-42"
    assert params[3] == "mentions"


def test_text_free_nodes_emit_nothing(emit) -> None:
    """A node with no text is skipped rather than routed on empty input."""
    out = emit([("n:page1", "page", ""), ("n:page2", "page", None)])

    assert out.inserted == 0
    assert out.inserts == []


def test_repeated_mention_dedupes_to_one_edge(emit) -> None:
    """Dedupe: the same (src, edge_type, dst) collapses to a single insert."""
    out = emit([("n:page1", "page", "TICKET-42 and again TICKET-42")])

    assert out.inserted == 1


def test_edge_already_in_edge_facts_is_not_reinserted(emit) -> None:
    """Dedupe against edge_facts: a present dedupe_key suppresses the insert."""
    import uuid

    key = uuid.uuid5(
        uuid.NAMESPACE_URL,
        "_extractor|n:page1|mentions|ticket:TICKET-42",
    ).hex
    out = emit(
        [("n:page1", "page", "TICKET-42")],
        present_dedupe_keys={key},
    )

    assert out.inserted == 0
    assert out.inserts == []


def test_edge_already_live_is_not_reinserted(emit) -> None:
    """Dedupe against the live graph: a present edge_id suppresses the insert."""
    from okg.substrate.graph_store import _edge_id

    live = _edge_id("n:page1", "mentions", "ticket:TICKET-42")
    out = emit(
        [("n:page1", "page", "TICKET-42")],
        live_edge_ids={int(live)},
    )

    assert out.inserted == 0
    assert out.inserts == []


def test_resolver_miss_is_recorded_for_retry(emit) -> None:
    """Alias resolution: an unresolved entity records a link-retry key and
    emits no edge, so the mention is retried rather than silently dropped."""
    out = emit(
        [("n:page1", "page", "TICKET-42")],
        resolver_misses_for=lambda ent: ("ticket", "TICKET-42"),
    )

    assert out.inserted == 0
    assert len(out.retry_recorded) == 1
    key = out.retry_recorded[0]
    assert key.node_id == "n:page1"
    assert key.entity_type == "ticket"
    assert key.entity_value == "TICKET-42"


def test_resolved_mention_clears_a_prior_retry(emit) -> None:
    """A previously-pending retry key that no longer misses is returned as a
    receipt, which is how the cleared retry gets retired."""
    from okg.substrate.projection.retry import LinkRetryKey

    stale = LinkRetryKey("test", "default", "n:page1", "ticket", "TICKET-99")
    out = emit(
        [("n:page1", "page", "TICKET-42")],
        prior_ready=(stale,),
    )

    assert stale in out.receipt
