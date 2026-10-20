"""Tests for deterministic derived-edge emission helpers."""
from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact


pytestmark = pytest.mark.postgres


def _seed_live_edge(cur, src: str, dst: str, *, reason: str) -> None:
    """Seed an existing LIVE edge src --references--> dst into the Chronos graph
    (okg.graph_edges) -- this is what insert_deterministic_edges reads for its
    live-dedup after the cutover (no longer okg.edges_live). apply_facts mints the
    edge_id identically to mint_edge_id, so the dedup matches by id + attrs hash.

    Note: the test DB's Chronos graph is NOT reset between tests (clean_runtime
    truncates the relational runtime tables but not the Chronos checkpoint state),
    so each test uses unique node ids to avoid cross-test live-edge collisions.
    """
    ChronosGraphStore.from_connection(cur.connection).apply_facts(
        [
            NodeFact(src, "page", {}, {"id": src}, {"v": 1}),
            NodeFact(dst, "page", {}, {"id": dst}, {"v": 1}),
            EdgeFact(src, dst, "references", attrs={"reason": reason},
                     source_record_id={"id": f"{src}->{dst}"}, source_revision={"v": 1}),
        ],
        source="_seed", strict_endpoints=True,
    )


def test_insert_deterministic_edges_filters_existing_live_edge(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"n:src-{sfx}", f"n:dst-{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_live_edge(cur, src, dst, reason="same")

        result = insert_deterministic_edges(
            cur,
            source="_enricher:test",
            candidates=[
                DerivedEdgeCandidate(
                    src=src,
                    edge_type="references",
                    dst=dst,
                    attrs={"reason": "same"},
                )
            ],
        )

        assert result.inserted == 0
        assert result.skipped_existing_live == 1
        cur.execute("SELECT count(*) FROM okg.edge_facts")
        assert cur.fetchone()[0] == 0


def test_insert_deterministic_edges_filters_existing_dedupe_key(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    candidate = DerivedEdgeCandidate(
        src=f"n:src-{sfx}",
        edge_type="references",
        dst=f"n:dst-{sfx}",
        attrs={"reason": "same"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        first = insert_deterministic_edges(
            cur,
            source="_enricher:test",
            candidates=[candidate],
        )
        second = insert_deterministic_edges(
            cur,
            source="_enricher:test",
            candidates=[candidate],
        )

        assert first.inserted == 1
        assert second.inserted == 0
        assert second.skipped_existing_dedupe == 1
        cur.execute("SELECT count(*) FROM okg.edge_facts")
        assert cur.fetchone()[0] == 1


def test_insert_deterministic_edges_second_generation_emits_zero(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"n:src-{sfx}", f"n:dst-{sfx}"
    candidate = DerivedEdgeCandidate(
        src=src,
        edge_type="references",
        dst=dst,
        attrs={"reason": "same"},
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        first = insert_deterministic_edges(
            cur,
            source="_enricher:test",
            candidates=[candidate],
        )
        # The first gen's edge becomes live (published to the Chronos graph).
        _seed_live_edge(cur, src, dst, reason="same")

        second = insert_deterministic_edges(
            cur,
            source="_enricher:test-later",
            candidates=[candidate],
        )

        assert first.inserted == 1
        assert second.inserted == 0
        assert second.skipped_existing_live == 1
        cur.execute("SELECT count(*) FROM okg.edge_facts")
        assert cur.fetchone()[0] == 1


def test_insert_deterministic_edges_changed_attrs_emit_new_fact(
    clean_runtime: str,
) -> None:
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"n:src-{sfx}", f"n:dst-{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        _seed_live_edge(cur, src, dst, reason="old")

        result = insert_deterministic_edges(
            cur,
            source="_enricher:test",
            candidates=[
                DerivedEdgeCandidate(
                    src=src,
                    edge_type="references",
                    dst=dst,
                    attrs={"reason": "new"},
                )
            ],
        )

        assert result.inserted == 1
        assert result.skipped == 0


def test_insert_deterministic_edges_scrubs_nul_and_rejects_identity(
    clean_runtime: str,
) -> None:
    """PACT sanitize-fact-text-at-admission (adversarial-review
    follow-up): this insert path bypassed the shared normalizer —
    attr values scrub to U+FFFD with counts; a candidate with NUL in
    an identity field OR in provenance (a closed CHECK vocabulary a
    scrubbed value could never satisfy) is rejected loudly instead
    of aborting the whole executemany."""
    sfx = uuid.uuid4().hex[:8]
    good_src, good_dst = f"n:gsrc-{sfx}", f"n:gdst-{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        result = insert_deterministic_edges(
            cur,
            source="_enricher:nul-test",
            candidates=[
                DerivedEdgeCandidate(
                    src=good_src,
                    edge_type="references",
                    dst=good_dst,
                    attrs={"reason": "cite\x00marker"},
                ),
                DerivedEdgeCandidate(
                    src=f"n:bsrc-{sfx}",
                    edge_type="references",
                    dst=f"n:\x00bdst-{sfx}",
                ),
                DerivedEdgeCandidate(
                    src=f"n:psrc-{sfx}",
                    edge_type="references",
                    dst=f"n:pdst-{sfx}",
                    provenance="derived\x00deterministic",
                ),
            ],
        )

        assert result.inserted == 1
        assert result.sanitized_value_count == 1
        # One NUL identity + one NUL provenance rejection.
        assert result.rejected_identity_nul == 2
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = '_enricher:nul-test'"
        )
        assert cur.fetchone()[0] == 1
        cur.execute(
            "SELECT attrs->>'reason', provenance "
            "FROM okg.edge_facts WHERE src = %s",
            (good_src,),
        )
        reason, provenance = cur.fetchone()
        assert reason == "cite�marker"
        assert provenance == "derived_deterministic"
