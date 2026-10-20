"""M5.3 — narrowing enforcement (application writer-path layer).

Layer 1: application writer-path check (okg.substrate.catalog.narrowings.
  assert_narrowing_permitted) — fast, readable errors with full
  context in the data payload. Strict whitelist when narrowings
  have rows at the active catalog_version; no-op when either no
  catalog is applied OR narrowings is empty at the active cv.

RETIRED (chronos cutover): the Layer-2 DB constraint trigger tests
(test_trigger_noop_when_no_narrowings, test_trigger_rejects_unlisted_triple,
test_trigger_accepts_listed_triple, test_trigger_passes_retractions,
test_trigger_honors_session_cache_gucs, test_trigger_uses_latest_nodes_live_subtype)
exercised the schema.sql `edges_live_narrowing_check` CONSTRAINT TRIGGER, which
fired on okg.edges_live INSERT and read okg.nodes_live for src/dst subtypes.
Both bitemporal tables are dropped by the migration (CASCADE removes the
trigger with them), so that enforcement seam no longer exists. The same
narrowing + no_dangling invariants are now enforced over the canonical Chronos
graph (okg.graph_nodes / okg.graph_edges) by the always-on
TransformConsistencyGate, covered by tests/substrate/test_chronos_narrowing_gate.py
(test_chronos_gate_blocks_narrowing_violation). Migrating the trigger seeds
could not save these tests: the edges_live-INSERT trigger they assert is gone,
so they were retired rather than weakened. The app-side checker tests below are
unaffected (they read the okg.narrowings catalog table, not the graph).
"""

from __future__ import annotations

import psycopg
import pytest

from okg.substrate.catalog import (
    NarrowingChecker,
    assert_narrowing_permitted,
)
from okg.substrate.errors import SubstrateError
from tests.pytest.catalog_fixture_builders import seed_catalog, seed_narrowings


pytestmark = pytest.mark.postgres


def _seed_cv(conn: psycopg.Connection) -> int:
    return seed_catalog(conn)


def _install_narrowings(
    conn: psycopg.Connection,
    triples: list[tuple[str, str, str]],
    cv: int,
) -> None:
    seed_narrowings(conn, catalog_version_id=cv, narrowings=triples)


# ---------- app-side checker ----------


def test_checker_inactive_when_no_narrowings(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)
        checker = NarrowingChecker(conn)
    assert checker.is_active is False
    assert checker.is_permitted("page", "references", "concept") is True


def test_checker_active_when_narrowings_exist(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _install_narrowings(conn, [("page", "references", "concept")], cv)
        checker = NarrowingChecker(conn)
    assert checker.is_active is True
    assert checker.is_permitted("page", "references", "concept") is True
    assert checker.is_permitted("page", "references", "ticket") is False


def test_assert_narrowing_permitted_raises(clean_runtime: str) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = _seed_cv(conn)
        _install_narrowings(conn, [("page", "references", "concept")], cv)
        with pytest.raises(SubstrateError) as exc:
            assert_narrowing_permitted(
                conn,
                "page",
                "references",
                "ticket",
                src_node_id="n:src",
                dst_node_id="n:dst",
            )
    err = exc.value
    assert err.code == "narrowing_violation"
    assert err.data["src_subtype"] == "page"
    assert err.data["edge_type"] == "references"
    assert err.data["dst_subtype"] == "ticket"
    assert err.data["src_node_id"] == "n:src"
    assert err.data["dst_node_id"] == "n:dst"


def test_assert_narrowing_permitted_no_op_when_inactive(
    clean_runtime: str,
) -> None:
    """Active cv with zero narrowings installed → every triple passes.

    We deliberately seed a FRESH cv so this test owns the
    max(catalog_version_id), dodging any narrowings rows
    prior tests installed under older cvs.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)  # fresh cv, no narrowings at it
        assert_narrowing_permitted(
            conn,
            "whatever",
            "wtf",
            "rubbish",
        )
