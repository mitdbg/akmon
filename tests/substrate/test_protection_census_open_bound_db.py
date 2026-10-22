"""Protection census open bound on a real interval-backed graph (issue 3881):
live rows count as open, and with no default branch the census raises
instead of measuring against a guessed bound."""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.deployment_protection_census import (
    generate_protection_census,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact


pytestmark = pytest.mark.postgres


@pytest.fixture()
def census_conn(clean_runtime: str):
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        yield conn


def _census(conn):
    return generate_protection_census(conn)


def test_census_counts_live_rows_of_a_published_graph_as_open(
    census_conn,
) -> None:
    """A real interval-backed graph: superseded rows are closed, live rows open.

    The open bound is the branch's current ``live_hi``; comparing against a
    fixed constant counted every live row as closed (issue 3881).
    """
    store = ChronosGraphStore.from_connection(census_conn)
    for revision in (1, 2):
        store.apply_facts(
            [
                NodeFact(
                    node_id="document:census-open-rows",
                    subtype="document",
                    attrs={"title": f"revision {revision}"},
                    source_record_id={"id": "census-open-rows"},
                    source_revision={"revision": revision},
                )
            ],
            source="issue-3881-proof",
            strict_endpoints=True,
        )
        store.checkpoint_generation(
            f"census-open-rows-{revision}",
            deployment_name="census-open-rows",
            status="published",
        )
    census = _census(census_conn)
    measured = [
        e["coldness"] for e in census["entries"]
        if e.get("rule") == "chronos_interval_history"
    ]
    assert measured
    total = sum(c["row_count"] for c in measured)
    closed = sum(c["closed_non_deleted_row_count"] for c in measured)
    deleted = sum(c["deleted_row_count"] for c in measured)
    open_rows = total - closed - deleted
    assert closed >= 1, "the superseded revision row must count as closed"
    assert open_rows >= 1, "live rows must not be counted as closed"


def test_census_refuses_when_the_default_branch_does_not_resolve(
    census_conn,
) -> None:
    """Without a default branch there is no open bound: the census raises
    instead of measuring against a guessed one (no numeric fallback)."""

    class _RollBack(Exception):
        pass

    with pytest.raises(_RollBack):
        with census_conn.transaction():
            with census_conn.cursor() as cur:
                cur.execute(
                    "UPDATE _chronos_branch_interval_branches "
                    "SET branch_id = 'census-moved-away' "
                    "WHERE branch_id = 'default'"
                )
                assert cur.rowcount == 1
            with pytest.raises(SubstrateError, match="default branch"):
                _census(census_conn)
            raise _RollBack
