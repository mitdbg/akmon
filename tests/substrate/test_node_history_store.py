"""ChronosGraphStore.node_version_history on real publishes, including a fork.

PACT okg-attribute-history-read, req.store: a branch's history is the rows of
its segment chain; a fork shows its parent's versions up to the fork point
and its own after it, the parent never shows the fork's; positions map to
generation ids through the ordinal registry, and a published generation with
no ordinal is refused, never ordered by time. The read also runs as the MCP
read role (okg_mcp), with no grant beyond what it already holds.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.errors import GenerationOutOfScope  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.node_history import (  # noqa: E402
    HistoryGenerationUnordered,
    HistoryRangeInvalid,
    HistoryRowBudgetExceeded,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres


def _publish(dsn: str, *, branch: str = "default") -> str:
    kwargs = {} if branch == "default" else {"branch_name": branch}
    out = Publisher(
        dsn, "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"), **kwargs,
    ).cycle()
    assert out["status"] == "published", out
    return str(out["generation_id"])


def _write(conn, node_id: str, cpus: int, *, branch: str = "default") -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        [NodeFact(node_id, "compute_node", {"cpus": cpus}, {"id": node_id}, {"v": 1})],
        branch=branch,
        source="inventory",
        strict_endpoints=True,
    )


@pytest.fixture
def forked(clean_runtime: str) -> dict:
    """default: g1 (cpus 1), g2 (cpus 2); fork cut after g2 writes cpus 30
    and publishes gf; default then writes cpus 3 and publishes g3."""
    sfx = uuid.uuid4().hex[:8]
    node = f"node:{sfx}"
    fork = f"experiment:{sfx}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["compute_node"])
        _write(conn, node, 1)
    g1 = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _write(conn, node, 2)
    g2 = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).create_branch(
            fork, from_branch="default",
            metadata={"base_branch": "default", "base_generation_id": g2},
        )
        _write(conn, node, 30, branch=fork)
    gf = _publish(clean_runtime, branch=fork)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _write(conn, node, 3)
    g3 = _publish(clean_runtime)
    return {"node": node, "fork": fork, "g1": g1, "g2": g2, "g3": g3, "gf": gf}


def _cpus(history) -> list:
    return [None if s is None else s.attrs.get("cpus") for s in history.states]


def _ids(history) -> list[str]:
    return [g.generation_id for g in history.generations]


def test_a_fork_shows_the_parent_up_to_the_fork_and_its_own_after(
    clean_runtime: str, forked: dict,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        on_fork = store.node_version_history(
            forked["node"], branch=forked["fork"], deployment_name="default",
            to_generation=forked["gf"],
        )
        on_default = store.node_version_history(
            forked["node"], branch="default", deployment_name="default",
            to_generation=forked["g3"],
        )
        # The checkout of each generation agrees with the history's state.
        for history in (on_fork, on_default):
            for generation, state in zip(history.generations, history.states):
                rows = store.checkout_generation(generation.generation_id).query(
                    "SELECT attrs FROM okg.graph_nodes WHERE node_id = :n",
                    {"n": forked["node"]},
                )
                assert [dict(r["attrs"]).get("cpus") for r in rows] == [state.attrs["cpus"]]

    assert _ids(on_fork) == [forked["g1"], forked["g2"], forked["gf"]]
    assert _cpus(on_fork) == [1, 2, 30]
    assert [g.branch for g in on_fork.generations] == ["default", "default", forked["fork"]]
    assert _ids(on_default) == [forked["g1"], forked["g2"], forked["g3"]]
    assert _cpus(on_default) == [1, 2, 3]
    ordinals = [g.ordinal for g in on_fork.generations]
    assert ordinals == sorted(ordinals) and len(set(ordinals)) == 3


def test_the_parent_never_reads_the_fork_generation(clean_runtime: str, forked: dict) -> None:
    """Failure case: the fork's generation is outside the parent's history."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with pytest.raises(GenerationOutOfScope):
            store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                to_generation=forked["gf"],
            )
        # Nor does the fork read the parent's post-fork generation.
        with pytest.raises(GenerationOutOfScope):
            store.node_version_history(
                forked["node"], branch=forked["fork"], deployment_name="default",
                to_generation=forked["g3"],
            )


def test_a_window_bounds_the_generations_and_a_reversed_one_is_refused(
    clean_runtime: str, forked: dict,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        window = store.node_version_history(
            forked["node"], branch="default", deployment_name="default",
            from_generation=forked["g2"], to_generation=forked["g3"],
        )
        assert _ids(window) == [forked["g2"], forked["g3"]]
        with pytest.raises(HistoryRangeInvalid):
            store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                from_generation=forked["g3"], to_generation=forked["g1"],
            )


def test_a_generation_with_no_ordinal_is_refused(clean_runtime: str, forked: dict) -> None:
    """Failure case: with g2's ordinal gone, its position cannot be placed;
    the read is refused rather than ordered by checkpoint time."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "DELETE FROM okg.generation_ordinals WHERE checkpoint_id = %s",
            (forked["g2"],),
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with pytest.raises(HistoryGenerationUnordered) as refused:
            store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                to_generation=forked["g3"],
            )
        assert refused.value.data["generation_ids"] == [forked["g2"]]
        # A window that does not cover g2 still reads.
        later = store.node_version_history(
            forked["node"], branch="default", deployment_name="default",
            from_generation=forked["g3"], to_generation=forked["g3"],
        )
        assert _cpus(later) == [3]


def test_more_rows_than_the_budget_is_refused_not_truncated(
    clean_runtime: str, forked: dict,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with pytest.raises(HistoryRowBudgetExceeded) as refused:
            store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                to_generation=forked["g3"], row_budget=1,
            )
    assert refused.value.data["budget"] == 1
    assert refused.value.data["budget_name"] == "node_history_row_budget"


def test_the_budget_counts_only_the_versions_of_the_window_read(
    clean_runtime: str, forked: dict,
) -> None:
    """A node with more stored versions than the budget still reads over a
    window that holds fewer: rows outside the window's positions are never
    scanned. A window holding more is still refused (failure case)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        for generation, cpus in (("g1", 1), ("g2", 2), ("g3", 3)):
            single = store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                from_generation=forked[generation], to_generation=forked[generation],
                row_budget=1,
            )
            assert _cpus(single) == [cpus] and single.rows_scanned == 1
        with pytest.raises(HistoryRowBudgetExceeded):
            store.node_version_history(
                forked["node"], branch="default", deployment_name="default",
                from_generation=forked["g2"], to_generation=forked["g3"],
                row_budget=1,
            )


def test_the_read_runs_as_okg_mcp_with_its_existing_grants(
    clean_runtime: str, forked: dict,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("SET SESSION ROLE okg_mcp")
        try:
            assert conn.execute("SELECT current_user").fetchone()[0] == "okg_mcp"
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            history = store.node_version_history(
                forked["node"], branch=forked["fork"], deployment_name="default",
                to_generation=forked["gf"],
            )
        finally:
            conn.execute("RESET ROLE")
    assert _cpus(history) == [1, 2, 30]
