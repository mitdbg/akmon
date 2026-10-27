"""Issue 3955: the console count rollup recounts the whole graph once when a
cycle changes a large share of it, instead of resolving every changed id at two
checkouts. The cause is recorded and the kept rows stay equal to the oracle.

This is a separate file so it has its own blocking pull-request owner row in
tests/qa_lanes.yaml; tests/substrate/test_console_count_rollup.py is owned only
by the nightly database job.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.derived_artifacts.maintained_derivation import (
    is_sentinel_row,
    parity_check,
)
from okg.substrate.graph_store import (
    CONSOLE_COUNT_ROLLUP_TABLE,
    DEFAULT_BRANCH,
    ChronosGraphStore,
)
from okg.substrate.publisher.console_count_rollup import (
    KIND_SUBTYPE,
    _COUNT_ORACLE,
    _COUNT_SENTINEL,
    maintain_console_count_rollup,
)

pytestmark = pytest.mark.postgres


# 4 nodes: subtype alpha x3, beta x1; source srcA x3, srcB x1. 2 edges.
_NODES = [
    {"node_id": "n:a1", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:a2", "subtype": "alpha", "source": "srcA"},
    {"node_id": "n:b1", "subtype": "beta", "source": "srcA"},
    {"node_id": "n:c1", "subtype": "alpha", "source": "srcB"},
]
_EDGES = [
    {"edge_id": 1, "src": "n:a1", "dst": "n:a2", "edge_type": "rel_one"},
    {"edge_id": 2, "src": "n:a1", "dst": "n:b1", "edge_type": "rel_two"},
]


def _seed_graph(store: ChronosGraphStore) -> None:
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows("okg.graph_nodes", _NODES)
        session.upsert_rows("okg.graph_edges", _EDGES)


def _seed_n_nodes(store: ChronosGraphStore, n: int, *, n_subtypes: int = 12) -> None:
    nodes = [
        {"node_id": f"bn{i}", "subtype": f"st{i % n_subtypes:02d}",
         "source": f"src{i % 6}"}
        for i in range(n)
    ]
    session = store.checkout(DEFAULT_BRANCH)
    with session.transaction():
        session.upsert_rows("okg.graph_nodes", nodes)


def _read_kept(checkout) -> dict[tuple[str, str], tuple[int]]:
    out: dict[tuple[str, str], tuple[int]] = {}
    for row in checkout.query(
        f"SELECT dimension_kind, dimension_value, item_count "
        f"FROM {CONSOLE_COUNT_ROLLUP_TABLE}"
    ):
        if is_sentinel_row(row, _COUNT_SENTINEL):
            continue
        out[(str(row["dimension_kind"]), str(row["dimension_value"]))] = (
            int(row["item_count"]),
        )
    return out


@pytest.mark.parametrize(
    ("added", "extra_prior", "expect_full"),
    [(5, 0, True), (2, 0, False), (4, 100, False)],
    ids=["bulk", "below_floor", "within_bound"],
)
def test_console_count_rollup_large_change_set_recounts_in_full(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    added: int,
    extra_prior: int,
    expect_full: bool,
) -> None:
    """Issue 3955: a bulk change recounts the whole graph once instead of
    resolving every changed id at two checkouts, records the cause, and stays
    equal to the oracle. Other changes still fold incrementally.

    The floor is lowered to 3 ids. On the 6-item graph, 5 added nodes are a
    large change set and 2 are below the floor. With 100 more prior nodes
    (106 items), 4 added nodes pass the floor but stay within the console
    rollup's 5 % bound, so they fold incrementally."""
    import okg.substrate.derived_artifacts.maintained_derivation as kit
    import okg.substrate.publisher.console_count_rollup as console

    # raising=False: against a tree without the bound the test must fail on
    # behavior (an id-list lookup runs), not on a missing constant.
    monkeypatch.setattr(kit, "LARGE_CHANGE_SET_MIN_IDS", 3, raising=False)
    if expect_full:
        def _no_id_list_reads(*_args, **_kwargs):
            raise AssertionError("a large change set must not resolve ids one by one")

        monkeypatch.setattr(console, "_changed_buckets", _no_id_list_reads)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        _seed_graph(store)
        if extra_prior:
            _seed_n_nodes(store, extra_prior)
        maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        new_ids = [f"n:bulk{i}" for i in range(added)]
        session = store.checkout(DEFAULT_BRANCH)
        with session.transaction():
            session.upsert_rows(
                "okg.graph_nodes",
                [{"node_id": nid, "subtype": "beta", "source": "srcB"} for nid in new_ids],
            )
        summary = maintain_console_count_rollup(
            conn,
            generation_id=2,
            changed_node_ids=new_ids,
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
        )
        assert summary["mode"] == "incremental"
        if expect_full:
            assert summary["full_reason"] == "large_change_set", summary
            assert summary["dirty_bucket_count"] is None, summary
        else:
            assert "full_reason" not in summary, summary

        checkout = store.checkout(DEFAULT_BRANCH)
        kept = _read_kept(checkout)
        result = parity_check(_COUNT_ORACLE, checkout, kept)
        assert result.ok, (result, kept)
        assert kept[(KIND_SUBTYPE, "beta")] == (1 + added,)
