"""ivm-effective-attribute-view / task.interval-row-representation: the WIN, measured.

Deterministic proof that the flag DROPS the O(total) per-publish carry-forward
INSERT: on a graph of N live targets, a publish that changes ONE target

  * flag OFF: carries forward ~N unchanged rows into the new snapshot
    (_MATERIALIZE_INSTRUMENTATION['carried_rows'] >= N -- the O(total) write), while
  * flag ON: carries forward ZERO (the snapshot is changed-only; unchanged targets
    live in the interval table) -- per-publish write is O(changed).

Run with -s to also see the wall-clock attribute_effective_view_materialize timing
for each (the ~410ms-bench cost vs the O(changed) cost).
"""
from __future__ import annotations

import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    _MATERIALIZE_INSTRUMENTATION,
    _reset_materialize_instrumentation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_N = int(os.environ.get("OKG_IVM_PERF_N", "2000") or "2000")
_DEPLOYMENT = "default"


def _facts(suffix, ids, val):
    return [
        NodeFact(f"doc:{i}:{suffix}", "doc", {"k": val}, {"id": f"doc:{i}:{suffix}"}, {"v": 1})
        for i in ids
    ]


def _seed_then_change_one(dsn: str) -> dict:
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            _facts(suffix, range(_N), "v"), source="default", strict_endpoints=True
        )
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()  # g1: all-N new (flag-on: backfills intervals)
    # Change exactly ONE of the N live targets.
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    f"doc:0:{suffix}", "doc", {"k": "CHANGED"},
                    {"id": f"doc:0:{suffix}"}, {"v": 2},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    _reset_materialize_instrumentation()
    res = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()  # g2: 1 changed of N
    return {
        "carried": _MATERIALIZE_INSTRUMENTATION["carried_rows"],
        "materialize_ms": res["timings_ms"].get("attribute_effective_view_materialize"),
        "sync_ms": res["timings_ms"].get("effective_interval_view_sync"),
    }


def test_perf_carry_forward_baseline_flag_off(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Pin the flag OFF explicitly. This test read as "flag off" only because the
    # shipped default used to be off; once the cutover flipped the default to on
    # it silently measured the flag-ON path and asserted the flag-OFF number.
    # Its flag-on sibling below has always pinned explicitly -- match it.
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    m = _seed_then_change_one(clean_runtime)
    print(
        f"\n[carry-forward BASELINE flag-off] N={_N} changed=1 -> "
        f"carried_rows={m['carried']} materialize={m['materialize_ms']}ms"
    )
    # Flag off: the O(total) carry-forward INSERT -- the ~N-1 unchanged rows
    # (all but the one changed) are re-stamped into the new generation's snapshot.
    assert m["carried"] >= _N - 1, m["carried"]


def test_perf_carry_forward_dropped_flag_on(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    m = _seed_then_change_one(clean_runtime)
    print(
        f"\n[carry-forward DROPPED flag-on]  N={_N} changed=1 -> "
        f"carried_rows={m['carried']} materialize={m['materialize_ms']}ms "
        f"interval_sync={m['sync_ms']}ms"
    )
    # Flag on: ZERO carry-forward -- the O(total) per-publish write is eliminated.
    assert m["carried"] == 0, m["carried"]
