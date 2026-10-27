"""okg#3112 step 2, slice 1: generation parity (PACT lane-h-3112-step2,
req.lane-h-3112-step2.parity). Native PostgreSQL.

* Cut-over gate 1 (design.md section 7): the reconciler builds every declared
  index on an empty deployment BEFORE the first publish; the E11 data then
  runs as three real publishes (``apply_facts`` + ``Publisher.cycle()``).
  At every ordinal the rows each index holds equal the graph rows that
  admit the key (G = X, read through the index with the index's own
  predicate). Failure case: an index built without ``deleted = false``.

The view-against-graph drift checks (gate 4, ``verify --against-graph``) are
gone with the effective-attribute view (PACT lane-s-storage-drop-copies).
"""

from __future__ import annotations

from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg.rows import dict_row  # noqa: E402

import declared_attr_index_support as s  # noqa: E402
from okg.substrate import declared_attr_indexes as dai  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402

pytestmark = pytest.mark.postgres


def _three_publishes(dsn: str, identity: Any) -> list[dict[str, Any]]:
    out = []
    for generation in (s.generation_1, s.generation_2, s.generation_3):
        nodes, rels = generation()
        s.apply(dsn, nodes)
        if rels:
            s.apply(dsn, rels)
        out.append(s.publish(dsn, identity))
    return out


_g_equals_x = s.g_equals_x




def test_gate1_indexes_built_before_the_first_publish_are_maintained_by_writes(
    clean_runtime: str,
) -> None:
    dsn = clean_runtime
    identity = s.seed(dsn)
    built = s.reconcile(dsn)
    assert built["status"] == "converged", built
    # Nodes only: no edge index is declared (okg#3112 step 3, decision 287d).
    assert len(built["built"]) == len(s.DECL)
    assert s.ordinals(dsn) == [], "the indexes must exist before the first publish"

    _three_publishes(dsn, identity)
    ordinals = s.ordinals(dsn)
    assert len(ordinals) == 3

    result = _g_equals_x(dsn)
    assert result["mismatches"] == []
    assert result["plans_without_index"] == []
    # Every (ordinal, kind, attr) pair compared, and the corpus is not trivial.
    assert len(result["pairs"]) == 3 * len(s.DECL)
    assert {k for _o, k, _a, _n in result["pairs"]} == {"node"}
    assert sum(n for *_, n in result["pairs"]) > 1000
    assert all(n > 0 for o, k, a, n in result["pairs"] if a == "t_text")

    assert not hasattr(dai, "verify_against_attr_index")


def test_gate1_failure_case_an_index_without_deleted_false_holds_tombstones(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dsn = clean_runtime
    # Since okg#3284 a new delete marker keeps only its key and keep set, so
    # its attrs are NULL and cannot reach this index. The markers that still
    # carry a declared key are the full-copy ones written before that change,
    # which deployments hold until scripts/storage/clear_tombstone_payloads.sql
    # runs. Write these retractions in that legacy shape, explicitly: the
    # upstream Chronos full copy, with OKG's keep sets withheld. The witness
    # below no longer depends on what a new marker carries.
    from chronos_core.branching import _interval_backend as interval_backend
    from okg.substrate.graph_store_internal import schema as store_schema

    # monkeypatch restores OKG's keep sets after the test. Every store
    # construction reapplies them through the schema mixin, so stub that
    # call where the mixin looks it up.
    monkeypatch.setattr(interval_backend, "_TOMBSTONE_KEEP", {})
    monkeypatch.setattr(interval_backend, "_TOMBSTONE_KEEP_DEFAULT", None)
    monkeypatch.setattr(
        store_schema, "configure_tombstone_payload", lambda: None
    )
    identity = s.seed(dsn, {"t_text": "text"})
    tables = s.physical(dsn)
    # The wrong index, built before the first publish like the right one.
    bad = "gate1_no_deleted_predicate"
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            f"CREATE INDEX {bad} ON public.\"{tables['node']}\" "
            f"({dai.key_expression('t_text', 'text')}, live_lo, live_hi) "
            "WHERE jsonb_typeof(attrs -> 't_text') = 'string'"
        )
    # Tombstones that keep their declared key: a retraction carrying attrs.
    s.apply(dsn, [s.node(i, 1) for i in range(40)])
    s.publish(dsn, identity)
    s.apply(dsn, [NodeFact(s.nid(i), "thing", {"t_text": f"gone{i}"}, {"row": i}, {"rev": 2}, op="R")
                  for i in range(0, 40, 2)])
    s.publish(dsn, identity)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"SELECT count(*) AS n FROM public.\"{tables['node']}\" "
            "WHERE deleted AND jsonb_typeof(attrs -> 't_text') = 'string'"
        )
        tombstones_with_key = int(cur.fetchone()["n"])
        cur.execute(
            f"SELECT count(*) AS n FROM public.\"{tables['node']}\" WHERE deleted"
        )
        tombstones = int(cur.fetchone()["n"])
        assert tombstones >= 20
        mismatched = []
        for o in s.ordinals(dsn):
            g = s.graph_set(cur, table=tables["node"], kind="node", attr="t_text",
                            value_type="text", bp=int(o["bp"]))
            x, plan = s.index_set(cur, index_name=bad, table=tables["node"], kind="node",
                                  attr="t_text", value_type="text", bp=int(o["bp"]))
            assert bad in plan
            if g != x:
                mismatched.append(int(o["ordinal"]))
    # The legacy full-copy markers carry the declared key; if they ever stop,
    # this failure case must be rebuilt on another witness.
    assert tombstones_with_key > 0, "no tombstone carries the key; the failure arm is vacuous"
    assert mismatched, "an index without deleted = false must disagree with the graph"
