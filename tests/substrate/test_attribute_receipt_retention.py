"""okg#1603: attribute-integrity receipts vs the interval retention prune.

The incremental receipt advances a running count by SUBTRACTING the attribute
payload as of the prior receipt's ordinal and ADDING the new one. The retention
prune in the same publish cycle deletes CLOSED interval rows outside its window,
which at ``keep_generations = 1`` is exactly the stab the next receipt reads. On
okg-workspace that cost a publish: the generation committed with 11,772 nodes and
the workflow still reported ``incremental attribute receipt count delta disagrees
with checkpoint``.

These are publish-path tests: they run real ``Publisher.cycle()`` publishes and
read the receipts the publish wrote. They live in their own file so the per-PR
database lane owns two publish-heavy rows rather than the whole interval oracle
suite.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.rows import dict_row  # noqa: E402
from okg.substrate.db.connection import connect as db_connect  # noqa: E402

from okg.substrate.attribute_integrity import (  # noqa: E402
    ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,
    ATTRIBUTE_INTEGRITY_STATE_TABLE,
)
import okg.substrate.attributes as attributes_module  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _facts(suffix, ids, attrs, *, rev=1):
    return [
        NodeFact(
            node_id=f"doc:{i}:{suffix}",
            subtype="doc",
            attrs=attrs,
            source_record_id={"id": f"doc:{i}:{suffix}"},
            source_revision={"v": rev},
        )
        for i in ids
    ]


def _apply(dsn: str, facts) -> None:
    if not facts:
        return
    with db_connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True
        )




def _publish(dsn: str):
    return Publisher(
        dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)
    ).cycle()


def _latest_receipt(dsn: str) -> dict:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor(
        row_factory=dict_row
    ) as cur:
        cur.execute(
            f"SELECT r.* FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} s "
            f"JOIN {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} r "
            "ON r.receipt_digest = s.latest_receipt_digest "
            "WHERE s.deployment_name = %s AND s.branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        return dict(cur.fetchone())


def _max_ordinal(dsn: str) -> int:
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT max(ordinal) FROM okg.generation_ordinals "
            "WHERE branch_id = 'default'"
        )
        return int(cur.fetchone()[0])


def test_incremental_receipt_survives_an_ordinal_consumed_by_a_failed_publish(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """okg#1603: an ordinal consumed by a failed publish must not wedge the
    attribute-integrity receipt chain.

    Before okg#1587, a publish that failed after ``assign_generation_ordinal``
    committed left the ordinal minted and no receipt for it. A failed publish
    now rolls its ordinal back (asserted below), but deployments that ran the
    old code still carry such a gap, and the receipt chain must tolerate it:
    the retry must still produce a sound receipt at the ordinal it takes, and
    the publish after it must return to the O(changed) incremental path rather
    than re-anchoring forever. The gap is recreated directly, as the old code
    left it: an ordinal row whose checkpoint was never created.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    assert _publish(dsn)["status"] == "published"
    anchor = _latest_receipt(dsn)
    assert anchor["receipt_type"] == "anchor"
    assert int(anchor["ordinal"]) == 0

    # A publish that fails AFTER assign_generation_ordinal has run no longer
    # consumes the ordinal (okg#1587): it rolls back with the publish.
    real_prune = attributes_module.prune_effective_interval_view_closed
    calls = {"n": 0}

    def failing_prune(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("injected failure after the ordinal was minted")
        return real_prune(*args, **kwargs)

    monkeypatch.setattr(
        attributes_module, "prune_effective_interval_view_closed", failing_prune
    )
    _apply(dsn, _facts(suffix, [1], {"k": "changed"}, rev=2))
    with pytest.raises(RuntimeError, match="injected failure after the ordinal"):
        _publish(dsn)
    monkeypatch.setattr(
        attributes_module, "prune_effective_interval_view_closed", real_prune
    )
    assert calls["n"] == 1
    assert _max_ordinal(dsn) == 0, "the failed publish consumed an ordinal"

    # Recreate the gap a pre-#1587 failure left behind: ordinal 1 names a
    # checkpoint that was never created, and no receipt covers it.
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.generation_ordinals "
            "(checkpoint_id, branch_id, ordinal, previous_checkpoint_id, "
            "deployment_name) "
            "SELECT %s, 'default', 1, checkpoint_id, %s "
            "FROM okg.generation_ordinals "
            "WHERE branch_id = 'default' AND ordinal = 0",
            (f"okg:default:branch:default:gen:stranded:{suffix}", _DEPLOYMENT),
        )
        assert cur.rowcount == 1
    consumed = _max_ordinal(dsn)
    assert consumed == 1, consumed

    # The retry publishes over the gap: it re-anchors (its scope is full), and
    # its receipt must be sound at the ordinal it actually took.
    _apply(dsn, _facts(suffix, [2], {"k": "changed-too"}, rev=2))
    third = _publish(dsn)
    assert third["status"] == "published"
    retry_receipt = _latest_receipt(dsn)
    assert int(retry_receipt["ordinal"]) == _max_ordinal(dsn)
    assert retry_receipt["logical_node_count"] == retry_receipt[
        "checkpoint_node_count"
    ]
    assert retry_receipt["logical_edge_count"] == retry_receipt[
        "checkpoint_edge_count"
    ]
    with db_connect(dsn, autocommit=True) as conn:
        classification = attributes_module.classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
    assert classification["integrity_receipt"]["ok"] is True, classification

    # And the chain is NOT wedged: the next publish returns to the O(changed)
    # fast path from that receipt.
    _apply(dsn, _facts(suffix, [0], {"k": "changed-again"}, rev=2))
    fourth = _publish(dsn)
    assert fourth["status"] == "published"
    receipt = _latest_receipt(dsn)
    assert receipt["receipt_type"] == "incremental", (
        fourth["attribute_effective_view_refresh"]["integrity_receipt"],
    )
    assert receipt["prior_receipt_digest"] == retry_receipt["receipt_digest"]
    assert receipt["logical_node_count"] == receipt["checkpoint_node_count"]
    assert receipt["logical_edge_count"] == receipt["checkpoint_edge_count"]


def test_prune_protects_the_ordinal_the_next_receipt_delta_reads(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """okg#1603: retention 1 prunes the stab the incremental delta subtracts.

    With the protected ordinal the closed rows survive and the fast path holds;
    without it they are deleted, and the receipt must REFUSE (the publisher
    re-anchors) rather than compute a delta against a truncated old side.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    assert _publish(dsn)["status"] == "published"
    anchor = _latest_receipt(dsn)
    assert anchor["receipt_type"] == "anchor"
    assert int(anchor["ordinal"]) == 0

    real_prune = attributes_module.prune_effective_interval_view_closed
    # The publish path binds keep_generations from the signature default, so
    # drive the window through the wrapper AND tell the receipt guard the same
    # retention the prune is running with.
    monkeypatch.setattr(
        attributes_module, "EFFECTIVE_VIEW_RETENTION_GENERATIONS", 1
    )

    def retention_one(conn, **kwargs):
        kwargs["keep_generations"] = 1
        return real_prune(conn, **kwargs)

    monkeypatch.setattr(
        attributes_module, "prune_effective_interval_view_closed", retention_one
    )
    _apply(dsn, _facts(suffix, [1], {"k": "changed"}, rev=2))
    second = _publish(dsn)
    assert second["status"] == "published"
    receipt = _latest_receipt(dsn)
    assert receipt["receipt_type"] == "incremental", receipt
    assert receipt["prior_receipt_digest"] == anchor["receipt_digest"]

    # Same retention, protection dropped: the old stab is gone, and the receipt
    # must say so instead of failing the publish on a truncated delta.
    def retention_one_unprotected(conn, **kwargs):
        kwargs["keep_generations"] = 1
        kwargs.pop("protect_from_ord", None)
        return real_prune(conn, **kwargs)

    monkeypatch.setattr(
        attributes_module,
        "prune_effective_interval_view_closed",
        retention_one_unprotected,
    )
    _apply(dsn, _facts(suffix, [2], {"k": "changed-too"}, rev=2))
    third = _publish(dsn)
    assert third["status"] == "published"
    integrity = third["attribute_effective_view_refresh"]["integrity_receipt"]
    assert integrity["advanced"] is False, integrity
    assert integrity["reason"] == "prior_interval_history_pruned", integrity
