"""The per-deployment publish fence stays concurrency=1.

Every child of this umbrella adds INTRA-stage parallelism. None may enable
concurrent publish cycles for one deployment: the fence is what makes a
generation's commit protocol single-writer, and the crash/resume model
(`formal/consistency/DbosCommitResume.tla`) assumes exactly one publish
workflow instance per generation.

This is the regression guard for that, asserted structurally because the
alternative — discovering it when two cycles interleave on a live deployment —
corrupts a generation.
"""
from __future__ import annotations

import inspect

from okg.substrate.runtime import partition_queue as pq


def test_the_deployment_queue_is_registered_with_concurrency_one() -> None:
    src = inspect.getsource(pq)
    assert "concurrency=1" in src, (
        "the per-deployment queue no longer declares concurrency=1; concurrent "
        "publish cycles for one deployment become possible"
    )


def test_the_registration_default_is_one() -> None:
    sig = inspect.signature(pq.register_deployment_partition_queue)
    concurrency = sig.parameters.get("concurrency")
    assert concurrency is not None, "the concurrency parameter disappeared"
    assert concurrency.default == 1, (
        f"the queue's default concurrency is {concurrency.default}, not 1"
    )


def test_no_concurrency_term_is_wired_into_the_publish_fence() -> None:
    """The budget registry sizes intra-stage lanes. If a term ever reached the
    partition queue, a manifest could widen the fence — which is the one thing
    this program must never do."""
    src = inspect.getsource(pq)
    assert "concurrency_budget" not in src, (
        "the partition queue now draws from the concurrency registry; a "
        "manifest width could widen the publish fence"
    )


def test_every_new_parallel_lane_is_intra_stage() -> None:
    """The lanes this program added must all be bounded by their own terms, none
    of which is the fence."""
    from okg.substrate.db import concurrency_budget as budget

    names = {t.name for t in budget.declared_terms()}
    # The fence is not a registry term and must not become one.
    assert not any("fence" in n or "partition_queue" in n for n in names), (
        f"a registry term now names the publish fence: {names}"
    )
    # And the lanes that DO exist are the intra-stage ones.
    assert {"gate.invariants", "embeddings.batch", "enrichment.model_calls"} <= names
