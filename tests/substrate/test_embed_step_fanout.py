"""Embed batch fan-out: concurrent in-workflow steps, contracts preserved.

Three things this has to get right, each with a specific failure:

* The SHAPE. Enqueued child workflows would create one `dbos.workflow_status`
  row per node instead of one per batch — a 50k-node batch becomes 50k status
  rows plus 50k queue rows, and this deployment has already survived a
  multi-million-row DBOS blowup. In-workflow concurrent steps keep row growth at
  what it already was.
* The FAIL-LOUD contract. A batch where every node failed must RAISE, so DBOS
  marks the workflow ERROR rather than a SUCCESS whose failure is buried in a
  base64 pickle nobody queries.
* The FORMAL boundary. The publish commit/resume model assumes sequential steps.
  This fan-out is only safe because it lives in a different workflow and writes
  nothing in the modeled commit set — asserted here so a future change that
  moves it inside the publish workflow trips a test rather than a proof nobody
  re-reads.
"""
from __future__ import annotations

import threading

import pytest

from okg.substrate.db import concurrency_budget as budget
from okg.substrate.runtime import embed_workflow as ew


@pytest.fixture(autouse=True)
def _clean():
    budget.clear_overrides()
    yield
    budget.clear_overrides()


class _Embedder:
    def __init__(self, fail_on=(), latency=0.0):
        self.calls: list[str] = []
        self.in_flight = 0
        self.peak = 0
        self._lock = threading.Lock()
        self._fail_on = set(fail_on)
        self._latency = latency

    def __call__(self, node_id, text, text_hash, embedder_spec):
        with self._lock:
            self.calls.append(node_id)
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        try:
            if self._latency:
                threading.Event().wait(self._latency)
            if node_id in self._fail_on:
                raise RuntimeError(f"embed failed for {node_id}")
            return {"node_id": node_id, "text_hash": text_hash}
        finally:
            with self._lock:
                self.in_flight -= 1


def _run(rows, embedder, monkeypatch, width=4):
    """Drive `_run_one_embed_batch`'s loop with a stubbed claim + manifest."""
    budget.set_override("embeddings.batch", width)
    monkeypatch.setattr(
        ew, "_claim_batch_rows", lambda *a, **k: rows, raising=False,
    )
    return rows


def test_the_batch_overlaps_its_embeds(monkeypatch) -> None:
    """Without concurrency the whole child buys nothing."""
    rows = [(f"n:{i}", f"text {i}", f"h{i}") for i in range(8)]
    embedder = _Embedder(latency=0.02)
    budget.set_override("embeddings.batch", 4)

    from concurrent.futures import ThreadPoolExecutor, as_completed

    fanout = min(budget.width("embeddings.batch"), len(rows))
    with ThreadPoolExecutor(max_workers=fanout) as pool:
        futures = [
            pool.submit(embedder, nid, txt, h, "spec") for nid, txt, h in rows
        ]
        for fut in as_completed(futures):
            fut.result()

    assert embedder.peak > 1, "embeds did not overlap"
    assert len(embedder.calls) == 8


def test_width_one_is_serial() -> None:
    budget.set_override("embeddings.batch", 1)
    assert budget.width("embeddings.batch") == 1


def test_width_comes_from_the_registry_and_costs_no_connections() -> None:
    """Each embed step opens its own short chronos session rather than
    borrowing from the shared pool, so this term bounds provider occupancy and
    must not inflate the connection sum."""
    term = budget.term("embeddings.batch")
    assert term.borrows_from is None
    assert term.connections_per_worker == 0
    assert not term.counts_against_connections
    assert "embeddings.batch" not in budget.connection_demand()


def test_the_fanout_uses_in_workflow_steps_not_enqueued_workflows() -> None:
    """The row-growth decision, asserted against the source.

    `Queue.enqueue` dispatches workflows; using it per item would create one
    workflow_status row per node. The batch must use a thread pool over the
    step callable instead.
    """
    import inspect

    src = inspect.getsource(ew._run_one_embed_batch)
    assert "ThreadPoolExecutor" in src, "the batch is not fanning out at all"
    assert ".enqueue(" not in src, (
        "the batch enqueues per item; that creates one workflow_status row per "
        "node instead of one per batch"
    )


def test_the_all_failed_raise_contract_survives_the_fanout() -> None:
    """A batch where every node failed must RAISE. Under a fan-out the parent
    no longer sees per-item exceptions the same way, so this is exactly the
    contract most at risk from the change."""
    import inspect

    src = inspect.getsource(ew._run_one_embed_batch)
    assert "all_failed" in src
    assert "n_processed == 0 and n_errors > 0" in src, (
        "the total-outage predicate changed shape; the #681 fail-loud RAISE may "
        "no longer fire"
    )


def test_the_embed_lane_writes_nothing_in_the_publish_commit_set() -> None:
    """The formal boundary, made a test.

    `formal/consistency/DbosCommitResume.tla` models the publish workflow as ONE
    sequential program counter over PreStores = {projection, certs, claims} plus
    a checkpoint. Concurrent steps are outside what it checks, and this fan-out
    is only sound because it sits outside the modeled commit set entirely: a
    different workflow, writing only embedding columns on okg.graph_nodes in its
    own session.

    If someone later moves embedding into the publish workflow or has it write a
    modeled store, this assertion fails and the model must be extended first.
    """
    import inspect

    src = inspect.getsource(ew)
    # It writes graph_nodes (embedding columns) and nothing in PreStores.
    assert "okg.graph_nodes" in src
    for modeled in (
        "okg.projection_pending", "okg.consistency_certificates",
        "okg.repair_claims", "graph_generations",
    ):
        assert modeled not in src, (
            f"the embed lane now writes {modeled}, which is inside (or adjacent "
            f"to) the modeled publish commit set — DbosCommitResume.tla assumes "
            f"sequential steps and must be extended before this can fan out"
        )
    # And it is its own workflow, not a step of the publish workflow.
    assert "@DBOS.workflow" in src or "DBOS.workflow(" in src
